//! Durable Operations-owned identity cuts before leadership confirmation.
//!
//! A prepared roster freezes logical membership admission, not Forge work.
//! The host must still validate its full Profile view and obtain a matching
//! Forge work cut. Dispatched uncertainty cannot be released as an unbound cut.

use super::{LeadershipScope, LeadershipState, work_exchange};
use crate::{LeadershipWorkExchangeReason as Reason, OperationsCatalog, OperationsError, catalog};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, WorkCutScope, WorkspaceId,
};
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod team_nodes;

const MAX_SESSIONS: usize = 128;
const MAX_BINDINGS: usize = 256;
const MAX_PAYLOAD: usize = 262_144;
const TABLE: &str = "CREATE TABLE ops_leadership_work_rosters (cut_id BLOB PRIMARY KEY NOT NULL CHECK(length(cut_id)=16 AND cut_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), election_id BLOB NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), payload BLOB NOT NULL CHECK(length(payload) BETWEEN 1 AND 262144), digest BLOB NOT NULL CHECK(length(digest)=32), state INTEGER NOT NULL CHECK(state IN(0,1,2)), revision INTEGER NOT NULL CHECK(revision>=1), prepared_at_ms INTEGER NOT NULL CHECK(prepared_at_ms>=0), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=prepared_at_ms), UNIQUE(cut_id,workspace_id), CHECK((state=0 AND revision=1 AND updated_at_ms=prepared_at_ms) OR (state IN(1,2) AND revision=2)))";
const ACTIVE: &str = "CREATE UNIQUE INDEX ops_leadership_work_rosters_active ON ops_leadership_work_rosters(workspace_id,election_id) WHERE state IN(0,1)";
const BARRIERS: &str = "CREATE TABLE ops_leadership_work_roster_barriers (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id!=zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id!=zeroblob(16)), cut_id BLOB NOT NULL CHECK(length(cut_id)=16), PRIMARY KEY(workspace_id,profile_id), UNIQUE(workspace_id,participation_id), FOREIGN KEY(cut_id,workspace_id) REFERENCES ops_leadership_work_rosters(cut_id,workspace_id))";
const SESSIONS: &str = "CREATE TABLE ops_leadership_work_roster_sessions (cut_id BLOB NOT NULL REFERENCES ops_leadership_work_rosters(cut_id) CHECK(length(cut_id)=16), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id!=zeroblob(16)), PRIMARY KEY(cut_id,session_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_leadership_work_rosters", TABLE),
        ("index", "ops_leadership_work_rosters_active", ACTIVE),
        ("table", "ops_leadership_work_roster_barriers", BARRIERS),
        ("table", "ops_leadership_work_roster_sessions", SESSIONS),
    ]
}
pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}
#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch("DROP TABLE IF EXISTS ops_approval_request_versions;")?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_leadership_work_roster_sessions; DROP TABLE IF EXISTS ops_leadership_work_roster_barriers; DROP TABLE IF EXISTS ops_leadership_work_rosters;")?;
    Ok(())
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeadershipWorkRosterSide {
    Incumbent,
    Elected,
}
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipWorkRosterState {
    Prepared,
    Dispatched,
    Released,
}
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipWorkRosterPrepareOutcome {
    Recorded(Box<LeadershipWorkRoster>),
    Deferred {
        pending_registrations: u64,
        pending_replacements: u64,
        unresolved_bindings: u64,
    },
    NotRequired(crate::LeadershipWorkExchangeReason),
}
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipWorkRosterDispatch {
    Execute(LeadershipWorkRoster),
    Readback(LeadershipWorkRoster),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PartyWire {
    participation: [u8; 16],
    profile: [u8; 16],
    source_participant: [u8; 16],
    revision: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionWire {
    side: LeadershipWorkRosterSide,
    participant: [u8; 16],
    session: [u8; 16],
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingWire {
    side: LeadershipWorkRosterSide,
    participant: [u8; 16],
    activity: [u8; 16],
    agent: [u8; 16],
    session: [u8; 16],
    principal_kind: i64,
    principal_id: Option<[u8; 16]>,
    source_kind: i64,
    source_request: [u8; 16],
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RosterWire {
    version: u8,
    cut: [u8; 16],
    workspace: [u8; 16],
    scope_kind: i64,
    activity: Option<[u8; 16]>,
    team: Option<[u8; 16]>,
    election: [u8; 16],
    leadership_revision: i64,
    prepared_at_ms: i64,
    parties: [PartyWire; 2],
    sessions: Vec<SessionWire>,
    bindings: Vec<BindingWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    team_nodes: Option<Vec<team_nodes::TeamNodeWitnessWire>>,
}

/// Immutable logical identity evidence; this is not a work-transfer permit.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipWorkRoster {
    wire: RosterWire,
    digest: [u8; 32],
    state: LeadershipWorkRosterState,
    revision: i64,
    updated_at_ms: i64,
}
impl LeadershipWorkRoster {
    #[must_use]
    pub fn cut_id(&self) -> Uuid {
        Uuid::from_bytes(self.wire.cut)
    }
    #[must_use]
    pub fn election_id(&self) -> Uuid {
        Uuid::from_bytes(self.wire.election)
    }
    pub fn scope(&self) -> LeadershipScope {
        self.wire.scope()
    }
    pub const fn state(&self) -> LeadershipWorkRosterState {
        self.state
    }
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    #[must_use]
    pub const fn prepared_at_ms(&self) -> i64 {
        self.wire.prepared_at_ms
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.wire.leadership_revision
    }
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
    pub fn sessions(&self) -> impl ExactSizeIterator<Item = LeadershipWorkRosterSession> + '_ {
        self.wire
            .sessions
            .iter()
            .copied()
            .map(LeadershipWorkRosterSession)
    }
    pub fn bindings(&self) -> impl ExactSizeIterator<Item = LeadershipWorkRosterBinding> + '_ {
        self.wire
            .bindings
            .iter()
            .copied()
            .map(LeadershipWorkRosterBinding)
    }
    #[must_use]
    pub fn participation_id(&self, side: LeadershipWorkRosterSide) -> Uuid {
        Uuid::from_bytes(self.wire.party(side).participation)
    }
    #[must_use]
    pub fn profile_id(&self, side: LeadershipWorkRosterSide) -> Uuid {
        Uuid::from_bytes(self.wire.party(side).profile)
    }

    /// Resolve the exact saved work scope that a Forge dispatch may use.
    ///
    /// # Errors
    /// Rejects Activity scopes and legacy Team rosters that predate mapping witnesses.
    pub fn forge_work_cut_scope(&self) -> Result<WorkCutScope, OperationsError> {
        match self.wire.scope_kind {
            0 => Ok(WorkCutScope::Workspace),
            1 => Err(OperationsError::Conflict(
                "Activity leadership roster has no Forge work-cut scope",
            )),
            2 => {
                let nodes = self
                    .wire
                    .team_nodes
                    .as_deref()
                    .ok_or(OperationsError::Conflict(
                        "Team work roster has no saved node mapping witness",
                    ))?;
                team_nodes::forge_work_cut_scope(
                    Uuid::from_bytes(
                        self.wire
                            .team
                            .ok_or(OperationsError::Corrupt("work roster Team identity"))?,
                    ),
                    nodes,
                )
            }
            _ => Err(OperationsError::Corrupt("work roster scope")),
        }
    }
}
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipWorkRosterSession(SessionWire);
impl LeadershipWorkRosterSession {
    pub const fn side(self) -> LeadershipWorkRosterSide {
        self.0.side
    }
    #[must_use]
    pub fn participant_id(self) -> Uuid {
        Uuid::from_bytes(self.0.participant)
    }
    pub fn session_id(self) -> AgentSessionId {
        AgentSessionId::from_uuid(Uuid::from_bytes(self.0.session))
    }
}
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipWorkRosterBindingOrigin {
    Registration(Uuid),
    LegacyAdoption(Uuid),
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipWorkRosterBinding(BindingWire);
impl LeadershipWorkRosterBinding {
    pub const fn side(self) -> LeadershipWorkRosterSide {
        self.0.side
    }
    #[must_use]
    pub fn participant_id(self) -> Uuid {
        Uuid::from_bytes(self.0.participant)
    }
    #[must_use]
    pub fn activity_id(self) -> ActivityId {
        ActivityId::from_uuid(Uuid::from_bytes(self.0.activity))
    }
    #[must_use]
    pub fn agent_id(self) -> AgentInstanceId {
        AgentInstanceId::from_uuid(Uuid::from_bytes(self.0.agent))
    }
    pub fn session_id(self) -> AgentSessionId {
        AgentSessionId::from_uuid(Uuid::from_bytes(self.0.session))
    }
    /// Decode the already validated principal using the canonical registration codec.
    ///
    /// # Errors
    /// Rejects corrupt principal representation.
    pub fn principal(self) -> Result<AuthenticatedPrincipal, OperationsError> {
        crate::registration::decode_principal_fields(
            self.0.principal_kind,
            self.0.principal_id.as_ref().map(<[u8; 16]>::as_slice),
        )
    }
    #[must_use]
    pub fn source_request_id(self) -> Uuid {
        Uuid::from_bytes(self.0.source_request)
    }
    pub fn origin(self) -> LeadershipWorkRosterBindingOrigin {
        let id = self.source_request_id();
        // Roster validation admits exactly the two canonical binding origins.
        if self.0.source_kind == 0 {
            LeadershipWorkRosterBindingOrigin::Registration(id)
        } else {
            LeadershipWorkRosterBindingOrigin::LegacyAdoption(id)
        }
    }
}

impl RosterWire {
    fn scope(&self) -> LeadershipScope {
        let workspace = WorkspaceId::from_uuid(Uuid::from_bytes(self.workspace));
        if let Some(activity) = self.activity {
            LeadershipScope::activity(workspace, ActivityId::from_uuid(Uuid::from_bytes(activity)))
        } else if let Some(team) = self.team {
            LeadershipScope::team(workspace, Uuid::from_bytes(team))
        } else {
            LeadershipScope::workspace(workspace)
        }
    }
    const fn party(&self, side: LeadershipWorkRosterSide) -> &PartyWire {
        match side {
            LeadershipWorkRosterSide::Incumbent => &self.parties[0],
            LeadershipWorkRosterSide::Elected => &self.parties[1],
        }
    }
    fn validate(&self) -> Result<(), OperationsError> {
        let valid_scope = matches!(
            (self.scope_kind, self.activity, self.team),
            (0, None, None) | (1, Some(_), None) | (2, None, Some(_))
        );
        let valid_team_nodes = matches!(
            (self.version, self.scope_kind, self.team_nodes.is_some()),
            (1, _, false) | (2, 0 | 1, false) | (2, 2, true)
        );
        if !matches!(self.version, 1 | 2)
            || !valid_scope
            || !valid_team_nodes
            || self.cut == [0; 16]
            || self.election == [0; 16]
            || self.leadership_revision < 1
            || self.prepared_at_ms < 0
            || self.sessions.len() > MAX_SESSIONS
            || self.bindings.len() > MAX_BINDINGS
        {
            return Err(OperationsError::Corrupt("work roster envelope"));
        }
        if let Some(nodes) = &self.team_nodes {
            team_nodes::validate_team_nodes(nodes)?;
        }
        for party in self.parties {
            if party.participation == [0; 16]
                || party.profile == [0; 16]
                || party.source_participant == [0; 16]
                || party.revision < 1
            {
                return Err(OperationsError::Corrupt("work roster party"));
            }
        }
        if self.parties[0].participation == self.parties[1].participation
            || self.parties[0].profile == self.parties[1].profile
        {
            return Err(OperationsError::Corrupt("work roster distinct parties"));
        }
        let mut sessions = std::collections::BTreeMap::new();
        for session in &self.sessions {
            if session.session == [0; 16]
                || session.participant == [0; 16]
                || sessions
                    .insert(session.session, (session.side, session.participant))
                    .is_some()
            {
                return Err(OperationsError::Corrupt("work roster Session"));
            }
        }
        let mut agents = std::collections::BTreeSet::new();
        for binding in &self.bindings {
            if !agents.insert(binding.agent)
                || binding.agent == [0; 16]
                || binding.source_request == [0; 16]
                || !matches!(binding.source_kind, 0 | 1)
                || sessions.get(&binding.session) != Some(&(binding.side, binding.participant))
            {
                return Err(OperationsError::Corrupt("work roster binding"));
            }
            let _principal = crate::registration::decode_principal_fields(
                binding.principal_kind,
                binding.principal_id.as_ref().map(<[u8; 16]>::as_slice),
            )?;
        }
        Ok(())
    }
}

impl OperationsCatalog {
    /// Freeze a complete bounded Operations roster before any Broker confirmation intent.
    /// Exact cut replay returns the original roster. Deferred/NotRequired creates no barrier.
    ///
    /// # Errors
    /// Rejects stale elections, unresolved identity, competing barriers, overflow and uncertain commits.
    pub fn prepare_leadership_work_roster(
        &mut self,
        scope: LeadershipScope,
        election_id: Uuid,
        expected_leadership_revision: i64,
        cut_id: Uuid,
        now_ms: i64,
    ) -> Result<LeadershipWorkRosterPrepareOutcome, OperationsError> {
        if cut_id.is_nil() || election_id.is_nil() || now_ms < 0 || expected_leadership_revision < 1
        {
            return Err(OperationsError::Bounds("work roster input"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load(&tx, cut_id)? {
            if existing.scope() != scope
                || existing.election_id() != election_id
                || existing.leadership_revision() != expected_leadership_revision
            {
                return Err(OperationsError::Conflict("work roster replay"));
            }
            catalog::commit_read(tx, self.destination)?;
            return Ok(LeadershipWorkRosterPrepareOutcome::Recorded(Box::new(
                existing,
            )));
        }
        super::governance::ensure_verified_workspace(&tx, scope.workspace_id(), self.destination)?;
        validate_preconfirm(
            &tx,
            scope,
            election_id,
            expected_leadership_revision,
            now_ms,
        )?;
        let (incumbent, elected) = election_pair(&tx, scope, election_id)?;
        let candidate = work_exchange::frozen_candidate(&tx, scope, election_id, elected)?;
        let (previous, conflicting) =
            work_exchange::frozen_incumbent(&tx, scope, election_id, incumbent)?;
        let reason = work_exchange::classify(incumbent, elected, previous, candidate, conflicting);
        if matches!(
            reason,
            Reason::Bootstrap | Reason::SameParticipant | Reason::SameParticipation
        ) {
            catalog::commit_read(tx, self.destination)?;
            return Ok(LeadershipWorkRosterPrepareOutcome::NotRequired(reason));
        }
        let (Some(incumbent), Some(previous), Some(candidate)) = (incumbent, previous, candidate)
        else {
            return Err(OperationsError::Conflict(
                "work roster frozen identity missing",
            ));
        };
        if conflicting {
            return Err(OperationsError::Conflict(
                "work roster frozen identity conflict",
            ));
        }
        let parties = [
            party(&tx, scope.workspace_id(), previous, incumbent)?,
            party(&tx, scope.workspace_id(), candidate, elected)?,
        ];
        let (normal, replacements, unknown_bindings) =
            pending_bindings(&tx, scope.workspace_id(), &parties)?;
        if normal != 0 || replacements != 0 || unknown_bindings != 0 {
            catalog::commit_read(tx, self.destination)?;
            return Ok(LeadershipWorkRosterPrepareOutcome::Deferred {
                pending_registrations: normal,
                pending_replacements: replacements,
                unresolved_bindings: unknown_bindings,
            });
        }
        let wire = build_wire(
            &tx,
            scope,
            election_id,
            expected_leadership_revision,
            cut_id,
            now_ms,
            parties,
        )?;
        wire.validate()?;
        let payload = serde_json::to_vec(&wire)?;
        if payload.len() > MAX_PAYLOAD {
            return Err(OperationsError::Bounds("work roster payload"));
        }
        let digest = payload_digest(&payload);
        insert(&tx, &wire, &payload, digest)?;
        let record = LeadershipWorkRoster {
            wire,
            digest,
            state: LeadershipWorkRosterState::Prepared,
            revision: 1,
            updated_at_ms: now_ms,
        };
        catalog::commit_read(tx, self.destination)?;
        Ok(LeadershipWorkRosterPrepareOutcome::Recorded(Box::new(
            record,
        )))
    }

    /// Read one exact immutable identity cut after uncertain preparation/dispatch.
    ///
    /// # Errors
    /// Rejects foreign scope, corrupt rows and unavailable storage.
    pub fn leadership_work_roster(
        &self,
        scope: LeadershipScope,
        cut_id: Uuid,
    ) -> Result<Option<LeadershipWorkRoster>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let record = load(&tx, cut_id)?;
        if record
            .as_ref()
            .is_some_and(|record| record.scope() != scope)
        {
            return Err(OperationsError::Conflict("work roster scope"));
        }
        catalog::commit_read(tx, self.destination)?;
        Ok(record)
    }

    /// Admit one cut dispatch. A repeated dispatch has readback authority only.
    ///
    /// # Errors
    /// Rejects stale scope/revision/election, released cuts and invalid clocks.
    pub fn dispatch_leadership_work_roster(
        &mut self,
        scope: LeadershipScope,
        cut_id: Uuid,
        expected_revision: i64,
        now_ms: i64,
    ) -> Result<LeadershipWorkRosterDispatch, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut record = require_record(&tx, scope, cut_id)?;
        if record.state == LeadershipWorkRosterState::Dispatched {
            if expected_revision != 1 && expected_revision != record.revision {
                return Err(OperationsError::Conflict("work roster dispatch revision"));
            }
            catalog::commit_read(tx, self.destination)?;
            return Ok(LeadershipWorkRosterDispatch::Readback(record));
        }
        if record.state != LeadershipWorkRosterState::Prepared
            || expected_revision != record.revision
        {
            return Err(OperationsError::Conflict("work roster dispatch state"));
        }
        let _forge_work_cut_scope = record.forge_work_cut_scope()?;
        super::governance::ensure_verified_workspace(&tx, scope.workspace_id(), self.destination)?;
        validate_preconfirm(
            &tx,
            scope,
            record.election_id(),
            record.leadership_revision(),
            now_ms,
        )?;
        transition(&tx, &record, 1, now_ms)?;
        record.state = LeadershipWorkRosterState::Dispatched;
        record.revision = 2;
        record.updated_at_ms = now_ms;
        catalog::commit_read(tx, self.destination)?;
        Ok(LeadershipWorkRosterDispatch::Execute(record))
    }

    /// Release only a cut that has never been dispatched to Forge.
    /// A dispatched cut needs a future correlated Forge resolution, never this method.
    ///
    /// # Errors
    /// Rejects unknown/dispatched cuts, begun confirmations, stale revisions and invalid clocks.
    pub fn release_unbound_leadership_work_roster(
        &mut self,
        scope: LeadershipScope,
        cut_id: Uuid,
        expected_revision: i64,
        now_ms: i64,
    ) -> Result<LeadershipWorkRoster, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut record = require_record(&tx, scope, cut_id)?;
        if record.state == LeadershipWorkRosterState::Released {
            if expected_revision != 1 && expected_revision != record.revision {
                return Err(OperationsError::Conflict("work roster release revision"));
            }
            catalog::commit_read(tx, self.destination)?;
            return Ok(record);
        }
        if record.state != LeadershipWorkRosterState::Prepared
            || expected_revision != record.revision
        {
            return Err(OperationsError::Conflict(
                "work roster may have been dispatched",
            ));
        }
        ensure_confirmation_not_started(&tx, scope, record.election_id())?;
        transition(&tx, &record, 2, now_ms)?;
        tx.execute(
            "DELETE FROM ops_leadership_work_roster_barriers WHERE cut_id=?1 AND workspace_id=?2",
            params![
                cut_id.as_bytes(),
                scope.workspace_id().into_uuid().as_bytes()
            ],
        )?;
        record.state = LeadershipWorkRosterState::Released;
        record.revision = 2;
        record.updated_at_ms = now_ms;
        catalog::commit_read(tx, self.destination)?;
        Ok(record)
    }
}

pub(crate) fn ensure_team_node_mapping_open(
    tx: &Connection,
    workspace: WorkspaceId,
    current_team: Option<Uuid>,
    next_team: Option<Uuid>,
) -> Result<(), OperationsError> {
    team_nodes::ensure_team_node_mapping_open(tx, workspace, current_team, next_team)
}

fn validate_preconfirm(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    election_id: Uuid,
    revision: i64,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let row = super::load_leadership(tx, scope)?
        .ok_or(OperationsError::Conflict("work roster leadership missing"))?;
    if row.state != LeadershipState::AuthorityPending
        || row.current_election_id != Some(election_id)
        || row.leadership_revision != revision
    {
        return Err(OperationsError::Conflict("work roster leadership changed"));
    }
    if now_ms < row.updated_at_ms {
        return Err(OperationsError::Bounds("work roster clock"));
    }
    ensure_confirmation_not_started(tx, scope, election_id)
}

fn ensure_confirmation_not_started(
    tx: &Connection,
    scope: LeadershipScope,
    election_id: Uuid,
) -> Result<(), OperationsError> {
    let attempted:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM ops_leadership_confirm_intents WHERE workspace_id=?1 AND election_id=?2)",params![scope.workspace_id().into_uuid().as_bytes(),election_id.as_bytes()],|row|row.get(0))?;
    if attempted {
        return Err(OperationsError::Conflict(
            "work roster must precede confirmation",
        ));
    }
    Ok(())
}
fn election_pair(
    tx: &Connection,
    scope: LeadershipScope,
    election: Uuid,
) -> Result<(Option<Uuid>, Uuid), OperationsError> {
    let raw:(Option<Vec<u8>>,Vec<u8>)=tx.query_row("SELECT substr(incumbent_participant_id,1,17),substr(elected_participant_id,1,17) FROM ops_leadership_elections WHERE workspace_id=?1 AND election_id=?2 AND scope_kind=?3 AND activity_id IS ?4 AND team_id IS ?5 AND state='CLOSED_ELECTED'",params![scope.workspace_id().into_uuid().as_bytes(),election.as_bytes(),scope.scope_kind(),scope.activity_id().map(|id|id.into_uuid().as_bytes().to_vec()),scope.team_id().map(|id|id.as_bytes().to_vec())],|row|Ok((row.get(0)?,row.get(1)?)))?;
    Ok((
        raw.0
            .map(crate::wire::id)
            .transpose()?
            .map(Uuid::from_bytes),
        Uuid::from_bytes(crate::wire::id(raw.1)?),
    ))
}
fn party(
    tx: &Connection,
    workspace: WorkspaceId,
    participation: Uuid,
    source: Uuid,
) -> Result<PartyWire, OperationsError> {
    let (profile,revision):(Vec<u8>,i64)=tx.query_row("SELECT substr(agent_profile_id,1,17),revision FROM ops_workspace_participations WHERE workspace_id=?1 AND participation_id=?2",params![workspace.into_uuid().as_bytes(),participation.as_bytes()],|row|Ok((row.get(0)?,row.get(1)?)))?;
    Ok(PartyWire {
        participation: participation.into_bytes(),
        profile: crate::wire::id(profile)?,
        source_participant: source.into_bytes(),
        revision,
    })
}
fn pending_bindings(
    tx: &Connection,
    workspace: WorkspaceId,
    parties: &[PartyWire; 2],
) -> Result<(u64, u64, u64), OperationsError> {
    // A Ready activation is not sufficient: Profile anchoring is a separate
    // operation. Current and historical rebind Sessions are both valid evidence.
    let normal: i64 = tx.query_row(
        "SELECT COUNT(*) FROM ops_registration_requests r LEFT JOIN ops_registration_profile_scopes p ON p.request_id=r.request_id WHERE r.workspace_id=?1 AND ((p.request_id IS NULL AND NOT EXISTS(SELECT 1 FROM ops_binding_activations a WHERE a.request_id=r.request_id)) OR (p.profile_id IN(?2,?3) AND (NOT EXISTS(SELECT 1 FROM ops_binding_activations a WHERE a.request_id=r.request_id) OR NOT EXISTS(SELECT 1 FROM ops_logical_bindings lb JOIN ops_workspace_participation_mappings m ON m.workspace_id=lb.workspace_id AND m.participant_id=lb.participant_id WHERE lb.request_id=r.request_id AND lb.workspace_id=r.workspace_id AND m.agent_profile_id=p.profile_id AND (m.session_id=r.session_id OR EXISTS(SELECT 1 FROM ops_workspace_participation_session_rebinds h WHERE h.workspace_id=m.workspace_id AND h.participation_id=m.participation_id AND h.participant_id=m.participant_id AND h.agent_profile_id=m.agent_profile_id AND (h.previous_session_id=r.session_id OR h.new_session_id=r.session_id)))))) OR (p.request_id IS NOT NULL AND (p.workspace_id!=r.workspace_id OR p.session_id!=r.session_id)))",
        params![workspace.into_uuid().as_bytes(), parties[0].profile.as_slice(), parties[1].profile.as_slice()], |row|row.get(0),
    )?;
    let replacements: i64 = tx.query_row("SELECT COUNT(*) FROM ops_workspace_participation_replacement_registrations WHERE workspace_id=?1 AND participation_id IN(?2,?3) AND phase!=3", params![workspace.into_uuid().as_bytes(), parties[0].participation.as_slice(), parties[1].participation.as_slice()], |row|row.get(0))?;
    // Unknown attribution cannot be assumed unrelated merely because activation
    // succeeded. Do not omit a live source binding whose participation is missing.
    let unknown: i64 = tx.query_row("SELECT COUNT(*) FROM ops_current_effect_bindings b WHERE b.workspace_id=?1 AND NOT EXISTS(SELECT 1 FROM ops_workspace_participation_mappings m WHERE m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id)", params![workspace.into_uuid().as_bytes()], |row|row.get(0))?;
    Ok((
        u64::try_from(normal).map_err(|_| OperationsError::Corrupt("work roster pending count"))?,
        u64::try_from(replacements)
            .map_err(|_| OperationsError::Corrupt("work roster replacement count"))?,
        u64::try_from(unknown)
            .map_err(|_| OperationsError::Corrupt("work roster unresolved binding count"))?,
    ))
}
fn build_wire(
    tx: &Connection,
    scope: LeadershipScope,
    election: Uuid,
    revision: i64,
    cut: Uuid,
    now: i64,
    parties: [PartyWire; 2],
) -> Result<RosterWire, OperationsError> {
    let team_nodes = scope
        .team_id()
        .map(|team| team_nodes::capture_team_nodes(tx, scope.workspace_id(), team))
        .transpose()?;
    let mut sessions = Vec::new();
    let mut bindings = Vec::new();
    for (side, party) in [
        (LeadershipWorkRosterSide::Incumbent, parties[0]),
        (LeadershipWorkRosterSide::Elected, parties[1]),
    ] {
        sessions.extend(load_sessions(tx, scope.workspace_id(), party, side)?);
        bindings.extend(load_bindings(tx, scope.workspace_id(), party, side)?);
    }
    if sessions.len() > MAX_SESSIONS || bindings.len() > MAX_BINDINGS {
        return Err(OperationsError::Bounds("work roster inventory"));
    }
    sessions.sort_by_key(|row| row.session);
    bindings.sort_by_key(|row| row.agent);
    Ok(RosterWire {
        version: 2,
        cut: cut.into_bytes(),
        workspace: scope.workspace_id().into_uuid().into_bytes(),
        scope_kind: scope.scope_kind(),
        activity: scope.activity_id().map(|id| id.into_uuid().into_bytes()),
        team: scope.team_id().map(Uuid::into_bytes),
        election: election.into_bytes(),
        leadership_revision: revision,
        prepared_at_ms: now,
        parties,
        sessions,
        bindings,
        team_nodes,
    })
}
fn load_sessions(
    tx: &Connection,
    workspace: WorkspaceId,
    party: PartyWire,
    side: LeadershipWorkRosterSide,
) -> Result<Vec<SessionWire>, OperationsError> {
    let mut q=tx.prepare("WITH members AS (SELECT participant_id,session_id FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participation_id=?2), sessions AS (SELECT participant_id,session_id FROM members UNION SELECT m.participant_id,l.session_id FROM members m JOIN ops_logical_members l ON l.workspace_id=?1 AND l.participant_id=m.participant_id UNION SELECT participant_id,previous_session_id FROM ops_workspace_participation_session_rebinds WHERE workspace_id=?1 AND participation_id=?2 UNION SELECT participant_id,new_session_id FROM ops_workspace_participation_session_rebinds WHERE workspace_id=?1 AND participation_id=?2 UNION SELECT b.participant_id,b.session_id FROM ops_current_effect_bindings b JOIN members m ON m.participant_id=b.participant_id WHERE b.workspace_id=?1) SELECT substr(participant_id,1,17),substr(session_id,1,17) FROM sessions ORDER BY session_id,participant_id LIMIT 129")?;
    let raw = q
        .query_map(
            params![
                workspace.into_uuid().as_bytes(),
                party.participation.as_slice()
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if raw.len() > MAX_SESSIONS {
        return Err(OperationsError::Bounds("work roster Sessions"));
    }
    if raw.is_empty() {
        return Err(OperationsError::Conflict(
            "work roster mapped Sessions missing",
        ));
    }
    raw.into_iter()
        .map(|(participant, session)| {
            Ok(SessionWire {
                side,
                participant: crate::wire::id(participant)?,
                session: crate::wire::id(session)?,
            })
        })
        .collect()
}
struct RawBinding {
    participant: Vec<u8>,
    activity: Vec<u8>,
    agent: Vec<u8>,
    session: Vec<u8>,
    kind: i64,
    principal: Option<Vec<u8>>,
    source: i64,
    request: Vec<u8>,
    current_session: Vec<u8>,
}
fn load_bindings(
    tx: &Connection,
    workspace: WorkspaceId,
    party: PartyWire,
    side: LeadershipWorkRosterSide,
) -> Result<Vec<BindingWire>, OperationsError> {
    let mut q=tx.prepare("SELECT substr(b.participant_id,1,17),substr(b.activity_id,1,17),substr(b.agent_id,1,17),substr(b.session_id,1,17),l.principal_kind,substr(l.principal_id,1,17),b.source_kind,substr(b.source_request_id,1,17),substr(l.session_id,1,17) FROM ops_current_effect_bindings b JOIN ops_workspace_participation_mappings m ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id JOIN ops_logical_members l ON l.workspace_id=b.workspace_id AND l.participant_id=b.participant_id WHERE b.workspace_id=?1 AND m.participation_id=?2 ORDER BY b.agent_id LIMIT 257")?;
    let raw = q
        .query_map(
            params![
                workspace.into_uuid().as_bytes(),
                party.participation.as_slice()
            ],
            |row| {
                Ok(RawBinding {
                    participant: row.get(0)?,
                    activity: row.get(1)?,
                    agent: row.get(2)?,
                    session: row.get(3)?,
                    kind: row.get(4)?,
                    principal: row.get(5)?,
                    source: row.get(6)?,
                    request: row.get(7)?,
                    current_session: row.get(8)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if raw.len() > MAX_BINDINGS {
        return Err(OperationsError::Bounds("work roster bindings"));
    }
    raw.into_iter()
        .map(|r| {
            if r.session != r.current_session {
                return Err(OperationsError::Conflict(
                    "work roster stale current binding Session",
                ));
            }
            Ok(BindingWire {
                side,
                participant: crate::wire::id(r.participant)?,
                activity: crate::wire::id(r.activity)?,
                agent: crate::wire::id(r.agent)?,
                session: crate::wire::id(r.session)?,
                principal_kind: r.kind,
                principal_id: r.principal.map(crate::wire::id).transpose()?,
                source_kind: r.source,
                source_request: crate::wire::id(r.request)?,
            })
        })
        .collect()
}
fn payload_digest(payload: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"heterarchy-leadership-work-roster-v1\0");
    hash.update(payload);
    hash.finalize().into()
}
fn insert(
    tx: &Transaction<'_>,
    wire: &RosterWire,
    payload: &[u8],
    digest: [u8; 32],
) -> Result<(), OperationsError> {
    tx.execute("INSERT INTO ops_leadership_work_rosters(cut_id,workspace_id,election_id,leadership_revision,payload,digest,state,revision,prepared_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,0,1,?7,?7)",params![wire.cut.as_slice(),wire.workspace.as_slice(),wire.election.as_slice(),wire.leadership_revision,payload,digest.as_slice(),wire.prepared_at_ms])?;
    for party in wire.parties {
        tx.execute("INSERT INTO ops_leadership_work_roster_barriers(workspace_id,profile_id,participation_id,cut_id) VALUES(?1,?2,?3,?4)",params![wire.workspace.as_slice(),party.profile.as_slice(),party.participation.as_slice(),wire.cut.as_slice()])?;
    }
    for session in &wire.sessions {
        tx.execute(
            "INSERT INTO ops_leadership_work_roster_sessions(cut_id,session_id) VALUES(?1,?2)",
            params![wire.cut.as_slice(), session.session.as_slice()],
        )?;
    }
    Ok(())
}
struct RawRoster {
    workspace: Vec<u8>,
    election: Vec<u8>,
    leadership: i64,
    payload: Vec<u8>,
    digest: Vec<u8>,
    state: i64,
    revision: i64,
    prepared: i64,
    updated: i64,
}
fn load(tx: &Connection, cut: Uuid) -> Result<Option<LeadershipWorkRoster>, OperationsError> {
    let raw=tx.query_row("SELECT substr(workspace_id,1,17),substr(election_id,1,17),leadership_revision,substr(payload,1,262145),substr(digest,1,33),state,revision,prepared_at_ms,updated_at_ms FROM ops_leadership_work_rosters WHERE cut_id=?1",params![cut.as_bytes()],|r|Ok(RawRoster{workspace:r.get(0)?,election:r.get(1)?,leadership:r.get(2)?,payload:r.get(3)?,digest:r.get(4)?,state:r.get(5)?,revision:r.get(6)?,prepared:r.get(7)?,updated:r.get(8)?})).optional()?;
    raw.map(|raw| decode(tx, cut, raw)).transpose()
}
fn decode(
    tx: &Connection,
    cut: Uuid,
    raw: RawRoster,
) -> Result<LeadershipWorkRoster, OperationsError> {
    if raw.payload.len() > MAX_PAYLOAD {
        return Err(OperationsError::Corrupt("work roster payload bounds"));
    }
    let wire: RosterWire = serde_json::from_slice(&raw.payload)?;
    wire.validate()?;
    let digest = payload_digest(&raw.payload);
    if raw.digest.as_slice() != digest
        || wire.cut != cut.into_bytes()
        || wire.workspace != crate::wire::id(raw.workspace)?
        || wire.election != crate::wire::id(raw.election)?
        || wire.leadership_revision != raw.leadership
        || wire.prepared_at_ms != raw.prepared
        || raw.updated < raw.prepared
    {
        return Err(OperationsError::Corrupt("work roster attribution"));
    }
    let state = match (raw.state, raw.revision) {
        (0, 1) if raw.updated == raw.prepared => LeadershipWorkRosterState::Prepared,
        (1, 2) => LeadershipWorkRosterState::Dispatched,
        (2, 2) => LeadershipWorkRosterState::Released,
        _ => return Err(OperationsError::Corrupt("work roster lifecycle")),
    };
    let expected_barriers = if state == LeadershipWorkRosterState::Released {
        0
    } else {
        2
    };
    let barriers: i64 = tx.query_row(
        "SELECT COUNT(*) FROM ops_leadership_work_roster_barriers WHERE cut_id=?1",
        params![cut.as_bytes()],
        |r| r.get(0),
    )?;
    if barriers != expected_barriers {
        return Err(OperationsError::Corrupt("work roster barrier count"));
    }
    if expected_barriers != 0 {
        for party in wire.parties {
            let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM ops_leadership_work_roster_barriers WHERE cut_id=?1 AND workspace_id=?2 AND profile_id=?3 AND participation_id=?4)",params![cut.as_bytes(),wire.workspace.as_slice(),party.profile.as_slice(),party.participation.as_slice()],|r|r.get(0))?;
            if !exists {
                return Err(OperationsError::Corrupt("work roster barrier identity"));
            }
        }
    }
    let mut q=tx.prepare("SELECT substr(session_id,1,17) FROM ops_leadership_work_roster_sessions WHERE cut_id=?1 ORDER BY session_id LIMIT 129")?;
    let sessions = q
        .query_map(params![cut.as_bytes()], |r| r.get::<_, Vec<u8>>(0))?
        .map(|r| r.map_err(OperationsError::from).and_then(crate::wire::id))
        .collect::<Result<Vec<_>, _>>()?;
    if sessions != wire.sessions.iter().map(|s| s.session).collect::<Vec<_>>() {
        return Err(OperationsError::Corrupt("work roster Session fence"));
    }
    Ok(LeadershipWorkRoster {
        wire,
        digest,
        state,
        revision: raw.revision,
        updated_at_ms: raw.updated,
    })
}
fn require_record(
    tx: &Connection,
    scope: LeadershipScope,
    cut: Uuid,
) -> Result<LeadershipWorkRoster, OperationsError> {
    let record = load(tx, cut)?.ok_or(OperationsError::Conflict("work roster missing"))?;
    if record.scope() != scope {
        return Err(OperationsError::Conflict("work roster scope"));
    }
    Ok(record)
}
fn transition(
    tx: &Connection,
    record: &LeadershipWorkRoster,
    state: i64,
    now: i64,
) -> Result<(), OperationsError> {
    if now < record.prepared_at_ms() {
        return Err(OperationsError::Bounds("work roster transition clock"));
    }
    let changed=tx.execute("UPDATE ops_leadership_work_rosters SET state=?1,revision=2,updated_at_ms=?2 WHERE cut_id=?3 AND state=0 AND revision=1",params![state,now,record.cut_id().as_bytes()])?;
    if changed != 1 {
        return Err(OperationsError::Conflict("work roster revision"));
    }
    Ok(())
}

pub(crate) fn ensure_profile_admission_open(
    tx: &Connection,
    workspace: WorkspaceId,
    profile: AgentProfileId,
) -> Result<(), OperationsError> {
    let blocked:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM ops_leadership_work_roster_barriers WHERE workspace_id=?1 AND profile_id=?2)",params![workspace.into_uuid().as_bytes(),profile.into_uuid().as_bytes()],|r|r.get(0))?;
    if blocked {
        return Err(OperationsError::Conflict(
            "Profile membership is frozen for leadership work cut",
        ));
    }
    Ok(())
}
pub(crate) fn ensure_session_admission_open(
    tx: &Connection,
    workspace: WorkspaceId,
    session: AgentSessionId,
) -> Result<(), OperationsError> {
    let blocked:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM ops_leadership_work_roster_barriers b JOIN ops_leadership_work_roster_sessions s ON s.cut_id=b.cut_id WHERE b.workspace_id=?1 AND s.session_id=?2)",params![workspace.into_uuid().as_bytes(),session.into_uuid().as_bytes()],|r|r.get(0))?;
    if blocked {
        return Err(OperationsError::Conflict(
            "Session membership is frozen for leadership work cut",
        ));
    }
    Ok(())
}
pub(crate) fn ensure_unscoped_admission_open(
    tx: &Connection,
    workspace: WorkspaceId,
) -> Result<(), OperationsError> {
    let blocked: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership_work_roster_barriers WHERE workspace_id=?1)",
        params![workspace.into_uuid().as_bytes()],
        |r| r.get(0),
    )?;
    if blocked {
        return Err(OperationsError::Conflict(
            "unscoped admission during leadership work cut",
        ));
    }
    Ok(())
}

/// Check fresh identity admission as one policy: known Profiles remain scoped,
/// while unprofiled admission cannot evade an active roster.
pub(crate) fn ensure_identity_admission_open(
    tx: &Connection,
    workspace: WorkspaceId,
    session: AgentSessionId,
    profile: Option<AgentProfileId>,
) -> Result<(), OperationsError> {
    if let Some(profile) = profile {
        ensure_profile_admission_open(tx, workspace, profile)?;
    } else {
        ensure_unscoped_admission_open(tx, workspace)?;
    }
    ensure_session_admission_open(tx, workspace, session)
}

#[cfg(test)]
mod tests {
    use super::team_nodes::TeamNodeWitnessWire;
    use super::{PartyWire, RosterWire, pending_bindings};
    use crate::OperationsError;
    use forge_domain::WorkspaceId;
    use rusqlite::{Connection, params};
    use std::error::Error;
    use uuid::Uuid;

    // Query-contract fixture only: the lifecycle tests in leadership.rs exercise
    // the actual catalog, election, durability and admission transitions.
    fn pending_roster_fixture() -> rusqlite::Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE TABLE ops_registration_requests(request_id BLOB, workspace_id BLOB, session_id BLOB);
             CREATE TABLE ops_registration_profile_scopes(request_id BLOB, workspace_id BLOB, session_id BLOB, profile_id BLOB);
             CREATE TABLE ops_binding_activations(request_id BLOB);
             CREATE TABLE ops_logical_bindings(request_id BLOB, workspace_id BLOB, participant_id BLOB);
             CREATE TABLE ops_workspace_participation_mappings(workspace_id BLOB, participant_id BLOB, agent_profile_id BLOB, session_id BLOB, participation_id BLOB);
             CREATE TABLE ops_workspace_participation_session_rebinds(workspace_id BLOB, participation_id BLOB, participant_id BLOB, agent_profile_id BLOB, previous_session_id BLOB, new_session_id BLOB);
             CREATE TABLE ops_workspace_participation_replacement_registrations(workspace_id BLOB, participation_id BLOB, phase INTEGER);
             CREATE TABLE ops_current_effect_bindings(workspace_id BLOB, participant_id BLOB);",
        )?;
        Ok(db)
    }

    fn team_roster_wire(version: u8, team_nodes: Option<Vec<TeamNodeWitnessWire>>) -> RosterWire {
        let parties = [
            PartyWire {
                participation: [1; 16],
                profile: [2; 16],
                source_participant: [3; 16],
                revision: 1,
            },
            PartyWire {
                participation: [4; 16],
                profile: [5; 16],
                source_participant: [6; 16],
                revision: 1,
            },
        ];
        RosterWire {
            version,
            cut: [7; 16],
            workspace: [8; 16],
            scope_kind: 2,
            activity: None,
            team: Some([9; 16]),
            election: [10; 16],
            leadership_revision: 1,
            prepared_at_ms: 0,
            parties,
            sessions: Vec::new(),
            bindings: Vec::new(),
            team_nodes,
        }
    }

    #[test]
    fn team_roster_wire_distinguishes_legacy_missing_from_complete_empty()
    -> Result<(), Box<dyn Error>> {
        let legacy = team_roster_wire(1, None);
        legacy.validate()?;
        let legacy_json = serde_json::to_value(legacy)?;
        assert!(legacy_json.get("team_nodes").is_none());

        let complete_empty = team_roster_wire(2, Some(Vec::new()));
        complete_empty.validate()?;
        let complete_empty_json = serde_json::to_value(complete_empty)?;
        assert_eq!(
            complete_empty_json.get("team_nodes"),
            Some(&serde_json::json!([]))
        );

        let missing_v2_witness = team_roster_wire(2, None);
        assert!(matches!(
            missing_v2_witness.validate(),
            Err(OperationsError::Corrupt("work roster envelope"))
        ));

        let first = TeamNodeWitnessWire {
            activity: [2; 16],
            goal: [1; 16],
            goal_created_at_ms: 1,
            dag_base_goal_revision: 1,
            dag_revision: 1,
            node: [1; 16],
            mapping_revision: 1,
        };
        let second = TeamNodeWitnessWire {
            activity: [1; 16],
            ..first
        };
        let noncanonical = team_roster_wire(2, Some(vec![first, second]));
        assert!(matches!(
            noncanonical.validate(),
            Err(OperationsError::Corrupt("work roster Team node ordering"))
        ));
        Ok(())
    }

    #[test]
    fn roster_pending_requires_profile_anchor_even_after_activation()
    -> Result<(), Box<dyn std::error::Error>> {
        let db = pending_roster_fixture()?;
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(1));
        let parties = [2_u128, 3].map(|id| PartyWire {
            participation: Uuid::from_u128(id + 10).into_bytes(),
            profile: Uuid::from_u128(id).into_bytes(),
            source_participant: Uuid::from_u128(id + 20).into_bytes(),
            revision: 1,
        });
        let request = Uuid::from_u128(30);
        let session = Uuid::from_u128(31);
        db.execute(
            "INSERT INTO ops_registration_requests VALUES(?1,?2,?3)",
            params![
                request.as_bytes(),
                workspace.into_uuid().as_bytes(),
                session.as_bytes()
            ],
        )?;
        db.execute(
            "INSERT INTO ops_registration_profile_scopes SELECT request_id,workspace_id,session_id,?1 FROM ops_registration_requests",
            params![parties[0].profile.as_slice()],
        )?;
        db.execute(
            "INSERT INTO ops_binding_activations VALUES(?1)",
            params![request.as_bytes()],
        )?;
        db.execute(
            "INSERT INTO ops_logical_bindings SELECT request_id,workspace_id,?1 FROM ops_registration_requests",
            params![parties[0].source_participant.as_slice()],
        )?;
        db.execute("INSERT INTO ops_current_effect_bindings SELECT workspace_id,participant_id FROM ops_logical_bindings", [])?;
        assert_eq!(pending_bindings(&db, workspace, &parties)?, (1, 0, 1));
        db.execute(
            "INSERT INTO ops_workspace_participation_mappings SELECT lb.workspace_id,lb.participant_id,p.profile_id,p.session_id,?1 FROM ops_logical_bindings lb JOIN ops_registration_profile_scopes p ON p.request_id=lb.request_id",
            params![parties[0].participation.as_slice()],
        )?;
        assert_eq!(pending_bindings(&db, workspace, &parties)?, (0, 0, 0));

        // A changed Session needs a durable rebind witness; the original mapping
        // alone cannot attest a newly activated request's Profile association.
        db.execute(
            "UPDATE ops_workspace_participation_mappings SET session_id=?1",
            params![Uuid::from_u128(32).as_bytes()],
        )?;
        assert_eq!(pending_bindings(&db, workspace, &parties)?, (1, 0, 0));
        db.execute(
            "INSERT INTO ops_workspace_participation_session_rebinds SELECT workspace_id,participation_id,participant_id,agent_profile_id,session_id,?1 FROM ops_workspace_participation_mappings",
            params![session.as_bytes()],
        )?;
        assert_eq!(pending_bindings(&db, workspace, &parties)?, (0, 0, 0));

        // Legacy unscoped live bindings are unknown, not implicitly unrelated.
        db.execute_batch("DELETE FROM ops_registration_profile_scopes; DELETE FROM ops_workspace_participation_mappings;")?;
        assert_eq!(pending_bindings(&db, workspace, &parties)?, (0, 0, 1));
        db.execute("DELETE FROM ops_current_effect_bindings", [])?;
        assert_eq!(pending_bindings(&db, workspace, &parties)?, (0, 0, 0));
        db.execute("DELETE FROM ops_binding_activations", [])?;
        assert_eq!(pending_bindings(&db, workspace, &parties)?, (1, 0, 0));
        Ok(())
    }
}
