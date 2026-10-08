//! Phase-D logical Session replacement for one existing Workspace participation.
//!
//! This transition preserves the logical participant and its original JOINED
//! identity while replacing only the Session bound to that participant. It is
//! deliberately fenced from current effects, presence, leadership, and open
//! election electorate state so runtime replacement cannot silently inherit
//! work or organizational authority before later cutover phases.

use forge_domain::{
    AgentSessionId, AgentSessionRevision, AgentSessionState, AuthenticatedPrincipal,
    RegisteredAgentSession, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileLogicalSessionLink, AgentProfileRevision,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{OperationsCatalog, OperationsError, WorkspaceParticipationId, wire};

const SESSION_REBINDS_SQL: &str = "CREATE TABLE ops_workspace_participation_session_rebinds (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), agent_profile_id BLOB NOT NULL CHECK(length(agent_profile_id)=16 AND agent_profile_id != zeroblob(16)), profile_revision INTEGER NOT NULL CHECK(profile_revision>0), profile_mapping_request_id BLOB NOT NULL CHECK(length(profile_mapping_request_id)=16 AND profile_mapping_request_id != zeroblob(16)), previous_session_id BLOB NOT NULL CHECK(length(previous_session_id)=16 AND previous_session_id != zeroblob(16)), new_session_id BLOB NOT NULL CHECK(length(new_session_id)=16 AND new_session_id != zeroblob(16) AND new_session_id != previous_session_id), new_session_revision INTEGER NOT NULL CHECK(new_session_revision>0), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), participant_membership_revision INTEGER NOT NULL CHECK(participant_membership_revision>0), expected_participation_revision INTEGER NOT NULL CHECK(expected_participation_revision>0), result_participation_revision INTEGER NOT NULL CHECK(result_participation_revision=expected_participation_revision+1), bound_at_ms INTEGER NOT NULL CHECK(bound_at_ms>=0), UNIQUE(workspace_id,new_session_id), UNIQUE(workspace_id,participation_id,result_participation_revision), FOREIGN KEY(workspace_id,agent_profile_id) REFERENCES ops_workspace_participations(workspace_id,agent_profile_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id))";

/// Exact operator/runtime intent for one logical Session replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceParticipationSessionRebindCommand {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    expected_previous_session_id: AgentSessionId,
    expected_participation_revision: u64,
    bound_at: TimestampMillis,
}

/// Exact current logical member snapshot for one `(Workspace, AgentProfile)`
/// participation. MCP registration uses this as a stale-fenced replacement
/// expectation; it is observation only and grants no authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceParticipationSessionExpectation {
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    profile_id: AgentProfileId,
    participant_id: Uuid,
    current_session_id: AgentSessionId,
    participant_membership_revision: u64,
    participation_revision: u64,
}

impl WorkspaceParticipationSessionExpectation {
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participation_id(self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(self) -> Uuid {
        self.participant_id
    }

    pub const fn current_session_id(self) -> AgentSessionId {
        self.current_session_id
    }

    #[must_use]
    pub const fn participant_membership_revision(self) -> u64 {
        self.participant_membership_revision
    }

    #[must_use]
    pub const fn participation_revision(self) -> u64 {
        self.participation_revision
    }
}

impl WorkspaceParticipationSessionRebindCommand {
    /// Construct one exact replacement intent.
    ///
    /// # Errors
    /// Rejects nil request identities and zero participation revisions.
    pub fn new(
        request_id: Uuid,
        workspace_id: WorkspaceId,
        expected_previous_session_id: AgentSessionId,
        expected_participation_revision: u64,
        bound_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil() || expected_participation_revision == 0 {
            return Err(OperationsError::Conflict("Session replacement input"));
        }
        Ok(Self {
            request_id,
            workspace_id,
            expected_previous_session_id,
            expected_participation_revision,
            bound_at,
        })
    }

    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn expected_previous_session_id(self) -> AgentSessionId {
        self.expected_previous_session_id
    }

    #[must_use]
    pub const fn expected_participation_revision(self) -> u64 {
        self.expected_participation_revision
    }

    #[must_use]
    pub const fn bound_at(self) -> TimestampMillis {
        self.bound_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceParticipationSessionRebindReceipt {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    profile_mapping_request_id: Uuid,
    previous_session_id: AgentSessionId,
    new_session_id: AgentSessionId,
    new_session_revision: AgentSessionRevision,
    participant_id: Uuid,
    participant_membership_revision: u64,
    expected_participation_revision: u64,
    result_participation_revision: u64,
    bound_at: TimestampMillis,
}

impl WorkspaceParticipationSessionRebindReceipt {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participation_id(self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(self) -> AgentProfileRevision {
        self.profile_revision
    }

    #[must_use]
    pub const fn profile_mapping_request_id(self) -> Uuid {
        self.profile_mapping_request_id
    }

    pub const fn previous_session_id(self) -> AgentSessionId {
        self.previous_session_id
    }

    pub const fn new_session_id(self) -> AgentSessionId {
        self.new_session_id
    }

    pub const fn new_session_revision(self) -> AgentSessionRevision {
        self.new_session_revision
    }

    #[must_use]
    pub const fn participant_id(self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn participant_membership_revision(self) -> u64 {
        self.participant_membership_revision
    }

    #[must_use]
    pub const fn expected_participation_revision(self) -> u64 {
        self.expected_participation_revision
    }

    #[must_use]
    pub const fn result_participation_revision(self) -> u64 {
        self.result_participation_revision
    }

    #[must_use]
    pub const fn bound_at(self) -> TimestampMillis {
        self.bound_at
    }
}

#[derive(Debug)]
struct RawMember {
    session_id: Vec<u8>,
    principal_kind: i64,
    principal_id: Option<Vec<u8>>,
    membership_revision: i64,
}

#[derive(Debug)]
struct RawRebind {
    request_id: Vec<u8>,
    workspace_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_revision: i64,
    profile_mapping_request_id: Vec<u8>,
    previous_session_id: Vec<u8>,
    new_session_id: Vec<u8>,
    new_session_revision: i64,
    participant_id: Vec<u8>,
    participant_membership_revision: i64,
    expected_participation_revision: i64,
    result_participation_revision: i64,
    bound_at_ms: i64,
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_workspace_participation_session_rebinds",
        SESSION_REBINDS_SQL,
    )]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(SESSION_REBINDS_SQL)?;
    Ok(())
}

impl OperationsCatalog {
    /// Read the current canonical logical member for one existing Profile and
    /// Workspace participation. Ambiguous historical participants fail closed.
    ///
    /// # Errors
    /// Returns corrupt/ambiguous mapping, member, or storage failures.
    pub fn workspace_participation_session_expectation(
        &self,
        workspace_id: WorkspaceId,
        profile_id: AgentProfileId,
    ) -> Result<Option<WorkspaceParticipationSessionExpectation>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let Some(participation) =
            crate::workspace_participation::load_participation(&tx, workspace_id, profile_id)?
        else {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(None);
        };
        let participant_id = unique_participant(&participation)?;
        let member = load_member(&tx, workspace_id, participant_id)?
            .ok_or(OperationsError::Corrupt("Workspace participant missing"))?;
        let current_session_id =
            AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(member.session_id.clone())?));
        let participant_membership_revision = u64::try_from(member.membership_revision)
            .map_err(|_| OperationsError::Corrupt("logical member revision"))?;
        if participant_membership_revision == 0 {
            return Err(OperationsError::Corrupt("logical member revision"));
        }
        let result = WorkspaceParticipationSessionExpectation {
            workspace_id,
            participation_id: participation.participation_id(),
            profile_id,
            participant_id,
            current_session_id,
            participant_membership_revision,
            participation_revision: participation.revision(),
        };
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(Some(result))
    }

    /// Replace the logical Session attached to one existing Profile/Workspace
    /// participation while preserving the participant and JOINED identity.
    ///
    /// The new Session must be an exact verified open Session for the same
    /// Profile. The participant must be inactive: no live presence, current
    /// Forge effect, pending registration, current leadership role, or open election electorate
    /// membership may exist. Those fences prevent implicit authority transfer.
    ///
    /// # Errors
    /// Rejects stale or ambiguous participation identity, Session/principal
    /// mismatch, active authority, competing Session use, replay substitution,
    /// or an unconfirmed durable commit.
    pub fn rebind_workspace_participation_session(
        &mut self,
        command: WorkspaceParticipationSessionRebindCommand,
        profile_link: AgentProfileLogicalSessionLink,
        verified_new_session: &RegisteredAgentSession,
    ) -> Result<WorkspaceParticipationSessionRebindReceipt, OperationsError> {
        validate_rebind_input(command, profile_link, verified_new_session)?;
        let destination = self.destination;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_rebind(&tx, command.request_id())? {
            validate_replay(existing, command, profile_link, verified_new_session)?;
            commit_rebind(tx, destination, command.request_id())?;
            return Ok(existing);
        }

        crate::leadership::work_roster::ensure_profile_admission_open(
            &tx,
            command.workspace_id(),
            profile_link.profile_id(),
        )?;
        crate::leadership::work_roster::ensure_session_admission_open(
            &tx,
            command.workspace_id(),
            command.expected_previous_session_id(),
        )?;
        crate::leadership::work_roster::ensure_session_admission_open(
            &tx,
            command.workspace_id(),
            verified_new_session.id(),
        )?;

        let participation = crate::workspace_participation::load_participation(
            &tx,
            command.workspace_id(),
            profile_link.profile_id(),
        )?
        .ok_or(OperationsError::Conflict(
            "workspace participation missing for Session replacement",
        ))?;
        if participation.revision() != command.expected_participation_revision() {
            return Err(OperationsError::Conflict(
                "workspace participation revision",
            ));
        }
        let participant_id = unique_participant(&participation)?;
        let member = load_member(&tx, command.workspace_id(), participant_id)?
            .ok_or(OperationsError::Corrupt("Workspace participant missing"))?;
        validate_member(
            &member,
            command.expected_previous_session_id(),
            verified_new_session.principal(),
            &participation,
        )?;
        ensure_new_session_unused(
            &tx,
            command.workspace_id(),
            verified_new_session.id(),
            participant_id,
        )?;
        ensure_participant_inactive(&tx, command.workspace_id(), participant_id)?;

        let result_revision = command
            .expected_participation_revision()
            .checked_add(1)
            .ok_or(OperationsError::Bounds("workspace participation revision"))?;
        update_member_session(
            &tx,
            command.workspace_id(),
            participant_id,
            command.expected_previous_session_id(),
            verified_new_session.id(),
            member.membership_revision,
        )?;
        update_participation_revision(
            &tx,
            command.workspace_id(),
            profile_link.profile_id(),
            command.expected_participation_revision(),
            result_revision,
        )?;
        let receipt = WorkspaceParticipationSessionRebindReceipt {
            request_id: command.request_id(),
            workspace_id: command.workspace_id(),
            participation_id: participation.participation_id(),
            profile_id: profile_link.profile_id(),
            profile_revision: profile_link.profile_revision(),
            profile_mapping_request_id: profile_link.request_id(),
            previous_session_id: command.expected_previous_session_id(),
            new_session_id: verified_new_session.id(),
            new_session_revision: verified_new_session.revision(),
            participant_id,
            participant_membership_revision: u64::try_from(member.membership_revision)
                .map_err(|_| OperationsError::Corrupt("logical member revision"))?,
            expected_participation_revision: command.expected_participation_revision(),
            result_participation_revision: result_revision,
            bound_at: command.bound_at(),
        };
        insert_rebind(&tx, receipt)?;
        let stored = load_rebind(&tx, command.request_id())?
            .ok_or(OperationsError::Corrupt("Session replacement readback"))?;
        if stored != receipt {
            return Err(OperationsError::Corrupt("Session replacement readback"));
        }
        commit_rebind(tx, destination, command.request_id())?;
        Ok(stored)
    }

    /// Read one durable Session replacement receipt by request identity.
    ///
    /// # Errors
    /// Returns storage or corrupt receipt failures.
    pub fn workspace_participation_session_rebind(
        &self,
        request_id: Uuid,
    ) -> Result<Option<WorkspaceParticipationSessionRebindReceipt>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let result = load_rebind(&tx, request_id)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(result)
    }

    /// Read the replacement receipt that bound one exact Session in a Workspace.
    ///
    /// # Errors
    /// Returns storage or corrupt receipt failures.
    pub fn workspace_participation_session_rebind_for_session(
        &self,
        workspace_id: WorkspaceId,
        session_id: AgentSessionId,
    ) -> Result<Option<WorkspaceParticipationSessionRebindReceipt>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let result = load_rebind_by_session(&tx, workspace_id, session_id)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(result)
    }
}

fn validate_rebind_input(
    command: WorkspaceParticipationSessionRebindCommand,
    profile_link: AgentProfileLogicalSessionLink,
    verified_new_session: &RegisteredAgentSession,
) -> Result<(), OperationsError> {
    if command.expected_previous_session_id() == verified_new_session.id() {
        return Err(OperationsError::Conflict("Session replacement identity"));
    }
    if profile_link.session_id() != verified_new_session.id()
        || profile_link.session_revision() != verified_new_session.revision()
        || verified_new_session.state() != AgentSessionState::Open
    {
        return Err(OperationsError::Conflict(
            "Session replacement verified Session",
        ));
    }
    if command.bound_at().get() < verified_new_session.updated_at().get()
        || command.bound_at().get() < profile_link.linked_at().get()
    {
        return Err(OperationsError::Conflict("Session replacement timestamp"));
    }
    Ok(())
}

fn unique_participant(
    participation: &crate::WorkspaceParticipationSnapshot,
) -> Result<Uuid, OperationsError> {
    let mut participant = None;
    for mapping in participation.session_mappings() {
        match participant {
            Some(existing) if existing != mapping.participant_id() => {
                return Err(OperationsError::Conflict(
                    "workspace participation participant ambiguity",
                ));
            }
            Some(_) => {}
            None => participant = Some(mapping.participant_id()),
        }
    }
    participant.ok_or(OperationsError::Corrupt(
        "workspace participation has no source mapping",
    ))
}

fn load_member(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<Option<RawMember>, OperationsError> {
    connection
        .query_row(
            "SELECT substr(session_id,1,17),principal_kind,substr(principal_id,1,17),membership_revision FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            |row| {
                Ok(RawMember {
                    session_id: row.get(0)?,
                    principal_kind: row.get(1)?,
                    principal_id: row.get(2)?,
                    membership_revision: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(OperationsError::from)
}

fn validate_member(
    member: &RawMember,
    expected_previous_session_id: AgentSessionId,
    new_principal: AuthenticatedPrincipal,
    participation: &crate::WorkspaceParticipationSnapshot,
) -> Result<(), OperationsError> {
    let current_session =
        AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(member.session_id.clone())?));
    if current_session != expected_previous_session_id {
        return Err(OperationsError::Conflict(
            "Session replacement previous Session",
        ));
    }
    if member.membership_revision <= 0 {
        return Err(OperationsError::Corrupt("logical member revision"));
    }
    let member_revision = u64::try_from(member.membership_revision)
        .map_err(|_| OperationsError::Corrupt("logical member revision"))?;
    if participation
        .session_mappings()
        .iter()
        .any(|mapping| mapping.participant_membership_revision() != member_revision)
    {
        return Err(OperationsError::Conflict(
            "Session replacement membership revision",
        ));
    }
    let principal = crate::registration::decode_principal_fields(
        member.principal_kind,
        member.principal_id.as_deref(),
    )?;
    if principal != new_principal {
        return Err(OperationsError::Conflict(
            "Session replacement authenticated principal",
        ));
    }
    Ok(())
}

fn ensure_new_session_unused(
    connection: &Connection,
    workspace_id: WorkspaceId,
    new_session_id: AgentSessionId,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    let existing: Option<Vec<u8>> = connection
        .query_row(
            "SELECT substr(participant_id,1,17) FROM ops_logical_members WHERE workspace_id=?1 AND session_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), new_session_id.into_uuid().as_bytes()],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(value) = existing {
        let existing_participant = Uuid::from_bytes(wire::id(value)?);
        if existing_participant != participant_id {
            return Err(OperationsError::Conflict(
                "Session replacement Session already in use",
            ));
        }
    }
    let pending: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_pending_participants WHERE workspace_id=?1 AND session_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), new_session_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if pending {
        return Err(OperationsError::Conflict(
            "Session replacement Session registration pending",
        ));
    }
    let migration_mapped: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND session_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), new_session_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if migration_mapped {
        return Err(OperationsError::Conflict(
            "Session replacement Session already mapped",
        ));
    }
    Ok(())
}

fn ensure_participant_inactive(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    // A prepared/reserved/committed registration can still activate in Forge.
    // Serialize replacement against that durable intent, not just its current
    // effect projection, so a late activation cannot cross the Session cut.
    let pending_registration: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_registration_requests r WHERE r.workspace_id=?1 AND r.participant_id=?2 AND NOT EXISTS(SELECT 1 FROM ops_binding_activations a WHERE a.request_id=r.request_id))",
        params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
        |row| row.get(0),
    )?;
    if pending_registration {
        return Err(OperationsError::Conflict(
            "Session replacement registration still pending",
        ));
    }
    let presence: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_logical_presence WHERE workspace_id=?1 AND participant_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
        |row| row.get(0),
    )?;
    if presence {
        return Err(OperationsError::Conflict(
            "Session replacement participant still present",
        ));
    }
    let effect: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
        |row| row.get(0),
    )?;
    if effect {
        return Err(OperationsError::Conflict(
            "Session replacement participant still has current effect",
        ));
    }
    let leader: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership WHERE workspace_id=?1 AND leader_participant_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
        |row| row.get(0),
    )?;
    if leader {
        return Err(OperationsError::Conflict(
            "Session replacement participant still leads",
        ));
    }
    let open_electorate = crate::election_subject::participant_in_open_electorate(
        connection,
        workspace_id,
        participant_id,
    )?;
    if open_electorate {
        return Err(OperationsError::Conflict(
            "Session replacement participant is in open electorate",
        ));
    }
    Ok(())
}

fn update_member_session(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    previous_session_id: AgentSessionId,
    new_session_id: AgentSessionId,
    membership_revision: i64,
) -> Result<(), OperationsError> {
    let changed = transaction.execute(
        "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3 AND session_id=?4 AND membership_revision=?5",
        params![
            new_session_id.into_uuid().as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            participant_id.as_bytes(),
            previous_session_id.into_uuid().as_bytes(),
            membership_revision,
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("Session replacement member CAS"));
    }
    Ok(())
}

fn update_participation_revision(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
    expected_revision: u64,
    result_revision: u64,
) -> Result<(), OperationsError> {
    let changed = transaction.execute(
        "UPDATE ops_workspace_participations SET revision=?1 WHERE workspace_id=?2 AND agent_profile_id=?3 AND revision=?4",
        params![
            crate::catalog::signed(result_revision)?,
            workspace_id.into_uuid().as_bytes(),
            profile_id.into_uuid().as_bytes(),
            crate::catalog::signed(expected_revision)?,
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict(
            "Session replacement participation CAS",
        ));
    }
    Ok(())
}

fn insert_rebind(
    transaction: &Transaction<'_>,
    receipt: WorkspaceParticipationSessionRebindReceipt,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_workspace_participation_session_rebinds(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,previous_session_id,new_session_id,new_session_revision,participant_id,participant_membership_revision,expected_participation_revision,result_participation_revision,bound_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            receipt.request_id.as_bytes(),
            receipt.workspace_id.into_uuid().as_bytes(),
            receipt.participation_id.into_uuid().as_bytes(),
            receipt.profile_id.into_uuid().as_bytes(),
            receipt.profile_revision.get(),
            receipt.profile_mapping_request_id.as_bytes(),
            receipt.previous_session_id.into_uuid().as_bytes(),
            receipt.new_session_id.into_uuid().as_bytes(),
            receipt.new_session_revision.get(),
            receipt.participant_id.as_bytes(),
            crate::catalog::signed(receipt.participant_membership_revision)?,
            crate::catalog::signed(receipt.expected_participation_revision)?,
            crate::catalog::signed(receipt.result_participation_revision)?,
            receipt.bound_at.get(),
        ],
    )?;
    Ok(())
}

fn load_rebind(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<WorkspaceParticipationSessionRebindReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(workspace_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(previous_session_id,1,17),substr(new_session_id,1,17),new_session_revision,substr(participant_id,1,17),participant_membership_revision,expected_participation_revision,result_participation_revision,bound_at_ms FROM ops_workspace_participation_session_rebinds WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawRebind {
                    request_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    participation_id: row.get(2)?,
                    profile_id: row.get(3)?,
                    profile_revision: row.get(4)?,
                    profile_mapping_request_id: row.get(5)?,
                    previous_session_id: row.get(6)?,
                    new_session_id: row.get(7)?,
                    new_session_revision: row.get(8)?,
                    participant_id: row.get(9)?,
                    participant_membership_revision: row.get(10)?,
                    expected_participation_revision: row.get(11)?,
                    result_participation_revision: row.get(12)?,
                    bound_at_ms: row.get(13)?,
                })
            },
        )
        .optional()?;
    raw.as_ref().map(decode_rebind).transpose()
}

pub(crate) fn load_rebind_by_session(
    connection: &Connection,
    workspace_id: WorkspaceId,
    session_id: AgentSessionId,
) -> Result<Option<WorkspaceParticipationSessionRebindReceipt>, OperationsError> {
    let request_id: Option<Vec<u8>> = connection
        .query_row(
            "SELECT substr(request_id,1,17) FROM ops_workspace_participation_session_rebinds WHERE workspace_id=?1 AND new_session_id=?2 LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes()
            ],
            |row| row.get(0),
        )
        .optional()?;
    let Some(request_id) = request_id else {
        return Ok(None);
    };
    load_rebind(connection, Uuid::from_bytes(wire::id(request_id)?))
}

fn decode_rebind(
    raw: &RawRebind,
) -> Result<WorkspaceParticipationSessionRebindReceipt, OperationsError> {
    let expected = u64::try_from(raw.expected_participation_revision)
        .map_err(|_| OperationsError::Corrupt("Session replacement expected revision"))?;
    let result = u64::try_from(raw.result_participation_revision)
        .map_err(|_| OperationsError::Corrupt("Session replacement result revision"))?;
    let membership_revision = u64::try_from(raw.participant_membership_revision)
        .map_err(|_| OperationsError::Corrupt("Session replacement membership revision"))?;
    if result
        != expected.checked_add(1).ok_or(OperationsError::Corrupt(
            "Session replacement revision overflow",
        ))?
    {
        return Err(OperationsError::Corrupt("Session replacement revision"));
    }
    Ok(WorkspaceParticipationSessionRebindReceipt {
        request_id: Uuid::from_bytes(wire::id(raw.request_id.clone())?),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.workspace_id.clone())?)),
        participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
            raw.participation_id.clone(),
        )?))?,
        profile_id: AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(raw.profile_id.clone())?))
            .map_err(|_| OperationsError::Corrupt("Session replacement Profile identity"))?,
        profile_revision: AgentProfileRevision::new(raw.profile_revision)
            .map_err(|_| OperationsError::Corrupt("Session replacement Profile revision"))?,
        profile_mapping_request_id: Uuid::from_bytes(wire::id(
            raw.profile_mapping_request_id.clone(),
        )?),
        previous_session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
            raw.previous_session_id.clone(),
        )?)),
        new_session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
            raw.new_session_id.clone(),
        )?)),
        new_session_revision: AgentSessionRevision::new(raw.new_session_revision)
            .map_err(|_| OperationsError::Corrupt("Session replacement Session revision"))?,
        participant_id: Uuid::from_bytes(wire::id(raw.participant_id.clone())?),
        participant_membership_revision: membership_revision,
        expected_participation_revision: expected,
        result_participation_revision: result,
        bound_at: TimestampMillis::new(raw.bound_at_ms)
            .map_err(|_| OperationsError::Corrupt("Session replacement timestamp"))?,
    })
}

fn validate_replay(
    stored: WorkspaceParticipationSessionRebindReceipt,
    command: WorkspaceParticipationSessionRebindCommand,
    profile_link: AgentProfileLogicalSessionLink,
    verified_new_session: &RegisteredAgentSession,
) -> Result<(), OperationsError> {
    if stored.workspace_id != command.workspace_id()
        || stored.profile_id != profile_link.profile_id()
        || stored.profile_revision != profile_link.profile_revision()
        || stored.profile_mapping_request_id != profile_link.request_id()
        || stored.previous_session_id != command.expected_previous_session_id()
        || stored.new_session_id != verified_new_session.id()
        || stored.new_session_revision != verified_new_session.revision()
        || stored.expected_participation_revision != command.expected_participation_revision()
        || stored.bound_at != command.bound_at()
    {
        return Err(OperationsError::Conflict("Session replacement replay"));
    }
    Ok(())
}

fn commit_rebind(
    transaction: Transaction<'_>,
    destination_id: Uuid,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::SessionRebindCommitUnconfirmed {
            destination_id,
            request_id,
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{AgentSessionOpenRequestId, OpenAgentSession};
    use heterarchy_orchestration_sessions::{
        AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileProviderFamily,
        AgentSessionCatalog, CreateAgentProfile, link_existing_logical_session,
    };
    use std::error::Error;

    struct RebindFixture {
        root: std::path::PathBuf,
        profiles: AgentProfileCatalog,
        operations: OperationsCatalog,
        profile_id: AgentProfileId,
        workspace_id: WorkspaceId,
        participant_id: Uuid,
        participation_id: WorkspaceParticipationId,
        old_session: RegisteredAgentSession,
        new_session: RegisteredAgentSession,
        new_link: AgentProfileLogicalSessionLink,
    }

    struct ProfileSessionFixture {
        profiles: AgentProfileCatalog,
        profile_id: AgentProfileId,
        old_session: RegisteredAgentSession,
        new_session: RegisteredAgentSession,
        old_link: AgentProfileLogicalSessionLink,
        new_link: AgentProfileLogicalSessionLink,
    }

    fn create_profile_sessions(
        root: &std::path::Path,
    ) -> Result<ProfileSessionFixture, Box<dyn Error>> {
        let mut profiles = AgentProfileCatalog::open(root.join("agent-profiles.sqlite3"))?;
        let mut sessions = AgentSessionCatalog::open(root.join("agent-sessions.sqlite3"))?;
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xD001))?;
        let profile = CreateAgentProfile::new(
            profile_id,
            Uuid::from_u128(0xD002),
            "luna-phase-d".to_owned(),
            "Luna Phase D".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            Some("gpt-5.6-luna".to_owned()),
            Some("max".to_owned()),
            vec!["rust".to_owned()],
            AgentProfileCreationOrigin::UserManual,
            TimestampMillis::new(10)?,
        )?;
        let _created = profiles.create_profile(&profile)?;
        let principal = AuthenticatedPrincipal::TrustedStdio;
        let old_session = sessions
            .open_session(&OpenAgentSession::new(
                AgentSessionId::from_uuid(Uuid::from_u128(0xD010)),
                AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xD011)),
                principal,
                TimestampMillis::new(11)?,
            ))?
            .session()
            .clone();
        let new_session = sessions
            .open_session(&OpenAgentSession::new(
                AgentSessionId::from_uuid(Uuid::from_u128(0xD012)),
                AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xD013)),
                principal,
                TimestampMillis::new(12)?,
            ))?
            .session()
            .clone();
        let old_link = link_existing_logical_session(
            &mut profiles,
            &sessions,
            profile_id,
            old_session.id(),
            principal,
            Uuid::from_u128(0xD014),
            TimestampMillis::new(13)?,
        )?
        .link();
        let new_link = link_existing_logical_session(
            &mut profiles,
            &sessions,
            profile_id,
            new_session.id(),
            principal,
            Uuid::from_u128(0xD015),
            TimestampMillis::new(14)?,
        )?
        .link();
        Ok(ProfileSessionFixture {
            profiles,
            profile_id,
            old_session,
            new_session,
            old_link,
            new_link,
        })
    }

    fn create_workspace_participation(
        root: &std::path::Path,
        profile_id: AgentProfileId,
        old_session: &RegisteredAgentSession,
        old_link: AgentProfileLogicalSessionLink,
    ) -> Result<
        (
            OperationsCatalog,
            WorkspaceId,
            Uuid,
            WorkspaceParticipationId,
        ),
        Box<dyn Error>,
    > {
        let mut operations =
            OperationsCatalog::open(&root.join("operations.sqlite3"), Uuid::from_u128(0xD020))?;
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0xD021));
        let participant_id = Uuid::from_u128(0xD022);
        operations.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'phase-d-member','worker',1,1,15)",
            params![
                workspace_id.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                old_session.id().into_uuid().as_bytes(),
            ],
        )?;
        let activity_id = forge_domain::ActivityId::from_uuid(Uuid::from_u128(0xD023));
        let joined_event_id = crate::membership::joined_event_id(workspace_id, participant_id);
        let joined_tx = operations.connection.unchecked_transaction()?;
        let joined_existing = crate::membership::ensure_joined_event(
            &joined_tx,
            workspace_id,
            participant_id,
            activity_id,
            joined_event_id,
            TimestampMillis::new(16)?,
        )?;
        assert!(!joined_existing);
        joined_tx.commit()?;
        let logical_identity = operations
            .logical_identity_for_session(workspace_id, old_session.id())?
            .ok_or("old Session logical identity missing")?;
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xD024))?;
        let mapped = operations.map_workspace_participation(
            Uuid::from_u128(0xD025),
            participation_id,
            old_link,
            &logical_identity,
            1,
            TimestampMillis::new(17)?,
        )?;
        assert_eq!(mapped.participation().profile_id(), profile_id);
        Ok((operations, workspace_id, participant_id, participation_id))
    }

    fn fixture() -> Result<RebindFixture, Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("orchestration-session-rebind-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let profile_sessions = create_profile_sessions(&root)?;
        let (operations, workspace_id, participant_id, participation_id) =
            create_workspace_participation(
                &root,
                profile_sessions.profile_id,
                &profile_sessions.old_session,
                profile_sessions.old_link,
            )?;
        Ok(RebindFixture {
            root,
            profiles: profile_sessions.profiles,
            operations,
            profile_id: profile_sessions.profile_id,
            workspace_id,
            participant_id,
            participation_id,
            old_session: profile_sessions.old_session,
            new_session: profile_sessions.new_session,
            new_link: profile_sessions.new_link,
        })
    }

    fn rebind_command(
        fixture: &RebindFixture,
        request_id: Uuid,
        bound_at: i64,
    ) -> Result<WorkspaceParticipationSessionRebindCommand, OperationsError> {
        WorkspaceParticipationSessionRebindCommand::new(
            request_id,
            fixture.workspace_id,
            fixture.old_session.id(),
            1,
            TimestampMillis::new(bound_at)
                .map_err(|_| OperationsError::Conflict("test replacement timestamp"))?,
        )
    }

    fn assert_rebind_storage(
        fixture: &mut RebindFixture,
        receipt: WorkspaceParticipationSessionRebindReceipt,
    ) -> Result<(), Box<dyn Error>> {
        assert_eq!(receipt.participant_id(), fixture.participant_id);
        assert_eq!(receipt.participation_id(), fixture.participation_id);
        assert_eq!(receipt.participant_membership_revision(), 1);
        assert_eq!(receipt.result_participation_revision(), 2);
        assert!(
            fixture
                .operations
                .logical_identity_for_session(fixture.workspace_id, fixture.old_session.id())?
                .is_none()
        );
        let current = fixture
            .operations
            .logical_identity_for_session(fixture.workspace_id, fixture.new_session.id())?
            .ok_or("replacement Session logical identity missing")?;
        assert_eq!(current.participant_id(), fixture.participant_id);
        assert!(current.effect_bindings().is_empty());
        let counts: (i64, i64, i64, i64, i64) = fixture.operations.connection.query_row(
            "SELECT (SELECT COUNT(*) FROM ops_logical_members WHERE workspace_id=?1),(SELECT COUNT(*) FROM ops_logical_events WHERE workspace_id=?1 AND event_kind=0),(SELECT COUNT(*) FROM ops_current_effect_bindings WHERE workspace_id=?1),(SELECT COUNT(*) FROM ops_leadership WHERE workspace_id=?1),(SELECT COUNT(*) FROM ops_work_credit_entries WHERE workspace_id=?1)",
            params![fixture.workspace_id.into_uuid().as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )?;
        assert_eq!(counts, (1, 1, 0, 0, 0));
        let origin_session: Vec<u8> = fixture.operations.connection.query_row(
            "SELECT session_id FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participation_id=?2",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participation_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        assert_eq!(
            Uuid::from_slice(&origin_session)?,
            fixture.old_session.id().into_uuid()
        );
        assert_eq!(
            fixture
                .operations
                .workspace_participation(fixture.workspace_id, fixture.profile_id)?
                .ok_or("participation missing after replacement")?
                .revision(),
            2
        );
        Ok(())
    }

    fn assert_rebind_read_view(
        fixture: &RebindFixture,
        request_id: Uuid,
    ) -> Result<(), Box<dyn Error>> {
        let view = fixture
            .operations
            .agent_profile_workspace_read_view(
                &fixture.profiles,
                fixture.profile_id,
                fixture.workspace_id,
                std::slice::from_ref(&fixture.new_session),
            )?
            .ok_or("Profile view missing after Session replacement")?;
        let old_view = view
            .sessions()
            .iter()
            .find(|session| session.session_id() == fixture.old_session.id())
            .ok_or("old Session view missing")?;
        let new_view = view
            .sessions()
            .iter()
            .find(|session| session.session_id() == fixture.new_session.id())
            .ok_or("new Session view missing")?;
        assert_eq!(
            old_view.participation_binding_kind(),
            Some(crate::AgentProfileWorkspaceSessionBindingKind::InitialMapping)
        );
        assert!(old_view.current_identity().is_none());
        assert_eq!(
            new_view.participation_binding_kind(),
            Some(crate::AgentProfileWorkspaceSessionBindingKind::SessionRebind)
        );
        assert_eq!(
            new_view.participation_mapping_request_id(),
            Some(request_id)
        );
        assert_eq!(
            new_view.mapped_participant_id(),
            Some(fixture.participant_id)
        );
        assert_eq!(new_view.mapped_membership_revision(), Some(1));
        assert_eq!(
            new_view
                .current_identity()
                .map(crate::AgentProfileWorkspaceCurrentIdentityView::participant_id),
            Some(fixture.participant_id)
        );
        Ok(())
    }

    #[test]
    fn replacement_preserves_one_member_and_one_joined_identity_and_replays_exactly()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let request_id = Uuid::from_u128(0xD030);
        let command = rebind_command(&fixture, request_id, 20)?;
        let receipt = fixture.operations.rebind_workspace_participation_session(
            command,
            fixture.new_link,
            &fixture.new_session,
        )?;
        assert_rebind_storage(&mut fixture, receipt)?;
        let current_activity = forge_domain::ActivityId::from_uuid(Uuid::from_u128(0xD031));
        let current_agent = forge_domain::AgentInstanceId::from_uuid(Uuid::from_u128(0xD032));
        fixture.operations.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participant_id.as_bytes(),
                current_activity.into_uuid().as_bytes(),
                fixture.new_session.id().into_uuid().as_bytes(),
                current_agent.into_uuid().as_bytes(),
                Uuid::from_u128(0xD033).as_bytes(),
            ],
        )?;
        let current_identity = fixture
            .operations
            .logical_identity_for_participant(fixture.workspace_id, fixture.participant_id)?
            .ok_or("current participant identity missing after Session replacement")?;
        assert_eq!(current_identity.session_id(), fixture.new_session.id());
        assert_eq!(current_identity.effect_bindings().len(), 1);
        assert_eq!(
            current_identity.effect_bindings()[0].activity_id(),
            current_activity
        );
        assert_eq!(
            current_identity.effect_bindings()[0].agent_id(),
            current_agent
        );
        assert_rebind_read_view(&fixture, request_id)?;
        let replay = fixture.operations.rebind_workspace_participation_session(
            command,
            fixture.new_link,
            &fixture.new_session,
        )?;
        assert_eq!(replay, receipt);
        assert_eq!(
            fixture
                .operations
                .workspace_participation(fixture.workspace_id, fixture.profile_id)?
                .ok_or("participation missing after replay")?
                .revision(),
            2
        );
        let root = fixture.root.clone();
        drop(fixture);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn assert_rebind_rejected_without_mutation(
        fixture: &mut RebindFixture,
        request_id: Uuid,
    ) -> Result<(), Box<dyn Error>> {
        let command = rebind_command(fixture, request_id, 30)?;
        let result = fixture.operations.rebind_workspace_participation_session(
            command,
            fixture.new_link,
            &fixture.new_session,
        );
        assert!(matches!(result, Err(OperationsError::Conflict(_))));
        assert!(
            fixture
                .operations
                .logical_identity_for_session(fixture.workspace_id, fixture.new_session.id())?
                .is_none()
        );
        assert_eq!(
            fixture
                .operations
                .workspace_participation(fixture.workspace_id, fixture.profile_id)?
                .ok_or("participation missing after rejection")?
                .revision(),
            1
        );
        let journal_count: i64 = fixture.operations.connection.query_row(
            "SELECT COUNT(*) FROM ops_workspace_participation_session_rebinds",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(journal_count, 0);
        Ok(())
    }

    fn insert_open_subject_election(fixture: &RebindFixture) -> Result<(), Box<dyn Error>> {
        let subject_election_id = Uuid::from_u128(0xD048);
        fixture.operations.connection.execute(
            "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,0,NULL,'RECALL','ELECTING',NULL,?3,200,NULL,NULL,NULL,NULL,28,NULL)",
            params![
                subject_election_id.as_bytes(),
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participant_id.as_bytes(),
            ],
        )?;
        fixture.operations.connection.execute(
            "INSERT INTO ops_election_subject_cutovers(election_id,workspace_id,scope_kind,activity_id,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) VALUES(?1,?2,0,NULL,1,2,1,1,1,?3,28)",
            params![
                subject_election_id.as_bytes(),
                fixture.workspace_id.into_uuid().as_bytes(),
                [7_u8; 32].as_slice(),
            ],
        )?;
        fixture.operations.connection.execute(
            "INSERT INTO ops_election_voter_subjects(election_id,subject_kind,subject_id,agent_profile_id,participation_revision,source_participant_count) VALUES(?1,0,?2,?3,1,1)",
            params![
                subject_election_id.as_bytes(),
                fixture.participation_id.into_uuid().as_bytes(),
                fixture.profile_id.into_uuid().as_bytes(),
            ],
        )?;
        fixture.operations.connection.execute(
            "INSERT INTO ops_election_voter_sources(election_id,participant_id,membership_revision,subject_kind,subject_id) VALUES(?1,?2,1,0,?3)",
            params![
                subject_election_id.as_bytes(),
                fixture.participant_id.as_bytes(),
                fixture.participation_id.into_uuid().as_bytes(),
            ],
        )?;
        Ok(())
    }

    #[test]
    fn replacement_fails_closed_while_effect_presence_leadership_or_election_is_live()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let activity = forge_domain::ActivityId::from_uuid(Uuid::from_u128(0xD040));
        let agent = forge_domain::AgentInstanceId::from_uuid(Uuid::from_u128(0xD041));
        fixture.operations.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participant_id.as_bytes(),
                activity.into_uuid().as_bytes(),
                fixture.old_session.id().into_uuid().as_bytes(),
                agent.into_uuid().as_bytes(),
                Uuid::from_u128(0xD042).as_bytes(),
            ],
        )?;
        assert_rebind_rejected_without_mutation(&mut fixture, Uuid::from_u128(0xD043))?;
        fixture.operations.connection.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participant_id.as_bytes()
            ],
        )?;

        fixture.operations.connection.execute(
            "INSERT INTO ops_logical_presence(workspace_id,participant_id,session_id,last_authenticated_at_ms,presence_revision) VALUES(?1,?2,?3,25,2)",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participant_id.as_bytes(),
                fixture.old_session.id().into_uuid().as_bytes(),
            ],
        )?;
        assert_rebind_rejected_without_mutation(&mut fixture, Uuid::from_u128(0xD044))?;
        fixture.operations.connection.execute(
            "DELETE FROM ops_logical_presence WHERE workspace_id=?1 AND participant_id=?2",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participant_id.as_bytes()
            ],
        )?;

        fixture.operations.connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,'ACTIVE',?2,1,NULL,NULL,26)",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participant_id.as_bytes()
            ],
        )?;
        assert_rebind_rejected_without_mutation(&mut fixture, Uuid::from_u128(0xD045))?;
        fixture.operations.connection.execute(
            "DELETE FROM ops_leadership WHERE workspace_id=?1",
            params![fixture.workspace_id.into_uuid().as_bytes()],
        )?;

        let election_id = Uuid::from_u128(0xD046);
        fixture.operations.connection.execute(
            "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,0,NULL,'RECALL','ELECTING',NULL,?3,100,NULL,NULL,NULL,NULL,27,NULL)",
            params![
                election_id.as_bytes(),
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participant_id.as_bytes(),
            ],
        )?;
        fixture.operations.connection.execute(
            "INSERT INTO ops_election_electorate(election_id,participant_id) VALUES(?1,?2)",
            params![election_id.as_bytes(), fixture.participant_id.as_bytes()],
        )?;
        assert_rebind_rejected_without_mutation(&mut fixture, Uuid::from_u128(0xD047))?;
        fixture.operations.connection.execute(
            "DELETE FROM ops_election_electorate WHERE election_id=?1",
            params![election_id.as_bytes()],
        )?;
        fixture.operations.connection.execute(
            "DELETE FROM ops_leadership_elections WHERE election_id=?1",
            params![election_id.as_bytes()],
        )?;

        insert_open_subject_election(&fixture)?;
        assert_rebind_rejected_without_mutation(&mut fixture, Uuid::from_u128(0xD049))?;
        let root = fixture.root.clone();
        drop(fixture);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn replacement_rejects_foreign_principal_and_ambiguous_legacy_participants()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let foreign = RegisteredAgentSession::open(
            fixture.new_session.id(),
            fixture.new_session.request_id(),
            AuthenticatedPrincipal::DirectToken(forge_domain::DirectTokenId::from_uuid(
                Uuid::from_u128(0xD050),
            )),
            fixture.new_session.created_at(),
        );
        let command = rebind_command(&fixture, Uuid::from_u128(0xD051), 30)?;
        assert!(matches!(
            fixture.operations.rebind_workspace_participation_session(
                command,
                fixture.new_link,
                &foreign,
            ),
            Err(OperationsError::Conflict(_))
        ));

        let second_participant = Uuid::from_u128(0xD052);
        let second_session = AgentSessionId::from_uuid(Uuid::from_u128(0xD053));
        fixture.operations.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'ambiguous-member','worker',1,1,31)",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                second_participant.as_bytes(),
                second_session.into_uuid().as_bytes(),
            ],
        )?;
        fixture.operations.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,32)",
            params![
                Uuid::from_u128(0xD054).as_bytes(),
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.participation_id.into_uuid().as_bytes(),
                fixture.profile_id.into_uuid().as_bytes(),
                Uuid::from_u128(0xD055).as_bytes(),
                second_session.into_uuid().as_bytes(),
                second_participant.as_bytes(),
            ],
        )?;
        let command = rebind_command(&fixture, Uuid::from_u128(0xD056), 33)?;
        assert!(matches!(
            fixture.operations.rebind_workspace_participation_session(
                command,
                fixture.new_link,
                &fixture.new_session,
            ),
            Err(OperationsError::Conflict(_))
        ));
        assert!(
            fixture
                .operations
                .logical_identity_for_session(fixture.workspace_id, fixture.new_session.id())?
                .is_none()
        );
        let root = fixture.root.clone();
        drop(fixture);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn table_sql(connection: &Connection, name: &str) -> Result<String, rusqlite::Error> {
        connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
            params![name],
            |row| row.get(0),
        )
    }

    #[test]
    fn v21_to_v22_adds_only_the_session_rebind_journal() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-session-rebind-v21-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xD060);
        let catalog = OperationsCatalog::open(&path, destination)?;
        let member_sql = table_sql(&catalog.connection, "ops_logical_members")?;
        let leadership_sql = table_sql(&catalog.connection, "ops_leadership")?;
        let work_credit_sql = table_sql(&catalog.connection, "ops_work_credit_entries")?;
        let participation_sql = table_sql(&catalog.connection, "ops_workspace_participations")?;
        drop(catalog);

        let connection = Connection::open(&path)?;
        crate::election_subject::drop_schema_for_legacy_fixture(&connection)?;
        connection.execute(
            "DROP TABLE ops_workspace_participation_replacement_registrations",
            [],
        )?;
        connection.execute("DROP TABLE ops_workspace_participation_session_rebinds", [])?;
        connection.pragma_update(None, "user_version", 21_i64)?;
        drop(connection);

        let migrated = OperationsCatalog::open(&path, destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        assert_eq!(
            table_sql(&migrated.connection, "ops_logical_members")?,
            member_sql
        );
        crate::catalog::assert_fixture_table_schema(
            &migrated.connection,
            "ops_leadership",
            &leadership_sql,
        )?;
        assert_eq!(
            table_sql(&migrated.connection, "ops_work_credit_entries")?,
            work_credit_sql
        );
        assert_eq!(
            table_sql(&migrated.connection, "ops_workspace_participations")?,
            participation_sql
        );
        assert!(
            !table_sql(
                &migrated.connection,
                "ops_workspace_participation_session_rebinds"
            )?
            .is_empty()
        );
        drop(migrated);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
