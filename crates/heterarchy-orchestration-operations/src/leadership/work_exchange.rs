//! Immutable obligations from Broker-fenced election confirmations.
//!
//! This journal records why work exchange is required. It never authorizes a
//! Forge effect or proves that work moved. `AwaitingInventory` requires an exact
//! election-linked historical/fenced work cut; a later live assignment list is
//! not that evidence. Legacy confirmations are not backfilled.

use super::{LeadershipConfirmIntent, LeadershipScope};
use crate::{OperationsCatalog, OperationsError, catalog};
use forge_domain::{ActivityId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use uuid::Uuid;

const TABLE: &str = "CREATE TABLE ops_leadership_work_exchanges (request_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_leadership_confirm_intents(request_id) CHECK(length(request_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1,2)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), team_id BLOB CHECK(team_id IS NULL OR length(team_id)=16), election_id BLOB NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16), incumbent_participant_id BLOB CHECK(incumbent_participant_id IS NULL OR length(incumbent_participant_id)=16), elected_participant_id BLOB NOT NULL CHECK(length(elected_participant_id)=16), incumbent_participation_id BLOB CHECK(incumbent_participation_id IS NULL OR length(incumbent_participation_id)=16), elected_participation_id BLOB CHECK(elected_participation_id IS NULL OR length(elected_participation_id)=16), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), fence_revision INTEGER NOT NULL CHECK(fence_revision>=1), confirmed_at_ms INTEGER NOT NULL CHECK(confirmed_at_ms>=0), reason INTEGER NOT NULL CHECK(reason BETWEEN 0 AND 5), CHECK((scope_kind=0 AND activity_id IS NULL AND team_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL AND team_id IS NULL) OR (scope_kind=2 AND activity_id IS NULL AND team_id IS NOT NULL)))";
const INDEX: &str = "CREATE INDEX ops_leadership_work_exchanges_pending ON ops_leadership_work_exchanges(workspace_id,request_id) WHERE reason IN(3,4,5)";
const SELECT: &str = "SELECT substr(request_id,1,17),substr(workspace_id,1,17),scope_kind,substr(activity_id,1,17),substr(team_id,1,17),substr(election_id,1,17),substr(incumbent_participant_id,1,17),substr(elected_participant_id,1,17),substr(incumbent_participation_id,1,17),substr(elected_participation_id,1,17),leadership_revision,fence_revision,confirmed_at_ms,reason FROM ops_leadership_work_exchanges";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_leadership_work_exchanges", TABLE),
        ("index", "ops_leadership_work_exchanges_pending", INDEX),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(TABLE)?;
    connection.execute_batch(INDEX)?;
    Ok(())
}

/// Initial obligation classification, not a work-transfer lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipWorkExchangeState {
    NoExchangeRequired,
    AwaitingInventory,
    AwaitingIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipWorkExchangeReason {
    Bootstrap,
    SameParticipant,
    SameParticipation,
    DistinctParticipation,
    MissingFrozenIdentity,
    ConflictingFrozenIdentity,
}

impl LeadershipWorkExchangeReason {
    const fn encode(self) -> i64 {
        match self {
            Self::Bootstrap => 0,
            Self::SameParticipant => 1,
            Self::SameParticipation => 2,
            Self::DistinctParticipation => 3,
            Self::MissingFrozenIdentity => 4,
            Self::ConflictingFrozenIdentity => 5,
        }
    }
    fn decode(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::Bootstrap),
            1 => Ok(Self::SameParticipant),
            2 => Ok(Self::SameParticipation),
            3 => Ok(Self::DistinctParticipation),
            4 => Ok(Self::MissingFrozenIdentity),
            5 => Ok(Self::ConflictingFrozenIdentity),
            _ => Err(OperationsError::Corrupt("work exchange reason")),
        }
    }
    const fn state(self) -> LeadershipWorkExchangeState {
        match self {
            Self::Bootstrap | Self::SameParticipant | Self::SameParticipation => {
                LeadershipWorkExchangeState::NoExchangeRequired
            }
            Self::DistinctParticipation => LeadershipWorkExchangeState::AwaitingInventory,
            Self::MissingFrozenIdentity | Self::ConflictingFrozenIdentity => {
                LeadershipWorkExchangeState::AwaitingIdentity
            }
        }
    }
}

/// Immutable evidence from one exact successful election confirmation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipWorkExchangeRecord {
    request_id: Uuid,
    scope: LeadershipScope,
    election_id: Uuid,
    incumbent_participant_id: Option<Uuid>,
    elected_participant_id: Uuid,
    incumbent_participation_id: Option<Uuid>,
    elected_participation_id: Option<Uuid>,
    leadership_revision: i64,
    fence_revision: i64,
    confirmed_at_ms: i64,
    reason: LeadershipWorkExchangeReason,
}

impl LeadershipWorkExchangeRecord {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    pub const fn scope(&self) -> LeadershipScope {
        self.scope
    }
    #[must_use]
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    #[must_use]
    pub const fn incumbent_participant_id(&self) -> Option<Uuid> {
        self.incumbent_participant_id
    }
    #[must_use]
    pub const fn elected_participant_id(&self) -> Uuid {
        self.elected_participant_id
    }
    #[must_use]
    pub const fn incumbent_participation_id(&self) -> Option<Uuid> {
        self.incumbent_participation_id
    }
    #[must_use]
    pub const fn elected_participation_id(&self) -> Option<Uuid> {
        self.elected_participation_id
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }
    #[must_use]
    pub const fn fence_revision(&self) -> i64 {
        self.fence_revision
    }
    #[must_use]
    pub const fn confirmed_at_ms(&self) -> i64 {
        self.confirmed_at_ms
    }
    #[must_use]
    pub const fn reason(&self) -> LeadershipWorkExchangeReason {
        self.reason
    }
    #[must_use]
    pub const fn state(&self) -> LeadershipWorkExchangeState {
        self.reason.state()
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipWorkExchangePage {
    records: Vec<LeadershipWorkExchangeRecord>,
    next_cursor: Option<Uuid>,
}
impl LeadershipWorkExchangePage {
    pub fn records(&self) -> &[LeadershipWorkExchangeRecord] {
        &self.records
    }
    #[must_use]
    pub const fn next_cursor(&self) -> Option<Uuid> {
        self.next_cursor
    }
}

impl OperationsCatalog {
    /// Read the same immutable request after an uncertain confirmation commit.
    /// Absence means no journal evidence, including pre-journal confirmations.
    ///
    /// # Errors
    /// Rejects foreign scopes, unverified Workspaces and malformed storage.
    pub fn leadership_work_exchange(
        &self,
        scope: LeadershipScope,
        request_id: Uuid,
    ) -> Result<Option<LeadershipWorkExchangeRecord>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        super::governance::ensure_verified_workspace(&tx, scope.workspace_id(), self.destination)?;
        let record = load(&tx, request_id)?;
        if record.as_ref().is_some_and(|record| record.scope != scope) {
            return Err(OperationsError::Conflict("work exchange scope"));
        }
        catalog::commit_read(tx, self.destination)?;
        Ok(record)
    }

    /// Read at most 32 unresolved obligations in one Workspace snapshot.
    /// Reconciliation starts a fresh scan after reaching the end; this cursor
    /// does not promise a cross-call snapshot or authorize work effects.
    ///
    /// # Errors
    /// Rejects invalid limits, foreign/missing cursors and malformed storage.
    pub fn leadership_work_exchange_pending_page(
        &self,
        workspace_id: WorkspaceId,
        after_request_id: Option<Uuid>,
        limit: u32,
    ) -> Result<LeadershipWorkExchangePage, OperationsError> {
        if !(1..=32).contains(&limit) {
            return Err(OperationsError::Bounds("work exchange page limit"));
        }
        let tx = self.connection.unchecked_transaction()?;
        super::governance::ensure_verified_workspace(&tx, workspace_id, self.destination)?;
        if let Some(cursor) = after_request_id {
            let previous =
                load(&tx, cursor)?.ok_or(OperationsError::Conflict("work exchange cursor"))?;
            if previous.scope.workspace_id() != workspace_id {
                return Err(OperationsError::Conflict("work exchange cursor scope"));
            }
        }
        let mut records = {
            let mut query = tx.prepare(&format!("{SELECT} WHERE workspace_id=?1 AND reason IN(3,4,5) AND (?2 IS NULL OR request_id>?2) ORDER BY request_id LIMIT ?3"))?;
            query
                .query_map(
                    params![
                        workspace_id.into_uuid().as_bytes(),
                        bytes(after_request_id),
                        i64::from(limit) + 1
                    ],
                    RawRecord::read,
                )?
                .map(|raw| {
                    raw.map_err(OperationsError::from)
                        .and_then(RawRecord::decode)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let more = records.len()
            > usize::try_from(limit).map_err(|_| OperationsError::Bounds("work exchange limit"))?;
        if more {
            let _ = records.pop();
        }
        let next_cursor = if more {
            records.last().map(|record| record.request_id)
        } else {
            None
        };
        catalog::commit_read(tx, self.destination)?;
        Ok(LeadershipWorkExchangePage {
            records,
            next_cursor,
        })
    }
}

pub(super) fn append_confirmed(
    tx: &Transaction<'_>,
    intent: &LeadershipConfirmIntent,
    leadership_revision: i64,
    confirmed_at_ms: i64,
) -> Result<(), OperationsError> {
    let scope = intent.scope;
    let incumbent: Option<Vec<u8>> = tx.query_row("SELECT substr(incumbent_participant_id,1,17) FROM ops_leadership_elections WHERE election_id=?1 AND workspace_id=?2 AND scope_kind=?3 AND activity_id IS ?4 AND team_id IS ?5 AND elected_participant_id=?6", params![intent.election_id.as_bytes(), scope.workspace_id().into_uuid().as_bytes(), scope.scope_kind(), bytes(scope.activity_id().map(ActivityId::into_uuid)), bytes(scope.team_id()), intent.elected_participant_id.as_bytes()], |row| row.get(0))?;
    let incumbent = optional_id(incumbent)?;
    let candidate = frozen_candidate(
        tx,
        intent.scope,
        intent.election_id,
        intent.elected_participant_id,
    )?;
    let (previous, conflicting) =
        frozen_incumbent(tx, intent.scope, intent.election_id, incumbent)?;
    let reason = classify(
        incumbent,
        intent.elected_participant_id,
        previous,
        candidate,
        conflicting,
    );
    tx.execute("INSERT INTO ops_leadership_work_exchanges(request_id,workspace_id,scope_kind,activity_id,team_id,election_id,incumbent_participant_id,elected_participant_id,incumbent_participation_id,elected_participation_id,leadership_revision,fence_revision,confirmed_at_ms,reason) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)", params![intent.request_id.as_bytes(),scope.workspace_id().into_uuid().as_bytes(),scope.scope_kind(),bytes(scope.activity_id().map(ActivityId::into_uuid)),bytes(scope.team_id()),intent.election_id.as_bytes(),bytes(incumbent),intent.elected_participant_id.as_bytes(),bytes(previous),bytes(candidate),leadership_revision,intent.requested_fence_revision,confirmed_at_ms,reason.encode()])?;
    Ok(())
}

pub(super) fn frozen_candidate(
    tx: &Connection,
    scope: LeadershipScope,
    election_id: Uuid,
    elected_participant_id: Uuid,
) -> Result<Option<Uuid>, OperationsError> {
    let value = tx.query_row("SELECT substr(subject_id,1,17) FROM ops_election_work_credit_snapshots WHERE election_id=?1 AND workspace_id=?2 AND candidate_participant_id=?3 AND subject_kind=0", params![election_id.as_bytes(), scope.workspace_id().into_uuid().as_bytes(), elected_participant_id.as_bytes()], |row| row.get::<_, Vec<u8>>(0)).optional()?;
    optional_id(value)
}

pub(super) fn frozen_incumbent(
    tx: &Connection,
    scope: LeadershipScope,
    election_id: Uuid,
    incumbent: Option<Uuid>,
) -> Result<(Option<Uuid>, bool), OperationsError> {
    let Some(incumbent) = incumbent else {
        return Ok((None, false));
    };
    let review: Option<Option<Vec<u8>>> = tx.query_row("SELECT substr(incumbent_participation_id,1,17) FROM ops_leadership_reviews WHERE election_id=?1 AND workspace_id=?2 AND scope_kind=?3 AND activity_id IS ?4 AND team_id IS ?5 AND incumbent_participant_id=?6", params![election_id.as_bytes(),scope.workspace_id().into_uuid().as_bytes(),scope.scope_kind(),bytes(scope.activity_id().map(ActivityId::into_uuid)),bytes(scope.team_id()),incumbent.as_bytes()], |row| row.get(0)).optional()?;
    let review = optional_id(review.flatten())?;
    let voter = tx.query_row("SELECT substr(subject_id,1,17) FROM ops_election_voter_sources WHERE election_id=?1 AND participant_id=?2 AND subject_kind=0", params![election_id.as_bytes(), incumbent.as_bytes()], |row| row.get::<_, Vec<u8>>(0)).optional()?;
    let voter = optional_id(voter)?;
    match (review, voter) {
        (Some(a), Some(b)) if a != b => Ok((None, true)),
        (a, b) => Ok((a.or(b), false)),
    }
}

pub(super) fn classify(
    incumbent: Option<Uuid>,
    elected: Uuid,
    previous: Option<Uuid>,
    candidate: Option<Uuid>,
    conflicting: bool,
) -> LeadershipWorkExchangeReason {
    if incumbent.is_none() {
        return LeadershipWorkExchangeReason::Bootstrap;
    }
    if incumbent == Some(elected) {
        return LeadershipWorkExchangeReason::SameParticipant;
    }
    if conflicting {
        return LeadershipWorkExchangeReason::ConflictingFrozenIdentity;
    }
    match (previous, candidate) {
        (Some(a), Some(b)) if a == b => LeadershipWorkExchangeReason::SameParticipation,
        (Some(_), Some(_)) => LeadershipWorkExchangeReason::DistinctParticipation,
        _ => LeadershipWorkExchangeReason::MissingFrozenIdentity,
    }
}

fn bytes(value: Option<Uuid>) -> Option<Vec<u8>> {
    value.map(|id| id.as_bytes().to_vec())
}
fn id(value: Vec<u8>) -> Result<Uuid, OperationsError> {
    Ok(Uuid::from_bytes(crate::wire::id(value)?))
}
fn optional_id(value: Option<Vec<u8>>) -> Result<Option<Uuid>, OperationsError> {
    value.map(id).transpose()
}

struct RawRecord {
    request: Vec<u8>,
    workspace: Vec<u8>,
    scope: i64,
    activity: Option<Vec<u8>>,
    team: Option<Vec<u8>>,
    election: Vec<u8>,
    incumbent: Option<Vec<u8>>,
    elected: Vec<u8>,
    previous: Option<Vec<u8>>,
    candidate: Option<Vec<u8>>,
    revision: i64,
    fence: i64,
    confirmed: i64,
    reason: i64,
}
impl RawRecord {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            request: row.get(0)?,
            workspace: row.get(1)?,
            scope: row.get(2)?,
            activity: row.get(3)?,
            team: row.get(4)?,
            election: row.get(5)?,
            incumbent: row.get(6)?,
            elected: row.get(7)?,
            previous: row.get(8)?,
            candidate: row.get(9)?,
            revision: row.get(10)?,
            fence: row.get(11)?,
            confirmed: row.get(12)?,
            reason: row.get(13)?,
        })
    }
    fn decode(self) -> Result<LeadershipWorkExchangeRecord, OperationsError> {
        let workspace = WorkspaceId::from_uuid(id(self.workspace)?);
        let scope = match (
            self.scope,
            optional_id(self.activity)?,
            optional_id(self.team)?,
        ) {
            (0, None, None) => LeadershipScope::workspace(workspace),
            (1, Some(activity), None) => {
                LeadershipScope::activity(workspace, ActivityId::from_uuid(activity))
            }
            (2, None, Some(team)) => LeadershipScope::team(workspace, team),
            _ => return Err(OperationsError::Corrupt("work exchange scope")),
        };
        if self.revision < 1 || self.fence < 1 || self.confirmed < 0 {
            return Err(OperationsError::Corrupt("work exchange confirmation"));
        }
        let record = LeadershipWorkExchangeRecord {
            request_id: id(self.request)?,
            scope,
            election_id: id(self.election)?,
            incumbent_participant_id: optional_id(self.incumbent)?,
            elected_participant_id: id(self.elected)?,
            incumbent_participation_id: optional_id(self.previous)?,
            elected_participation_id: optional_id(self.candidate)?,
            leadership_revision: self.revision,
            fence_revision: self.fence,
            confirmed_at_ms: self.confirmed,
            reason: LeadershipWorkExchangeReason::decode(self.reason)?,
        };
        let expected = classify(
            record.incumbent_participant_id,
            record.elected_participant_id,
            record.incumbent_participation_id,
            record.elected_participation_id,
            record.reason == LeadershipWorkExchangeReason::ConflictingFrozenIdentity,
        );
        if record.reason != expected {
            return Err(OperationsError::Corrupt(
                "work exchange identity classification",
            ));
        }
        Ok(record)
    }
}
fn load(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<LeadershipWorkExchangeRecord>, OperationsError> {
    connection
        .query_row(
            &format!("{SELECT} WHERE request_id=?1"),
            params![request_id.as_bytes()],
            RawRecord::read,
        )
        .optional()?
        .map(RawRecord::decode)
        .transpose()
}
