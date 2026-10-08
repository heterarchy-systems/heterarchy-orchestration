//! Durable resolver output for one admitted `AgentRequest`.
//!
//! Profile claims are created in the app-global Profile catalog first. This
//! module records those exact claim readbacks together with `PROVISION_NEW`
//! slots in one Operations transaction. No provider/browser effect occurs here.

use forge_domain::TimestampMillis;
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileResolutionClaim, AgentProfileResolutionClaimKind,
    AgentProfileRevision, AgentProfileRuntimeSessionId,
};
use rusqlite::{Connection, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    AgentRequestRecord, AgentRequestState, OperationsCatalog, OperationsError,
    agent_request::load_request_for_resolution, wire,
};

const AGENT_REQUEST_RESOLUTIONS_SQL: &str = r"
CREATE TABLE ops_agent_request_resolutions (
    request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    slot_ordinal INTEGER NOT NULL CHECK(slot_ordinal BETWEEN 0 AND 3),
    resolution_kind INTEGER NOT NULL CHECK(resolution_kind BETWEEN 1 AND 5),
    claim_id BLOB CHECK(claim_id IS NULL OR (length(claim_id)=16 AND claim_id != zeroblob(16))),
    profile_id BLOB CHECK(profile_id IS NULL OR (length(profile_id)=16 AND profile_id != zeroblob(16))),
    profile_revision INTEGER CHECK(profile_revision IS NULL OR profile_revision>0),
    runtime_session_id BLOB CHECK(runtime_session_id IS NULL OR (length(runtime_session_id)=16 AND runtime_session_id != zeroblob(16))),
    runtime_session_revision INTEGER CHECK(runtime_session_revision IS NULL OR runtime_session_revision>0),
    policy_revision INTEGER NOT NULL CHECK(policy_revision>0),
    reserved_at_ms INTEGER NOT NULL CHECK(reserved_at_ms>=0),
    PRIMARY KEY(request_id,slot_ordinal),
    UNIQUE(claim_id),
    FOREIGN KEY(request_id) REFERENCES ops_agent_requests(request_id),
    CHECK(
        (resolution_kind=5 AND claim_id IS NULL AND profile_id IS NULL AND profile_revision IS NULL AND runtime_session_id IS NULL AND runtime_session_revision IS NULL)
        OR
        (resolution_kind IN(1,4) AND claim_id IS NOT NULL AND profile_id IS NOT NULL AND profile_revision IS NOT NULL AND runtime_session_id IS NULL AND runtime_session_revision IS NULL)
        OR
        (resolution_kind IN(2,3) AND claim_id IS NOT NULL AND profile_id IS NOT NULL AND profile_revision IS NOT NULL AND runtime_session_id IS NOT NULL AND runtime_session_revision IS NOT NULL)
    )
);
";

/// Resolver result stored for one requested Agent slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRequestResolutionKind {
    ReuseCurrentScope,
    ReuseReadyRuntime,
    WakeExistingRuntime,
    WakeNewRuntime,
    ProvisionNew,
}

/// Exact immutable selection evidence. Profile-backed variants can only be
/// constructed from a durable app-global Profile claim readback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentRequestResolutionSelection {
    slot_ordinal: u8,
    kind: AgentRequestResolutionKind,
    claim_id: Option<Uuid>,
    profile_id: Option<AgentProfileId>,
    profile_revision: Option<AgentProfileRevision>,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    runtime_session_revision: Option<i64>,
}

impl AgentRequestResolutionSelection {
    /// Create one Profile-backed selection from the exact claim returned by the
    /// global Profile catalog.
    ///
    /// # Errors
    /// Rejects slots outside the `AgentRequest` fan-out bound.
    pub fn from_claim(
        slot_ordinal: u8,
        claim: AgentProfileResolutionClaim,
    ) -> Result<Self, OperationsError> {
        validate_slot(slot_ordinal)?;
        let kind = match claim.kind() {
            AgentProfileResolutionClaimKind::ReuseCurrentScope => {
                AgentRequestResolutionKind::ReuseCurrentScope
            }
            AgentProfileResolutionClaimKind::ReuseReadyRuntime => {
                AgentRequestResolutionKind::ReuseReadyRuntime
            }
            AgentProfileResolutionClaimKind::WakeExistingRuntime => {
                AgentRequestResolutionKind::WakeExistingRuntime
            }
            AgentProfileResolutionClaimKind::WakeNewRuntime => {
                AgentRequestResolutionKind::WakeNewRuntime
            }
        };
        Ok(Self {
            slot_ordinal,
            kind,
            claim_id: Some(claim.claim_id()),
            profile_id: Some(claim.profile_id()),
            profile_revision: Some(claim.profile_revision()),
            runtime_session_id: claim.runtime_session_id(),
            runtime_session_revision: claim.runtime_session_revision(),
        })
    }

    /// Create one slot that requires a new Profile/provider Runtime path.
    ///
    /// # Errors
    /// Rejects slots outside the `AgentRequest` fan-out bound.
    pub fn provision_new(slot_ordinal: u8) -> Result<Self, OperationsError> {
        validate_slot(slot_ordinal)?;
        Ok(Self {
            slot_ordinal,
            kind: AgentRequestResolutionKind::ProvisionNew,
            claim_id: None,
            profile_id: None,
            profile_revision: None,
            runtime_session_id: None,
            runtime_session_revision: None,
        })
    }

    #[must_use]
    pub const fn slot_ordinal(self) -> u8 {
        self.slot_ordinal
    }

    #[must_use]
    pub const fn kind(self) -> AgentRequestResolutionKind {
        self.kind
    }

    #[must_use]
    pub const fn claim_id(self) -> Option<Uuid> {
        self.claim_id
    }

    #[must_use]
    pub const fn profile_id(self) -> Option<AgentProfileId> {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(self) -> Option<AgentProfileRevision> {
        self.profile_revision
    }

    #[must_use]
    pub const fn runtime_session_id(self) -> Option<AgentProfileRuntimeSessionId> {
        self.runtime_session_id
    }

    #[must_use]
    pub const fn runtime_session_revision(self) -> Option<i64> {
        self.runtime_session_revision
    }
}

/// One transactionally committed resolver batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAgentRequestResolution {
    request_id: Uuid,
    expected_request_revision: u64,
    policy_revision: u64,
    selections: Vec<AgentRequestResolutionSelection>,
    reserved_at: TimestampMillis,
}

impl CommitAgentRequestResolution {
    /// Validate one complete resolution batch before persistence.
    ///
    /// # Errors
    /// Rejects nil request ids, zero revisions, empty/oversized selections, or
    /// non-contiguous slot ordinals.
    pub fn new(
        request_id: Uuid,
        expected_request_revision: u64,
        policy_revision: u64,
        mut selections: Vec<AgentRequestResolutionSelection>,
        reserved_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil() || expected_request_revision == 0 || policy_revision == 0 {
            return Err(OperationsError::Conflict(
                "AgentRequest resolution identity/revision",
            ));
        }
        if selections.is_empty() || selections.len() > 4 {
            return Err(OperationsError::Bounds("AgentRequest resolution slots"));
        }
        selections.sort_by_key(|selection| selection.slot_ordinal);
        for (index, selection) in selections.iter().enumerate() {
            let expected = u8::try_from(index)
                .map_err(|_| OperationsError::Bounds("AgentRequest resolution slot"))?;
            if selection.slot_ordinal != expected {
                return Err(OperationsError::Conflict(
                    "AgentRequest resolution slot ordering",
                ));
            }
        }
        Ok(Self {
            request_id,
            expected_request_revision,
            policy_revision,
            selections,
            reserved_at,
        })
    }
}

/// Durable resolution slot readback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentRequestResolutionSlot {
    selection: AgentRequestResolutionSelection,
    policy_revision: u64,
    reserved_at: TimestampMillis,
}

impl AgentRequestResolutionSlot {
    #[must_use]
    pub const fn selection(self) -> AgentRequestResolutionSelection {
        self.selection
    }

    #[must_use]
    pub const fn policy_revision(self) -> u64 {
        self.policy_revision
    }

    #[must_use]
    pub const fn reserved_at(self) -> TimestampMillis {
        self.reserved_at
    }
}

/// Request + complete resolver reservation projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestResolution {
    request: AgentRequestRecord,
    slots: Vec<AgentRequestResolutionSlot>,
}

impl AgentRequestResolution {
    #[must_use]
    pub fn request(&self) -> &AgentRequestRecord {
        &self.request
    }

    #[must_use]
    pub fn slots(&self) -> &[AgentRequestResolutionSlot] {
        &self.slots
    }
}

/// Idempotent resolution commit result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestResolutionOutcome {
    resolution: AgentRequestResolution,
    created: bool,
}

impl AgentRequestResolutionOutcome {
    #[must_use]
    pub fn resolution(&self) -> &AgentRequestResolution {
        &self.resolution
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

impl OperationsCatalog {
    /// Commit one complete resolver reservation batch. Exact replay returns the
    /// first batch; changed replay conflicts.
    ///
    /// # Errors
    /// Rejects stale request revisions, selection/count mismatches, changed
    /// replay, malformed durable rows, or uncertain commits.
    pub fn commit_agent_request_resolution(
        &mut self,
        command: &CommitAgentRequestResolution,
    ) -> Result<AgentRequestResolutionOutcome, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let request = load_request_for_resolution(&transaction, command.request_id)?
            .ok_or(OperationsError::Conflict("AgentRequest resolution request"))?;
        let selection_count = u8::try_from(command.selections.len())
            .map_err(|_| OperationsError::Bounds("AgentRequest resolution slots"))?;
        if request.count() != selection_count {
            return Err(OperationsError::Conflict(
                "AgentRequest resolution requested count",
            ));
        }
        if matches!(
            request.state(),
            AgentRequestState::ResolutionReserved | AgentRequestState::Fulfilled
        ) {
            let existing = load_resolution(&transaction, &request)?.ok_or(
                OperationsError::Corrupt("AgentRequest reserved resolution missing"),
            )?;
            if !resolution_matches_command(&existing, command) {
                return Err(OperationsError::Conflict("AgentRequest resolution replay"));
            }
            transaction.commit()?;
            return Ok(AgentRequestResolutionOutcome {
                resolution: existing,
                created: false,
            });
        }
        if request.state() != AgentRequestState::ResolutionPending
            || request.revision() != command.expected_request_revision
        {
            return Err(OperationsError::Conflict(
                "AgentRequest resolution request revision",
            ));
        }
        if resolution_row_count(&transaction, command.request_id)? != 0 {
            return Err(OperationsError::Corrupt(
                "AgentRequest pending resolution already has rows",
            ));
        }
        for selection in &command.selections {
            insert_selection(&transaction, command, *selection)?;
        }
        let changed = transaction.execute(
            "UPDATE ops_agent_requests SET state=2,revision=2
             WHERE request_id=?1 AND state=1 AND revision=?2",
            params![
                command.request_id.as_bytes(),
                i64::try_from(command.expected_request_revision)
                    .map_err(|_| OperationsError::Bounds("AgentRequest resolution revision"))?,
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict(
                "AgentRequest resolution request revision",
            ));
        }
        let reserved_request = load_request_for_resolution(&transaction, command.request_id)?
            .ok_or(OperationsError::Corrupt(
                "AgentRequest resolution update readback",
            ))?;
        let resolution = load_resolution(&transaction, &reserved_request)?.ok_or(
            OperationsError::Corrupt("AgentRequest resolution insert readback"),
        )?;
        transaction
            .commit()
            .map_err(|source| OperationsError::AgentRequestCommitUnconfirmed {
                request_id: command.request_id,
                source,
            })?;
        Ok(AgentRequestResolutionOutcome {
            resolution,
            created: true,
        })
    }

    /// Read a committed resolution batch by `AgentRequest` identity.
    ///
    /// # Errors
    /// Returns malformed durable rows or storage failures.
    pub fn agent_request_resolution(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentRequestResolution>, OperationsError> {
        let Some(request) = load_request_for_resolution(&self.connection, request_id)? else {
            return Ok(None);
        };
        load_resolution(&self.connection, &request)
    }
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(AGENT_REQUEST_RESOLUTIONS_SQL)?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    crate::agent_provision::drop_schema_for_legacy_fixture(connection)?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_agent_request_resolutions;")?;
    Ok(())
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_agent_request_resolutions",
        AGENT_REQUEST_RESOLUTIONS_SQL,
    )]
}

fn validate_slot(slot_ordinal: u8) -> Result<(), OperationsError> {
    if slot_ordinal > 3 {
        return Err(OperationsError::Bounds("AgentRequest resolution slot"));
    }
    Ok(())
}

fn insert_selection(
    connection: &Connection,
    command: &CommitAgentRequestResolution,
    selection: AgentRequestResolutionSelection,
) -> Result<(), OperationsError> {
    connection.execute(
        "INSERT INTO ops_agent_request_resolutions(
            request_id,slot_ordinal,resolution_kind,claim_id,profile_id,profile_revision,
            runtime_session_id,runtime_session_revision,policy_revision,reserved_at_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            command.request_id.as_bytes(),
            i64::from(selection.slot_ordinal),
            encode_kind(selection.kind),
            selection.claim_id.map(Uuid::into_bytes).map(Vec::from),
            selection
                .profile_id
                .map(AgentProfileId::into_uuid)
                .map(Uuid::into_bytes)
                .map(Vec::from),
            selection.profile_revision.map(AgentProfileRevision::get),
            selection
                .runtime_session_id
                .map(AgentProfileRuntimeSessionId::into_uuid)
                .map(Uuid::into_bytes)
                .map(Vec::from),
            selection.runtime_session_revision,
            i64::try_from(command.policy_revision)
                .map_err(|_| OperationsError::Bounds("AgentRequest policy revision"))?,
            command.reserved_at.get(),
        ],
    )?;
    Ok(())
}

fn resolution_row_count(connection: &Connection, request_id: Uuid) -> Result<i64, OperationsError> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM ops_agent_request_resolutions WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

pub(crate) fn load_resolution(
    connection: &Connection,
    request: &AgentRequestRecord,
) -> Result<Option<AgentRequestResolution>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT slot_ordinal,resolution_kind,claim_id,profile_id,profile_revision,
                runtime_session_id,runtime_session_revision,policy_revision,reserved_at_ms
         FROM ops_agent_request_resolutions WHERE request_id=?1 ORDER BY slot_ordinal ASC",
    )?;
    let rows = statement.query_map(params![request.request_id().as_bytes()], decode_slot_row)?;
    let slots = rows
        .map(|row| row.and_then(|decoded| decoded.map_err(to_sql_decode_error)))
        .collect::<Result<Vec<_>, _>>()?;
    if slots.is_empty() {
        return Ok(None);
    }
    if slots.len() != usize::from(request.count()) {
        return Err(OperationsError::Corrupt(
            "AgentRequest resolution slot count",
        ));
    }
    for (index, slot) in slots.iter().enumerate() {
        if usize::from(slot.selection.slot_ordinal) != index {
            return Err(OperationsError::Corrupt(
                "AgentRequest resolution slot ordering",
            ));
        }
    }
    Ok(Some(AgentRequestResolution {
        request: request.clone(),
        slots,
    }))
}

fn to_sql_decode_error(_: OperationsError) -> rusqlite::Error {
    rusqlite::Error::InvalidQuery
}

fn decode_slot_row(
    row: &rusqlite::Row<'_>,
) -> Result<Result<AgentRequestResolutionSlot, OperationsError>, rusqlite::Error> {
    let raw = RawResolutionSlot {
        slot_ordinal: row.get(0)?,
        kind: row.get(1)?,
        claim_id: row.get(2)?,
        profile_id: row.get(3)?,
        profile_revision: row.get(4)?,
        runtime_session_id: row.get(5)?,
        runtime_session_revision: row.get(6)?,
        policy_revision: row.get(7)?,
        reserved_at_ms: row.get(8)?,
    };
    Ok(decode_slot(&raw))
}

struct RawResolutionSlot {
    slot_ordinal: i64,
    kind: i64,
    claim_id: Option<Vec<u8>>,
    profile_id: Option<Vec<u8>>,
    profile_revision: Option<i64>,
    runtime_session_id: Option<Vec<u8>>,
    runtime_session_revision: Option<i64>,
    policy_revision: i64,
    reserved_at_ms: i64,
}

fn decode_slot(raw: &RawResolutionSlot) -> Result<AgentRequestResolutionSlot, OperationsError> {
    let slot_ordinal = u8::try_from(raw.slot_ordinal)
        .map_err(|_| OperationsError::Corrupt("AgentRequest resolution slot"))?;
    validate_slot(slot_ordinal)?;
    if raw.policy_revision <= 0 {
        return Err(OperationsError::Corrupt(
            "AgentRequest resolution policy revision",
        ));
    }
    let kind = decode_kind(raw.kind)?;
    let claim_id = raw
        .claim_id
        .as_ref()
        .map(|value| wire::id(value.clone()).map(Uuid::from_bytes))
        .transpose()?;
    let profile_id = raw
        .profile_id
        .as_ref()
        .map(|value| {
            wire::id(value.clone()).and_then(|bytes| {
                AgentProfileId::from_uuid(Uuid::from_bytes(bytes))
                    .map_err(|_| OperationsError::Corrupt("AgentRequest resolution Profile"))
            })
        })
        .transpose()?;
    let profile_revision = raw
        .profile_revision
        .map(AgentProfileRevision::new)
        .transpose()
        .map_err(|_| OperationsError::Corrupt("AgentRequest resolution Profile revision"))?;
    let runtime_session_id = raw
        .runtime_session_id
        .as_ref()
        .map(|value| {
            wire::id(value.clone()).and_then(|bytes| {
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(bytes)).map_err(|_| {
                    OperationsError::Corrupt("AgentRequest resolution Runtime Session")
                })
            })
        })
        .transpose()?;
    let selection = AgentRequestResolutionSelection {
        slot_ordinal,
        kind,
        claim_id,
        profile_id,
        profile_revision,
        runtime_session_id,
        runtime_session_revision: raw.runtime_session_revision,
    };
    validate_selection_shape(selection)?;
    Ok(AgentRequestResolutionSlot {
        selection,
        policy_revision: u64::try_from(raw.policy_revision)
            .map_err(|_| OperationsError::Corrupt("AgentRequest policy revision"))?,
        reserved_at: TimestampMillis::new(raw.reserved_at_ms)
            .map_err(|_| OperationsError::Corrupt("AgentRequest resolution timestamp"))?,
    })
}

fn validate_selection_shape(
    selection: AgentRequestResolutionSelection,
) -> Result<(), OperationsError> {
    let has_claim = selection.claim_id.is_some();
    let has_profile = selection.profile_id.is_some() && selection.profile_revision.is_some();
    let has_runtime =
        selection.runtime_session_id.is_some() && selection.runtime_session_revision.is_some();
    let valid = match selection.kind {
        AgentRequestResolutionKind::ProvisionNew => !has_claim && !has_profile && !has_runtime,
        AgentRequestResolutionKind::ReuseCurrentScope
        | AgentRequestResolutionKind::WakeNewRuntime => has_claim && has_profile && !has_runtime,
        AgentRequestResolutionKind::ReuseReadyRuntime
        | AgentRequestResolutionKind::WakeExistingRuntime => {
            has_claim && has_profile && has_runtime
        }
    };
    if !valid {
        return Err(OperationsError::Corrupt(
            "AgentRequest resolution selection shape",
        ));
    }
    Ok(())
}

fn resolution_matches_command(
    resolution: &AgentRequestResolution,
    command: &CommitAgentRequestResolution,
) -> bool {
    resolution.slots.len() == command.selections.len()
        && resolution
            .slots
            .iter()
            .zip(&command.selections)
            .all(|(slot, selection)| {
                slot.selection == *selection && slot.policy_revision == command.policy_revision
            })
}

const fn encode_kind(kind: AgentRequestResolutionKind) -> i64 {
    match kind {
        AgentRequestResolutionKind::ReuseCurrentScope => 1,
        AgentRequestResolutionKind::ReuseReadyRuntime => 2,
        AgentRequestResolutionKind::WakeExistingRuntime => 3,
        AgentRequestResolutionKind::WakeNewRuntime => 4,
        AgentRequestResolutionKind::ProvisionNew => 5,
    }
}

fn decode_kind(value: i64) -> Result<AgentRequestResolutionKind, OperationsError> {
    match value {
        1 => Ok(AgentRequestResolutionKind::ReuseCurrentScope),
        2 => Ok(AgentRequestResolutionKind::ReuseReadyRuntime),
        3 => Ok(AgentRequestResolutionKind::WakeExistingRuntime),
        4 => Ok(AgentRequestResolutionKind::WakeNewRuntime),
        5 => Ok(AgentRequestResolutionKind::ProvisionNew),
        _ => Err(OperationsError::Corrupt("AgentRequest resolution kind")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentRequestCommand, AgentRequestEffectScope, AgentRequestIndependence, AgentRequestIntent,
        AgentRequestRequester,
    };
    use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
    use rusqlite::Connection;
    use std::{error::Error, fs};

    #[test]
    fn v26_request_migrates_and_resolution_reservation_replays_exactly()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("agent-request-v27-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xD701);
        let request_id = Uuid::from_u128(0xD702);
        let mut catalog = OperationsCatalog::open(&path, destination)?;
        let request = AgentRequestCommand::new(
            request_id,
            AgentRequestRequester::new(
                WorkspaceId::from_uuid(Uuid::from_u128(0xD703)),
                ActivityId::from_uuid(Uuid::from_u128(0xD704)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xD705)),
                AgentSessionId::from_uuid(Uuid::from_u128(0xD706)),
                1,
            )?,
            AgentRequestIntent::new(
                "resolve one verifier".to_owned(),
                "verification".to_owned(),
                1,
                AgentRequestEffectScope::ReadOnly,
                AgentRequestIndependence::None,
                "independent verification".to_owned(),
            )?,
            TimestampMillis::new(10)?,
        )?;
        let _ = catalog.submit_agent_request(&request)?;
        drop(catalog);

        let connection = Connection::open(&path)?;
        crate::agent_request::downgrade_schema_to_v26_fixture(&connection)?;
        drop(connection);

        let mut migrated = OperationsCatalog::open(&path, destination)?;
        let preserved = migrated
            .agent_request(request_id)?
            .ok_or("request missing")?;
        assert_eq!(preserved.state(), AgentRequestState::ResolutionPending);
        assert_eq!(preserved.revision(), 1);
        let command = CommitAgentRequestResolution::new(
            request_id,
            1,
            1,
            vec![AgentRequestResolutionSelection::provision_new(0)?],
            TimestampMillis::new(20)?,
        )?;
        let first = migrated.commit_agent_request_resolution(&command)?;
        assert!(first.created());
        assert_eq!(
            first.resolution().request().state(),
            AgentRequestState::ResolutionReserved
        );
        assert_eq!(first.resolution().request().revision(), 2);
        assert_eq!(
            first.resolution().slots()[0].selection().kind(),
            AgentRequestResolutionKind::ProvisionNew
        );
        let replay = migrated.commit_agent_request_resolution(&command)?;
        assert!(!replay.created());
        assert_eq!(replay.resolution(), first.resolution());
        drop(migrated);

        let reopened = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened.agent_request_resolution(request_id)?,
            Some(first.resolution().clone())
        );
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
