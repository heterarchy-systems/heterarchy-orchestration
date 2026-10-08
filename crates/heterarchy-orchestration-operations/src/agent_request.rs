//! Durable GPT-initiated Agent capability request admission.
//!
//! Submission commits request intent only. Resolver/provider/participation
//! authorities remain separate. A later additive fulfillment witness promotes
//! the read model to `FULFILLED` only after every resolved slot has a durable
//! Ready registration in the requested Workspace/Activity.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_policy::{
    AgentRequestAdmissionDecision, AgentRequestAdmissionInput, MAX_AGENT_REQUEST_COUNT,
    evaluate_agent_request_admission,
};
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use uuid::Uuid;

use crate::teams::ensure_verified_workspace;
use crate::{OperationsCatalog, OperationsError, wire};

const AGENT_REQUESTS_V26_SQL: &str = "CREATE TABLE ops_agent_requests (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id != zeroblob(16)), requester_agent_id BLOB NOT NULL CHECK(length(requester_agent_id)=16 AND requester_agent_id != zeroblob(16)), requester_session_id BLOB NOT NULL CHECK(length(requester_session_id)=16 AND requester_session_id != zeroblob(16)), coordinator_revision INTEGER NOT NULL CHECK(coordinator_revision>0), objective TEXT NOT NULL CHECK(length(CAST(objective AS BLOB)) BETWEEN 1 AND 4096), capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 128), requested_count INTEGER NOT NULL CHECK(requested_count BETWEEN 1 AND 4), effect_scope INTEGER NOT NULL CHECK(effect_scope IN(0,1)), independence_requirement INTEGER NOT NULL CHECK(independence_requirement IN(0,1)), reason TEXT NOT NULL CHECK(length(CAST(reason AS BLOB)) BETWEEN 1 AND 2048), state INTEGER NOT NULL CHECK(state=1), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), revision INTEGER NOT NULL CHECK(revision=1))";
const AGENT_REQUESTS_SQL: &str = "CREATE TABLE ops_agent_requests (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id != zeroblob(16)), requester_agent_id BLOB NOT NULL CHECK(length(requester_agent_id)=16 AND requester_agent_id != zeroblob(16)), requester_session_id BLOB NOT NULL CHECK(length(requester_session_id)=16 AND requester_session_id != zeroblob(16)), coordinator_revision INTEGER NOT NULL CHECK(coordinator_revision>0), objective TEXT NOT NULL CHECK(length(CAST(objective AS BLOB)) BETWEEN 1 AND 4096), capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 128), requested_count INTEGER NOT NULL CHECK(requested_count BETWEEN 1 AND 4), effect_scope INTEGER NOT NULL CHECK(effect_scope IN(0,1)), independence_requirement INTEGER NOT NULL CHECK(independence_requirement IN(0,1)), reason TEXT NOT NULL CHECK(length(CAST(reason AS BLOB)) BETWEEN 1 AND 2048), state INTEGER NOT NULL CHECK(state IN(1,2)), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), revision INTEGER NOT NULL CHECK(revision IN(1,2)), CHECK(state=revision))";
const AGENT_REQUEST_FULFILLMENTS_SQL: &str = "CREATE TABLE ops_agent_request_fulfillments (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), fulfilled_at_ms INTEGER NOT NULL CHECK(fulfilled_at_ms>=0), ready_slot_count INTEGER NOT NULL CHECK(ready_slot_count BETWEEN 1 AND 4), FOREIGN KEY(request_id) REFERENCES ops_agent_requests(request_id))";

/// Upper bound of one bounded request page, mirroring the team read model.
pub const MAX_AGENT_REQUESTS_PER_READ: usize = 256;

const MAX_OBJECTIVE_BYTES: usize = 4_096;
const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_REASON_BYTES: usize = 2_048;
pub const MAX_AGENT_REQUEST_CURRENT_SCOPE_PROFILES: usize = 64;

/// Effect intent requested from a future resolved Agent. This never grants the
/// corresponding permission; Plan05/approval/Writer Lease still own effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRequestEffectScope {
    ReadOnly,
    SourceWrite,
}

/// Independence intent for resolver matching. It is provenance/selection
/// input, not hierarchy or leadership authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRequestIndependence {
    None,
    IndependentFromCurrentWriter,
}

/// Durable `AgentRequest` lifecycle states. The physical request row remains a
/// stable v27 parent for resolver/provision foreign keys; fulfillment is an
/// additive v29 witness projected as logical revision 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRequestState {
    ResolutionPending,
    ResolutionReserved,
    Fulfilled,
}

/// Validated durable capability request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestRequester {
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    requester_agent_id: AgentInstanceId,
    requester_session_id: AgentSessionId,
    coordinator_revision: u64,
}

impl AgentRequestRequester {
    /// Bind the request to one exact current Coordinator fence.
    ///
    /// # Errors
    /// Rejects a zero Coordinator revision.
    pub fn new(
        workspace_id: WorkspaceId,
        activity_id: ActivityId,
        requester_agent_id: AgentInstanceId,
        requester_session_id: AgentSessionId,
        coordinator_revision: u64,
    ) -> Result<Self, OperationsError> {
        if coordinator_revision == 0 {
            return Err(OperationsError::Conflict(
                "AgentRequest coordinator revision",
            ));
        }
        Ok(Self {
            workspace_id,
            activity_id,
            requester_agent_id,
            requester_session_id,
            coordinator_revision,
        })
    }
}

/// Validated capability intent. Provider/model/session selection is
/// deliberately absent from Slice A.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestIntent {
    objective: String,
    capability: String,
    count: u8,
    effect_scope: AgentRequestEffectScope,
    independence: AgentRequestIndependence,
    reason: String,
}

impl AgentRequestIntent {
    /// Validate bounded request content and the P0 fan-out policy.
    ///
    /// # Errors
    /// Rejects blank/oversized text or counts outside the policy ceiling.
    pub fn new(
        objective: String,
        capability: String,
        count: u8,
        effect_scope: AgentRequestEffectScope,
        independence: AgentRequestIndependence,
        reason: String,
    ) -> Result<Self, OperationsError> {
        validate_text("AgentRequest objective", &objective, MAX_OBJECTIVE_BYTES)?;
        validate_text("AgentRequest capability", &capability, MAX_CAPABILITY_BYTES)?;
        validate_text("AgentRequest reason", &reason, MAX_REASON_BYTES)?;
        if !matches!(
            evaluate_agent_request_admission(AgentRequestAdmissionInput::new(
                count,
                MAX_AGENT_REQUEST_COUNT,
            )),
            AgentRequestAdmissionDecision::Admit { .. }
        ) {
            return Err(OperationsError::Bounds("AgentRequest count"));
        }
        Ok(Self {
            objective,
            capability,
            count,
            effect_scope,
            independence,
            reason,
        })
    }
}

/// Validated durable capability request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestCommand {
    request_id: Uuid,
    requester: AgentRequestRequester,
    intent: AgentRequestIntent,
    created_at: TimestampMillis,
}

impl AgentRequestCommand {
    /// Construct one bounded request before persistence.
    ///
    /// # Errors
    /// Rejects nil request ids, zero revisions, invalid UTF-8 bounds, or P0
    /// count admission outside the configured hard ceiling.
    pub fn new(
        request_id: Uuid,
        requester: AgentRequestRequester,
        intent: AgentRequestIntent,
        created_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil() {
            return Err(OperationsError::Conflict("AgentRequest identity"));
        }
        Ok(Self {
            request_id,
            requester,
            intent,
            created_at,
        })
    }
}

/// Durable request readback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestRecord {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    requester_agent_id: AgentInstanceId,
    requester_session_id: AgentSessionId,
    coordinator_revision: u64,
    objective: String,
    capability: String,
    count: u8,
    effect_scope: AgentRequestEffectScope,
    independence: AgentRequestIndependence,
    reason: String,
    state: AgentRequestState,
    created_at: TimestampMillis,
    revision: u64,
}

impl AgentRequestRecord {
    /// Compare the complete immutable request intent, excluding observation time.
    #[must_use]
    pub fn matches_intent(&self, command: &AgentRequestCommand) -> bool {
        matches_command(self, command)
    }
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn requester_agent_id(&self) -> AgentInstanceId {
        self.requester_agent_id
    }
    pub const fn requester_session_id(&self) -> AgentSessionId {
        self.requester_session_id
    }
    #[must_use]
    pub const fn coordinator_revision(&self) -> u64 {
        self.coordinator_revision
    }
    #[must_use]
    pub fn objective(&self) -> &str {
        &self.objective
    }
    #[must_use]
    pub fn capability(&self) -> &str {
        &self.capability
    }
    #[must_use]
    pub const fn count(&self) -> u8 {
        self.count
    }
    #[must_use]
    pub const fn effect_scope(&self) -> AgentRequestEffectScope {
        self.effect_scope
    }
    #[must_use]
    pub const fn independence(&self) -> AgentRequestIndependence {
        self.independence
    }
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
    #[must_use]
    pub const fn state(&self) -> AgentRequestState {
        self.state
    }
    #[must_use]
    pub const fn created_at(&self) -> TimestampMillis {
        self.created_at
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Idempotent submission result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestSubmitOutcome {
    request: AgentRequestRecord,
    created: bool,
}

impl AgentRequestSubmitOutcome {
    #[must_use]
    pub fn request(&self) -> &AgentRequestRecord {
        &self.request
    }
    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

/// Bounded current-Activity Profile observation used only as resolver input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestCurrentScopeProfiles {
    profiles: Vec<AgentProfileId>,
    truncated: bool,
}

impl AgentRequestCurrentScopeProfiles {
    #[must_use]
    pub fn profiles(&self) -> &[AgentProfileId] {
        &self.profiles
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

impl OperationsCatalog {
    /// Commit one admitted Agent request. Exact request replay returns the same
    /// row; changed replay conflicts. No resolver/provider effect is performed.
    ///
    /// # Errors
    /// Rejects replay conflicts or uncertain commits.
    pub fn submit_agent_request(
        &mut self,
        command: &AgentRequestCommand,
    ) -> Result<AgentRequestSubmitOutcome, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_request(&transaction, command.request_id)? {
            if !existing.matches_intent(command) {
                return Err(OperationsError::Conflict("AgentRequest replay"));
            }
            transaction.commit()?;
            return Ok(AgentRequestSubmitOutcome {
                request: existing,
                created: false,
            });
        }
        transaction.execute(
            "INSERT INTO ops_agent_requests(request_id,workspace_id,activity_id,requester_agent_id,requester_session_id,coordinator_revision,objective,capability,requested_count,effect_scope,independence_requirement,reason,state,created_at_ms,revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,1,?13,1)",
            params![
                command.request_id.as_bytes(),
                command.requester.workspace_id.into_uuid().as_bytes(),
                command.requester.activity_id.into_uuid().as_bytes(),
                command.requester.requester_agent_id.into_uuid().as_bytes(),
                command.requester.requester_session_id.into_uuid().as_bytes(),
                i64::try_from(command.requester.coordinator_revision).map_err(|_| OperationsError::Bounds("AgentRequest coordinator revision"))?,
                command.intent.objective,
                command.intent.capability,
                command.intent.count,
                encode_effect_scope(command.intent.effect_scope),
                encode_independence(command.intent.independence),
                command.intent.reason,
                command.created_at.get(),
            ],
        )?;
        let request = load_request(&transaction, command.request_id)?
            .ok_or(OperationsError::Corrupt("AgentRequest insert"))?;
        transaction
            .commit()
            .map_err(|source| OperationsError::AgentRequestCommitUnconfirmed {
                request_id: command.request_id,
                source,
            })?;
        Ok(AgentRequestSubmitOutcome {
            request,
            created: true,
        })
    }

    /// Read one Agent request by idempotency identity.
    ///
    /// # Errors
    /// Returns storage or corruption failures while decoding the durable row.
    pub fn agent_request(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentRequestRecord>, OperationsError> {
        load_request(&self.connection, request_id)
    }

    /// Reconcile one reserved `AgentRequest` to Fulfilled when every resolver slot
    /// has a durable Profile-to-Session participation mapping whose exact
    /// registration is Ready in the requested Workspace/Activity.
    ///
    /// This method never creates or activates participation. It only observes
    /// existing resolver/provider/Plan05 evidence and writes the terminal
    /// fulfillment witness once all requested slots are ready.
    ///
    /// # Errors
    /// Rejects malformed/missing resolution evidence, provider slots without a
    /// confirmed Profile target, time regression, storage corruption, or an
    /// uncertain fulfillment commit.
    pub fn reconcile_agent_request_fulfillment(
        &mut self,
        request_id: Uuid,
        observed_at: TimestampMillis,
    ) -> Result<AgentRequestRecord, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let request = load_request(&transaction, request_id)?
            .ok_or(OperationsError::Conflict("AgentRequest missing"))?;
        if request.state() == AgentRequestState::Fulfilled {
            transaction.commit()?;
            return Ok(request);
        }
        if request.state() != AgentRequestState::ResolutionReserved {
            return Err(OperationsError::Conflict(
                "AgentRequest fulfillment requires reserved resolution",
            ));
        }
        if observed_at.get() < request.created_at().get() {
            return Err(OperationsError::Conflict(
                "AgentRequest fulfillment time regression",
            ));
        }
        let resolution = crate::agent_request_resolution::load_resolution(&transaction, &request)?
            .ok_or(OperationsError::Conflict("AgentRequest resolution missing"))?;
        if resolution.slots().len() != usize::from(request.count()) {
            return Err(OperationsError::Corrupt(
                "AgentRequest fulfillment resolution count",
            ));
        }
        for slot in resolution.slots() {
            let selection = slot.selection();
            let profile_id = if let Some(profile_id) = selection.profile_id() {
                profile_id
            } else {
                let operation = crate::agent_provision::operation_for_request_slot(
                    &transaction,
                    request_id,
                    selection.slot_ordinal(),
                )?
                .ok_or(OperationsError::Conflict(
                    "AgentRequest fulfillment provision missing",
                ))?;
                if operation.state() != crate::AgentProvisionState::Confirmed {
                    transaction.commit()?;
                    return Ok(request);
                }
                operation.profile_id().ok_or(OperationsError::Corrupt(
                    "AgentRequest fulfillment provision Profile",
                ))?
            };
            let Some(participation) = crate::workspace_participation::load_participation(
                &transaction,
                request.workspace_id(),
                profile_id,
            )?
            else {
                transaction.commit()?;
                return Ok(request);
            };
            let mut ready = false;
            for mapping in participation.session_mappings() {
                if crate::registration::session_is_ready(
                    &transaction,
                    request.workspace_id(),
                    request.activity_id(),
                    mapping.session_id(),
                    mapping.participant_id(),
                )? {
                    ready = true;
                    break;
                }
            }
            if !ready {
                transaction.commit()?;
                return Ok(request);
            }
        }
        transaction.execute(
            "INSERT INTO ops_agent_request_fulfillments(request_id,fulfilled_at_ms,ready_slot_count) VALUES(?1,?2,?3)",
            params![
                request_id.as_bytes(),
                observed_at.get(),
                i64::from(request.count()),
            ],
        )?;
        let fulfilled = load_request(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt("AgentRequest fulfillment insert"))?;
        transaction
            .commit()
            .map_err(|source| OperationsError::AgentRequestCommitUnconfirmed {
                request_id,
                source,
            })?;
        Ok(fulfilled)
    }

    /// Read capability-independent Profile identities already attached to the
    /// request's current Activity, excluding the requester Agent. Session
    /// replacement is stable because the join uses canonical participant id.
    ///
    /// # Errors
    /// Returns malformed identity, storage, or bound conversion failures.
    pub fn agent_request_current_scope_profiles(
        &self,
        request: &AgentRequestRecord,
    ) -> Result<AgentRequestCurrentScopeProfiles, OperationsError> {
        load_current_scope_profiles(&self.connection, request)
    }

    /// Read one bounded, newest-first page of the Workspace's durable
    /// `AgentRequests`.
    ///
    /// # Errors
    /// Returns out-of-bounds limits, foreign workspaces, and durable catalog
    /// failures.
    pub fn agent_requests(
        &self,
        workspace_id: WorkspaceId,
        limit: u32,
    ) -> Result<AgentRequestPage, OperationsError> {
        let limit = bounded_request_limit(limit)?;
        let transaction = self.connection.unchecked_transaction()?;
        ensure_verified_workspace(&transaction, workspace_id)?;
        let mut statement = transaction.prepare(
            "SELECT r.request_id,r.workspace_id,r.activity_id,r.requester_agent_id,r.requester_session_id,r.coordinator_revision,r.objective,r.capability,r.requested_count,r.effect_scope,r.independence_requirement,r.reason,r.state,r.created_at_ms,r.revision,f.fulfilled_at_ms FROM ops_agent_requests AS r LEFT JOIN ops_agent_request_fulfillments AS f ON f.request_id=r.request_id WHERE r.workspace_id=?1 ORDER BY r.created_at_ms DESC, r.request_id LIMIT ?2",
        )?;
        let mut rows = statement.query(params![
            workspace_id.into_uuid().as_bytes(),
            i64::try_from(limit + 1).map_err(|_| OperationsError::Bounds("AgentRequest list"))?
        ])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            let raw = RawAgentRequest {
                request_id: row.get(0)?,
                workspace_id: row.get(1)?,
                activity_id: row.get(2)?,
                requester_agent_id: row.get(3)?,
                requester_session_id: row.get(4)?,
                coordinator_revision: row.get(5)?,
                objective: row.get(6)?,
                capability: row.get(7)?,
                count: row.get(8)?,
                effect_scope: row.get(9)?,
                independence: row.get(10)?,
                reason: row.get(11)?,
                state: row.get(12)?,
                created_at_ms: row.get(13)?,
                revision: row.get(14)?,
                fulfilled_at_ms: row.get(15)?,
            };
            items.push(decode_request(&raw)?);
        }
        drop(rows);
        drop(statement);
        let truncated = items.len() > limit;
        items.truncate(limit);
        transaction.commit()?;
        Ok(AgentRequestPage { items, truncated })
    }
}

fn bounded_request_limit(value: u32) -> Result<usize, OperationsError> {
    let value = usize::try_from(value).map_err(|_| OperationsError::Bounds("AgentRequest list"))?;
    if value == 0 || value > MAX_AGENT_REQUESTS_PER_READ {
        return Err(OperationsError::Bounds("AgentRequest list"));
    }
    Ok(value)
}

pub(crate) fn load_current_scope_profiles(
    connection: &Connection,
    request: &AgentRequestRecord,
) -> Result<AgentRequestCurrentScopeProfiles, OperationsError> {
    let limit = i64::try_from(MAX_AGENT_REQUEST_CURRENT_SCOPE_PROFILES + 1)
        .map_err(|_| OperationsError::Bounds("AgentRequest current-scope profiles"))?;
    let mut statement = connection.prepare(
        "SELECT DISTINCT m.agent_profile_id
             FROM ops_current_effect_bindings b
             JOIN ops_workspace_participation_mappings m
               ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id
             WHERE b.workspace_id=?1 AND b.activity_id=?2 AND b.agent_id!=?3
             ORDER BY m.agent_profile_id ASC
             LIMIT ?4",
    )?;
    let rows = statement.query_map(
        params![
            request.workspace_id.into_uuid().as_bytes(),
            request.activity_id.into_uuid().as_bytes(),
            request.requester_agent_id.into_uuid().as_bytes(),
            limit,
        ],
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    let mut profiles = rows
        .map(|raw| {
            let bytes = wire::id(raw?)?;
            AgentProfileId::from_uuid(Uuid::from_bytes(bytes))
                .map_err(|_| OperationsError::Corrupt("AgentRequest current-scope Profile"))
        })
        .collect::<Result<Vec<_>, OperationsError>>()?;
    let truncated = profiles.len() > MAX_AGENT_REQUEST_CURRENT_SCOPE_PROFILES;
    profiles.truncate(MAX_AGENT_REQUEST_CURRENT_SCOPE_PROFILES);
    Ok(AgentRequestCurrentScopeProfiles {
        profiles,
        truncated,
    })
}

pub(crate) fn create_schema(connection: &rusqlite::Transaction<'_>) -> Result<(), OperationsError> {
    connection.execute_batch(AGENT_REQUESTS_SQL)?;
    Ok(())
}

pub(crate) fn create_fulfillment_schema(
    connection: &rusqlite::Transaction<'_>,
) -> Result<(), OperationsError> {
    connection.execute_batch(AGENT_REQUEST_FULFILLMENTS_SQL)?;
    Ok(())
}

pub(crate) fn migrate_v26_to_v27(
    connection: &rusqlite::Transaction<'_>,
) -> Result<(), OperationsError> {
    connection.execute_batch("ALTER TABLE ops_agent_requests RENAME TO ops_agent_requests_v26;")?;
    connection.execute_batch(AGENT_REQUESTS_SQL)?;
    connection.execute_batch(
        "INSERT INTO ops_agent_requests(
            request_id,workspace_id,activity_id,requester_agent_id,requester_session_id,
            coordinator_revision,objective,capability,requested_count,effect_scope,
            independence_requirement,reason,state,created_at_ms,revision
         )
         SELECT request_id,workspace_id,activity_id,requester_agent_id,requester_session_id,
                coordinator_revision,objective,capability,requested_count,effect_scope,
                independence_requirement,reason,state,created_at_ms,revision
         FROM ops_agent_requests_v26;
         DROP TABLE ops_agent_requests_v26;",
    )?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(
        "DROP TABLE IF EXISTS ops_agent_request_fulfillments;
         DROP TABLE IF EXISTS ops_agent_requests;",
    )?;
    Ok(())
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_agent_requests", AGENT_REQUESTS_SQL),
        (
            "table",
            "ops_agent_request_fulfillments",
            AGENT_REQUEST_FULFILLMENTS_SQL,
        ),
    ]
}

pub(crate) fn schema_objects_v28() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![("table", "ops_agent_requests", AGENT_REQUESTS_SQL)]
}

pub(crate) fn schema_objects_v26() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![("table", "ops_agent_requests", AGENT_REQUESTS_V26_SQL)]
}

#[cfg(test)]
pub(crate) fn downgrade_schema_to_v26_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    crate::agent_request_resolution::drop_schema_for_legacy_fixture(connection)?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_agent_request_fulfillments;")?;
    connection.execute_batch("ALTER TABLE ops_agent_requests RENAME TO ops_agent_requests_v27;")?;
    connection.execute_batch(AGENT_REQUESTS_V26_SQL)?;
    connection.execute_batch(
        "INSERT INTO ops_agent_requests(
            request_id,workspace_id,activity_id,requester_agent_id,requester_session_id,
            coordinator_revision,objective,capability,requested_count,effect_scope,
            independence_requirement,reason,state,created_at_ms,revision
         )
         SELECT request_id,workspace_id,activity_id,requester_agent_id,requester_session_id,
                coordinator_revision,objective,capability,requested_count,effect_scope,
                independence_requirement,reason,state,created_at_ms,revision
         FROM ops_agent_requests_v27;
         DROP TABLE ops_agent_requests_v27;",
    )?;
    connection.pragma_update(None, "user_version", 26_i64)?;
    Ok(())
}

fn validate_text(field: &'static str, value: &str, maximum: usize) -> Result<(), OperationsError> {
    if value.trim().is_empty() || value.len() > maximum {
        return Err(OperationsError::Bounds(field));
    }
    Ok(())
}

fn matches_command(record: &AgentRequestRecord, command: &AgentRequestCommand) -> bool {
    record.workspace_id == command.requester.workspace_id
        && record.activity_id == command.requester.activity_id
        && record.requester_agent_id == command.requester.requester_agent_id
        && record.requester_session_id == command.requester.requester_session_id
        && record.coordinator_revision == command.requester.coordinator_revision
        && record.objective == command.intent.objective
        && record.capability == command.intent.capability
        && record.count == command.intent.count
        && record.effect_scope == command.intent.effect_scope
        && record.independence == command.intent.independence
        && record.reason == command.intent.reason
}

#[derive(Debug)]
struct RawAgentRequest {
    request_id: Vec<u8>,
    workspace_id: Vec<u8>,
    activity_id: Vec<u8>,
    requester_agent_id: Vec<u8>,
    requester_session_id: Vec<u8>,
    coordinator_revision: i64,
    objective: String,
    capability: String,
    count: i64,
    effect_scope: i64,
    independence: i64,
    reason: String,
    state: i64,
    created_at_ms: i64,
    revision: i64,
    fulfilled_at_ms: Option<i64>,
}

fn load_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentRequestRecord>, OperationsError> {
    let raw = connection.query_row(
        "SELECT r.request_id,r.workspace_id,r.activity_id,r.requester_agent_id,r.requester_session_id,r.coordinator_revision,r.objective,r.capability,r.requested_count,r.effect_scope,r.independence_requirement,r.reason,r.state,r.created_at_ms,r.revision,f.fulfilled_at_ms FROM ops_agent_requests AS r LEFT JOIN ops_agent_request_fulfillments AS f ON f.request_id=r.request_id WHERE r.request_id=?1",
        params![request_id.as_bytes()],
        |row| Ok(RawAgentRequest { request_id: row.get(0)?, workspace_id: row.get(1)?, activity_id: row.get(2)?, requester_agent_id: row.get(3)?, requester_session_id: row.get(4)?, coordinator_revision: row.get(5)?, objective: row.get(6)?, capability: row.get(7)?, count: row.get(8)?, effect_scope: row.get(9)?, independence: row.get(10)?, reason: row.get(11)?, state: row.get(12)?, created_at_ms: row.get(13)?, revision: row.get(14)?, fulfilled_at_ms: row.get(15)? }),
    ).optional()?;
    raw.as_ref().map(decode_request).transpose()
}

/// One bounded page of durable `AgentRequests`, newest first.
#[derive(Debug, Clone)]
pub struct AgentRequestPage {
    items: Vec<AgentRequestRecord>,
    truncated: bool,
}

impl AgentRequestPage {
    #[must_use]
    pub fn items(&self) -> &[AgentRequestRecord] {
        &self.items
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

pub(crate) fn load_request_for_resolution(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentRequestRecord>, OperationsError> {
    load_request(connection, request_id)
}

fn decode_request(raw: &RawAgentRequest) -> Result<AgentRequestRecord, OperationsError> {
    let count =
        u8::try_from(raw.count).map_err(|_| OperationsError::Corrupt("AgentRequest count"))?;
    if count == 0 || count > MAX_AGENT_REQUEST_COUNT || raw.coordinator_revision <= 0 {
        return Err(OperationsError::Corrupt("AgentRequest row"));
    }
    let (state, revision) =
        decode_request_state(raw.state, raw.revision, raw.fulfilled_at_ms.is_some())?;
    if let Some(fulfilled_at_ms) = raw.fulfilled_at_ms
        && fulfilled_at_ms < raw.created_at_ms
    {
        return Err(OperationsError::Corrupt(
            "AgentRequest fulfillment timestamp",
        ));
    }
    validate_text(
        "AgentRequest objective",
        &raw.objective,
        MAX_OBJECTIVE_BYTES,
    )?;
    validate_text(
        "AgentRequest capability",
        &raw.capability,
        MAX_CAPABILITY_BYTES,
    )?;
    validate_text("AgentRequest reason", &raw.reason, MAX_REASON_BYTES)?;
    Ok(AgentRequestRecord {
        request_id: Uuid::from_bytes(wire::id(raw.request_id.clone())?),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.workspace_id.clone())?)),
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(raw.activity_id.clone())?)),
        requester_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
            raw.requester_agent_id.clone(),
        )?)),
        requester_session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
            raw.requester_session_id.clone(),
        )?)),
        coordinator_revision: u64::try_from(raw.coordinator_revision)
            .map_err(|_| OperationsError::Corrupt("AgentRequest coordinator revision"))?,
        objective: raw.objective.clone(),
        capability: raw.capability.clone(),
        count,
        effect_scope: decode_effect_scope(raw.effect_scope)?,
        independence: decode_independence(raw.independence)?,
        reason: raw.reason.clone(),
        state,
        created_at: TimestampMillis::new(raw.created_at_ms)
            .map_err(|_| OperationsError::Corrupt("AgentRequest timestamp"))?,
        revision,
    })
}

fn decode_request_state(
    state: i64,
    revision: i64,
    fulfilled: bool,
) -> Result<(AgentRequestState, u64), OperationsError> {
    let (state, revision) = match (state, revision, fulfilled) {
        (1, 1, false) => (AgentRequestState::ResolutionPending, 1),
        (2, 2, false) => (AgentRequestState::ResolutionReserved, 2),
        (2, 2, true) => (AgentRequestState::Fulfilled, 3),
        _ => return Err(OperationsError::Corrupt("AgentRequest state/revision")),
    };
    Ok((state, revision))
}

const fn encode_effect_scope(value: AgentRequestEffectScope) -> i64 {
    match value {
        AgentRequestEffectScope::ReadOnly => 0,
        AgentRequestEffectScope::SourceWrite => 1,
    }
}
fn decode_effect_scope(value: i64) -> Result<AgentRequestEffectScope, OperationsError> {
    match value {
        0 => Ok(AgentRequestEffectScope::ReadOnly),
        1 => Ok(AgentRequestEffectScope::SourceWrite),
        _ => Err(OperationsError::Corrupt("AgentRequest effect scope")),
    }
}
const fn encode_independence(value: AgentRequestIndependence) -> i64 {
    match value {
        AgentRequestIndependence::None => 0,
        AgentRequestIndependence::IndependentFromCurrentWriter => 1,
    }
}
fn decode_independence(value: i64) -> Result<AgentRequestIndependence, OperationsError> {
    match value {
        0 => Ok(AgentRequestIndependence::None),
        1 => Ok(AgentRequestIndependence::IndependentFromCurrentWriter),
        _ => Err(OperationsError::Corrupt("AgentRequest independence")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::fs;

    fn request(
        request_id: Uuid,
        created_at_ms: i64,
        reason: &str,
    ) -> Result<AgentRequestCommand, OperationsError> {
        AgentRequestCommand::new(
            request_id,
            AgentRequestRequester::new(
                WorkspaceId::from_uuid(Uuid::from_u128(0xA001)),
                ActivityId::from_uuid(Uuid::from_u128(0xA002)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xA003)),
                AgentSessionId::from_uuid(Uuid::from_u128(0xA004)),
                7,
            )?,
            AgentRequestIntent::new(
                "independently verify the durable AgentRequest boundary".to_owned(),
                "verification".to_owned(),
                4,
                AgentRequestEffectScope::ReadOnly,
                AgentRequestIndependence::IndependentFromCurrentWriter,
                reason.to_owned(),
            )?,
            TimestampMillis::new(created_at_ms)
                .map_err(|_| OperationsError::Conflict("AgentRequest test timestamp"))?,
        )
    }

    #[test]
    fn durable_agent_request_replays_semantic_identity_and_bounds_fanout()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("agent-request-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xA100);
        let request_id = Uuid::from_u128(0xA200);
        let mut catalog = OperationsCatalog::open(&path, destination)?;

        let first =
            catalog.submit_agent_request(&request(request_id, 10, "independent evidence")?)?;
        assert!(first.created());
        assert_eq!(
            first.request().state(),
            AgentRequestState::ResolutionPending
        );
        assert_eq!(first.request().count(), 4);
        assert_eq!(first.request().created_at(), TimestampMillis::new(10)?);

        let replay =
            catalog.submit_agent_request(&request(request_id, 20, "independent evidence")?)?;
        assert!(!replay.created());
        assert_eq!(replay.request(), first.request());
        assert!(matches!(
            catalog.submit_agent_request(&request(request_id, 30, "changed semantic intent")?),
            Err(OperationsError::Conflict("AgentRequest replay"))
        ));
        assert!(matches!(
            AgentRequestIntent::new(
                "objective".to_owned(),
                "verification".to_owned(),
                5,
                AgentRequestEffectScope::ReadOnly,
                AgentRequestIndependence::None,
                "reason".to_owned(),
            ),
            Err(OperationsError::Bounds("AgentRequest count"))
        ));

        drop(catalog);
        let reopened = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened.agent_request(request_id)?,
            Some(first.request().clone())
        );
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn agent_requests_page_is_bounded_newest_first_and_workspace_scoped()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("agent-request-list-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let destination = Uuid::now_v7();
        let mut catalog = OperationsCatalog::open(&root.join("operations.sqlite3"), destination)?;

        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xA001));
        // The bounded list verifies its workspace against a verified import;
        // seed the phase-2 import rows for both workspaces.
        for seeded in [workspace, WorkspaceId::from_uuid(Uuid::from_u128(0xB001))] {
            let migration = Uuid::now_v7();
            catalog.connection.execute(
                "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
                rusqlite::params![
                    migration.as_bytes(),
                    seeded.into_uuid().as_bytes(),
                    destination.as_bytes(),
                    [0_u8; 32].as_slice()
                ],
            )?;
        }
        let mut submit_at = |request_id: Uuid, workspace: WorkspaceId, created_at_ms: i64| {
            let command = AgentRequestCommand::new(
                request_id,
                AgentRequestRequester::new(
                    workspace,
                    ActivityId::from_uuid(Uuid::from_u128(0xA002)),
                    AgentInstanceId::from_uuid(Uuid::from_u128(0xA003)),
                    AgentSessionId::from_uuid(Uuid::from_u128(0xA004)),
                    7,
                )?,
                AgentRequestIntent::new(
                    "bounded list observation".to_owned(),
                    "verification".to_owned(),
                    1,
                    AgentRequestEffectScope::ReadOnly,
                    AgentRequestIndependence::None,
                    "reason".to_owned(),
                )?,
                TimestampMillis::new(created_at_ms)
                    .map_err(|_| OperationsError::Conflict("AgentRequest test timestamp"))?,
            )?;
            catalog.submit_agent_request(&command)
        };

        let older = Uuid::from_u128(0xA200);
        let newer = Uuid::from_u128(0xA201);
        let foreign = Uuid::from_u128(0xA202);
        assert!(submit_at(older, workspace, 10)?.created());
        assert!(submit_at(newer, workspace, 20)?.created());
        assert!(submit_at(foreign, WorkspaceId::from_uuid(Uuid::from_u128(0xB001)), 30)?.created());

        let page = catalog.agent_requests(workspace, 1)?;
        assert_eq!(page.items().len(), 1);
        assert_eq!(page.items()[0].request_id(), newer);
        assert!(page.truncated());

        let page = catalog.agent_requests(
            workspace,
            u32::try_from(MAX_AGENT_REQUESTS_PER_READ)
                .map_err(|_| OperationsError::Bounds("AgentRequest list"))?,
        )?;
        let ids: Vec<Uuid> = page
            .items()
            .iter()
            .map(AgentRequestRecord::request_id)
            .collect();
        assert_eq!(ids, vec![newer, older]);
        assert!(!page.truncated());

        let foreign_page =
            catalog.agent_requests(WorkspaceId::from_uuid(Uuid::from_u128(0xB001)), 64)?;
        assert_eq!(foreign_page.items().len(), 1);
        assert_eq!(foreign_page.items()[0].request_id(), foreign);

        assert!(matches!(
            catalog.agent_requests(workspace, 0),
            Err(OperationsError::Bounds("AgentRequest list"))
        ));
        assert!(matches!(
            catalog.agent_requests(
                workspace,
                u32::try_from(MAX_AGENT_REQUESTS_PER_READ)
                    .map_err(|_| OperationsError::Bounds("AgentRequest list"))?
                    + 1,
            ),
            Err(OperationsError::Bounds("AgentRequest list"))
        ));

        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
