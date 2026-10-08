//! Orchestration-owned logical departure commit after a Forge departure
//! reconciliation.
//!
//! Forge remains authoritative for the detach/handoff effect mutation and
//! emits an immutable reconciliation receipt. This module accepts only a
//! receipt that exactly matches the resolved Ready logical binding, then
//! atomically appends the departure events, advances the departing
//! participant's logical membership revision chain, clears the departing
//! participant's live presence, retires the departing participant's current
//! effect-binding projection, and (for handoff) delivers one unacknowledged
//! receipt to the receiving participant. The logical member identity and
//! departure journal persist for history and elections; replay is journal-first
//! and does not require a current effect binding.
//!
//! The shared `ops_logical_events` journal is constrained to the single
//! JOINED event per participant (`event_kind=0`, `membership_revision=1`)
//! and `ops_logical_receipts` requires its event to live there, so departure
//! events and the target delivery receipt own dedicated additive tables
//! (schema version 8). For the same reason the departure membership revision
//! chain is carried by the departure events themselves (`membership_revision`
//! is pinned to 1 on `ops_logical_members` by the frozen ladder): each
//! departure event records the participant's post-transition revision, and
//! the exact transition is fenced by the Ready-binding resolution inside one
//! IMMEDIATE transaction plus the deterministic event identity.
//!
//! Deterministic event identities follow the `joined_event_id` SHA-256
//! domain pattern over the Workspace, departing participant, and request id
//! for `Left` and `HandoffStarted`; `HandoffCompleted` mirrors Forge's
//! `AgentEventId::for_handoff_completed` by hashing the derived
//! `HandoffStarted` identity under its own domain.

use crate::{
    OperationsCatalog, OperationsError,
    departure_observation::{
        DepartureJournalInput, DepartureObservation, detach_journal_input, handoff_journal_input,
        validate_detach_replay, validate_detach_source, validate_handoff_replay,
        validate_handoff_source,
    },
    presence::resolve_ready_logical_target_binding,
    registration, wire,
};
use forge_domain::{
    ActivityCoordinatorRevision, ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId,
    AgentInstanceRevision, AgentSessionId, AgentTransportKind, AuthenticatedPrincipal,
    TimestampMillis, WorkspaceId,
};
use forge_persistence::AgentDepartureReconciliation;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const LEFT_EVENT_DOMAIN: &[u8] = b"heterarchy-orchestration-departure-left-v1";
const HANDOFF_STARTED_EVENT_DOMAIN: &[u8] =
    b"heterarchy-orchestration-departure-handoff-started-v1";
const HANDOFF_COMPLETED_EVENT_DOMAIN: &[u8] =
    b"heterarchy-orchestration-departure-handoff-completed-v1";

/// Departure participation event kinds; `ops_logical_events` keeps kind 0 for
/// the single JOINED event per participant.
const DEPARTURE_EVENT_LEFT: i64 = 1;
const DEPARTURE_EVENT_HANDOFF_STARTED: i64 = 2;
const DEPARTURE_EVENT_HANDOFF_COMPLETED: i64 = 3;

const DEPARTURE_EVENTS_SQL: &str = "CREATE TABLE ops_logical_departure_events (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), event_id BLOB NOT NULL CHECK(length(event_id)=16 AND event_id != zeroblob(16)), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), event_kind INTEGER NOT NULL CHECK(event_kind IN(1,2,3)), membership_revision INTEGER NOT NULL CHECK(membership_revision>=2), request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), PRIMARY KEY(workspace_id,event_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id))";
const DEPARTURES_SQL: &str = "CREATE TABLE ops_logical_departures (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), agent_id BLOB NOT NULL CHECK(length(agent_id)=16 AND agent_id != zeroblob(16)), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), kind INTEGER NOT NULL CHECK(kind IN(0,1)), expected_agent_revision INTEGER NOT NULL CHECK(expected_agent_revision>=1), result_agent_revision INTEGER NOT NULL CHECK(result_agent_revision=expected_agent_revision+1), target_participant_id BLOB CHECK(target_participant_id IS NULL OR (length(target_participant_id)=16 AND target_participant_id != zeroblob(16))), target_agent_id BLOB CHECK(target_agent_id IS NULL OR length(target_agent_id)=16), target_session_id BLOB CHECK(target_session_id IS NULL OR length(target_session_id)=16), result_coordinator_revision INTEGER CHECK(result_coordinator_revision IS NULL OR result_coordinator_revision>=1), departed_event_id BLOB CHECK(departed_event_id IS NULL OR length(departed_event_id)=16), started_event_id BLOB CHECK(started_event_id IS NULL OR length(started_event_id)=16), completed_event_id BLOB CHECK(completed_event_id IS NULL OR length(completed_event_id)=16), membership_revision INTEGER NOT NULL CHECK(membership_revision>=2), presence_cleared INTEGER NOT NULL CHECK(presence_cleared IN(0,1)), source_reconciled_at_ms INTEGER NOT NULL CHECK(source_reconciled_at_ms>=0), committed_at_ms INTEGER NOT NULL CHECK(committed_at_ms>=source_reconciled_at_ms), CHECK((kind=0 AND target_participant_id IS NULL AND target_agent_id IS NULL AND target_session_id IS NULL AND result_coordinator_revision IS NULL AND departed_event_id IS NOT NULL AND started_event_id IS NULL AND completed_event_id IS NULL) OR (kind=1 AND target_participant_id IS NOT NULL AND target_agent_id IS NOT NULL AND target_session_id IS NOT NULL AND result_coordinator_revision IS NOT NULL AND departed_event_id IS NULL AND started_event_id IS NOT NULL AND completed_event_id IS NOT NULL)), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id), FOREIGN KEY(workspace_id,departed_event_id) REFERENCES ops_logical_departure_events(workspace_id,event_id), FOREIGN KEY(workspace_id,started_event_id) REFERENCES ops_logical_departure_events(workspace_id,event_id), FOREIGN KEY(workspace_id,completed_event_id) REFERENCES ops_logical_departure_events(workspace_id,event_id))";
const DEPARTURE_RECEIPTS_SQL: &str = "CREATE TABLE ops_logical_departure_receipts (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), recipient_participant_id BLOB NOT NULL CHECK(length(recipient_participant_id)=16 AND recipient_participant_id != zeroblob(16)), event_id BLOB NOT NULL CHECK(length(event_id)=16), receipt_revision INTEGER NOT NULL CHECK(receipt_revision=1), acknowledged_at_ms INTEGER CHECK(acknowledged_at_ms IS NULL OR acknowledged_at_ms>=0), committed_at_ms INTEGER NOT NULL CHECK(committed_at_ms>=0), PRIMARY KEY(workspace_id,recipient_participant_id,event_id), FOREIGN KEY(workspace_id,recipient_participant_id) REFERENCES ops_logical_members(workspace_id,participant_id), FOREIGN KEY(workspace_id,event_id) REFERENCES ops_logical_departure_events(workspace_id,event_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_logical_departure_events",
            DEPARTURE_EVENTS_SQL,
        ),
        ("table", "ops_logical_departures", DEPARTURES_SQL),
        (
            "table",
            "ops_logical_departure_receipts",
            DEPARTURE_RECEIPTS_SQL,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Which departure lifecycle a committed logical departure receipt records.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalDepartureKind {
    Detach,
    Handoff,
}

impl LogicalDepartureKind {
    const fn encode(self) -> i64 {
        match self {
            Self::Detach => 0,
            Self::Handoff => 1,
        }
    }

    const fn decode(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::Detach),
            1 => Ok(Self::Handoff),
            _ => Err(OperationsError::Corrupt("logical departure kind")),
        }
    }
}

/// Exact logical detach subject resolved on the authenticated boundary.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalDetachCommand {
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    participant_id: Uuid,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    observed_at: TimestampMillis,
}

impl LogicalDetachCommand {
    pub const fn new(
        workspace_id: WorkspaceId,
        activity_id: ActivityId,
        participant_id: Uuid,
        agent_id: AgentInstanceId,
        session_id: AgentSessionId,
        principal: AuthenticatedPrincipal,
        observed_at: TimestampMillis,
    ) -> Self {
        Self {
            workspace_id,
            activity_id,
            participant_id,
            agent_id,
            session_id,
            principal,
            observed_at,
        }
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
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
    }

    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

/// Exact logical handoff subject plus the receiving participant binding.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalHandoffCommand {
    detach: LogicalDetachCommand,
    target_participant_id: Uuid,
    target_agent_id: AgentInstanceId,
    target_session_id: AgentSessionId,
}

impl LogicalHandoffCommand {
    pub const fn new(
        detach: LogicalDetachCommand,
        target_participant_id: Uuid,
        target_agent_id: AgentInstanceId,
        target_session_id: AgentSessionId,
    ) -> Self {
        Self {
            detach,
            target_participant_id,
            target_agent_id,
            target_session_id,
        }
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.detach.workspace_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.detach.activity_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.detach.participant_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.detach.agent_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.detach.session_id
    }

    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.detach.principal
    }

    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.detach.observed_at
    }

    #[must_use]
    pub const fn target_participant_id(&self) -> Uuid {
        self.target_participant_id
    }

    #[must_use]
    pub const fn target_agent_id(&self) -> AgentInstanceId {
        self.target_agent_id
    }

    pub const fn target_session_id(&self) -> AgentSessionId {
        self.target_session_id
    }
}

/// Immutable readback of one committed Orchestration logical departure.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDepartureReceipt {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    kind: LogicalDepartureKind,
    expected_agent_revision: AgentInstanceRevision,
    result_agent_revision: AgentInstanceRevision,
    target_participant_id: Option<Uuid>,
    target_agent_id: Option<AgentInstanceId>,
    target_session_id: Option<AgentSessionId>,
    result_coordinator_revision: Option<ActivityCoordinatorRevision>,
    departed_event_id: Option<Uuid>,
    handoff_started_event_id: Option<Uuid>,
    handoff_completed_event_id: Option<Uuid>,
    membership_revision: u64,
    presence_cleared: bool,
    source_reconciled_at: TimestampMillis,
    committed_at: TimestampMillis,
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
}

impl LogicalDepartureReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    pub const fn kind(&self) -> LogicalDepartureKind {
        self.kind
    }

    #[must_use]
    pub const fn expected_agent_revision(&self) -> AgentInstanceRevision {
        self.expected_agent_revision
    }

    #[must_use]
    pub const fn result_agent_revision(&self) -> AgentInstanceRevision {
        self.result_agent_revision
    }

    #[must_use]
    pub const fn target_participant_id(&self) -> Option<Uuid> {
        self.target_participant_id
    }

    #[must_use]
    pub const fn target_agent_id(&self) -> Option<AgentInstanceId> {
        self.target_agent_id
    }

    #[must_use]
    pub const fn target_session_id(&self) -> Option<AgentSessionId> {
        self.target_session_id
    }

    #[must_use]
    pub const fn result_coordinator_revision(&self) -> Option<ActivityCoordinatorRevision> {
        self.result_coordinator_revision
    }

    #[must_use]
    pub const fn departed_event_id(&self) -> Option<Uuid> {
        self.departed_event_id
    }

    #[must_use]
    pub const fn handoff_started_event_id(&self) -> Option<Uuid> {
        self.handoff_started_event_id
    }

    #[must_use]
    pub const fn handoff_completed_event_id(&self) -> Option<Uuid> {
        self.handoff_completed_event_id
    }

    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }

    #[must_use]
    pub const fn presence_cleared(&self) -> bool {
        self.presence_cleared
    }

    #[must_use]
    pub const fn source_reconciled_at(&self) -> TimestampMillis {
        self.source_reconciled_at
    }

    #[must_use]
    pub const fn committed_at(&self) -> TimestampMillis {
        self.committed_at
    }

    #[must_use]
    pub const fn display_name(&self) -> &AgentDisplayName {
        &self.display_name
    }

    #[must_use]
    pub const fn declared_role(&self) -> &AgentDeclaredRole {
        &self.declared_role
    }

    #[must_use]
    pub const fn transport(&self) -> AgentTransportKind {
        self.transport
    }
}

impl OperationsCatalog {
    /// Commit the Orchestration side of a Forge-confirmed logical detach.
    /// No Forge operational presence/event/inbox row is written here.
    ///
    /// # Errors
    /// Rejects foreign source receipts, non-Ready bindings, participant
    /// binding mismatches, conflicting replay, corrupt state, and uncertain
    /// commits.
    pub fn commit_logical_detach(
        &mut self,
        command: LogicalDetachCommand,
        source: &AgentDepartureReconciliation,
        committed_at: TimestampMillis,
    ) -> Result<LogicalDepartureReceipt, OperationsError> {
        self.commit_logical_detach_observation(
            command,
            &DepartureObservation::from(source),
            committed_at,
        )
    }

    /// Commit the Orchestration side of a checked Forge departure observation.
    /// No Forge operational presence/event/inbox row is written here.
    ///
    /// # Errors
    /// Rejects foreign source observations, non-Ready bindings, participant
    /// binding mismatches, conflicting replay, corrupt state, and uncertain
    /// commits.
    pub fn commit_logical_detach_observation(
        &mut self,
        command: LogicalDetachCommand,
        source: &DepartureObservation,
        committed_at: TimestampMillis,
    ) -> Result<LogicalDepartureReceipt, OperationsError> {
        if command.participant_id.is_nil() {
            return Err(OperationsError::Conflict("departure participant identity"));
        }
        if committed_at.get() < source.reconciled_at().get() {
            return Err(OperationsError::Conflict("departure timestamp"));
        }
        validate_detach_source(&command, source)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = replay_detach(&transaction, &command, source)? {
            commit_departure(transaction, command.workspace_id, command.participant_id)?;
            return Ok(receipt);
        }
        let binding = crate::presence::resolve_ready_logical_binding(
            &transaction,
            command.workspace_id,
            command.activity_id,
            command.agent_id,
            command.session_id,
            command.principal,
        )?;
        if binding.participant_id != command.participant_id {
            return Err(OperationsError::Conflict("departure participant binding"));
        }

        let left_event_id = departure_event_id(
            LEFT_EVENT_DOMAIN,
            command.workspace_id,
            command.participant_id,
            source.request_id(),
        );
        let next_revision = participant_departure_revision(
            &transaction,
            command.workspace_id,
            command.participant_id,
        )?
        .checked_add(1)
        .ok_or(OperationsError::Bounds("departure membership revision"))?;
        let presence_cleared =
            clear_presence(&transaction, command.workspace_id, command.participant_id)?;
        insert_departure_event(
            &transaction,
            &DepartureEventRow {
                workspace_id: command.workspace_id,
                event_id: left_event_id,
                participant_id: command.participant_id,
                activity_id: command.activity_id,
                event_kind: DEPARTURE_EVENT_LEFT,
                membership_revision: next_revision,
                request_id: source.request_id(),
                observed_at: command.observed_at,
            },
        )?;
        insert_departure_journal(
            &transaction,
            &detach_journal_input(
                &command,
                source,
                committed_at,
                left_event_id,
                next_revision,
                presence_cleared,
            ),
        )?;
        crate::leadership::reconcile_departing_leader(
            &transaction,
            command.workspace_id,
            command.activity_id,
            command.participant_id,
            committed_at.get(),
        )?;
        crate::effect_binding::retire_exact(
            &transaction,
            command.workspace_id,
            command.participant_id,
            command.activity_id,
            command.session_id,
            command.agent_id,
        )?;
        let receipt = decode_departure(&transaction, source.request_id())?
            .ok_or(OperationsError::Corrupt("departure readback"))?;
        validate_detach_replay(&receipt, source, command.participant_id, left_event_id)?;
        commit_departure(transaction, command.workspace_id, command.participant_id)?;
        Ok(receipt)
    }

    /// Commit the Orchestration side of a Forge-confirmed logical handoff.
    /// Both the departing and receiving participants must resolve as Ready
    /// logical bindings in the same Workspace and Activity; the target's
    /// live presence is left untouched so its own touch advances it.
    ///
    /// # Errors
    /// Rejects foreign source receipts, non-Ready source or target bindings,
    /// identical source/target participants, conflicting replay, corrupt
    /// state, and uncertain commits.
    pub fn commit_logical_handoff(
        &mut self,
        command: LogicalHandoffCommand,
        source: &AgentDepartureReconciliation,
        committed_at: TimestampMillis,
    ) -> Result<LogicalDepartureReceipt, OperationsError> {
        self.commit_logical_handoff_observation(
            command,
            &DepartureObservation::from(source),
            committed_at,
        )
    }

    /// Commit the Orchestration side of a checked Forge departure observation.
    /// Both the departing and receiving participants must resolve as Ready
    /// logical bindings in the same Workspace and Activity.
    ///
    /// # Errors
    /// Rejects foreign source observations, non-Ready source or target bindings,
    /// identical source/target participants, conflicting replay, corrupt
    /// state, and uncertain commits.
    pub fn commit_logical_handoff_observation(
        &mut self,
        command: LogicalHandoffCommand,
        source: &DepartureObservation,
        committed_at: TimestampMillis,
    ) -> Result<LogicalDepartureReceipt, OperationsError> {
        if command.participant_id().is_nil() || command.target_participant_id().is_nil() {
            return Err(OperationsError::Conflict("departure participant identity"));
        }
        if command.target_participant_id() == command.participant_id() {
            return Err(OperationsError::Conflict("departure handoff target"));
        }
        if committed_at.get() < source.reconciled_at().get() {
            return Err(OperationsError::Conflict("departure timestamp"));
        }
        validate_handoff_source(&command, source)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = replay_handoff(&transaction, &command, source)? {
            commit_departure(
                transaction,
                command.workspace_id(),
                command.participant_id(),
            )?;
            return Ok(receipt);
        }
        let binding = crate::presence::resolve_ready_logical_binding(
            &transaction,
            command.workspace_id(),
            command.activity_id(),
            command.agent_id(),
            command.session_id(),
            command.principal(),
        )?;
        if binding.participant_id != command.participant_id() {
            return Err(OperationsError::Conflict("departure participant binding"));
        }
        let target = resolve_ready_logical_target_binding(
            &transaction,
            command.workspace_id(),
            command.activity_id(),
            command.target_agent_id(),
            command.target_session_id(),
        )?;
        if target.participant_id != command.target_participant_id() {
            return Err(OperationsError::Conflict(
                "departure handoff target binding",
            ));
        }

        let (started_event_id, completed_event_id) =
            record_handoff_writes(&transaction, &command, source, committed_at)?;
        crate::leadership::reconcile_departing_leader(
            &transaction,
            command.workspace_id(),
            command.activity_id(),
            command.participant_id(),
            committed_at.get(),
        )?;
        crate::effect_binding::retire_exact(
            &transaction,
            command.workspace_id(),
            command.participant_id(),
            command.activity_id(),
            command.session_id(),
            command.agent_id(),
        )?;
        let receipt = decode_departure(&transaction, source.request_id())?
            .ok_or(OperationsError::Corrupt("departure readback"))?;
        validate_handoff_replay(
            &receipt,
            source,
            &command,
            started_event_id,
            completed_event_id,
        )?;
        commit_departure(
            transaction,
            command.workspace_id(),
            command.participant_id(),
        )?;
        Ok(receipt)
    }

    /// Read one immutable Orchestration logical departure receipt.
    ///
    /// # Errors
    /// Returns storage failures or corrupt journal/event state.
    pub fn logical_departure(
        &self,
        request_id: Uuid,
    ) -> Result<Option<LogicalDepartureReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let value = decode_departure(&transaction, request_id)?;
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(value)
    }
}

fn replay_detach(
    transaction: &rusqlite::Connection,
    command: &LogicalDetachCommand,
    source: &DepartureObservation,
) -> Result<Option<LogicalDepartureReceipt>, OperationsError> {
    let left_event_id = departure_event_id(
        LEFT_EVENT_DOMAIN,
        command.workspace_id,
        command.participant_id,
        source.request_id(),
    );
    if !departure_event_exists(transaction, command.workspace_id, left_event_id)? {
        return Ok(None);
    }
    let receipt = decode_departure(transaction, source.request_id())?
        .ok_or(OperationsError::Corrupt("departure journal missing"))?;
    validate_detach_replay(&receipt, source, command.participant_id, left_event_id)?;
    Ok(Some(receipt))
}

fn replay_handoff(
    transaction: &rusqlite::Connection,
    command: &LogicalHandoffCommand,
    source: &DepartureObservation,
) -> Result<Option<LogicalDepartureReceipt>, OperationsError> {
    let started_event_id = departure_event_id(
        HANDOFF_STARTED_EVENT_DOMAIN,
        command.workspace_id(),
        command.participant_id(),
        source.request_id(),
    );
    let completed_event_id = handoff_completed_event_id(started_event_id);
    if !departure_event_exists(transaction, command.workspace_id(), completed_event_id)? {
        return Ok(None);
    }
    let receipt = decode_departure(transaction, source.request_id())?
        .ok_or(OperationsError::Corrupt("departure journal missing"))?;
    validate_handoff_replay(
        &receipt,
        source,
        command,
        started_event_id,
        completed_event_id,
    )?;
    Ok(Some(receipt))
}

/// Deterministic departure event identity following the `joined_event_id`
/// domain-separated SHA-256 pattern.
fn departure_event_id(
    domain: &[u8],
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    request_id: Uuid,
) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update((16_u32).to_be_bytes());
    digest.update(workspace_id.into_uuid().as_bytes());
    digest.update((16_u32).to_be_bytes());
    digest.update(participant_id.as_bytes());
    digest.update((16_u32).to_be_bytes());
    digest.update(request_id.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// Mirror of Forge's `AgentEventId::for_handoff_completed` pairing: the
/// completed identity is derived from the started identity alone.
fn handoff_completed_event_id(started_event_id: Uuid) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(HANDOFF_COMPLETED_EVENT_DOMAIN);
    digest.update(started_event_id.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// Post-transition membership revision: the departure event chain carries the
/// advancement because `ops_logical_members` pins its canonical revision to 1.
fn participant_departure_revision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<u64, OperationsError> {
    let value: Option<i64> = connection.query_row(
        "SELECT MAX(membership_revision) FROM ops_logical_departure_events WHERE workspace_id=?1 AND participant_id=?2",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    value.map_or(Ok(1), |raw| {
        u64::try_from(raw).map_err(|_| OperationsError::Corrupt("departure membership revision"))
    })
}

fn departure_event_exists(
    connection: &Connection,
    workspace_id: WorkspaceId,
    event_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_logical_departure_events WHERE workspace_id=?1 AND event_id=?2)",
        params![
            workspace_id.into_uuid().as_bytes(),
            event_id.as_bytes(),
        ],
        |row| row.get(0),
    )?)
}

/// One departure participation event: the exact written (or expected) row.
struct DepartureEventRow {
    workspace_id: WorkspaceId,
    event_id: Uuid,
    participant_id: Uuid,
    activity_id: ActivityId,
    event_kind: i64,
    membership_revision: u64,
    request_id: Uuid,
    observed_at: TimestampMillis,
}

fn insert_departure_event(
    transaction: &rusqlite::Transaction<'_>,
    row: &DepartureEventRow,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_logical_departure_events(workspace_id,event_id,participant_id,activity_id,event_kind,membership_revision,request_id,observed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            row.workspace_id.into_uuid().as_bytes(),
            row.event_id.as_bytes(),
            row.participant_id.as_bytes(),
            row.activity_id.into_uuid().as_bytes(),
            row.event_kind,
            crate::catalog::signed(row.membership_revision)?,
            row.request_id.as_bytes(),
            row.observed_at.get(),
        ],
    )?;
    Ok(())
}

/// Absence is the canonical initial presence revision 1 afterwards.
fn clear_presence(
    transaction: &rusqlite::Transaction<'_>,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<bool, OperationsError> {
    let cleared = transaction.execute(
        "DELETE FROM ops_logical_presence WHERE workspace_id=?1 AND participant_id=?2",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant_id.as_bytes(),
        ],
    )?;
    Ok(cleared == 1)
}

/// Record the handoff participation plane: the deterministic
/// `HandoffStarted`/`HandoffCompleted` events, the single unacknowledged
/// target receipt, the source presence clearing, and the journal row. Returns
/// the two deterministic event identities for replay validation.
fn record_handoff_writes(
    transaction: &rusqlite::Transaction<'_>,
    command: &LogicalHandoffCommand,
    source: &DepartureObservation,
    committed_at: TimestampMillis,
) -> Result<(Uuid, Uuid), OperationsError> {
    let started_event_id = departure_event_id(
        HANDOFF_STARTED_EVENT_DOMAIN,
        command.workspace_id(),
        command.participant_id(),
        source.request_id(),
    );
    let completed_event_id = handoff_completed_event_id(started_event_id);
    let started_revision = participant_departure_revision(
        transaction,
        command.workspace_id(),
        command.participant_id(),
    )?
    .checked_add(1)
    .ok_or(OperationsError::Bounds("departure membership revision"))?;
    let completed_revision = started_revision
        .checked_add(1)
        .ok_or(OperationsError::Bounds("departure membership revision"))?;
    for (event_id, event_kind, membership_revision) in [
        (
            started_event_id,
            DEPARTURE_EVENT_HANDOFF_STARTED,
            started_revision,
        ),
        (
            completed_event_id,
            DEPARTURE_EVENT_HANDOFF_COMPLETED,
            completed_revision,
        ),
    ] {
        insert_departure_event(
            transaction,
            &DepartureEventRow {
                workspace_id: command.workspace_id(),
                event_id,
                participant_id: command.participant_id(),
                activity_id: command.activity_id(),
                event_kind,
                membership_revision,
                request_id: source.request_id(),
                observed_at: command.observed_at(),
            },
        )?;
    }
    transaction.execute(
        "INSERT INTO ops_logical_departure_receipts(workspace_id,recipient_participant_id,event_id,receipt_revision,acknowledged_at_ms,committed_at_ms) VALUES(?1,?2,?3,1,NULL,?4)",
        params![
            command.workspace_id().into_uuid().as_bytes(),
            command.target_participant_id().as_bytes(),
            completed_event_id.as_bytes(),
            committed_at.get(),
        ],
    )?;
    let presence_cleared = clear_presence(
        transaction,
        command.workspace_id(),
        command.participant_id(),
    )?;
    insert_departure_journal(
        transaction,
        &handoff_journal_input(
            command,
            source,
            committed_at,
            started_event_id,
            completed_event_id,
            completed_revision,
            presence_cleared,
        ),
    )?;
    Ok((started_event_id, completed_event_id))
}

fn insert_departure_journal(
    transaction: &rusqlite::Transaction<'_>,
    input: &DepartureJournalInput,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_logical_departures(request_id,workspace_id,participant_id,activity_id,agent_id,session_id,kind,expected_agent_revision,result_agent_revision,target_participant_id,target_agent_id,target_session_id,result_coordinator_revision,departed_event_id,started_event_id,completed_event_id,membership_revision,presence_cleared,source_reconciled_at_ms,committed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
        params![
            input.request_id.as_bytes(),
            input.workspace_id.into_uuid().as_bytes(),
            input.participant_id.as_bytes(),
            input.activity_id.into_uuid().as_bytes(),
            input.agent_id.into_uuid().as_bytes(),
            input.session_id.into_uuid().as_bytes(),
            input.kind.encode(),
            input.expected_agent_revision.get(),
            input.result_agent_revision.get(),
            input.target_participant_id.as_ref().map(Uuid::as_bytes),
            input
                .target_agent_id
                .map(|value| value.into_uuid().into_bytes()),
            input
                .target_session_id
                .map(|value| value.into_uuid().into_bytes()),
            input
                .result_coordinator_revision
                .map(forge_domain::ActivityCoordinatorRevision::get),
            input.departed_event_id.as_ref().map(Uuid::as_bytes),
            input.started_event_id.as_ref().map(Uuid::as_bytes),
            input.completed_event_id.as_ref().map(Uuid::as_bytes),
            crate::catalog::signed(input.membership_revision)?,
            i64::from(input.presence_cleared),
            input.source_reconciled_at.get(),
            input.committed_at.get(),
        ],
    )?;
    Ok(())
}

#[derive(Debug)]
struct RawDeparture {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    kind: i64,
    expected_agent_revision: i64,
    result_agent_revision: i64,
    target_participant_id: Option<Vec<u8>>,
    target_agent_id: Option<Vec<u8>>,
    target_session_id: Option<Vec<u8>>,
    result_coordinator_revision: Option<i64>,
    departed_event_id: Option<Uuid>,
    started_event_id: Option<Uuid>,
    completed_event_id: Option<Uuid>,
    membership_revision: i64,
    presence_cleared: i64,
    source_reconciled_at: i64,
    committed_at: i64,
}

fn load_departure(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawDeparture>, OperationsError> {
    connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(workspace_id,1,17),substr(participant_id,1,17),substr(activity_id,1,17),substr(agent_id,1,17),substr(session_id,1,17),kind,expected_agent_revision,result_agent_revision,substr(target_participant_id,1,17),substr(target_agent_id,1,17),substr(target_session_id,1,17),result_coordinator_revision,substr(departed_event_id,1,17),substr(started_event_id,1,17),substr(completed_event_id,1,17),membership_revision,presence_cleared,source_reconciled_at_ms,committed_at_ms FROM ops_logical_departures WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<Vec<u8>>>(9)?,
                    row.get::<_, Option<Vec<u8>>>(10)?,
                    row.get::<_, Option<Vec<u8>>>(11)?,
                    row.get::<_, Option<i64>>(12)?,
                    row.get::<_, Option<Vec<u8>>>(13)?,
                    row.get::<_, Option<Vec<u8>>>(14)?,
                    row.get::<_, Option<Vec<u8>>>(15)?,
                    row.get::<_, i64>(16)?,
                    row.get::<_, i64>(17)?,
                    row.get::<_, i64>(18)?,
                    row.get::<_, i64>(19)?,
                ))
            },
        )
        .optional()?
        .map(
            |(
                request,
                workspace,
                participant,
                activity,
                agent,
                session,
                kind,
                expected,
                result,
                target_participant,
                target_agent,
                target_session,
                coordinator_revision,
                departed,
                started,
                completed,
                membership_revision,
                presence_cleared,
                source_reconciled_at,
                committed_at,
            )| {
                Ok(RawDeparture {
                    request_id: Uuid::from_bytes(wire::id(request)?),
                    workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
                    participant_id: Uuid::from_bytes(wire::id(participant)?),
                    activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
                    agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(agent)?)),
                    session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(session)?)),
                    kind,
                    expected_agent_revision: expected,
                    result_agent_revision: result,
                    target_participant_id: target_participant,
                    target_agent_id: target_agent,
                    target_session_id: target_session,
                    result_coordinator_revision: coordinator_revision,
                    departed_event_id: departed
                        .map(wire::id)
                        .transpose()?
                        .map(Uuid::from_bytes),
                    started_event_id: started
                        .map(wire::id)
                        .transpose()?
                        .map(Uuid::from_bytes),
                    completed_event_id: completed
                        .map(wire::id)
                        .transpose()?
                        .map(Uuid::from_bytes),
                    membership_revision,
                    presence_cleared,
                    source_reconciled_at,
                    committed_at,
                })
            },
        )
        .transpose()
}

/// Read one departure journal row and validate it against its referenced
/// departure events and immutable registration metadata for the exact source
/// binding.
fn decode_departure(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<LogicalDepartureReceipt>, OperationsError> {
    let Some(raw) = load_departure(connection, request_id)? else {
        return Ok(None);
    };
    let kind = LogicalDepartureKind::decode(raw.kind)?;
    let expected_agent_revision = AgentInstanceRevision::new(raw.expected_agent_revision)
        .map_err(|_| OperationsError::Corrupt("departure expected revision"))?;
    let result_agent_revision = AgentInstanceRevision::new(raw.result_agent_revision)
        .map_err(|_| OperationsError::Corrupt("departure result revision"))?;
    if expected_agent_revision.checked_next().ok() != Some(result_agent_revision) {
        return Err(OperationsError::Corrupt("departure revision transition"));
    }
    let membership_revision = u64::try_from(raw.membership_revision)
        .map_err(|_| OperationsError::Corrupt("departure membership revision"))?;
    let presence_cleared = match raw.presence_cleared {
        0 => false,
        1 => true,
        _ => return Err(OperationsError::Corrupt("departure presence flag")),
    };
    if membership_revision < 2 {
        return Err(OperationsError::Corrupt("departure membership revision"));
    }
    validate_departure_events(connection, &raw, kind, membership_revision)?;
    if raw.target_participant_id.is_some() != (kind == LogicalDepartureKind::Handoff)
        || raw.result_coordinator_revision.is_some() != (kind == LogicalDepartureKind::Handoff)
    {
        return Err(OperationsError::Corrupt("departure journal shape"));
    }
    let member = departure_member_identity(connection, &raw)?;
    Ok(Some(LogicalDepartureReceipt {
        request_id: raw.request_id,
        workspace_id: raw.workspace_id,
        participant_id: raw.participant_id,
        activity_id: raw.activity_id,
        agent_id: raw.agent_id,
        session_id: raw.session_id,
        kind,
        expected_agent_revision,
        result_agent_revision,
        target_participant_id: raw
            .target_participant_id
            .map(wire::id)
            .transpose()?
            .map(Uuid::from_bytes),
        target_agent_id: raw
            .target_agent_id
            .map(wire::id)
            .transpose()?
            .map(Uuid::from_bytes)
            .map(AgentInstanceId::from_uuid),
        target_session_id: raw
            .target_session_id
            .map(wire::id)
            .transpose()?
            .map(Uuid::from_bytes)
            .map(AgentSessionId::from_uuid),
        result_coordinator_revision: raw
            .result_coordinator_revision
            .map(forge_domain::ActivityCoordinatorRevision::new)
            .transpose()
            .map_err(|_| OperationsError::Corrupt("departure coordinator revision"))?,
        departed_event_id: raw.departed_event_id,
        handoff_started_event_id: raw.started_event_id,
        handoff_completed_event_id: raw.completed_event_id,
        membership_revision,
        presence_cleared,
        source_reconciled_at: TimestampMillis::new(raw.source_reconciled_at)
            .map_err(|_| OperationsError::Corrupt("departure source timestamp"))?,
        committed_at: TimestampMillis::new(raw.committed_at)
            .map_err(|_| OperationsError::Corrupt("departure commit timestamp"))?,
        display_name: member.0,
        declared_role: member.1,
        transport: member.2,
    }))
}

/// Validate every departure event referenced by the journal row against the
/// exact event the journal describes.
fn validate_departure_events(
    connection: &Connection,
    raw: &RawDeparture,
    kind: LogicalDepartureKind,
    membership_revision: u64,
) -> Result<(), OperationsError> {
    let expectations = match kind {
        LogicalDepartureKind::Detach => vec![DepartureEventExpectation {
            event_id: raw
                .departed_event_id
                .ok_or(OperationsError::Corrupt("departure journal shape"))?,
            event_kind: DEPARTURE_EVENT_LEFT,
            membership_revision,
        }],
        LogicalDepartureKind::Handoff => vec![
            DepartureEventExpectation {
                event_id: raw
                    .started_event_id
                    .ok_or(OperationsError::Corrupt("departure journal shape"))?,
                event_kind: DEPARTURE_EVENT_HANDOFF_STARTED,
                membership_revision: membership_revision
                    .checked_sub(1)
                    .ok_or(OperationsError::Corrupt("departure membership revision"))?,
            },
            DepartureEventExpectation {
                event_id: raw
                    .completed_event_id
                    .ok_or(OperationsError::Corrupt("departure journal shape"))?,
                event_kind: DEPARTURE_EVENT_HANDOFF_COMPLETED,
                membership_revision,
            },
        ],
    };
    for expectation in &expectations {
        validate_stored_event(
            connection,
            raw.workspace_id,
            expectation,
            raw.participant_id,
            raw.activity_id,
            raw.request_id,
        )?;
    }
    Ok(())
}

/// The stored departure event row expected for one journal reference.
struct DepartureEventExpectation {
    event_id: Uuid,
    event_kind: i64,
    membership_revision: u64,
}

/// Validate one referenced departure event row against the journal row.
fn validate_stored_event(
    connection: &Connection,
    workspace_id: WorkspaceId,
    expected: &DepartureEventExpectation,
    participant_id: Uuid,
    activity_id: ActivityId,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(participant_id,1,17),substr(activity_id,1,17),event_kind,membership_revision,substr(request_id,1,17) FROM ops_logical_departure_events WHERE workspace_id=?1 AND event_id=?2 LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                expected.event_id.as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            },
        )
        .optional()?
        .ok_or(OperationsError::Corrupt("departure event missing"))?;
    if Uuid::from_bytes(wire::id(raw.0)?) != participant_id
        || ActivityId::from_uuid(Uuid::from_bytes(wire::id(raw.1)?)) != activity_id
        || raw.2 != expected.event_kind
        || u64::try_from(raw.3).map_err(|_| OperationsError::Corrupt("departure event revision"))?
            != expected.membership_revision
        || Uuid::from_bytes(wire::id(raw.4)?) != request_id
    {
        return Err(OperationsError::Corrupt("departure event identity"));
    }
    Ok(())
}

/// Resolve immutable metadata for the exact registered departure binding.
///
/// Normal registrations retain Forge's activated metadata beside the exact
/// binding request. Profile replacements retain their validated proposal in
/// the replacement-registration row. The current logical-member row is
/// mutable across Session replacement and is not historical receipt authority.
fn departure_member_identity(
    connection: &Connection,
    departure: &RawDeparture,
) -> Result<(AgentDisplayName, AgentDeclaredRole, AgentTransportKind), OperationsError> {
    let normal = normal_departure_member_identity(connection, departure)?;
    let replacement = replacement_departure_member_identity(connection, departure)?;
    match (normal, replacement) {
        (Some(_), Some(_)) => Err(OperationsError::Corrupt(
            "departure member source ambiguous",
        )),
        (Some(identity), None) | (None, Some(identity)) => Ok(identity),
        (None, None) => Err(OperationsError::Corrupt("departure member missing")),
    }
}

fn normal_departure_member_identity(
    connection: &Connection,
    departure: &RawDeparture,
) -> Result<Option<(AgentDisplayName, AgentDeclaredRole, AgentTransportKind)>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(CAST(a.display_name AS BLOB),1,129),substr(CAST(a.declared_role AS BLOB),1,129),a.transport_kind FROM ops_logical_bindings b LEFT JOIN ops_binding_activations a ON a.request_id=b.request_id WHERE b.workspace_id=?1 AND b.participant_id=?2 AND b.activity_id=?3 AND b.agent_id=?4 AND b.session_id=?5 LIMIT 1",
            params![
                departure.workspace_id.into_uuid().as_bytes(),
                departure.participant_id.as_bytes(),
                departure.activity_id.into_uuid().as_bytes(),
                departure.agent_id.into_uuid().as_bytes(),
                departure.session_id.into_uuid().as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let (Some(display_name), Some(declared_role), Some(transport)) = raw else {
        return Err(OperationsError::Corrupt(
            "departure member activation missing",
        ));
    };
    let display_name = AgentDisplayName::new(
        String::from_utf8(display_name)
            .map_err(|_| OperationsError::Corrupt("departure member display"))?,
    )
    .map_err(|_| OperationsError::Corrupt("departure member display"))?;
    let declared_role = AgentDeclaredRole::new(
        String::from_utf8(declared_role)
            .map_err(|_| OperationsError::Corrupt("departure member role"))?,
    )
    .map_err(|_| OperationsError::Corrupt("departure member role"))?;
    Ok(Some((
        display_name,
        declared_role,
        registration::decode_transport(transport)?,
    )))
}

fn replacement_departure_member_identity(
    connection: &Connection,
    departure: &RawDeparture,
) -> Result<Option<(AgentDisplayName, AgentDeclaredRole, AgentTransportKind)>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(request_wire,1,4097) FROM ops_workspace_participation_replacement_registrations WHERE workspace_id=?1 AND participant_id=?2 AND activity_id=?3 AND agent_id=?4 AND new_session_id=?5 AND phase=3 LIMIT 1",
            params![
                departure.workspace_id.into_uuid().as_bytes(),
                departure.participant_id.as_bytes(),
                departure.activity_id.into_uuid().as_bytes(),
                departure.agent_id.into_uuid().as_bytes(),
                departure.session_id.into_uuid().as_bytes(),
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    let Some((request_id, request_wire)) = raw else {
        return Ok(None);
    };
    let request_id = Uuid::from_bytes(wire::id(request_id)?);
    let request = registration::RegistrationRequest::decode(&request_wire)?;
    if request.proposed_participant_id() != departure.participant_id
        || request.binding().request_id() != request_id
        || request.binding().activity_id() != departure.activity_id
        || request.binding().agent_id() != departure.agent_id
        || request.binding().session_id() != departure.session_id
    {
        return Err(OperationsError::Corrupt(
            "departure replacement source identity",
        ));
    }
    Ok(Some((
        request.display_name().clone(),
        request.declared_role().clone(),
        request.transport(),
    )))
}

fn commit_departure(
    transaction: rusqlite::Transaction<'_>,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::DepartureCommitUnconfirmed {
            workspace_id: workspace_id.into_uuid(),
            participant_id,
            source,
        })
}
