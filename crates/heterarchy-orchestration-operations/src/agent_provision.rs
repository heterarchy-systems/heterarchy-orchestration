//! Durable provider-effect journal for autonomous `AgentRequest` resolutions.
//!
//! The journal is intentionally written before any browser/provider effect is
//! released. Once an operation reaches `EffectPending`, callers must keep the
//! same operation identity and inspect/reconcile it rather than allocating a
//! second provider conversation.

use forge_domain::TimestampMillis;
use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{AgentRequestResolutionKind, OperationsCatalog, OperationsError, wire};

const AGENT_PROVISION_OPERATION_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-provision.operation.v1\0";
const MAX_PROVIDER_CONVERSATION_ID_BYTES: usize = 512;
const MIN_AGENT_PROVISION_OPEN_INTERVAL_MS: i64 = 10_000;

const AGENT_PROVISION_OPERATIONS_V29_SQL: &str = r"
CREATE TABLE ops_agent_provision_operations (
    operation_id BLOB PRIMARY KEY NOT NULL CHECK(length(operation_id)=16 AND operation_id != zeroblob(16)),
    request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    slot_ordinal INTEGER NOT NULL CHECK(slot_ordinal BETWEEN 0 AND 3),
    effect_kind INTEGER NOT NULL CHECK(effect_kind BETWEEN 1 AND 3),
    provider_family INTEGER NOT NULL CHECK(provider_family=1),
    profile_id BLOB CHECK(profile_id IS NULL OR (length(profile_id)=16 AND profile_id != zeroblob(16))),
    runtime_session_id BLOB CHECK(runtime_session_id IS NULL OR (length(runtime_session_id)=16 AND runtime_session_id != zeroblob(16))),
    state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 7),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR (length(provider_conversation_id) BETWEEN 1 AND 512)),
    prepared_at_ms INTEGER NOT NULL CHECK(prepared_at_ms>=0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=prepared_at_ms),
    revision INTEGER NOT NULL CHECK(revision>=1),
    UNIQUE(request_id,slot_ordinal),
    FOREIGN KEY(request_id,slot_ordinal) REFERENCES ops_agent_request_resolutions(request_id,slot_ordinal),
    CHECK(
        (effect_kind=1 AND profile_id IS NOT NULL AND runtime_session_id IS NOT NULL)
        OR (effect_kind=2 AND profile_id IS NOT NULL)
        OR effect_kind=3
    ),
    CHECK(state=1 OR (profile_id IS NOT NULL AND runtime_session_id IS NOT NULL)),
    CHECK(
        (state IN(4,5) AND provider_conversation_id IS NOT NULL)
        OR (state NOT IN(4,5) AND provider_conversation_id IS NULL)
    )
);
";

const AGENT_PROVISION_OPERATIONS_SQL: &str = r"
CREATE TABLE ops_agent_provision_operations (
    operation_id BLOB PRIMARY KEY NOT NULL CHECK(length(operation_id)=16 AND operation_id != zeroblob(16)),
    request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    slot_ordinal INTEGER NOT NULL CHECK(slot_ordinal BETWEEN 0 AND 3),
    effect_kind INTEGER NOT NULL CHECK(effect_kind BETWEEN 1 AND 4),
    provider_family INTEGER NOT NULL CHECK(provider_family=1),
    profile_id BLOB CHECK(profile_id IS NULL OR (length(profile_id)=16 AND profile_id != zeroblob(16))),
    runtime_session_id BLOB CHECK(runtime_session_id IS NULL OR (length(runtime_session_id)=16 AND runtime_session_id != zeroblob(16))),
    state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 7),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR (length(provider_conversation_id) BETWEEN 1 AND 512)),
    prepared_at_ms INTEGER NOT NULL CHECK(prepared_at_ms>=0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=prepared_at_ms),
    revision INTEGER NOT NULL CHECK(revision>=1),
    UNIQUE(request_id,slot_ordinal),
    FOREIGN KEY(request_id,slot_ordinal) REFERENCES ops_agent_request_resolutions(request_id,slot_ordinal),
    CHECK(
        (effect_kind IN(1,4) AND profile_id IS NOT NULL AND runtime_session_id IS NOT NULL)
        OR (effect_kind=2 AND profile_id IS NOT NULL)
        OR effect_kind=3
    ),
    CHECK(state=1 OR (profile_id IS NOT NULL AND runtime_session_id IS NOT NULL)),
    CHECK(
        (state IN(4,5) AND provider_conversation_id IS NOT NULL)
        OR (state NOT IN(4,5) AND provider_conversation_id IS NULL)
    )
);
";

const AGENT_PROVISION_FAILURES_SQL: &str = r"
CREATE TABLE ops_agent_provision_failures (
    operation_id BLOB PRIMARY KEY NOT NULL CHECK(length(operation_id)=16 AND operation_id != zeroblob(16)),
    operation_revision INTEGER NOT NULL CHECK(operation_revision>=1),
    failure_code INTEGER NOT NULL CHECK(failure_code BETWEEN 0 AND 17),
    observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0),
    FOREIGN KEY(operation_id) REFERENCES ops_agent_provision_operations(operation_id)
);
";

/// Provider-side effect selected by the durable resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProvisionEffectKind {
    WakeExistingRuntime,
    WakeNewRuntime,
    ProvisionNew,
    ReuseReadyRuntime,
}

/// Initial provider family supported by the provisioner effect contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProvisionProviderFamily {
    ChatGpt,
}

/// Durable effect lifecycle. `EffectPending` and `EffectUnknown` always retain
/// the same operation id; neither state authorizes allocating a new operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProvisionState {
    Prepared,
    EffectPending,
    EffectUnknown,
    Observed,
    Confirmed,
    Failed,
    Cancelled,
}

/// Definite provider-side failure reported after the browser adapter has
/// established that the admitted effect did not succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProvisionFailureCode {
    TabLoadTimeout,
    DocumentObservationFailed,
    DocumentIdentityChanged,
    DocumentIdentityUnavailable,
    ScriptInjectionFailed,
    ComposerUnavailable,
    ComposerFormUnavailable,
    ComposerBusy,
    ComposerNotEmpty,
    ComposerFocusFailed,
    ComposerSelectionFailed,
    ComposerInsertFailed,
    ComposerBodyMismatch,
    SendButtonUnavailable,
    ComposerReadyTimeout,
    UnknownAdapterFailure,
    SurfaceOpenFailed,
    JournalWriteFailed,
}

impl AgentProvisionFailureCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TabLoadTimeout => "tab_load_timeout",
            Self::DocumentObservationFailed => "document_observation_failed",
            Self::DocumentIdentityChanged => "document_identity_changed",
            Self::DocumentIdentityUnavailable => "document_identity_unavailable",
            Self::ScriptInjectionFailed => "script_injection_failed",
            Self::ComposerUnavailable => "composer_unavailable",
            Self::ComposerFormUnavailable => "composer_form_unavailable",
            Self::ComposerBusy => "composer_busy",
            Self::ComposerNotEmpty => "composer_not_empty",
            Self::ComposerFocusFailed => "composer_focus_failed",
            Self::ComposerSelectionFailed => "composer_selection_failed",
            Self::ComposerInsertFailed => "composer_insert_failed",
            Self::ComposerBodyMismatch => "composer_body_mismatch",
            Self::SendButtonUnavailable => "send_button_unavailable",
            Self::ComposerReadyTimeout => "composer_ready_timeout",
            Self::UnknownAdapterFailure => "unknown_adapter_failure",
            Self::SurfaceOpenFailed => "surface_open_failed",
            Self::JournalWriteFailed => "journal_write_failed",
        }
    }

    /// Decode the closed provider wire vocabulary. Uncertain outcomes such as
    /// `click_reply_uncertain` are intentionally rejected.
    ///
    /// # Errors
    /// Returns `OperationsError::Conflict` for any name outside the closed
    /// definite-failure vocabulary.
    pub fn from_wire_name(value: &str) -> Result<Self, OperationsError> {
        match value {
            "tab_load_timeout" => Ok(Self::TabLoadTimeout),
            "document_observation_failed" => Ok(Self::DocumentObservationFailed),
            "document_identity_changed" => Ok(Self::DocumentIdentityChanged),
            "document_identity_unavailable" => Ok(Self::DocumentIdentityUnavailable),
            "script_injection_failed" => Ok(Self::ScriptInjectionFailed),
            "composer_unavailable" => Ok(Self::ComposerUnavailable),
            "composer_form_unavailable" => Ok(Self::ComposerFormUnavailable),
            "composer_busy" => Ok(Self::ComposerBusy),
            "composer_not_empty" => Ok(Self::ComposerNotEmpty),
            "composer_focus_failed" => Ok(Self::ComposerFocusFailed),
            "composer_selection_failed" => Ok(Self::ComposerSelectionFailed),
            "composer_insert_failed" => Ok(Self::ComposerInsertFailed),
            "composer_body_mismatch" => Ok(Self::ComposerBodyMismatch),
            "send_button_unavailable" => Ok(Self::SendButtonUnavailable),
            "composer_ready_timeout" => Ok(Self::ComposerReadyTimeout),
            "unknown_adapter_failure" => Ok(Self::UnknownAdapterFailure),
            "surface_open_failed" => Ok(Self::SurfaceOpenFailed),
            "journal_write_failed" => Ok(Self::JournalWriteFailed),
            _ => Err(OperationsError::Conflict("Agent provision failure code")),
        }
    }
}

/// Transport-neutral action for the provider bridge. `Execute` may run the
/// admitted provider effect once; `Inspect` may only reconcile the same durable
/// operation after an uncertain outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProvisionDispatchAction {
    Execute,
    Inspect,
}

/// Opaque provider-dispatch fence. It intentionally carries only durable
/// routing/effect identity and never `AgentRequest` objective, role, Goal/DAG, or
/// capability text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProvisionDispatch {
    operation_id: Uuid,
    operation_revision: u64,
    action: AgentProvisionDispatchAction,
    effect_kind: AgentProvisionEffectKind,
    provider_family: AgentProvisionProviderFamily,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
}

impl AgentProvisionDispatch {
    #[must_use]
    pub const fn operation_id(self) -> Uuid {
        self.operation_id
    }

    #[must_use]
    pub const fn operation_revision(self) -> u64 {
        self.operation_revision
    }

    #[must_use]
    pub const fn action(self) -> AgentProvisionDispatchAction {
        self.action
    }

    #[must_use]
    pub const fn effect_kind(self) -> AgentProvisionEffectKind {
        self.effect_kind
    }

    #[must_use]
    pub const fn provider_family(self) -> AgentProvisionProviderFamily {
        self.provider_family
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn runtime_session_id(self) -> AgentProfileRuntimeSessionId {
        self.runtime_session_id
    }
}

/// Durable operation that maps one resolver slot to at most one provider
/// conversation identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProvisionOperation {
    operation_id: Uuid,
    request_id: Uuid,
    slot_ordinal: u8,
    effect_kind: AgentProvisionEffectKind,
    provider_family: AgentProvisionProviderFamily,
    profile_id: Option<AgentProfileId>,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    state: AgentProvisionState,
    provider_conversation_id: Option<String>,
    reported_failure_code: Option<AgentProvisionFailureCode>,
    prepared_at: TimestampMillis,
    updated_at: TimestampMillis,
    revision: u64,
}

impl AgentProvisionOperation {
    /// Canonical immutable operation pointer for a request's bounded slot.
    /// This identity grants no dispatch, participation, or repository authority.
    #[must_use]
    pub fn identity_for_slot(request_id: Uuid, slot_ordinal: u8) -> Uuid {
        derived_operation_id(request_id, slot_ordinal)
    }
    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn slot_ordinal(&self) -> u8 {
        self.slot_ordinal
    }

    #[must_use]
    pub const fn effect_kind(&self) -> AgentProvisionEffectKind {
        self.effect_kind
    }

    #[must_use]
    pub const fn provider_family(&self) -> AgentProvisionProviderFamily {
        self.provider_family
    }

    #[must_use]
    pub const fn profile_id(&self) -> Option<AgentProfileId> {
        self.profile_id
    }

    #[must_use]
    pub const fn runtime_session_id(&self) -> Option<AgentProfileRuntimeSessionId> {
        self.runtime_session_id
    }

    #[must_use]
    pub const fn state(&self) -> AgentProvisionState {
        self.state
    }

    #[must_use]
    pub fn provider_conversation_id(&self) -> Option<&str> {
        self.provider_conversation_id.as_deref()
    }

    #[must_use]
    pub const fn reported_failure_code(&self) -> Option<AgentProvisionFailureCode> {
        self.reported_failure_code
    }

    #[must_use]
    pub const fn prepared_at(&self) -> TimestampMillis {
        self.prepared_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Idempotent prepare result. `created=false` means the exact durable operation
/// already existed and must be reused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProvisionPrepareOutcome {
    operation: AgentProvisionOperation,
    created: bool,
}

impl AgentProvisionPrepareOutcome {
    #[must_use]
    pub fn operation(&self) -> &AgentProvisionOperation {
        &self.operation
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

/// Provider release/readback result. Only the transaction that first changes
/// `Prepared -> EffectPending` receives `Execute`; later readback of the same
/// pending/unknown operation is `Inspect`-only so a lost transport response
/// cannot authorize a second provider effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProvisionDispatchOutcome {
    operation: AgentProvisionOperation,
    dispatch: Option<AgentProvisionDispatch>,
}

impl AgentProvisionDispatchOutcome {
    #[must_use]
    pub fn operation(&self) -> &AgentProvisionOperation {
        &self.operation
    }

    #[must_use]
    pub const fn dispatch(&self) -> Option<AgentProvisionDispatch> {
        self.dispatch
    }
}

impl OperationsCatalog {
    /// Observe the current single in-flight operation without releasing Execute.
    ///
    /// # Errors
    /// Rejects multiple in-flight rows, corrupt data or catalog failures.
    pub fn agent_provision_in_flight_operation(
        &self,
    ) -> Result<Option<AgentProvisionOperation>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let mut ids = in_flight_operation_ids(&tx)?;
        if ids.len() > 1 {
            return Err(OperationsError::Corrupt(
                "multiple in-flight provision observations",
            ));
        }
        let operation = ids
            .pop()
            .map(|id| load_operation(&tx, id))
            .transpose()?
            .flatten();
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(operation)
    }

    /// Prepare one durable provider-effect operation from an already reserved
    /// resolver slot. Reuse-only slots cannot enter provider provisioning.
    ///
    /// # Errors
    /// Rejects missing/non-effect resolver slots, malformed identities, and
    /// persistence failures. A commit uncertainty must be recovered by reading
    /// the same deterministic operation identity.
    pub fn prepare_agent_provision_operation(
        &mut self,
        request_id: Uuid,
        slot_ordinal: u8,
        prepared_at: TimestampMillis,
    ) -> Result<AgentProvisionPrepareOutcome, OperationsError> {
        if request_id.is_nil() || slot_ordinal > 3 {
            return Err(OperationsError::Conflict(
                "Agent provision operation identity",
            ));
        }
        let resolution = self
            .agent_request_resolution(request_id)?
            .ok_or(OperationsError::Conflict("AgentRequest resolution missing"))?;
        let selection = resolution
            .slots()
            .iter()
            .copied()
            .find(|slot| slot.selection().slot_ordinal() == slot_ordinal)
            .map(crate::AgentRequestResolutionSlot::selection)
            .ok_or(OperationsError::Conflict(
                "AgentRequest resolution slot missing",
            ))?;
        let (effect_kind, profile_id, runtime_session_id) = match selection.kind() {
            AgentRequestResolutionKind::ReuseReadyRuntime => (
                AgentProvisionEffectKind::ReuseReadyRuntime,
                selection.profile_id(),
                selection.runtime_session_id(),
            ),
            AgentRequestResolutionKind::WakeExistingRuntime => (
                AgentProvisionEffectKind::WakeExistingRuntime,
                selection.profile_id(),
                selection.runtime_session_id(),
            ),
            AgentRequestResolutionKind::WakeNewRuntime => (
                AgentProvisionEffectKind::WakeNewRuntime,
                selection.profile_id(),
                None,
            ),
            AgentRequestResolutionKind::ProvisionNew => {
                (AgentProvisionEffectKind::ProvisionNew, None, None)
            }
            AgentRequestResolutionKind::ReuseCurrentScope => {
                return Err(OperationsError::Conflict(
                    "reuse resolution has no provider provision effect",
                ));
            }
        };
        validate_initial_target(effect_kind, profile_id, runtime_session_id)?;
        let operation_id = derived_operation_id(request_id, slot_ordinal);
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_operation(&tx, operation_id)? {
            validate_prepare_replay(
                &existing,
                request_id,
                slot_ordinal,
                effect_kind,
                profile_id,
                runtime_session_id,
            )?;
            tx.commit()?;
            return Ok(AgentProvisionPrepareOutcome {
                operation: existing,
                created: false,
            });
        }
        if let Some(profile_id) = profile_id {
            crate::profile_deletion::require_profile_admission_open(&tx, profile_id)?;
        }
        tx.execute(
            "INSERT INTO ops_agent_provision_operations(operation_id,request_id,slot_ordinal,effect_kind,provider_family,profile_id,runtime_session_id,state,provider_conversation_id,prepared_at_ms,updated_at_ms,revision) VALUES(?1,?2,?3,?4,1,?5,?6,1,NULL,?7,?7,1)",
            params![
                operation_id.as_bytes(),
                request_id.as_bytes(),
                i64::from(slot_ordinal),
                encode_effect_kind(effect_kind),
                profile_id.map(|id| id.into_uuid().into_bytes().to_vec()),
                runtime_session_id.map(|id| id.into_uuid().into_bytes().to_vec()),
                prepared_at.get(),
            ],
        )?;
        let operation = load_operation(&tx, operation_id)?
            .ok_or(OperationsError::Corrupt("Agent provision insert"))?;
        commit_operation(tx, operation_id)?;
        Ok(AgentProvisionPrepareOutcome {
            operation,
            created: true,
        })
    }

    /// Read one durable provider-effect operation by its stable identity.
    ///
    /// # Errors
    /// Rejects malformed stored rows and database read failures.
    pub fn agent_provision_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<AgentProvisionOperation>, OperationsError> {
        if operation_id.is_nil() {
            return Err(OperationsError::Conflict(
                "Agent provision operation identity",
            ));
        }
        load_operation(&self.connection, operation_id)
    }

    /// Read an exact request/slot operation without preparing an effect or
    /// guessing an operation identity after a lost/partial response.
    ///
    /// # Errors
    /// Rejects invalid selectors and malformed durable operation rows.
    pub fn agent_provision_operation_for_request_slot(
        &self,
        request_id: Uuid,
        slot_ordinal: u8,
    ) -> Result<Option<AgentProvisionOperation>, OperationsError> {
        if request_id.is_nil()
            || slot_ordinal >= heterarchy_orchestration_policy::MAX_AGENT_REQUEST_COUNT
        {
            return Err(OperationsError::Bounds("provision request slot"));
        }
        operation_for_request_slot(&self.connection, request_id, slot_ordinal)
    }

    /// Bind the exact durable Profile/Runtime target before external I/O.
    /// Exact replay returns the already-bound row without increasing revision.
    ///
    /// # Errors
    /// Rejects target changes, non-prepared state, stale revision/time, and
    /// persistence failures.
    pub fn bind_agent_provision_target(
        &mut self,
        operation_id: Uuid,
        expected_revision: u64,
        profile_id: AgentProfileId,
        runtime_session_id: AgentProfileRuntimeSessionId,
        observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = required_operation(&tx, operation_id)?;
        validate_target_binding(&existing, profile_id, runtime_session_id)?;
        if existing.profile_id == Some(profile_id)
            && existing.runtime_session_id == Some(runtime_session_id)
        {
            tx.commit()?;
            return Ok(existing);
        }
        validate_mutation_fence(
            &existing,
            expected_revision,
            observed_at,
            AgentProvisionState::Prepared,
        )?;
        crate::profile_deletion::require_profile_admission_open(&tx, profile_id)?;
        let next_revision = next_revision(existing.revision)?;
        let changed = tx.execute(
            "UPDATE ops_agent_provision_operations SET profile_id=?1,runtime_session_id=?2,updated_at_ms=?3,revision=?4 WHERE operation_id=?5 AND revision=?6 AND state=1",
            params![
                profile_id.into_uuid().as_bytes(),
                runtime_session_id.into_uuid().as_bytes(),
                observed_at.get(),
                signed_revision(next_revision)?,
                operation_id.as_bytes(),
                signed_revision(expected_revision)?,
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("Agent provision target CAS"));
        }
        let operation = load_operation(&tx, operation_id)?
            .ok_or(OperationsError::Corrupt("Agent provision target update"))?;
        commit_operation(tx, operation_id)?;
        Ok(operation)
    }

    /// Select the single backend-admitted provider action. An existing
    /// `EffectPending`/`EffectUnknown` operation is always returned as
    /// `Inspect`; otherwise the oldest fully-bound `Prepared` operation is
    /// atomically admitted and returned once as `Execute`.
    /// Elapsed time cannot prove that a released provider effect failed: retain
    /// its fence until an exact observation or definite failure resolves it.
    ///
    /// # Errors
    /// Rejects corrupt multiple in-flight operations, clock regression, and
    /// persistence failures.
    pub fn next_agent_provision_dispatch(
        &mut self,
        observed_at: TimestampMillis,
    ) -> Result<Option<AgentProvisionDispatchOutcome>, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut in_flight = in_flight_operation_ids(&tx)?;
        if in_flight.len() > 1 {
            return Err(OperationsError::Corrupt(
                "multiple Agent provision operations are in flight",
            ));
        }
        if let Some(operation_id) = in_flight.pop() {
            let operation = load_operation(&tx, operation_id)?.ok_or(OperationsError::Corrupt(
                "Agent provision in-flight readback",
            ))?;
            let dispatch = provider_dispatch(&operation, AgentProvisionDispatchAction::Inspect)?;
            tx.commit()?;
            return Ok(Some(AgentProvisionDispatchOutcome {
                operation,
                dispatch: Some(dispatch),
            }));
        }

        if observed_operation_exists(&tx)? {
            return Err(OperationsError::Conflict(
                "Agent provision observed recovery required",
            ));
        }

        if provider_open_cooldown_active(&tx, observed_at)? {
            tx.commit()?;
            return Ok(None);
        }

        let Some(operation_id) = oldest_bound_prepared_operation_id(&tx)? else {
            tx.commit()?;
            return Ok(None);
        };
        let existing = load_operation(&tx, operation_id)?.ok_or(OperationsError::Corrupt(
            "Agent provision prepared readback",
        ))?;
        validate_mutation_fence(
            &existing,
            existing.revision,
            observed_at,
            AgentProvisionState::Prepared,
        )?;
        let operation = admit_prepared_operation(&tx, &existing, observed_at)?;
        let dispatch = provider_dispatch(&operation, AgentProvisionDispatchAction::Execute)?;
        commit_operation(tx, operation_id)?;
        Ok(Some(AgentProvisionDispatchOutcome {
            operation,
            dispatch: Some(dispatch),
        }))
    }

    /// Mark an uncertain external outcome. Recovery must inspect/reconcile this
    /// same operation identity before deciding whether the provider effect exists.
    ///
    /// # Errors
    /// Rejects invalid lifecycle transitions, stale fences, and persistence failures.
    pub fn mark_agent_provision_effect_unknown(
        &mut self,
        operation_id: Uuid,
        expected_revision: u64,
        observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        let existing = required_operation(&self.connection, operation_id)?;
        if existing.state == AgentProvisionState::EffectUnknown {
            return Ok(existing);
        }
        transition(
            &mut self.connection,
            &existing,
            expected_revision,
            observed_at,
            AgentProvisionState::EffectPending,
            ProvisionTransition::Unknown,
        )
    }

    /// Atomically record the exact provider conversation identity and confirm
    /// the provider effect after the Sessions catalog has already bound that
    /// same identity to the target `RuntimeSession`.
    ///
    /// # Errors
    /// Rejects empty/unbounded identity, identity replacement, stale fences,
    /// invalid lifecycle states, and persistence failures.
    pub fn confirm_agent_provision_conversation(
        &mut self,
        operation_id: Uuid,
        expected_revision: u64,
        provider_conversation_id: &str,
        observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        validate_provider_conversation_id(provider_conversation_id)?;
        let existing = required_operation(&self.connection, operation_id)?;
        if existing.state == AgentProvisionState::Confirmed {
            if existing.provider_conversation_id.as_deref() == Some(provider_conversation_id) {
                return Ok(existing);
            }
            return Err(OperationsError::Conflict(
                "Agent provision provider conversation identity changed",
            ));
        }
        if !matches!(
            existing.state,
            AgentProvisionState::EffectPending | AgentProvisionState::EffectUnknown
        ) {
            return Err(OperationsError::Conflict(
                "Agent provision conversation confirmation state",
            ));
        }
        if existing.revision != expected_revision || observed_at < existing.updated_at {
            return Err(OperationsError::Conflict(
                "Agent provision conversation confirmation fence",
            ));
        }
        let next_revision = next_revision(existing.revision)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE ops_agent_provision_operations
             SET state=5,provider_conversation_id=?1,updated_at_ms=?2,revision=?3
             WHERE operation_id=?4 AND revision=?5 AND state IN(2,3)",
            params![
                provider_conversation_id,
                observed_at.get(),
                signed_revision(next_revision)?,
                operation_id.as_bytes(),
                signed_revision(expected_revision)?,
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict(
                "Agent provision conversation confirmation CAS",
            ));
        }
        let operation = load_operation(&tx, operation_id)?.ok_or(OperationsError::Corrupt(
            "Agent provision conversation confirmation readback",
        ))?;
        commit_operation(tx, operation_id)?;
        Ok(operation)
    }

    /// Persist a definite provider failure. Unknown outcomes must not use this
    /// transition until inspection proves the effect did not succeed.
    ///
    /// # Errors
    /// Rejects invalid lifecycle transitions, stale fences, and persistence failures.
    pub fn fail_agent_provision_effect(
        &mut self,
        operation_id: Uuid,
        expected_revision: u64,
        observed_at: TimestampMillis,
        failure_code: Option<AgentProvisionFailureCode>,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        let existing = required_operation(&self.connection, operation_id)?;
        if existing.state == AgentProvisionState::Failed {
            let Some(replayed_revision) = expected_revision.checked_add(1) else {
                return Err(OperationsError::Conflict(
                    "Agent provision failure replay fence",
                ));
            };
            if replayed_revision != existing.revision
                || failure_code != existing.reported_failure_code
            {
                return Err(OperationsError::Conflict("Agent provision failure replay"));
            }
            return Ok(existing);
        }
        let expected_state = match existing.state {
            AgentProvisionState::EffectPending => AgentProvisionState::EffectPending,
            AgentProvisionState::EffectUnknown => AgentProvisionState::EffectUnknown,
            _ => return Err(OperationsError::Conflict("Agent provision failure state")),
        };
        transition(
            &mut self.connection,
            &existing,
            expected_revision,
            observed_at,
            expected_state,
            ProvisionTransition::Failed(failure_code),
        )
    }

    /// Cancel an operation only before external effects are released.
    ///
    /// # Errors
    /// Rejects post-release cancellation, stale fences, and persistence failures.
    pub fn cancel_agent_provision_operation(
        &mut self,
        operation_id: Uuid,
        expected_revision: u64,
        observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        let existing = required_operation(&self.connection, operation_id)?;
        if existing.state == AgentProvisionState::Cancelled {
            return Ok(existing);
        }
        transition(
            &mut self.connection,
            &existing,
            expected_revision,
            observed_at,
            AgentProvisionState::Prepared,
            ProvisionTransition::Cancelled,
        )
    }
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(AGENT_PROVISION_OPERATIONS_SQL)?;
    Ok(())
}

pub(crate) fn create_failure_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(AGENT_PROVISION_FAILURES_SQL)?;
    Ok(())
}

pub(crate) fn migrate_v29_to_v30(
    connection: &rusqlite::Transaction<'_>,
) -> Result<(), OperationsError> {
    connection.execute_batch(
        "ALTER TABLE ops_agent_provision_operations
             RENAME TO ops_agent_provision_operations_v29;",
    )?;
    connection.execute_batch(AGENT_PROVISION_OPERATIONS_SQL)?;
    connection.execute_batch(
        "INSERT INTO ops_agent_provision_operations(
            operation_id,request_id,slot_ordinal,effect_kind,provider_family,
            profile_id,runtime_session_id,state,provider_conversation_id,
            prepared_at_ms,updated_at_ms,revision
         )
         SELECT operation_id,request_id,slot_ordinal,effect_kind,provider_family,
                profile_id,runtime_session_id,state,provider_conversation_id,
                prepared_at_ms,updated_at_ms,revision
         FROM ops_agent_provision_operations_v29;
         DROP TABLE ops_agent_provision_operations_v29;",
    )?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    crate::catalog::drop_v31_objects_for_legacy_fixture(connection)?;
    // The node-team bindings table only exists from v34 on; every legacy
    // fixture stamped below v34 must not retain it, or the exact-inventory
    // validation rejects the simulated catalog.
    connection.execute_batch(
        "DROP TABLE IF EXISTS ops_dag_node_team_bindings;
         DROP TABLE IF EXISTS ops_agent_provision_failures;
         DROP TABLE IF EXISTS ops_agent_provision_operations;",
    )?;
    Ok(())
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_agent_provision_operations",
        AGENT_PROVISION_OPERATIONS_SQL,
    )]
}

pub(crate) fn failure_schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_agent_provision_failures",
        AGENT_PROVISION_FAILURES_SQL,
    )]
}

pub(crate) fn schema_objects_v29() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_agent_provision_operations",
        AGENT_PROVISION_OPERATIONS_V29_SQL,
    )]
}

#[derive(Debug, Clone, Copy)]
enum ProvisionTransition {
    Unknown,
    Cancelled,
    Failed(Option<AgentProvisionFailureCode>),
}

fn transition(
    connection: &mut Connection,
    existing: &AgentProvisionOperation,
    expected_revision: u64,
    observed_at: TimestampMillis,
    expected_state: AgentProvisionState,
    transition: ProvisionTransition,
) -> Result<AgentProvisionOperation, OperationsError> {
    let (next_state, failure_code) = match transition {
        ProvisionTransition::Unknown => (AgentProvisionState::EffectUnknown, None),
        ProvisionTransition::Cancelled => (AgentProvisionState::Cancelled, None),
        ProvisionTransition::Failed(code) => (AgentProvisionState::Failed, code),
    };
    validate_mutation_fence(existing, expected_revision, observed_at, expected_state)?;
    let next_revision = next_revision(existing.revision)?;
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = tx.execute(
        "UPDATE ops_agent_provision_operations SET state=?1,provider_conversation_id=?2,updated_at_ms=?3,revision=?4 WHERE operation_id=?5 AND revision=?6 AND state=?7",
        params![
            encode_state(next_state),
            Option::<&str>::None,
            observed_at.get(),
            signed_revision(next_revision)?,
            existing.operation_id.as_bytes(),
            signed_revision(expected_revision)?,
            encode_state(expected_state),
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("Agent provision transition CAS"));
    }
    if let Some(failure_code) = failure_code {
        tx.execute(
            "INSERT INTO ops_agent_provision_failures(operation_id,operation_revision,failure_code,observed_at_ms) VALUES(?1,?2,?3,?4)",
            params![
                existing.operation_id.as_bytes(),
                signed_revision(next_revision)?,
                encode_failure_code(failure_code),
                observed_at.get(),
            ],
        )?;
    }
    let operation = load_operation(&tx, existing.operation_id)?.ok_or(OperationsError::Corrupt(
        "Agent provision transition readback",
    ))?;
    commit_operation(tx, existing.operation_id)?;
    Ok(operation)
}

fn validate_mutation_fence(
    operation: &AgentProvisionOperation,
    expected_revision: u64,
    observed_at: TimestampMillis,
    expected_state: AgentProvisionState,
) -> Result<(), OperationsError> {
    if expected_revision == 0
        || operation.revision != expected_revision
        || operation.state != expected_state
    {
        return Err(OperationsError::Conflict("Agent provision mutation fence"));
    }
    if observed_at < operation.updated_at {
        return Err(OperationsError::Conflict(
            "Agent provision observation time regression",
        ));
    }
    Ok(())
}

fn validate_initial_target(
    effect_kind: AgentProvisionEffectKind,
    profile_id: Option<AgentProfileId>,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
) -> Result<(), OperationsError> {
    let valid = match effect_kind {
        AgentProvisionEffectKind::WakeExistingRuntime
        | AgentProvisionEffectKind::ReuseReadyRuntime => {
            profile_id.is_some() && runtime_session_id.is_some()
        }
        AgentProvisionEffectKind::WakeNewRuntime => {
            profile_id.is_some() && runtime_session_id.is_none()
        }
        AgentProvisionEffectKind::ProvisionNew => {
            profile_id.is_none() && runtime_session_id.is_none()
        }
    };
    if !valid {
        return Err(OperationsError::Corrupt("Agent provision initial target"));
    }
    Ok(())
}

fn validate_target_binding(
    operation: &AgentProvisionOperation,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
) -> Result<(), OperationsError> {
    if operation.state != AgentProvisionState::Prepared {
        return Err(OperationsError::Conflict("Agent provision target state"));
    }
    if operation.profile_id.is_some_and(|id| id != profile_id)
        || operation
            .runtime_session_id
            .is_some_and(|id| id != runtime_session_id)
    {
        return Err(OperationsError::Conflict("Agent provision target changed"));
    }
    Ok(())
}

fn validate_prepare_replay(
    operation: &AgentProvisionOperation,
    request_id: Uuid,
    slot_ordinal: u8,
    effect_kind: AgentProvisionEffectKind,
    profile_id: Option<AgentProfileId>,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
) -> Result<(), OperationsError> {
    if operation.request_id != request_id
        || operation.slot_ordinal != slot_ordinal
        || operation.effect_kind != effect_kind
        || (profile_id.is_some() && operation.profile_id != profile_id)
        || (runtime_session_id.is_some() && operation.runtime_session_id != runtime_session_id)
    {
        return Err(OperationsError::Conflict("Agent provision prepare replay"));
    }
    Ok(())
}

fn validate_provider_conversation_id(value: &str) -> Result<(), OperationsError> {
    if value.is_empty() || value.len() > MAX_PROVIDER_CONVERSATION_ID_BYTES {
        return Err(OperationsError::Bounds("provider conversation identity"));
    }
    Ok(())
}

fn provider_dispatch(
    operation: &AgentProvisionOperation,
    action: AgentProvisionDispatchAction,
) -> Result<AgentProvisionDispatch, OperationsError> {
    let profile_id = operation.profile_id.ok_or(OperationsError::Corrupt(
        "Agent provision dispatch Profile target",
    ))?;
    let runtime_session_id = operation
        .runtime_session_id
        .ok_or(OperationsError::Corrupt(
            "Agent provision dispatch RuntimeSession target",
        ))?;
    Ok(AgentProvisionDispatch {
        operation_id: operation.operation_id,
        operation_revision: operation.revision,
        action,
        effect_kind: operation.effect_kind,
        provider_family: operation.provider_family,
        profile_id,
        runtime_session_id,
    })
}

fn in_flight_operation_ids(connection: &Connection) -> Result<Vec<Uuid>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT operation_id FROM ops_agent_provision_operations
         WHERE state IN(2,3)
         ORDER BY updated_at_ms ASC, operation_id ASC
         LIMIT 2",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    rows.map(|row| {
        let bytes = wire::id(row?)?;
        Ok(Uuid::from_bytes(bytes))
    })
    .collect()
}

fn observed_operation_exists(connection: &Connection) -> Result<bool, OperationsError> {
    let exists: i64 = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_agent_provision_operations WHERE state=4 LIMIT 1)",
        [],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn oldest_bound_prepared_operation_id(
    connection: &Connection,
) -> Result<Option<Uuid>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT operation.operation_id FROM ops_agent_provision_operations AS operation
             WHERE operation.state=1 AND operation.profile_id IS NOT NULL
               AND operation.runtime_session_id IS NOT NULL
               AND NOT EXISTS(
                    SELECT 1 FROM ops_agent_profile_deletion_barriers AS barrier
                     WHERE barrier.profile_id=operation.profile_id
               )
             ORDER BY operation.prepared_at_ms ASC, operation.operation_id ASC
             LIMIT 1",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    raw.map(|value| wire::id(value).map(Uuid::from_bytes))
        .transpose()
}

fn provider_open_cooldown_active(
    connection: &Connection,
    observed_at: TimestampMillis,
) -> Result<bool, OperationsError> {
    let last_effect_at: Option<i64> = connection.query_row(
        "SELECT MAX(updated_at_ms) FROM ops_agent_provision_operations WHERE state IN(4,5,6)",
        [],
        |row| row.get(0),
    )?;
    let Some(last_effect_at) = last_effect_at else {
        return Ok(false);
    };
    let last_effect_at = TimestampMillis::new(last_effect_at)
        .map_err(|_| OperationsError::Corrupt("Agent provision cooldown timestamp"))?;
    if observed_at < last_effect_at {
        return Err(OperationsError::Conflict(
            "Agent provision admission clock regression",
        ));
    }
    Ok(observed_at.get() - last_effect_at.get() < MIN_AGENT_PROVISION_OPEN_INTERVAL_MS)
}

fn admit_prepared_operation(
    transaction: &rusqlite::Transaction<'_>,
    existing: &AgentProvisionOperation,
    observed_at: TimestampMillis,
) -> Result<AgentProvisionOperation, OperationsError> {
    let next_revision = next_revision(existing.revision)?;
    let changed = transaction.execute(
        "UPDATE ops_agent_provision_operations SET state=2,updated_at_ms=?1,revision=?2 WHERE operation_id=?3 AND revision=?4 AND state=1",
        params![
            observed_at.get(),
            signed_revision(next_revision)?,
            existing.operation_id.as_bytes(),
            signed_revision(existing.revision)?,
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("Agent provision release CAS"));
    }
    load_operation(transaction, existing.operation_id)?
        .ok_or(OperationsError::Corrupt("Agent provision release readback"))
}

fn required_operation(
    connection: &Connection,
    operation_id: Uuid,
) -> Result<AgentProvisionOperation, OperationsError> {
    if operation_id.is_nil() {
        return Err(OperationsError::Conflict(
            "Agent provision operation identity",
        ));
    }
    load_operation(connection, operation_id)?.ok_or(OperationsError::Conflict(
        "Agent provision operation missing",
    ))
}

fn load_operation(
    connection: &Connection,
    operation_id: Uuid,
) -> Result<Option<AgentProvisionOperation>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(p.request_id,1,17),p.slot_ordinal,p.effect_kind,p.provider_family,substr(p.profile_id,1,17),substr(p.runtime_session_id,1,17),p.state,substr(p.provider_conversation_id,1,513),p.prepared_at_ms,p.updated_at_ms,p.revision,f.operation_revision,f.failure_code,f.observed_at_ms
             FROM ops_agent_provision_operations AS p
             LEFT JOIN ops_agent_provision_failures AS f ON f.operation_id=p.operation_id
             WHERE p.operation_id=?1",
            params![operation_id.as_bytes()],
            |row| {
                Ok(RawAgentProvisionOperation {
                    request_id: row.get(0)?,
                    slot_ordinal: row.get(1)?,
                    effect_kind: row.get(2)?,
                    provider_family: row.get(3)?,
                    profile_id: row.get(4)?,
                    runtime_session_id: row.get(5)?,
                    state: row.get(6)?,
                    provider_conversation_id: row.get(7)?,
                    prepared_at: row.get(8)?,
                    updated_at: row.get(9)?,
                    revision: row.get(10)?,
                    failure_operation_revision: row.get(11)?,
                    failure_code: row.get(12)?,
                    failure_observed_at: row.get(13)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| decode_operation(operation_id, raw))
        .transpose()
}

pub(crate) fn operation_for_request_slot(
    connection: &Connection,
    request_id: Uuid,
    slot_ordinal: u8,
) -> Result<Option<AgentProvisionOperation>, OperationsError> {
    if request_id.is_nil() || slot_ordinal > 3 {
        return Err(OperationsError::Conflict(
            "Agent provision operation identity",
        ));
    }
    let operation_id = derived_operation_id(request_id, slot_ordinal);
    let operation = load_operation(connection, operation_id)?;
    if let Some(operation) = operation.as_ref()
        && (operation.request_id() != request_id || operation.slot_ordinal() != slot_ordinal)
    {
        return Err(OperationsError::Corrupt(
            "Agent provision operation request/slot identity",
        ));
    }
    Ok(operation)
}

struct RawAgentProvisionOperation {
    request_id: Vec<u8>,
    slot_ordinal: i64,
    effect_kind: i64,
    provider_family: i64,
    profile_id: Option<Vec<u8>>,
    runtime_session_id: Option<Vec<u8>>,
    state: i64,
    provider_conversation_id: Option<String>,
    prepared_at: i64,
    updated_at: i64,
    revision: i64,
    failure_operation_revision: Option<i64>,
    failure_code: Option<i64>,
    failure_observed_at: Option<i64>,
}

fn decode_operation(
    operation_id: Uuid,
    raw: RawAgentProvisionOperation,
) -> Result<AgentProvisionOperation, OperationsError> {
    let slot_ordinal = u8::try_from(raw.slot_ordinal)
        .map_err(|_| OperationsError::Corrupt("Agent provision slot ordinal"))?;
    if slot_ordinal > 3 {
        return Err(OperationsError::Corrupt("Agent provision slot ordinal"));
    }
    let effect_kind = decode_effect_kind(raw.effect_kind)?;
    let provider_family = decode_provider_family(raw.provider_family)?;
    let profile_id = raw
        .profile_id
        .map(|value| {
            AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(value)?))
                .map_err(|_| OperationsError::Corrupt("Agent provision Profile id"))
        })
        .transpose()?;
    let runtime_session_id = raw
        .runtime_session_id
        .map(|value| {
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(wire::id(value)?))
                .map_err(|_| OperationsError::Corrupt("Agent provision RuntimeSession id"))
        })
        .transpose()?;
    let state = decode_state(raw.state)?;
    if let Some(value) = raw.provider_conversation_id.as_deref() {
        validate_provider_conversation_id(value)?;
    }
    let prepared_at = TimestampMillis::new(raw.prepared_at)
        .map_err(|_| OperationsError::Corrupt("Agent provision prepared time"))?;
    let updated_at = TimestampMillis::new(raw.updated_at)
        .map_err(|_| OperationsError::Corrupt("Agent provision updated time"))?;
    let revision = u64::try_from(raw.revision)
        .map_err(|_| OperationsError::Corrupt("Agent provision revision"))?;
    if revision == 0 || updated_at < prepared_at {
        return Err(OperationsError::Corrupt("Agent provision row shape"));
    }
    validate_initial_or_bound_target(effect_kind, profile_id, runtime_session_id, state)?;
    let has_conversation = raw.provider_conversation_id.is_some();
    if has_conversation
        != matches!(
            state,
            AgentProvisionState::Observed | AgentProvisionState::Confirmed
        )
    {
        return Err(OperationsError::Corrupt(
            "Agent provision conversation/state shape",
        ));
    }
    let reported_failure_code = match (
        raw.failure_operation_revision,
        raw.failure_code,
        raw.failure_observed_at,
    ) {
        (None, None, None) => None,
        (Some(failure_revision), Some(failure_code), Some(failure_observed_at)) => {
            if state != AgentProvisionState::Failed
                || failure_revision != raw.revision
                || failure_observed_at != raw.updated_at
            {
                return Err(OperationsError::Corrupt(
                    "Agent provision failure diagnostic shape",
                ));
            }
            Some(decode_failure_code(failure_code)?)
        }
        _ => {
            return Err(OperationsError::Corrupt(
                "Agent provision failure diagnostic shape",
            ));
        }
    };
    Ok(AgentProvisionOperation {
        operation_id,
        request_id: Uuid::from_bytes(wire::id(raw.request_id)?),
        slot_ordinal,
        effect_kind,
        provider_family,
        profile_id,
        runtime_session_id,
        state,
        provider_conversation_id: raw.provider_conversation_id,
        reported_failure_code,
        prepared_at,
        updated_at,
        revision,
    })
}

fn validate_initial_or_bound_target(
    effect_kind: AgentProvisionEffectKind,
    profile_id: Option<AgentProfileId>,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    state: AgentProvisionState,
) -> Result<(), OperationsError> {
    if state != AgentProvisionState::Prepared
        && (profile_id.is_none() || runtime_session_id.is_none())
    {
        return Err(OperationsError::Corrupt("Agent provision released target"));
    }
    match effect_kind {
        AgentProvisionEffectKind::WakeExistingRuntime
        | AgentProvisionEffectKind::ReuseReadyRuntime => {
            if profile_id.is_none() || runtime_session_id.is_none() {
                return Err(OperationsError::Corrupt(
                    "Agent provision existing-runtime target",
                ));
            }
        }
        AgentProvisionEffectKind::WakeNewRuntime => {
            if profile_id.is_none() {
                return Err(OperationsError::Corrupt("Agent provision wake-new target"));
            }
        }
        AgentProvisionEffectKind::ProvisionNew => {}
    }
    Ok(())
}

fn derived_operation_id(request_id: Uuid, slot_ordinal: u8) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(AGENT_PROVISION_OPERATION_DOMAIN);
    hasher.update(request_id.as_bytes());
    hasher.update([slot_ordinal]);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

const fn encode_effect_kind(value: AgentProvisionEffectKind) -> i64 {
    match value {
        AgentProvisionEffectKind::WakeExistingRuntime => 1,
        AgentProvisionEffectKind::WakeNewRuntime => 2,
        AgentProvisionEffectKind::ProvisionNew => 3,
        AgentProvisionEffectKind::ReuseReadyRuntime => 4,
    }
}

fn decode_effect_kind(value: i64) -> Result<AgentProvisionEffectKind, OperationsError> {
    match value {
        1 => Ok(AgentProvisionEffectKind::WakeExistingRuntime),
        2 => Ok(AgentProvisionEffectKind::WakeNewRuntime),
        3 => Ok(AgentProvisionEffectKind::ProvisionNew),
        4 => Ok(AgentProvisionEffectKind::ReuseReadyRuntime),
        _ => Err(OperationsError::Corrupt("Agent provision effect kind")),
    }
}

const fn encode_state(value: AgentProvisionState) -> i64 {
    match value {
        AgentProvisionState::Prepared => 1,
        AgentProvisionState::EffectPending => 2,
        AgentProvisionState::EffectUnknown => 3,
        AgentProvisionState::Observed => 4,
        AgentProvisionState::Confirmed => 5,
        AgentProvisionState::Failed => 6,
        AgentProvisionState::Cancelled => 7,
    }
}

fn decode_state(value: i64) -> Result<AgentProvisionState, OperationsError> {
    match value {
        1 => Ok(AgentProvisionState::Prepared),
        2 => Ok(AgentProvisionState::EffectPending),
        3 => Ok(AgentProvisionState::EffectUnknown),
        4 => Ok(AgentProvisionState::Observed),
        5 => Ok(AgentProvisionState::Confirmed),
        6 => Ok(AgentProvisionState::Failed),
        7 => Ok(AgentProvisionState::Cancelled),
        _ => Err(OperationsError::Corrupt("Agent provision state")),
    }
}

const fn decode_provider_family(
    value: i64,
) -> Result<AgentProvisionProviderFamily, OperationsError> {
    match value {
        1 => Ok(AgentProvisionProviderFamily::ChatGpt),
        _ => Err(OperationsError::Corrupt("Agent provision provider family")),
    }
}

const fn encode_failure_code(value: AgentProvisionFailureCode) -> i64 {
    match value {
        AgentProvisionFailureCode::TabLoadTimeout => 0,
        AgentProvisionFailureCode::DocumentObservationFailed => 1,
        AgentProvisionFailureCode::DocumentIdentityChanged => 2,
        AgentProvisionFailureCode::DocumentIdentityUnavailable => 3,
        AgentProvisionFailureCode::ScriptInjectionFailed => 4,
        AgentProvisionFailureCode::ComposerUnavailable => 5,
        AgentProvisionFailureCode::ComposerFormUnavailable => 6,
        AgentProvisionFailureCode::ComposerBusy => 7,
        AgentProvisionFailureCode::ComposerNotEmpty => 8,
        AgentProvisionFailureCode::ComposerFocusFailed => 9,
        AgentProvisionFailureCode::ComposerSelectionFailed => 10,
        AgentProvisionFailureCode::ComposerInsertFailed => 11,
        AgentProvisionFailureCode::ComposerBodyMismatch => 12,
        AgentProvisionFailureCode::SendButtonUnavailable => 13,
        AgentProvisionFailureCode::ComposerReadyTimeout => 14,
        AgentProvisionFailureCode::UnknownAdapterFailure => 15,
        AgentProvisionFailureCode::SurfaceOpenFailed => 16,
        AgentProvisionFailureCode::JournalWriteFailed => 17,
    }
}

fn decode_failure_code(value: i64) -> Result<AgentProvisionFailureCode, OperationsError> {
    match value {
        0 => Ok(AgentProvisionFailureCode::TabLoadTimeout),
        1 => Ok(AgentProvisionFailureCode::DocumentObservationFailed),
        2 => Ok(AgentProvisionFailureCode::DocumentIdentityChanged),
        3 => Ok(AgentProvisionFailureCode::DocumentIdentityUnavailable),
        4 => Ok(AgentProvisionFailureCode::ScriptInjectionFailed),
        5 => Ok(AgentProvisionFailureCode::ComposerUnavailable),
        6 => Ok(AgentProvisionFailureCode::ComposerFormUnavailable),
        7 => Ok(AgentProvisionFailureCode::ComposerBusy),
        8 => Ok(AgentProvisionFailureCode::ComposerNotEmpty),
        9 => Ok(AgentProvisionFailureCode::ComposerFocusFailed),
        10 => Ok(AgentProvisionFailureCode::ComposerSelectionFailed),
        11 => Ok(AgentProvisionFailureCode::ComposerInsertFailed),
        12 => Ok(AgentProvisionFailureCode::ComposerBodyMismatch),
        13 => Ok(AgentProvisionFailureCode::SendButtonUnavailable),
        14 => Ok(AgentProvisionFailureCode::ComposerReadyTimeout),
        15 => Ok(AgentProvisionFailureCode::UnknownAdapterFailure),
        16 => Ok(AgentProvisionFailureCode::SurfaceOpenFailed),
        17 => Ok(AgentProvisionFailureCode::JournalWriteFailed),
        _ => Err(OperationsError::Corrupt("Agent provision failure code")),
    }
}

fn next_revision(current: u64) -> Result<u64, OperationsError> {
    current
        .checked_add(1)
        .ok_or(OperationsError::Bounds("Agent provision revision"))
}

fn signed_revision(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("Agent provision revision"))
}

fn commit_operation(
    transaction: rusqlite::Transaction<'_>,
    operation_id: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::AgentProvisionCommitUnconfirmed {
            operation_id,
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentRequestCommand, AgentRequestEffectScope, AgentRequestIndependence, AgentRequestIntent,
        AgentRequestRequester, AgentRequestResolutionSelection, CommitAgentRequestResolution,
    };
    use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
    use heterarchy_orchestration_sessions::{
        AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileDeletionCommand,
        AgentProfileDeletionIntent, AgentProfileProviderFamily, CreateAgentProfile,
    };
    use std::{error::Error, fs};

    fn seed_v27_provision_resolution(
        path: &std::path::Path,
        destination: Uuid,
        request_id: Uuid,
    ) -> Result<(), Box<dyn Error>> {
        let mut catalog = OperationsCatalog::open(path, destination)?;
        let request = AgentRequestCommand::new(
            request_id,
            AgentRequestRequester::new(
                WorkspaceId::from_uuid(Uuid::from_u128(0xE803)),
                ActivityId::from_uuid(Uuid::from_u128(0xE804)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xE805)),
                AgentSessionId::from_uuid(Uuid::from_u128(0xE806)),
                1,
            )?,
            AgentRequestIntent::new(
                "provision verifier".to_owned(),
                "verification".to_owned(),
                1,
                AgentRequestEffectScope::ReadOnly,
                AgentRequestIndependence::None,
                "provider effect recovery".to_owned(),
            )?,
            TimestampMillis::new(10)?,
        )?;
        let _ = catalog.submit_agent_request(&request)?;
        let resolution = CommitAgentRequestResolution::new(
            request_id,
            1,
            1,
            vec![AgentRequestResolutionSelection::provision_new(0)?],
            TimestampMillis::new(20)?,
        )?;
        let _ = catalog.commit_agent_request_resolution(&resolution)?;
        drop(catalog);

        let connection = Connection::open(path)?;
        crate::catalog::drop_v31_objects_for_legacy_fixture(&connection)?;
        connection.execute_batch(
            "DROP TABLE IF EXISTS ops_dag_node_team_bindings;
             DROP TABLE ops_agent_request_fulfillments;
             DROP TABLE IF EXISTS ops_agent_provision_failures;
             DROP TABLE ops_agent_provision_operations;
             PRAGMA user_version=27;",
        )?;
        Ok(())
    }

    /// Rebuild the provision table at its exact v29 shape with all rows
    /// preserved, then stamp the requested catalog version, so the fixture
    /// matches what v28/v29 code actually left on disk.
    fn downgrade_catalog_to_v29_provision(
        path: &std::path::Path,
        drop_fulfillments: bool,
        version: i64,
    ) -> Result<(), Box<dyn Error>> {
        let connection = Connection::open(path)?;
        crate::catalog::drop_v31_objects_for_legacy_fixture(&connection)?;
        connection.execute_batch(
            "DROP TABLE IF EXISTS ops_dag_node_team_bindings;
             DROP TABLE IF EXISTS ops_agent_provision_failures;
             ALTER TABLE ops_agent_provision_operations
                 RENAME TO ops_agent_provision_operations_v30_fixture;",
        )?;
        if drop_fulfillments {
            connection.execute_batch("DROP TABLE ops_agent_request_fulfillments;")?;
        }
        connection.execute_batch(AGENT_PROVISION_OPERATIONS_V29_SQL)?;
        connection.execute_batch(
            "INSERT INTO ops_agent_provision_operations(
                operation_id,request_id,slot_ordinal,effect_kind,provider_family,
                profile_id,runtime_session_id,state,provider_conversation_id,
                prepared_at_ms,updated_at_ms,revision
             )
             SELECT operation_id,request_id,slot_ordinal,effect_kind,provider_family,
                    profile_id,runtime_session_id,state,provider_conversation_id,
                    prepared_at_ms,updated_at_ms,revision
             FROM ops_agent_provision_operations_v30_fixture;
             DROP TABLE ops_agent_provision_operations_v30_fixture;",
        )?;
        connection.pragma_update(None, "user_version", version)?;
        Ok(())
    }

    fn downgrade_catalog_to_v32(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
        let connection = Connection::open(path)?;
        crate::catalog::restore_v34_shapes_for_legacy_fixture(&connection)?;
        connection.execute_batch(
            "DROP TABLE IF EXISTS ops_dag_node_team_bindings;
             DROP TABLE ops_agent_provision_failures;
             PRAGMA user_version=32;",
        )?;
        Ok(())
    }

    fn prepare_new_provision_operation(
        catalog: &mut OperationsCatalog,
        request_id: Uuid,
        base_time_ms: i64,
    ) -> Result<Uuid, Box<dyn Error>> {
        let request = AgentRequestCommand::new(
            request_id,
            AgentRequestRequester::new(
                WorkspaceId::from_uuid(Uuid::from_u128(0xE803)),
                ActivityId::from_uuid(Uuid::from_u128(0xE804)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xE805)),
                AgentSessionId::from_uuid(Uuid::from_u128(0xE806)),
                1,
            )?,
            AgentRequestIntent::new(
                "provision verifier".to_owned(),
                "verification".to_owned(),
                1,
                AgentRequestEffectScope::ReadOnly,
                AgentRequestIndependence::None,
                "global provider admission".to_owned(),
            )?,
            TimestampMillis::new(base_time_ms)?,
        )?;
        let _ = catalog.submit_agent_request(&request)?;
        let resolution = CommitAgentRequestResolution::new(
            request_id,
            1,
            1,
            vec![AgentRequestResolutionSelection::provision_new(0)?],
            TimestampMillis::new(base_time_ms + 1)?,
        )?;
        let _ = catalog.commit_agent_request_resolution(&resolution)?;
        let prepared = catalog.prepare_agent_provision_operation(
            request_id,
            0,
            TimestampMillis::new(base_time_ms + 2)?,
        )?;
        Ok(prepared.operation().operation_id())
    }

    fn create_bound_provision_operation(
        catalog: &mut OperationsCatalog,
        request_id: Uuid,
        profile_id: AgentProfileId,
        runtime_id: AgentProfileRuntimeSessionId,
        base_time_ms: i64,
    ) -> Result<Uuid, Box<dyn Error>> {
        let operation_id = prepare_new_provision_operation(catalog, request_id, base_time_ms)?;
        let operation = catalog
            .agent_provision_operation(operation_id)?
            .ok_or("prepared operation missing")?;
        let _ = catalog.bind_agent_provision_target(
            operation_id,
            operation.revision(),
            profile_id,
            runtime_id,
            TimestampMillis::new(base_time_ms + 3)?,
        )?;
        Ok(operation_id)
    }

    fn begin_profile_deletion_intent(
        profile_id: AgentProfileId,
        path: &std::path::Path,
        request_id: Uuid,
        requested_at: i64,
    ) -> Result<(AgentProfileCatalog, AgentProfileDeletionIntent), Box<dyn Error>> {
        let mut profiles = AgentProfileCatalog::open(path)?;
        let created = CreateAgentProfile::new(
            profile_id,
            Uuid::now_v7(),
            "profile-delete-fixture".to_owned(),
            "Profile deletion fixture".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            None,
            None,
            Vec::new(),
            AgentProfileCreationOrigin::UserManual,
            TimestampMillis::new(requested_at - 1)?,
        )?;
        let profile = profiles.create_profile(&created)?;
        let command = AgentProfileDeletionCommand::new(
            request_id,
            profile_id,
            profile.profile().revision(),
            TimestampMillis::new(requested_at)?,
        )?;
        let intent = profiles.begin_profile_deletion(&command)?;
        Ok((profiles, intent))
    }

    fn verify_global_admission_and_replay(
        catalog: &mut OperationsCatalog,
        operation_id: Uuid,
        second_operation_id: Uuid,
    ) -> Result<AgentProvisionOperation, Box<dyn Error>> {
        let pending = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(50)?)?
            .ok_or("first provision dispatch missing")?;
        assert_eq!(pending.operation().operation_id(), operation_id);
        assert_eq!(
            pending.operation().state(),
            AgentProvisionState::EffectPending
        );
        assert_eq!(
            pending.dispatch().map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Execute)
        );
        let pending_replay = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(51)?)?
            .ok_or("pending provision inspect missing")?;
        assert_eq!(pending_replay.operation().operation_id(), operation_id);
        assert_eq!(
            pending_replay
                .dispatch()
                .map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Inspect)
        );
        let unknown = catalog.mark_agent_provision_effect_unknown(
            operation_id,
            3,
            TimestampMillis::new(60)?,
        )?;
        assert_eq!(unknown.state(), AgentProvisionState::EffectUnknown);
        let unknown_replay = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(61)?)?
            .ok_or("unknown provision inspect missing")?;
        assert_eq!(unknown_replay.operation().operation_id(), operation_id);
        assert_eq!(
            unknown_replay
                .dispatch()
                .map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Inspect)
        );
        let confirmed = catalog.confirm_agent_provision_conversation(
            operation_id,
            4,
            "conversation-e809",
            TimestampMillis::new(70)?,
        )?;
        assert_eq!(confirmed.state(), AgentProvisionState::Confirmed);
        assert_eq!(
            confirmed.provider_conversation_id(),
            Some("conversation-e809")
        );
        assert!(
            catalog
                .next_agent_provision_dispatch(TimestampMillis::new(72)?)?
                .is_none()
        );
        assert!(
            catalog
                .agent_provision_operation(second_operation_id)?
                .is_some()
        );
        Ok(confirmed)
    }

    #[test]
    fn profile_barrier_skips_prepared_work_and_rejects_late_binding_but_keeps_replay()
    -> Result<(), Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("profile-delete-provision-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let ops_path = root.join("operations.sqlite3");
        let profile_path = root.join("profiles.sqlite3");
        let destination = Uuid::now_v7();
        let mut catalog = OperationsCatalog::open(&ops_path, destination)?;
        let profile_a = AgentProfileId::from_uuid(Uuid::from_u128(0xE901))?;
        let runtime_a = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE902))?;
        let request_a = Uuid::from_u128(0xE903);
        let prepared_a =
            create_bound_provision_operation(&mut catalog, request_a, profile_a, runtime_a, 100)?;
        let late_request = Uuid::from_u128(0xE904);
        let late_operation = prepare_new_provision_operation(&mut catalog, late_request, 200)?;
        let profile_b = AgentProfileId::from_uuid(Uuid::from_u128(0xE905))?;
        let runtime_b = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE906))?;
        let operation_b = create_bound_provision_operation(
            &mut catalog,
            Uuid::from_u128(0xE907),
            profile_b,
            runtime_b,
            300,
        )?;
        let (profiles, intent) =
            begin_profile_deletion_intent(profile_a, &profile_path, Uuid::from_u128(0xE908), 500)?;
        let barrier = catalog.begin_profile_deletion_barrier(&intent)?;
        assert_eq!(catalog.begin_profile_deletion_barrier(&intent)?, barrier);
        assert_eq!(catalog.profile_deletion_barrier(profile_a)?, Some(barrier));
        assert_eq!(
            catalog.profile_deletion_barrier_by_request(intent.request_id())?,
            Some(barrier)
        );

        let replay =
            catalog.prepare_agent_provision_operation(request_a, 0, TimestampMillis::new(501)?)?;
        assert!(!replay.created());
        assert_eq!(replay.operation().operation_id(), prepared_a);
        assert!(
            catalog
                .bind_agent_provision_target(
                    late_operation,
                    1,
                    profile_a,
                    AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE909))?,
                    TimestampMillis::new(502)?,
                )
                .is_err()
        );

        let next = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(600)?)?
            .ok_or("unrelated prepared provider work did not continue")?;
        assert_eq!(next.operation().operation_id(), operation_b);
        assert_eq!(
            next.dispatch().map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Execute)
        );
        assert_eq!(
            catalog
                .agent_provision_operation(late_operation)?
                .ok_or("late-bound operation missing")?
                .state(),
            AgentProvisionState::Prepared
        );
        drop(profiles);
        drop(catalog);
        fs::remove_file(profile_path)?;
        fs::remove_file(ops_path)?;
        fs::remove_dir(root)?;
        Ok(())
    }

    #[test]
    fn profile_barrier_preserves_inspection_of_admitted_provision_effect()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("profile-delete-inspect-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let ops_path = root.join("operations.sqlite3");
        let profile_path = root.join("profiles.sqlite3");
        let destination = Uuid::now_v7();
        let mut catalog = OperationsCatalog::open(&ops_path, destination)?;
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xE910))?;
        let operation_id = create_bound_provision_operation(
            &mut catalog,
            Uuid::from_u128(0xE911),
            profile_id,
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE912))?,
            100,
        )?;
        let executed = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(200)?)?
            .ok_or("provision Execute missing")?;
        assert_eq!(executed.operation().operation_id(), operation_id);
        let (profiles, intent) =
            begin_profile_deletion_intent(profile_id, &profile_path, Uuid::from_u128(0xE913), 300)?;
        let _ = catalog.begin_profile_deletion_barrier(&intent)?;
        let counts = catalog.profile_deletion_in_flight_counts(profile_id)?;
        assert_eq!(counts.pending_provision_operations(), 1);
        let inspected = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(301)?)?
            .ok_or("pending provision Inspect missing")?;
        assert_eq!(inspected.operation().operation_id(), operation_id);
        assert_eq!(
            inspected.dispatch().map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Inspect)
        );
        assert_eq!(
            inspected.operation().state(),
            AgentProvisionState::EffectPending
        );
        drop(profiles);
        drop(catalog);
        fs::remove_file(profile_path)?;
        fs::remove_file(ops_path)?;
        fs::remove_dir(root)?;
        Ok(())
    }

    #[test]
    fn aged_provision_keeps_its_fence_until_positive_reconciliation() -> Result<(), Box<dyn Error>>
    {
        let root = std::env::temp_dir().join(format!("agent-provision-aged-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE801);
        let mut catalog = OperationsCatalog::open(&path, destination)?;
        let first = create_bound_provision_operation(
            &mut catalog,
            Uuid::from_u128(0xE802),
            AgentProfileId::from_uuid(Uuid::from_u128(0xE807))?,
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE808))?,
            10,
        )?;
        let second = create_bound_provision_operation(
            &mut catalog,
            Uuid::from_u128(0xE80A),
            AgentProfileId::from_uuid(Uuid::from_u128(0xE80B))?,
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE80C))?,
            20,
        )?;
        let queued = catalog.agent_provision_operation(second)?;
        // Waiting in Prepared does not consume a provider execution deadline.
        let dispatch_at = 60 * 60_000;
        let pending = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(dispatch_at)?)?
            .ok_or("first provision dispatch missing")?;
        assert_eq!(pending.operation().operation_id(), first);
        assert_eq!(
            pending.dispatch().map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Execute)
        );
        let pending_inspect = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(dispatch_at + 1)?)?
            .ok_or("pending provision must remain inspectable")?;
        assert_eq!(pending_inspect.operation(), pending.operation());
        assert_eq!(
            pending_inspect
                .dispatch()
                .map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Inspect)
        );
        let unknown = catalog.mark_agent_provision_effect_unknown(
            first,
            pending.operation().revision(),
            TimestampMillis::new(dispatch_at + 2)?,
        )?;
        drop(catalog);
        let mut catalog = OperationsCatalog::open(&path, destination)?;
        // Missing acknowledgements and restarts are not evidence of failure.
        let recovered_at = dispatch_at + 24 * 60 * 60_000;
        for at in [dispatch_at + 3, recovered_at] {
            let inspect = catalog
                .next_agent_provision_dispatch(TimestampMillis::new(at)?)?
                .ok_or("unknown provision must remain inspectable")?;
            assert_eq!(inspect.operation(), &unknown);
            assert_eq!(
                inspect.dispatch().map(AgentProvisionDispatch::action),
                Some(AgentProvisionDispatchAction::Inspect)
            );
            assert_eq!(catalog.agent_provision_operation(second)?, queued);
        }
        let confirmed = catalog.confirm_agent_provision_conversation(
            first,
            unknown.revision(),
            "delayed-conversation",
            TimestampMillis::new(recovered_at + 1)?,
        )?;
        assert_eq!(confirmed.state(), AgentProvisionState::Confirmed);
        assert_eq!(
            confirmed.provider_conversation_id(),
            Some("delayed-conversation")
        );
        assert_eq!(confirmed.reported_failure_code(), None);
        let admitted = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(
                recovered_at + MIN_AGENT_PROVISION_OPEN_INTERVAL_MS + 2,
            )?)?
            .ok_or("next provision must proceed after confirmed reconciliation")?;
        assert_eq!(admitted.operation().operation_id(), second);
        assert_eq!(
            admitted.dispatch().map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Execute)
        );
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v27_migrates_and_provision_operation_reconciles_without_new_identity()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("agent-provision-v28-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE801);
        let request_id = Uuid::from_u128(0xE802);
        seed_v27_provision_resolution(&path, destination, request_id)?;

        let mut migrated = OperationsCatalog::open(&path, destination)?;
        let prepared =
            migrated.prepare_agent_provision_operation(request_id, 0, TimestampMillis::new(30)?)?;
        assert!(prepared.created());
        assert_eq!(prepared.operation().state(), AgentProvisionState::Prepared);
        let operation_id = prepared.operation().operation_id();
        let replay =
            migrated.prepare_agent_provision_operation(request_id, 0, TimestampMillis::new(31)?)?;
        assert!(!replay.created());
        assert_eq!(replay.operation().operation_id(), operation_id);

        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xE807))?;
        let runtime_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE808))?;
        let bound = migrated.bind_agent_provision_target(
            operation_id,
            1,
            profile_id,
            runtime_id,
            TimestampMillis::new(40)?,
        )?;
        assert_eq!(bound.revision(), 2);
        let second_operation_id = create_bound_provision_operation(
            &mut migrated,
            Uuid::from_u128(0xE80A),
            AgentProfileId::from_uuid(Uuid::from_u128(0xE80B))?,
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE80C))?,
            41,
        )?;
        let confirmed =
            verify_global_admission_and_replay(&mut migrated, operation_id, second_operation_id)?;
        let second_pending = migrated
            .next_agent_provision_dispatch(TimestampMillis::new(10_070)?)?
            .ok_or("second provision dispatch missing after cooldown")?;
        assert_eq!(
            second_pending.operation().operation_id(),
            second_operation_id
        );
        assert_eq!(
            second_pending
                .dispatch()
                .map(AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Execute)
        );
        drop(migrated);

        downgrade_catalog_to_v29_provision(&path, true, 28)?;

        let reopened = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened.agent_provision_operation(operation_id)?,
            Some(confirmed.clone())
        );
        assert!(reopened.agent_request(request_id)?.is_some());
        drop(reopened);

        downgrade_catalog_to_v29_provision(&path, false, 29)?;

        let reopened_v29 = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened_v29.agent_provision_operation(operation_id)?,
            Some(confirmed)
        );
        drop(reopened_v29);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v32_additive_migration_preserves_operation_without_fabricating_reason()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("agent-provision-v32-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE810);
        let operation_id;
        {
            let mut catalog = OperationsCatalog::open(&path, destination)?;
            operation_id = create_bound_provision_operation(
                &mut catalog,
                Uuid::from_u128(0xE811),
                AgentProfileId::from_uuid(Uuid::from_u128(0xE812))?,
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE813))?,
                10,
            )?;
        }
        downgrade_catalog_to_v32(&path)?;
        let reopened = OperationsCatalog::open(&path, destination)?;
        let operation = reopened
            .agent_provision_operation(operation_id)?
            .ok_or("operation missing after v32 migration")?;
        assert_eq!(operation.reported_failure_code(), None);
        assert_eq!(
            reopened
                .connection
                .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            crate::catalog::VERSION
        );
        assert_eq!(
            reopened.connection.query_row(
                "SELECT COUNT(*) FROM ops_agent_provision_failures",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            0
        );
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn failure_code_wire_vocabulary_is_closed() -> Result<(), Box<dyn Error>> {
        let values = [
            (
                AgentProvisionFailureCode::TabLoadTimeout,
                "tab_load_timeout",
            ),
            (
                AgentProvisionFailureCode::DocumentObservationFailed,
                "document_observation_failed",
            ),
            (
                AgentProvisionFailureCode::DocumentIdentityChanged,
                "document_identity_changed",
            ),
            (
                AgentProvisionFailureCode::DocumentIdentityUnavailable,
                "document_identity_unavailable",
            ),
            (
                AgentProvisionFailureCode::ScriptInjectionFailed,
                "script_injection_failed",
            ),
            (
                AgentProvisionFailureCode::ComposerUnavailable,
                "composer_unavailable",
            ),
            (
                AgentProvisionFailureCode::ComposerFormUnavailable,
                "composer_form_unavailable",
            ),
            (AgentProvisionFailureCode::ComposerBusy, "composer_busy"),
            (
                AgentProvisionFailureCode::ComposerNotEmpty,
                "composer_not_empty",
            ),
            (
                AgentProvisionFailureCode::ComposerFocusFailed,
                "composer_focus_failed",
            ),
            (
                AgentProvisionFailureCode::ComposerSelectionFailed,
                "composer_selection_failed",
            ),
            (
                AgentProvisionFailureCode::ComposerInsertFailed,
                "composer_insert_failed",
            ),
            (
                AgentProvisionFailureCode::ComposerBodyMismatch,
                "composer_body_mismatch",
            ),
            (
                AgentProvisionFailureCode::SendButtonUnavailable,
                "send_button_unavailable",
            ),
            (
                AgentProvisionFailureCode::ComposerReadyTimeout,
                "composer_ready_timeout",
            ),
            (
                AgentProvisionFailureCode::UnknownAdapterFailure,
                "unknown_adapter_failure",
            ),
            (
                AgentProvisionFailureCode::SurfaceOpenFailed,
                "surface_open_failed",
            ),
            (
                AgentProvisionFailureCode::JournalWriteFailed,
                "journal_write_failed",
            ),
        ];
        for (code, name) in values {
            assert_eq!(code.as_str(), name);
            assert_eq!(AgentProvisionFailureCode::from_wire_name(name)?, code);
        }
        assert!(AgentProvisionFailureCode::from_wire_name("click_reply_uncertain").is_err());
        assert!(AgentProvisionFailureCode::from_wire_name("arbitrary").is_err());
        Ok(())
    }

    #[test]
    fn definite_failure_persists_and_exact_replay_requires_code_and_fence()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("agent-provision-failure-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE820);
        let operation_id;
        {
            let mut catalog = OperationsCatalog::open(&path, destination)?;
            operation_id = create_bound_provision_operation(
                &mut catalog,
                Uuid::from_u128(0xE821),
                AgentProfileId::from_uuid(Uuid::from_u128(0xE822))?,
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE823))?,
                10,
            )?;
            let pending = catalog
                .next_agent_provision_dispatch(TimestampMillis::new(50)?)?
                .ok_or("pending provision dispatch missing")?;
            let failed = catalog.fail_agent_provision_effect(
                operation_id,
                pending.operation().revision(),
                TimestampMillis::new(60)?,
                Some(AgentProvisionFailureCode::ComposerBodyMismatch),
            )?;
            assert_eq!(failed.state(), AgentProvisionState::Failed);
            assert_eq!(
                failed.reported_failure_code(),
                Some(AgentProvisionFailureCode::ComposerBodyMismatch)
            );
            assert_eq!(failed.revision(), pending.operation().revision() + 1);
        }

        let mut reopened = OperationsCatalog::open(&path, destination)?;
        let loaded = reopened
            .agent_provision_operation(operation_id)?
            .ok_or("failed operation missing after reopen")?;
        assert_eq!(loaded.state(), AgentProvisionState::Failed);
        assert_eq!(
            loaded.reported_failure_code(),
            Some(AgentProvisionFailureCode::ComposerBodyMismatch)
        );
        let replay = reopened.fail_agent_provision_effect(
            operation_id,
            loaded.revision() - 1,
            loaded.updated_at(),
            Some(AgentProvisionFailureCode::ComposerBodyMismatch),
        )?;
        assert_eq!(replay, loaded);
        assert!(
            reopened
                .fail_agent_provision_effect(
                    operation_id,
                    loaded.revision() - 1,
                    loaded.updated_at(),
                    Some(AgentProvisionFailureCode::ComposerBusy),
                )
                .is_err()
        );
        assert!(
            reopened
                .fail_agent_provision_effect(
                    operation_id,
                    loaded.revision(),
                    loaded.updated_at(),
                    Some(AgentProvisionFailureCode::ComposerBodyMismatch),
                )
                .is_err()
        );
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn no_reason_failure_remains_none_after_reopen() -> Result<(), Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("agent-provision-no-reason-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE830);
        let mut catalog = OperationsCatalog::open(&path, destination)?;
        let operation_id = create_bound_provision_operation(
            &mut catalog,
            Uuid::from_u128(0xE831),
            AgentProfileId::from_uuid(Uuid::from_u128(0xE832))?,
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE833))?,
            10,
        )?;
        let pending = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(50)?)?
            .ok_or("pending provision dispatch missing")?;
        let failed = catalog.fail_agent_provision_effect(
            operation_id,
            pending.operation().revision(),
            TimestampMillis::new(60)?,
            None,
        )?;
        assert_eq!(failed.reported_failure_code(), None);
        drop(catalog);
        let reopened = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened
                .agent_provision_operation(operation_id)?
                .ok_or("failed operation missing")?
                .reported_failure_code(),
            None
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn failure_transition_rolls_back_when_diagnostic_insert_fails() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "agent-provision-failure-rollback-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE840);
        let mut catalog = OperationsCatalog::open(&path, destination)?;
        let operation_id = create_bound_provision_operation(
            &mut catalog,
            Uuid::from_u128(0xE841),
            AgentProfileId::from_uuid(Uuid::from_u128(0xE842))?,
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE843))?,
            10,
        )?;
        let pending = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(50)?)?
            .ok_or("pending provision dispatch missing")?;
        catalog.connection.execute_batch(
            "CREATE TRIGGER test_agent_provision_failure_abort
             BEFORE INSERT ON ops_agent_provision_failures
             BEGIN SELECT RAISE(ABORT, 'diagnostic insert rejected'); END;",
        )?;
        assert!(
            catalog
                .fail_agent_provision_effect(
                    operation_id,
                    pending.operation().revision(),
                    TimestampMillis::new(60)?,
                    Some(AgentProvisionFailureCode::TabLoadTimeout),
                )
                .is_err()
        );
        let (state, revision): (i64, i64) = catalog.connection.query_row(
            "SELECT state,revision FROM ops_agent_provision_operations WHERE operation_id=?1",
            params![operation_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(state, encode_state(AgentProvisionState::EffectPending));
        assert_eq!(revision, i64::try_from(pending.operation().revision())?);
        let diagnostics: i64 = catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_agent_provision_failures WHERE operation_id=?1",
            params![operation_id.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(diagnostics, 0);
        catalog
            .connection
            .execute_batch("DROP TRIGGER test_agent_provision_failure_abort;")?;
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn invalid_failure_diagnostics_are_rejected_on_read() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "agent-provision-failure-corrupt-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE850);
        let mut catalog = OperationsCatalog::open(&path, destination)?;
        let operation_id = create_bound_provision_operation(
            &mut catalog,
            Uuid::from_u128(0xE851),
            AgentProfileId::from_uuid(Uuid::from_u128(0xE852))?,
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xE853))?,
            10,
        )?;
        let pending = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(50)?)?
            .ok_or("pending provision dispatch missing")?;
        let failed = catalog.fail_agent_provision_effect(
            operation_id,
            pending.operation().revision(),
            TimestampMillis::new(60)?,
            Some(AgentProvisionFailureCode::TabLoadTimeout),
        )?;
        catalog
            .connection
            .pragma_update(None, "ignore_check_constraints", true)?;
        catalog.connection.execute(
            "UPDATE ops_agent_provision_failures SET failure_code=99 WHERE operation_id=?1",
            params![operation_id.as_bytes()],
        )?;
        catalog
            .connection
            .pragma_update(None, "ignore_check_constraints", false)?;
        assert!(catalog.agent_provision_operation(operation_id).is_err());
        catalog.connection.execute(
            "UPDATE ops_agent_provision_failures SET failure_code=0,operation_revision=?2 WHERE operation_id=?1",
            params![operation_id.as_bytes(), i64::try_from(failed.revision() + 1)?],
        )?;
        assert!(catalog.agent_provision_operation(operation_id).is_err());
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
