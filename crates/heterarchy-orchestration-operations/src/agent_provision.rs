//! Historical `ChatGPT` provider-effect journal for `AgentRequest` resolutions.
//!
//! New provider operations are retired. This module retains exact historical
//! rows and readback; only an unreleased local `Prepared` operation can be
//! cancelled.

use forge_domain::TimestampMillis;
use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{OperationsCatalog, OperationsError, wire};

mod uncertainty_retirement;

pub use uncertainty_retirement::{
    AgentProvisionUncertaintyRetirement, AgentProvisionUncertaintyRetirementCommand,
    AgentProvisionUncertaintyRetirementOutcome, AgentProvisionUncertaintyRetirementTarget,
};

const AGENT_PROVISION_OPERATION_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-provision.operation.v1\0";
const MAX_PROVIDER_CONVERSATION_ID_BYTES: usize = 512;

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
    uncertainty_retirement: Option<AgentProvisionUncertaintyRetirement>,
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

    /// Durable evidence that an unobservable `ProvisionNew` effect was retired
    /// without pretending to know whether its provider-side effect occurred.
    #[must_use]
    pub const fn uncertainty_retirement(&self) -> Option<&AgentProvisionUncertaintyRetirement> {
        self.uncertainty_retirement.as_ref()
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

    /// Retired provider provisioning entrypoint.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog.
    pub fn prepare_agent_provision_operation(
        &mut self,
        _request_id: Uuid,
        _slot_ordinal: u8,
        _prepared_at: TimestampMillis,
    ) -> Result<AgentProvisionPrepareOutcome, OperationsError> {
        Err(OperationsError::ProviderRetired)
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

    /// Retired provider target binding entrypoint.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog.
    pub fn bind_agent_provision_target(
        &mut self,
        _operation_id: Uuid,
        _expected_revision: u64,
        _profile_id: AgentProfileId,
        _runtime_session_id: AgentProfileRuntimeSessionId,
        _observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        Err(OperationsError::ProviderRetired)
    }

    /// Retired provider dispatch entrypoint.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog.
    pub fn next_agent_provision_dispatch(
        &mut self,
        _observed_at: TimestampMillis,
    ) -> Result<Option<AgentProvisionDispatchOutcome>, OperationsError> {
        Err(OperationsError::ProviderRetired)
    }

    /// Retired provider outcome update entrypoint.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog.
    pub fn mark_agent_provision_effect_unknown(
        &mut self,
        _operation_id: Uuid,
        _expected_revision: u64,
        _observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        Err(OperationsError::ProviderRetired)
    }

    /// Retired provider conversation confirmation entrypoint.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog.
    pub fn confirm_agent_provision_conversation(
        &mut self,
        _operation_id: Uuid,
        _expected_revision: u64,
        _provider_conversation_id: &str,
        _observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        Err(OperationsError::ProviderRetired)
    }

    /// Retired provider failure update entrypoint.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog.
    pub fn fail_agent_provision_effect(
        &mut self,
        _operation_id: Uuid,
        _expected_revision: u64,
        _observed_at: TimestampMillis,
        _failure_code: Option<AgentProvisionFailureCode>,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        Err(OperationsError::ProviderRetired)
    }

    /// Retired provider operation cancellation entrypoint.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog.
    pub fn cancel_agent_provision_operation(
        &mut self,
        _operation_id: Uuid,
        _expected_revision: u64,
        _observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, OperationsError> {
        Err(OperationsError::ProviderRetired)
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

pub(crate) fn create_uncertainty_retirement_schema(
    connection: &Connection,
) -> Result<(), OperationsError> {
    uncertainty_retirement::create_schema(connection)
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

pub(crate) fn uncertainty_retirement_schema_objects()
-> Vec<(&'static str, &'static str, &'static str)> {
    uncertainty_retirement::schema_objects()
}
fn validate_provider_conversation_id(value: &str) -> Result<(), OperationsError> {
    if value.is_empty() || value.len() > MAX_PROVIDER_CONVERSATION_ID_BYTES {
        return Err(OperationsError::Bounds("provider conversation identity"));
    }
    Ok(())
}

fn in_flight_operation_ids(connection: &Connection) -> Result<Vec<Uuid>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT operation_id FROM ops_agent_provision_operations
         WHERE state IN(2,3)
           AND NOT EXISTS(
                SELECT 1 FROM ops_agent_provision_uncertainty_retirements AS retirement
                 WHERE retirement.operation_id=ops_agent_provision_operations.operation_id
           )
         ORDER BY updated_at_ms ASC, operation_id ASC
         LIMIT 2",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    rows.map(|row| Ok(Uuid::from_bytes(wire::id(row?)?)))
        .collect()
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
    let Some(raw) = raw else {
        return Ok(None);
    };
    let mut operation = decode_operation(operation_id, raw)?;
    operation.uncertainty_retirement =
        uncertainty_retirement::load_for_operation(connection, operation_id)?;
    uncertainty_retirement::validate_operation_projection(&operation)?;
    Ok(Some(operation))
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
    let state = decode_state(raw.state)?;
    let reported_failure_code = decode_failure_diagnostic(&raw, state)?;
    let (profile_id, runtime_session_id) =
        decode_operation_target(raw.profile_id, raw.runtime_session_id)?;
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
    validate_conversation_state(raw.provider_conversation_id.as_deref(), state)?;
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
        uncertainty_retirement: None,
        prepared_at,
        updated_at,
        revision,
    })
}

fn decode_operation_target(
    profile_id: Option<Vec<u8>>,
    runtime_session_id: Option<Vec<u8>>,
) -> Result<(Option<AgentProfileId>, Option<AgentProfileRuntimeSessionId>), OperationsError> {
    let profile_id = profile_id
        .map(|value| {
            AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(value)?))
                .map_err(|_| OperationsError::Corrupt("Agent provision Profile id"))
        })
        .transpose()?;
    let runtime_session_id = runtime_session_id
        .map(|value| {
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(wire::id(value)?))
                .map_err(|_| OperationsError::Corrupt("Agent provision RuntimeSession id"))
        })
        .transpose()?;
    Ok((profile_id, runtime_session_id))
}

fn validate_conversation_state(
    provider_conversation_id: Option<&str>,
    state: AgentProvisionState,
) -> Result<(), OperationsError> {
    let has_conversation = provider_conversation_id.is_some();
    let conversation_state = matches!(
        state,
        AgentProvisionState::Observed | AgentProvisionState::Confirmed
    );
    if has_conversation == conversation_state {
        Ok(())
    } else {
        Err(OperationsError::Corrupt(
            "Agent provision conversation/state shape",
        ))
    }
}

fn decode_failure_diagnostic(
    raw: &RawAgentProvisionOperation,
    state: AgentProvisionState,
) -> Result<Option<AgentProvisionFailureCode>, OperationsError> {
    match (
        raw.failure_operation_revision,
        raw.failure_code,
        raw.failure_observed_at,
    ) {
        (None, None, None) => Ok(None),
        (Some(failure_revision), Some(failure_code), Some(failure_observed_at)) => {
            if state != AgentProvisionState::Failed
                || failure_revision != raw.revision
                || failure_observed_at != raw.updated_at
            {
                return Err(OperationsError::Corrupt(
                    "Agent provision failure diagnostic shape",
                ));
            }
            Ok(Some(decode_failure_code(failure_code)?))
        }
        _ => Err(OperationsError::Corrupt(
            "Agent provision failure diagnostic shape",
        )),
    }
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

fn decode_effect_kind(value: i64) -> Result<AgentProvisionEffectKind, OperationsError> {
    match value {
        1 => Ok(AgentProvisionEffectKind::WakeExistingRuntime),
        2 => Ok(AgentProvisionEffectKind::WakeNewRuntime),
        3 => Ok(AgentProvisionEffectKind::ProvisionNew),
        4 => Ok(AgentProvisionEffectKind::ReuseReadyRuntime),
        _ => Err(OperationsError::Corrupt("Agent provision effect kind")),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentRequestCommand, AgentRequestEffectScope, AgentRequestIndependence, AgentRequestIntent,
        AgentRequestRequester,
    };
    use forge_domain::{
        ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, WorkspaceId,
    };
    use rusqlite::params;
    use std::{error::Error, fs};

    const DESTINATION: Uuid = Uuid::from_u128(0xE801);
    const REQUEST: Uuid = Uuid::from_u128(0xE802);
    const WORKSPACE: Uuid = Uuid::from_u128(0xE803);
    const ACTIVITY: Uuid = Uuid::from_u128(0xE804);
    const REQUESTER_AGENT: Uuid = Uuid::from_u128(0xE805);
    const REQUESTER_SESSION: Uuid = Uuid::from_u128(0xE806);
    const PROFILE: Uuid = Uuid::from_u128(0xE807);
    const RUNTIME: Uuid = Uuid::from_u128(0xE808);

    fn fixture() -> Result<(std::path::PathBuf, OperationsCatalog), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("agent-provision-retired-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let catalog = OperationsCatalog::open(&root.join("operations.sqlite3"), DESTINATION)?;
        Ok((root, catalog))
    }

    fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
        TimestampMillis::new(value).map_err(|_| OperationsError::Bounds("test timestamp"))
    }

    fn reserve_provision_request(catalog: &mut OperationsCatalog) -> Result<(), Box<dyn Error>> {
        let request = AgentRequestCommand::new(
            REQUEST,
            AgentRequestRequester::new(
                WorkspaceId::from_uuid(WORKSPACE),
                ActivityId::from_uuid(ACTIVITY),
                AgentInstanceId::from_uuid(REQUESTER_AGENT),
                AgentSessionId::from_uuid(REQUESTER_SESSION),
                1,
            )?,
            AgentRequestIntent::new(
                "verify provider retirement".to_owned(),
                "verification".to_owned(),
                1,
                AgentRequestEffectScope::ReadOnly,
                AgentRequestIndependence::None,
                "preserve durable intent".to_owned(),
            )?,
            timestamp(10)?,
        )?;
        catalog.submit_agent_request(&request)?;
        catalog.connection.execute(
            "INSERT INTO ops_agent_request_resolutions(request_id,slot_ordinal,resolution_kind,claim_id,profile_id,profile_revision,runtime_session_id,runtime_session_revision,policy_revision,reserved_at_ms) VALUES(?1,0,5,NULL,NULL,NULL,NULL,NULL,1,20)",
            params![REQUEST.as_bytes()],
        )?;
        catalog.connection.execute(
            "UPDATE ops_agent_requests SET state=2,revision=2 WHERE request_id=?1 AND state=1 AND revision=1",
            params![REQUEST.as_bytes()],
        )?;
        Ok(())
    }

    fn seed_historical_operation(
        catalog: &OperationsCatalog,
        state: i64,
        revision: i64,
        profile_id: Option<Uuid>,
        runtime_session_id: Option<Uuid>,
    ) -> Result<Uuid, Box<dyn Error>> {
        let operation_id = AgentProvisionOperation::identity_for_slot(REQUEST, 0);
        catalog.connection.execute(
            "INSERT INTO ops_agent_provision_operations(operation_id,request_id,slot_ordinal,effect_kind,provider_family,profile_id,runtime_session_id,state,provider_conversation_id,prepared_at_ms,updated_at_ms,revision) VALUES(?1,?2,0,3,1,?3,?4,?5,NULL,10,20,?6)",
            params![
                operation_id.as_bytes(),
                REQUEST.as_bytes(),
                profile_id.map(|id| id.as_bytes().to_vec()),
                runtime_session_id.map(|id| id.as_bytes().to_vec()),
                state,
                revision,
            ],
        )?;
        Ok(operation_id)
    }

    #[test]
    fn retired_provider_writers_leave_unknown_history_and_request_intent_unchanged()
    -> Result<(), Box<dyn Error>> {
        let (root, mut catalog) = fixture()?;
        reserve_provision_request(&mut catalog)?;
        let operation_id = seed_historical_operation(&catalog, 3, 7, Some(PROFILE), Some(RUNTIME))?;
        let before_operation = catalog
            .agent_provision_operation(operation_id)?
            .ok_or("historical provision row missing")?;
        let before_request = catalog
            .agent_request(REQUEST)?
            .ok_or("AgentRequest missing")?;
        assert_eq!(before_operation.state(), AgentProvisionState::EffectUnknown);
        assert_eq!(before_operation.revision(), 7);

        assert!(matches!(
            catalog.prepare_agent_provision_operation(REQUEST, 0, timestamp(30)?),
            Err(OperationsError::ProviderRetired)
        ));
        assert!(matches!(
            catalog.bind_agent_provision_target(
                operation_id,
                7,
                AgentProfileId::from_uuid(PROFILE)?,
                AgentProfileRuntimeSessionId::from_uuid(RUNTIME)?,
                timestamp(30)?,
            ),
            Err(OperationsError::ProviderRetired)
        ));
        assert!(matches!(
            catalog.next_agent_provision_dispatch(timestamp(30)?),
            Err(OperationsError::ProviderRetired)
        ));
        assert!(matches!(
            catalog.mark_agent_provision_effect_unknown(operation_id, 7, timestamp(30)?),
            Err(OperationsError::ProviderRetired)
        ));
        assert!(matches!(
            catalog.confirm_agent_provision_conversation(
                operation_id,
                7,
                "legacy-conversation",
                timestamp(30)?,
            ),
            Err(OperationsError::ProviderRetired)
        ));
        assert!(matches!(
            catalog.fail_agent_provision_effect(operation_id, 7, timestamp(30)?, None),
            Err(OperationsError::ProviderRetired)
        ));
        let retirement_target = AgentProvisionUncertaintyRetirementTarget::new(
            operation_id,
            7,
            WorkspaceId::from_uuid(WORKSPACE),
            AgentSessionId::from_uuid(REQUESTER_SESSION),
        )?;
        let retirement = AgentProvisionUncertaintyRetirementCommand::new(
            Uuid::from_u128(0xE809),
            retirement_target,
            AuthenticatedPrincipal::TrustedStdio,
            1,
            timestamp(30)?,
        )?;
        assert!(matches!(
            catalog.retire_agent_provision_uncertainty(retirement),
            Err(OperationsError::ProviderRetired)
        ));
        assert!(matches!(
            catalog.reconcile_agent_request_fulfillment(REQUEST, timestamp(30)?),
            Err(OperationsError::ProviderRetired)
        ));

        assert_eq!(
            catalog.agent_provision_operation(operation_id)?,
            Some(before_operation)
        );
        assert_eq!(catalog.agent_request(REQUEST)?, Some(before_request));
        assert!(
            catalog
                .agent_provision_uncertainty_retirement(operation_id)?
                .is_none()
        );
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn prepared_provider_operation_remains_unchanged_when_cancel_is_retired()
    -> Result<(), Box<dyn Error>> {
        let (root, mut catalog) = fixture()?;
        reserve_provision_request(&mut catalog)?;
        let operation_id = seed_historical_operation(&catalog, 1, 1, None, None)?;
        let before = catalog
            .agent_provision_operation(operation_id)?
            .ok_or("historical Prepared operation missing")?;
        assert_eq!(before.state(), AgentProvisionState::Prepared);
        assert!(matches!(
            catalog.cancel_agent_provision_operation(operation_id, 1, timestamp(21)?),
            Err(OperationsError::ProviderRetired)
        ));
        assert_eq!(
            catalog.agent_provision_operation(operation_id)?,
            Some(before)
        );
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
