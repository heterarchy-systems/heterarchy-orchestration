//! Exact provider-deletion dispatch, unknown-outcome, and receipt handling.

use crate::{OrchestrationRuntime, RuntimeError};
use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    AgentProfileProviderDeletionAction, AgentProfileProviderDeletionDeferredReason,
    AgentProfileProviderDeletionDispatch, AgentProfileProviderDeletionOperation,
    AgentProfileProviderDeletionReceipt, AgentProfileProviderDeletionReceiptOutcome,
    AgentProfileProviderDeletionState,
};
use heterarchy_orchestration_sessions::{
    AgentProfileDeletionManifest, AgentProfileDeletionRuntimeEntry,
    AgentProfileDeletionRuntimeTarget, AgentProfileId, AgentProfileProviderFamily,
    AgentProfileRuntimeSessionId,
};
use uuid::Uuid;

const MAX_PROVIDER_ID_BYTES: usize = 512;
const MIN_RETRY_DELAY_MS: i64 = 30_000;

/// Full immutable identity carried from one Operations dispatch to its receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionFence {
    operation_id: Uuid,
    request_id: Uuid,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
    provider_conversation_id: String,
    expected_revision: u64,
    attempt_number: u64,
    action: AgentProfileProviderDeletionAction,
}

impl AgentProfileProviderDeletionFence {
    /// Construct an exact provider-action fence from typed HTTP fields.
    ///
    /// # Errors
    /// Rejects nil identities, an invalid provider target, or zero revision and
    /// attempt values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation_id: Uuid,
        request_id: Uuid,
        profile_id: Uuid,
        runtime_session_id: Uuid,
        provider_conversation_id: String,
        expected_revision: u64,
        attempt_number: u64,
        action: AgentProfileProviderDeletionAction,
    ) -> Result<Self, RuntimeError> {
        if operation_id.is_nil()
            || request_id.is_nil()
            || expected_revision == 0
            || attempt_number == 0
            || provider_conversation_id.is_empty()
            || provider_conversation_id.len() > MAX_PROVIDER_ID_BYTES
        {
            return Err(RuntimeError::InvalidInput);
        }
        Ok(Self {
            operation_id,
            request_id,
            profile_id: AgentProfileId::from_uuid(profile_id)
                .map_err(|_| RuntimeError::InvalidInput)?,
            runtime_session_id: AgentProfileRuntimeSessionId::from_uuid(runtime_session_id)
                .map_err(|_| RuntimeError::InvalidInput)?,
            provider_conversation_id,
            expected_revision,
            attempt_number,
            action,
        })
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
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn runtime_session_id(&self) -> AgentProfileRuntimeSessionId {
        self.runtime_session_id
    }

    #[must_use]
    pub fn provider_conversation_id(&self) -> &str {
        &self.provider_conversation_id
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    #[must_use]
    pub const fn attempt_number(&self) -> u64 {
        self.attempt_number
    }

    #[must_use]
    pub const fn action(&self) -> AgentProfileProviderDeletionAction {
        self.action
    }
}

/// Trusted-host statement to record against an exact provider dispatch.
/// `NotStartedBeforeClick` may only follow an Execute action; positive no-effect
/// readback is only accepted for Inspect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentProfileProviderDeletionReceiptInput {
    NotStartedBeforeClick {
        reason: AgentProfileProviderDeletionDeferredReason,
        browser_session_id: Option<String>,
        tab_id: Option<String>,
        document_id: Option<String>,
    },
    PositiveNoEffectReadback {
        reason: AgentProfileProviderDeletionDeferredReason,
        browser_session_id: Option<String>,
        tab_id: Option<String>,
        document_id: Option<String>,
    },
    ProviderDeleteConfirmed {
        browser_session_id: String,
        tab_id: String,
        document_id: String,
    },
}

/// One currently actionable provider dispatch and its durable operation state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionDispatchSummary {
    pub fence: AgentProfileProviderDeletionFence,
    pub state: AgentProfileProviderDeletionState,
}

/// Durable acknowledgement of an exact Unknown transition or receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionReceiptSummary {
    pub fence: AgentProfileProviderDeletionFence,
    pub state: AgentProfileProviderDeletionState,
    pub observed_at_ms: i64,
}

/// Exact provider operation and the optional receipt for the requested attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionReadbackSummary {
    pub operation: AgentProfileProviderDeletionOperation,
    pub receipt: Option<AgentProfileProviderDeletionReceipt>,
}

impl OrchestrationRuntime {
    /// Observe a current selection without admitting Execute or changing state.
    ///
    /// # Errors
    /// Rejects corrupt/foreign frozen targets and catalog failures.
    pub fn profile_deletion_provider_selection(
        &self,
    ) -> Result<Option<AgentProfileProviderDeletionOperation>, RuntimeError> {
        let Some(operation) = self
            .catalog()?
            .agent_profile_provider_deletion_in_flight()?
        else {
            return Ok(None);
        };
        let selector = AgentProfileProviderDeletionFence::new(
            operation.operation_id(),
            operation.request_id(),
            operation.profile_id().into_uuid(),
            operation.runtime_session_id().into_uuid(),
            operation.provider_conversation_id().to_owned(),
            operation.revision(),
            operation.attempt_number(),
            AgentProfileProviderDeletionAction::Inspect,
        )?;
        validate_operation_target(self, &operation, &selector)?;
        Ok(Some(operation))
    }

    /// Read an original callback fence/receipt without replaying a mutation.
    /// Absence does not establish nonadmission, request equivalence or retry permission.
    ///
    /// # Errors
    /// Rejects foreign targets, changed receipt fences or catalog failures.
    pub fn profile_deletion_provider_readback(
        &self,
        fence: &AgentProfileProviderDeletionFence,
    ) -> Result<AgentProfileProviderDeletionReadbackSummary, RuntimeError> {
        let operation = read_operation(self, fence.operation_id)?.ok_or(RuntimeError::NotFound)?;
        validate_operation_target(self, &operation, fence)?;
        let receipt = read_receipt(self, fence.operation_id, fence.attempt_number)?;
        if receipt
            .as_ref()
            .is_some_and(|receipt| !dispatch_matches_fence(receipt.dispatch(), fence))
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(AgentProfileProviderDeletionReadbackSummary { operation, receipt })
    }

    /// Return the next global provider deletion dispatch. Operations enforces a
    /// single in-flight action and changes every existing Pending/Unknown item
    /// into Inspect; this method never starts provider I/O.
    ///
    /// # Errors
    /// Returns invalid time, owner poison, or an Operations/frozen-target error.
    pub fn next_agent_profile_provider_deletion_dispatch(
        &self,
        observed_at_ms: i64,
    ) -> Result<Option<AgentProfileProviderDeletionDispatchSummary>, RuntimeError> {
        let observed_at =
            TimestampMillis::new(observed_at_ms).map_err(|_| RuntimeError::InvalidInput)?;
        let dispatch = self
            .catalog()?
            .next_agent_profile_provider_deletion_dispatch(observed_at)?;
        let Some(dispatch) = dispatch else {
            return Ok(None);
        };
        let operation = self
            .catalog()?
            .agent_profile_provider_deletion(dispatch.operation_id())?
            .ok_or(RuntimeError::Conflict)?;
        validate_dispatch_and_frozen_target(self, &operation, &dispatch)?;
        Ok(Some(AgentProfileProviderDeletionDispatchSummary {
            fence: fence_from_dispatch(&dispatch),
            state: operation.state(),
        }))
    }

    /// Mark only the exact Execute attempt Unknown after its caller lost the
    /// dispatch/effect outcome. Replaying the same fence reads the durable
    /// Unknown state and never emits another Execute.
    ///
    /// # Errors
    /// Rejects Inspect, stale attempts/revisions, changed targets, invalid
    /// timestamps, or catalog failures.
    pub fn mark_agent_profile_provider_deletion_unknown(
        &self,
        fence: &AgentProfileProviderDeletionFence,
        observed_at_ms: i64,
    ) -> Result<AgentProfileProviderDeletionReceiptSummary, RuntimeError> {
        if fence.action != AgentProfileProviderDeletionAction::Execute {
            return Err(RuntimeError::Conflict);
        }
        let observed_at =
            TimestampMillis::new(observed_at_ms).map_err(|_| RuntimeError::InvalidInput)?;
        let operation = read_operation(self, fence.operation_id)?.ok_or(RuntimeError::NotFound)?;
        validate_operation_target(self, &operation, fence)?;
        if operation.state() == AgentProfileProviderDeletionState::EffectUnknown
            && fence.expected_revision.checked_add(1) == Some(operation.revision())
            && operation.attempt_number() == fence.attempt_number
        {
            return Ok(AgentProfileProviderDeletionReceiptSummary {
                fence: fence.clone(),
                state: operation.state(),
                observed_at_ms: operation.updated_at().get(),
            });
        }
        if operation.state() != AgentProfileProviderDeletionState::EffectPending
            || operation.revision() != fence.expected_revision
            || operation.attempt_number() != fence.attempt_number
        {
            return Err(RuntimeError::Conflict);
        }
        let result = self
            .catalog()?
            .mark_agent_profile_provider_deletion_unknown(
                fence.operation_id,
                fence.expected_revision,
                fence.attempt_number,
                observed_at,
            );
        let updated = match result {
            Ok(updated) => updated,
            Err(error) => {
                let Some(readback) = read_operation(self, fence.operation_id)? else {
                    return Err(error.into());
                };
                if readback.state() == AgentProfileProviderDeletionState::EffectUnknown
                    && fence.expected_revision.checked_add(1) == Some(readback.revision())
                    && readback.attempt_number() == fence.attempt_number
                {
                    readback
                } else {
                    return Err(error.into());
                }
            }
        };
        validate_operation_target(self, &updated, fence)?;
        Ok(AgentProfileProviderDeletionReceiptSummary {
            fence: fence.clone(),
            state: updated.state(),
            observed_at_ms: updated.updated_at().get(),
        })
    }

    /// Persist or replay one exact trusted-host receipt. A `NotStarted` receipt
    /// requires Execute; a positive no-effect readback requires Inspect; a
    /// deletion confirmation requires exact browser, tab, and document IDs.
    ///
    /// # Errors
    /// Rejects changed receipt payloads/fences, stale operations, invalid
    /// evidence or timestamps, and catalog failures.
    pub fn record_agent_profile_provider_deletion_receipt(
        &self,
        fence: &AgentProfileProviderDeletionFence,
        input: &AgentProfileProviderDeletionReceiptInput,
        observed_at_ms: i64,
    ) -> Result<AgentProfileProviderDeletionReceiptSummary, RuntimeError> {
        let observed_at =
            TimestampMillis::new(observed_at_ms).map_err(|_| RuntimeError::InvalidInput)?;
        if let Some(receipt) = read_receipt(self, fence.operation_id, fence.attempt_number)? {
            return existing_receipt_ack(self, fence, input, &receipt);
        }
        validate_receipt_action(fence.action, input)?;

        let dispatch = self
            .catalog()?
            .agent_profile_provider_deletion_receipt_dispatch(
                fence.operation_id,
                fence.expected_revision,
                fence.attempt_number,
                fence.action,
            )?
            .ok_or(RuntimeError::Conflict)?;
        if !dispatch_matches_fence(&dispatch, fence) {
            return Err(RuntimeError::Conflict);
        }
        let operation = read_operation(self, fence.operation_id)?.ok_or(RuntimeError::NotFound)?;
        validate_dispatch_and_frozen_target(self, &operation, &dispatch)?;
        let receipt = build_receipt(&dispatch, input, observed_at)?;
        let recorded = self
            .catalog()?
            .record_agent_profile_provider_deletion_receipt(&receipt);
        let recorded = match recorded {
            Ok(recorded) => recorded,
            Err(error) => {
                let Some(readback) = read_receipt(self, fence.operation_id, fence.attempt_number)?
                else {
                    return Err(error.into());
                };
                return existing_receipt_ack(self, fence, input, &readback);
            }
        };
        let updated = read_operation(self, fence.operation_id)?.ok_or(RuntimeError::Conflict)?;
        validate_operation_target(self, &updated, fence)?;
        Ok(AgentProfileProviderDeletionReceiptSummary {
            fence: fence.clone(),
            state: updated.state(),
            observed_at_ms: recorded.observed_at().get(),
        })
    }
}

fn existing_receipt_ack(
    runtime: &OrchestrationRuntime,
    fence: &AgentProfileProviderDeletionFence,
    input: &AgentProfileProviderDeletionReceiptInput,
    receipt: &AgentProfileProviderDeletionReceipt,
) -> Result<AgentProfileProviderDeletionReceiptSummary, RuntimeError> {
    if !dispatch_matches_fence(receipt.dispatch(), fence)
        || !receipt_payload_matches(receipt, input)
    {
        return Err(RuntimeError::Conflict);
    }
    let operation = read_operation(runtime, fence.operation_id)?.ok_or(RuntimeError::NotFound)?;
    validate_operation_target(runtime, &operation, fence)?;
    Ok(AgentProfileProviderDeletionReceiptSummary {
        fence: fence.clone(),
        state: operation.state(),
        observed_at_ms: receipt.observed_at().get(),
    })
}

fn validate_dispatch_and_frozen_target(
    runtime: &OrchestrationRuntime,
    operation: &AgentProfileProviderDeletionOperation,
    dispatch: &AgentProfileProviderDeletionDispatch,
) -> Result<(), RuntimeError> {
    let action_state_matches = match dispatch.action() {
        AgentProfileProviderDeletionAction::Execute => {
            operation.state() == AgentProfileProviderDeletionState::EffectPending
        }
        AgentProfileProviderDeletionAction::Inspect => matches!(
            operation.state(),
            AgentProfileProviderDeletionState::EffectPending
                | AgentProfileProviderDeletionState::EffectUnknown
        ),
    };
    if operation.operation_id() != dispatch.operation_id()
        || operation.request_id() != dispatch.request_id()
        || operation.profile_id() != dispatch.profile_id()
        || operation.runtime_session_id() != dispatch.runtime_session_id()
        || operation.provider_family() != dispatch.provider_family()
        || operation.provider_conversation_id() != dispatch.provider_conversation_id()
        || operation.revision() != dispatch.revision()
        || operation.attempt_number() != dispatch.attempt_number()
        || !action_state_matches
    {
        return Err(RuntimeError::Conflict);
    }
    let fence = fence_from_dispatch(dispatch);
    validate_operation_target(runtime, operation, &fence)
}

fn validate_operation_target(
    runtime: &OrchestrationRuntime,
    operation: &AgentProfileProviderDeletionOperation,
    fence: &AgentProfileProviderDeletionFence,
) -> Result<(), RuntimeError> {
    let entry = frozen_entry(runtime, fence)?;
    if operation.operation_id() != fence.operation_id
        || operation.request_id() != fence.request_id
        || operation.profile_id() != fence.profile_id
        || operation.runtime_session_id() != fence.runtime_session_id
        || operation.provider_family() != AgentProfileProviderFamily::ChatGpt
        || operation.provider_conversation_id() != fence.provider_conversation_id.as_str()
        || operation.original_runtime_revision() != entry.original_revision()
        || operation.original_runtime_state() != entry.original_state()
    {
        return Err(RuntimeError::Conflict);
    }
    Ok(())
}

fn frozen_entry(
    runtime: &OrchestrationRuntime,
    fence: &AgentProfileProviderDeletionFence,
) -> Result<heterarchy_orchestration_sessions::AgentProfileDeletionRuntimeEntry, RuntimeError> {
    let catalog = runtime.profile_catalog_guard()?;
    let manifest = catalog
        .profile_deletion_manifest(fence.request_id)?
        .ok_or(RuntimeError::Conflict)?;
    if manifest.profile_id() != fence.profile_id {
        return Err(RuntimeError::Conflict);
    }
    let entry = catalog
        .profile_deletion_runtime_entry(fence.request_id, fence.runtime_session_id)?
        .ok_or(RuntimeError::Conflict)?;
    if !entry_matches_fence(&entry, manifest, fence) {
        return Err(RuntimeError::Conflict);
    }
    Ok(entry)
}

fn entry_matches_fence(
    entry: &AgentProfileDeletionRuntimeEntry,
    manifest: AgentProfileDeletionManifest,
    fence: &AgentProfileProviderDeletionFence,
) -> bool {
    if entry.request_id() != fence.request_id
        || entry.profile_id() != fence.profile_id
        || entry.runtime_session_id() != fence.runtime_session_id
        || manifest.request_id() != fence.request_id
        || manifest.profile_id() != fence.profile_id
    {
        return false;
    }
    matches!(
        entry.target(),
        AgentProfileDeletionRuntimeTarget::LinkedProviderConversation { conversation_id }
            if entry.provider_family() == AgentProfileProviderFamily::ChatGpt
                && conversation_id == &fence.provider_conversation_id
    )
}

fn read_operation(
    runtime: &OrchestrationRuntime,
    operation_id: Uuid,
) -> Result<Option<AgentProfileProviderDeletionOperation>, RuntimeError> {
    Ok(runtime
        .catalog()?
        .agent_profile_provider_deletion(operation_id)?)
}

fn read_receipt(
    runtime: &OrchestrationRuntime,
    operation_id: Uuid,
    attempt_number: u64,
) -> Result<Option<AgentProfileProviderDeletionReceipt>, RuntimeError> {
    Ok(runtime
        .catalog()?
        .agent_profile_provider_deletion_receipt(operation_id, attempt_number)?)
}

fn dispatch_matches_fence(
    dispatch: &AgentProfileProviderDeletionDispatch,
    fence: &AgentProfileProviderDeletionFence,
) -> bool {
    dispatch.operation_id() == fence.operation_id
        && dispatch.request_id() == fence.request_id
        && dispatch.profile_id() == fence.profile_id
        && dispatch.runtime_session_id() == fence.runtime_session_id
        && dispatch.provider_family() == AgentProfileProviderFamily::ChatGpt
        && dispatch.provider_conversation_id() == fence.provider_conversation_id.as_str()
        && dispatch.revision() == fence.expected_revision
        && dispatch.attempt_number() == fence.attempt_number
        && dispatch.action() == fence.action
}

fn fence_from_dispatch(
    dispatch: &AgentProfileProviderDeletionDispatch,
) -> AgentProfileProviderDeletionFence {
    AgentProfileProviderDeletionFence {
        operation_id: dispatch.operation_id(),
        request_id: dispatch.request_id(),
        profile_id: dispatch.profile_id(),
        runtime_session_id: dispatch.runtime_session_id(),
        provider_conversation_id: dispatch.provider_conversation_id().to_owned(),
        expected_revision: dispatch.revision(),
        attempt_number: dispatch.attempt_number(),
        action: dispatch.action(),
    }
}

fn validate_receipt_action(
    action: AgentProfileProviderDeletionAction,
    input: &AgentProfileProviderDeletionReceiptInput,
) -> Result<(), RuntimeError> {
    let valid = match input {
        AgentProfileProviderDeletionReceiptInput::NotStartedBeforeClick { .. } => {
            action == AgentProfileProviderDeletionAction::Execute
        }
        AgentProfileProviderDeletionReceiptInput::PositiveNoEffectReadback { .. } => {
            action == AgentProfileProviderDeletionAction::Inspect
        }
        AgentProfileProviderDeletionReceiptInput::ProviderDeleteConfirmed { .. } => true,
    };
    if valid {
        Ok(())
    } else {
        Err(RuntimeError::Conflict)
    }
}

fn receipt_payload_matches(
    receipt: &AgentProfileProviderDeletionReceipt,
    input: &AgentProfileProviderDeletionReceiptInput,
) -> bool {
    match (receipt.outcome(), input) {
        (
            AgentProfileProviderDeletionReceiptOutcome::NotStartedBeforeClick { reason, .. },
            AgentProfileProviderDeletionReceiptInput::NotStartedBeforeClick {
                reason: input_reason,
                browser_session_id,
                tab_id,
                document_id,
            },
        )
        | (
            AgentProfileProviderDeletionReceiptOutcome::PositiveNoEffectReadback { reason, .. },
            AgentProfileProviderDeletionReceiptInput::PositiveNoEffectReadback {
                reason: input_reason,
                browser_session_id,
                tab_id,
                document_id,
            },
        ) => {
            *reason == *input_reason
                && receipt.browser_session_id() == browser_session_id.as_deref()
                && receipt.tab_id() == tab_id.as_deref()
                && receipt.document_id() == document_id.as_deref()
        }
        (
            AgentProfileProviderDeletionReceiptOutcome::ProviderDeleteConfirmed,
            AgentProfileProviderDeletionReceiptInput::ProviderDeleteConfirmed {
                browser_session_id,
                tab_id,
                document_id,
            },
        ) => {
            receipt.browser_session_id() == Some(browser_session_id.as_str())
                && receipt.tab_id() == Some(tab_id.as_str())
                && receipt.document_id() == Some(document_id.as_str())
        }
        _ => false,
    }
}

fn build_receipt(
    dispatch: &AgentProfileProviderDeletionDispatch,
    input: &AgentProfileProviderDeletionReceiptInput,
    observed_at: TimestampMillis,
) -> Result<AgentProfileProviderDeletionReceipt, RuntimeError> {
    let retry_after = || {
        observed_at
            .get()
            .checked_add(MIN_RETRY_DELAY_MS)
            .and_then(|value| TimestampMillis::new(value).ok())
            .ok_or(RuntimeError::InvalidInput)
    };
    match input {
        AgentProfileProviderDeletionReceiptInput::NotStartedBeforeClick {
            reason,
            browser_session_id,
            tab_id,
            document_id,
        } => Ok(
            AgentProfileProviderDeletionReceipt::not_started_before_click(
                dispatch,
                *reason,
                retry_after()?,
                browser_session_id.clone(),
                tab_id.clone(),
                document_id.clone(),
                observed_at,
            )?,
        ),
        AgentProfileProviderDeletionReceiptInput::PositiveNoEffectReadback {
            reason,
            browser_session_id,
            tab_id,
            document_id,
        } => Ok(
            AgentProfileProviderDeletionReceipt::confirmed_no_effect_after_readback(
                dispatch,
                *reason,
                retry_after()?,
                browser_session_id.clone(),
                tab_id.clone(),
                document_id.clone(),
                observed_at,
            )?,
        ),
        AgentProfileProviderDeletionReceiptInput::ProviderDeleteConfirmed {
            browser_session_id,
            tab_id,
            document_id,
        } => Ok(
            AgentProfileProviderDeletionReceipt::deleted_after_positive_provider_confirmation(
                dispatch,
                Some(browser_session_id.clone()),
                Some(tab_id.clone()),
                Some(document_id.clone()),
                observed_at,
            )?,
        ),
    }
}
