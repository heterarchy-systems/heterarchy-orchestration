//! Operator-authorized provider work release and reconciliation.
//!
//! Operations owns the durable provider-effect journal. This module binds its
//! opaque fences to the Sessions Profile/Runtime state before external work is
//! released or confirmed. It contains no transport or serialization types.

use forge_domain::{AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_operations::{
    AgentProvisionDispatch, AgentProvisionDispatchAction, AgentProvisionDispatchOutcome,
    AgentProvisionEffectKind, AgentProvisionFailureCode, AgentProvisionOperation,
    AgentProvisionState,
};
use heterarchy_orchestration_sessions::{
    AgentProfileLifecycleState, AgentProfileProviderFamily, AgentProfileRuntimeSessionState,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

/// A released action or a durable reconciliation that released no action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvisionWorkerOutcome {
    Action(AgentProvisionDispatchOutcome),
    Reconciled(AgentProvisionOperation),
}

impl ProvisionWorkerOutcome {
    #[must_use]
    pub fn operation(&self) -> &AgentProvisionOperation {
        match self {
            Self::Action(outcome) => outcome.operation(),
            Self::Reconciled(operation) => operation,
        }
    }

    #[must_use]
    pub fn dispatch(&self) -> Option<AgentProvisionDispatch> {
        match self {
            Self::Action(outcome) => outcome.dispatch(),
            Self::Reconciled(_) => None,
        }
    }
}

impl OrchestrationRuntime {
    /// Observe the current in-flight selection; no action or retry is admitted.
    ///
    /// # Errors
    /// Rejects nonoperators and corrupt/ambiguous Operations evidence.
    pub fn provider_worker_in_flight_provision(
        &self,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<AgentProvisionOperation>, RuntimeError> {
        self.require_operator(principal)?;
        Ok(self.catalog()?.agent_provision_in_flight_operation()?)
    }

    /// Resolve provider routing for one exact released operation revision.
    /// This is a frozen worker selector, never an independent permission grant.
    ///
    /// # Errors
    /// Rejects nonoperators, stale operation revisions and changed Profile/Runtime identity.
    pub fn provider_worker_provision_conversation(
        &self,
        principal: AuthenticatedPrincipal,
        operation_id: Uuid,
        expected_revision: u64,
    ) -> Result<Option<String>, RuntimeError> {
        self.require_operator(principal)?;
        if operation_id.is_nil() || expected_revision == 0 {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = self.catalog()?;
        let operation = catalog
            .agent_provision_operation(operation_id)?
            .ok_or(RuntimeError::NotFound)?;
        if operation.revision() != expected_revision {
            return Err(RuntimeError::Conflict);
        }
        let profiles = self.profile_catalog_guard()?;
        let target = operation
            .runtime_session_id()
            .ok_or(RuntimeError::Conflict)?;
        let runtime = profiles
            .runtime_session(target)?
            .ok_or(RuntimeError::Conflict)?;
        if Some(runtime.profile_id()) != operation.profile_id()
            || runtime.provider_family() != AgentProfileProviderFamily::ChatGpt
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(runtime.provider_conversation_id().map(str::to_owned))
    }

    /// Admit at most one global provider provision operation for the trusted
    /// operator worker. A stale target is failed before its Execute fence is
    /// returned; an invalid Inspect remains Unknown because its effect may have
    /// escaped earlier.
    ///
    /// # Errors
    /// Rejects non-operator callers, invalid timestamps, or catalog failures.
    pub fn provider_worker_next_provision(
        &self,
        principal: AuthenticatedPrincipal,
        observed_at_ms: i64,
    ) -> Result<Option<ProvisionWorkerOutcome>, RuntimeError> {
        self.require_operator(principal)?;
        let observed_at = timestamp(observed_at_ms)?;
        let outcome = self.catalog()?.next_agent_provision_dispatch(observed_at)?;
        let Some(outcome) = outcome else {
            return Ok(None);
        };
        let Some(dispatch) = outcome.dispatch() else {
            return Ok(Some(ProvisionWorkerOutcome::Action(outcome)));
        };

        if !self.runtime_binding_is_intact(&dispatch)? {
            let operation = match dispatch.action() {
                AgentProvisionDispatchAction::Execute => self.fail_provision_effect(
                    dispatch.operation_id(),
                    dispatch.operation_revision(),
                    observed_at,
                    Some(AgentProvisionFailureCode::DocumentIdentityUnavailable),
                    false,
                )?,
                AgentProvisionDispatchAction::Inspect => self.mark_provision_effect_unknown(
                    dispatch.operation_id(),
                    dispatch.operation_revision(),
                    observed_at,
                )?,
            };
            return Ok(Some(ProvisionWorkerOutcome::Reconciled(operation)));
        }

        if dispatch.action() == AgentProvisionDispatchAction::Execute
            && !self.fresh_profile_admission_is_open(&dispatch)?
        {
            // This Execute has not escaped the Runtime. The same operation is
            // made terminal; no second operation or provider retry is created.
            self.fail_runtime_if_unbound(outcome.operation(), observed_at)?;
            let operation = self.fail_provision_effect(
                dispatch.operation_id(),
                dispatch.operation_revision(),
                observed_at,
                None,
                false,
            )?;
            return Ok(Some(ProvisionWorkerOutcome::Reconciled(operation)));
        }

        Ok(Some(ProvisionWorkerOutcome::Action(outcome)))
    }

    /// Read one exact provision operation after a lost worker response. This
    /// does not reserve or dispatch provider work.
    ///
    /// # Errors
    /// Rejects non-operator callers, nil identities, or catalog failures.
    pub fn provider_worker_inspect_provision(
        &self,
        principal: AuthenticatedPrincipal,
        operation_id: Uuid,
    ) -> Result<Option<AgentProvisionOperation>, RuntimeError> {
        self.require_operator(principal)?;
        if operation_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        self.catalog()?
            .agent_provision_operation(operation_id)
            .map_err(RuntimeError::from)
    }

    /// Reconcile a lost provision dispatch/result acknowledgement as Unknown.
    /// This never retries the provider effect.
    ///
    /// # Errors
    /// Rejects non-operator callers, invalid identities/fences, or catalog
    /// failures.
    pub fn provider_worker_mark_provision_unknown(
        &self,
        principal: AuthenticatedPrincipal,
        operation_id: Uuid,
        expected_revision: u64,
        observed_at_ms: i64,
    ) -> Result<AgentProvisionOperation, RuntimeError> {
        self.require_operator(principal)?;
        if operation_id.is_nil() || expected_revision == 0 {
            return Err(RuntimeError::InvalidInput);
        }
        let observed_at = timestamp(observed_at_ms)?;
        self.mark_provision_effect_unknown(operation_id, expected_revision, observed_at)
    }

    /// Record a definite provider failure after the worker established that the
    /// admitted effect did not succeed. Unknown outcomes are never inferred to
    /// be failures from a timeout.
    ///
    /// # Errors
    /// Rejects non-operator callers, invalid identities/fences, and invalid
    /// lifecycle transitions.
    pub fn provider_worker_fail_provision(
        &self,
        principal: AuthenticatedPrincipal,
        operation_id: Uuid,
        expected_revision: u64,
        observed_at_ms: i64,
        failure_code: Option<AgentProvisionFailureCode>,
    ) -> Result<AgentProvisionOperation, RuntimeError> {
        self.require_operator(principal)?;
        if operation_id.is_nil() || expected_revision == 0 {
            return Err(RuntimeError::InvalidInput);
        }
        let observed_at = timestamp(observed_at_ms)?;
        self.fail_provision_effect(
            operation_id,
            expected_revision,
            observed_at,
            failure_code,
            true,
        )
    }

    /// Confirm an observed provider conversation. Sessions binds the exact
    /// provider identity first; Operations is confirmed second, so a lost
    /// Operations commit can be recovered by replaying the same identity.
    ///
    /// # Errors
    /// Rejects non-operator callers, changed targets, stale fences, invalid
    /// transitions, or catalog failures.
    pub fn provider_worker_confirm_provision(
        &self,
        principal: AuthenticatedPrincipal,
        operation_id: Uuid,
        expected_revision: u64,
        provider_conversation_id: &str,
        observed_at_ms: i64,
    ) -> Result<AgentProvisionOperation, RuntimeError> {
        self.require_operator(principal)?;
        if operation_id.is_nil() || expected_revision == 0 {
            return Err(RuntimeError::InvalidInput);
        }
        let observed_at = timestamp(observed_at_ms)?;

        let operation = self
            .catalog()?
            .agent_provision_operation(operation_id)?
            .ok_or(RuntimeError::NotFound)?;
        if operation.state() == AgentProvisionState::Confirmed {
            if operation.provider_conversation_id() == Some(provider_conversation_id) {
                return Ok(operation);
            }
            return Err(RuntimeError::Conflict);
        }
        validate_effect_fence(&operation, expected_revision, observed_at)?;
        self.confirm_runtime_conversation(&operation, provider_conversation_id, observed_at)?;

        self.catalog()?
            .confirm_agent_provision_conversation(
                operation_id,
                expected_revision,
                provider_conversation_id,
                observed_at,
            )
            .map_err(RuntimeError::from)
    }

    fn mark_provision_effect_unknown(
        &self,
        operation_id: Uuid,
        expected_revision: u64,
        observed_at: TimestampMillis,
    ) -> Result<AgentProvisionOperation, RuntimeError> {
        self.catalog()?
            .mark_agent_provision_effect_unknown(operation_id, expected_revision, observed_at)
            .map_err(RuntimeError::from)
    }

    fn fail_provision_effect(
        &self,
        operation_id: Uuid,
        expected_revision: u64,
        observed_at: TimestampMillis,
        failure_code: Option<AgentProvisionFailureCode>,
        reconcile_profile: bool,
    ) -> Result<AgentProvisionOperation, RuntimeError> {
        let operation = self
            .catalog()?
            .agent_provision_operation(operation_id)?
            .ok_or(RuntimeError::NotFound)?;
        if operation.state() != AgentProvisionState::Failed {
            validate_effect_fence(&operation, expected_revision, observed_at)?;
            if reconcile_profile {
                self.fail_runtime_if_unbound(&operation, observed_at)?;
            }
        }
        self.catalog()?
            .fail_agent_provision_effect(operation_id, expected_revision, observed_at, failure_code)
            .map_err(RuntimeError::from)
    }

    fn runtime_binding_is_intact(
        &self,
        dispatch: &AgentProvisionDispatch,
    ) -> Result<bool, RuntimeError> {
        let Some(runtime) = self
            .profile_catalog_guard()?
            .runtime_session(dispatch.runtime_session_id())?
        else {
            return Ok(false);
        };
        Ok(runtime.profile_id() == dispatch.profile_id()
            && runtime.provider_family() == AgentProfileProviderFamily::ChatGpt)
    }

    fn fresh_profile_admission_is_open(
        &self,
        dispatch: &AgentProvisionDispatch,
    ) -> Result<bool, RuntimeError> {
        let catalog = self.catalog()?;
        let operation = catalog
            .agent_provision_operation(dispatch.operation_id())?
            .ok_or(RuntimeError::NotFound)?;
        let resolution = catalog
            .agent_request_resolution(operation.request_id())?
            .ok_or(RuntimeError::Conflict)?;
        let selection = resolution
            .slots()
            .iter()
            .find(|slot| slot.selection().slot_ordinal() == operation.slot_ordinal())
            .ok_or(RuntimeError::Conflict)?
            .selection();
        let profiles = self.profile_catalog_guard()?;
        let profile = profiles
            .resolve(dispatch.profile_id())?
            .ok_or(RuntimeError::NotFound)?;
        let runtime = profiles
            .runtime_session(dispatch.runtime_session_id())?
            .ok_or(RuntimeError::NotFound)?;
        if profile.lifecycle_state() != AgentProfileLifecycleState::Active
            || catalog
                .profile_deletion_barrier(dispatch.profile_id())?
                .is_some()
        {
            return Ok(false);
        }
        Ok(match dispatch.effect_kind() {
            AgentProvisionEffectKind::ProvisionNew | AgentProvisionEffectKind::WakeNewRuntime => {
                runtime.state() == AgentProfileRuntimeSessionState::Provisioning
                    && runtime.provider_conversation_id().is_none()
            }
            AgentProvisionEffectKind::WakeExistingRuntime
            | AgentProvisionEffectKind::ReuseReadyRuntime => {
                let expected_state =
                    if dispatch.effect_kind() == AgentProvisionEffectKind::WakeExistingRuntime {
                        AgentProfileRuntimeSessionState::Offline
                    } else {
                        AgentProfileRuntimeSessionState::Ready
                    };
                selection.profile_id() == Some(dispatch.profile_id())
                    && selection.runtime_session_id() == Some(dispatch.runtime_session_id())
                    && selection.runtime_session_revision() == Some(runtime.revision().get())
                    && runtime.state() == expected_state
                    && runtime.provider_conversation_id().is_some()
            }
        })
    }

    fn confirm_runtime_conversation(
        &self,
        operation: &AgentProvisionOperation,
        provider_conversation_id: &str,
        observed_at: TimestampMillis,
    ) -> Result<(), RuntimeError> {
        if provider_conversation_id.is_empty() || provider_conversation_id.len() > 512 {
            return Err(RuntimeError::InvalidInput);
        }
        let profile_id = operation.profile_id().ok_or(RuntimeError::Conflict)?;
        let runtime_session_id = operation
            .runtime_session_id()
            .ok_or(RuntimeError::Conflict)?;
        let mut profiles = self.profile_catalog_guard()?;
        let runtime = profiles
            .runtime_session(runtime_session_id)?
            .ok_or(RuntimeError::Conflict)?;
        if runtime.profile_id() != profile_id
            || runtime.provider_family() != AgentProfileProviderFamily::ChatGpt
        {
            return Err(RuntimeError::Conflict);
        }
        match operation.effect_kind() {
            AgentProvisionEffectKind::ProvisionNew | AgentProvisionEffectKind::WakeNewRuntime => {
                confirm_new_runtime(
                    &mut profiles,
                    &runtime,
                    provider_conversation_id,
                    observed_at,
                )?;
            }
            AgentProvisionEffectKind::WakeExistingRuntime => {
                confirm_existing_runtime(
                    &mut profiles,
                    &runtime,
                    provider_conversation_id,
                    observed_at,
                )?;
            }
            AgentProvisionEffectKind::ReuseReadyRuntime => {
                confirm_ready_runtime(&runtime, provider_conversation_id)?;
            }
        }
        Ok(())
    }

    fn fail_runtime_if_unbound(
        &self,
        operation: &AgentProvisionOperation,
        observed_at: TimestampMillis,
    ) -> Result<(), RuntimeError> {
        if matches!(
            operation.effect_kind(),
            AgentProvisionEffectKind::WakeExistingRuntime
                | AgentProvisionEffectKind::ReuseReadyRuntime
        ) {
            return Ok(());
        }
        let runtime_session_id = operation
            .runtime_session_id()
            .ok_or(RuntimeError::Conflict)?;
        let mut profiles = self.profile_catalog_guard()?;
        let runtime = profiles
            .runtime_session(runtime_session_id)?
            .ok_or(RuntimeError::Conflict)?;
        if operation.profile_id() != Some(runtime.profile_id())
            || runtime.provider_family() != AgentProfileProviderFamily::ChatGpt
        {
            return Err(RuntimeError::Conflict);
        }
        match runtime.state() {
            AgentProfileRuntimeSessionState::Provisioning => {
                profiles.transition_runtime_session(
                    runtime.id(),
                    runtime.revision(),
                    AgentProfileRuntimeSessionState::Failed,
                    observed_at,
                )?;
            }
            AgentProfileRuntimeSessionState::Failed | AgentProfileRuntimeSessionState::Closed => {}
            _ => return Err(RuntimeError::Conflict),
        }
        Ok(())
    }
}

fn confirm_new_runtime(
    catalog: &mut heterarchy_orchestration_sessions::AgentProfileCatalog,
    runtime: &heterarchy_orchestration_sessions::AgentProfileRuntimeSession,
    provider_conversation_id: &str,
    observed_at: TimestampMillis,
) -> Result<(), RuntimeError> {
    match runtime.state() {
        AgentProfileRuntimeSessionState::Provisioning => {
            catalog.confirm_runtime_session_ready(
                runtime.id(),
                runtime.revision(),
                provider_conversation_id,
                observed_at,
            )?;
        }
        AgentProfileRuntimeSessionState::Ready
            if runtime.provider_conversation_id() == Some(provider_conversation_id) => {}
        _ => return Err(RuntimeError::Conflict),
    }
    Ok(())
}

fn confirm_existing_runtime(
    catalog: &mut heterarchy_orchestration_sessions::AgentProfileCatalog,
    runtime: &heterarchy_orchestration_sessions::AgentProfileRuntimeSession,
    provider_conversation_id: &str,
    observed_at: TimestampMillis,
) -> Result<(), RuntimeError> {
    if runtime.provider_conversation_id() != Some(provider_conversation_id)
        || !matches!(
            runtime.state(),
            AgentProfileRuntimeSessionState::Offline
                | AgentProfileRuntimeSessionState::Waking
                | AgentProfileRuntimeSessionState::Ready
        )
    {
        return Err(RuntimeError::Conflict);
    }
    catalog.confirm_existing_runtime_wake_observed(
        runtime.id(),
        runtime.revision(),
        provider_conversation_id,
        observed_at,
    )?;
    Ok(())
}

fn confirm_ready_runtime(
    runtime: &heterarchy_orchestration_sessions::AgentProfileRuntimeSession,
    provider_conversation_id: &str,
) -> Result<(), RuntimeError> {
    if runtime.state() != AgentProfileRuntimeSessionState::Ready
        || runtime.provider_conversation_id() != Some(provider_conversation_id)
    {
        return Err(RuntimeError::Conflict);
    }
    Ok(())
}

fn validate_effect_fence(
    operation: &AgentProvisionOperation,
    expected_revision: u64,
    observed_at: TimestampMillis,
) -> Result<(), RuntimeError> {
    if expected_revision == 0
        || operation.revision() != expected_revision
        || observed_at < operation.updated_at()
        || !matches!(
            operation.state(),
            AgentProvisionState::EffectPending | AgentProvisionState::EffectUnknown
        )
    {
        return Err(RuntimeError::Conflict);
    }
    Ok(())
}

fn timestamp(value: i64) -> Result<TimestampMillis, RuntimeError> {
    TimestampMillis::new(value).map_err(|_| RuntimeError::InvalidInput)
}

#[cfg(test)]
mod tests;
