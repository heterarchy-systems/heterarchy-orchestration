//! Scoped full projections and cancellation for durable provision operations.
//!
//! The Operations catalog remains the mutation owner. These methods bind an
//! operation to its canonical `AgentRequest` Workspace before reading or
//! cancelling it, so a globally unique operation ID cannot cross a scope.

use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    AgentProvisionEffectKind, AgentProvisionFailureCode, AgentProvisionOperation,
    AgentProvisionProviderFamily, OperationsError,
};
use uuid::Uuid;

use super::provision_state_label;
use crate::{OrchestrationRuntime, RuntimeError};

/// Full transport-neutral projection of one durable provision operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionOperationFullSummary {
    pub operation_id: Uuid,
    pub request_id: Uuid,
    pub slot_ordinal: u8,
    pub effect_kind: &'static str,
    pub provider_family: &'static str,
    pub profile_id: Option<Uuid>,
    pub runtime_session_id: Option<Uuid>,
    pub state: &'static str,
    pub provider_conversation_id: Option<String>,
    pub reported_failure_code: Option<&'static str>,
    pub prepared_at_ms: i64,
    pub updated_at_ms: i64,
    pub revision: u64,
}

/// Cancellation result retaining whether this call committed a state change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionOperationCancelSummary {
    pub operation: ProvisionOperationFullSummary,
    pub changed: bool,
}

impl OrchestrationRuntime {
    /// Read one operation only when its canonical `AgentRequest` belongs to the
    /// requested Workspace.
    ///
    /// # Errors
    /// Returns invalid identities, forbidden cross-Workspace selectors, or
    /// durable catalog failures.
    pub fn provision_operation(
        &self,
        workspace_id: Uuid,
        operation_id: Uuid,
    ) -> Result<Option<ProvisionOperationFullSummary>, RuntimeError> {
        validate_identity(workspace_id)?;
        validate_identity(operation_id)?;
        let catalog = self.catalog()?;
        let Some(operation) = catalog.agent_provision_operation(operation_id)? else {
            return Ok(None);
        };
        ensure_operation_workspace(&catalog, workspace_id, &operation)?;
        Ok(Some(full_summary(&operation)))
    }

    /// Cancel one prepared operation after binding its request Workspace in
    /// the same held Operations owner guard. Already-cancelled operations are
    /// returned unchanged regardless of the supplied expected revision.
    ///
    /// # Errors
    /// Returns invalid identities/timestamps, forbidden cross-Workspace
    /// selectors, illegal lifecycle transitions, stale fences, or catalog
    /// failures.
    pub fn cancel_provision_operation(
        &self,
        workspace_id: Uuid,
        operation_id: Uuid,
        expected_revision: u64,
        now_ms: i64,
    ) -> Result<ProvisionOperationCancelSummary, RuntimeError> {
        validate_identity(workspace_id)?;
        validate_identity(operation_id)?;
        if expected_revision == 0 {
            return Err(RuntimeError::InvalidInput);
        }
        let observed_at = TimestampMillis::new(now_ms).map_err(|_| RuntimeError::InvalidInput)?;
        let mut catalog = self.catalog()?;
        let operation = catalog
            .agent_provision_operation(operation_id)?
            .ok_or(RuntimeError::NotFound)?;
        ensure_operation_workspace(&catalog, workspace_id, &operation)?;
        let before_revision = operation.revision();
        let before_state = operation.state();
        let operation = catalog.cancel_agent_provision_operation(
            operation_id,
            expected_revision,
            observed_at,
        )?;
        let changed = operation.revision() != before_revision || operation.state() != before_state;
        Ok(ProvisionOperationCancelSummary {
            operation: full_summary(&operation),
            changed,
        })
    }
}

fn validate_identity(value: Uuid) -> Result<(), RuntimeError> {
    if value.is_nil() {
        Err(RuntimeError::InvalidInput)
    } else {
        Ok(())
    }
}

fn ensure_operation_workspace(
    catalog: &heterarchy_orchestration_operations::OperationsCatalog,
    workspace_id: Uuid,
    operation: &AgentProvisionOperation,
) -> Result<(), RuntimeError> {
    let request = catalog
        .agent_request(operation.request_id())?
        .ok_or(OperationsError::Corrupt("Agent provision request missing"))?;
    if request.workspace_id().into_uuid() != workspace_id {
        return Err(RuntimeError::Forbidden);
    }
    Ok(())
}

pub(super) fn full_summary(operation: &AgentProvisionOperation) -> ProvisionOperationFullSummary {
    ProvisionOperationFullSummary {
        operation_id: operation.operation_id(),
        request_id: operation.request_id(),
        slot_ordinal: operation.slot_ordinal(),
        effect_kind: effect_kind_label(operation.effect_kind()),
        provider_family: provider_family_label(operation.provider_family()),
        profile_id: operation
            .profile_id()
            .map(heterarchy_orchestration_sessions::AgentProfileId::into_uuid),
        runtime_session_id: operation
            .runtime_session_id()
            .map(heterarchy_orchestration_sessions::AgentProfileRuntimeSessionId::into_uuid),
        state: provision_state_label(operation.state()),
        provider_conversation_id: operation.provider_conversation_id().map(str::to_owned),
        reported_failure_code: operation
            .reported_failure_code()
            .map(AgentProvisionFailureCode::as_str),
        prepared_at_ms: operation.prepared_at().get(),
        updated_at_ms: operation.updated_at().get(),
        revision: operation.revision(),
    }
}

const fn effect_kind_label(kind: AgentProvisionEffectKind) -> &'static str {
    match kind {
        AgentProvisionEffectKind::WakeExistingRuntime => "wake_existing_runtime",
        AgentProvisionEffectKind::WakeNewRuntime => "wake_new_runtime",
        AgentProvisionEffectKind::ProvisionNew => "provision_new",
        AgentProvisionEffectKind::ReuseReadyRuntime => "reuse_ready_runtime",
    }
}

const fn provider_family_label(family: AgentProvisionProviderFamily) -> &'static str {
    match family {
        AgentProvisionProviderFamily::ChatGpt => "chatgpt",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provisioning::{
        AgentRequestEffectScope, AgentRequestIndependence, AgentRequestSubmit,
    };
    use heterarchy_orchestration_operations::AgentProvisionState;

    #[test]
    fn scoped_operation_query_cancel_and_cancel_noop_use_canonical_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("provision-operation-scope-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let workspace_id = Uuid::from_u128(0xA101);
        let request_id = Uuid::from_u128(0xA105);
        runtime.submit_agent_request(AgentRequestSubmit {
            workspace_id,
            activity_id: Uuid::from_u128(0xA102),
            requester_agent_id: Uuid::from_u128(0xA103),
            requester_session_id: Uuid::from_u128(0xA104),
            coordinator_revision: 1,
            request_id,
            objective: "prepare one provider target".to_owned(),
            capability: "chat".to_owned(),
            count: 2,
            effect_scope: AgentRequestEffectScope::SourceWrite,
            independence: AgentRequestIndependence::None,
            reason: "operation scope test".to_owned(),
            created_at_ms: 1,
        })?;
        runtime.reserve_agent_request_resolution(request_id, 2, 3)?;
        let prepared = runtime.prepare_agent_request_effect_targets(request_id, 6, 7)?;
        assert_eq!(prepared.len(), 2);
        let operation = &prepared[0];
        let queried = runtime
            .provision_operation(workspace_id, operation.operation_id)?
            .ok_or("operation query")?;
        assert_eq!(queried.operation_id, operation.operation_id);
        assert!(matches!(
            runtime.provision_operation(Uuid::from_u128(0xA999), operation.operation_id),
            Err(RuntimeError::Forbidden)
        ));
        assert!(matches!(
            runtime.cancel_provision_operation(
                Uuid::from_u128(0xA999),
                operation.operation_id,
                operation.revision,
                8,
            ),
            Err(RuntimeError::Forbidden)
        ));
        let unchanged = runtime
            .provision_operation(workspace_id, operation.operation_id)?
            .ok_or("foreign cancel readback")?;
        assert_eq!(unchanged.revision, operation.revision);

        let cancelled = runtime.cancel_provision_operation(
            workspace_id,
            operation.operation_id,
            operation.revision,
            8,
        )?;
        assert!(cancelled.changed);
        assert_eq!(cancelled.operation.state, "cancelled");
        let noop =
            runtime.cancel_provision_operation(workspace_id, operation.operation_id, 999, 9)?;
        assert!(!noop.changed);
        assert_eq!(noop.operation.revision, cancelled.operation.revision);

        let released = {
            let mut catalog = runtime.catalog()?;
            catalog
                .next_agent_provision_dispatch(TimestampMillis::new(10)?)?
                .ok_or("pending dispatch")?
                .operation()
                .clone()
        };
        assert_eq!(released.state(), AgentProvisionState::EffectPending);
        assert!(matches!(
            runtime.cancel_provision_operation(
                workspace_id,
                released.operation_id(),
                released.revision(),
                11,
            ),
            Err(RuntimeError::Conflict)
        ));

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
