//! Scoped readback for historical provision operations.
//!
//! Every read binds the operation to its canonical `AgentRequest` Workspace;
//! this surface exposes no provider-effect mutation.

use heterarchy_orchestration_operations::{
    AgentProvisionEffectKind, AgentProvisionFailureCode, AgentProvisionOperation,
    AgentProvisionProviderFamily, AgentProvisionUncertaintyRetirement, OperationsError,
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
    pub uncertainty_retirement: Option<AgentProvisionUncertaintyRetirement>,
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
        uncertainty_retirement: operation.uncertainty_retirement().cloned(),
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

    #[test]
    fn provider_retirement_remains_explicit_at_the_runtime_boundary() {
        assert!(matches!(
            RuntimeError::from(OperationsError::ProviderRetired),
            RuntimeError::ProviderRetired
        ));
    }
}
