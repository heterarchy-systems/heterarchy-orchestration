//! `Workspace`-owned `AgentRequest` read projections.

use uuid::Uuid;

use crate::{AgentRequestSummary, OrchestrationRuntime, RuntimeError, agent_request_summary};

impl OrchestrationRuntime {
    /// Read an `AgentRequest` only when it belongs to the requested `Workspace`.
    ///
    /// Workspace ownership is checked against the durable record before the
    /// transport summary is projected, so a foreign request is indistinguishable
    /// from an absent request.
    ///
    /// # Errors
    /// Returns invalid identities or durable catalog failures.
    pub fn agent_request_in_workspace(
        &self,
        workspace_id: Uuid,
        request_id: Uuid,
    ) -> Result<Option<AgentRequestSummary>, RuntimeError> {
        if workspace_id.is_nil() || request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = self.catalog()?;
        let Some(record) = catalog.agent_request(request_id)? else {
            return Ok(None);
        };
        if record.workspace_id().into_uuid() != workspace_id {
            return Ok(None);
        }
        Ok(Some(agent_request_summary(&record)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provisioning::AgentRequestSubmit;
    use std::error::Error;

    #[test]
    fn request_lookup_returns_only_the_exact_workspace_owner() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("agent-request-namespace-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
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
            objective: "verify the Workspace-owned request read".to_owned(),
            capability: "verification".to_owned(),
            count: 1,
            effect_scope: heterarchy_orchestration_operations::AgentRequestEffectScope::ReadOnly,
            independence: heterarchy_orchestration_operations::AgentRequestIndependence::None,
            reason: "keep request identity scoped".to_owned(),
            created_at_ms: 10,
        })?;

        let request = runtime
            .agent_request_in_workspace(workspace_id, request_id)?
            .ok_or("owned request was not returned")?;
        assert_eq!(request.request_id, request_id);
        assert!(
            runtime
                .agent_request_in_workspace(Uuid::from_u128(0xB101), request_id)?
                .is_none()
        );
        assert!(
            runtime
                .agent_request_in_workspace(workspace_id, Uuid::from_u128(0xB105))?
                .is_none()
        );

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
