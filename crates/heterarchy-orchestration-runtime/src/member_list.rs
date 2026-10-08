//! Caller-authorized Workspace member observations from the canonical owner.

use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::VisibleLogicalMember;
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

impl OrchestrationRuntime {
    /// Read up to 64 current logical members in the owner's deterministic order.
    /// Rows describe current bindings; they do not grant execution authority.
    ///
    /// # Errors
    /// Rejects invalid bounds/Workspace selectors, unauthorized callers and corrupt storage.
    pub fn list_members(
        &self,
        workspace_id: Uuid,
        session_id: Option<Uuid>,
        principal: AuthenticatedPrincipal,
        limit: u32,
    ) -> Result<Vec<VisibleLogicalMember>, RuntimeError> {
        if !(1..=64).contains(&limit) {
            return Err(RuntimeError::InvalidInput);
        }
        self.require_workspace_read(workspace_id, session_id, principal)?;
        let catalog = self.catalog()?;
        catalog
            .visible_logical_members(WorkspaceId::from_uuid(workspace_id), limit)
            .map_err(Into::into)
    }
}
