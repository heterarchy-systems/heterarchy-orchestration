//! Session-free operator access to Workspace `ChatGPT` Project configuration.

use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    WorkspaceProjectBinding, WorkspaceProjectBindingReceipt, WorkspaceProjectConfigureCommand,
    WorkspaceProjectConfigureOutcome, WorkspaceProjectEnrollment, WorkspaceProjectScope,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

impl OrchestrationRuntime {
    /// Read one Workspace's current `ChatGPT` Project binding for an operator.
    ///
    /// # Errors
    /// Rejects non-operators, nil Workspace identities, and catalog failures.
    pub fn workspace_project_binding(
        &self,
        principal: AuthenticatedPrincipal,
        workspace_id: Uuid,
    ) -> Result<Option<WorkspaceProjectBinding>, RuntimeError> {
        self.require_operator(principal)?;
        self.catalog()?
            .workspace_project_binding(workspace_id)
            .map_err(RuntimeError::from)
    }

    /// Configure one Project binding with exact request idempotency and CAS.
    /// This path intentionally requires no logical Session or Activity.
    ///
    /// # Errors
    /// Rejects non-operators, stale revisions, request-ID payload conflicts,
    /// and catalog failures.
    pub fn configure_workspace_project(
        &self,
        principal: AuthenticatedPrincipal,
        command: WorkspaceProjectConfigureCommand,
        static_workspace_ids: &[Uuid],
    ) -> Result<WorkspaceProjectConfigureOutcome, RuntimeError> {
        self.require_operator(principal)?;
        self.catalog()?
            .configure_workspace_project(command, static_workspace_ids)
            .map_err(RuntimeError::from)
    }

    /// Load the bounded Source enrollment projection before the Backend serves.
    ///
    /// # Errors
    /// Rejects malformed, stale, orphaned, or over-capacity persisted rows.
    pub fn workspace_project_enrollments(
        &self,
    ) -> Result<Vec<WorkspaceProjectEnrollment>, RuntimeError> {
        self.catalog()?
            .workspace_project_enrollments()
            .map_err(RuntimeError::from)
    }

    /// Read one exact configure receipt without authorizing a retry.
    ///
    /// # Errors
    /// Rejects non-operators, malformed request identities, Workspace scope
    /// mismatches, and catalog failures.
    pub fn workspace_project_receipt(
        &self,
        principal: AuthenticatedPrincipal,
        request_id: Uuid,
        scope: &WorkspaceProjectScope,
    ) -> Result<Option<WorkspaceProjectBindingReceipt>, RuntimeError> {
        self.require_operator(principal)?;
        self.catalog()?
            .workspace_project_receipt(request_id, scope)
            .map_err(RuntimeError::from)
    }
}
