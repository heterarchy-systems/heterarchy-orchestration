//! Retired current Chat route projection with compatibility-only DTOs.

use forge_domain::{ActivityId, WorkspaceId};
use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};
use uuid::Uuid;

use crate::{OperationsCatalog, OperationsError};

/// One confirmed provider binding for an exact Workspace/Profile route.
///
/// This record proves only that Operations has a confirmed provider binding;
/// it does not prove current Forge runtime readiness or logical membership.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChatTarget {
    operation_id: Uuid,
    activity_id: ActivityId,
    profile_id: AgentProfileId,
    participation_id: crate::WorkspaceParticipationId,
    runtime_session_id: AgentProfileRuntimeSessionId,
    conversation: String,
}

impl WorkspaceChatTarget {
    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> crate::WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn runtime_session_id(&self) -> AgentProfileRuntimeSessionId {
        self.runtime_session_id
    }

    #[must_use]
    pub fn provider_conversation_id(&self) -> &str {
        &self.conversation
    }
}

/// Bounded Chat route page. `truncated` is explicit so callers cannot present
/// a partial candidate set as a complete Workspace authority projection.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChatTargetPage {
    targets: Vec<WorkspaceChatTarget>,
    truncated: bool,
}

impl WorkspaceChatTargetPage {
    pub fn targets(&self) -> &[WorkspaceChatTarget] {
        &self.targets
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

impl OperationsCatalog {
    /// Retired current provider Chat route projection.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading
    /// or writing the catalog. Exact historical provisioning and outbox rows
    /// remain readable through their owning read APIs.
    pub fn workspace_chat_targets(
        &self,
        workspace_id: WorkspaceId,
        profile_id: AgentProfileId,
        limit: usize,
    ) -> Result<WorkspaceChatTargetPage, OperationsError> {
        let _ = (workspace_id, profile_id, limit);
        Err(OperationsError::ProviderRetired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, fs};

    #[test]
    fn current_workspace_chat_route_projection_is_retired() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "heterarchy-workspace-chat-retired-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let catalog = OperationsCatalog::open(&path, Uuid::now_v7())?;
        assert!(matches!(
            catalog.workspace_chat_targets(
                WorkspaceId::from_uuid(Uuid::now_v7()),
                AgentProfileId::from_uuid(Uuid::now_v7())?,
                8,
            ),
            Err(OperationsError::ProviderRetired)
        ));
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
