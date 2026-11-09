//! Canonical Forge Workspace scope value type.

use uuid::Uuid;

use crate::OperationsError;

const MAX_LOCATOR_BYTES: usize = 4096;
pub const MAX_WORKSPACE_AUTHORITY_SCOPES: usize = 64;

/// Canonical Forge Workspace scope retained in durable operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProjectScope {
    workspace_id: Uuid,
    location_id: Uuid,
    location_revision: i64,
    workspace_revision: i64,
    locator: String,
}

impl WorkspaceProjectScope {
    /// Validate and normalize one exact `WorkspaceQueryScope`.
    ///
    /// # Errors
    /// Rejects nil identities, non-positive revisions, and oversized/empty locators.
    pub fn new(
        workspace_id: Uuid,
        location_id: Uuid,
        location_revision: i64,
        workspace_revision: i64,
        locator: String,
    ) -> Result<Self, OperationsError> {
        if workspace_id.is_nil()
            || location_id.is_nil()
            || location_revision <= 0
            || workspace_revision <= 0
            || locator.is_empty()
            || locator.len() > MAX_LOCATOR_BYTES
        {
            return Err(OperationsError::Conflict("Workspace scope"));
        }
        Ok(Self {
            workspace_id,
            location_id,
            location_revision,
            workspace_revision,
            locator,
        })
    }

    #[must_use]
    pub const fn workspace_id(&self) -> Uuid {
        self.workspace_id
    }

    #[must_use]
    pub const fn location_id(&self) -> Uuid {
        self.location_id
    }

    #[must_use]
    pub const fn location_revision(&self) -> i64 {
        self.location_revision
    }

    #[must_use]
    pub const fn workspace_revision(&self) -> i64 {
        self.workspace_revision
    }

    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
    }
}
