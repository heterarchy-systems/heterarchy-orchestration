//! Exact durable readback for one leadership review statement.

use forge_domain::WorkspaceId;
use uuid::Uuid;

use super::{LeadershipStatementRecord, load_review_by_request, load_statement};
use crate::{OperationsCatalog, OperationsError};

/// One stored statement together with its canonical Workspace and request IDs.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipStatementReadback {
    workspace_id: WorkspaceId,
    request_id: Uuid,
    statement: LeadershipStatementRecord,
}

impl LeadershipStatementReadback {
    /// Return the Workspace that owns the stored statement.
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// Return the review request that owns the stored statement.
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    /// Return the immutable statement record.
    pub const fn statement(&self) -> &LeadershipStatementRecord {
        &self.statement
    }
}

impl OperationsCatalog {
    /// Read one exact statement from its verified Workspace's durable journal.
    ///
    /// The statement's request, election, and Workspace must still agree with
    /// the canonical review row. Historical reads remain available after the
    /// election closes or the author's current binding is removed.
    ///
    /// # Errors
    /// Returns an import, scope, corruption, storage, or identity error.
    pub fn leadership_statement(
        &self,
        workspace_id: WorkspaceId,
        message_id: Uuid,
    ) -> Result<Option<LeadershipStatementReadback>, OperationsError> {
        if workspace_id.into_uuid().is_nil() {
            return Err(OperationsError::Conflict("leadership statement Workspace"));
        }
        if message_id.is_nil() {
            return Err(OperationsError::Conflict("leadership statement identity"));
        }

        let tx = self.connection.unchecked_transaction()?;
        super::ensure_verified_workspace(&tx, workspace_id, self.destination)?;
        let Some(statement) = load_statement(&tx, message_id)? else {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(None);
        };
        if statement.workspace_id != workspace_id {
            return Err(OperationsError::Conflict("leadership statement Workspace"));
        }

        let review = load_review_by_request(&tx, statement.request_id)?.ok_or(
            OperationsError::Corrupt("leadership statement review context"),
        )?;
        if review.request_id != statement.request_id
            || review.election_id != statement.election_id
            || review.scope.workspace_id() != statement.workspace_id
        {
            return Err(OperationsError::Corrupt(
                "leadership statement review context",
            ));
        }

        let request_id = statement.request_id;
        let statement = statement.into_public();
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(Some(LeadershipStatementReadback {
            workspace_id,
            request_id,
            statement,
        }))
    }
}
