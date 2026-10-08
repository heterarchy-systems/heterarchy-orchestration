//! Full bounded review reads through the existing governance owner.

use heterarchy_orchestration_operations::{LeadershipReviewQuery, LeadershipReviewSnapshot};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

impl OrchestrationRuntime {
    /// Read one review and bounded statement page without advancing governance.
    ///
    /// # Errors
    /// Rejects invalid selectors, clocks/cursors/page bounds, unverified
    /// Workspaces, foreign review identity, or catalog failures.
    pub fn leadership_review(
        &self,
        workspace_id: Uuid,
        query: LeadershipReviewQuery,
        now_ms: i64,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<Option<LeadershipReviewSnapshot>, RuntimeError> {
        if workspace_id.is_nil()
            || now_ms < 0
            || !(1..=64).contains(&limit)
            || after_sequence.is_some_and(|cursor| i64::try_from(cursor).is_err())
            || !matches!((query.request_id, query.election_id),
                (Some(id), None) | (None, Some(id)) if !id.is_nil())
        {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = self.catalog()?;
        Ok(catalog.read_leadership_review(
            forge_domain::WorkspaceId::from_uuid(workspace_id),
            query,
            now_ms,
            after_sequence,
            limit,
        )?)
    }
}
