//! Runtime identity resolution for exact frozen-subject ballot readback.

use forge_domain::ActivityId;
use heterarchy_orchestration_operations::LeadershipBallotSnapshot;
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

use super::{member_agent_id, require_activity, require_workspace};

impl OrchestrationRuntime {
    /// Read the caller Session's immutable ballot in one exact election.
    ///
    /// The transport boundary authenticates the principal and proves the
    /// source Workspace scope. This owner resolves only the current Ready
    /// participant binding; the election's frozen source mapping selects the
    /// canonical subject and durable ballot.
    ///
    /// # Errors
    /// Rejects invalid IDs, an unknown/non-ready member binding, or durable
    /// ballot read failures.
    pub fn leadership_ballot(
        &self,
        workspace_id: Uuid,
        activity_id: Uuid,
        session_id: Uuid,
        election_id: Uuid,
    ) -> Result<Option<LeadershipBallotSnapshot>, RuntimeError> {
        let workspace = require_workspace(workspace_id)?;
        let activity = ActivityId::from_uuid(require_activity(activity_id)?);
        if session_id.is_nil() || election_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let agent_id = member_agent_id(workspace_id, activity_id, session_id);
        let catalog = self.catalog()?;
        let participant = catalog
            .ready_participant_for_agent(workspace, activity, agent_id)?
            .ok_or(
                heterarchy_orchestration_operations::OperationsError::Conflict(
                    "leadership voter not ready",
                ),
            )?;
        Ok(catalog.leadership_ballot(workspace, election_id, participant)?)
    }
}
