//! Recall request replay preflight through the existing governance owner.

use forge_domain::{ActivityId, AgentInstanceId, WorkspaceId};
use heterarchy_orchestration_operations::{LeadershipScope, OperationalActor};

use super::{LeadershipTarget, RecallReviewCommand, member_agent_id};
use crate::{OrchestrationRuntime, RuntimeError};

pub(super) struct ValidatedContext {
    pub(super) workspace: WorkspaceId,
    pub(super) activity: ActivityId,
    pub(super) scope: LeadershipScope,
    pub(super) actor: OperationalActor,
    pub(super) agent_id: AgentInstanceId,
    pub(super) now_ms: i64,
}

pub(super) fn validated_context(
    command: &RecallReviewCommand,
) -> Result<ValidatedContext, RuntimeError> {
    let workspace = super::require_workspace(command.workspace_id)?;
    let activity = ActivityId::from_uuid(super::require_activity(command.activity_id)?);
    let session = super::require_session(command.session_id)?;
    if command.expected_revision < 1 {
        return Err(RuntimeError::InvalidInput);
    }
    let now_ms = super::timestamp(command.now_ms)?;
    let scope = match command.target {
        LeadershipTarget::Workspace => LeadershipScope::workspace(workspace),
        LeadershipTarget::Activity => LeadershipScope::activity(workspace, activity),
        LeadershipTarget::Team(team) => {
            LeadershipScope::team(workspace, super::require_non_nil(team)?)
        }
    };
    let agent_id = member_agent_id(
        command.workspace_id,
        command.activity_id,
        command.session_id,
    );
    let actor = OperationalActor::Participant {
        activity_id: activity,
        agent_id,
        session_id: session,
    };
    Ok(ValidatedContext {
        workspace,
        activity,
        scope,
        actor,
        agent_id,
        now_ms,
    })
}

impl OrchestrationRuntime {
    /// Probe whether this request ID has a stored scope/proposer-compatible
    /// replay candidate before the caller performs live Ready admission.
    ///
    /// This is a preflight only. The subsequent `request_leadership_review`
    /// call remains the authority that validates revision, candidate, reason,
    /// and complete replay identity.
    ///
    /// # Errors
    /// Rejects invalid request identity/context, a foreign request context,
    /// catalog failures, or a poisoned owner.
    pub fn leadership_review_replay_exists(
        &self,
        command: &RecallReviewCommand,
    ) -> Result<bool, RuntimeError> {
        if command.request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let context = validated_context(command)?;
        let catalog = self.catalog()?;
        Ok(catalog
            .leadership_review_replay_candidate(
                context.workspace,
                context.scope,
                command.request_id,
                context.actor,
            )?
            .is_some())
    }
}
