//! Runtime access to Operations-owned T2 defer observations.
//!
//! The Runtime authenticates the caller's current Workspace binding and passes
//! a trusted clock callback through to Operations. It does not evaluate review
//! evidence, decide performance, or select a Leader.

use forge_domain::{ActivityId, AgentSessionId, AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    MissionEvaluationAuthority, MissionEvaluationPlanId, MissionId, MissionT2CheckpointId,
    MissionT2DeferCommand, MissionT2DeferContent, MissionT2DeferOutcome,
    MissionT2DeferredCheckpoint, OperationalActor, OperationsError,
};
use uuid::Uuid;

use crate::{
    OrchestrationRuntime, RuntimeError, leadership::member_agent_id,
    mission_evaluation::MissionEvaluationParticipantSpec, participation::ParticipationTarget,
};

/// Immutable caller intent to record one deferred T2 check.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionT2DeferSpec {
    pub request_id: Uuid,
    pub checkpoint_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub expected_revision: u64,
    pub expected_previous_checkpoint_id: Option<Uuid>,
    pub reason: String,
    pub next_check_condition: String,
    pub next_check_at_ms: i64,
    pub participant: MissionEvaluationParticipantSpec,
}

/// Immutable request content sufficient for current-Orchestrator recovery.
///
/// It intentionally omits the original Activity, Agent, Session, and
/// leadership epoch. Operations returns those values from the persisted source
/// record; they are never fabricated by a recovery caller.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionT2DeferContentSpec {
    pub request_id: Uuid,
    pub checkpoint_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub expected_revision: u64,
    pub expected_previous_checkpoint_id: Option<Uuid>,
    pub reason: String,
    pub next_check_condition: String,
    pub next_check_at_ms: i64,
}

impl OrchestrationRuntime {
    /// Record a defer statement for the exact started Mission plan revision.
    ///
    /// The trusted clock callback is invoked by Operations after its
    /// `IMMEDIATE` transaction has begun and all current source/fence checks
    /// pass. An exact replay returns the original receipt without sampling the
    /// clock again.
    ///
    /// # Errors
    /// Rejects invalid identities, a foreign or closed Session, a non-Ready
    /// participant, stale Workspace leadership, and source/persistence errors.
    pub fn defer_mission_t2_checkpoint<F>(
        &self,
        spec: &MissionT2DeferSpec,
        principal: AuthenticatedPrincipal,
        trusted_now_ms: F,
    ) -> Result<MissionT2DeferOutcome, RuntimeError>
    where
        F: FnOnce() -> Result<i64, RuntimeError>,
    {
        let workspace = workspace(spec.workspace_id)?;
        let (actor, target) = participant_identity(
            workspace,
            spec.participant.activity_id,
            spec.participant.session_id,
        )?;
        self.require_participation(target, principal)?;
        let authority = authority_for(actor, spec.participant)?;
        let content_spec = defer_content_spec(spec);
        let content = defer_content(&content_spec)?;
        let command = MissionT2DeferCommand::new(content, authority)?;
        let result = self
            .catalog()?
            .defer_mission_t2_checkpoint(&command, || {
                trusted_now_ms()
                    .map_err(|_| OperationsError::Conflict("T2 trusted clock unavailable"))
            })
            .map_err(RuntimeError::persistence)?;
        Ok(result)
    }

    /// Read a deferred checkpoint by request ID as the original semantic actor
    /// or the currently authorized Workspace leader.
    ///
    /// This query is observation only and never retries the mutation.
    ///
    /// # Errors
    /// Rejects invalid IDs, a foreign or closed Session, a non-Ready binding,
    /// an unrelated participant, stale Workspace leadership, and persistence errors.
    pub fn mission_t2_deferred_checkpoint_for_request(
        &self,
        intent: &MissionT2DeferContentSpec,
        reader: MissionEvaluationParticipantSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<MissionT2DeferredCheckpoint>, RuntimeError> {
        let workspace = workspace(intent.workspace_id)?;
        let content = defer_content(intent)?;
        let (actor, target) =
            participant_identity(workspace, reader.activity_id, reader.session_id)?;
        self.require_participation(target, principal)?;
        let authority = authority_for(actor, reader)?;
        let record = self
            .catalog()?
            .mission_t2_deferred_checkpoint_for_request(
                workspace,
                identity(intent.request_id)?,
                authority,
            )
            .map_err(RuntimeError::persistence)?;
        if record
            .as_ref()
            .is_some_and(|record| !record.matches_intent(&content))
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(record)
    }
}

fn defer_content_spec(spec: &MissionT2DeferSpec) -> MissionT2DeferContentSpec {
    MissionT2DeferContentSpec {
        request_id: spec.request_id,
        checkpoint_id: spec.checkpoint_id,
        workspace_id: spec.workspace_id,
        mission_id: spec.mission_id,
        plan_id: spec.plan_id,
        expected_revision: spec.expected_revision,
        expected_previous_checkpoint_id: spec.expected_previous_checkpoint_id,
        reason: spec.reason.clone(),
        next_check_condition: spec.next_check_condition.clone(),
        next_check_at_ms: spec.next_check_at_ms,
    }
}

fn defer_content(spec: &MissionT2DeferContentSpec) -> Result<MissionT2DeferContent, RuntimeError> {
    Ok(MissionT2DeferContent::new(
        identity(spec.request_id)?,
        MissionT2CheckpointId::from_uuid(spec.checkpoint_id)?,
        workspace(spec.workspace_id)?,
        MissionId::from_uuid(spec.mission_id)?,
        MissionEvaluationPlanId::from_uuid(spec.plan_id)?,
        spec.expected_revision,
        spec.expected_previous_checkpoint_id
            .map(MissionT2CheckpointId::from_uuid)
            .transpose()?,
        spec.reason.clone(),
        spec.next_check_condition.clone(),
        spec.next_check_at_ms,
    )?)
}

fn participant_identity(
    workspace: WorkspaceId,
    activity_id: Uuid,
    session_id: Uuid,
) -> Result<(OperationalActor, ParticipationTarget), RuntimeError> {
    let activity_id = identity(activity_id)?;
    let session_id = identity(session_id)?;
    let agent_id = member_agent_id(workspace.into_uuid(), activity_id, session_id);
    let actor = OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(activity_id),
        agent_id,
        session_id: AgentSessionId::from_uuid(session_id),
    };
    let target = ParticipationTarget {
        workspace_id: workspace.into_uuid(),
        activity_id,
        agent_id: agent_id.into_uuid(),
        session_id,
    };
    Ok((actor, target))
}

fn authority_for(
    actor: OperationalActor,
    participant: MissionEvaluationParticipantSpec,
) -> Result<MissionEvaluationAuthority, RuntimeError> {
    if participant.expected_workspace_leadership_revision < 1 {
        return Err(RuntimeError::InvalidInput);
    }
    MissionEvaluationAuthority::new(
        actor,
        Some(participant.expected_workspace_leadership_revision),
    )
    .map_err(RuntimeError::persistence)
}

fn workspace(value: Uuid) -> Result<WorkspaceId, RuntimeError> {
    Ok(WorkspaceId::from_uuid(identity(value)?))
}

fn identity(value: Uuid) -> Result<Uuid, RuntimeError> {
    if value.is_nil() {
        Err(RuntimeError::InvalidInput)
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn participant_spec() -> MissionEvaluationParticipantSpec {
        MissionEvaluationParticipantSpec {
            activity_id: Uuid::from_u128(1),
            session_id: Uuid::from_u128(2),
            expected_workspace_leadership_revision: 3,
        }
    }

    fn defer_spec() -> MissionT2DeferSpec {
        MissionT2DeferSpec {
            request_id: Uuid::from_u128(4),
            checkpoint_id: Uuid::from_u128(5),
            workspace_id: Uuid::from_u128(6),
            mission_id: Uuid::from_u128(7),
            plan_id: Uuid::from_u128(8),
            expected_revision: 9,
            expected_previous_checkpoint_id: Some(Uuid::from_u128(10)),
            reason: "review pending".to_owned(),
            next_check_condition: "read reviewer receipt".to_owned(),
            next_check_at_ms: 100,
            participant: participant_spec(),
        }
    }

    #[test]
    fn defer_intent_keeps_exact_plan_predecessor_and_has_no_caller_clock()
    -> Result<(), Box<dyn std::error::Error>> {
        let spec = defer_spec();
        let content = defer_content(&defer_content_spec(&spec))?;
        assert_eq!(content.request_id(), spec.request_id);
        assert_eq!(content.checkpoint_id().into_uuid(), spec.checkpoint_id);
        assert_eq!(content.workspace_id().into_uuid(), spec.workspace_id);
        assert_eq!(content.plan_id().into_uuid(), spec.plan_id);
        assert_eq!(content.revision(), spec.expected_revision);
        assert_eq!(
            content
                .expected_previous_checkpoint_id()
                .map(MissionT2CheckpointId::into_uuid),
            spec.expected_previous_checkpoint_id
        );
        assert_eq!(content.next_check_at_ms(), spec.next_check_at_ms);
        Ok(())
    }

    #[test]
    fn defer_identity_is_derived_from_activity_and_session() -> Result<(), RuntimeError> {
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(11));
        let (actor, target) =
            participant_identity(workspace, Uuid::from_u128(12), Uuid::from_u128(13))?;
        let OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } = actor
        else {
            return Err(RuntimeError::InvalidInput);
        };
        assert_eq!(activity_id.into_uuid(), Uuid::from_u128(12));
        assert_eq!(session_id.into_uuid(), Uuid::from_u128(13));
        assert_eq!(
            agent_id,
            member_agent_id(
                workspace.into_uuid(),
                Uuid::from_u128(12),
                Uuid::from_u128(13)
            )
        );
        assert_eq!(target.agent_id, agent_id.into_uuid());
        assert!(participant_identity(workspace, Uuid::nil(), Uuid::from_u128(13)).is_err());
        Ok(())
    }
}
