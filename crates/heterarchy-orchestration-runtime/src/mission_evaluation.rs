//! Runtime-owned access to durable, organization-owned Mission planning facts.
//!
//! The HTTP boundary supplies only an authenticated Session selector and an
//! Activity selector. The Agent identity is derived here from the existing
//! Workspace binding. Operations revalidates the Ready participant and
//! Workspace leadership revision for writes; receipt reads bind the current
//! Ready logical actor without rechecking its current leadership role.

use forge_domain::{ActivityId, AgentSessionId, AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    MissionCreateCommand, MissionCreateOutcome, MissionEvaluationAuthority,
    MissionEvaluationPlanId, MissionEvaluationPlanObligationInput,
    MissionEvaluationPlanPublishCommand, MissionEvaluationPlanPublishOutcome,
    MissionEvaluationPlanStartCommand, MissionEvaluationPlanStartOutcome,
    MissionEvaluationPlanStartRecord, MissionEvaluationPlanVersion, MissionEvaluationPlanVersionId,
    MissionEvaluationRequestRecord, MissionId, MissionObligationId, MissionObligationRecord,
    MissionObligationRecordCommand, MissionObligationRecordOutcome, MissionRecord,
    MissionWorkUnitId, OperationalActor, OperationsError, PositiveWeight, WorkspaceParticipationId,
};
use uuid::Uuid;

use crate::{
    OrchestrationRuntime, RuntimeError, leadership::member_agent_id,
    participation::ParticipationTarget,
};

const MAX_PLAN_COHORT: usize = 256;

/// Caller-selected identity for the authenticated participant making a
/// Mission mutation. The Agent identity is always derived by this runtime.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvaluationParticipantSpec {
    /// Existing Workspace Activity of the authenticated participant.
    pub activity_id: Uuid,
    /// Existing logical Session owned by the authenticated principal.
    pub session_id: Uuid,
    /// Compare-and-swap witness for the current Workspace leader.
    pub expected_workspace_leadership_revision: i64,
}

/// Request to create the single active Mission for an exact existing Team.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionCreateSpec {
    /// Stable idempotency identity for this request.
    pub request_id: Uuid,
    /// New Mission identity.
    pub mission_id: Uuid,
    /// Exact Workspace identity.
    pub workspace_id: Uuid,
    /// Existing Team that owns the Mission.
    pub team_id: Uuid,
    /// Organization-declared Mission purpose.
    pub purpose: String,
    /// Authenticated Workspace participant and leadership fence.
    pub participant: MissionEvaluationParticipantSpec,
}

/// Request to record one stable personal obligation under a Mission.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionObligationRecordSpec {
    /// Stable idempotency identity for this request.
    pub request_id: Uuid,
    /// New stable obligation identity.
    pub obligation_id: Uuid,
    /// Exact Workspace identity.
    pub workspace_id: Uuid,
    /// Owning Mission identity.
    pub mission_id: Uuid,
    /// Existing logical Workspace participation assigned this work.
    pub participation_id: Uuid,
    /// Identity of the original work unit assigned to this obligation.
    pub work_unit_id: Uuid,
    /// Positive exact integer weight.
    pub weight: u64,
    /// Organization-declared acceptance criteria.
    pub acceptance_criteria: String,
    /// Original due time in Unix milliseconds.
    pub due_at_ms: i64,
    /// Authenticated Workspace participant and leadership fence.
    pub participant: MissionEvaluationParticipantSpec,
}

/// Request to publish an immutable Mission evaluation-plan version.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvaluationPlanPublishSpec {
    /// Stable idempotency identity for this request.
    pub request_id: Uuid,
    /// Stable plan identity.
    pub plan_id: Uuid,
    /// New immutable version identity.
    pub version_id: Uuid,
    /// Exact Workspace identity.
    pub workspace_id: Uuid,
    /// Owning Mission identity.
    pub mission_id: Uuid,
    /// Exact plan-head revision the caller observed; zero creates the first version.
    pub expected_head_revision: u64,
    /// Caller-owned submission cutoff, T1.
    pub t1_submission_cutoff_ms: i64,
    /// Caller-owned checkpoint, T2.
    pub t2_checkpoint_ms: i64,
    /// Complete bounded obligation cohort for this version.
    pub obligation_ids: Vec<Uuid>,
    /// Authenticated Workspace participant and leadership fence.
    pub participant: MissionEvaluationParticipantSpec,
}

/// Request to explicitly start one exact published plan revision.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvaluationPlanStartSpec {
    /// Stable idempotency identity for this request.
    pub request_id: Uuid,
    /// Exact Workspace identity.
    pub workspace_id: Uuid,
    /// Owning Mission identity, checked against the selected plan version.
    pub mission_id: Uuid,
    /// Stable plan identity.
    pub plan_id: Uuid,
    /// Exact immutable plan revision to start.
    pub expected_revision: u64,
    /// Authenticated Workspace participant and leadership fence.
    pub participant: MissionEvaluationParticipantSpec,
}

impl OrchestrationRuntime {
    /// Create the active Mission for one existing Team.
    ///
    /// `created_at_ms` must be supplied by the trusted application/worker
    /// boundary. External request bodies do not carry this observation.
    ///
    /// # Errors
    /// Rejects invalid identities, a foreign/closed Session, a non-Ready actor,
    /// a stale Workspace-leadership fence, or durable catalog failures.
    pub fn create_mission(
        &self,
        spec: MissionCreateSpec,
        principal: AuthenticatedPrincipal,
        created_at_ms: i64,
    ) -> Result<MissionCreateOutcome, RuntimeError> {
        let workspace = workspace(spec.workspace_id)?;
        let (target, authority) = participant_authority(workspace, spec.participant)?;
        self.require_participation(target, principal)?;

        let command = MissionCreateCommand::new(
            identity(spec.request_id)?,
            MissionId::from_uuid(spec.mission_id)?,
            workspace,
            identity(spec.team_id)?,
            spec.purpose,
            created_at_ms,
            authority,
        )?;
        self.catalog()?
            .create_mission(&command)
            .map_err(RuntimeError::persistence)
    }

    /// Read one immutable Mission under exact Workspace scope.
    ///
    /// This historical record does not prove that it is the Team's current
    /// Mission; mutation and source-evidence owners retain those checks.
    ///
    /// # Errors
    /// Rejects invalid or foreign Workspace/Mission identities and storage errors.
    pub fn mission(
        &self,
        workspace_id: Uuid,
        mission_id: Uuid,
    ) -> Result<Option<MissionRecord>, RuntimeError> {
        self.catalog()?
            .mission(workspace(workspace_id)?, MissionId::from_uuid(mission_id)?)
            .map_err(RuntimeError::persistence)
    }

    /// Record one stable obligation for a currently joined Team participant.
    ///
    /// `recorded_at_ms` is a trusted application/worker observation and is not
    /// accepted from the external request body.
    ///
    /// # Errors
    /// Rejects invalid identities/weight, a foreign or closed Session, a
    /// non-Ready actor, a stale Workspace-leadership fence, or persistence errors.
    pub fn record_mission_obligation(
        &self,
        spec: MissionObligationRecordSpec,
        principal: AuthenticatedPrincipal,
        recorded_at_ms: i64,
    ) -> Result<MissionObligationRecordOutcome, RuntimeError> {
        let workspace = workspace(spec.workspace_id)?;
        let (target, authority) = participant_authority(workspace, spec.participant)?;
        self.require_participation(target, principal)?;

        let command = MissionObligationRecordCommand::new(
            identity(spec.request_id)?,
            MissionObligationId::from_uuid(spec.obligation_id)?,
            workspace,
            MissionId::from_uuid(spec.mission_id)?,
            WorkspaceParticipationId::from_uuid(spec.participation_id)?,
            MissionWorkUnitId::from_uuid(spec.work_unit_id)?,
            PositiveWeight::new(spec.weight)?,
            spec.acceptance_criteria,
            spec.due_at_ms,
            recorded_at_ms,
            authority,
        )?;
        self.catalog()?
            .record_mission_obligation(&command)
            .map_err(RuntimeError::persistence)
    }

    /// Read one immutable obligation under exact Workspace scope.
    ///
    /// The caller must also compare its Mission selector with the returned
    /// record; the Operations read is intentionally keyed by obligation ID.
    ///
    /// # Errors
    /// Rejects invalid or foreign Workspace/obligation identities and storage errors.
    pub fn mission_obligation(
        &self,
        workspace_id: Uuid,
        obligation_id: Uuid,
    ) -> Result<Option<MissionObligationRecord>, RuntimeError> {
        self.catalog()?
            .mission_obligation(
                workspace(workspace_id)?,
                MissionObligationId::from_uuid(obligation_id)?,
            )
            .map_err(RuntimeError::persistence)
    }

    /// Publish one immutable plan version with a trusted server T0.
    ///
    /// T1 and T2 are organization plan content supplied by the caller. `t0_commit_at_ms`
    /// is produced by the trusted application/worker and is never accepted from
    /// an external request body.
    ///
    /// # Errors
    /// Rejects invalid identities, oversized/duplicate cohorts, invalid timeline,
    /// a foreign/closed Session, stale leadership/head revisions, or storage errors.
    pub fn publish_mission_evaluation_plan(
        &self,
        spec: MissionEvaluationPlanPublishSpec,
        principal: AuthenticatedPrincipal,
        t0_commit_at_ms: i64,
    ) -> Result<MissionEvaluationPlanPublishOutcome, RuntimeError> {
        let workspace = workspace(spec.workspace_id)?;
        let (target, authority) = participant_authority(workspace, spec.participant)?;
        self.require_participation(target, principal)?;
        if spec.obligation_ids.len() > MAX_PLAN_COHORT {
            return Err(
                heterarchy_orchestration_operations::OperationsError::Bounds(
                    "mission evaluation plan cohort",
                )
                .into(),
            );
        }
        let mut cohort = Vec::with_capacity(spec.obligation_ids.len());
        for obligation_id in spec.obligation_ids {
            cohort.push(MissionEvaluationPlanObligationInput::new(
                MissionObligationId::from_uuid(obligation_id)?,
            ));
        }
        let command = MissionEvaluationPlanPublishCommand::new(
            identity(spec.request_id)?,
            MissionEvaluationPlanId::from_uuid(spec.plan_id)?,
            MissionEvaluationPlanVersionId::from_uuid(spec.version_id)?,
            workspace,
            MissionId::from_uuid(spec.mission_id)?,
            spec.expected_head_revision,
            t0_commit_at_ms,
            spec.t1_submission_cutoff_ms,
            spec.t2_checkpoint_ms,
            cohort,
            authority,
        )?;
        self.catalog()?
            .publish_mission_evaluation_plan(&command)
            .map_err(RuntimeError::persistence)
    }

    /// Read one immutable plan version by exact Workspace, Mission, plan and revision.
    ///
    /// # Errors
    /// Rejects invalid or foreign IDs and storage errors.
    pub fn mission_evaluation_plan_version(
        &self,
        workspace_id: Uuid,
        mission_id: Uuid,
        plan_id: Uuid,
        revision: u64,
    ) -> Result<Option<MissionEvaluationPlanVersion>, RuntimeError> {
        let workspace = workspace(workspace_id)?;
        let mission_id = MissionId::from_uuid(mission_id)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(plan_id)?;
        if revision == 0 || revision > i64::MAX.unsigned_abs() {
            return Err(RuntimeError::InvalidInput);
        }
        let version = self
            .catalog()?
            .mission_evaluation_plan_version(workspace, plan_id, revision)
            .map_err(RuntimeError::persistence)?;
        if version
            .as_ref()
            .is_some_and(|version| version.mission_id() != mission_id)
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(version)
    }

    /// Start one exact plan revision with a trusted server clock callback.
    ///
    /// Operations samples the callback only after acquiring its IMMEDIATE
    /// transaction and checking the current Mission, plan head, and authority,
    /// so SQLite lock wait is included in the T0/T1 eligibility check. The
    /// recorded sample does not prove commit before T1 or clock monotonicity.
    ///
    /// # Errors
    /// Rejects invalid IDs/revision, cross-Mission plan selection, a foreign or
    /// closed Session, stale leadership, or persistence failures.
    pub fn start_mission_evaluation_plan(
        &self,
        spec: MissionEvaluationPlanStartSpec,
        principal: AuthenticatedPrincipal,
        trusted_now_ms: impl FnOnce() -> Result<i64, RuntimeError>,
    ) -> Result<MissionEvaluationPlanStartOutcome, RuntimeError> {
        let workspace = workspace(spec.workspace_id)?;
        let mission_id = MissionId::from_uuid(spec.mission_id)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(spec.plan_id)?;
        let (target, authority) = participant_authority(workspace, spec.participant)?;
        self.require_participation(target, principal)?;
        let command = MissionEvaluationPlanStartCommand::new(
            identity(spec.request_id)?,
            workspace,
            mission_id,
            plan_id,
            spec.expected_revision,
            authority,
        )?;

        let mut catalog = self.catalog()?;
        catalog
            .start_mission_evaluation_plan(&command, || {
                trusted_now_ms().map_err(|_| {
                    OperationsError::Conflict("trusted Mission plan start clock unavailable")
                })
            })
            .map_err(RuntimeError::persistence)
    }

    /// Read the immutable start receipt for one exact Workspace/Mission/plan.
    ///
    /// # Errors
    /// Rejects invalid or foreign IDs and storage errors.
    pub fn mission_evaluation_plan_start(
        &self,
        workspace_id: Uuid,
        mission_id: Uuid,
        plan_id: Uuid,
    ) -> Result<Option<MissionEvaluationPlanStartRecord>, RuntimeError> {
        let workspace = workspace(workspace_id)?;
        let mission_id = MissionId::from_uuid(mission_id)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(plan_id)?;
        let start = self
            .catalog()?
            .mission_evaluation_plan_start(workspace, plan_id)
            .map_err(RuntimeError::persistence)?;
        if start
            .as_ref()
            .is_some_and(|start| start.mission_id() != mission_id)
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(start)
    }

    /// Read one mutation receipt only for its current authenticated participant.
    ///
    /// Operations resolves the exact current Ready actor in the read transaction
    /// and compares its logical identity with the stored request actor. Workspace
    /// leadership is not rechecked: a legitimate actor rebind does not change the
    /// semantic author of an already committed mutation.
    ///
    /// # Errors
    /// Rejects invalid IDs, a foreign/closed Session, a non-Ready actor, a
    /// different logical actor, cross-Workspace receipts, or persistence failures.
    pub fn mission_evaluation_request_for_actor(
        &self,
        workspace_id: Uuid,
        request_id: Uuid,
        activity_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<MissionEvaluationRequestRecord>, RuntimeError> {
        let workspace = workspace(workspace_id)?;
        let (actor, target) = participant_identity(workspace, activity_id, session_id)?;
        self.require_participation(target, principal)?;
        self.catalog()?
            .mission_evaluation_request_for_actor(workspace, identity(request_id)?, actor)
            .map_err(RuntimeError::persistence)
    }
}

fn participant_authority(
    workspace: WorkspaceId,
    participant: MissionEvaluationParticipantSpec,
) -> Result<(ParticipationTarget, MissionEvaluationAuthority), RuntimeError> {
    let (actor, target) =
        participant_identity(workspace, participant.activity_id, participant.session_id)?;
    let authority = authority_for(actor, participant)?;
    Ok((target, authority))
}

fn participant_identity(
    workspace: WorkspaceId,
    activity_id: Uuid,
    session_id: Uuid,
) -> Result<(OperationalActor, ParticipationTarget), RuntimeError> {
    if activity_id.is_nil() || session_id.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
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

    #[test]
    fn receipt_actor_is_derived_from_the_current_activity_session_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(1));
        let activity_id = Uuid::from_u128(2);
        let session_id = Uuid::from_u128(3);
        let agent_id = member_agent_id(workspace.into_uuid(), activity_id, session_id);
        let (actor, target) = participant_identity(workspace, activity_id, session_id)?;

        assert_eq!(
            actor,
            OperationalActor::Participant {
                activity_id: ActivityId::from_uuid(activity_id),
                agent_id,
                session_id: AgentSessionId::from_uuid(session_id),
            }
        );
        assert_eq!(target.agent_id, agent_id.into_uuid());
        assert!(participant_identity(workspace, Uuid::nil(), session_id).is_err());
        Ok(())
    }
}
