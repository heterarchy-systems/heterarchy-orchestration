//! Runtime-owned access to Forge-backed Mission evidence.
//!
//! The Runtime derives the logical participant from the authenticated
//! Activity/Session pair and asks Operations to revalidate leadership and the
//! frozen plan at each local mutation. Forge source I/O stays in the Backend.

use forge_domain::{
    ActivityId, AgentAssignmentId, AgentAssignmentRevision, AgentInstanceId, AgentSessionId,
    AuthenticatedPrincipal, EvaluationCutId, EvaluationManifestId, EvaluationManifestProposal,
    EvaluationReviewCutId, EvaluationRevision, ForgeBackendId, WorkspaceId,
};
use heterarchy_orchestration_operations::mission_evidence::{
    MissionEvidenceManifestBindingCommand, MissionEvidenceManifestBindingOutcome,
    MissionEvidenceManifestBundle, MissionEvidenceManifestCandidateMember,
    MissionEvidenceManifestCandidatePartition, MissionEvidenceManifestPreparation,
    MissionEvidenceManifestPreparationCommand, MissionEvidenceMutationRecord,
    MissionEvidenceObservationBundle, MissionEvidenceObservationCommand,
    MissionEvidenceObservationOutcome,
};
use heterarchy_orchestration_operations::{
    MissionEvaluationAuthority, MissionEvaluationPlanId, MissionId,
};
use uuid::Uuid;

use crate::{
    OrchestrationRuntime, RuntimeError, leadership::member_agent_id,
    mission_evaluation::MissionEvaluationParticipantSpec, participation::ParticipationTarget,
};

const MAX_CANDIDATE_PARTITIONS: usize = 256;
const MAX_CANDIDATE_MEMBERS: usize = 256;
const MANIFEST_ID_DOMAIN: &[u8] = b"heterarchy-orchestration/mission-evidence-manifest/v1\0";
const T1_CUT_ID_DOMAIN: &[u8] = b"heterarchy-orchestration/mission-evidence-t1-cut/v1\0";
const REVIEW_CUT_ID_DOMAIN: &[u8] = b"heterarchy-orchestration/mission-evidence-review-cut/v1\0";

/// Stable T1 cut identity for one exact immutable source manifest.
///
/// # Errors
/// Returns `RuntimeError::InvalidInput` if the derived UUID is nil and the
/// domain `EvaluationCutId` validation rejects it.
pub fn mission_evidence_t1_cut_id(
    proposal: &EvaluationManifestProposal,
) -> Result<EvaluationCutId, RuntimeError> {
    EvaluationCutId::new(derived_uuid(
        T1_CUT_ID_DOMAIN,
        &[
            proposal.source().workspace_id().into_uuid().as_bytes(),
            proposal.manifest_id().into_uuid().as_bytes(),
            &proposal.revision().get().to_be_bytes(),
            proposal.digest().as_bytes(),
        ],
    ))
    .map_err(|_| RuntimeError::InvalidInput)
}

/// Stable review-cut identity for one exact observation request and Activity.
///
/// # Errors
/// Returns `RuntimeError::InvalidInput` if the derived UUID is nil and the
/// domain `EvaluationReviewCutId` validation rejects it.
pub fn mission_evidence_review_cut_id(
    proposal: &EvaluationManifestProposal,
    request_id: Uuid,
) -> Result<EvaluationReviewCutId, RuntimeError> {
    EvaluationReviewCutId::new(derived_uuid(
        REVIEW_CUT_ID_DOMAIN,
        &[
            proposal.source().workspace_id().into_uuid().as_bytes(),
            proposal.source().activity_id().into_uuid().as_bytes(),
            proposal.manifest_id().into_uuid().as_bytes(),
            &proposal.revision().get().to_be_bytes(),
            request_id.as_bytes(),
        ],
    ))
    .map_err(|_| RuntimeError::InvalidInput)
}

/// Caller-selected complete source candidate mapping for one Activity.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceCandidatePartitionSpec {
    /// Existing source Activity selected for this partition.
    pub activity_id: Uuid,
    /// Exact candidate assignment mappings for local obligations.
    pub members: Vec<MissionEvidenceCandidateMemberSpec>,
}

/// One exact source assignment and expected target for a frozen obligation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvidenceCandidateMemberSpec {
    /// Stable local obligation identity.
    pub obligation_id: Uuid,
    /// Exact existing Forge assignment identity.
    pub assignment_id: Uuid,
    /// Original assignment revision before work starts.
    pub assignment_revision: i64,
    /// Expected Agent identity from source membership history.
    pub target_agent_id: Uuid,
    /// Expected Session identity from source membership history.
    pub target_session_id: Uuid,
}

/// Read-only preparation request for one exact started Mission plan.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidencePreparationSpec {
    /// Exact Workspace identity.
    pub workspace_id: Uuid,
    /// Expected owning Mission.
    pub mission_id: Uuid,
    /// Frozen plan identity.
    pub plan_id: Uuid,
    /// Frozen plan revision.
    pub plan_revision: u64,
    /// Candidate mapping partitioned by source Activity.
    pub partitions: Vec<MissionEvidenceCandidatePartitionSpec>,
    /// Authenticated Ready participant and leadership fence.
    pub participant: MissionEvaluationParticipantSpec,
}

/// Read request for plan digests before candidate assignments are selected.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvidencePlanDigestSpec {
    /// Exact Workspace identity.
    pub workspace_id: Uuid,
    /// Expected owning Mission.
    pub mission_id: Uuid,
    /// Frozen plan identity.
    pub plan_id: Uuid,
    /// Frozen plan revision.
    pub plan_revision: u64,
    /// Authenticated participant Activity.
    pub activity_id: Uuid,
    /// Logical Session owned by the authenticated principal.
    pub session_id: Uuid,
}

/// Local commit request for a complete Forge registration readback bundle.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceManifestBindingSpec {
    /// Stable local mutation receipt identity.
    pub request_id: Uuid,
    /// Exact Workspace identity.
    pub workspace_id: Uuid,
    /// Expected owning Mission.
    pub mission_id: Uuid,
    /// Frozen plan identity.
    pub plan_id: Uuid,
    /// Frozen plan revision.
    pub plan_revision: u64,
    /// Complete source-owned registered manifest readbacks.
    pub bundle: MissionEvidenceManifestBundle,
    /// Authenticated Ready participant and leadership fence.
    pub participant: MissionEvaluationParticipantSpec,
}

/// Local commit request for complete source T1/review cuts.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceObservationSpec {
    /// Stable local observation and source review-cut identity.
    pub request_id: Uuid,
    /// Exact Workspace identity.
    pub workspace_id: Uuid,
    /// Expected owning Mission.
    pub mission_id: Uuid,
    /// Frozen plan identity.
    pub plan_id: Uuid,
    /// Frozen plan revision.
    pub plan_revision: u64,
    /// Exact local observation head observed by the caller.
    pub expected_head_revision: u64,
    /// Complete exact source cut pairs for every bound Activity.
    pub cuts: MissionEvidenceObservationBundle,
    /// Authenticated Ready participant and leadership fence.
    pub participant: MissionEvaluationParticipantSpec,
}

impl OrchestrationRuntime {
    /// Derive canonical local plan digests for an exact started plan.
    ///
    /// # Errors
    /// Rejects invalid or foreign identities, a non-Ready participant, a plan
    /// owned by another Mission, and catalog failures.
    pub fn mission_evidence_plan_digests(
        &self,
        spec: MissionEvidencePlanDigestSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<
        heterarchy_orchestration_operations::mission_evidence::MissionEvidencePlanDigests,
        RuntimeError,
    > {
        let workspace_id = workspace(spec.workspace_id)?;
        let (_, target) = participant_identity(workspace_id, spec.activity_id, spec.session_id)?;
        self.require_participation(target, principal)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(identity(spec.plan_id)?)?;
        let mut catalog = self.catalog()?;
        require_plan_mission(
            &mut catalog,
            workspace_id,
            MissionId::from_uuid(identity(spec.mission_id)?)?,
            plan_id,
            spec.plan_revision,
        )?;
        catalog
            .mission_evidence_plan_digests(workspace_id, plan_id, spec.plan_revision)
            .map_err(RuntimeError::persistence)
    }

    /// Validate the full local cohort and every historical source target before
    /// any caller can seal a Forge manifest.
    ///
    /// # Errors
    /// Rejects invalid identities, stale leadership, mismatched Mission/plan,
    /// incomplete source candidates, or catalog failures.
    pub fn prepare_mission_evidence_manifests(
        &self,
        spec: MissionEvidencePreparationSpec,
        backend_id: ForgeBackendId,
        principal: AuthenticatedPrincipal,
    ) -> Result<MissionEvidenceManifestPreparation, RuntimeError> {
        let workspace_id = workspace(spec.workspace_id)?;
        let (target, authority) = participant_authority(workspace_id, spec.participant)?;
        self.require_participation(target, principal)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(identity(spec.plan_id)?)?;
        let mission_id = MissionId::from_uuid(identity(spec.mission_id)?)?;
        let mut catalog = self.catalog()?;
        require_plan_mission(
            &mut catalog,
            workspace_id,
            mission_id,
            plan_id,
            spec.plan_revision,
        )?;

        if spec.partitions.is_empty() || spec.partitions.len() > MAX_CANDIDATE_PARTITIONS {
            return Err(RuntimeError::InvalidInput);
        }
        let member_count = spec
            .partitions
            .iter()
            .try_fold(0_usize, |sum, partition| {
                sum.checked_add(partition.members.len())
            })
            .ok_or(RuntimeError::InvalidInput)?;
        if member_count == 0 || member_count > MAX_CANDIDATE_MEMBERS {
            return Err(RuntimeError::InvalidInput);
        }

        let mut partitions = Vec::with_capacity(spec.partitions.len());
        for partition in spec.partitions {
            let activity_id = ActivityId::from_uuid(identity(partition.activity_id)?);
            let manifest_id = EvaluationManifestId::new(stable_manifest_id(
                spec.workspace_id,
                spec.plan_id,
                spec.plan_revision,
                partition.activity_id,
            ))
            .map_err(|_| RuntimeError::InvalidInput)?;
            let revision = EvaluationRevision::new(1).map_err(|_| RuntimeError::InvalidInput)?;
            let mut members = Vec::with_capacity(partition.members.len());
            for member in partition.members {
                members.push(MissionEvidenceManifestCandidateMember::new(
                    heterarchy_orchestration_operations::MissionObligationId::from_uuid(identity(
                        member.obligation_id,
                    )?)?,
                    AgentAssignmentId::from_uuid(identity(member.assignment_id)?),
                    AgentAssignmentRevision::new(member.assignment_revision)
                        .map_err(|_| RuntimeError::InvalidInput)?,
                    AgentInstanceId::from_uuid(identity(member.target_agent_id)?),
                    AgentSessionId::from_uuid(identity(member.target_session_id)?),
                )?);
            }
            partitions.push(MissionEvidenceManifestCandidatePartition::new(
                activity_id,
                manifest_id,
                revision,
                members,
            )?);
        }
        let command = MissionEvidenceManifestPreparationCommand::new(
            workspace_id,
            plan_id,
            spec.plan_revision,
            backend_id,
            partitions,
            authority,
        )?;
        catalog
            .prepare_mission_evidence_manifests(&command)
            .map_err(RuntimeError::persistence)
    }

    /// Atomically bind the complete configured-Forge manifest readback bundle
    /// to its exact started local plan.
    ///
    /// # Errors
    /// Rejects stale authority/plan state, incomplete or foreign source data,
    /// changed same-ID retries, and unconfirmed persistence outcomes.
    pub fn bind_mission_evidence_manifest(
        &self,
        spec: MissionEvidenceManifestBindingSpec,
        principal: AuthenticatedPrincipal,
        recorded_at_ms: i64,
    ) -> Result<MissionEvidenceManifestBindingOutcome, RuntimeError> {
        let workspace_id = workspace(spec.workspace_id)?;
        let (target, authority) = participant_authority(workspace_id, spec.participant)?;
        self.require_participation(target, principal)?;
        let mission_id = MissionId::from_uuid(identity(spec.mission_id)?)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(identity(spec.plan_id)?)?;
        let mut catalog = self.catalog()?;
        require_plan_mission(
            &mut catalog,
            workspace_id,
            mission_id,
            plan_id,
            spec.plan_revision,
        )?;
        let command = MissionEvidenceManifestBindingCommand::new(
            identity(spec.request_id)?,
            workspace_id,
            plan_id,
            spec.plan_revision,
            spec.bundle,
            recorded_at_ms,
            authority,
        )?;
        catalog
            .bind_mission_evidence_manifest(&command)
            .map_err(RuntimeError::persistence)
    }

    /// Atomically append a complete T1/review-cut observation under the exact
    /// expected local observation head.
    ///
    /// # Errors
    /// Rejects stale authority/head, foreign or incomplete source cuts,
    /// changed same-ID retries, and unconfirmed persistence outcomes.
    pub fn record_mission_evidence_observation(
        &self,
        spec: MissionEvidenceObservationSpec,
        principal: AuthenticatedPrincipal,
        recorded_at_ms: i64,
    ) -> Result<MissionEvidenceObservationOutcome, RuntimeError> {
        let workspace_id = workspace(spec.workspace_id)?;
        let (target, authority) = participant_authority(workspace_id, spec.participant)?;
        self.require_participation(target, principal)?;
        let mission_id = MissionId::from_uuid(identity(spec.mission_id)?)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(identity(spec.plan_id)?)?;
        let mut catalog = self.catalog()?;
        require_plan_mission(
            &mut catalog,
            workspace_id,
            mission_id,
            plan_id,
            spec.plan_revision,
        )?;
        let command = MissionEvidenceObservationCommand::new(
            identity(spec.request_id)?,
            workspace_id,
            plan_id,
            spec.plan_revision,
            spec.expected_head_revision,
            spec.cuts,
            recorded_at_ms,
            authority,
        )?;
        catalog
            .record_mission_evidence_observation(&command)
            .map_err(RuntimeError::persistence)
    }

    /// Read the canonical binding for one exact Workspace/plan revision.
    ///
    /// # Errors
    /// Rejects invalid scope, cross-Mission records, and catalog failures.
    pub fn mission_evidence_binding(
        &self,
        workspace_id: Uuid,
        mission_id: Uuid,
        plan_id: Uuid,
        plan_revision: u64,
    ) -> Result<
        Option<heterarchy_orchestration_operations::mission_evidence::MissionEvidenceManifestBindingRecord>,
        RuntimeError,
    >{
        let workspace_id = workspace(workspace_id)?;
        let mission_id = MissionId::from_uuid(identity(mission_id)?)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(identity(plan_id)?)?;
        let binding = self
            .catalog()?
            .mission_evidence_binding(workspace_id, plan_id, plan_revision)
            .map_err(RuntimeError::persistence)?;
        if binding
            .as_ref()
            .is_some_and(|record| record.mission_id() != mission_id)
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(binding)
    }

    /// Read one immutable observation by exact Workspace and mutation ID.
    ///
    /// # Errors
    /// Rejects invalid scope, cross-plan records, and catalog failures.
    pub fn mission_evidence_observation(
        &self,
        workspace_id: Uuid,
        mission_id: Uuid,
        plan_id: Uuid,
        plan_revision: u64,
        request_id: Uuid,
    ) -> Result<
        Option<
            heterarchy_orchestration_operations::mission_evidence::MissionEvidenceObservationRecord,
        >,
        RuntimeError,
    > {
        let workspace_id = workspace(workspace_id)?;
        let mission_id = MissionId::from_uuid(identity(mission_id)?)?;
        let plan_id = MissionEvaluationPlanId::from_uuid(identity(plan_id)?)?;
        let record = self
            .catalog()?
            .mission_evidence_observation(workspace_id, identity(request_id)?)
            .map_err(RuntimeError::persistence)?;
        if record.as_ref().is_some_and(|value| {
            value.plan_id() != plan_id || value.plan_revision() != plan_revision
        }) {
            return Err(RuntimeError::Conflict);
        }
        let binding = self
            .catalog()?
            .mission_evidence_binding(workspace_id, plan_id, plan_revision)
            .map_err(RuntimeError::persistence)?;
        let Some(binding) = binding else {
            return Ok(None);
        };
        if binding.mission_id() != mission_id
            || record
                .as_ref()
                .is_some_and(|value| value.binding_request_id() != binding.request_id())
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(record)
    }

    /// Read a mutation receipt only when the current authenticated Ready actor
    /// matches its original logical actor.
    ///
    /// # Errors
    /// Rejects a foreign/closed Session, non-Ready actor, foreign Workspace,
    /// actor mismatch, and catalog failures.
    pub fn mission_evidence_request_for_actor(
        &self,
        workspace_id: Uuid,
        request_id: Uuid,
        activity_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<MissionEvidenceMutationRecord>, RuntimeError> {
        let workspace_id = workspace(workspace_id)?;
        let (actor, target) = participant_identity(workspace_id, activity_id, session_id)?;
        self.require_participation(target, principal)?;
        self.catalog()?
            .mission_evidence_request_for_actor(workspace_id, identity(request_id)?, actor)
            .map_err(RuntimeError::persistence)
    }
}

fn participant_authority(
    workspace: WorkspaceId,
    participant: MissionEvaluationParticipantSpec,
) -> Result<(ParticipationTarget, MissionEvaluationAuthority), RuntimeError> {
    let (actor, target) =
        participant_identity(workspace, participant.activity_id, participant.session_id)?;
    if participant.expected_workspace_leadership_revision < 1 {
        return Err(RuntimeError::InvalidInput);
    }
    let authority = MissionEvaluationAuthority::new(
        actor,
        Some(participant.expected_workspace_leadership_revision),
    )
    .map_err(RuntimeError::persistence)?;
    Ok((target, authority))
}

fn participant_identity(
    workspace: WorkspaceId,
    activity_id: Uuid,
    session_id: Uuid,
) -> Result<
    (
        heterarchy_orchestration_operations::OperationalActor,
        ParticipationTarget,
    ),
    RuntimeError,
> {
    if activity_id.is_nil() || session_id.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    let agent_id = member_agent_id(workspace.into_uuid(), activity_id, session_id);
    let actor = heterarchy_orchestration_operations::OperationalActor::Participant {
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

fn require_plan_mission(
    catalog: &mut heterarchy_orchestration_operations::OperationsCatalog,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
) -> Result<(), RuntimeError> {
    let version = catalog
        .mission_evaluation_plan_version(workspace_id, plan_id, revision)
        .map_err(RuntimeError::persistence)?
        .ok_or(RuntimeError::NotFound)?;
    if version.mission_id() != mission_id {
        return Err(RuntimeError::Conflict);
    }
    Ok(())
}

fn stable_manifest_id(workspace: Uuid, plan: Uuid, revision: u64, activity: Uuid) -> Uuid {
    derived_uuid(
        MANIFEST_ID_DOMAIN,
        &[
            workspace.as_bytes(),
            plan.as_bytes(),
            &revision.to_be_bytes(),
            activity.as_bytes(),
        ],
    )
}

fn derived_uuid(domain: &[u8], components: &[&[u8]]) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    for component in components {
        hasher.update(component);
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
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
