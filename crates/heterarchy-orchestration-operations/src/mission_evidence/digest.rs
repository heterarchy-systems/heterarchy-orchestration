use std::collections::BTreeMap;

use blake3::Hasher;
use forge_domain::{EvaluationDigest, WorkspaceId};

use crate::{
    MissionEvaluationPlanId, MissionEvaluationPlanVersion, MissionObligationId,
    MissionObligationRecord, MissionWorkUnitId, OperationsError,
};

const PLAN_DOMAIN: &[u8] = b"heterarchy-orchestration-mission-evidence-plan-v1\0";
const CRITERIA_SET_DOMAIN: &[u8] = b"heterarchy-orchestration-mission-evidence-criteria-set-v1\0";
const CRITERION_DOMAIN: &[u8] = b"heterarchy-orchestration-mission-evidence-criterion-v1\0";

/// Canonical local digests for one exact immutable started-plan version.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidencePlanDigests {
    workspace_id: WorkspaceId,
    mission_id: crate::MissionId,
    plan_id: MissionEvaluationPlanId,
    plan_version_id: crate::MissionEvaluationPlanVersionId,
    revision: u64,
    t1_submission_cutoff_ms: i64,
    plan_digest: EvaluationDigest,
    criteria_digest: EvaluationDigest,
    member_criteria_digests: Vec<MissionEvidenceMemberCriteriaDigest>,
}

impl MissionEvidencePlanDigests {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn mission_id(&self) -> crate::MissionId {
        self.mission_id
    }

    pub const fn plan_id(&self) -> MissionEvaluationPlanId {
        self.plan_id
    }

    pub const fn plan_version_id(&self) -> crate::MissionEvaluationPlanVersionId {
        self.plan_version_id
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn t1_submission_cutoff_ms(&self) -> i64 {
        self.t1_submission_cutoff_ms
    }

    #[must_use]
    pub const fn plan_digest(&self) -> EvaluationDigest {
        self.plan_digest
    }

    #[must_use]
    pub const fn criteria_digest(&self) -> EvaluationDigest {
        self.criteria_digest
    }

    pub fn member_criteria_digests(&self) -> &[MissionEvidenceMemberCriteriaDigest] {
        &self.member_criteria_digests
    }
}

/// The local criterion digest aligned with one immutable Mission obligation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvidenceMemberCriteriaDigest {
    obligation_id: MissionObligationId,
    work_unit_id: MissionWorkUnitId,
    participation_id: crate::WorkspaceParticipationId,
    digest: EvaluationDigest,
}

impl MissionEvidenceMemberCriteriaDigest {
    pub const fn obligation_id(self) -> MissionObligationId {
        self.obligation_id
    }

    pub const fn work_unit_id(self) -> MissionWorkUnitId {
        self.work_unit_id
    }

    #[must_use]
    pub const fn participation_id(self) -> crate::WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn digest(self) -> EvaluationDigest {
        self.digest
    }
}

pub(super) fn derive(
    plan: &MissionEvaluationPlanVersion,
    obligations: &[MissionObligationRecord],
) -> Result<MissionEvidencePlanDigests, OperationsError> {
    if plan.revision() == 0
        || plan.t0_commit_at_ms() < 0
        || plan.t1_submission_cutoff_ms() < 0
        || plan.t2_checkpoint_ms() < 0
        || plan.t0_commit_at_ms() >= plan.t1_submission_cutoff_ms()
        || plan.t1_submission_cutoff_ms() > plan.t2_checkpoint_ms()
    {
        return Err(OperationsError::Corrupt("mission evidence plan terms"));
    }

    let mut by_id = BTreeMap::new();
    for obligation in obligations {
        if obligation.workspace_id() != plan.workspace_id()
            || obligation.mission_id() != plan.mission_id()
            || by_id
                .insert(obligation.obligation_id(), obligation)
                .is_some()
        {
            return Err(OperationsError::Corrupt(
                "mission evidence obligation cohort",
            ));
        }
    }
    if by_id.len() != plan.cohort().len() {
        return Err(OperationsError::Corrupt(
            "mission evidence obligation cohort",
        ));
    }

    let mut plan_hasher = Hasher::new();
    plan_hasher.update(PLAN_DOMAIN);
    plan_hasher.update(plan.workspace_id().into_uuid().as_bytes());
    plan_hasher.update(plan.mission_id().into_uuid().as_bytes());
    plan_hasher.update(plan.plan_id().into_uuid().as_bytes());
    plan_hasher.update(plan.version_id().into_uuid().as_bytes());
    plan_hasher.update(&plan.revision().to_be_bytes());
    plan_hasher.update(&plan.t0_commit_at_ms().to_be_bytes());
    plan_hasher.update(&plan.t1_submission_cutoff_ms().to_be_bytes());
    plan_hasher.update(&plan.t2_checkpoint_ms().to_be_bytes());
    write_count(&mut plan_hasher, plan.cohort().len())?;

    let mut criteria_hasher = Hasher::new();
    criteria_hasher.update(CRITERIA_SET_DOMAIN);
    criteria_hasher.update(plan.workspace_id().into_uuid().as_bytes());
    criteria_hasher.update(plan.mission_id().into_uuid().as_bytes());
    criteria_hasher.update(plan.plan_id().into_uuid().as_bytes());
    criteria_hasher.update(&plan.revision().to_be_bytes());
    write_count(&mut criteria_hasher, plan.cohort().len())?;

    let mut member_criteria_digests = Vec::with_capacity(plan.cohort().len());
    for planned in plan.cohort() {
        let obligation =
            by_id
                .get(&planned.obligation_id())
                .copied()
                .ok_or(OperationsError::Corrupt(
                    "mission evidence plan obligation missing",
                ))?;
        if obligation.weight() != planned.weight() {
            return Err(OperationsError::Corrupt(
                "mission evidence plan obligation weight",
            ));
        }
        let criteria = obligation.acceptance_criteria().as_bytes();
        let criteria_length = u64::try_from(criteria.len())
            .map_err(|_| OperationsError::Bounds("mission evidence criteria length"))?;

        plan_hasher.update(planned.obligation_id().into_uuid().as_bytes());
        plan_hasher.update(obligation.work_unit_id().into_uuid().as_bytes());
        plan_hasher.update(obligation.participation_id().into_uuid().as_bytes());
        plan_hasher.update(&planned.weight().get().to_be_bytes());
        plan_hasher.update(&obligation.due_at_ms().to_be_bytes());
        plan_hasher.update(&criteria_length.to_be_bytes());
        plan_hasher.update(criteria);

        let mut criterion_hasher = Hasher::new();
        criterion_hasher.update(CRITERION_DOMAIN);
        criterion_hasher.update(planned.obligation_id().into_uuid().as_bytes());
        criterion_hasher.update(&criteria_length.to_be_bytes());
        criterion_hasher.update(criteria);
        let member_digest = evaluation_digest(criterion_hasher.finalize().as_bytes())?;
        criteria_hasher.update(planned.obligation_id().into_uuid().as_bytes());
        criteria_hasher.update(member_digest.as_bytes());
        member_criteria_digests.push(MissionEvidenceMemberCriteriaDigest {
            obligation_id: planned.obligation_id(),
            work_unit_id: obligation.work_unit_id(),
            participation_id: obligation.participation_id(),
            digest: member_digest,
        });
    }

    let plan_digest = evaluation_digest(plan_hasher.finalize().as_bytes())?;
    let criteria_digest = evaluation_digest(criteria_hasher.finalize().as_bytes())?;
    Ok(MissionEvidencePlanDigests {
        workspace_id: plan.workspace_id(),
        mission_id: plan.mission_id(),
        plan_id: plan.plan_id(),
        plan_version_id: plan.version_id(),
        revision: plan.revision(),
        t1_submission_cutoff_ms: plan.t1_submission_cutoff_ms(),
        plan_digest,
        criteria_digest,
        member_criteria_digests,
    })
}

fn write_count(hasher: &mut Hasher, count: usize) -> Result<(), OperationsError> {
    let count = u64::try_from(count)
        .map_err(|_| OperationsError::Bounds("mission evidence plan cohort"))?;
    hasher.update(&count.to_be_bytes());
    Ok(())
}

fn evaluation_digest(bytes: &[u8; 32]) -> Result<EvaluationDigest, OperationsError> {
    EvaluationDigest::new(*bytes).map_err(|_| OperationsError::Corrupt("mission evidence digest"))
}
