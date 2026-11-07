use std::collections::BTreeMap;

use forge_domain::{
    EvaluationAdmissionTiming, EvaluationOriginalBindingStatus, EvaluationRegisteredManifest,
    EvaluationReviewCut, EvaluationReviewCutMember, EvaluationReviewOutcome, EvaluationReviewState,
    EvaluationT1Coverage, EvaluationT1CoverageCut,
};
use rusqlite::Connection;
use uuid::Uuid;

use crate::{
    MissionEvaluationPlanVersion, MissionObligationId, MissionObligationRecord, OperationsError,
    WorkspaceParticipationId,
};

use super::model::MissionEvidenceObservationMemberParts;
use super::{
    MissionEvidenceDisposition, MissionEvidenceIdentityProof, MissionEvidenceIdentityResolution,
    MissionEvidenceManifestBindingRecord, MissionEvidenceObservationBundle,
    MissionEvidenceObservationMember, MissionEvidenceParticipationMeasures,
    MissionEvidenceUnresolvedReason,
};

pub(super) struct EvaluatedObservation {
    pub(super) members: Vec<MissionEvidenceObservationMember>,
    pub(super) measures: Vec<MissionEvidenceParticipationMeasures>,
}

pub(super) fn evaluate(
    connection: &Connection,
    destination_id: Uuid,
    binding: &MissionEvidenceManifestBindingRecord,
    plan: &MissionEvaluationPlanVersion,
    obligations: &[MissionObligationRecord],
    cuts: &MissionEvidenceObservationBundle,
) -> Result<EvaluatedObservation, OperationsError> {
    validate_cut_scope(binding, plan, cuts)?;
    let obligations_by_id = obligations
        .iter()
        .map(|obligation| (obligation.obligation_id(), obligation))
        .collect::<BTreeMap<_, _>>();
    if obligations_by_id.len() != plan.cohort().len()
        || binding.members().len() != plan.cohort().len()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence observation cohort",
        ));
    }

    let mut members = Vec::with_capacity(binding.members().len());
    for (partition_ordinal, (manifest, cuts)) in binding
        .manifest_bundle()
        .manifests()
        .iter()
        .zip(cuts.partitions())
        .enumerate()
    {
        members.extend(evaluate_partition(
            connection,
            destination_id,
            binding,
            manifest,
            cuts,
            partition_ordinal,
            &obligations_by_id,
        )?);
    }
    let measures = calculate_measures(&members, plan.t1_submission_cutoff_ms())?;
    Ok(EvaluatedObservation { members, measures })
}

fn evaluate_partition(
    connection: &Connection,
    destination_id: Uuid,
    binding: &MissionEvidenceManifestBindingRecord,
    manifest: &EvaluationRegisteredManifest,
    cuts: &super::MissionEvidenceObservationPartition,
    partition_ordinal: usize,
    obligations_by_id: &BTreeMap<MissionObligationId, &MissionObligationRecord>,
) -> Result<Vec<MissionEvidenceObservationMember>, OperationsError> {
    let bound_members = binding
        .members()
        .iter()
        .filter(|member| member.partition_ordinal() as usize == partition_ordinal)
        .collect::<Vec<_>>();
    if bound_members.len() != manifest.members().len()
        || bound_members.len() != cuts.review_cut().members().len()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence partition member count",
        ));
    }
    let mut members = Vec::with_capacity(bound_members.len());
    for (bound, source) in bound_members.iter().zip(cuts.review_cut().members()) {
        if bound.source_activity_id() != cuts.activity_id()
            || source.member() != bound.declaration()
        {
            return Err(OperationsError::Conflict(
                "mission evidence partition member identity",
            ));
        }
        let obligation = obligations_by_id
            .get(&bound.obligation_id())
            .copied()
            .ok_or(OperationsError::Corrupt(
                "mission evidence local obligation missing",
            ))?;
        if obligation.weight().get() != bound.weight()
            || obligation.due_at_ms() != bound.due_at_ms()
            || obligation.work_unit_id() != bound.work_unit_id()
            || obligation.participation_id() != bound.participation_id()
        {
            return Err(OperationsError::Corrupt(
                "mission evidence frozen obligation changed",
            ));
        }
        validate_member_source(bound, source, cuts.t1_cut(), cuts.review_cut())?;
        let target_identity = super::historical_identity::resolve(
            connection,
            destination_id,
            binding.workspace_id(),
            Some(cuts.activity_id()),
            bound.declaration().target_agent_id(),
            bound.declaration().target_session_id(),
        )?;
        let author_identity = source.submission().map(|_| target_identity.clone());
        let reviewer_identity = match source.review() {
            EvaluationReviewState::Reviewed(receipt) => Some(super::historical_identity::resolve(
                connection,
                destination_id,
                binding.workspace_id(),
                None,
                receipt.reviewer_agent_id(),
                receipt.reviewer_session_id(),
            )?),
            EvaluationReviewState::NotApplicable
            | EvaluationReviewState::Pending { .. }
            | EvaluationReviewState::Unknown(_) => None,
        };
        let disposition = disposition(
            manifest,
            bound,
            &target_identity,
            source,
            author_identity.as_ref(),
            reviewer_identity.as_ref(),
        )?;
        members.push(MissionEvidenceObservationMember::from_parts(
            MissionEvidenceObservationMemberParts {
                partition_ordinal: u32::try_from(partition_ordinal)
                    .map_err(|_| OperationsError::Bounds("mission evidence partition ordinal"))?,
                source_activity_id: cuts.activity_id(),
                source: source.clone(),
                participation_id: bound.participation_id(),
                weight: obligation.weight().get(),
                due_at_ms: obligation.due_at_ms(),
                target_identity,
                author_identity,
                reviewer_identity,
                disposition,
            },
        )?);
    }
    Ok(members)
}

fn validate_cut_scope(
    binding: &MissionEvidenceManifestBindingRecord,
    plan: &MissionEvaluationPlanVersion,
    cuts: &MissionEvidenceObservationBundle,
) -> Result<(), OperationsError> {
    if binding.workspace_id() != plan.workspace_id()
        || binding.mission_id() != plan.mission_id()
        || binding.plan_id() != plan.plan_id()
        || binding.plan_revision() != plan.revision()
        || binding.manifest_bundle().manifests().len() != cuts.partitions().len()
    {
        return Err(OperationsError::Conflict(
            "mission evidence cut bundle scope",
        ));
    }
    let mut total = 0_usize;
    for (partition_ordinal, (manifest, cuts)) in binding
        .manifest_bundle()
        .manifests()
        .iter()
        .zip(cuts.partitions())
        .enumerate()
    {
        let proposal = manifest.proposal();
        let t1 = cuts.t1_cut();
        let review = cuts.review_cut();
        let deadline = plan.t1_submission_cutoff_ms();
        if cuts.activity_id() != proposal.source().activity_id()
            || t1.manifest_id() != proposal.manifest_id()
            || t1.manifest_revision() != proposal.revision()
            || t1.manifest_digest() != proposal.digest()
            || t1.deadline().get() != deadline
            || t1.closed_at().get() <= deadline
            || t1.source_high_water().get() < manifest.source_sequence().get()
            || review.t1_cut_id() != t1.cut_id()
            || review.source_high_water().get() <= t1.source_high_water().get()
            || proposal.members().len() != manifest.members().len()
            || t1.members().len() != manifest.members().len()
            || review.members().len() != manifest.members().len()
        {
            return Err(OperationsError::Conflict(
                "mission evidence source partition cut identity",
            ));
        }
        total = total
            .checked_add(proposal.members().len())
            .ok_or(OperationsError::Bounds("mission evidence cut member count"))?;
        let bound_members = binding
            .members()
            .iter()
            .filter(|member| member.partition_ordinal() as usize == partition_ordinal)
            .collect::<Vec<_>>();
        if bound_members.len() != proposal.members().len() {
            return Err(OperationsError::Corrupt(
                "mission evidence bound partition member count",
            ));
        }
        for (index, declaration) in proposal.members().iter().enumerate() {
            let registered = manifest
                .members()
                .get(index)
                .ok_or(OperationsError::Corrupt(
                    "mission evidence registered member",
                ))?;
            let t1_member = t1
                .members()
                .get(index)
                .ok_or(OperationsError::Corrupt("mission evidence T1 member"))?;
            let review_member = review
                .members()
                .get(index)
                .ok_or(OperationsError::Corrupt("mission evidence review member"))?;
            let bound = bound_members
                .get(index)
                .copied()
                .ok_or(OperationsError::Corrupt(
                    "mission evidence bound partition member",
                ))?;
            if registered.declaration() != *declaration
                || t1_member.member() != *declaration
                || review_member.member() != *declaration
                || review_member.coverage() != t1_member.coverage()
                || bound.declaration() != *declaration
                || bound.source_activity_id() != cuts.activity_id()
                || bound.partition_ordinal() as usize != partition_ordinal
            {
                return Err(OperationsError::Conflict(
                    "mission evidence partition member set",
                ));
            }
        }
    }
    if total != plan.cohort().len() || total != binding.members().len() {
        return Err(OperationsError::Conflict(
            "mission evidence cut bundle incomplete",
        ));
    }
    Ok(())
}

pub(super) fn validate_member_source(
    bound: &super::MissionEvidenceBoundMember,
    source: &EvaluationReviewCutMember,
    t1_cut: &EvaluationT1CoverageCut,
    review_cut: &EvaluationReviewCut,
) -> Result<(), OperationsError> {
    let declaration = bound.declaration();
    if source.member() != declaration {
        return Err(OperationsError::Conflict(
            "mission evidence source assignment member",
        ));
    }
    if let Some(submission) = source.submission() {
        if submission.assignment_id() != declaration.assignment_id()
            || submission.author_agent_id() != declaration.target_agent_id()
            || submission.author_session_id() != declaration.target_session_id()
            || submission.source_sequence().get() > review_cut.source_high_water().get()
        {
            return Err(OperationsError::Conflict(
                "mission evidence submission identity",
            ));
        }
        match bound.original_binding() {
            EvaluationOriginalBindingStatus::Bound(original)
                if submission.original_binding_digest() == Some(original.digest())
                    && submission.criteria_digest() == Some(declaration.criteria_digest()) => {}
            EvaluationOriginalBindingStatus::UnknownHistorical
                if submission.original_binding_digest().is_none()
                    && submission.criteria_digest().is_none() => {}
            _ => {
                return Err(OperationsError::Conflict(
                    "mission evidence submission binding",
                ));
            }
        }
    }
    match (source.coverage(), source.submission()) {
        (
            EvaluationT1Coverage::Eligible {
                submitted_revision,
                receipt_sequence,
                admitted_at,
                result_digest,
                sources_digest,
            },
            Some(submission),
        ) if submission.submitted_revision() == submitted_revision
            && submission.source_sequence() == receipt_sequence
            && submission.result_digest() == result_digest
            && submission.sources_digest() == sources_digest
            && matches!(submission.timing(), EvaluationAdmissionTiming::Trusted(time) if time == admitted_at && time.get() <= t1_cut.deadline().get())
            && submitted_revision.get() > declaration.assignment_revision().get()
            && receipt_sequence.get() <= t1_cut.source_high_water().get() => {}
        (EvaluationT1Coverage::Eligible { .. }, _) => {
            return Err(OperationsError::Conflict(
                "mission evidence eligible source receipt",
            ));
        }
        (EvaluationT1Coverage::NoEligibleSubmission, Some(submission)) if !matches!(submission.timing(), EvaluationAdmissionTiming::Trusted(time) if time.get() > t1_cut.deadline().get()) =>
        {
            return Err(OperationsError::Conflict(
                "mission evidence no-eligible source receipt",
            ));
        }
        (EvaluationT1Coverage::NoEligibleSubmission | EvaluationT1Coverage::Unknown { .. }, _) => {}
    }
    if let EvaluationReviewState::Reviewed(review) = source.review() {
        let submission = source.submission().ok_or(OperationsError::Conflict(
            "mission evidence review without source submission",
        ))?;
        if review.assignment_id() != declaration.assignment_id()
            || review.submitted_revision() != submission.submitted_revision()
            || review.author_agent_id() != submission.author_agent_id()
            || review.author_session_id() != submission.author_session_id()
            || review.result_digest() != submission.result_digest()
            || review.source_sequence().get() <= submission.source_sequence().get()
            || review.source_sequence().get() > review_cut.source_high_water().get()
        {
            return Err(OperationsError::Conflict(
                "mission evidence review receipt identity",
            ));
        }
    }
    Ok(())
}

pub(super) fn disposition(
    manifest: &EvaluationRegisteredManifest,
    bound: &super::MissionEvidenceBoundMember,
    target_identity: &MissionEvidenceIdentityResolution,
    source: &EvaluationReviewCutMember,
    author_identity: Option<&MissionEvidenceIdentityResolution>,
    reviewer_identity: Option<&MissionEvidenceIdentityResolution>,
) -> Result<MissionEvidenceDisposition, OperationsError> {
    if matches!(
        manifest.seal_timing(),
        EvaluationAdmissionTiming::Unknown(_)
    ) || matches!(
        manifest.seal_timing(),
        EvaluationAdmissionTiming::Trusted(at)
            if at.get() > manifest.proposal().deadline().get()
    ) || matches!(
        bound.original_binding(),
        EvaluationOriginalBindingStatus::UnknownHistorical
    ) {
        return Ok(unresolved(MissionEvidenceUnresolvedReason::BindingPending));
    }
    let MissionEvidenceIdentityResolution::Verified(target) = target_identity else {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::TargetIdentityPending,
        ));
    };
    let Some(expected_participation) = target.participation_id() else {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::TargetIdentityPending,
        ));
    };
    if expected_participation != bound.participation_id() {
        return Err(OperationsError::Conflict(
            "mission evidence target participation mismatch",
        ));
    }
    if let EvaluationT1Coverage::NoEligibleSubmission = source.coverage() {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::NoEligibleOriginal,
        ));
    }
    if matches!(source.coverage(), EvaluationT1Coverage::Unknown { .. }) {
        return Ok(unresolved(MissionEvidenceUnresolvedReason::CoverageUnknown));
    }
    let Some(author_identity) = author_identity else {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::AuthorIdentityPending,
        ));
    };
    let MissionEvidenceIdentityResolution::Verified(author) = author_identity else {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::AuthorIdentityPending,
        ));
    };
    if author.participation_id() != Some(expected_participation)
        || !same_logical_actor(target, author)?
    {
        return Err(OperationsError::Conflict(
            "mission evidence author target mismatch",
        ));
    }
    let review = match source.review() {
        EvaluationReviewState::NotApplicable => {
            return Ok(unresolved(
                MissionEvidenceUnresolvedReason::ReviewNotAccepted,
            ));
        }
        EvaluationReviewState::Pending { .. } => {
            return Ok(unresolved(MissionEvidenceUnresolvedReason::ReviewPending));
        }
        EvaluationReviewState::Unknown(_) => {
            return Ok(unresolved(MissionEvidenceUnresolvedReason::ReviewUnknown));
        }
        EvaluationReviewState::Reviewed(review) => review,
    };
    if review.outcome() != EvaluationReviewOutcome::Accepted {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::ReviewNotAccepted,
        ));
    }
    let Some(reviewer_identity) = reviewer_identity else {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::ReviewerIdentityPending,
        ));
    };
    let MissionEvidenceIdentityResolution::Verified(reviewer) = reviewer_identity else {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::ReviewerIdentityPending,
        ));
    };
    if same_logical_actor(author, reviewer)? {
        return Ok(unresolved(
            MissionEvidenceUnresolvedReason::ReviewerAliasesAuthor,
        ));
    }
    Ok(MissionEvidenceDisposition::AcceptedOriginal)
}

pub(super) fn same_logical_actor(
    left: &MissionEvidenceIdentityProof,
    right: &MissionEvidenceIdentityProof,
) -> Result<bool, OperationsError> {
    match (left.profile_id(), right.profile_id()) {
        (Some(left_profile), Some(right_profile)) => {
            if left_profile == right_profile {
                Ok(true)
            } else if left.participant_id() == right.participant_id() {
                Err(OperationsError::Corrupt(
                    "mission evidence identity Profile conflict",
                ))
            } else {
                Ok(false)
            }
        }
        _ => Ok(left.participant_id() == right.participant_id()
            || left.actor_agent_id() == right.actor_agent_id()),
    }
}

fn unresolved(reason: MissionEvidenceUnresolvedReason) -> MissionEvidenceDisposition {
    MissionEvidenceDisposition::Unresolved(reason)
}

pub(super) fn calculate_measures(
    members: &[MissionEvidenceObservationMember],
    t1_submission_cutoff_ms: i64,
) -> Result<Vec<MissionEvidenceParticipationMeasures>, OperationsError> {
    #[derive(Clone, Copy, Default)]
    struct Totals {
        planned_due: u64,
        accepted_original_due: u64,
        unresolved_due: u64,
        future: u64,
    }
    let mut by_participation = BTreeMap::<WorkspaceParticipationId, Totals>::new();
    for member in members {
        let totals = by_participation
            .entry(member.participation_id())
            .or_default();
        if member.due_at_ms() > t1_submission_cutoff_ms {
            totals.future = totals
                .future
                .checked_add(member.weight())
                .ok_or(OperationsError::Bounds("mission evidence future weight"))?;
            continue;
        }
        totals.planned_due = totals
            .planned_due
            .checked_add(member.weight())
            .ok_or(OperationsError::Bounds("mission evidence planned weight"))?;
        match member.disposition() {
            MissionEvidenceDisposition::AcceptedOriginal => {
                totals.accepted_original_due = totals
                    .accepted_original_due
                    .checked_add(member.weight())
                    .ok_or(OperationsError::Bounds("mission evidence accepted weight"))?;
            }
            MissionEvidenceDisposition::Unresolved(_) => {
                totals.unresolved_due = totals.unresolved_due.checked_add(member.weight()).ok_or(
                    OperationsError::Bounds("mission evidence unresolved weight"),
                )?;
            }
        }
    }
    Ok(by_participation
        .into_iter()
        .map(|(participation_id, totals)| {
            MissionEvidenceParticipationMeasures::new(
                participation_id,
                totals.planned_due,
                totals.accepted_original_due,
                totals.unresolved_due,
                totals.future,
            )
        })
        .collect())
}
