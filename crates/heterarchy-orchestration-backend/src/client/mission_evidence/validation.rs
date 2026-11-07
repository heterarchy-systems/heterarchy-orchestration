use std::collections::BTreeSet;

use forge_backend::ops::evaluation_evidence::wire as source_wire;
use uuid::Uuid;

use crate::wire::mission_evidence::{
    MissionEvidenceActorKindWire, MissionEvidenceActorReceiptWire,
    MissionEvidenceBindingReadRequest, MissionEvidenceBindingReadResponse,
    MissionEvidenceCandidatePartitionWire, MissionEvidenceDispositionWire,
    MissionEvidenceIdentityProofWire, MissionEvidenceIdentityResolutionWire,
    MissionEvidenceManifestBindRequest, MissionEvidenceManifestBindResponse,
    MissionEvidenceManifestPrepareRequest, MissionEvidenceManifestPrepareResponse,
    MissionEvidenceMutationRequest, MissionEvidenceMutationStatusWire, MissionEvidenceMutationWire,
    MissionEvidenceObservationMemberWire, MissionEvidenceObservationReadRequest,
    MissionEvidenceObservationReadResponse, MissionEvidenceObservationRequest,
    MissionEvidenceObservationResponse, MissionEvidenceObservationWire,
    MissionEvidencePendingReasonWire, MissionEvidencePendingSourceWire,
    MissionEvidencePendingTargetWire, MissionEvidencePlanDigestsRequest,
    MissionEvidencePlanDigestsResponse, MissionEvidencePreparationStatusWire,
    MissionEvidenceRequestReadActor, MissionEvidenceRequestReadRequest,
    MissionEvidenceRequestReadResponse,
};

const MAX_COHORT: usize = 256;
const MAX_SQLITE_INTEGER: u64 = i64::MAX.unsigned_abs();

pub(super) fn valid_plan_digests_request(request: &MissionEvidencePlanDigestsRequest) -> bool {
    valid_scope(&request.scope)
        && !request.session_id.is_nil()
        && !request.sender_activity_id.is_nil()
        && !request.mission_id.is_nil()
        && !request.plan_id.is_nil()
        && valid_revision(request.revision)
}

pub(super) fn valid_plan_digests_response(
    response: &MissionEvidencePlanDigestsResponse,
    request: &MissionEvidencePlanDigestsRequest,
) -> bool {
    if response.scope != request.scope
        || response.mission_id != request.mission_id
        || response.plan_id != request.plan_id
        || response.plan_version_id.is_nil()
        || response.revision != request.revision
        || response.t1_submission_cutoff_ms < 0
        || !valid_digest(&response.plan_digest)
        || !valid_digest(&response.criteria_digest)
        || response.member_criteria_digests.len() > MAX_COHORT
    {
        return false;
    }
    let mut obligations = BTreeSet::new();
    let mut units = BTreeSet::new();
    response.member_criteria_digests.iter().all(|member| {
        !member.obligation_id.is_nil()
            && !member.work_unit_id.is_nil()
            && !member.participation_id.is_nil()
            && valid_digest(&member.digest)
            && obligations.insert(member.obligation_id)
            && units.insert(member.work_unit_id)
    }) && response
        .member_criteria_digests
        .windows(2)
        .all(|pair| pair[0].obligation_id.as_bytes() < pair[1].obligation_id.as_bytes())
}

pub(super) fn valid_prepare_request(request: &MissionEvidenceManifestPrepareRequest) -> bool {
    valid_mutation_context(&request.context)
        && valid_plan_selectors(request.mission_id, request.plan_id, request.plan_revision)
        && valid_candidates(&request.partitions)
}

pub(super) fn valid_prepare_response(
    response: &MissionEvidenceManifestPrepareResponse,
    request: &MissionEvidenceManifestPrepareRequest,
) -> bool {
    if response.scope != request.context.scope
        || response.mission_id != request.mission_id
        || response.plan_id != request.plan_id
        || response.plan_revision != request.plan_revision
        || response.proposals.len() != request.partitions.len()
        || !valid_pending_targets(&response.pending_targets, &request.partitions)
    {
        return false;
    }
    match response.status {
        MissionEvidencePreparationStatusWire::Ready if !response.pending_targets.is_empty() => {
            return false;
        }
        MissionEvidencePreparationStatusWire::Pending if response.pending_targets.is_empty() => {
            return false;
        }
        _ => {}
    }
    let mut seen_activities = BTreeSet::new();
    let mut seen_manifests = BTreeSet::new();
    let mut common_plan: Option<(Uuid, i64, String, i64)> = None;
    let mut common_source: Option<(Uuid, Uuid, i64, String)> = None;
    for proposal in &response.proposals {
        let Some(partition) = request
            .partitions
            .iter()
            .find(|candidate| candidate.activity_id == proposal.source.activity_id)
        else {
            return false;
        };
        if !valid_proposal_for_partition(
            proposal,
            partition,
            request.context.scope.workspace_id,
            request.plan_id,
            request.plan_revision,
        ) || !seen_activities.insert(proposal.source.activity_id)
            || !seen_manifests.insert(proposal.manifest_id)
        {
            return false;
        }
        let plan_binding = (
            proposal.plan.plan_id,
            proposal.plan.revision,
            proposal.plan.digest.clone(),
            proposal.deadline_ms,
        );
        let source_binding = (
            proposal.source.backend_id,
            proposal.source.delegation_destination_id,
            proposal.source.delegation_freeze_revision,
            proposal.source.delegation_manifest_digest.clone(),
        );
        if common_plan
            .as_ref()
            .is_some_and(|expected| expected != &plan_binding)
            || common_source
                .as_ref()
                .is_some_and(|expected| expected != &source_binding)
        {
            return false;
        }
        common_plan.get_or_insert(plan_binding);
        common_source.get_or_insert(source_binding);
    }
    true
}

pub(super) fn valid_bind_request(request: &MissionEvidenceManifestBindRequest) -> bool {
    valid_mutation_context(&request.context)
        && !request.request_id.is_nil()
        && valid_plan_selectors(request.mission_id, request.plan_id, request.plan_revision)
        && valid_candidates(&request.partitions)
}

pub(super) fn valid_bind_response(
    response: &MissionEvidenceManifestBindResponse,
    request: &MissionEvidenceManifestBindRequest,
) -> bool {
    if response.scope != request.context.scope || response.request_id != request.request_id {
        return false;
    }
    match response.status {
        MissionEvidenceMutationStatusWire::Pending => {
            !response.replayed
                && response.binding.is_none()
                && ((response.pending_targets.is_empty()
                    && !response.pending_sources.is_empty()
                    && valid_pending_sources(&response.pending_sources, &request.partitions))
                    || (!response.pending_targets.is_empty()
                        && response.pending_sources.is_empty()
                        && valid_pending_targets(&response.pending_targets, &request.partitions)))
        }
        MissionEvidenceMutationStatusWire::Complete => {
            response.pending_targets.is_empty()
                && response.pending_sources.is_empty()
                && response
                    .binding
                    .as_ref()
                    .is_some_and(|binding| valid_binding_for_request(binding, request))
        }
    }
}

pub(super) fn valid_binding_read_request(request: &MissionEvidenceBindingReadRequest) -> bool {
    valid_scope(&request.scope)
        && !request.session_id.is_nil()
        && !request.mission_id.is_nil()
        && !request.plan_id.is_nil()
        && valid_revision(request.plan_revision)
}

pub(super) fn valid_binding_read_response(
    response: &MissionEvidenceBindingReadResponse,
    request: &MissionEvidenceBindingReadRequest,
) -> bool {
    response.scope == request.scope
        && response.mission_id == request.mission_id
        && response.plan_id == request.plan_id
        && response.plan_revision == request.plan_revision
        && response.binding.as_ref().is_none_or(|binding| {
            valid_binding_wire(
                binding,
                request.scope.workspace_id,
                request.mission_id,
                request.plan_id,
                request.plan_revision,
            )
        })
}

pub(super) fn valid_observation_request(request: &MissionEvidenceObservationRequest) -> bool {
    valid_mutation_context(&request.context)
        && !request.request_id.is_nil()
        && valid_plan_selectors(request.mission_id, request.plan_id, request.plan_revision)
        && request.expected_head_revision <= MAX_SQLITE_INTEGER
}

pub(super) fn valid_observation_response(
    response: &MissionEvidenceObservationResponse,
    request: &MissionEvidenceObservationRequest,
) -> bool {
    if response.scope != request.context.scope || response.request_id != request.request_id {
        return false;
    }
    match response.status {
        MissionEvidenceMutationStatusWire::Pending => {
            !response.replayed
                && response.observation.is_none()
                && ((response.pending_targets.is_empty() && !response.pending_sources.is_empty())
                    || (!response.pending_targets.is_empty()
                        && response.pending_sources.is_empty()))
                && valid_pending_targets(&response.pending_targets, &[])
                && valid_pending_sources(&response.pending_sources, &[])
        }
        MissionEvidenceMutationStatusWire::Complete => {
            response.pending_targets.is_empty()
                && response.pending_sources.is_empty()
                && response.observation.as_ref().is_some_and(|observation| {
                    valid_observation_wire(
                        observation,
                        request.context.scope.workspace_id,
                        request.mission_id,
                        request.plan_id,
                        request.plan_revision,
                        request.expected_head_revision,
                        request.request_id,
                    ) && actor_matches_context(&observation.actor, &request.context)
                })
        }
    }
}

pub(super) fn valid_observation_read_request(
    request: &MissionEvidenceObservationReadRequest,
) -> bool {
    valid_scope(&request.scope)
        && !request.session_id.is_nil()
        && !request.mission_id.is_nil()
        && !request.plan_id.is_nil()
        && !request.request_id.is_nil()
        && valid_revision(request.plan_revision)
}

pub(super) fn valid_observation_read_response(
    response: &MissionEvidenceObservationReadResponse,
    request: &MissionEvidenceObservationReadRequest,
) -> bool {
    response.scope == request.scope
        && response.mission_id == request.mission_id
        && response.plan_id == request.plan_id
        && response.plan_revision == request.plan_revision
        && response.request_id == request.request_id
        && response.observation.as_ref().is_none_or(|observation| {
            valid_observation_wire(
                observation,
                request.scope.workspace_id,
                request.mission_id,
                request.plan_id,
                request.plan_revision,
                observation.expected_head_revision,
                request.request_id,
            )
        })
}

pub(super) fn valid_readback_request(request: &MissionEvidenceRequestReadRequest) -> bool {
    valid_scope(&request.scope)
        && !request.session_id.is_nil()
        && !request.sender_activity_id.is_nil()
        && request.mutation.scope() == &request.scope
        && match &request.mutation {
            MissionEvidenceMutationRequest::Bind(intent) => valid_bind_request(intent),
            MissionEvidenceMutationRequest::Observe(intent) => valid_observation_request(intent),
        }
}

pub(super) fn valid_mutation_readback(
    response: &MissionEvidenceRequestReadResponse,
    mutation: &MissionEvidenceMutationRequest,
    reader: MissionEvidenceRequestReadActor,
) -> bool {
    if response.scope != *mutation.scope()
        || response.session_id != reader.session_id
        || response.sender_activity_id != reader.sender_activity_id
        || response.request_id != mutation.request_id()
    {
        return false;
    }
    match (mutation, &response.mutation) {
        (_, None) => true,
        (
            MissionEvidenceMutationRequest::Bind(request),
            Some(MissionEvidenceMutationWire::ManifestBinding(binding)),
        ) => valid_binding_for_request(binding, request),
        (
            MissionEvidenceMutationRequest::Observe(request),
            Some(MissionEvidenceMutationWire::Observation(observation)),
        ) => {
            valid_observation_wire(
                observation,
                request.context.scope.workspace_id,
                request.mission_id,
                request.plan_id,
                request.plan_revision,
                request.expected_head_revision,
                request.request_id,
            ) && actor_matches_context(&observation.actor, &request.context)
        }
        _ => false,
    }
}

fn valid_binding_for_request(
    binding: &crate::wire::mission_evidence::MissionEvidenceManifestBindingWire,
    request: &MissionEvidenceManifestBindRequest,
) -> bool {
    if !valid_binding_wire(
        binding,
        request.context.scope.workspace_id,
        request.mission_id,
        request.plan_id,
        request.plan_revision,
    ) || binding.request_id != request.request_id
        || !actor_matches_context(&binding.actor, &request.context)
        || binding.manifests.len() != request.partitions.len()
    {
        return false;
    }
    request.partitions.iter().all(|partition| {
        binding
            .manifests
            .iter()
            .find(|manifest| manifest.proposal.source.activity_id == partition.activity_id)
            .is_some_and(|manifest| {
                valid_proposal_for_partition(
                    &manifest.proposal,
                    partition,
                    request.context.scope.workspace_id,
                    request.plan_id,
                    request.plan_revision,
                ) && valid_registered_manifest(manifest)
            })
    })
}

fn valid_binding_wire(
    binding: &crate::wire::mission_evidence::MissionEvidenceManifestBindingWire,
    workspace_id: Uuid,
    mission_id: Uuid,
    plan_id: Uuid,
    plan_revision: u64,
) -> bool {
    if binding.workspace_id != workspace_id
        || binding.mission_id != mission_id
        || binding.plan_id != plan_id
        || binding.plan_revision != plan_revision
        || binding.request_id.is_nil()
        || binding.plan_version_id.is_nil()
        || binding.plan_start_request_id.is_nil()
        || binding.plan_started_at_ms < 0
        || binding.t0_commit_at_ms < 0
        || binding.t1_submission_cutoff_ms <= binding.t0_commit_at_ms
        || binding.t2_checkpoint_ms < binding.t1_submission_cutoff_ms
        || !valid_digest(&binding.local_plan_digest)
        || !valid_digest(&binding.local_criteria_digest)
        || !valid_actor_receipt(&binding.actor)
        || binding.recorded_at_ms < 0
        || binding.manifests.is_empty()
        || binding.manifests.len() > MAX_COHORT
    {
        return false;
    }
    let mut activities = BTreeSet::new();
    let mut manifest_ids = BTreeSet::new();
    let mut obligations = BTreeSet::new();
    let mut units = BTreeSet::new();
    let mut assignments = BTreeSet::new();
    let mut common_source: Option<(Uuid, Uuid, Uuid, Uuid, i64, String)> = None;
    let mut total = 0_usize;
    for manifest in &binding.manifests {
        let source = &manifest.proposal.source;
        let source_binding = (
            source.backend_id,
            source.workspace_id,
            source.delegation_migration_id,
            source.delegation_destination_id,
            source.delegation_freeze_revision,
            source.delegation_manifest_digest.clone(),
        );
        if !valid_registered_manifest(manifest)
            || manifest.proposal.source.workspace_id != workspace_id
            || manifest.proposal.plan.plan_id != plan_id
            || i64::try_from(plan_revision).ok() != Some(manifest.proposal.plan.revision)
            || manifest.proposal.deadline_ms != binding.t1_submission_cutoff_ms
            || manifest.proposal.plan.digest != binding.local_plan_digest
            || !activities.insert(manifest.proposal.source.activity_id)
            || !manifest_ids.insert(manifest.proposal.manifest_id)
            || common_source
                .as_ref()
                .is_some_and(|expected| expected != &source_binding)
        {
            return false;
        }
        common_source.get_or_insert(source_binding);
        total = match total.checked_add(manifest.proposal.members.len()) {
            Some(value) if value <= MAX_COHORT => value,
            _ => return false,
        };
        for member in &manifest.proposal.members {
            if !obligations.insert(member.obligation_id)
                || !units.insert(member.work_unit_id)
                || !assignments.insert(member.assignment_id)
            {
                return false;
            }
        }
    }
    true
}

fn valid_observation_wire(
    observation: &MissionEvidenceObservationWire,
    workspace_id: Uuid,
    mission_id: Uuid,
    plan_id: Uuid,
    plan_revision: u64,
    expected_head_revision: u64,
    request_id: Uuid,
) -> bool {
    if observation.request_id != request_id
        || observation.binding_request_id.is_nil()
        || observation.workspace_id != workspace_id
        || observation.mission_id != mission_id
        || observation.plan_id != plan_id
        || observation.plan_revision != plan_revision
        || observation.expected_head_revision != expected_head_revision
        || observation.revision == 0
        || observation.revision > MAX_SQLITE_INTEGER
        || observation.recorded_at_ms < 0
        || !valid_actor_receipt(&observation.actor)
        || observation.cuts.is_empty()
        || observation.cuts.len() > MAX_COHORT
        || observation.members.len() > MAX_COHORT
        || observation.measures.len() > MAX_COHORT
    {
        return false;
    }
    let Some(expected_revision) = expected_head_revision.checked_add(1) else {
        return false;
    };
    if observation.revision != expected_revision {
        return false;
    }
    let mut activities = BTreeSet::new();
    let mut manifests = BTreeSet::new();
    let mut assignments = BTreeSet::new();
    let mut total_members = 0_usize;
    for pair in &observation.cuts {
        if pair.activity_id.is_nil()
            || !activities.insert(pair.activity_id)
            || !valid_t1_cut(&pair.t1_cut)
            || !valid_review_cut(&pair.review_cut, &pair.t1_cut)
            || !manifests.insert(pair.t1_cut.manifest_id)
        {
            return false;
        }
        if pair.t1_cut.deadline_ms != observation.cuts[0].t1_cut.deadline_ms {
            return false;
        }
        total_members = match total_members.checked_add(pair.t1_cut.members.len()) {
            Some(value) if value <= MAX_COHORT => value,
            _ => return false,
        };
        if pair
            .t1_cut
            .members
            .iter()
            .any(|member| !assignments.insert(member.member.assignment_id))
        {
            return false;
        }
    }
    let member_total = observation.cuts.iter().try_fold(0_usize, |sum, pair| {
        sum.checked_add(pair.review_cut.members.len())
    });
    if member_total != Some(observation.members.len()) {
        return false;
    }
    let mut observation_members = BTreeSet::new();
    if observation.members.iter().any(|member| {
        !valid_observation_member(member, &observation.cuts)
            || !observation_members.insert((
                member.source_activity_id,
                member.source.member.assignment_id,
            ))
    }) {
        return false;
    }
    valid_derived_measures(observation)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ExpectedMeasures {
    planned_due: u64,
    accepted_original_due: u64,
    unresolved_due: u64,
    future: u64,
}

fn valid_derived_measures(observation: &MissionEvidenceObservationWire) -> bool {
    let Some(first_cut) = observation.cuts.first() else {
        return false;
    };
    let deadline = first_cut.t1_cut.deadline_ms;
    let mut expected = std::collections::BTreeMap::<Uuid, ExpectedMeasures>::new();
    for member in &observation.members {
        let totals = expected.entry(member.participation_id).or_default();
        if member.due_at_ms > deadline {
            let Some(future) = totals.future.checked_add(member.weight) else {
                return false;
            };
            totals.future = future;
            continue;
        }
        let Some(planned_due) = totals.planned_due.checked_add(member.weight) else {
            return false;
        };
        totals.planned_due = planned_due;
        match member.disposition {
            MissionEvidenceDispositionWire::AcceptedOriginal => {
                let Some(accepted) = totals.accepted_original_due.checked_add(member.weight) else {
                    return false;
                };
                totals.accepted_original_due = accepted;
            }
            MissionEvidenceDispositionWire::Unresolved(_) => {
                let Some(unresolved) = totals.unresolved_due.checked_add(member.weight) else {
                    return false;
                };
                totals.unresolved_due = unresolved;
            }
        }
    }
    if expected.len() != observation.measures.len() {
        return false;
    }
    let mut seen = BTreeSet::new();
    observation.measures.iter().all(|actual| {
        let Some(expected) = expected.get(&actual.participation_id) else {
            return false;
        };
        actual.participation_id != Uuid::nil()
            && seen.insert(actual.participation_id)
            && actual.planned_due_weight == expected.planned_due
            && actual.accepted_original_due_weight == expected.accepted_original_due
            && actual.unresolved_due_weight == expected.unresolved_due
            && actual.future_weight == expected.future
    })
}

fn valid_observation_member(
    member: &MissionEvidenceObservationMemberWire,
    cuts: &[crate::wire::mission_evidence::MissionEvidenceCutPairWire],
) -> bool {
    let Ok(partition_ordinal) = usize::try_from(member.partition_ordinal) else {
        return false;
    };
    if member.source_activity_id.is_nil()
        || member.participation_id.is_nil()
        || member.weight == 0
        || member.weight > MAX_SQLITE_INTEGER
        || member.due_at_ms < 0
        || partition_ordinal >= cuts.len()
    {
        return false;
    }
    let Some(pair) = cuts
        .iter()
        .find(|pair| pair.activity_id == member.source_activity_id)
    else {
        return false;
    };
    if cuts
        .get(partition_ordinal)
        .is_none_or(|expected| expected.activity_id != member.source_activity_id)
    {
        return false;
    }
    let Some(source_member) = pair
        .review_cut
        .members
        .iter()
        .find(|candidate| candidate.member.assignment_id == member.source.member.assignment_id)
    else {
        return false;
    };
    if source_member != &member.source
        || !identity_resolution_matches(
            &member.target_identity,
            member.source.member.target_agent_id,
            member.source.member.target_session_id,
            Some(member.participation_id),
        )
    {
        return false;
    }
    let author_matches = match (&member.source.submission, &member.author_identity) {
        (None, None) => true,
        (Some(submission), Some(identity)) => identity_resolution_matches(
            identity,
            submission.author_agent_id,
            submission.author_session_id,
            Some(member.participation_id),
        ),
        _ => false,
    };
    let reviewer_matches = match (&member.source.review, &member.reviewer_identity) {
        (source_wire::EvaluationReviewStateWire::Reviewed { receipt }, Some(identity)) => {
            identity_resolution_matches(
                identity,
                receipt.reviewer_agent_id,
                receipt.reviewer_session_id,
                None,
            )
        }
        (_, None) => !matches!(
            &member.source.review,
            source_wire::EvaluationReviewStateWire::Reviewed { .. }
        ),
        _ => false,
    };
    author_matches && reviewer_matches && valid_disposition(member)
}

fn valid_actor_receipt(actor: &MissionEvidenceActorReceiptWire) -> bool {
    if actor.semantic_key.is_empty()
        || actor.semantic_key.len() > 256
        || actor.semantic_key.as_bytes().contains(&0)
        || actor.display_name.is_empty()
        || actor.display_name.len() > 128
        || actor.display_name.as_bytes().contains(&0)
        || actor.participation_id.is_some_and(|value| value.is_nil())
        || actor.profile_id.is_some_and(|value| value.is_nil())
        || actor.participant_id.is_some_and(|value| value.is_nil())
        || actor.session_id.is_none_or(|value| value.is_nil())
        || actor.leadership_revision.is_none_or(|value| value < 1)
    {
        return false;
    }
    match actor.kind {
        MissionEvidenceActorKindWire::ProfileParticipant => {
            let (Some(participation_id), Some(profile_id), Some(participant_id)) = (
                actor.participation_id,
                actor.profile_id,
                actor.participant_id,
            ) else {
                return false;
            };
            !profile_id.is_nil()
                && !participant_id.is_nil()
                && actor.semantic_key == format!("participant:{participation_id}")
        }
        MissionEvidenceActorKindWire::NativeParticipant => {
            let Some(participant_id) = actor.participant_id else {
                return false;
            };
            actor.participation_id.is_none()
                && actor.profile_id.is_none()
                && actor.semantic_key == format!("native-participant:{participant_id}")
        }
        MissionEvidenceActorKindWire::LocalOperator => false,
    }
}

fn actor_matches_context(
    actor: &MissionEvidenceActorReceiptWire,
    context: &crate::wire::mission_evaluation::MissionEvaluationMutationContextWire,
) -> bool {
    valid_actor_receipt(actor)
        && actor.session_id == Some(context.session_id)
        && actor.leadership_revision == Some(context.expected_workspace_leadership_revision)
}

fn identity_resolution_matches(
    identity: &MissionEvidenceIdentityResolutionWire,
    agent_id: Uuid,
    session_id: Uuid,
    expected_participation_id: Option<Uuid>,
) -> bool {
    match identity {
        MissionEvidenceIdentityResolutionWire::Verified(proof) => {
            valid_identity_proof(proof)
                && proof.actor_agent_id == agent_id
                && proof.actor_session_id == session_id
                && proof.participation_id.is_none_or(|actual| {
                    expected_participation_id.is_none_or(|expected| actual == expected)
                })
        }
        MissionEvidenceIdentityResolutionWire::Pending {
            agent_id: actual_agent,
            session_id: actual_session,
            registration_request_id,
            activation_receipt_id,
            ..
        } => {
            *actual_agent == agent_id
                && *actual_session == session_id
                && registration_request_id.is_none_or(|value| !value.is_nil())
                && activation_receipt_id.is_none_or(|value| !value.is_nil())
        }
    }
}

fn valid_identity_proof(proof: &MissionEvidenceIdentityProofWire) -> bool {
    if proof.actor_agent_id.is_nil()
        || proof.actor_session_id.is_nil()
        || proof.participant_id.is_nil()
        || proof.registration_request_id.is_nil()
        || proof.migration_id.is_nil()
        || proof.destination_id.is_nil()
        || proof.freeze_revision == 0
        || !valid_digest(&proof.membership_commit_digest)
        || proof.activation_receipt_id.is_nil()
        || proof.activated_at_ms < 0
        || proof.profile_id.is_some_and(|value| value.is_nil())
        || proof.participation_id.is_some_and(|value| value.is_nil())
        || proof
            .profile_mapping_request_id
            .is_some_and(|value| value.is_nil())
        || proof
            .participation_mapping_request_id
            .is_some_and(|value| value.is_nil())
        || proof.rebind_request_id.is_some_and(|value| value.is_nil())
        || proof.profile_revision.is_some_and(|value| value == 0)
        || proof.session_revision.is_some_and(|value| value == 0)
        || proof
            .participant_membership_revision
            .is_some_and(|value| value == 0)
        || proof
            .participation_mapped_at_ms
            .is_some_and(|value| value < 0)
        || proof.rebind_revision.is_some_and(|value| value == 0)
    {
        return false;
    }
    let profile_shape = proof.profile_id.is_some() == proof.profile_mapping_request_id.is_some()
        && proof.profile_id.is_some() == proof.profile_revision.is_some()
        && proof.profile_id.is_some() == proof.session_revision.is_some();
    let participation_mapping_shape = proof.participation_mapping_request_id.is_some()
        == proof.participation_mapped_at_ms.is_some()
        && proof.participation_id.is_some() == proof.participant_membership_revision.is_some()
        && (proof.participation_mapping_request_id.is_none() || proof.participation_id.is_some());
    let registration_shape = match proof.registration_kind {
        crate::wire::mission_evidence::MissionEvidenceRegistrationKindWire::Original => {
            proof.rebind_request_id.is_none() && proof.rebind_revision.is_none()
        }
        crate::wire::mission_evidence::MissionEvidenceRegistrationKindWire::Replacement => {
            proof.rebind_request_id.is_some() && proof.rebind_revision.is_some()
        }
    };
    let participation_provenance = proof.participation_id.is_none()
        || proof.participation_mapping_request_id.is_some()
        || proof.rebind_request_id.is_some();
    profile_shape
        && participation_mapping_shape
        && registration_shape
        && participation_provenance
        && (proof.participation_id.is_none() || proof.profile_id.is_some())
}

fn valid_disposition(member: &MissionEvidenceObservationMemberWire) -> bool {
    match member.disposition {
        MissionEvidenceDispositionWire::AcceptedOriginal => {
            matches!(
                &member.source.coverage,
                source_wire::EvaluationT1CoverageWire::Eligible { .. }
            ) && matches!(
                &member.source.review,
                source_wire::EvaluationReviewStateWire::Reviewed {
                    receipt: source_wire::EvaluationReviewReceiptWire {
                        outcome: source_wire::EvaluationReviewOutcomeWire::Accepted,
                        ..
                    }
                }
            ) && matches!(
                &member.target_identity,
                MissionEvidenceIdentityResolutionWire::Verified(_)
            ) && member.author_identity.as_ref().is_some_and(|identity| {
                matches!(identity, MissionEvidenceIdentityResolutionWire::Verified(_))
            }) && member.reviewer_identity.as_ref().is_some_and(|identity| {
                matches!(identity, MissionEvidenceIdentityResolutionWire::Verified(_))
            }) && member
                .author_identity
                .as_ref()
                .zip(member.reviewer_identity.as_ref())
                .is_some_and(|(author, reviewer)| identities_are_independent(author, reviewer))
        }
        MissionEvidenceDispositionWire::Unresolved(_) => true,
    }
}

fn identities_are_independent(
    author: &MissionEvidenceIdentityResolutionWire,
    reviewer: &MissionEvidenceIdentityResolutionWire,
) -> bool {
    let (
        MissionEvidenceIdentityResolutionWire::Verified(author),
        MissionEvidenceIdentityResolutionWire::Verified(reviewer),
    ) = (author, reviewer)
    else {
        return false;
    };
    author.actor_agent_id != reviewer.actor_agent_id
        && author.participant_id != reviewer.participant_id
        && author
            .profile_id
            .zip(reviewer.profile_id)
            .is_none_or(|(a, b)| a != b)
        && author
            .participation_id
            .zip(reviewer.participation_id)
            .is_none_or(|(a, b)| a != b)
}

fn valid_t1_cut(cut: &source_wire::EvaluationT1CoverageCutWire) -> bool {
    if cut.cut_id.is_nil()
        || cut.manifest_id.is_nil()
        || cut.manifest_revision <= 0
        || !valid_digest(&cut.manifest_digest)
        || cut.deadline_ms < 0
        || cut.closed_at_ms <= cut.deadline_ms
        || cut.source_high_water <= 0
        || cut.members.is_empty()
        || cut.members.len() > MAX_COHORT
    {
        return false;
    }
    let mut assignments = BTreeSet::new();
    cut.members.iter().all(|member| {
        valid_manifest_member(&member.member)
            && assignments.insert(member.member.assignment_id)
            && valid_coverage(&member.coverage, cut.deadline_ms)
    })
}

fn valid_review_cut(
    cut: &source_wire::EvaluationReviewCutWire,
    t1: &source_wire::EvaluationT1CoverageCutWire,
) -> bool {
    if cut.review_cut_id.is_nil()
        || cut.t1_cut_id != t1.cut_id
        || cut.source_high_water <= t1.source_high_water
        || cut.members.len() != t1.members.len()
    {
        return false;
    }
    cut.members
        .iter()
        .zip(&t1.members)
        .all(|(member, t1_member)| {
            member.member == t1_member.member
                && member.coverage == t1_member.coverage
                && member.submission.as_ref().is_none_or(|submission| {
                    valid_submission(submission)
                        && valid_submission_for_member(
                            submission,
                            &member.member,
                            cut.source_high_water,
                        )
                })
                && valid_review_coverage(member, t1.deadline_ms)
                && valid_review_state(
                    &member.review,
                    member,
                    cut.source_high_water,
                    t1.deadline_ms,
                )
        })
}

fn valid_manifest_member(member: &source_wire::EvaluationManifestMemberWire) -> bool {
    !member.work_unit_id.is_nil()
        && !member.obligation_id.is_nil()
        && !member.assignment_id.is_nil()
        && member.assignment_revision > 0
        && !member.target_agent_id.is_nil()
        && !member.target_session_id.is_nil()
        && valid_digest(&member.criteria_digest)
}

fn valid_coverage(coverage: &source_wire::EvaluationT1CoverageWire, deadline: i64) -> bool {
    match coverage {
        source_wire::EvaluationT1CoverageWire::Eligible {
            submitted_revision,
            receipt_sequence,
            admitted_at_ms,
            result_digest,
            sources_digest,
        } => {
            *submitted_revision > 0
                && *receipt_sequence > 0
                && (0..=deadline).contains(admitted_at_ms)
                && valid_digest(result_digest)
                && sources_digest.as_deref().is_none_or(valid_digest)
        }
        source_wire::EvaluationT1CoverageWire::NoEligibleSubmission
        | source_wire::EvaluationT1CoverageWire::Unknown { .. } => true,
    }
}

fn valid_submission(receipt: &source_wire::EvaluationSubmissionReceiptWire) -> bool {
    !receipt.assignment_id.is_nil()
        && receipt.submitted_revision > 0
        && !receipt.author_agent_id.is_nil()
        && !receipt.author_session_id.is_nil()
        && receipt.source_sequence > 0
        && receipt
            .original_binding_digest
            .as_deref()
            .is_none_or(valid_digest)
        && receipt.criteria_digest.as_deref().is_none_or(valid_digest)
        && receipt.original_binding_digest.is_some() == receipt.criteria_digest.is_some()
        && valid_timing(&receipt.timing)
        && valid_digest(&receipt.result_digest)
        && receipt.sources_digest.as_deref().is_none_or(valid_digest)
}

fn valid_review_state(
    state: &source_wire::EvaluationReviewStateWire,
    member: &source_wire::EvaluationReviewCutMemberWire,
    high_water: i64,
    deadline: i64,
) -> bool {
    match state {
        source_wire::EvaluationReviewStateWire::NotApplicable => match member.submission.as_ref() {
            None => !matches!(
                &member.coverage,
                source_wire::EvaluationT1CoverageWire::Eligible { .. }
            ),
            Some(submission) => {
                matches!(
                    &member.coverage,
                    source_wire::EvaluationT1CoverageWire::NoEligibleSubmission
                ) && matches!(
                    &submission.timing,
                    source_wire::EvaluationAdmissionTimingWire::Trusted { observed_at_ms }
                        if *observed_at_ms > deadline
                )
            }
        },
        source_wire::EvaluationReviewStateWire::Pending {
            submitted_revision,
            receipt_sequence,
        } => member.submission.as_ref().is_some_and(|submission| {
            *submitted_revision == submission.submitted_revision
                && *receipt_sequence == submission.source_sequence
                && *receipt_sequence <= high_water
        }),
        source_wire::EvaluationReviewStateWire::Reviewed { receipt } => {
            let Some(submission) = member.submission.as_ref() else {
                return false;
            };
            receipt.assignment_id == member.member.assignment_id
                && receipt.assignment_id == submission.assignment_id
                && receipt.submitted_revision == submission.submitted_revision
                && receipt.author_agent_id == submission.author_agent_id
                && receipt.author_session_id == submission.author_session_id
                && receipt.result_digest == submission.result_digest
                && receipt.source_sequence > submission.source_sequence
                && receipt.source_sequence <= high_water
                && receipt.review_revision > receipt.submitted_revision
                && !receipt.reviewer_agent_id.is_nil()
                && !receipt.reviewer_session_id.is_nil()
                && receipt.reviewer_agent_id != receipt.author_agent_id
                && valid_digest(&receipt.result_digest)
        }
        source_wire::EvaluationReviewStateWire::Unknown { .. } => true,
    }
}

fn valid_review_coverage(
    member: &source_wire::EvaluationReviewCutMemberWire,
    deadline: i64,
) -> bool {
    match (&member.coverage, &member.submission) {
        (
            source_wire::EvaluationT1CoverageWire::Eligible {
                submitted_revision,
                receipt_sequence,
                admitted_at_ms,
                result_digest,
                sources_digest,
            },
            Some(submission),
        ) => {
            submission.submitted_revision == *submitted_revision
                && submission.source_sequence == *receipt_sequence
                && submission.result_digest == *result_digest
                && submission.sources_digest == *sources_digest
                && matches!(
                    &submission.timing,
                    source_wire::EvaluationAdmissionTimingWire::Trusted { observed_at_ms }
                        if observed_at_ms == admitted_at_ms && *observed_at_ms <= deadline
                )
        }
        (source_wire::EvaluationT1CoverageWire::NoEligibleSubmission, Some(submission)) => {
            matches!(
                &submission.timing,
                source_wire::EvaluationAdmissionTimingWire::Trusted { observed_at_ms }
                    if *observed_at_ms > deadline
            )
        }
        (source_wire::EvaluationT1CoverageWire::Eligible { .. }, None) => false,
        (source_wire::EvaluationT1CoverageWire::NoEligibleSubmission, None)
        | (source_wire::EvaluationT1CoverageWire::Unknown { .. }, _) => true,
    }
}

fn valid_submission_for_member(
    submission: &source_wire::EvaluationSubmissionReceiptWire,
    member: &source_wire::EvaluationManifestMemberWire,
    high_water: i64,
) -> bool {
    submission.assignment_id == member.assignment_id
        && submission.author_agent_id == member.target_agent_id
        && submission.author_session_id == member.target_session_id
        && submission.source_sequence <= high_water
        && (submission.original_binding_digest.is_some() == submission.criteria_digest.is_some())
        && submission
            .criteria_digest
            .as_ref()
            .is_none_or(|digest| digest == &member.criteria_digest)
}

fn valid_timing(timing: &source_wire::EvaluationAdmissionTimingWire) -> bool {
    match timing {
        source_wire::EvaluationAdmissionTimingWire::Trusted { observed_at_ms } => {
            *observed_at_ms >= 0
        }
        source_wire::EvaluationAdmissionTimingWire::Unknown { .. } => true,
    }
}

fn valid_registered_manifest(manifest: &source_wire::EvaluationRegisteredManifestWire) -> bool {
    let proposal = &manifest.proposal;
    if !valid_proposal(proposal)
        || manifest.source_sequence <= 0
        || !valid_timing(&manifest.seal_timing)
        || manifest.members.len() != proposal.members.len()
    {
        return false;
    }
    manifest
        .members
        .iter()
        .zip(&proposal.members)
        .all(|(member, expected)| {
            member.declaration == *expected
                && match &member.binding {
                    source_wire::EvaluationOriginalBindingWire::Bound {
                        digest,
                        assignment_revision,
                        source_sequence,
                    } => {
                        valid_digest(digest)
                            && *assignment_revision == expected.assignment_revision
                            && *source_sequence > 0
                            && *source_sequence <= manifest.source_sequence
                    }
                    source_wire::EvaluationOriginalBindingWire::UnknownHistorical => true,
                }
        })
}

fn valid_proposal(proposal: &source_wire::EvaluationManifestProposalWire) -> bool {
    !proposal.manifest_id.is_nil()
        && proposal.revision > 0
        && valid_source(&proposal.source)
        && !proposal.plan.plan_id.is_nil()
        && proposal.plan.revision > 0
        && valid_digest(&proposal.plan.digest)
        && proposal.deadline_ms >= 0
        && valid_digest(&proposal.digest)
        && !proposal.members.is_empty()
        && proposal.members.len() <= MAX_COHORT
        && proposal.members.iter().all(valid_manifest_member)
        && proposal.members.windows(2).all(|pair| {
            (
                pair[0].work_unit_id,
                pair[0].obligation_id,
                pair[0].assignment_id,
            ) < (
                pair[1].work_unit_id,
                pair[1].obligation_id,
                pair[1].assignment_id,
            )
        })
}

fn valid_proposal_for_partition(
    proposal: &source_wire::EvaluationManifestProposalWire,
    partition: &MissionEvidenceCandidatePartitionWire,
    workspace_id: Uuid,
    plan_id: Uuid,
    plan_revision: u64,
) -> bool {
    valid_proposal(proposal)
        && proposal.source.workspace_id == workspace_id
        && proposal.source.activity_id == partition.activity_id
        && proposal.plan.plan_id == plan_id
        && i64::try_from(plan_revision).ok() == Some(proposal.plan.revision)
        && proposal.members.len() == partition.members.len()
        && partition.members.iter().all(|candidate| {
            proposal.members.iter().any(|member| {
                member.obligation_id == candidate.obligation_id
                    && member.assignment_id == candidate.assignment_id
                    && member.assignment_revision == candidate.assignment_revision
                    && member.target_agent_id == candidate.target_agent_id
                    && member.target_session_id == candidate.target_session_id
            })
        })
}

fn valid_source(source: &source_wire::EvaluationSourceIdentityWire) -> bool {
    !source.backend_id.is_nil()
        && !source.workspace_id.is_nil()
        && !source.activity_id.is_nil()
        && !source.delegation_migration_id.is_nil()
        && !source.delegation_destination_id.is_nil()
        && source.delegation_freeze_revision > 0
        && valid_digest(&source.delegation_manifest_digest)
}

fn valid_pending_targets(
    targets: &[MissionEvidencePendingTargetWire],
    partitions: &[MissionEvidenceCandidatePartitionWire],
) -> bool {
    if targets.is_empty() {
        return true;
    }
    if partitions.is_empty() {
        return targets.iter().all(|target| {
            !target.obligation_id.is_nil()
                && !target.agent_id.is_nil()
                && !target.session_id.is_nil()
                && target
                    .registration_request_id
                    .is_none_or(|value| !value.is_nil())
                && target
                    .activation_receipt_id
                    .is_none_or(|value| !value.is_nil())
        });
    }
    let candidates = partitions
        .iter()
        .flat_map(|partition| partition.members.iter())
        .collect::<Vec<_>>();
    let mut obligations = BTreeSet::new();
    targets.iter().all(|target| {
        let Some(candidate) = candidates
            .iter()
            .find(|candidate| candidate.obligation_id == target.obligation_id)
        else {
            return false;
        };
        target.agent_id == candidate.target_agent_id
            && target.session_id == candidate.target_session_id
            && obligations.insert(target.obligation_id)
            && target
                .registration_request_id
                .is_none_or(|value| !value.is_nil())
            && target
                .activation_receipt_id
                .is_none_or(|value| !value.is_nil())
    })
}

fn valid_pending_sources(
    sources: &[MissionEvidencePendingSourceWire],
    partitions: &[MissionEvidenceCandidatePartitionWire],
) -> bool {
    if sources.is_empty() {
        return true;
    }
    let mut activities = BTreeSet::new();
    sources.iter().all(|source| {
        !source.activity_id.is_nil()
            && !source.manifest_id.is_nil()
            && source.manifest_revision > 0
            && valid_digest(&source.manifest_digest)
            && match source.stage {
                crate::wire::mission_evidence::MissionEvidenceSourceStageWire::Manifest => {
                    source.cut_id.is_none() && source.review_cut_id.is_none()
                }
                crate::wire::mission_evidence::MissionEvidenceSourceStageWire::T1Cut => {
                    source.cut_id.is_some_and(|value| !value.is_nil())
                        && source.review_cut_id.is_none()
                }
                crate::wire::mission_evidence::MissionEvidenceSourceStageWire::ReviewCut => {
                    source.cut_id.is_some_and(|value| !value.is_nil())
                        && source.review_cut_id.is_some_and(|value| !value.is_nil())
                }
            }
            && match source.reason {
                MissionEvidencePendingReasonWire::TargetHistoryUnresolved => false,
                MissionEvidencePendingReasonWire::T1NotClosed => {
                    source.stage
                        == crate::wire::mission_evidence::MissionEvidenceSourceStageWire::T1Cut
                }
                MissionEvidencePendingReasonWire::SourceUnavailable
                | MissionEvidencePendingReasonWire::MutationOutcomeUnknown
                | MissionEvidencePendingReasonWire::ReadbackUnavailable
                | MissionEvidencePendingReasonWire::SourceRecordMissing => true,
            }
            && (partitions.is_empty()
                || partitions
                    .iter()
                    .any(|candidate| candidate.activity_id == source.activity_id))
            && activities.insert(source.activity_id)
    })
}

fn valid_candidates(partitions: &[MissionEvidenceCandidatePartitionWire]) -> bool {
    if partitions.is_empty() || partitions.len() > MAX_COHORT {
        return false;
    }
    let member_count = partitions.iter().try_fold(0_usize, |sum, partition| {
        sum.checked_add(partition.members.len())
    });
    if member_count.is_none_or(|count| count == 0 || count > MAX_COHORT) {
        return false;
    }
    let mut activities = BTreeSet::new();
    let mut obligations = BTreeSet::new();
    let mut assignments = BTreeSet::new();
    partitions.iter().all(|partition| {
        !partition.activity_id.is_nil()
            && !partition.members.is_empty()
            && activities.insert(partition.activity_id)
            && partition.members.iter().all(|member| {
                !member.obligation_id.is_nil()
                    && !member.assignment_id.is_nil()
                    && member.assignment_revision > 0
                    && !member.target_agent_id.is_nil()
                    && !member.target_session_id.is_nil()
                    && obligations.insert(member.obligation_id)
                    && assignments.insert(member.assignment_id)
            })
    })
}

fn valid_mutation_context(
    context: &crate::wire::mission_evaluation::MissionEvaluationMutationContextWire,
) -> bool {
    valid_scope(&context.scope)
        && !context.session_id.is_nil()
        && !context.sender_activity_id.is_nil()
        && context.expected_workspace_leadership_revision >= 1
}

fn valid_plan_selectors(mission_id: Uuid, plan_id: Uuid, revision: u64) -> bool {
    !mission_id.is_nil() && !plan_id.is_nil() && valid_revision(revision)
}

fn valid_scope(scope: &crate::wire::WorkspaceQueryScope) -> bool {
    crate::client::teams::valid_scope(scope)
}

fn valid_revision(revision: u64) -> bool {
    (1..=MAX_SQLITE_INTEGER).contains(&revision)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::mission_evidence::{
        MissionEvidenceCutPairWire, MissionEvidenceParticipationMeasureWire,
    };
    use crate::wire::{
        WorkspaceQueryScope, mission_evaluation::MissionEvaluationMutationContextWire,
    };

    const DIGEST: &str = "ab0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd";

    #[test]
    fn maximum_observation_projection_stays_within_the_evidence_response_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = observation_request();
        let response = max_observation_response(&request);
        assert!(valid_observation_response(&response, &request));
        let encoded = serde_json::to_vec(&response)?;
        assert!(encoded.len() > 2 * 1024 * 1024);
        assert!(
            encoded.len() <= super::super::MAX_MISSION_EVIDENCE_RESPONSE_BYTES,
            "max-cohort observation response was {} bytes",
            encoded.len()
        );

        let mut wrong_revision = response.clone();
        if let Some(observation) = wrong_revision.observation.as_mut() {
            observation.revision = 2;
        }
        assert!(!valid_observation_response(&wrong_revision, &request));

        let mut missing_measures = response.clone();
        if let Some(observation) = missing_measures.observation.as_mut() {
            observation.measures.clear();
        }
        assert!(!valid_observation_response(&missing_measures, &request));

        let mut inconsistent_measures = response.clone();
        if let Some(observation) = inconsistent_measures.observation.as_mut()
            && let Some(measure) = observation.measures.first_mut()
        {
            measure.unresolved_due_weight = 1;
        }
        assert!(!valid_observation_response(
            &inconsistent_measures,
            &request
        ));

        let mut oversized = response;
        if let Some(observation) = oversized.observation.as_mut() {
            observation.cuts.push(observation.cuts[0].clone());
        }
        assert!(!valid_observation_response(&oversized, &request));
        Ok(())
    }

    #[test]
    fn maximum_candidate_request_and_recovery_envelope_fit_the_path_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let context = MissionEvaluationMutationContextWire {
            scope: scope(),
            session_id: id(30),
            sender_activity_id: id(31),
            expected_workspace_leadership_revision: i64::MAX,
        };
        let partitions = (0..MAX_COHORT)
            .map(|index| {
                let ordinal = index as u128;
                MissionEvidenceCandidatePartitionWire {
                    activity_id: id(1_000 + ordinal),
                    members: vec![
                        crate::wire::mission_evidence::MissionEvidenceCandidateMemberWire {
                            obligation_id: id(2_000 + ordinal),
                            assignment_id: id(3_000 + ordinal),
                            assignment_revision: i64::MAX,
                            target_agent_id: id(4_000 + ordinal),
                            target_session_id: id(5_000 + ordinal),
                        },
                    ],
                }
            })
            .collect::<Vec<_>>();
        let bind = MissionEvidenceManifestBindRequest {
            context,
            request_id: id(6),
            mission_id: id(7),
            plan_id: id(8),
            plan_revision: 1,
            partitions,
        };
        assert!(valid_bind_request(&bind));
        let intent = MissionEvidenceMutationRequest::Bind(bind.clone());
        let readback = MissionEvidenceRequestReadRequest {
            scope: bind.context.scope.clone(),
            session_id: id(40),
            sender_activity_id: id(41),
            mutation: intent,
        };
        assert!(valid_readback_request(&readback));

        let bind_bytes = serde_json::to_vec(&bind)?;
        let readback_bytes = serde_json::to_vec(&readback)?;
        assert!(bind_bytes.len() > 64 * 1024);
        assert!(readback_bytes.len() > bind_bytes.len());
        assert!(bind_bytes.len() <= super::super::MAX_MISSION_EVIDENCE_REQUEST_BYTES);
        assert!(readback_bytes.len() <= super::super::MAX_MISSION_EVIDENCE_REQUEST_BYTES);
        Ok(())
    }

    #[test]
    fn evidence_response_limit_applies_only_to_canonical_evidence_paths() {
        let workspace = id(1);
        assert!(super::super::is_mission_evidence_path(&format!(
            "/internal/v1/workspaces/{workspace}/mission-evidence/manifests/bind"
        )));
        assert!(super::super::is_mission_evidence_path(&format!(
            "/internal/v1/workspaces/{workspace}/mission-evidence-requests/{}/query",
            id(2)
        )));
        assert!(!super::super::is_mission_evidence_path(&format!(
            "/internal/v1/workspaces/{workspace}/missions/create"
        )));
        assert!(!super::super::is_mission_evidence_path(&format!(
            "/internal/v1/workspaces/{workspace}/mission-evidence/plans/not-a-uuid/revisions/1/binding/query"
        )));
    }

    fn observation_request() -> MissionEvidenceObservationRequest {
        MissionEvidenceObservationRequest {
            context: MissionEvaluationMutationContextWire {
                scope: scope(),
                session_id: id(10),
                sender_activity_id: id(11),
                expected_workspace_leadership_revision: 3,
            },
            request_id: id(12),
            mission_id: id(13),
            plan_id: id(14),
            plan_revision: MAX_SQLITE_INTEGER,
            expected_head_revision: MAX_SQLITE_INTEGER - 1,
        }
    }

    fn max_observation_response(
        request: &MissionEvidenceObservationRequest,
    ) -> MissionEvidenceObservationResponse {
        let cuts = (0..MAX_COHORT)
            .map(|index| {
                let ordinal = index as u128;
                let activity = id(1_000 + ordinal);
                let member = manifest_member(index);
                let source = review_member(index);
                let cut_id = id(2_000 + ordinal);
                MissionEvidenceCutPairWire {
                    activity_id: activity,
                    t1_cut: source_wire::EvaluationT1CoverageCutWire {
                        cut_id,
                        manifest_id: id(3_000 + ordinal),
                        manifest_revision: i64::MAX,
                        manifest_digest: DIGEST.to_owned(),
                        deadline_ms: i64::MAX - 1,
                        closed_at_ms: i64::MAX,
                        source_high_water: i64::MAX - 1,
                        members: vec![source_wire::EvaluationT1CoverageMemberWire {
                            member: member.clone(),
                            coverage: coverage(),
                        }],
                    },
                    review_cut: source_wire::EvaluationReviewCutWire {
                        review_cut_id: id(4_000 + ordinal),
                        t1_cut_id: cut_id,
                        source_high_water: i64::MAX,
                        members: vec![source],
                    },
                }
            })
            .collect::<Vec<_>>();
        let members = (0..MAX_COHORT)
            .map(|index| {
                let ordinal = u32::try_from(index).unwrap_or_default();
                let source = review_member(index);
                let target = id(5_000 + u128::from(ordinal));
                let session = id(6_000 + u128::from(ordinal));
                let participation = id(7_000 + u128::from(ordinal));
                MissionEvidenceObservationMemberWire {
                    partition_ordinal: ordinal,
                    source_activity_id: id(1_000 + u128::from(ordinal)),
                    source,
                    participation_id: participation,
                    weight: MAX_SQLITE_INTEGER,
                    due_at_ms: i64::MAX - 1,
                    target_identity: verified_identity(
                        target,
                        session,
                        participation,
                        u128::from(ordinal),
                    ),
                    author_identity: Some(verified_identity(
                        target,
                        session,
                        participation,
                        u128::from(ordinal),
                    )),
                    reviewer_identity: Some(verified_identity(
                        id(8_000 + u128::from(ordinal)),
                        id(9_000 + u128::from(ordinal)),
                        id(10_000 + u128::from(ordinal)),
                        1_000 + u128::from(ordinal),
                    )),
                    disposition: MissionEvidenceDispositionWire::AcceptedOriginal,
                }
            })
            .collect::<Vec<_>>();
        let measures = (0..MAX_COHORT)
            .map(|index| MissionEvidenceParticipationMeasureWire {
                participation_id: id(7_000 + index as u128),
                planned_due_weight: MAX_SQLITE_INTEGER,
                accepted_original_due_weight: MAX_SQLITE_INTEGER,
                unresolved_due_weight: 0,
                future_weight: 0,
            })
            .collect();
        MissionEvidenceObservationResponse {
            scope: request.context.scope.clone(),
            request_id: request.request_id,
            status: MissionEvidenceMutationStatusWire::Complete,
            replayed: false,
            observation: Some(MissionEvidenceObservationWire {
                request_id: request.request_id,
                binding_request_id: id(15),
                workspace_id: request.context.scope.workspace_id,
                mission_id: request.mission_id,
                plan_id: request.plan_id,
                plan_revision: request.plan_revision,
                revision: MAX_SQLITE_INTEGER,
                expected_head_revision: request.expected_head_revision,
                cuts,
                members,
                measures,
                actor: actor_receipt(request.context.session_id),
                recorded_at_ms: i64::MAX,
            }),
            pending_targets: Vec::new(),
            pending_sources: Vec::new(),
        }
    }

    fn review_member(index: usize) -> source_wire::EvaluationReviewCutMemberWire {
        let member = manifest_member(index);
        let receipt = source_wire::EvaluationSubmissionReceiptWire {
            assignment_id: member.assignment_id,
            submitted_revision: i64::MAX - 2,
            author_agent_id: member.target_agent_id,
            author_session_id: member.target_session_id,
            original_binding_digest: Some(DIGEST.to_owned()),
            criteria_digest: Some(member.criteria_digest.clone()),
            source_sequence: i64::MAX - 2,
            timing: source_wire::EvaluationAdmissionTimingWire::Trusted {
                observed_at_ms: i64::MAX - 2,
            },
            result_digest: DIGEST.to_owned(),
            sources_digest: Some(DIGEST.to_owned()),
        };
        source_wire::EvaluationReviewCutMemberWire {
            member,
            coverage: coverage(),
            submission: Some(receipt.clone()),
            review: source_wire::EvaluationReviewStateWire::Reviewed {
                receipt: source_wire::EvaluationReviewReceiptWire {
                    assignment_id: receipt.assignment_id,
                    submitted_revision: receipt.submitted_revision,
                    review_revision: i64::MAX,
                    source_sequence: i64::MAX,
                    outcome: source_wire::EvaluationReviewOutcomeWire::Accepted,
                    author_agent_id: receipt.author_agent_id,
                    author_session_id: receipt.author_session_id,
                    reviewer_agent_id: id(8_000 + index as u128),
                    reviewer_session_id: id(9_000 + index as u128),
                    result_digest: receipt.result_digest,
                },
            },
        }
    }

    fn manifest_member(index: usize) -> source_wire::EvaluationManifestMemberWire {
        let ordinal = index as u128;
        source_wire::EvaluationManifestMemberWire {
            work_unit_id: id(20_000 + ordinal),
            obligation_id: id(30_000 + ordinal),
            assignment_id: id(40_000 + ordinal),
            assignment_revision: i64::MAX,
            target_agent_id: id(5_000 + ordinal),
            target_session_id: id(6_000 + ordinal),
            criteria_digest: DIGEST.to_owned(),
        }
    }

    fn coverage() -> source_wire::EvaluationT1CoverageWire {
        source_wire::EvaluationT1CoverageWire::Eligible {
            submitted_revision: i64::MAX - 2,
            receipt_sequence: i64::MAX - 2,
            admitted_at_ms: i64::MAX - 2,
            result_digest: DIGEST.to_owned(),
            sources_digest: Some(DIGEST.to_owned()),
        }
    }

    fn verified_identity(
        agent: Uuid,
        session: Uuid,
        participation: Uuid,
        seed: u128,
    ) -> MissionEvidenceIdentityResolutionWire {
        MissionEvidenceIdentityResolutionWire::Verified(Box::new(
            MissionEvidenceIdentityProofWire {
                actor_agent_id: agent,
                actor_session_id: session,
                participant_id: id(11_000 + seed),
                profile_id: Some(id(12_000 + seed)),
                participation_id: Some(participation),
                registration_kind:
                    crate::wire::mission_evidence::MissionEvidenceRegistrationKindWire::Replacement,
                registration_request_id: id(13_000 + seed),
                migration_id: id(14_000 + seed),
                destination_id: id(15_000 + seed),
                freeze_revision: u64::MAX,
                membership_commit_digest: DIGEST.to_owned(),
                activation_receipt_id: id(16_000 + seed),
                activated_at_ms: i64::MAX,
                profile_mapping_request_id: Some(id(17_000 + seed)),
                profile_revision: Some(u64::MAX),
                session_revision: Some(u64::MAX),
                participation_mapping_request_id: Some(id(18_000 + seed)),
                participant_membership_revision: Some(u64::MAX),
                participation_mapped_at_ms: Some(i64::MAX),
                rebind_request_id: Some(id(19_000 + seed)),
                rebind_revision: Some(u64::MAX),
            },
        ))
    }

    fn actor_receipt(session_id: Uuid) -> MissionEvidenceActorReceiptWire {
        let participation_id = id(50);
        MissionEvidenceActorReceiptWire {
            kind: MissionEvidenceActorKindWire::ProfileParticipant,
            semantic_key: format!("participant:{participation_id}"),
            participation_id: Some(participation_id),
            profile_id: Some(id(51)),
            participant_id: Some(id(52)),
            session_id: Some(session_id),
            display_name: "\\".repeat(128),
            leadership_revision: Some(3),
        }
    }

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: id(1),
            location_id: id(2),
            location_revision: i64::MAX,
            workspace_revision: i64::MAX,
            locator: "\\".repeat(4_096),
        }
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }
}
