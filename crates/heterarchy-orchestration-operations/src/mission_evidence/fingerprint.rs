use blake3::Hasher;
use forge_domain::{
    EvaluationAdmissionTiming, EvaluationClockUncertainty, EvaluationCoverageUnknownReason,
    EvaluationOriginalBindingStatus, EvaluationReviewCutMember, EvaluationReviewOutcome,
    EvaluationReviewState, EvaluationT1Coverage,
};

use crate::OperationsError;
use crate::mission_evaluation::MissionActorIdentity;
use uuid::Uuid;

use super::{MissionEvidenceManifestBindingCommand, MissionEvidenceObservationCommand};

const BIND_DOMAIN: &[u8] = b"heterarchy-orchestration-mission-evidence-bind-request-v1\0";
const OBSERVE_DOMAIN: &[u8] = b"heterarchy-orchestration-mission-evidence-observation-request-v1\0";

pub(super) fn binding(
    command: &MissionEvidenceManifestBindingCommand,
    actor: &MissionActorIdentity,
) -> Result<[u8; 32], OperationsError> {
    let mut hash = Hasher::new();
    hash.update(BIND_DOMAIN);
    write_actor(&mut hash, actor)?;
    hash.update(command.workspace_id().into_uuid().as_bytes());
    hash.update(command.plan_id().into_uuid().as_bytes());
    hash.update(&command.plan_revision().to_be_bytes());
    hash.update(&0_u64.to_be_bytes());

    write_count(&mut hash, command.bundle().manifests().len())?;
    for manifest in command.bundle().manifests() {
        let proposal = manifest.proposal();
        hash.update(proposal.manifest_id().into_uuid().as_bytes());
        hash.update(&proposal.revision().get().to_be_bytes());
        write_source(&mut hash, proposal.source());
        hash.update(proposal.plan().plan_id().into_uuid().as_bytes());
        hash.update(&proposal.plan().revision().get().to_be_bytes());
        hash.update(proposal.plan().digest().as_bytes());
        hash.update(&proposal.deadline().get().to_be_bytes());
        hash.update(proposal.digest().as_bytes());
        hash.update(&manifest.source_sequence().get().to_be_bytes());
        write_timing(&mut hash, manifest.seal_timing());
        write_count(&mut hash, proposal.members().len())?;
        for (declaration, member) in proposal.members().iter().zip(manifest.members()) {
            if *declaration != member.declaration() {
                return Err(OperationsError::Conflict(
                    "mission evidence manifest member set",
                ));
            }
            write_declaration(&mut hash, *declaration);
            match member.binding() {
                EvaluationOriginalBindingStatus::Bound(binding) => {
                    hash.update(&[0]);
                    hash.update(binding.digest().as_bytes());
                    hash.update(&binding.assignment_revision().get().to_be_bytes());
                    hash.update(&binding.source_sequence().get().to_be_bytes());
                }
                EvaluationOriginalBindingStatus::UnknownHistorical => {
                    hash.update(&[1]);
                }
            }
        }
    }
    Ok(*hash.finalize().as_bytes())
}

pub(super) fn observation(
    command: &MissionEvidenceObservationCommand,
    actor: &MissionActorIdentity,
) -> Result<[u8; 32], OperationsError> {
    let mut hash = Hasher::new();
    hash.update(OBSERVE_DOMAIN);
    write_actor(&mut hash, actor)?;
    hash.update(command.workspace_id().into_uuid().as_bytes());
    hash.update(command.plan_id().into_uuid().as_bytes());
    hash.update(&command.plan_revision().to_be_bytes());
    hash.update(&command.expected_head_revision().to_be_bytes());

    write_count(&mut hash, command.cuts().partitions().len())?;
    for partition in command.cuts().partitions() {
        hash.update(partition.activity_id().into_uuid().as_bytes());
        let t1 = partition.t1_cut();
        hash.update(t1.cut_id().into_uuid().as_bytes());
        hash.update(t1.manifest_id().into_uuid().as_bytes());
        hash.update(&t1.manifest_revision().get().to_be_bytes());
        hash.update(t1.manifest_digest().as_bytes());
        hash.update(&t1.deadline().get().to_be_bytes());
        hash.update(&t1.closed_at().get().to_be_bytes());
        hash.update(&t1.source_high_water().get().to_be_bytes());
        write_count(&mut hash, t1.members().len())?;
        for member in t1.members() {
            write_declaration(&mut hash, member.member());
            write_coverage(&mut hash, member.coverage());
        }

        let review = partition.review_cut();
        hash.update(review.review_cut_id().into_uuid().as_bytes());
        hash.update(review.t1_cut_id().into_uuid().as_bytes());
        hash.update(&review.source_high_water().get().to_be_bytes());
        write_count(&mut hash, review.members().len())?;
        for member in review.members() {
            write_review_member(&mut hash, member);
        }
    }
    Ok(*hash.finalize().as_bytes())
}

fn write_actor(hash: &mut Hasher, actor: &MissionActorIdentity) -> Result<(), OperationsError> {
    actor.validate()?;
    hash.update(&actor.kind.to_be_bytes());
    write_bytes(hash, actor.key.as_bytes())?;
    write_optional_uuid(
        hash,
        actor
            .participation_id
            .map(crate::WorkspaceParticipationId::into_uuid),
    );
    write_optional_uuid(hash, actor.profile_id);
    write_optional_uuid(hash, actor.participant_id);
    Ok(())
}

fn write_source(hash: &mut Hasher, source: forge_domain::EvaluationSourceIdentity) {
    hash.update(source.backend_id().into_uuid().as_bytes());
    hash.update(source.workspace_id().into_uuid().as_bytes());
    hash.update(source.activity_id().into_uuid().as_bytes());
    hash.update(source.delegation_migration_id().as_bytes());
    hash.update(source.delegation_destination_id().as_bytes());
    hash.update(&source.delegation_freeze_revision().get().to_be_bytes());
    hash.update(source.delegation_manifest_digest().as_bytes());
}

fn write_declaration(hash: &mut Hasher, member: forge_domain::EvaluationManifestMember) {
    hash.update(member.work_unit_id().into_uuid().as_bytes());
    hash.update(member.obligation_id().into_uuid().as_bytes());
    hash.update(member.assignment_id().into_uuid().as_bytes());
    hash.update(&member.assignment_revision().get().to_be_bytes());
    hash.update(member.target_agent_id().into_uuid().as_bytes());
    hash.update(member.target_session_id().into_uuid().as_bytes());
    hash.update(member.criteria_digest().as_bytes());
}

fn write_timing(hash: &mut Hasher, timing: EvaluationAdmissionTiming) {
    match timing {
        EvaluationAdmissionTiming::Trusted(value) => {
            hash.update(&[0]);
            hash.update(&value.get().to_be_bytes());
        }
        EvaluationAdmissionTiming::Unknown(reason) => {
            hash.update(&[1, encode_clock_uncertainty(reason)]);
        }
    }
}

fn write_coverage(hash: &mut Hasher, coverage: EvaluationT1Coverage) {
    match coverage {
        EvaluationT1Coverage::Eligible {
            submitted_revision,
            receipt_sequence,
            admitted_at,
            result_digest,
            sources_digest,
        } => {
            hash.update(&[0]);
            hash.update(&submitted_revision.get().to_be_bytes());
            hash.update(&receipt_sequence.get().to_be_bytes());
            hash.update(&admitted_at.get().to_be_bytes());
            hash.update(result_digest.as_bytes());
            write_optional_digest(hash, sources_digest);
        }
        EvaluationT1Coverage::NoEligibleSubmission => {
            hash.update(&[1]);
        }
        EvaluationT1Coverage::Unknown { reason } => {
            hash.update(&[2, encode_unknown_reason(reason)]);
        }
    }
}

fn write_review_member(hash: &mut Hasher, member: &EvaluationReviewCutMember) {
    write_declaration(hash, member.member());
    write_coverage(hash, member.coverage());
    match member.submission() {
        Some(submission) => {
            hash.update(&[1]);
            hash.update(submission.assignment_id().into_uuid().as_bytes());
            hash.update(&submission.submitted_revision().get().to_be_bytes());
            hash.update(submission.author_agent_id().into_uuid().as_bytes());
            hash.update(submission.author_session_id().into_uuid().as_bytes());
            write_optional_digest(hash, submission.original_binding_digest());
            write_optional_digest(hash, submission.criteria_digest());
            hash.update(&submission.source_sequence().get().to_be_bytes());
            write_timing(hash, submission.timing());
            hash.update(submission.result_digest().as_bytes());
            write_optional_digest(hash, submission.sources_digest());
        }
        None => {
            hash.update(&[0]);
        }
    }
    match member.review() {
        EvaluationReviewState::NotApplicable => {
            hash.update(&[0]);
        }
        EvaluationReviewState::Pending {
            submitted_revision,
            receipt_sequence,
        } => {
            hash.update(&[1]);
            hash.update(&submitted_revision.get().to_be_bytes());
            hash.update(&receipt_sequence.get().to_be_bytes());
        }
        EvaluationReviewState::Reviewed(receipt) => {
            hash.update(&[2]);
            hash.update(receipt.assignment_id().into_uuid().as_bytes());
            hash.update(&receipt.submitted_revision().get().to_be_bytes());
            hash.update(&receipt.review_revision().get().to_be_bytes());
            hash.update(&receipt.source_sequence().get().to_be_bytes());
            hash.update(&[match receipt.outcome() {
                EvaluationReviewOutcome::Accepted => 0,
                EvaluationReviewOutcome::Rejected => 1,
            }]);
            hash.update(receipt.author_agent_id().into_uuid().as_bytes());
            hash.update(receipt.author_session_id().into_uuid().as_bytes());
            hash.update(receipt.reviewer_agent_id().into_uuid().as_bytes());
            hash.update(receipt.reviewer_session_id().into_uuid().as_bytes());
            hash.update(receipt.result_digest().as_bytes());
        }
        EvaluationReviewState::Unknown(reason) => {
            hash.update(&[3, encode_unknown_reason(reason)]);
        }
    }
}

fn write_optional_uuid(hash: &mut Hasher, value: Option<Uuid>) {
    if let Some(value) = value {
        hash.update(&[1]);
        hash.update(value.as_bytes());
    } else {
        hash.update(&[0]);
    }
}

fn write_optional_digest(hash: &mut Hasher, value: Option<forge_domain::EvaluationDigest>) {
    if let Some(value) = value {
        hash.update(&[1]);
        hash.update(value.as_bytes());
    } else {
        hash.update(&[0]);
    }
}

fn write_bytes(hash: &mut Hasher, value: &[u8]) -> Result<(), OperationsError> {
    let length = u64::try_from(value.len())
        .map_err(|_| OperationsError::Bounds("mission evidence fingerprint length"))?;
    hash.update(&length.to_be_bytes());
    hash.update(value);
    Ok(())
}

fn write_count(hash: &mut Hasher, count: usize) -> Result<(), OperationsError> {
    let count = u64::try_from(count)
        .map_err(|_| OperationsError::Bounds("mission evidence fingerprint count"))?;
    hash.update(&count.to_be_bytes());
    Ok(())
}

const fn encode_clock_uncertainty(value: EvaluationClockUncertainty) -> u8 {
    match value {
        EvaluationClockUncertainty::Unknown => 0,
        EvaluationClockUncertainty::Regressed => 1,
    }
}

const fn encode_unknown_reason(value: EvaluationCoverageUnknownReason) -> u8 {
    match value {
        EvaluationCoverageUnknownReason::OriginalBindingMissing => 0,
        EvaluationCoverageUnknownReason::LegacySubmissionWithoutReceipt => 1,
        EvaluationCoverageUnknownReason::SubmissionTimingUnknown => 2,
        EvaluationCoverageUnknownReason::ClockHistoryUncertain => 3,
        EvaluationCoverageUnknownReason::ReceiptBindingMismatch => 4,
    }
}
