use forge_domain::{
    EvaluationAdmissionTiming, EvaluationClockUncertainty, EvaluationCoverageUnknownReason,
    EvaluationOriginalAssignmentBinding, EvaluationOriginalBindingStatus, EvaluationReviewOutcome,
    EvaluationReviewState, EvaluationSourceSequence, EvaluationT1Coverage,
};

use super::{
    MissionEvidenceIdentityPendingReason, MissionEvidenceRegistrationKind,
    MissionEvidenceUnresolvedReason,
};
use crate::OperationsError;

pub(super) fn encode_original_binding(
    value: EvaluationOriginalBindingStatus,
) -> (i64, Option<[u8; 32]>, Option<i64>, Option<i64>) {
    match value {
        EvaluationOriginalBindingStatus::Bound(binding) => (
            0,
            Some(*binding.digest().as_bytes()),
            Some(binding.assignment_revision().get()),
            Some(binding.source_sequence().get()),
        ),
        EvaluationOriginalBindingStatus::UnknownHistorical => (1, None, None, None),
    }
}

pub(super) fn decode_original_binding(
    state: i64,
    digest: Option<Vec<u8>>,
    assignment_revision: Option<i64>,
    source_sequence: Option<i64>,
) -> Result<EvaluationOriginalBindingStatus, OperationsError> {
    match (state, digest, assignment_revision, source_sequence) {
        (0, Some(digest), Some(revision), Some(sequence)) => {
            let digest = decode_digest(digest, "mission evidence original binding digest")?;
            let revision = forge_domain::AgentAssignmentRevision::new(revision)
                .map_err(|_| OperationsError::Corrupt("mission evidence original revision"))?;
            let sequence = EvaluationSourceSequence::new(sequence)
                .map_err(|_| OperationsError::Corrupt("mission evidence original sequence"))?;
            Ok(EvaluationOriginalBindingStatus::Bound(
                EvaluationOriginalAssignmentBinding::new(digest, revision, sequence),
            ))
        }
        (1, None, None, None) => Ok(EvaluationOriginalBindingStatus::UnknownHistorical),
        _ => Err(OperationsError::Corrupt(
            "mission evidence original binding state",
        )),
    }
}

pub(super) fn encode_timing(value: EvaluationAdmissionTiming) -> (i64, Option<i64>, Option<i64>) {
    match value {
        EvaluationAdmissionTiming::Trusted(at) => (0, Some(at.get()), None),
        EvaluationAdmissionTiming::Unknown(reason) => {
            (1, None, Some(encode_clock_uncertainty(reason)))
        }
    }
}

pub(super) fn decode_timing(
    kind: i64,
    at_ms: Option<i64>,
    uncertainty: Option<i64>,
) -> Result<EvaluationAdmissionTiming, OperationsError> {
    match (kind, at_ms, uncertainty) {
        (0, Some(at), None) if at >= 0 => Ok(EvaluationAdmissionTiming::Trusted(
            forge_domain::TimestampMillis::new(at)
                .map_err(|_| OperationsError::Corrupt("mission evidence source time"))?,
        )),
        (1, None, Some(reason)) => Ok(EvaluationAdmissionTiming::Unknown(
            decode_clock_uncertainty(reason)?,
        )),
        _ => Err(OperationsError::Corrupt("mission evidence timing")),
    }
}

pub(super) fn encode_coverage(value: EvaluationT1Coverage) -> CoverageParts {
    match value {
        EvaluationT1Coverage::Eligible {
            submitted_revision,
            receipt_sequence,
            admitted_at,
            result_digest,
            sources_digest,
        } => CoverageParts {
            state: 0,
            unknown_reason: None,
            submitted_revision: Some(submitted_revision.get()),
            receipt_sequence: Some(receipt_sequence.get()),
            admitted_at_ms: Some(admitted_at.get()),
            result_digest: Some(*result_digest.as_bytes()),
            sources_digest: sources_digest.map(|value| *value.as_bytes()),
        },
        EvaluationT1Coverage::NoEligibleSubmission => CoverageParts {
            state: 1,
            unknown_reason: None,
            submitted_revision: None,
            receipt_sequence: None,
            admitted_at_ms: None,
            result_digest: None,
            sources_digest: None,
        },
        EvaluationT1Coverage::Unknown { reason } => CoverageParts {
            state: 2,
            unknown_reason: Some(encode_coverage_unknown(reason)),
            submitted_revision: None,
            receipt_sequence: None,
            admitted_at_ms: None,
            result_digest: None,
            sources_digest: None,
        },
    }
}

pub(super) fn decode_coverage(
    parts: CoverageParts,
) -> Result<EvaluationT1Coverage, OperationsError> {
    match (
        parts.state,
        parts.unknown_reason,
        parts.submitted_revision,
        parts.receipt_sequence,
        parts.admitted_at_ms,
        parts.result_digest,
        parts.sources_digest,
    ) {
        (0, None, Some(revision), Some(sequence), Some(at), Some(result), sources) => {
            if at < 0 {
                return Err(OperationsError::Corrupt("mission evidence admitted time"));
            }
            Ok(EvaluationT1Coverage::Eligible {
                submitted_revision: forge_domain::AgentAssignmentRevision::new(revision)
                    .map_err(|_| OperationsError::Corrupt("mission evidence submitted revision"))?,
                receipt_sequence: EvaluationSourceSequence::new(sequence)
                    .map_err(|_| OperationsError::Corrupt("mission evidence receipt sequence"))?,
                admitted_at: forge_domain::TimestampMillis::new(at)
                    .map_err(|_| OperationsError::Corrupt("mission evidence admitted time"))?,
                result_digest: decode_digest(result.to_vec(), "mission evidence result digest")?,
                sources_digest: sources
                    .map(|value| decode_digest(value.to_vec(), "mission evidence sources digest"))
                    .transpose()?,
            })
        }
        (1, None, None, None, None, None, None) => Ok(EvaluationT1Coverage::NoEligibleSubmission),
        (2, Some(reason), None, None, None, None, None) => Ok(EvaluationT1Coverage::Unknown {
            reason: decode_coverage_unknown(reason)?,
        }),
        _ => Err(OperationsError::Corrupt("mission evidence coverage state")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CoverageParts {
    pub(super) state: i64,
    pub(super) unknown_reason: Option<i64>,
    pub(super) submitted_revision: Option<i64>,
    pub(super) receipt_sequence: Option<i64>,
    pub(super) admitted_at_ms: Option<i64>,
    pub(super) result_digest: Option<[u8; 32]>,
    pub(super) sources_digest: Option<[u8; 32]>,
}

pub(super) fn encode_review_state(value: EvaluationReviewState) -> ReviewStateParts {
    match value {
        EvaluationReviewState::NotApplicable => ReviewStateParts::not_applicable(),
        EvaluationReviewState::Pending {
            submitted_revision,
            receipt_sequence,
        } => ReviewStateParts {
            state: 1,
            unknown_reason: None,
            pending_submitted_revision: Some(submitted_revision.get()),
            pending_receipt_sequence: Some(receipt_sequence.get()),
        },
        EvaluationReviewState::Reviewed(_) => ReviewStateParts {
            state: 2,
            unknown_reason: None,
            pending_submitted_revision: None,
            pending_receipt_sequence: None,
        },
        EvaluationReviewState::Unknown(reason) => ReviewStateParts {
            state: 3,
            unknown_reason: Some(encode_coverage_unknown(reason)),
            pending_submitted_revision: None,
            pending_receipt_sequence: None,
        },
    }
}

pub(super) fn decode_review_state(
    parts: ReviewStateParts,
    reviewed: Option<forge_domain::EvaluationReviewReceipt>,
) -> Result<EvaluationReviewState, OperationsError> {
    match (
        parts.state,
        parts.unknown_reason,
        parts.pending_submitted_revision,
        parts.pending_receipt_sequence,
        reviewed,
    ) {
        (0, None, None, None, None) => Ok(EvaluationReviewState::NotApplicable),
        (1, None, Some(revision), Some(sequence), None) => Ok(EvaluationReviewState::Pending {
            submitted_revision: forge_domain::AgentAssignmentRevision::new(revision)
                .map_err(|_| OperationsError::Corrupt("mission evidence pending revision"))?,
            receipt_sequence: EvaluationSourceSequence::new(sequence)
                .map_err(|_| OperationsError::Corrupt("mission evidence pending sequence"))?,
        }),
        (2, None, None, None, Some(reviewed)) => Ok(EvaluationReviewState::Reviewed(reviewed)),
        (3, Some(reason), None, None, None) => Ok(EvaluationReviewState::Unknown(
            decode_coverage_unknown(reason)?,
        )),
        _ => Err(OperationsError::Corrupt("mission evidence review state")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReviewStateParts {
    pub(super) state: i64,
    pub(super) unknown_reason: Option<i64>,
    pub(super) pending_submitted_revision: Option<i64>,
    pub(super) pending_receipt_sequence: Option<i64>,
}

impl ReviewStateParts {
    const fn not_applicable() -> Self {
        Self {
            state: 0,
            unknown_reason: None,
            pending_submitted_revision: None,
            pending_receipt_sequence: None,
        }
    }
}

pub(super) const fn encode_review_outcome(value: EvaluationReviewOutcome) -> i64 {
    match value {
        EvaluationReviewOutcome::Accepted => 0,
        EvaluationReviewOutcome::Rejected => 1,
    }
}

pub(super) fn decode_review_outcome(
    value: i64,
) -> Result<EvaluationReviewOutcome, OperationsError> {
    match value {
        0 => Ok(EvaluationReviewOutcome::Accepted),
        1 => Ok(EvaluationReviewOutcome::Rejected),
        _ => Err(OperationsError::Corrupt("mission evidence review outcome")),
    }
}

pub(super) const fn encode_coverage_unknown(value: EvaluationCoverageUnknownReason) -> i64 {
    match value {
        EvaluationCoverageUnknownReason::OriginalBindingMissing => 0,
        EvaluationCoverageUnknownReason::LegacySubmissionWithoutReceipt => 1,
        EvaluationCoverageUnknownReason::SubmissionTimingUnknown => 2,
        EvaluationCoverageUnknownReason::ClockHistoryUncertain => 3,
        EvaluationCoverageUnknownReason::ReceiptBindingMismatch => 4,
    }
}

pub(super) fn decode_coverage_unknown(
    value: i64,
) -> Result<EvaluationCoverageUnknownReason, OperationsError> {
    match value {
        0 => Ok(EvaluationCoverageUnknownReason::OriginalBindingMissing),
        1 => Ok(EvaluationCoverageUnknownReason::LegacySubmissionWithoutReceipt),
        2 => Ok(EvaluationCoverageUnknownReason::SubmissionTimingUnknown),
        3 => Ok(EvaluationCoverageUnknownReason::ClockHistoryUncertain),
        4 => Ok(EvaluationCoverageUnknownReason::ReceiptBindingMismatch),
        _ => Err(OperationsError::Corrupt("mission evidence unknown reason")),
    }
}

pub(super) const fn encode_identity_pending_reason(
    value: MissionEvidenceIdentityPendingReason,
) -> i64 {
    match value {
        MissionEvidenceIdentityPendingReason::NoRegistrationHistory => 0,
        MissionEvidenceIdentityPendingReason::RegistrationNotCommitted => 1,
        MissionEvidenceIdentityPendingReason::ActivationMissing => 2,
        MissionEvidenceIdentityPendingReason::ProfileLinkMissing => 3,
        MissionEvidenceIdentityPendingReason::ParticipationMappingMissing => 4,
        MissionEvidenceIdentityPendingReason::ReplacementRebindMissing => 5,
        MissionEvidenceIdentityPendingReason::AmbiguousHistory => 6,
    }
}

pub(super) fn decode_identity_pending_reason(
    value: i64,
) -> Result<MissionEvidenceIdentityPendingReason, OperationsError> {
    match value {
        0 => Ok(MissionEvidenceIdentityPendingReason::NoRegistrationHistory),
        1 => Ok(MissionEvidenceIdentityPendingReason::RegistrationNotCommitted),
        2 => Ok(MissionEvidenceIdentityPendingReason::ActivationMissing),
        3 => Ok(MissionEvidenceIdentityPendingReason::ProfileLinkMissing),
        4 => Ok(MissionEvidenceIdentityPendingReason::ParticipationMappingMissing),
        5 => Ok(MissionEvidenceIdentityPendingReason::ReplacementRebindMissing),
        6 => Ok(MissionEvidenceIdentityPendingReason::AmbiguousHistory),
        _ => Err(OperationsError::Corrupt(
            "mission evidence identity pending reason",
        )),
    }
}

pub(super) const fn encode_registration_kind(value: MissionEvidenceRegistrationKind) -> i64 {
    match value {
        MissionEvidenceRegistrationKind::Original => 0,
        MissionEvidenceRegistrationKind::Replacement => 1,
    }
}

pub(super) fn decode_registration_kind(
    value: i64,
) -> Result<MissionEvidenceRegistrationKind, OperationsError> {
    match value {
        0 => Ok(MissionEvidenceRegistrationKind::Original),
        1 => Ok(MissionEvidenceRegistrationKind::Replacement),
        _ => Err(OperationsError::Corrupt(
            "mission evidence registration kind",
        )),
    }
}

pub(super) const fn encode_unresolved_reason(value: MissionEvidenceUnresolvedReason) -> i64 {
    match value {
        MissionEvidenceUnresolvedReason::BindingPending => 0,
        MissionEvidenceUnresolvedReason::TargetIdentityPending => 1,
        MissionEvidenceUnresolvedReason::NoEligibleOriginal => 2,
        MissionEvidenceUnresolvedReason::CoverageUnknown => 3,
        MissionEvidenceUnresolvedReason::AdmissionTimingUnknown => 4,
        MissionEvidenceUnresolvedReason::ReviewNotAccepted => 5,
        MissionEvidenceUnresolvedReason::ReviewPending => 6,
        MissionEvidenceUnresolvedReason::ReviewUnknown => 7,
        MissionEvidenceUnresolvedReason::AuthorIdentityPending => 8,
        MissionEvidenceUnresolvedReason::ReviewerIdentityPending => 9,
        MissionEvidenceUnresolvedReason::ReviewerAliasesAuthor => 10,
    }
}

pub(super) fn decode_unresolved_reason(
    value: i64,
) -> Result<MissionEvidenceUnresolvedReason, OperationsError> {
    match value {
        0 => Ok(MissionEvidenceUnresolvedReason::BindingPending),
        1 => Ok(MissionEvidenceUnresolvedReason::TargetIdentityPending),
        2 => Ok(MissionEvidenceUnresolvedReason::NoEligibleOriginal),
        3 => Ok(MissionEvidenceUnresolvedReason::CoverageUnknown),
        4 => Ok(MissionEvidenceUnresolvedReason::AdmissionTimingUnknown),
        5 => Ok(MissionEvidenceUnresolvedReason::ReviewNotAccepted),
        6 => Ok(MissionEvidenceUnresolvedReason::ReviewPending),
        7 => Ok(MissionEvidenceUnresolvedReason::ReviewUnknown),
        8 => Ok(MissionEvidenceUnresolvedReason::AuthorIdentityPending),
        9 => Ok(MissionEvidenceUnresolvedReason::ReviewerIdentityPending),
        10 => Ok(MissionEvidenceUnresolvedReason::ReviewerAliasesAuthor),
        _ => Err(OperationsError::Corrupt(
            "mission evidence unresolved reason",
        )),
    }
}

fn encode_clock_uncertainty(value: EvaluationClockUncertainty) -> i64 {
    match value {
        EvaluationClockUncertainty::Unknown => 0,
        EvaluationClockUncertainty::Regressed => 1,
    }
}

fn decode_clock_uncertainty(value: i64) -> Result<EvaluationClockUncertainty, OperationsError> {
    match value {
        0 => Ok(EvaluationClockUncertainty::Unknown),
        1 => Ok(EvaluationClockUncertainty::Regressed),
        _ => Err(OperationsError::Corrupt(
            "mission evidence clock uncertainty",
        )),
    }
}

fn decode_digest(
    bytes: Vec<u8>,
    label: &'static str,
) -> Result<forge_domain::EvaluationDigest, OperationsError> {
    let digest: [u8; 32] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    forge_domain::EvaluationDigest::new(digest).map_err(|_| OperationsError::Corrupt(label))
}
