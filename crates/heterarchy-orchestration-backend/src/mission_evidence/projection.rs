use heterarchy_orchestration_operations::mission_evidence::{
    MissionEvidenceActorKind, MissionEvidenceActorReceipt, MissionEvidenceBindingState,
    MissionEvidenceDisposition, MissionEvidenceIdentityPendingReason, MissionEvidenceIdentityProof,
    MissionEvidenceIdentityResolution, MissionEvidenceManifestBindingRecord,
    MissionEvidenceObservationMember, MissionEvidenceObservationRecord,
    MissionEvidenceRegistrationKind, MissionEvidenceUnresolvedReason,
};
use uuid::Uuid;

use crate::wire::mission_evidence::{
    MissionEvidenceActorKindWire, MissionEvidenceActorReceiptWire, MissionEvidenceBindingStateWire,
    MissionEvidenceCutPairWire, MissionEvidenceDispositionWire,
    MissionEvidenceIdentityPendingReasonWire, MissionEvidenceIdentityProofWire,
    MissionEvidenceIdentityResolutionWire, MissionEvidenceManifestBindingWire,
    MissionEvidenceObservationMemberWire, MissionEvidenceObservationWire,
    MissionEvidenceParticipationMeasureWire, MissionEvidenceRegistrationKindWire,
    MissionEvidenceUnresolvedReasonWire,
};

use super::source::{registered_manifest_wire, review_cut_wire, t1_cut_wire};

pub(super) fn binding_wire(
    record: &MissionEvidenceManifestBindingRecord,
) -> MissionEvidenceManifestBindingWire {
    MissionEvidenceManifestBindingWire {
        request_id: record.request_id(),
        workspace_id: record.workspace_id().into_uuid(),
        mission_id: record.mission_id().into_uuid(),
        plan_id: record.plan_id().into_uuid(),
        plan_version_id: record.plan_version_id().into_uuid(),
        plan_revision: record.plan_revision(),
        plan_start_request_id: record.plan_start_request_id(),
        plan_started_at_ms: record.plan_started_at_ms(),
        t0_commit_at_ms: record.t0_commit_at_ms(),
        t1_submission_cutoff_ms: record.t1_submission_cutoff_ms(),
        t2_checkpoint_ms: record.t2_checkpoint_ms(),
        local_plan_digest: digest_hex(record.local_plan_digest()),
        local_criteria_digest: digest_hex(record.local_criteria_digest()),
        state: match record.state() {
            MissionEvidenceBindingState::Verified => MissionEvidenceBindingStateWire::Verified,
            MissionEvidenceBindingState::Pending => MissionEvidenceBindingStateWire::Pending,
        },
        manifests: record
            .manifest_bundle()
            .manifests()
            .iter()
            .map(registered_manifest_wire)
            .collect(),
        actor: actor_wire(record.actor()),
        recorded_at_ms: record.recorded_at_ms(),
    }
}

pub(super) fn observation_wire(
    record: &MissionEvidenceObservationRecord,
    mission_id: Uuid,
) -> MissionEvidenceObservationWire {
    MissionEvidenceObservationWire {
        request_id: record.request_id(),
        binding_request_id: record.binding_request_id(),
        workspace_id: record.workspace_id().into_uuid(),
        mission_id,
        plan_id: record.plan_id().into_uuid(),
        plan_revision: record.plan_revision(),
        revision: record.revision(),
        expected_head_revision: record.expected_head_revision(),
        cuts: record
            .cuts()
            .partitions()
            .iter()
            .map(|partition| MissionEvidenceCutPairWire {
                activity_id: partition.activity_id().into_uuid(),
                t1_cut: t1_cut_wire(partition.t1_cut()),
                review_cut: review_cut_wire(partition.review_cut()),
            })
            .collect(),
        members: record
            .members()
            .iter()
            .map(observation_member_wire)
            .collect(),
        measures: record
            .measures()
            .iter()
            .map(|measure| MissionEvidenceParticipationMeasureWire {
                participation_id: measure.participation_id().into_uuid(),
                planned_due_weight: measure.planned_due_weight(),
                accepted_original_due_weight: measure.accepted_original_due_weight(),
                unresolved_due_weight: measure.unresolved_due_weight(),
                future_weight: measure.future_weight(),
            })
            .collect(),
        actor: actor_wire(record.actor()),
        recorded_at_ms: record.recorded_at_ms(),
    }
}

fn actor_wire(actor: &MissionEvidenceActorReceipt) -> MissionEvidenceActorReceiptWire {
    MissionEvidenceActorReceiptWire {
        kind: match actor.kind() {
            MissionEvidenceActorKind::LocalOperator => MissionEvidenceActorKindWire::LocalOperator,
            MissionEvidenceActorKind::ProfileParticipant => {
                MissionEvidenceActorKindWire::ProfileParticipant
            }
            MissionEvidenceActorKind::NativeParticipant => {
                MissionEvidenceActorKindWire::NativeParticipant
            }
        },
        semantic_key: actor.semantic_key().to_owned(),
        participation_id: actor
            .participation_id()
            .map(heterarchy_orchestration_operations::WorkspaceParticipationId::into_uuid),
        profile_id: actor.profile_id(),
        participant_id: actor.participant_id(),
        session_id: actor
            .session_id()
            .map(forge_domain::AgentSessionId::into_uuid),
        display_name: actor.display_name().to_owned(),
        leadership_revision: actor.leadership_revision(),
    }
}

fn observation_member_wire(
    member: &MissionEvidenceObservationMember,
) -> MissionEvidenceObservationMemberWire {
    MissionEvidenceObservationMemberWire {
        partition_ordinal: member.partition_ordinal(),
        source_activity_id: member.source_activity_id().into_uuid(),
        source: super::source::review_member_wire(member.source()),
        participation_id: member.participation_id().into_uuid(),
        weight: member.weight(),
        due_at_ms: member.due_at_ms(),
        target_identity: identity_resolution_wire(member.target_identity()),
        author_identity: member.author_identity().map(identity_resolution_wire),
        reviewer_identity: member.reviewer_identity().map(identity_resolution_wire),
        disposition: match member.disposition() {
            MissionEvidenceDisposition::AcceptedOriginal => {
                MissionEvidenceDispositionWire::AcceptedOriginal
            }
            MissionEvidenceDisposition::Unresolved(reason) => {
                MissionEvidenceDispositionWire::Unresolved(unresolved_reason_wire(reason))
            }
        },
    }
}

fn identity_resolution_wire(
    identity: &MissionEvidenceIdentityResolution,
) -> MissionEvidenceIdentityResolutionWire {
    match identity {
        MissionEvidenceIdentityResolution::Verified(proof) => {
            MissionEvidenceIdentityResolutionWire::Verified(Box::new(identity_proof_wire(
                proof.as_ref(),
            )))
        }
        MissionEvidenceIdentityResolution::Pending {
            agent_id,
            session_id,
            reason,
            registration_request_id,
            activation_receipt_id,
        } => MissionEvidenceIdentityResolutionWire::Pending {
            agent_id: agent_id.into_uuid(),
            session_id: session_id.into_uuid(),
            reason: identity_pending_reason_wire(*reason),
            registration_request_id: *registration_request_id,
            activation_receipt_id: *activation_receipt_id,
        },
    }
}

fn identity_proof_wire(proof: &MissionEvidenceIdentityProof) -> MissionEvidenceIdentityProofWire {
    MissionEvidenceIdentityProofWire {
        actor_agent_id: proof.actor_agent_id().into_uuid(),
        actor_session_id: proof.actor_session_id().into_uuid(),
        participant_id: proof.participant_id(),
        profile_id: proof.profile_id(),
        participation_id: proof
            .participation_id()
            .map(heterarchy_orchestration_operations::WorkspaceParticipationId::into_uuid),
        registration_kind: match proof.registration_kind() {
            MissionEvidenceRegistrationKind::Original => {
                MissionEvidenceRegistrationKindWire::Original
            }
            MissionEvidenceRegistrationKind::Replacement => {
                MissionEvidenceRegistrationKindWire::Replacement
            }
        },
        registration_request_id: proof.registration_request_id(),
        migration_id: proof.migration_id(),
        destination_id: proof.destination_id(),
        freeze_revision: proof.freeze_revision(),
        membership_commit_digest: digest_bytes_hex(proof.membership_commit_digest()),
        activation_receipt_id: proof.activation_receipt_id(),
        activated_at_ms: proof.activated_at_ms(),
        profile_mapping_request_id: proof.profile_mapping_request_id(),
        profile_revision: proof.profile_revision(),
        session_revision: proof.session_revision(),
        participation_mapping_request_id: proof.participation_mapping_request_id(),
        participant_membership_revision: proof.participant_membership_revision(),
        participation_mapped_at_ms: proof.participation_mapped_at_ms(),
        rebind_request_id: proof.rebind_request_id(),
        rebind_revision: proof.rebind_revision(),
    }
}

fn identity_pending_reason_wire(
    reason: MissionEvidenceIdentityPendingReason,
) -> MissionEvidenceIdentityPendingReasonWire {
    match reason {
        MissionEvidenceIdentityPendingReason::NoRegistrationHistory => {
            MissionEvidenceIdentityPendingReasonWire::NoRegistrationHistory
        }
        MissionEvidenceIdentityPendingReason::RegistrationNotCommitted => {
            MissionEvidenceIdentityPendingReasonWire::RegistrationNotCommitted
        }
        MissionEvidenceIdentityPendingReason::ActivationMissing => {
            MissionEvidenceIdentityPendingReasonWire::ActivationMissing
        }
        MissionEvidenceIdentityPendingReason::ProfileLinkMissing => {
            MissionEvidenceIdentityPendingReasonWire::ProfileLinkMissing
        }
        MissionEvidenceIdentityPendingReason::ParticipationMappingMissing => {
            MissionEvidenceIdentityPendingReasonWire::ParticipationMappingMissing
        }
        MissionEvidenceIdentityPendingReason::ReplacementRebindMissing => {
            MissionEvidenceIdentityPendingReasonWire::ReplacementRebindMissing
        }
        MissionEvidenceIdentityPendingReason::AmbiguousHistory => {
            MissionEvidenceIdentityPendingReasonWire::AmbiguousHistory
        }
    }
}

fn unresolved_reason_wire(
    reason: MissionEvidenceUnresolvedReason,
) -> MissionEvidenceUnresolvedReasonWire {
    match reason {
        MissionEvidenceUnresolvedReason::BindingPending => {
            MissionEvidenceUnresolvedReasonWire::BindingPending
        }
        MissionEvidenceUnresolvedReason::TargetIdentityPending => {
            MissionEvidenceUnresolvedReasonWire::TargetIdentityPending
        }
        MissionEvidenceUnresolvedReason::NoEligibleOriginal => {
            MissionEvidenceUnresolvedReasonWire::NoEligibleOriginal
        }
        MissionEvidenceUnresolvedReason::CoverageUnknown => {
            MissionEvidenceUnresolvedReasonWire::CoverageUnknown
        }
        MissionEvidenceUnresolvedReason::AdmissionTimingUnknown => {
            MissionEvidenceUnresolvedReasonWire::AdmissionTimingUnknown
        }
        MissionEvidenceUnresolvedReason::ReviewNotAccepted => {
            MissionEvidenceUnresolvedReasonWire::ReviewNotAccepted
        }
        MissionEvidenceUnresolvedReason::ReviewPending => {
            MissionEvidenceUnresolvedReasonWire::ReviewPending
        }
        MissionEvidenceUnresolvedReason::ReviewUnknown => {
            MissionEvidenceUnresolvedReasonWire::ReviewUnknown
        }
        MissionEvidenceUnresolvedReason::AuthorIdentityPending => {
            MissionEvidenceUnresolvedReasonWire::AuthorIdentityPending
        }
        MissionEvidenceUnresolvedReason::ReviewerIdentityPending => {
            MissionEvidenceUnresolvedReasonWire::ReviewerIdentityPending
        }
        MissionEvidenceUnresolvedReason::ReviewerAliasesAuthor => {
            MissionEvidenceUnresolvedReasonWire::ReviewerAliasesAuthor
        }
    }
}

fn digest_hex(digest: forge_domain::EvaluationDigest) -> String {
    let mut value = String::with_capacity(64);
    for byte in digest.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn digest_bytes_hex(digest: [u8; 32]) -> String {
    let mut value = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}
