//! Strict HTTP requests and readbacks for configured Forge Mission evidence.

use forge_backend::ops::evaluation_evidence::wire::{
    EvaluationRegisteredManifestWire, EvaluationReviewCutMemberWire, EvaluationReviewCutWire,
    EvaluationT1CoverageCutWire,
};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::{WorkspaceQueryScope, mission_evaluation::MissionEvaluationMutationContextWire};

/// Maximum JSON body size accepted by the Mission evidence route family.
pub const MAX_MISSION_EVIDENCE_BODY_BYTES: usize = 128 * 1024;

/// Maximum serialized exact source-cut/provenance response size.
pub const MAX_MISSION_EVIDENCE_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// One caller-selected original assignment mapping. Source facts are never
/// accepted in this request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceCandidateMemberWire {
    pub obligation_id: Uuid,
    pub assignment_id: Uuid,
    pub assignment_revision: i64,
    pub target_agent_id: Uuid,
    pub target_session_id: Uuid,
}

/// Complete caller-selected candidate set for one source Activity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceCandidatePartitionWire {
    pub activity_id: Uuid,
    pub members: Vec<MissionEvidenceCandidateMemberWire>,
}

/// Read canonical local plan and criteria digests before source registration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidencePlanDigestsRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub revision: u64,
}

/// Canonical digest readback derived only from a frozen local plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidencePlanDigestsResponse {
    pub scope: WorkspaceQueryScope,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_version_id: Uuid,
    pub revision: u64,
    pub t1_submission_cutoff_ms: i64,
    pub plan_digest: String,
    pub criteria_digest: String,
    pub member_criteria_digests: Vec<MissionEvidenceMemberCriteriaDigestWire>,
}

/// One exact local obligation criteria digest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceMemberCriteriaDigestWire {
    pub obligation_id: Uuid,
    pub work_unit_id: Uuid,
    pub participation_id: Uuid,
    pub digest: String,
}

/// Read-only source candidate preflight; no Forge source mutation occurs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceManifestPrepareRequest {
    pub context: MissionEvaluationMutationContextWire,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
    pub partitions: Vec<MissionEvidenceCandidatePartitionWire>,
}

/// Durable request to register and bind every Activity partition for a plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceManifestBindRequest {
    pub context: MissionEvaluationMutationContextWire,
    pub request_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
    pub partitions: Vec<MissionEvidenceCandidatePartitionWire>,
}

/// Append exact Forge T1 and review cuts for one frozen plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceObservationRequest {
    pub context: MissionEvaluationMutationContextWire,
    pub request_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
    pub expected_head_revision: u64,
}

/// Original mutation intent retained for explicit actor-bound recovery.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    rename_all = "snake_case",
    tag = "kind",
    content = "request",
    deny_unknown_fields
)]
pub enum MissionEvidenceMutationRequest {
    Bind(MissionEvidenceManifestBindRequest),
    Observe(MissionEvidenceObservationRequest),
}

impl MissionEvidenceMutationRequest {
    #[must_use]
    pub fn scope(&self) -> &WorkspaceQueryScope {
        match self {
            Self::Bind(request) => &request.context.scope,
            Self::Observe(request) => &request.context.scope,
        }
    }

    #[must_use]
    pub const fn session_id(&self) -> Uuid {
        match self {
            Self::Bind(request) => request.context.session_id,
            Self::Observe(request) => request.context.session_id,
        }
    }

    #[must_use]
    pub const fn sender_activity_id(&self) -> Uuid {
        match self {
            Self::Bind(request) => request.context.sender_activity_id,
            Self::Observe(request) => request.context.sender_activity_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        match self {
            Self::Bind(request) => request.request_id,
            Self::Observe(request) => request.request_id,
        }
    }
}

/// Exact binding read selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceBindingReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
}

/// Exact observation read selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceObservationReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
    pub request_id: Uuid,
}

/// Exact semantic actor selector for a durable evidence mutation receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceRequestReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub mutation: MissionEvidenceMutationRequest,
}

/// Current authenticated participant selector for exact mutation readback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvidenceRequestReadActor {
    pub sender_activity_id: Uuid,
    pub session_id: Uuid,
}

/// One unresolved original target returned by native local history preflight.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidencePendingTargetWire {
    pub obligation_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub reason: MissionEvidenceIdentityPendingReasonWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub registration_request_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub activation_receipt_id: Option<Uuid>,
}

/// Closed set of historical identity holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidenceIdentityPendingReasonWire {
    NoRegistrationHistory,
    RegistrationNotCommitted,
    ActivationMissing,
    ProfileLinkMissing,
    ParticipationMappingMissing,
    ReplacementRebindMissing,
    AmbiguousHistory,
}

/// State of read-only candidate preparation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidencePreparationStatusWire {
    Ready,
    Pending,
}

/// Result state of a source and native mutation workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidenceMutationStatusWire {
    Complete,
    Pending,
}

/// Checked proposals and exact unresolved local target history.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceManifestPrepareResponse {
    pub scope: WorkspaceQueryScope,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
    pub status: MissionEvidencePreparationStatusWire,
    pub proposals:
        Vec<forge_backend::ops::evaluation_evidence::wire::EvaluationManifestProposalWire>,
    pub pending_targets: Vec<MissionEvidencePendingTargetWire>,
}

/// Verified or unresolved state of the immutable native source binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidenceBindingStateWire {
    Verified,
    Pending,
}

/// Readback of the complete source registration bundle bound to a local plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceManifestBindingWire {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_version_id: Uuid,
    pub plan_revision: u64,
    pub plan_start_request_id: Uuid,
    pub plan_started_at_ms: i64,
    pub t0_commit_at_ms: i64,
    pub t1_submission_cutoff_ms: i64,
    pub t2_checkpoint_ms: i64,
    pub local_plan_digest: String,
    pub local_criteria_digest: String,
    pub state: MissionEvidenceBindingStateWire,
    pub manifests: Vec<EvaluationRegisteredManifestWire>,
    pub actor: MissionEvidenceActorReceiptWire,
    pub recorded_at_ms: i64,
}

/// Runtime-derived semantic actor attribution on a native mutation receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceActorReceiptWire {
    pub kind: MissionEvidenceActorKindWire,
    pub semantic_key: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participant_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
    pub display_name: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub leadership_revision: Option<i64>,
}

/// Closed semantic actor category stored by Operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidenceActorKindWire {
    LocalOperator,
    ProfileParticipant,
    NativeParticipant,
}

/// Source workflow stage for an incomplete cross-owner operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidenceSourceStageWire {
    Manifest,
    T1Cut,
    ReviewCut,
}

/// Why the source workflow remains incomplete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidencePendingReasonWire {
    TargetHistoryUnresolved,
    SourceUnavailable,
    MutationOutcomeUnknown,
    ReadbackUnavailable,
    SourceRecordMissing,
    T1NotClosed,
}

/// Exact source identities needed to continue readback of a partial workflow.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidencePendingSourceWire {
    pub activity_id: Uuid,
    pub stage: MissionEvidenceSourceStageWire,
    pub reason: MissionEvidencePendingReasonWire,
    pub manifest_id: Uuid,
    pub manifest_revision: i64,
    pub manifest_digest: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cut_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub review_cut_id: Option<Uuid>,
}

/// Cross-owner result after source staging and optional local binding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceManifestBindResponse {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub status: MissionEvidenceMutationStatusWire,
    pub replayed: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub binding: Option<MissionEvidenceManifestBindingWire>,
    pub pending_targets: Vec<MissionEvidencePendingTargetWire>,
    pub pending_sources: Vec<MissionEvidencePendingSourceWire>,
}

/// One exact Activity cut pair included by a native observation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceCutPairWire {
    pub activity_id: Uuid,
    pub t1_cut: EvaluationT1CoverageCutWire,
    pub review_cut: EvaluationReviewCutWire,
}

/// Immutable as-observed source cut vector and local evidence measures.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceObservationWire {
    pub request_id: Uuid,
    pub binding_request_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
    pub revision: u64,
    pub expected_head_revision: u64,
    pub cuts: Vec<MissionEvidenceCutPairWire>,
    pub members: Vec<MissionEvidenceObservationMemberWire>,
    pub measures: Vec<MissionEvidenceParticipationMeasureWire>,
    pub actor: MissionEvidenceActorReceiptWire,
    pub recorded_at_ms: i64,
}

/// One source member plus the immutable local identity resolutions used when
/// this observation was assembled.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceObservationMemberWire {
    pub partition_ordinal: u32,
    pub source_activity_id: Uuid,
    pub source: EvaluationReviewCutMemberWire,
    pub participation_id: Uuid,
    pub weight: u64,
    pub due_at_ms: i64,
    pub target_identity: MissionEvidenceIdentityResolutionWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub author_identity: Option<MissionEvidenceIdentityResolutionWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub reviewer_identity: Option<MissionEvidenceIdentityResolutionWire>,
    pub disposition: MissionEvidenceDispositionWire,
}

/// Exact historical profile, registration, and activation provenance.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceIdentityProofWire {
    pub actor_agent_id: Uuid,
    pub actor_session_id: Uuid,
    pub participant_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_id: Option<Uuid>,
    pub registration_kind: MissionEvidenceRegistrationKindWire,
    pub registration_request_id: Uuid,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub freeze_revision: u64,
    pub membership_commit_digest: String,
    pub activation_receipt_id: Uuid,
    pub activated_at_ms: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_mapping_request_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_mapping_request_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participant_membership_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_mapped_at_ms: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub rebind_request_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub rebind_revision: Option<u64>,
}

/// Verified identity or explicit unresolved historical provenance.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    rename_all = "snake_case",
    tag = "kind",
    content = "record",
    deny_unknown_fields
)]
pub enum MissionEvidenceIdentityResolutionWire {
    Verified(Box<MissionEvidenceIdentityProofWire>),
    Pending {
        agent_id: Uuid,
        session_id: Uuid,
        reason: MissionEvidenceIdentityPendingReasonWire,
        #[serde(deserialize_with = "deserialize_required_option")]
        registration_request_id: Option<Uuid>,
        #[serde(deserialize_with = "deserialize_required_option")]
        activation_receipt_id: Option<Uuid>,
    },
}

/// Immutable registration lifecycle used to establish a historical Agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidenceRegistrationKindWire {
    Original,
    Replacement,
}

/// Bounded evidence disposition; it is not formal performance adjudication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "reason")]
pub enum MissionEvidenceDispositionWire {
    AcceptedOriginal,
    Unresolved(MissionEvidenceUnresolvedReasonWire),
}

/// Exact known reason a member remains unresolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEvidenceUnresolvedReasonWire {
    BindingPending,
    TargetIdentityPending,
    NoEligibleOriginal,
    CoverageUnknown,
    AdmissionTimingUnknown,
    ReviewNotAccepted,
    ReviewPending,
    ReviewUnknown,
    AuthorIdentityPending,
    ReviewerIdentityPending,
    ReviewerAliasesAuthor,
}

/// Bounded evidence-only weights, not an official `WorkCredit` or election score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceParticipationMeasureWire {
    pub participation_id: Uuid,
    pub planned_due_weight: u64,
    pub accepted_original_due_weight: u64,
    pub unresolved_due_weight: u64,
    pub future_weight: u64,
}

/// Cross-owner result after observing and optionally committing all Activity cuts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceObservationResponse {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub status: MissionEvidenceMutationStatusWire,
    pub replayed: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub observation: Option<MissionEvidenceObservationWire>,
    pub pending_targets: Vec<MissionEvidencePendingTargetWire>,
    pub pending_sources: Vec<MissionEvidencePendingSourceWire>,
}

/// Exact mutation record returned after actor-bound native receipt readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "record")]
pub enum MissionEvidenceMutationWire {
    ManifestBinding(MissionEvidenceManifestBindingWire),
    Observation(MissionEvidenceObservationWire),
}

/// Exact binding read response; `binding` is always present as a key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceBindingReadResponse {
    pub scope: WorkspaceQueryScope,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub binding: Option<MissionEvidenceManifestBindingWire>,
}

/// Exact observation read response; `observation` is always present as a key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceObservationReadResponse {
    pub scope: WorkspaceQueryScope,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub plan_revision: u64,
    pub request_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub observation: Option<MissionEvidenceObservationWire>,
}

/// Actor-bound durable request receipt readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvidenceRequestReadResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub request_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub mutation: Option<MissionEvidenceMutationWire>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn candidate_wire_rejects_unknown_source_authority_fields() -> Result<(), Box<dyn Error>> {
        let candidate = MissionEvidenceCandidateMemberWire {
            obligation_id: Uuid::from_u128(1),
            assignment_id: Uuid::from_u128(2),
            assignment_revision: 1,
            target_agent_id: Uuid::from_u128(3),
            target_session_id: Uuid::from_u128(4),
        };
        let mut value = serde_json::to_value(candidate)?;
        value
            .as_object_mut()
            .ok_or("candidate wire must serialize as an object")?
            .insert("trustedAdmittedAtMs".to_owned(), serde_json::json!(1));
        assert!(serde_json::from_value::<MissionEvidenceCandidateMemberWire>(value).is_err());
        Ok(())
    }

    #[test]
    fn recovery_intent_serializes_original_and_current_actor_separately()
    -> Result<(), Box<dyn Error>> {
        let scope = WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(10),
            location_id: Uuid::from_u128(11),
            location_revision: 2,
            workspace_revision: 3,
            locator: "workspace".to_owned(),
        };
        let intent = MissionEvidenceMutationRequest::Observe(MissionEvidenceObservationRequest {
            context: MissionEvaluationMutationContextWire {
                scope: scope.clone(),
                session_id: Uuid::from_u128(12),
                sender_activity_id: Uuid::from_u128(13),
                expected_workspace_leadership_revision: 4,
            },
            request_id: Uuid::from_u128(14),
            mission_id: Uuid::from_u128(15),
            plan_id: Uuid::from_u128(16),
            plan_revision: 1,
            expected_head_revision: 0,
        });
        let request = MissionEvidenceRequestReadRequest {
            scope,
            session_id: Uuid::from_u128(17),
            sender_activity_id: Uuid::from_u128(18),
            mutation: intent,
        };
        let value = serde_json::to_value(&request)?;
        assert_eq!(value["sessionId"], Uuid::from_u128(17).to_string());
        assert_eq!(
            value["mutation"]["request"]["context"]["sessionId"],
            Uuid::from_u128(12).to_string()
        );
        assert_eq!(
            value["mutation"]["request"]["requestId"],
            Uuid::from_u128(14).to_string()
        );
        assert_eq!(
            serde_json::from_value::<MissionEvidenceRequestReadRequest>(value)?,
            request
        );
        Ok(())
    }
}
