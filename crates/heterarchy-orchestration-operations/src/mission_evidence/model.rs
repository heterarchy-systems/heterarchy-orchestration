use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, EvaluationManifestMember,
    EvaluationOriginalBindingStatus, EvaluationRegisteredManifest, EvaluationReviewCut,
    EvaluationReviewCutMember, EvaluationReviewState, EvaluationT1CoverageCut, WorkspaceId,
};
use uuid::Uuid;

use crate::{
    MissionEvaluationAuthority, MissionEvaluationPlanId, MissionEvaluationPlanVersionId, MissionId,
    MissionObligationId, MissionWorkUnitId, OperationsError, WorkspaceParticipationId,
};

use super::{MissionEvidenceMemberCriteriaDigest, MissionEvidencePlanDigests};

const MAX_EVIDENCE_PARTITIONS: usize = 256;
const MAX_EVIDENCE_MEMBERS: usize = 256;

/// Bounded, canonical set of source manifests for a cross-Activity local plan.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceManifestBundle {
    manifests: Vec<EvaluationRegisteredManifest>,
    workspace_id: WorkspaceId,
    plan_binding: forge_domain::EvaluationPlanBinding,
    deadline_ms: i64,
}

impl MissionEvidenceManifestBundle {
    /// Construct a complete set of source partitions sharing one Workspace
    /// delegation and one exact local plan binding.
    ///
    /// # Errors
    /// Rejects empty/oversized bundles, repeated Activities/source members, and
    /// partitions whose Workspace, backend, delegation, plan, or T1 differs.
    pub fn new(mut manifests: Vec<EvaluationRegisteredManifest>) -> Result<Self, OperationsError> {
        if manifests.is_empty() || manifests.len() > MAX_EVIDENCE_PARTITIONS {
            return Err(OperationsError::Bounds(
                "mission evidence manifest partitions",
            ));
        }
        manifests.sort_by_key(|value| value.proposal().source().activity_id().into_uuid());
        let first = manifests.first().ok_or(OperationsError::Conflict(
            "mission evidence manifest bundle",
        ))?;
        let source = first.proposal().source();
        let plan = first.proposal().plan();
        let deadline = first.proposal().deadline();
        let mut activities = std::collections::BTreeSet::new();
        let mut manifest_ids = std::collections::BTreeSet::new();
        let mut obligations = std::collections::BTreeSet::new();
        let mut units = std::collections::BTreeSet::new();
        let mut assignments = std::collections::BTreeSet::new();
        let mut total_members = 0_usize;
        for manifest in &manifests {
            let proposal = manifest.proposal();
            let actual_source = proposal.source();
            let actual_plan = proposal.plan();
            if actual_source.backend_id() != source.backend_id()
                || actual_source.workspace_id() != source.workspace_id()
                || actual_source.delegation_migration_id() != source.delegation_migration_id()
                || actual_source.delegation_destination_id() != source.delegation_destination_id()
                || actual_source.delegation_freeze_revision() != source.delegation_freeze_revision()
                || actual_source.delegation_manifest_digest() != source.delegation_manifest_digest()
                || actual_plan != plan
                || proposal.deadline() != deadline
            {
                return Err(OperationsError::Conflict(
                    "mission evidence manifest partition scope",
                ));
            }
            if !activities.insert(actual_source.activity_id().into_uuid()) {
                return Err(OperationsError::Conflict(
                    "mission evidence duplicate Activity partition",
                ));
            }
            if !manifest_ids.insert(proposal.manifest_id().into_uuid()) {
                return Err(OperationsError::Conflict(
                    "mission evidence duplicate manifest partition",
                ));
            }
            total_members = total_members
                .checked_add(proposal.members().len())
                .ok_or(OperationsError::Bounds("mission evidence manifest members"))?;
            for member in proposal.members() {
                if !obligations.insert(member.obligation_id().into_uuid())
                    || !units.insert(member.work_unit_id().into_uuid())
                    || !assignments.insert(member.assignment_id().into_uuid())
                {
                    return Err(OperationsError::Conflict(
                        "mission evidence duplicate source member",
                    ));
                }
            }
        }
        if total_members == 0 || total_members > MAX_EVIDENCE_MEMBERS {
            return Err(OperationsError::Bounds("mission evidence manifest members"));
        }
        Ok(Self {
            manifests,
            workspace_id: source.workspace_id(),
            plan_binding: plan,
            deadline_ms: deadline.get(),
        })
    }

    pub fn manifests(&self) -> &[EvaluationRegisteredManifest] {
        &self.manifests
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn plan_binding(&self) -> forge_domain::EvaluationPlanBinding {
        self.plan_binding
    }

    #[must_use]
    pub const fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }
}

/// One Activity's exact T1 and review cut pair for an existing manifest partition.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceObservationPartition {
    activity_id: ActivityId,
    t1_cut: EvaluationT1CoverageCut,
    review_cut: EvaluationReviewCut,
}

impl MissionEvidenceObservationPartition {
    /// Construct one source cut pair whose review cut follows the exact T1 cut.
    ///
    /// # Errors
    /// Rejects nil Activity identity, a different T1 cut ID, or a non-advancing
    /// review high-water.
    pub fn new(
        activity_id: ActivityId,
        t1_cut: EvaluationT1CoverageCut,
        review_cut: EvaluationReviewCut,
    ) -> Result<Self, OperationsError> {
        if activity_id.into_uuid().is_nil()
            || review_cut.t1_cut_id() != t1_cut.cut_id()
            || review_cut.source_high_water().get() <= t1_cut.source_high_water().get()
        {
            return Err(OperationsError::Conflict(
                "mission evidence observation partition",
            ));
        }
        Ok(Self {
            activity_id,
            t1_cut,
            review_cut,
        })
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    pub fn t1_cut(&self) -> &EvaluationT1CoverageCut {
        &self.t1_cut
    }

    pub fn review_cut(&self) -> &EvaluationReviewCut {
        &self.review_cut
    }
}

/// Canonically ordered complete cut-pair set for every registered Activity partition.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceObservationBundle {
    partitions: Vec<MissionEvidenceObservationPartition>,
}

impl MissionEvidenceObservationBundle {
    /// Construct a bounded cut bundle with one entry per source Activity.
    ///
    /// # Errors
    /// Rejects an empty/oversized set or duplicate Activity partitions.
    pub fn new(
        mut partitions: Vec<MissionEvidenceObservationPartition>,
    ) -> Result<Self, OperationsError> {
        if partitions.is_empty() || partitions.len() > MAX_EVIDENCE_PARTITIONS {
            return Err(OperationsError::Bounds(
                "mission evidence observation partitions",
            ));
        }
        partitions.sort_by_key(|value| value.activity_id().into_uuid());
        if partitions
            .windows(2)
            .any(|pair| pair[0].activity_id() == pair[1].activity_id())
        {
            return Err(OperationsError::Conflict(
                "mission evidence duplicate observation Activity",
            ));
        }
        Ok(Self { partitions })
    }

    pub fn partitions(&self) -> &[MissionEvidenceObservationPartition] {
        &self.partitions
    }
}

/// Native mutation command binding a started local plan to Forge's exact
/// registered manifest readback. The record itself is descriptive source data;
/// the trusted Runtime must fetch it from the configured Forge owner.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceManifestBindingCommand {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    plan_revision: u64,
    bundle: MissionEvidenceManifestBundle,
    recorded_at_ms: i64,
    authority: MissionEvaluationAuthority,
}

impl MissionEvidenceManifestBindingCommand {
    /// Construct one checked manifest-binding request.
    ///
    /// # Errors
    /// Rejects a nil request ID or negative local observation time.
    pub fn new(
        request_id: Uuid,
        workspace_id: WorkspaceId,
        plan_id: MissionEvaluationPlanId,
        plan_revision: u64,
        bundle: MissionEvidenceManifestBundle,
        recorded_at_ms: i64,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil()
            || workspace_id.into_uuid().is_nil()
            || plan_revision == 0
            || recorded_at_ms < 0
            || bundle.workspace_id() != workspace_id
            || bundle.plan_binding().plan_id().into_uuid() != plan_id.into_uuid()
            || bundle.plan_binding().revision().get()
                != i64::try_from(plan_revision)
                    .map_err(|_| OperationsError::Bounds("mission evidence plan revision"))?
        {
            return Err(OperationsError::Conflict(
                "mission evidence manifest request",
            ));
        }
        Ok(Self {
            request_id,
            workspace_id,
            plan_id,
            plan_revision,
            bundle,
            recorded_at_ms,
            authority,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn plan_id(&self) -> MissionEvaluationPlanId {
        self.plan_id
    }

    #[must_use]
    pub const fn plan_revision(&self) -> u64 {
        self.plan_revision
    }

    pub fn bundle(&self) -> &MissionEvidenceManifestBundle {
        &self.bundle
    }

    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }

    pub(crate) const fn authority(&self) -> MissionEvaluationAuthority {
        self.authority
    }
}

/// Native mutation command appending one exact Forge T1 and review cut.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceObservationCommand {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    plan_revision: u64,
    expected_head_revision: u64,
    cuts: MissionEvidenceObservationBundle,
    recorded_at_ms: i64,
    authority: MissionEvaluationAuthority,
}

impl MissionEvidenceObservationCommand {
    /// Construct one checked exact-cut observation command.
    ///
    /// # Errors
    /// Rejects nil identities, zero plan revisions, invalid local CAS revisions,
    /// negative observation time, a review cut for a different T1 cut, and a
    /// review source high-water that does not advance the T1 cut.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: Uuid,
        workspace_id: WorkspaceId,
        plan_id: MissionEvaluationPlanId,
        plan_revision: u64,
        expected_head_revision: u64,
        cuts: MissionEvidenceObservationBundle,
        recorded_at_ms: i64,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil()
            || workspace_id.into_uuid().is_nil()
            || plan_revision == 0
            || expected_head_revision > i64::MAX as u64
            || recorded_at_ms < 0
        {
            return Err(OperationsError::Conflict(
                "mission evidence observation request",
            ));
        }
        Ok(Self {
            request_id,
            workspace_id,
            plan_id,
            plan_revision,
            expected_head_revision,
            cuts,
            recorded_at_ms,
            authority,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn plan_id(&self) -> MissionEvaluationPlanId {
        self.plan_id
    }

    #[must_use]
    pub const fn plan_revision(&self) -> u64 {
        self.plan_revision
    }

    #[must_use]
    pub const fn expected_head_revision(&self) -> u64 {
        self.expected_head_revision
    }

    pub fn cuts(&self) -> &MissionEvidenceObservationBundle {
        &self.cuts
    }

    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }

    pub(crate) const fn authority(&self) -> MissionEvaluationAuthority {
        self.authority
    }
}

/// Proof captured for a historical Forge actor tuple at observation time.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceIdentityProof {
    pub(super) actor_agent_id: AgentInstanceId,
    pub(super) actor_session_id: AgentSessionId,
    pub(super) participant_id: Uuid,
    pub(super) profile_id: Option<Uuid>,
    pub(super) participation_id: Option<WorkspaceParticipationId>,
    pub(super) registration_kind: MissionEvidenceRegistrationKind,
    pub(super) registration_request_id: Uuid,
    pub(super) migration_id: Uuid,
    pub(super) destination_id: Uuid,
    pub(super) freeze_revision: u64,
    pub(super) membership_commit_digest: [u8; 32],
    pub(super) activation_receipt_id: Uuid,
    pub(super) activated_at_ms: i64,
    pub(super) profile_mapping_request_id: Option<Uuid>,
    pub(super) profile_revision: Option<u64>,
    pub(super) session_revision: Option<u64>,
    pub(super) participation_mapping_request_id: Option<Uuid>,
    pub(super) participant_membership_revision: Option<u64>,
    pub(super) participation_mapped_at_ms: Option<i64>,
    pub(super) rebind_request_id: Option<Uuid>,
    pub(super) rebind_revision: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MissionEvidenceIdentityActor {
    pub(super) agent: AgentInstanceId,
    pub(super) session: AgentSessionId,
    pub(super) participant: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MissionEvidenceRegistrationProvenance {
    pub(super) kind: MissionEvidenceRegistrationKind,
    pub(super) request_id: Uuid,
    pub(super) migration_id: Uuid,
    pub(super) destination_id: Uuid,
    pub(super) freeze_revision: u64,
    pub(super) membership_commit_digest: [u8; 32],
    pub(super) activation_receipt_id: Uuid,
    pub(super) activated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MissionEvidenceProfileProvenance {
    pub(super) profile_id: Uuid,
    pub(super) mapping_request_id: Uuid,
    pub(super) profile_revision: u64,
    pub(super) session_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MissionEvidenceParticipationProvenance {
    pub(super) participation_id: WorkspaceParticipationId,
    pub(super) mapping_request_id: Option<Uuid>,
    pub(super) participant_membership_revision: u64,
    pub(super) mapped_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MissionEvidenceReplacementProvenance {
    pub(super) rebind_request_id: Uuid,
    pub(super) rebind_revision: u64,
}

/// Named provenance groups used when a historical identity proof is assembled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MissionEvidenceIdentityProofParts {
    pub(super) actor: MissionEvidenceIdentityActor,
    pub(super) registration: MissionEvidenceRegistrationProvenance,
    pub(super) profile: Option<MissionEvidenceProfileProvenance>,
    pub(super) participation: Option<MissionEvidenceParticipationProvenance>,
    pub(super) replacement: Option<MissionEvidenceReplacementProvenance>,
}

#[derive(Clone, Copy)]
struct IdentityProofFields {
    actor_agent_id: AgentInstanceId,
    actor_session_id: AgentSessionId,
    participant_id: Uuid,
    profile_id: Option<Uuid>,
    participation_id: Option<WorkspaceParticipationId>,
    registration_kind: MissionEvidenceRegistrationKind,
    registration_request_id: Uuid,
    migration_id: Uuid,
    destination_id: Uuid,
    freeze_revision: u64,
    membership_commit_digest: [u8; 32],
    activation_receipt_id: Uuid,
    activated_at_ms: i64,
    profile_mapping_request_id: Option<Uuid>,
    profile_revision: Option<u64>,
    session_revision: Option<u64>,
    participation_mapping_request_id: Option<Uuid>,
    participant_membership_revision: Option<u64>,
    participation_mapped_at_ms: Option<i64>,
    rebind_request_id: Option<Uuid>,
    rebind_revision: Option<u64>,
}

fn identity_proof_fields(parts: &MissionEvidenceIdentityProofParts) -> IdentityProofFields {
    let actor = parts.actor;
    let registration = parts.registration;
    let profile = parts.profile;
    let participation = parts.participation;
    let replacement = parts.replacement;
    IdentityProofFields {
        actor_agent_id: actor.agent,
        actor_session_id: actor.session,
        participant_id: actor.participant,
        profile_id: profile.map(|value| value.profile_id),
        participation_id: participation.map(|value| value.participation_id),
        registration_kind: registration.kind,
        registration_request_id: registration.request_id,
        migration_id: registration.migration_id,
        destination_id: registration.destination_id,
        freeze_revision: registration.freeze_revision,
        membership_commit_digest: registration.membership_commit_digest,
        activation_receipt_id: registration.activation_receipt_id,
        activated_at_ms: registration.activated_at_ms,
        profile_mapping_request_id: profile.map(|value| value.mapping_request_id),
        profile_revision: profile.map(|value| value.profile_revision),
        session_revision: profile.map(|value| value.session_revision),
        participation_mapping_request_id: participation.and_then(|value| value.mapping_request_id),
        participant_membership_revision: participation
            .map(|value| value.participant_membership_revision),
        participation_mapped_at_ms: participation.and_then(|value| value.mapped_at_ms),
        rebind_request_id: replacement.map(|value| value.rebind_request_id),
        rebind_revision: replacement.map(|value| value.rebind_revision),
    }
}

fn validate_identity_proof_fields(fields: &IdentityProofFields) -> Result<(), OperationsError> {
    if fields.actor_agent_id.into_uuid().is_nil()
        || fields.actor_session_id.into_uuid().is_nil()
        || fields.participant_id.is_nil()
        || fields.registration_request_id.is_nil()
        || fields.migration_id.is_nil()
        || fields.destination_id.is_nil()
        || fields.freeze_revision == 0
        || fields.membership_commit_digest == [0; 32]
        || fields.activation_receipt_id.is_nil()
        || fields.activated_at_ms < 0
        || fields.profile_id.is_some_and(|value| value.is_nil())
        || fields
            .participation_id
            .is_some_and(|value| value.into_uuid().is_nil())
        || fields
            .profile_mapping_request_id
            .is_some_and(|value| value.is_nil())
        || fields
            .participation_mapping_request_id
            .is_some_and(|value| value.is_nil())
        || fields.rebind_request_id.is_some_and(|value| value.is_nil())
        || fields.profile_revision.is_some_and(|value| value == 0)
        || fields.session_revision.is_some_and(|value| value == 0)
        || fields
            .participant_membership_revision
            .is_some_and(|value| value == 0)
        || fields
            .participation_mapped_at_ms
            .is_some_and(|value| value < 0)
        || fields.rebind_revision.is_some_and(|value| value == 0)
    {
        return Err(OperationsError::Conflict(
            "mission evidence historical identity proof",
        ));
    }
    let profile_shape = fields.profile_id.is_some() == fields.profile_mapping_request_id.is_some()
        && fields.profile_id.is_some() == fields.profile_revision.is_some()
        && fields.profile_id.is_some() == fields.session_revision.is_some();
    let participation_mapping_shape = fields.participation_mapping_request_id.is_some()
        == fields.participation_mapped_at_ms.is_some()
        && fields.participation_id.is_some() == fields.participant_membership_revision.is_some()
        && fields
            .participation_mapping_request_id
            .is_none_or(|_| fields.participation_id.is_some());
    let replacement_shape = fields.registration_kind
        != MissionEvidenceRegistrationKind::Replacement
        || (fields.rebind_request_id.is_some() && fields.rebind_revision.is_some());
    let original_shape = fields.registration_kind != MissionEvidenceRegistrationKind::Original
        || (fields.rebind_request_id.is_none() && fields.rebind_revision.is_none());
    let participation_provenance = fields.participation_id.is_none()
        || fields.participation_mapping_request_id.is_some()
        || fields.rebind_request_id.is_some();
    if !profile_shape
        || !participation_mapping_shape
        || !participation_provenance
        || !replacement_shape
        || !original_shape
        || (fields.participation_id.is_some() && fields.profile_id.is_none())
    {
        return Err(OperationsError::Conflict(
            "mission evidence historical identity proof",
        ));
    }
    Ok(())
}

impl MissionEvidenceIdentityProof {
    pub(super) fn from_parts(
        parts: &MissionEvidenceIdentityProofParts,
    ) -> Result<Self, OperationsError> {
        let fields = identity_proof_fields(parts);
        validate_identity_proof_fields(&fields)?;
        Ok(Self {
            actor_agent_id: fields.actor_agent_id,
            actor_session_id: fields.actor_session_id,
            participant_id: fields.participant_id,
            profile_id: fields.profile_id,
            participation_id: fields.participation_id,
            registration_kind: fields.registration_kind,
            registration_request_id: fields.registration_request_id,
            migration_id: fields.migration_id,
            destination_id: fields.destination_id,
            freeze_revision: fields.freeze_revision,
            membership_commit_digest: fields.membership_commit_digest,
            activation_receipt_id: fields.activation_receipt_id,
            activated_at_ms: fields.activated_at_ms,
            profile_mapping_request_id: fields.profile_mapping_request_id,
            profile_revision: fields.profile_revision,
            session_revision: fields.session_revision,
            participation_mapping_request_id: fields.participation_mapping_request_id,
            participant_membership_revision: fields.participant_membership_revision,
            participation_mapped_at_ms: fields.participation_mapped_at_ms,
            rebind_request_id: fields.rebind_request_id,
            rebind_revision: fields.rebind_revision,
        })
    }

    #[must_use]
    pub const fn actor_agent_id(&self) -> AgentInstanceId {
        self.actor_agent_id
    }

    pub const fn actor_session_id(&self) -> AgentSessionId {
        self.actor_session_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> Option<Uuid> {
        self.profile_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.participation_id
    }

    pub const fn registration_kind(&self) -> MissionEvidenceRegistrationKind {
        self.registration_kind
    }

    #[must_use]
    pub const fn registration_request_id(&self) -> Uuid {
        self.registration_request_id
    }

    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration_id
    }

    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.destination_id
    }

    #[must_use]
    pub const fn freeze_revision(&self) -> u64 {
        self.freeze_revision
    }

    #[must_use]
    pub const fn membership_commit_digest(&self) -> [u8; 32] {
        self.membership_commit_digest
    }

    #[must_use]
    pub const fn activation_receipt_id(&self) -> Uuid {
        self.activation_receipt_id
    }

    #[must_use]
    pub const fn activated_at_ms(&self) -> i64 {
        self.activated_at_ms
    }

    #[must_use]
    pub const fn profile_mapping_request_id(&self) -> Option<Uuid> {
        self.profile_mapping_request_id
    }

    #[must_use]
    pub const fn profile_revision(&self) -> Option<u64> {
        self.profile_revision
    }

    #[must_use]
    pub const fn session_revision(&self) -> Option<u64> {
        self.session_revision
    }

    #[must_use]
    pub const fn participation_mapping_request_id(&self) -> Option<Uuid> {
        self.participation_mapping_request_id
    }

    #[must_use]
    pub const fn participant_membership_revision(&self) -> Option<u64> {
        self.participant_membership_revision
    }

    #[must_use]
    pub const fn participation_mapped_at_ms(&self) -> Option<i64> {
        self.participation_mapped_at_ms
    }

    #[must_use]
    pub const fn rebind_request_id(&self) -> Option<Uuid> {
        self.rebind_request_id
    }

    #[must_use]
    pub const fn rebind_revision(&self) -> Option<u64> {
        self.rebind_revision
    }
}

/// Durable state of the exact source-to-local actor resolution.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissionEvidenceIdentityResolution {
    Verified(Box<MissionEvidenceIdentityProof>),
    Pending {
        agent_id: AgentInstanceId,
        session_id: AgentSessionId,
        reason: MissionEvidenceIdentityPendingReason,
        registration_request_id: Option<Uuid>,
        activation_receipt_id: Option<Uuid>,
    },
}

/// Why a source Agent/Session tuple did not resolve to one complete historical identity.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionEvidenceIdentityPendingReason {
    NoRegistrationHistory,
    RegistrationNotCommitted,
    ActivationMissing,
    ProfileLinkMissing,
    ParticipationMappingMissing,
    ReplacementRebindMissing,
    AmbiguousHistory,
}

/// Which immutable registration lifecycle established a historical actor.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionEvidenceRegistrationKind {
    Original,
    Replacement,
}

/// Whether every original target in a source manifest has a verified local lineage.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionEvidenceBindingState {
    Verified,
    Pending,
}

/// One exact member binding from a registered source manifest to its frozen local obligation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceBoundMember {
    partition_ordinal: u32,
    source_activity_id: ActivityId,
    declaration: EvaluationManifestMember,
    original_binding: EvaluationOriginalBindingStatus,
    obligation_id: MissionObligationId,
    work_unit_id: MissionWorkUnitId,
    participation_id: WorkspaceParticipationId,
    weight: u64,
    due_at_ms: i64,
    criteria_digest: MissionEvidenceMemberCriteriaDigest,
    target_identity: MissionEvidenceIdentityResolution,
}

impl MissionEvidenceBoundMember {
    pub(super) fn from_parts(
        parts: MissionEvidenceBoundMemberParts,
    ) -> Result<Self, OperationsError> {
        let MissionEvidenceBoundMemberParts {
            partition_ordinal,
            source_activity_id,
            declaration,
            original_binding,
            obligation_id,
            work_unit_id,
            participation_id,
            weight,
            due_at_ms,
            criteria_digest,
            target_identity,
        } = parts;
        if source_activity_id.into_uuid().is_nil()
            || declaration.obligation_id().into_uuid() != obligation_id.into_uuid()
            || declaration.work_unit_id().into_uuid() != work_unit_id.into_uuid()
            || criteria_digest.obligation_id() != obligation_id
            || criteria_digest.work_unit_id() != work_unit_id
            || criteria_digest.participation_id() != participation_id
            || weight == 0
            || due_at_ms < 0
        {
            return Err(OperationsError::Conflict(
                "mission evidence bound member identity",
            ));
        }
        Ok(Self {
            partition_ordinal,
            source_activity_id,
            declaration,
            original_binding,
            obligation_id,
            work_unit_id,
            participation_id,
            weight,
            due_at_ms,
            criteria_digest,
            target_identity,
        })
    }

    #[must_use]
    pub const fn partition_ordinal(&self) -> u32 {
        self.partition_ordinal
    }

    #[must_use]
    pub const fn source_activity_id(&self) -> ActivityId {
        self.source_activity_id
    }

    pub const fn declaration(&self) -> EvaluationManifestMember {
        self.declaration
    }

    pub const fn original_binding(&self) -> EvaluationOriginalBindingStatus {
        self.original_binding
    }

    pub const fn obligation_id(&self) -> MissionObligationId {
        self.obligation_id
    }

    pub const fn work_unit_id(&self) -> MissionWorkUnitId {
        self.work_unit_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn weight(&self) -> u64 {
        self.weight
    }

    #[must_use]
    pub const fn due_at_ms(&self) -> i64 {
        self.due_at_ms
    }

    pub const fn criteria_digest(&self) -> MissionEvidenceMemberCriteriaDigest {
        self.criteria_digest
    }

    pub fn target_identity(&self) -> &MissionEvidenceIdentityResolution {
        &self.target_identity
    }
}

pub(super) struct MissionEvidenceBoundMemberParts {
    pub(super) partition_ordinal: u32,
    pub(super) source_activity_id: ActivityId,
    pub(super) declaration: EvaluationManifestMember,
    pub(super) original_binding: EvaluationOriginalBindingStatus,
    pub(super) obligation_id: MissionObligationId,
    pub(super) work_unit_id: MissionWorkUnitId,
    pub(super) participation_id: WorkspaceParticipationId,
    pub(super) weight: u64,
    pub(super) due_at_ms: i64,
    pub(super) criteria_digest: MissionEvidenceMemberCriteriaDigest,
    pub(super) target_identity: MissionEvidenceIdentityResolution,
}

/// Immutable local binding of one exact started plan to a Forge registration readback.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceManifestBindingRecord {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    plan_id: MissionEvaluationPlanId,
    plan_version_id: MissionEvaluationPlanVersionId,
    plan_revision: u64,
    plan_start_request_id: Uuid,
    plan_started_at_ms: i64,
    t0_commit_at_ms: i64,
    t1_submission_cutoff_ms: i64,
    t2_checkpoint_ms: i64,
    manifest_bundle: MissionEvidenceManifestBundle,
    local_plan_digest: forge_domain::EvaluationDigest,
    local_criteria_digest: forge_domain::EvaluationDigest,
    state: MissionEvidenceBindingState,
    members: Vec<MissionEvidenceBoundMember>,
    actor: MissionEvidenceActorReceipt,
    recorded_at_ms: i64,
}

impl MissionEvidenceManifestBindingRecord {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn mission_id(&self) -> MissionId {
        self.mission_id
    }

    pub const fn plan_id(&self) -> MissionEvaluationPlanId {
        self.plan_id
    }

    pub const fn plan_version_id(&self) -> MissionEvaluationPlanVersionId {
        self.plan_version_id
    }

    #[must_use]
    pub const fn plan_revision(&self) -> u64 {
        self.plan_revision
    }

    #[must_use]
    pub const fn plan_start_request_id(&self) -> Uuid {
        self.plan_start_request_id
    }

    #[must_use]
    pub const fn plan_started_at_ms(&self) -> i64 {
        self.plan_started_at_ms
    }

    #[must_use]
    pub const fn t0_commit_at_ms(&self) -> i64 {
        self.t0_commit_at_ms
    }

    #[must_use]
    pub const fn t1_submission_cutoff_ms(&self) -> i64 {
        self.t1_submission_cutoff_ms
    }

    #[must_use]
    pub const fn t2_checkpoint_ms(&self) -> i64 {
        self.t2_checkpoint_ms
    }

    pub fn manifest_bundle(&self) -> &MissionEvidenceManifestBundle {
        &self.manifest_bundle
    }

    #[must_use]
    pub const fn local_plan_digest(&self) -> forge_domain::EvaluationDigest {
        self.local_plan_digest
    }

    #[must_use]
    pub const fn local_criteria_digest(&self) -> forge_domain::EvaluationDigest {
        self.local_criteria_digest
    }

    pub const fn state(&self) -> MissionEvidenceBindingState {
        self.state
    }

    pub fn members(&self) -> &[MissionEvidenceBoundMember] {
        &self.members
    }

    pub const fn actor(&self) -> &MissionEvidenceActorReceipt {
        &self.actor
    }

    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }
}

/// Immutable result of a manifest-binding mutation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceManifestBindingOutcome {
    binding: MissionEvidenceManifestBindingRecord,
    replayed: bool,
}

impl MissionEvidenceManifestBindingOutcome {
    pub(crate) const fn new(binding: MissionEvidenceManifestBindingRecord, replayed: bool) -> Self {
        Self { binding, replayed }
    }

    pub const fn binding(&self) -> &MissionEvidenceManifestBindingRecord {
        &self.binding
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Actor attribution retained on one immutable evidence request.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceActorReceipt {
    kind: MissionEvidenceActorKind,
    semantic_key: String,
    participation_id: Option<WorkspaceParticipationId>,
    profile_id: Option<Uuid>,
    participant_id: Option<Uuid>,
    session_id: Option<AgentSessionId>,
    display_name: String,
    leadership_revision: Option<i64>,
}

impl MissionEvidenceActorReceipt {
    pub(crate) fn from_snapshot(
        snapshot: &crate::mission_evaluation::MissionActorSnapshot,
    ) -> Result<Self, OperationsError> {
        snapshot.validate()?;
        let identity = &snapshot.identity;
        let kind = match identity.kind {
            0 => MissionEvidenceActorKind::LocalOperator,
            1 => MissionEvidenceActorKind::ProfileParticipant,
            2 => MissionEvidenceActorKind::NativeParticipant,
            _ => return Err(OperationsError::Corrupt("mission evidence actor kind")),
        };
        Ok(Self {
            kind,
            semantic_key: identity.key.clone(),
            participation_id: identity.participation_id,
            profile_id: identity.profile_id,
            participant_id: identity.participant_id,
            session_id: snapshot.session_id,
            display_name: snapshot.display_name.clone(),
            leadership_revision: snapshot.leadership_revision,
        })
    }

    pub(super) fn from_parts(
        parts: MissionEvidenceActorReceiptParts,
    ) -> Result<Self, OperationsError> {
        let MissionEvidenceActorReceiptParts {
            kind,
            semantic_key,
            participation_id,
            profile_id,
            participant_id,
            session_id,
            display_name,
            leadership_revision,
        } = parts;
        let kind_code = match kind {
            MissionEvidenceActorKind::LocalOperator => 0,
            MissionEvidenceActorKind::ProfileParticipant => 1,
            MissionEvidenceActorKind::NativeParticipant => 2,
        };
        let identity = crate::mission_evaluation::MissionActorIdentity {
            kind: kind_code,
            key: semantic_key.clone(),
            participation_id,
            profile_id,
            participant_id,
        };
        identity.validate()?;
        if display_name.is_empty()
            || display_name.len() > 128
            || display_name.as_bytes().contains(&0)
        {
            return Err(OperationsError::Corrupt("mission evidence actor display"));
        }
        let shape = match kind {
            MissionEvidenceActorKind::LocalOperator => {
                session_id.is_none() && leadership_revision.is_none()
            }
            MissionEvidenceActorKind::ProfileParticipant
            | MissionEvidenceActorKind::NativeParticipant => {
                session_id.is_some_and(|id| !id.into_uuid().is_nil())
                    && leadership_revision.is_some_and(|revision| revision >= 1)
            }
        };
        if !shape {
            return Err(OperationsError::Corrupt("mission evidence actor snapshot"));
        }
        Ok(Self {
            kind,
            semantic_key,
            participation_id,
            profile_id,
            participant_id,
            session_id,
            display_name,
            leadership_revision,
        })
    }

    pub const fn kind(&self) -> MissionEvidenceActorKind {
        self.kind
    }

    #[must_use]
    pub fn semantic_key(&self) -> &str {
        &self.semantic_key
    }

    #[must_use]
    pub const fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> Option<Uuid> {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Option<Uuid> {
        self.participant_id
    }

    #[must_use]
    pub const fn session_id(&self) -> Option<AgentSessionId> {
        self.session_id
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    #[must_use]
    pub const fn leadership_revision(&self) -> Option<i64> {
        self.leadership_revision
    }
}

pub(super) struct MissionEvidenceActorReceiptParts {
    pub(super) kind: MissionEvidenceActorKind,
    pub(super) semantic_key: String,
    pub(super) participation_id: Option<WorkspaceParticipationId>,
    pub(super) profile_id: Option<Uuid>,
    pub(super) participant_id: Option<Uuid>,
    pub(super) session_id: Option<AgentSessionId>,
    pub(super) display_name: String,
    pub(super) leadership_revision: Option<i64>,
}

/// Closed semantic actor class captured by the Mission evidence journal.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionEvidenceActorKind {
    LocalOperator,
    ProfileParticipant,
    NativeParticipant,
}

/// A bounded state that never promotes unresolved source facts to performance.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionEvidenceDisposition {
    AcceptedOriginal,
    Unresolved(MissionEvidenceUnresolvedReason),
}

/// Source or identity condition that leaves personal evidence unresolved.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionEvidenceUnresolvedReason {
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

/// One member's original submission, review, identity provenance, and bounded disposition.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceObservationMember {
    partition_ordinal: u32,
    source_activity_id: ActivityId,
    source: EvaluationReviewCutMember,
    participation_id: WorkspaceParticipationId,
    weight: u64,
    due_at_ms: i64,
    target_identity: MissionEvidenceIdentityResolution,
    author_identity: Option<MissionEvidenceIdentityResolution>,
    reviewer_identity: Option<MissionEvidenceIdentityResolution>,
    disposition: MissionEvidenceDisposition,
}

impl MissionEvidenceObservationMember {
    pub(super) fn from_parts(
        parts: MissionEvidenceObservationMemberParts,
    ) -> Result<Self, OperationsError> {
        let MissionEvidenceObservationMemberParts {
            partition_ordinal,
            source_activity_id,
            source,
            participation_id,
            weight,
            due_at_ms,
            target_identity,
            author_identity,
            reviewer_identity,
            disposition,
        } = parts;
        let author_matches_target = match (source.submission(), author_identity.as_ref()) {
            (None, None) => true,
            (Some(_), Some(author)) => author == &target_identity,
            _ => false,
        };
        let reviewer_matches_source = match (source.review(), reviewer_identity.as_ref()) {
            (EvaluationReviewState::Reviewed(receipt), Some(identity)) => match identity {
                MissionEvidenceIdentityResolution::Verified(proof) => {
                    proof.actor_agent_id() == receipt.reviewer_agent_id()
                        && proof.actor_session_id() == receipt.reviewer_session_id()
                }
                MissionEvidenceIdentityResolution::Pending {
                    agent_id,
                    session_id,
                    ..
                } => {
                    *agent_id == receipt.reviewer_agent_id()
                        && *session_id == receipt.reviewer_session_id()
                }
            },
            (EvaluationReviewState::Reviewed(_), None) | (_, Some(_)) => false,
            (_, None) => true,
        };
        if source_activity_id.into_uuid().is_nil()
            || participation_id.into_uuid().is_nil()
            || weight == 0
            || due_at_ms < 0
            || !author_matches_target
            || !reviewer_matches_source
            || match &target_identity {
                MissionEvidenceIdentityResolution::Verified(proof) => {
                    proof.actor_agent_id() != source.member().target_agent_id()
                        || proof.actor_session_id() != source.member().target_session_id()
                        || proof
                            .participation_id()
                            .is_some_and(|value| value != participation_id)
                }
                MissionEvidenceIdentityResolution::Pending {
                    agent_id,
                    session_id,
                    ..
                } => {
                    *agent_id != source.member().target_agent_id()
                        || *session_id != source.member().target_session_id()
                }
            }
        {
            return Err(OperationsError::Conflict(
                "mission evidence observation member",
            ));
        }
        Ok(Self {
            partition_ordinal,
            source_activity_id,
            source,
            participation_id,
            weight,
            due_at_ms,
            target_identity,
            author_identity,
            reviewer_identity,
            disposition,
        })
    }

    #[must_use]
    pub const fn partition_ordinal(&self) -> u32 {
        self.partition_ordinal
    }

    #[must_use]
    pub const fn source_activity_id(&self) -> ActivityId {
        self.source_activity_id
    }

    pub const fn source(&self) -> &EvaluationReviewCutMember {
        &self.source
    }

    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn weight(&self) -> u64 {
        self.weight
    }

    #[must_use]
    pub const fn due_at_ms(&self) -> i64 {
        self.due_at_ms
    }

    pub const fn target_identity(&self) -> &MissionEvidenceIdentityResolution {
        &self.target_identity
    }

    #[must_use]
    pub fn author_identity(&self) -> Option<&MissionEvidenceIdentityResolution> {
        self.author_identity.as_ref()
    }

    #[must_use]
    pub fn reviewer_identity(&self) -> Option<&MissionEvidenceIdentityResolution> {
        self.reviewer_identity.as_ref()
    }

    pub const fn disposition(&self) -> MissionEvidenceDisposition {
        self.disposition
    }
}

pub(super) struct MissionEvidenceObservationMemberParts {
    pub(super) partition_ordinal: u32,
    pub(super) source_activity_id: ActivityId,
    pub(super) source: EvaluationReviewCutMember,
    pub(super) participation_id: WorkspaceParticipationId,
    pub(super) weight: u64,
    pub(super) due_at_ms: i64,
    pub(super) target_identity: MissionEvidenceIdentityResolution,
    pub(super) author_identity: Option<MissionEvidenceIdentityResolution>,
    pub(super) reviewer_identity: Option<MissionEvidenceIdentityResolution>,
    pub(super) disposition: MissionEvidenceDisposition,
}

/// Per-participation weight measures from one immutable observation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvidenceParticipationMeasures {
    participation_id: WorkspaceParticipationId,
    planned_due_weight: u64,
    accepted_original_due_weight: u64,
    unresolved_due_weight: u64,
    future_weight: u64,
}

impl MissionEvidenceParticipationMeasures {
    pub(crate) const fn new(
        participation_id: WorkspaceParticipationId,
        planned_due_weight: u64,
        accepted_original_due_weight: u64,
        unresolved_due_weight: u64,
        future_weight: u64,
    ) -> Self {
        Self {
            participation_id,
            planned_due_weight,
            accepted_original_due_weight,
            unresolved_due_weight,
            future_weight,
        }
    }

    #[must_use]
    pub const fn participation_id(self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn planned_due_weight(self) -> u64 {
        self.planned_due_weight
    }

    #[must_use]
    pub const fn accepted_original_due_weight(self) -> u64 {
        self.accepted_original_due_weight
    }

    #[must_use]
    pub const fn unresolved_due_weight(self) -> u64 {
        self.unresolved_due_weight
    }

    #[must_use]
    pub const fn future_weight(self) -> u64 {
        self.future_weight
    }
}

/// Immutable source-cut observation with local revision and weight measures.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceObservationRecord {
    request_id: Uuid,
    binding_request_id: Uuid,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    plan_revision: u64,
    revision: u64,
    expected_head_revision: u64,
    cuts: MissionEvidenceObservationBundle,
    members: Vec<MissionEvidenceObservationMember>,
    measures: Vec<MissionEvidenceParticipationMeasures>,
    actor: MissionEvidenceActorReceipt,
    recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewObservationParts {
    pub(crate) request_id: Uuid,
    pub(crate) binding_request_id: Uuid,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) plan_id: MissionEvaluationPlanId,
    pub(crate) plan_revision: u64,
    pub(crate) revision: u64,
    pub(crate) expected_head_revision: u64,
    pub(crate) cuts: MissionEvidenceObservationBundle,
    pub(crate) members: Vec<MissionEvidenceObservationMember>,
    pub(crate) measures: Vec<MissionEvidenceParticipationMeasures>,
    pub(crate) actor: MissionEvidenceActorReceipt,
    pub(crate) recorded_at_ms: i64,
}

impl From<NewObservationParts> for MissionEvidenceObservationRecord {
    fn from(value: NewObservationParts) -> Self {
        Self {
            request_id: value.request_id,
            binding_request_id: value.binding_request_id,
            workspace_id: value.workspace_id,
            plan_id: value.plan_id,
            plan_revision: value.plan_revision,
            revision: value.revision,
            expected_head_revision: value.expected_head_revision,
            cuts: value.cuts,
            members: value.members,
            measures: value.measures,
            actor: value.actor,
            recorded_at_ms: value.recorded_at_ms,
        }
    }
}

impl MissionEvidenceObservationRecord {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn binding_request_id(&self) -> Uuid {
        self.binding_request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn plan_id(&self) -> MissionEvaluationPlanId {
        self.plan_id
    }

    #[must_use]
    pub const fn plan_revision(&self) -> u64 {
        self.plan_revision
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn expected_head_revision(&self) -> u64 {
        self.expected_head_revision
    }

    pub fn cuts(&self) -> &MissionEvidenceObservationBundle {
        &self.cuts
    }

    pub fn members(&self) -> &[MissionEvidenceObservationMember] {
        &self.members
    }

    pub fn measures(&self) -> &[MissionEvidenceParticipationMeasures] {
        &self.measures
    }

    pub const fn actor(&self) -> &MissionEvidenceActorReceipt {
        &self.actor
    }

    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }
}

/// Immutable result of one source-cut append.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceObservationOutcome {
    observation: MissionEvidenceObservationRecord,
    replayed: bool,
}

impl MissionEvidenceObservationOutcome {
    pub(crate) const fn new(observation: MissionEvidenceObservationRecord, replayed: bool) -> Self {
        Self {
            observation,
            replayed,
        }
    }

    pub const fn observation(&self) -> &MissionEvidenceObservationRecord {
        &self.observation
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Exact actor-bound readback of one immutable evidence mutation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissionEvidenceMutationRecord {
    ManifestBinding(MissionEvidenceManifestBindingRecord),
    Observation(MissionEvidenceObservationRecord),
}

impl MissionEvidenceMutationRecord {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        match self {
            Self::ManifestBinding(value) => value.request_id(),
            Self::Observation(value) => value.request_id(),
        }
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        match self {
            Self::ManifestBinding(value) => value.workspace_id(),
            Self::Observation(value) => value.workspace_id(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewBindingParts {
    pub(crate) request_id: Uuid,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) mission_id: MissionId,
    pub(crate) plan_id: MissionEvaluationPlanId,
    pub(crate) plan_version_id: MissionEvaluationPlanVersionId,
    pub(crate) plan_revision: u64,
    pub(crate) plan_start_request_id: Uuid,
    pub(crate) plan_started_at_ms: i64,
    pub(crate) t0_commit_at_ms: i64,
    pub(crate) t1_submission_cutoff_ms: i64,
    pub(crate) t2_checkpoint_ms: i64,
    pub(crate) manifest_bundle: MissionEvidenceManifestBundle,
    pub(crate) plan_digests: MissionEvidencePlanDigests,
    pub(crate) state: MissionEvidenceBindingState,
    pub(crate) members: Vec<MissionEvidenceBoundMember>,
    pub(crate) actor: MissionEvidenceActorReceipt,
    pub(crate) recorded_at_ms: i64,
}

impl From<NewBindingParts> for MissionEvidenceManifestBindingRecord {
    fn from(value: NewBindingParts) -> Self {
        Self {
            request_id: value.request_id,
            workspace_id: value.workspace_id,
            mission_id: value.mission_id,
            plan_id: value.plan_id,
            plan_version_id: value.plan_version_id,
            plan_revision: value.plan_revision,
            plan_start_request_id: value.plan_start_request_id,
            plan_started_at_ms: value.plan_started_at_ms,
            t0_commit_at_ms: value.t0_commit_at_ms,
            t1_submission_cutoff_ms: value.t1_submission_cutoff_ms,
            t2_checkpoint_ms: value.t2_checkpoint_ms,
            manifest_bundle: value.manifest_bundle,
            local_plan_digest: value.plan_digests.plan_digest(),
            local_criteria_digest: value.plan_digests.criteria_digest(),
            state: value.state,
            members: value.members,
            actor: value.actor,
            recorded_at_ms: value.recorded_at_ms,
        }
    }
}

pub(crate) fn binding_from_parts(parts: NewBindingParts) -> MissionEvidenceManifestBindingRecord {
    parts.into()
}
