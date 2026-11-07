use std::collections::{BTreeMap, BTreeSet};

use forge_domain::{
    ActivityId, AgentAssignmentId, AgentAssignmentRevision, AgentInstanceId, AgentSessionId,
    EvaluationDigest, EvaluationManifestId, EvaluationManifestMember, EvaluationManifestProposal,
    EvaluationPlanBinding, EvaluationPlanId, EvaluationRevision, EvaluationSourceIdentity,
    ForgeBackendId, TimestampMillis, WorkspaceId,
};
use uuid::Uuid;

use crate::{
    ImportHandoffSeal, ImportPhase, MissionEvaluationAuthority, MissionEvaluationPlanId,
    MissionObligationId, OperationsCatalog, OperationsError,
    mission_evaluation::{MissionActorSnapshot, frozen_plan_in_transaction},
};

use super::MissionEvidenceIdentityResolution;

const MAX_PREPARATION_PARTITIONS: usize = 256;
const MAX_PREPARATION_MEMBERS: usize = 256;

/// One exact obligation selector for a future Forge manifest declaration.
///
/// This is native preparation input, not proof that Forge accepted the
/// assignment or the target Session.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvidenceManifestCandidateMember {
    obligation_id: MissionObligationId,
    assignment_id: AgentAssignmentId,
    assignment_revision: AgentAssignmentRevision,
    target_agent_id: AgentInstanceId,
    target_session_id: AgentSessionId,
}

impl MissionEvidenceManifestCandidateMember {
    /// Construct one bounded candidate keyed by the exact frozen obligation.
    ///
    /// # Errors
    /// Rejects nil IDs and a nonpositive base assignment revision.
    pub fn new(
        obligation_id: MissionObligationId,
        assignment_id: AgentAssignmentId,
        assignment_revision: AgentAssignmentRevision,
        target_agent_id: AgentInstanceId,
        target_session_id: AgentSessionId,
    ) -> Result<Self, OperationsError> {
        if obligation_id.into_uuid().is_nil()
            || assignment_id.into_uuid().is_nil()
            || assignment_revision.get() <= 0
            || target_agent_id.into_uuid().is_nil()
            || target_session_id.into_uuid().is_nil()
        {
            return Err(OperationsError::Conflict(
                "mission evidence candidate member identity",
            ));
        }
        Ok(Self {
            obligation_id,
            assignment_id,
            assignment_revision,
            target_agent_id,
            target_session_id,
        })
    }

    pub const fn obligation_id(self) -> MissionObligationId {
        self.obligation_id
    }

    #[must_use]
    pub const fn assignment_id(self) -> AgentAssignmentId {
        self.assignment_id
    }

    /// Frozen original base revision, before any submitted work revision.
    #[must_use]
    pub const fn assignment_revision(self) -> AgentAssignmentRevision {
        self.assignment_revision
    }

    #[must_use]
    pub const fn target_agent_id(self) -> AgentInstanceId {
        self.target_agent_id
    }

    pub const fn target_session_id(self) -> AgentSessionId {
        self.target_session_id
    }
}

/// Stable per-Activity manifest identity and complete candidate member set.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceManifestCandidatePartition {
    activity_id: ActivityId,
    manifest_id: EvaluationManifestId,
    manifest_revision: EvaluationRevision,
    members: Vec<MissionEvidenceManifestCandidateMember>,
}

impl MissionEvidenceManifestCandidatePartition {
    /// Construct one canonical candidate partition.
    ///
    /// # Errors
    /// Rejects nil identity, empty/oversized membership, and duplicate
    /// obligation or assignment IDs.
    pub fn new(
        activity_id: ActivityId,
        manifest_id: EvaluationManifestId,
        manifest_revision: EvaluationRevision,
        mut members: Vec<MissionEvidenceManifestCandidateMember>,
    ) -> Result<Self, OperationsError> {
        if activity_id.into_uuid().is_nil()
            || manifest_id.into_uuid().is_nil()
            || manifest_revision.get() <= 0
            || members.is_empty()
            || members.len() > MAX_PREPARATION_MEMBERS
        {
            return Err(OperationsError::Bounds(
                "mission evidence candidate partition",
            ));
        }
        members.sort_by_key(|member| member.obligation_id());
        let mut obligations = BTreeSet::new();
        let mut assignments = BTreeSet::new();
        for member in &members {
            if !obligations.insert(member.obligation_id().into_uuid())
                || !assignments.insert(member.assignment_id().into_uuid())
            {
                return Err(OperationsError::Conflict(
                    "mission evidence candidate duplicate member",
                ));
            }
        }
        Ok(Self {
            activity_id,
            manifest_id,
            manifest_revision,
            members,
        })
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn manifest_id(&self) -> EvaluationManifestId {
        self.manifest_id
    }

    #[must_use]
    pub const fn manifest_revision(&self) -> EvaluationRevision {
        self.manifest_revision
    }

    pub fn members(&self) -> &[MissionEvidenceManifestCandidateMember] {
        &self.members
    }
}

/// Read-only preparation request for the exact started plan and complete source candidates.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceManifestPreparationCommand {
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    plan_revision: u64,
    backend_id: ForgeBackendId,
    partitions: Vec<MissionEvidenceManifestCandidatePartition>,
    authority: MissionEvaluationAuthority,
}

impl MissionEvidenceManifestPreparationCommand {
    /// Construct a bounded complete-candidate request.
    ///
    /// # Errors
    /// Rejects invalid identities, oversized or empty partitions, duplicate
    /// Activities/manifests/obligations/assignments, or unrepresentable plan revision.
    pub fn new(
        workspace_id: WorkspaceId,
        plan_id: MissionEvaluationPlanId,
        plan_revision: u64,
        backend_id: ForgeBackendId,
        mut partitions: Vec<MissionEvidenceManifestCandidatePartition>,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        if workspace_id.into_uuid().is_nil()
            || plan_id.into_uuid().is_nil()
            || plan_revision == 0
            || i64::try_from(plan_revision).is_err()
            || backend_id.into_uuid().is_nil()
            || partitions.is_empty()
            || partitions.len() > MAX_PREPARATION_PARTITIONS
        {
            return Err(OperationsError::Conflict(
                "mission evidence preparation identity",
            ));
        }
        partitions.sort_by_key(|partition| partition.activity_id().into_uuid());
        let mut activities = BTreeSet::new();
        let mut manifests = BTreeSet::new();
        let mut obligations = BTreeSet::new();
        let mut assignments = BTreeSet::new();
        let mut member_count = 0_usize;
        for partition in &partitions {
            if !activities.insert(partition.activity_id().into_uuid())
                || !manifests.insert(partition.manifest_id().into_uuid())
            {
                return Err(OperationsError::Conflict(
                    "mission evidence preparation duplicate partition",
                ));
            }
            member_count = member_count.checked_add(partition.members().len()).ok_or(
                OperationsError::Bounds("mission evidence preparation members"),
            )?;
            for member in partition.members() {
                if !obligations.insert(member.obligation_id().into_uuid())
                    || !assignments.insert(member.assignment_id().into_uuid())
                {
                    return Err(OperationsError::Conflict(
                        "mission evidence preparation duplicate source member",
                    ));
                }
            }
        }
        if member_count == 0 || member_count > MAX_PREPARATION_MEMBERS {
            return Err(OperationsError::Bounds(
                "mission evidence preparation members",
            ));
        }
        Ok(Self {
            workspace_id,
            plan_id,
            plan_revision,
            backend_id,
            partitions,
            authority,
        })
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

    pub const fn backend_id(&self) -> ForgeBackendId {
        self.backend_id
    }

    pub fn partitions(&self) -> &[MissionEvidenceManifestCandidatePartition] {
        &self.partitions
    }

    pub(crate) const fn authority(&self) -> MissionEvaluationAuthority {
        self.authority
    }
}

/// Read-only result; proposals are prepared input, never a Source proof.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidenceManifestPreparation {
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    plan_revision: u64,
    proposals: Vec<EvaluationManifestProposal>,
    pending_targets: Vec<MissionEvidencePreparationPendingTarget>,
}

/// Whether every candidate target has complete historical local identity proof.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionEvidenceManifestPreparationStatus {
    Ready,
    Pending,
}

impl MissionEvidenceManifestPreparation {
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

    /// Checked manifest proposals only. They are not registered source records.
    pub fn proposals(&self) -> &[EvaluationManifestProposal] {
        &self.proposals
    }

    pub fn pending_targets(&self) -> &[MissionEvidencePreparationPendingTarget] {
        &self.pending_targets
    }

    pub fn status(&self) -> MissionEvidenceManifestPreparationStatus {
        if self.pending_targets.is_empty() {
            MissionEvidenceManifestPreparationStatus::Ready
        } else {
            MissionEvidenceManifestPreparationStatus::Pending
        }
    }

    #[must_use]
    pub fn ready(&self) -> bool {
        self.pending_targets.is_empty()
    }
}

/// One historical target that is still unresolved during preparation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvidencePreparationPendingTarget {
    obligation_id: MissionObligationId,
    identity: MissionEvidenceIdentityResolution,
}

struct PreparationPlan<'a> {
    command: &'a MissionEvidenceManifestPreparationCommand,
    seal: &'a ImportHandoffSeal,
    source_freeze_revision: EvaluationRevision,
    source_digest: EvaluationDigest,
    plan_binding: EvaluationPlanBinding,
    deadline: TimestampMillis,
    obligations: BTreeMap<MissionObligationId, &'a crate::MissionObligationRecord>,
    criteria: BTreeMap<MissionObligationId, super::MissionEvidenceMemberCriteriaDigest>,
}

struct PreparedPartition {
    proposal: EvaluationManifestProposal,
    obligations: BTreeSet<MissionObligationId>,
    work_units: BTreeSet<crate::MissionWorkUnitId>,
    pending_targets: Vec<MissionEvidencePreparationPendingTarget>,
}

struct PreparedCandidate {
    declaration: EvaluationManifestMember,
    work_unit_id: crate::MissionWorkUnitId,
    pending_target: Option<MissionEvidencePreparationPendingTarget>,
}

impl MissionEvidencePreparationPendingTarget {
    pub const fn obligation_id(&self) -> MissionObligationId {
        self.obligation_id
    }

    pub fn identity(&self) -> &MissionEvidenceIdentityResolution {
        &self.identity
    }
}

impl OperationsCatalog {
    /// Prepare exact per-Activity Forge proposals from a frozen local plan and
    /// the current sealed import handoff, without creating Source state.
    ///
    /// # Errors
    /// Rejects stale/foreign plans, missing or changed source handoff, incomplete
    /// cohorts, foreign targets, and conflicting prior source declarations.
    pub fn prepare_mission_evidence_manifests(
        &self,
        command: &MissionEvidenceManifestPreparationCommand,
    ) -> Result<MissionEvidenceManifestPreparation, OperationsError> {
        prepare(self, command)
    }
}

fn prepare(
    catalog: &OperationsCatalog,
    command: &MissionEvidenceManifestPreparationCommand,
) -> Result<MissionEvidenceManifestPreparation, OperationsError> {
    let authorization_transaction = catalog.connection.unchecked_transaction()?;
    let authorization_actor = MissionActorSnapshot::resolve(
        &authorization_transaction,
        command.workspace_id(),
        command.authority(),
    )?;
    authorization_actor.authorize_current(
        &authorization_transaction,
        command.workspace_id(),
        command.authority(),
    )?;
    crate::catalog::commit_read(authorization_transaction, catalog.destination_id())?;

    let import_handoff = catalog
        .current_import_handoff(command.workspace_id().into_uuid(), catalog.destination_id())?;
    let progress = import_handoff.progress().ok_or(OperationsError::Conflict(
        "mission evidence source import missing",
    ))?;
    let seal = import_handoff
        .handoff_seal()
        .ok_or(OperationsError::Conflict(
            "mission evidence source handoff seal missing",
        ))?;
    validate_source_handoff(
        progress,
        seal,
        command.workspace_id(),
        catalog.destination_id(),
        command.backend_id(),
    )?;

    let transaction = catalog.connection.unchecked_transaction()?;
    let actor =
        MissionActorSnapshot::resolve(&transaction, command.workspace_id(), command.authority())?;
    actor.authorize_current(&transaction, command.workspace_id(), command.authority())?;
    let frozen = frozen_plan_in_transaction(
        &transaction,
        command.workspace_id(),
        command.plan_id(),
        command.plan_revision(),
    )?;
    let plan = build_preparation_plan(command, seal, &frozen)?;

    let mut seen_obligations = BTreeSet::new();
    let mut seen_units = BTreeSet::new();
    let mut proposals = Vec::with_capacity(command.partitions().len());
    let mut pending_targets = Vec::new();
    for partition in command.partitions() {
        let prepared = prepare_partition(&transaction, catalog.destination_id(), &plan, partition)?;
        if prepared
            .obligations
            .iter()
            .any(|value| !seen_obligations.insert(*value))
            || prepared
                .work_units
                .iter()
                .any(|value| !seen_units.insert(*value))
        {
            return Err(OperationsError::Conflict(
                "mission evidence preparation duplicate cohort member",
            ));
        }
        proposals.push(prepared.proposal);
        pending_targets.extend(prepared.pending_targets);
    }
    if seen_obligations.len() != plan.obligations.len()
        || seen_units.len() != plan.obligations.len()
    {
        return Err(OperationsError::Conflict(
            "mission evidence preparation incomplete cohort",
        ));
    }
    crate::catalog::commit_read(transaction, catalog.destination_id())?;
    Ok(MissionEvidenceManifestPreparation {
        workspace_id: command.workspace_id(),
        plan_id: command.plan_id(),
        plan_revision: command.plan_revision(),
        proposals,
        pending_targets,
    })
}

fn build_preparation_plan<'a>(
    command: &'a MissionEvidenceManifestPreparationCommand,
    seal: &'a ImportHandoffSeal,
    frozen: &'a crate::mission_evaluation::FrozenMissionEvaluationPlan,
) -> Result<PreparationPlan<'a>, OperationsError> {
    let plan_digests = super::digest::derive(&frozen.version, &frozen.obligations)?;
    let obligations = frozen
        .obligations
        .iter()
        .map(|obligation| (obligation.obligation_id(), obligation))
        .collect::<BTreeMap<_, _>>();
    let criteria = plan_digests
        .member_criteria_digests()
        .iter()
        .map(|value| (value.obligation_id(), *value))
        .collect::<BTreeMap<_, _>>();
    if obligations.len() != frozen.obligations.len() || criteria.len() != frozen.obligations.len() {
        return Err(OperationsError::Corrupt(
            "mission evidence preparation local cohort",
        ));
    }
    Ok(PreparationPlan {
        command,
        seal,
        source_freeze_revision: EvaluationRevision::new(
            i64::try_from(seal.source_revision())
                .map_err(|_| OperationsError::Bounds("mission evidence source revision"))?,
        )
        .map_err(|_| OperationsError::Corrupt("mission evidence source revision"))?,
        source_digest: EvaluationDigest::new(seal.verified_digest())
            .map_err(|_| OperationsError::Corrupt("mission evidence source digest"))?,
        plan_binding: EvaluationPlanBinding::new(
            EvaluationPlanId::new(command.plan_id().into_uuid())
                .map_err(|_| OperationsError::Conflict("mission evidence prepared plan"))?,
            EvaluationRevision::new(
                i64::try_from(command.plan_revision())
                    .map_err(|_| OperationsError::Bounds("mission evidence plan revision"))?,
            )
            .map_err(|_| OperationsError::Conflict("mission evidence prepared plan revision"))?,
            plan_digests.plan_digest(),
        ),
        deadline: TimestampMillis::new(frozen.version.t1_submission_cutoff_ms())
            .map_err(|_| OperationsError::Corrupt("mission evidence plan T1"))?,
        obligations,
        criteria,
    })
}

fn prepare_partition(
    transaction: &rusqlite::Transaction<'_>,
    destination_id: Uuid,
    plan: &PreparationPlan<'_>,
    partition: &MissionEvidenceManifestCandidatePartition,
) -> Result<PreparedPartition, OperationsError> {
    let command = plan.command;
    let source = EvaluationSourceIdentity::new(
        command.backend_id(),
        command.workspace_id(),
        partition.activity_id(),
        plan.seal.migration_id(),
        plan.seal.destination_id(),
        plan.source_freeze_revision,
        plan.source_digest,
    )
    .map_err(|_| OperationsError::Conflict("mission evidence prepared source identity"))?;
    let mut declarations = Vec::with_capacity(partition.members().len());
    let mut obligations = BTreeSet::new();
    let mut work_units = BTreeSet::new();
    let mut pending_targets = Vec::new();
    for candidate in partition.members() {
        if !obligations.insert(candidate.obligation_id()) {
            return Err(OperationsError::Conflict(
                "mission evidence preparation duplicate obligation",
            ));
        }
        let prepared = prepare_candidate_member(
            transaction,
            destination_id,
            command,
            partition,
            source,
            candidate,
            plan,
        )?;
        if !work_units.insert(prepared.work_unit_id) {
            return Err(OperationsError::Conflict(
                "mission evidence preparation duplicate work unit",
            ));
        }
        if let Some(pending) = prepared.pending_target {
            pending_targets.push(pending);
        }
        declarations.push(prepared.declaration);
    }
    let proposal = EvaluationManifestProposal::new(
        partition.manifest_id(),
        partition.manifest_revision(),
        source,
        plan.plan_binding,
        plan.deadline,
        declarations,
    )
    .map_err(|_| OperationsError::Conflict("mission evidence prepared manifest proposal"))?;
    Ok(PreparedPartition {
        proposal,
        obligations,
        work_units,
        pending_targets,
    })
}

fn prepare_candidate_member(
    transaction: &rusqlite::Transaction<'_>,
    destination_id: Uuid,
    command: &MissionEvidenceManifestPreparationCommand,
    partition: &MissionEvidenceManifestCandidatePartition,
    source: EvaluationSourceIdentity,
    candidate: &MissionEvidenceManifestCandidateMember,
    plan: &PreparationPlan<'_>,
) -> Result<PreparedCandidate, OperationsError> {
    let obligation = plan
        .obligations
        .get(&candidate.obligation_id())
        .copied()
        .ok_or(OperationsError::Conflict(
            "mission evidence preparation foreign obligation",
        ))?;
    let criteria_digest = plan
        .criteria
        .get(&candidate.obligation_id())
        .copied()
        .ok_or(OperationsError::Corrupt(
            "mission evidence preparation criteria digest",
        ))?;
    let declaration = EvaluationManifestMember::new(
        forge_domain::EvaluationWorkUnitId::new(obligation.work_unit_id().into_uuid())
            .map_err(|_| OperationsError::Corrupt("mission evidence work unit"))?,
        forge_domain::EvaluationObligationId::new(obligation.obligation_id().into_uuid())
            .map_err(|_| OperationsError::Corrupt("mission evidence obligation"))?,
        candidate.assignment_id(),
        candidate.assignment_revision(),
        candidate.target_agent_id(),
        candidate.target_session_id(),
        criteria_digest.digest(),
    )
    .map_err(|_| OperationsError::Conflict("mission evidence candidate declaration"))?;
    let expectation = super::history::SourceMemberHistoryExpectation {
        source,
        activity_id: partition.activity_id(),
        declaration,
        participation_id: obligation.participation_id(),
        weight: obligation.weight().get(),
        due_at_ms: obligation.due_at_ms(),
        criteria_digest: criteria_digest.digest(),
        expected_original_binding: None,
    };
    super::history::check_prior_source_member_history(transaction, &expectation)?;
    let target_identity = super::historical_identity::resolve(
        transaction,
        destination_id,
        command.workspace_id(),
        Some(partition.activity_id()),
        candidate.target_agent_id(),
        candidate.target_session_id(),
    )?;
    let pending_target = if super::store::target_identity_matches_participation(
        &target_identity,
        obligation.participation_id(),
    )? {
        None
    } else {
        Some(MissionEvidencePreparationPendingTarget {
            obligation_id: candidate.obligation_id(),
            identity: target_identity,
        })
    };
    Ok(PreparedCandidate {
        declaration,
        work_unit_id: obligation.work_unit_id(),
        pending_target,
    })
}

fn validate_source_handoff(
    progress: &crate::ImportProgress,
    seal: &ImportHandoffSeal,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    backend_id: ForgeBackendId,
) -> Result<(), OperationsError> {
    if progress.phase() != ImportPhase::Verified
        || progress.workspace_id() != workspace_id
        || progress.destination_id() != destination_id
        || progress.migration_id() != seal.migration_id()
        || progress.source_revision() != seal.source_revision()
        || progress.digest() != seal.verified_digest()
        || progress.page_count() != seal.page_count()
        || progress.record_count() != seal.record_count()
        || progress.verified_pages() != seal.verified_pages()
        || progress.verify_digest() != progress.digest()
        || seal.workspace_id() != workspace_id
        || seal.destination_id() != destination_id
        || seal.backend_id() != backend_id
    {
        return Err(OperationsError::Conflict(
            "mission evidence source handoff scope",
        ));
    }
    Ok(())
}
