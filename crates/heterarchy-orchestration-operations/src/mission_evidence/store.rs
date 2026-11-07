use std::collections::BTreeMap;

use forge_domain::{
    EvaluationAdmissionTiming, EvaluationDigest, EvaluationManifestId, EvaluationManifestMember,
    EvaluationOriginalBindingStatus, EvaluationPlanBinding, EvaluationPlanId,
    EvaluationRegisteredManifest, EvaluationRegisteredManifestMember, EvaluationReviewCut,
    EvaluationReviewCutId, EvaluationReviewCutMember, EvaluationReviewReceipt,
    EvaluationReviewState, EvaluationRevision, EvaluationSourceIdentity, EvaluationSourceSequence,
    EvaluationSubmissionReceipt, EvaluationT1CoverageCut, EvaluationT1CoverageMember,
    ForgeBackendId, TimestampMillis, WorkspaceId,
};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, params_from_iter,
    types::Value,
};
use uuid::Uuid;

use crate::{
    ImportPhase, MissionObligationRecord, OperationalActor, OperationsCatalog, OperationsError,
    WorkspaceParticipationId,
    catalog::ImportProgress,
    mission_evaluation::{MissionActorIdentity, MissionActorSnapshot, frozen_plan_in_transaction},
};

use super::model::{
    MissionEvidenceActorReceiptParts, MissionEvidenceBoundMemberParts,
    MissionEvidenceIdentityActor, MissionEvidenceIdentityProofParts,
    MissionEvidenceObservationMemberParts, MissionEvidenceParticipationProvenance,
    MissionEvidenceProfileProvenance, MissionEvidenceRegistrationProvenance,
    MissionEvidenceReplacementProvenance,
};
use super::model::{NewBindingParts, NewObservationParts, binding_from_parts};
use super::{
    MissionEvidenceActorKind, MissionEvidenceActorReceipt, MissionEvidenceBindingState,
    MissionEvidenceBoundMember, MissionEvidenceDisposition, MissionEvidenceIdentityProof,
    MissionEvidenceIdentityResolution, MissionEvidenceManifestBindingCommand,
    MissionEvidenceManifestBindingOutcome, MissionEvidenceManifestBindingRecord,
    MissionEvidenceMutationRecord, MissionEvidenceObservationBundle,
    MissionEvidenceObservationCommand, MissionEvidenceObservationMember,
    MissionEvidenceObservationOutcome, MissionEvidenceObservationRecord,
};

const BIND_OPERATION: i64 = 0;
const OBSERVE_OPERATION: i64 = 1;

impl OperationsCatalog {
    /// Bind one exact started plan to the configured Forge owner's registered manifest readback.
    ///
    /// # Errors
    /// Rejects foreign plan/source scope, stale authority, incomplete or changed
    /// source mappings, same-ID replay changes, local conflicts, corrupt history,
    /// and unconfirmed commits.
    pub fn bind_mission_evidence_manifest(
        &mut self,
        command: &MissionEvidenceManifestBindingCommand,
    ) -> Result<MissionEvidenceManifestBindingOutcome, OperationsError> {
        bind(self, command)
    }

    /// Append one immutable exact T1/review-cut observation for a bound plan.
    ///
    /// # Errors
    /// Rejects stale local heads, foreign or regressing source cuts, changed
    /// request replays, stale authority, corrupt identity history, and
    /// unconfirmed commits.
    pub fn record_mission_evidence_observation(
        &mut self,
        command: &MissionEvidenceObservationCommand,
    ) -> Result<MissionEvidenceObservationOutcome, OperationsError> {
        observe(self, command)
    }

    /// Read an immutable mutation receipt only for the current authenticated
    /// Ready actor that originally requested it.
    ///
    /// # Errors
    /// Rejects invalid/foreign Workspace scope, a missing Ready actor, a
    /// different logical actor, corrupt stored values, and SQLite failures.
    pub fn mission_evidence_request_for_actor(
        &self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<MissionEvidenceMutationRecord>, OperationsError> {
        read_request_for_actor(self, workspace_id, request_id, actor)
    }

    /// Read the canonical immutable binding for one exact Workspace plan revision.
    ///
    /// Runtime owns caller authorization; this bounded Operations read performs
    /// exact scope validation and does not require the original request actor.
    ///
    /// # Errors
    /// Rejects invalid identities, corrupt history, and storage failures.
    pub fn mission_evidence_binding(
        &self,
        workspace_id: WorkspaceId,
        plan_id: crate::MissionEvaluationPlanId,
        plan_revision: u64,
    ) -> Result<Option<MissionEvidenceManifestBindingRecord>, OperationsError> {
        if workspace_id.into_uuid().is_nil() || plan_revision == 0 {
            return Err(OperationsError::Conflict("mission evidence binding read"));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let request_id = binding_for_plan(&transaction, workspace_id, plan_id, plan_revision)?;
        let binding = request_id
            .map(|request_id| load_binding(&transaction, self.destination, request_id))
            .transpose()?
            .flatten();
        if binding.as_ref().is_some_and(|value| {
            value.workspace_id() != workspace_id
                || value.plan_id() != plan_id
                || value.plan_revision() != plan_revision
        }) {
            return Err(OperationsError::Corrupt(
                "mission evidence binding read scope",
            ));
        }
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(binding)
    }

    /// Read one canonical immutable observation by exact Workspace and request ID.
    ///
    /// Runtime owns caller authorization; this bounded Operations read does
    /// not require the original mutation actor.
    ///
    /// # Errors
    /// Rejects invalid/foreign identities, non-observation IDs, corrupt history,
    /// and storage failures.
    pub fn mission_evidence_observation(
        &self,
        workspace_id: WorkspaceId,
        observation_request_id: Uuid,
    ) -> Result<Option<MissionEvidenceObservationRecord>, OperationsError> {
        if workspace_id.into_uuid().is_nil() || observation_request_id.is_nil() {
            return Err(OperationsError::Conflict(
                "mission evidence observation read",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let Some(request) = load_request(&transaction, observation_request_id)? else {
            crate::catalog::commit_read(transaction, self.destination)?;
            return Ok(None);
        };
        if request.workspace_id != workspace_id || request.operation != OBSERVE_OPERATION {
            return Err(OperationsError::Conflict(
                "mission evidence observation read scope",
            ));
        }
        let observation = load_observation(&transaction, self.destination, observation_request_id)?
            .ok_or(OperationsError::Corrupt(
                "mission evidence observation readback",
            ))?;
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(Some(observation))
    }
}

fn bind(
    catalog: &mut OperationsCatalog,
    command: &MissionEvidenceManifestBindingCommand,
) -> Result<MissionEvidenceManifestBindingOutcome, OperationsError> {
    let workspace_id = command.workspace_id();
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let actor = MissionActorSnapshot::resolve(&transaction, workspace_id, command.authority())?;
    let payload_digest = super::fingerprint::binding(command, &actor.identity)?;

    if let Some(existing) = load_request(&transaction, command.request_id())? {
        require_replay(
            &existing,
            StoredRequestReplay {
                operation: BIND_OPERATION,
                workspace_id,
                plan_id: command.plan_id().into_uuid(),
                plan_revision: command.plan_revision(),
                expected_head_revision: 0,
                payload_digest,
            },
            &actor.identity,
        )?;
        let binding = load_binding(&transaction, catalog.destination, command.request_id())?
            .ok_or(OperationsError::Corrupt("mission evidence binding replay"))?;
        crate::catalog::commit_read(transaction, catalog.destination)?;
        return Ok(MissionEvidenceManifestBindingOutcome::new(binding, true));
    }

    actor.authorize_current(&transaction, workspace_id, command.authority())?;
    let record = prepare_binding(catalog.destination, &transaction, command, &actor)?;
    if binding_exists_for_plan(
        &transaction,
        workspace_id,
        record.plan_id(),
        record.plan_revision(),
    )? {
        return Err(OperationsError::Conflict(
            "mission evidence plan already bound",
        ));
    }
    check_prior_source_binding_history(&transaction, &record)?;
    insert_request(
        &transaction,
        StoredRequestInsert {
            operation: BIND_OPERATION,
            request_id: command.request_id(),
            workspace_id,
            plan_id: record.plan_id().into_uuid(),
            plan_revision: record.plan_revision(),
            expected_head_revision: 0,
            payload_digest,
            actor: &actor,
            recorded_at_ms: command.recorded_at_ms(),
        },
    )?;
    insert_binding(&transaction, &record)?;
    let stored = load_binding(&transaction, catalog.destination, command.request_id())?.ok_or(
        OperationsError::Corrupt("mission evidence binding readback"),
    )?;
    commit_write(transaction, workspace_id, command.request_id())?;
    Ok(MissionEvidenceManifestBindingOutcome::new(stored, false))
}

fn observe(
    catalog: &mut OperationsCatalog,
    command: &MissionEvidenceObservationCommand,
) -> Result<MissionEvidenceObservationOutcome, OperationsError> {
    let workspace_id = command.workspace_id();
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let actor = MissionActorSnapshot::resolve(&transaction, workspace_id, command.authority())?;
    let payload_digest = super::fingerprint::observation(command, &actor.identity)?;

    if let Some(existing) = load_request(&transaction, command.request_id())? {
        require_replay(
            &existing,
            StoredRequestReplay {
                operation: OBSERVE_OPERATION,
                workspace_id,
                plan_id: command.plan_id().into_uuid(),
                plan_revision: command.plan_revision(),
                expected_head_revision: command.expected_head_revision(),
                payload_digest,
            },
            &actor.identity,
        )?;
        let observation =
            load_observation(&transaction, catalog.destination, command.request_id())?.ok_or(
                OperationsError::Corrupt("mission evidence observation replay"),
            )?;
        crate::catalog::commit_read(transaction, catalog.destination)?;
        return Ok(MissionEvidenceObservationOutcome::new(observation, true));
    }

    actor.authorize_current(&transaction, workspace_id, command.authority())?;
    let stored = append_observation(
        &transaction,
        catalog.destination,
        command,
        &actor,
        payload_digest,
    )?;
    commit_write(transaction, workspace_id, command.request_id())?;
    Ok(MissionEvidenceObservationOutcome::new(stored, false))
}

fn append_observation(
    transaction: &Transaction<'_>,
    destination_id: Uuid,
    command: &MissionEvidenceObservationCommand,
    actor: &MissionActorSnapshot,
    payload_digest: [u8; 32],
) -> Result<MissionEvidenceObservationRecord, OperationsError> {
    let workspace_id = command.workspace_id();
    let binding_request_id = binding_for_plan(
        transaction,
        workspace_id,
        command.plan_id(),
        command.plan_revision(),
    )?
    .ok_or(OperationsError::Conflict(
        "mission evidence binding missing",
    ))?;
    let binding = load_binding(transaction, destination_id, binding_request_id)?.ok_or(
        OperationsError::Corrupt("mission evidence binding disappeared"),
    )?;
    validate_observation_source(transaction, destination_id, command, &binding)?;
    let frozen = frozen_plan_in_transaction(
        transaction,
        workspace_id,
        command.plan_id(),
        command.plan_revision(),
    )?;
    let head = load_head(transaction, binding_request_id, &binding)?;
    let head_revision = head.map_or(0, |value| value.revision);
    if head_revision != command.expected_head_revision() {
        return Err(OperationsError::Conflict(
            "mission evidence observation head revision",
        ));
    }
    if let Some(head) = head {
        let previous = load_observation(transaction, destination_id, head.observation_request_id)?
            .ok_or(OperationsError::Corrupt(
                "mission evidence head observation missing",
            ))?;
        validate_observation_head(&previous, command.cuts())?;
    }
    let next_revision = head_revision
        .checked_add(1)
        .filter(|value| i64::try_from(*value).is_ok())
        .ok_or(OperationsError::Bounds(
            "mission evidence observation revision",
        ))?;
    let evaluated = super::evaluate::evaluate(
        transaction,
        destination_id,
        &binding,
        &frozen.version,
        &frozen.obligations,
        command.cuts(),
    )?;
    insert_request(
        transaction,
        StoredRequestInsert {
            operation: OBSERVE_OPERATION,
            request_id: command.request_id(),
            workspace_id,
            plan_id: command.plan_id().into_uuid(),
            plan_revision: command.plan_revision(),
            expected_head_revision: command.expected_head_revision(),
            payload_digest,
            actor,
            recorded_at_ms: command.recorded_at_ms(),
        },
    )?;
    insert_observation(
        transaction,
        command,
        &binding,
        next_revision,
        &evaluated.members,
    )?;
    update_head(transaction, &binding, command, next_revision)?;
    let stored = load_observation(transaction, destination_id, command.request_id())?.ok_or(
        OperationsError::Corrupt("mission evidence observation readback"),
    )?;
    if stored.measures() != evaluated.measures.as_slice() {
        return Err(OperationsError::Corrupt(
            "mission evidence measures readback",
        ));
    }
    Ok(stored)
}

fn validate_observation_head(
    previous: &MissionEvidenceObservationRecord,
    current: &MissionEvidenceObservationBundle,
) -> Result<(), OperationsError> {
    let previous_partitions = previous.cuts().partitions();
    if previous_partitions.len() != current.partitions().len() {
        return Err(OperationsError::Corrupt(
            "mission evidence head partition set",
        ));
    }
    for (partition, prior) in current.partitions().iter().zip(previous_partitions) {
        if partition.activity_id() != prior.activity_id() || partition.t1_cut() != prior.t1_cut() {
            return Err(OperationsError::Conflict(
                "mission evidence source T1 partition changed",
            ));
        }
        let current_high_water = partition.review_cut().source_high_water().get();
        let prior_high_water = prior.review_cut().source_high_water().get();
        if current_high_water < prior_high_water {
            return Err(OperationsError::Conflict(
                "mission evidence source partition regression",
            ));
        }
        if current_high_water == prior_high_water && partition != prior {
            return Err(OperationsError::Conflict(
                "mission evidence equal high-water source payload changed",
            ));
        }
        if current_high_water > prior_high_water
            && partition.review_cut().review_cut_id() == prior.review_cut().review_cut_id()
            && partition.review_cut() != prior.review_cut()
        {
            return Err(OperationsError::Conflict(
                "mission evidence review cut ID reused",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservationHead {
    revision: u64,
    observation_request_id: Uuid,
}

fn load_head(
    connection: &Connection,
    binding_request_id: Uuid,
    binding: &MissionEvidenceManifestBindingRecord,
) -> Result<Option<ObservationHead>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),substr(plan_id,1,17),plan_revision,revision,substr(observation_request_id,1,17) FROM ops_mission_evidence_heads WHERE binding_request_id=?1 LIMIT 1",
            params![binding_request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((workspace, plan, plan_revision, revision, observation)) = raw else {
        return Ok(None);
    };
    if decode_workspace(workspace)? != binding.workspace_id()
        || decode_uuid(plan, "mission evidence head plan")? != binding.plan_id().into_uuid()
        || positive(plan_revision, "mission evidence head plan revision")?
            != binding.plan_revision()
    {
        return Err(OperationsError::Corrupt("mission evidence head scope"));
    }
    Ok(Some(ObservationHead {
        revision: positive(revision, "mission evidence head revision")?,
        observation_request_id: decode_uuid(observation, "mission evidence head observation")?,
    }))
}

fn update_head(
    transaction: &Transaction<'_>,
    binding: &MissionEvidenceManifestBindingRecord,
    command: &MissionEvidenceObservationCommand,
    revision: u64,
) -> Result<(), OperationsError> {
    let revision = signed(revision, "mission evidence head revision")?;
    let expected = signed(
        command.expected_head_revision(),
        "mission evidence expected head revision",
    )?;
    let plan_revision = signed(command.plan_revision(), "mission evidence plan revision")?;
    if expected == 0 {
        transaction.execute(
            "INSERT INTO ops_mission_evidence_heads(binding_request_id,workspace_id,plan_id,plan_revision,revision,observation_request_id) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                binding.request_id().as_bytes(),
                binding.workspace_id().into_uuid().as_bytes(),
                binding.plan_id().into_uuid().as_bytes(),
                plan_revision,
                revision,
                command.request_id().as_bytes(),
            ],
        )?;
    } else {
        let changed = transaction.execute(
            "UPDATE ops_mission_evidence_heads SET revision=?1,observation_request_id=?2 WHERE binding_request_id=?3 AND workspace_id=?4 AND plan_id=?5 AND plan_revision=?6 AND revision=?7",
            params![
                revision,
                command.request_id().as_bytes(),
                binding.request_id().as_bytes(),
                binding.workspace_id().into_uuid().as_bytes(),
                binding.plan_id().into_uuid().as_bytes(),
                plan_revision,
                expected,
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("mission evidence head CAS"));
        }
    }
    Ok(())
}

fn validate_observation_source(
    connection: &Connection,
    destination_id: Uuid,
    command: &MissionEvidenceObservationCommand,
    binding: &MissionEvidenceManifestBindingRecord,
) -> Result<(), OperationsError> {
    if binding.workspace_id() != command.workspace_id()
        || binding.plan_id() != command.plan_id()
        || binding.plan_revision() != command.plan_revision()
    {
        return Err(OperationsError::Conflict(
            "mission evidence observation scope",
        ));
    }
    let manifests = binding.manifest_bundle().manifests();
    let cuts = command.cuts().partitions();
    if manifests.len() != cuts.len() {
        return Err(OperationsError::Conflict(
            "mission evidence incomplete source cut bundle",
        ));
    }
    let common_source = manifests
        .first()
        .ok_or(OperationsError::Conflict(
            "mission evidence manifest bundle",
        ))?
        .proposal()
        .source();
    require_verified_source_import(
        connection,
        destination_id,
        command.workspace_id(),
        common_source,
    )?;
    for (manifest, partition) in manifests.iter().zip(cuts) {
        let proposal = manifest.proposal();
        let source = proposal.source();
        let t1 = partition.t1_cut();
        let review = partition.review_cut();
        if partition.activity_id() != source.activity_id()
            || t1.manifest_id() != proposal.manifest_id()
            || t1.manifest_revision() != proposal.revision()
            || t1.manifest_digest() != proposal.digest()
            || t1.deadline() != proposal.deadline()
            || t1.closed_at().get() <= proposal.deadline().get()
            || t1.source_high_water().get() < manifest.source_sequence().get()
            || review.t1_cut_id() != t1.cut_id()
            || review.source_high_water().get() <= t1.source_high_water().get()
            || t1.members().len() != manifest.members().len()
            || review.members().len() != manifest.members().len()
        {
            return Err(OperationsError::Conflict(
                "mission evidence source partition cut identity",
            ));
        }
    }
    Ok(())
}

fn insert_observation(
    transaction: &Transaction<'_>,
    command: &MissionEvidenceObservationCommand,
    binding: &MissionEvidenceManifestBindingRecord,
    revision: u64,
    members: &[MissionEvidenceObservationMember],
) -> Result<(), OperationsError> {
    let cuts = command.cuts().partitions();
    if members.len() != binding.members().len() {
        return Err(OperationsError::Corrupt(
            "mission evidence observation member count",
        ));
    }
    insert_observation_header(
        transaction,
        command,
        binding,
        revision,
        cuts.len(),
        members.len(),
    )?;
    for (ordinal, partition) in cuts.iter().enumerate() {
        insert_observation_partition(transaction, command, binding, ordinal, partition)?;
    }
    for member in members {
        insert_observation_member(transaction, command, binding, member)?;
    }
    Ok(())
}

fn insert_observation_header(
    transaction: &Transaction<'_>,
    command: &MissionEvidenceObservationCommand,
    binding: &MissionEvidenceManifestBindingRecord,
    revision: u64,
    partition_count: usize,
    member_count: usize,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_mission_evidence_observations(request_id,binding_request_id,workspace_id,mission_id,plan_id,plan_revision,revision,expected_head_revision,partition_count,member_count,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            command.request_id().as_bytes(),
            binding.request_id().as_bytes(),
            command.workspace_id().into_uuid().as_bytes(),
            binding.mission_id().into_uuid().as_bytes(),
            command.plan_id().into_uuid().as_bytes(),
            signed(command.plan_revision(), "mission evidence plan revision")?,
            signed(revision, "mission evidence observation revision")?,
            signed(command.expected_head_revision(), "mission evidence expected head")?,
            i64::try_from(partition_count)
                .map_err(|_| OperationsError::Bounds("mission evidence cut partition count"))?,
            i64::try_from(member_count)
                .map_err(|_| OperationsError::Bounds("mission evidence observation member count"))?,
            command.recorded_at_ms(),
        ],
    )?;
    Ok(())
}

fn insert_observation_partition(
    transaction: &Transaction<'_>,
    command: &MissionEvidenceObservationCommand,
    binding: &MissionEvidenceManifestBindingRecord,
    ordinal: usize,
    partition: &super::MissionEvidenceObservationPartition,
) -> Result<(), OperationsError> {
    let ordinal = i64::try_from(ordinal)
        .map_err(|_| OperationsError::Bounds("mission evidence observation partition"))?;
    let t1 = partition.t1_cut();
    let review = partition.review_cut();
    transaction.execute(
        "INSERT INTO ops_mission_evidence_observation_partitions(observation_request_id,binding_request_id,partition_ordinal,activity_id,manifest_id,manifest_revision,manifest_digest,t1_cut_id,deadline_ms,closed_at_ms,t1_source_high_water,review_cut_id,review_t1_cut_id,review_source_high_water,member_count) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            command.request_id().as_bytes(),
            binding.request_id().as_bytes(),
            ordinal,
            partition.activity_id().into_uuid().as_bytes(),
            t1.manifest_id().into_uuid().as_bytes(),
            t1.manifest_revision().get(),
            t1.manifest_digest().as_bytes(),
            t1.cut_id().into_uuid().as_bytes(),
            t1.deadline().get(),
            t1.closed_at().get(),
            t1.source_high_water().get(),
            review.review_cut_id().into_uuid().as_bytes(),
            review.t1_cut_id().into_uuid().as_bytes(),
            review.source_high_water().get(),
            i64::try_from(review.members().len())
                .map_err(|_| OperationsError::Bounds("mission evidence partition members"))?,
        ],
    )?;
    Ok(())
}

struct SubmissionColumns {
    present: i64,
    revision: Option<i64>,
    author_agent: Option<Vec<u8>>,
    author_session: Option<Vec<u8>>,
    original_binding_digest: Option<Vec<u8>>,
    criteria_digest: Option<Vec<u8>>,
    sequence: Option<i64>,
    timing_kind: Option<i64>,
    timing_at: Option<i64>,
    timing_uncertainty: Option<i64>,
    result_digest: Option<Vec<u8>>,
    sources_digest: Option<Vec<u8>>,
}

struct ReviewColumns {
    assignment: Option<Vec<u8>>,
    submitted_revision: Option<i64>,
    revision: Option<i64>,
    sequence: Option<i64>,
    outcome: Option<i64>,
    author_agent: Option<Vec<u8>>,
    author_session: Option<Vec<u8>>,
    reviewer_agent: Option<Vec<u8>>,
    reviewer_session: Option<Vec<u8>>,
    result_digest: Option<Vec<u8>>,
}

struct EncodedObservationMember {
    coverage: super::codec::CoverageParts,
    review_state: super::codec::ReviewStateParts,
    submission: SubmissionColumns,
    review: ReviewColumns,
    disposition: i64,
    unresolved_reason: Option<i64>,
}

fn insert_observation_member(
    transaction: &Transaction<'_>,
    command: &MissionEvidenceObservationCommand,
    binding: &MissionEvidenceManifestBindingRecord,
    member: &MissionEvidenceObservationMember,
) -> Result<(), OperationsError> {
    let partition_ordinal = usize::try_from(member.partition_ordinal())
        .map_err(|_| OperationsError::Bounds("mission evidence member partition"))?;
    let partition =
        command
            .cuts()
            .partitions()
            .get(partition_ordinal)
            .ok_or(OperationsError::Corrupt(
                "mission evidence member partition",
            ))?;
    if partition.activity_id() != member.source_activity_id() {
        return Err(OperationsError::Corrupt(
            "mission evidence observation member Activity",
        ));
    }
    let encoded = encode_observation_member(member);
    insert_observation_member_row(transaction, command, binding, member, encoded)?;
    let obligation_id = member.source().member().obligation_id().into_uuid();
    insert_identity_proof(
        transaction,
        command.request_id(),
        0,
        obligation_id,
        member.target_identity(),
    )?;
    if let Some(identity) = member.author_identity() {
        insert_identity_proof(
            transaction,
            command.request_id(),
            1,
            obligation_id,
            identity,
        )?;
    }
    if let Some(identity) = member.reviewer_identity() {
        insert_identity_proof(
            transaction,
            command.request_id(),
            2,
            obligation_id,
            identity,
        )?;
    }
    Ok(())
}

fn encode_observation_member(
    member: &MissionEvidenceObservationMember,
) -> EncodedObservationMember {
    let source = member.source();
    let (disposition, unresolved_reason) = match member.disposition() {
        MissionEvidenceDisposition::AcceptedOriginal => (0, None),
        MissionEvidenceDisposition::Unresolved(reason) => {
            (1, Some(super::codec::encode_unresolved_reason(reason)))
        }
    };
    EncodedObservationMember {
        coverage: super::codec::encode_coverage(source.coverage()),
        review_state: super::codec::encode_review_state(source.review()),
        submission: encode_submission_columns(source.submission()),
        review: encode_review_columns(source.review()),
        disposition,
        unresolved_reason,
    }
}

fn encode_submission_columns(submission: Option<EvaluationSubmissionReceipt>) -> SubmissionColumns {
    let Some(receipt) = submission else {
        return SubmissionColumns {
            present: 0,
            revision: None,
            author_agent: None,
            author_session: None,
            original_binding_digest: None,
            criteria_digest: None,
            sequence: None,
            timing_kind: None,
            timing_at: None,
            timing_uncertainty: None,
            result_digest: None,
            sources_digest: None,
        };
    };
    let (timing_kind, timing_at, timing_uncertainty) =
        super::codec::encode_timing(receipt.timing());
    SubmissionColumns {
        present: 1,
        revision: Some(receipt.submitted_revision().get()),
        author_agent: Some(receipt.author_agent_id().into_uuid().into_bytes().to_vec()),
        author_session: Some(
            receipt
                .author_session_id()
                .into_uuid()
                .into_bytes()
                .to_vec(),
        ),
        original_binding_digest: receipt
            .original_binding_digest()
            .map(|value| value.as_bytes().to_vec()),
        criteria_digest: receipt
            .criteria_digest()
            .map(|value| value.as_bytes().to_vec()),
        sequence: Some(receipt.source_sequence().get()),
        timing_kind: Some(timing_kind),
        timing_at,
        timing_uncertainty,
        result_digest: Some(receipt.result_digest().as_bytes().to_vec()),
        sources_digest: receipt
            .sources_digest()
            .map(|value| value.as_bytes().to_vec()),
    }
}

fn encode_review_columns(review: EvaluationReviewState) -> ReviewColumns {
    match review {
        EvaluationReviewState::Reviewed(receipt) => ReviewColumns {
            assignment: Some(receipt.assignment_id().into_uuid().into_bytes().to_vec()),
            submitted_revision: Some(receipt.submitted_revision().get()),
            revision: Some(receipt.review_revision().get()),
            sequence: Some(receipt.source_sequence().get()),
            outcome: Some(super::codec::encode_review_outcome(receipt.outcome())),
            author_agent: Some(receipt.author_agent_id().into_uuid().into_bytes().to_vec()),
            author_session: Some(
                receipt
                    .author_session_id()
                    .into_uuid()
                    .into_bytes()
                    .to_vec(),
            ),
            reviewer_agent: Some(
                receipt
                    .reviewer_agent_id()
                    .into_uuid()
                    .into_bytes()
                    .to_vec(),
            ),
            reviewer_session: Some(
                receipt
                    .reviewer_session_id()
                    .into_uuid()
                    .into_bytes()
                    .to_vec(),
            ),
            result_digest: Some(receipt.result_digest().as_bytes().to_vec()),
        },
        EvaluationReviewState::NotApplicable
        | EvaluationReviewState::Pending { .. }
        | EvaluationReviewState::Unknown(_) => ReviewColumns {
            assignment: None,
            submitted_revision: None,
            revision: None,
            sequence: None,
            outcome: None,
            author_agent: None,
            author_session: None,
            reviewer_agent: None,
            reviewer_session: None,
            result_digest: None,
        },
    }
}

fn insert_observation_member_row(
    transaction: &Transaction<'_>,
    command: &MissionEvidenceObservationCommand,
    binding: &MissionEvidenceManifestBindingRecord,
    member: &MissionEvidenceObservationMember,
    encoded: EncodedObservationMember,
) -> Result<(), OperationsError> {
    let source = member.source();
    let EncodedObservationMember {
        coverage,
        review_state,
        submission,
        review,
        disposition,
        unresolved_reason,
    } = encoded;
    transaction.execute(
        "INSERT INTO ops_mission_evidence_observation_members(observation_request_id,binding_request_id,partition_ordinal,obligation_id,participation_id,weight,due_at_ms,assignment_id,coverage_state,coverage_unknown_reason,eligible_submitted_revision,eligible_receipt_sequence,admitted_at_ms,eligible_result_digest,eligible_sources_digest,submission_present,submission_revision,author_agent_id,author_session_id,original_binding_digest,submission_criteria_digest,submission_sequence,submission_timing_kind,submission_timing_ms,submission_clock_uncertainty,submission_result_digest,submission_sources_digest,review_state,review_unknown_reason,pending_review_submitted_revision,pending_review_receipt_sequence,review_assignment_id,review_submitted_revision,review_revision,review_sequence,review_outcome,review_author_agent_id,review_author_session_id,reviewer_agent_id,reviewer_session_id,review_result_digest,disposition,unresolved_reason) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,?39,?40,?41,?42,?43)",
        params![
            command.request_id().as_bytes(),
            binding.request_id().as_bytes(),
            i64::from(member.partition_ordinal()),
            source.member().obligation_id().into_uuid().as_bytes(),
            member.participation_id().into_uuid().as_bytes(),
            signed(member.weight(), "mission evidence member weight")?,
            member.due_at_ms(),
            source.member().assignment_id().into_uuid().as_bytes(),
            coverage.state,
            coverage.unknown_reason,
            coverage.submitted_revision,
            coverage.receipt_sequence,
            coverage.admitted_at_ms,
            coverage.result_digest.as_ref().map(<[u8; 32]>::as_slice),
            coverage.sources_digest.as_ref().map(<[u8; 32]>::as_slice),
            submission.present,
            submission.revision,
            submission.author_agent,
            submission.author_session,
            submission.original_binding_digest,
            submission.criteria_digest,
            submission.sequence,
            submission.timing_kind,
            submission.timing_at,
            submission.timing_uncertainty,
            submission.result_digest,
            submission.sources_digest,
            review_state.state,
            review_state.unknown_reason,
            review_state.pending_submitted_revision,
            review_state.pending_receipt_sequence,
            review.assignment,
            review.submitted_revision,
            review.revision,
            review.sequence,
            review.outcome,
            review.author_agent,
            review.author_session,
            review.reviewer_agent,
            review.reviewer_session,
            review.result_digest,
            disposition,
            unresolved_reason,
        ],
    )?;
    Ok(())
}

#[derive(Debug)]
struct RawObservation {
    binding_request_id: Vec<u8>,
    workspace_id: Vec<u8>,
    mission_id: Vec<u8>,
    plan_id: Vec<u8>,
    plan_revision: i64,
    revision: i64,
    expected_head_revision: i64,
    partition_count: i64,
    member_count: i64,
    recorded_at_ms: i64,
}

#[derive(Debug)]
struct RawObservationPartition {
    ordinal: i64,
    activity_id: Vec<u8>,
    manifest_id: Vec<u8>,
    manifest_revision: i64,
    manifest_digest: Vec<u8>,
    t1_cut_id: Vec<u8>,
    deadline_ms: i64,
    closed_at_ms: i64,
    t1_source_high_water: i64,
    review_cut_id: Vec<u8>,
    review_t1_cut_id: Vec<u8>,
    review_source_high_water: i64,
    member_count: i64,
}

#[derive(Debug)]
struct RawObservationMember {
    partition_ordinal: i64,
    ordinal: i64,
    binding_request_id: Vec<u8>,
    obligation_id: Vec<u8>,
    participation_id: Vec<u8>,
    weight: i64,
    due_at_ms: i64,
    assignment_id: Vec<u8>,
    coverage_state: i64,
    coverage_unknown_reason: Option<i64>,
    eligible_submitted_revision: Option<i64>,
    eligible_receipt_sequence: Option<i64>,
    admitted_at_ms: Option<i64>,
    eligible_result_digest: Option<Vec<u8>>,
    eligible_sources_digest: Option<Vec<u8>>,
    submission_present: i64,
    submission_revision: Option<i64>,
    author_agent_id: Option<Vec<u8>>,
    author_session_id: Option<Vec<u8>>,
    original_binding_digest: Option<Vec<u8>>,
    submission_criteria_digest: Option<Vec<u8>>,
    submission_sequence: Option<i64>,
    submission_timing_kind: Option<i64>,
    submission_timing_ms: Option<i64>,
    submission_clock_uncertainty: Option<i64>,
    submission_result_digest: Option<Vec<u8>>,
    submission_sources_digest: Option<Vec<u8>>,
    review_state: i64,
    review_unknown_reason: Option<i64>,
    pending_review_submitted_revision: Option<i64>,
    pending_review_receipt_sequence: Option<i64>,
    review_assignment_id: Option<Vec<u8>>,
    review_submitted_revision: Option<i64>,
    review_revision: Option<i64>,
    review_sequence: Option<i64>,
    review_outcome: Option<i64>,
    review_author_agent_id: Option<Vec<u8>>,
    review_author_session_id: Option<Vec<u8>>,
    reviewer_agent_id: Option<Vec<u8>>,
    reviewer_session_id: Option<Vec<u8>>,
    review_result_digest: Option<Vec<u8>>,
    disposition: i64,
    unresolved_reason: Option<i64>,
}

fn load_observation(
    connection: &Connection,
    destination_id: Uuid,
    request_id: Uuid,
) -> Result<Option<MissionEvidenceObservationRecord>, OperationsError> {
    let Some(request) = load_request(connection, request_id)? else {
        return Ok(None);
    };
    if request.operation != OBSERVE_OPERATION {
        return Err(OperationsError::Corrupt(
            "mission evidence observation request",
        ));
    }
    let raw = connection
        .query_row(
            "SELECT substr(binding_request_id,1,17),substr(workspace_id,1,17),substr(mission_id,1,17),substr(plan_id,1,17),plan_revision,revision,expected_head_revision,partition_count,member_count,recorded_at_ms FROM ops_mission_evidence_observations WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawObservation {
                    binding_request_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    mission_id: row.get(2)?,
                    plan_id: row.get(3)?,
                    plan_revision: row.get(4)?,
                    revision: row.get(5)?,
                    expected_head_revision: row.get(6)?,
                    partition_count: row.get(7)?,
                    member_count: row.get(8)?,
                    recorded_at_ms: row.get(9)?,
                })
            },
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    decode_observation(connection, destination_id, request_id, request, raw).map(Some)
}

fn load_raw_observation_partitions(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Vec<RawObservationPartition>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT partition_ordinal,substr(activity_id,1,17),substr(manifest_id,1,17),manifest_revision,substr(manifest_digest,1,33),substr(t1_cut_id,1,17),deadline_ms,closed_at_ms,t1_source_high_water,substr(review_cut_id,1,17),substr(review_t1_cut_id,1,17),review_source_high_water,member_count FROM ops_mission_evidence_observation_partitions WHERE observation_request_id=?1 ORDER BY partition_ordinal LIMIT 257",
    )?;
    let rows = statement.query_map(params![request_id.as_bytes()], |row| {
        Ok(RawObservationPartition {
            ordinal: row.get(0)?,
            activity_id: row.get(1)?,
            manifest_id: row.get(2)?,
            manifest_revision: row.get(3)?,
            manifest_digest: row.get(4)?,
            t1_cut_id: row.get(5)?,
            deadline_ms: row.get(6)?,
            closed_at_ms: row.get(7)?,
            t1_source_high_water: row.get(8)?,
            review_cut_id: row.get(9)?,
            review_t1_cut_id: row.get(10)?,
            review_source_high_water: row.get(11)?,
            member_count: row.get(12)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_raw_observation_members(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Vec<RawObservationMember>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT m.partition_ordinal,b.ordinal,m.binding_request_id,substr(m.obligation_id,1,17),substr(m.participation_id,1,17),m.weight,m.due_at_ms,substr(m.assignment_id,1,17),m.coverage_state,m.coverage_unknown_reason,m.eligible_submitted_revision,m.eligible_receipt_sequence,m.admitted_at_ms,substr(m.eligible_result_digest,1,33),substr(m.eligible_sources_digest,1,33),m.submission_present,m.submission_revision,substr(m.author_agent_id,1,17),substr(m.author_session_id,1,17),substr(m.original_binding_digest,1,33),substr(m.submission_criteria_digest,1,33),m.submission_sequence,m.submission_timing_kind,m.submission_timing_ms,m.submission_clock_uncertainty,substr(m.submission_result_digest,1,33),substr(m.submission_sources_digest,1,33),m.review_state,m.review_unknown_reason,m.pending_review_submitted_revision,m.pending_review_receipt_sequence,substr(m.review_assignment_id,1,17),m.review_submitted_revision,m.review_revision,m.review_sequence,m.review_outcome,substr(m.review_author_agent_id,1,17),substr(m.review_author_session_id,1,17),substr(m.reviewer_agent_id,1,17),substr(m.reviewer_session_id,1,17),substr(m.review_result_digest,1,33),m.disposition,m.unresolved_reason FROM ops_mission_evidence_observation_members m JOIN ops_mission_evidence_observations o ON o.request_id=m.observation_request_id AND o.binding_request_id=m.binding_request_id JOIN ops_mission_evidence_binding_members b ON b.binding_request_id=m.binding_request_id AND b.partition_ordinal=m.partition_ordinal AND b.obligation_id=m.obligation_id WHERE m.observation_request_id=?1 ORDER BY m.partition_ordinal,b.ordinal LIMIT 257",
    )?;
    let rows = statement.query_map(params![request_id.as_bytes()], |row| {
        Ok(RawObservationMember {
            partition_ordinal: row.get(0)?,
            ordinal: row.get(1)?,
            binding_request_id: row.get(2)?,
            obligation_id: row.get(3)?,
            participation_id: row.get(4)?,
            weight: row.get(5)?,
            due_at_ms: row.get(6)?,
            assignment_id: row.get(7)?,
            coverage_state: row.get(8)?,
            coverage_unknown_reason: row.get(9)?,
            eligible_submitted_revision: row.get(10)?,
            eligible_receipt_sequence: row.get(11)?,
            admitted_at_ms: row.get(12)?,
            eligible_result_digest: row.get(13)?,
            eligible_sources_digest: row.get(14)?,
            submission_present: row.get(15)?,
            submission_revision: row.get(16)?,
            author_agent_id: row.get(17)?,
            author_session_id: row.get(18)?,
            original_binding_digest: row.get(19)?,
            submission_criteria_digest: row.get(20)?,
            submission_sequence: row.get(21)?,
            submission_timing_kind: row.get(22)?,
            submission_timing_ms: row.get(23)?,
            submission_clock_uncertainty: row.get(24)?,
            submission_result_digest: row.get(25)?,
            submission_sources_digest: row.get(26)?,
            review_state: row.get(27)?,
            review_unknown_reason: row.get(28)?,
            pending_review_submitted_revision: row.get(29)?,
            pending_review_receipt_sequence: row.get(30)?,
            review_assignment_id: row.get(31)?,
            review_submitted_revision: row.get(32)?,
            review_revision: row.get(33)?,
            review_sequence: row.get(34)?,
            review_outcome: row.get(35)?,
            review_author_agent_id: row.get(36)?,
            review_author_session_id: row.get(37)?,
            reviewer_agent_id: row.get(38)?,
            reviewer_session_id: row.get(39)?,
            review_result_digest: row.get(40)?,
            disposition: row.get(41)?,
            unresolved_reason: row.get(42)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

struct ObservationDecodeMetadata {
    binding_request_id: Uuid,
    workspace_id: WorkspaceId,
    mission_id: crate::MissionId,
    plan_id: crate::MissionEvaluationPlanId,
    plan_revision: u64,
    revision: u64,
    expected_head_revision: u64,
    partition_count: usize,
    member_count: usize,
    recorded_at_ms: i64,
}

fn decode_observation_metadata(
    request: &StoredRequest,
    raw: RawObservation,
) -> Result<ObservationDecodeMetadata, OperationsError> {
    let binding_request_id =
        decode_uuid(raw.binding_request_id, "mission evidence binding request")?;
    let workspace_id = decode_workspace(raw.workspace_id)?;
    let mission_id =
        crate::MissionId::from_uuid(decode_uuid(raw.mission_id, "mission evidence Mission")?)?;
    let plan_id = crate::MissionEvaluationPlanId::from_uuid(decode_uuid(
        raw.plan_id,
        "mission evidence plan",
    )?)?;
    let plan_revision = positive(raw.plan_revision, "mission evidence plan revision")?;
    let revision = positive(raw.revision, "mission evidence observation revision")?;
    let expected_head_revision =
        nonnegative(raw.expected_head_revision, "mission evidence expected head")?;
    let partition_count = usize::try_from(positive(
        raw.partition_count,
        "mission evidence observation partition count",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evidence observation partition count"))?;
    let member_count = usize::try_from(positive(
        raw.member_count,
        "mission evidence observation member count",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evidence observation member count"))?;
    let recorded_at_ms = nonnegative_i64(raw.recorded_at_ms, "mission evidence observation time")?;
    if request.operation != OBSERVE_OPERATION
        || request.workspace_id != workspace_id
        || request.plan_id != plan_id
        || request.plan_revision != plan_revision
        || request.expected_head_revision != expected_head_revision
        || request.recorded_at_ms != recorded_at_ms
        || revision
            != expected_head_revision
                .checked_add(1)
                .ok_or(OperationsError::Corrupt(
                    "mission evidence observation revision",
                ))?
    {
        return Err(OperationsError::Corrupt(
            "mission evidence observation scope",
        ));
    }
    Ok(ObservationDecodeMetadata {
        binding_request_id,
        workspace_id,
        mission_id,
        plan_id,
        plan_revision,
        revision,
        expected_head_revision,
        partition_count,
        member_count,
        recorded_at_ms,
    })
}

fn decode_observation(
    connection: &Connection,
    destination_id: Uuid,
    request_id: Uuid,
    request: StoredRequest,
    raw: RawObservation,
) -> Result<MissionEvidenceObservationRecord, OperationsError> {
    let metadata = decode_observation_metadata(&request, raw)?;
    let ObservationDecodeMetadata {
        binding_request_id,
        workspace_id,
        mission_id,
        plan_id,
        plan_revision,
        revision,
        expected_head_revision,
        partition_count,
        member_count,
        recorded_at_ms,
    } = metadata;
    let binding = load_observation_binding(
        connection,
        destination_id,
        binding_request_id,
        workspace_id,
        mission_id,
        plan_id,
        plan_revision,
    )?;
    let mut raw_partitions = load_raw_observation_partitions(connection, request_id)?;
    let raw_members = load_raw_observation_members(connection, request_id)?;
    if raw_partitions.len() != partition_count
        || partition_count != binding.manifest_bundle().manifests().len()
        || raw_members.len() != member_count
        || member_count != binding.members().len()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence observation partition or member count",
        ));
    }
    raw_partitions.sort_by_key(|partition| partition.ordinal);
    let context = ObservationDecodeContext {
        connection,
        request_id,
        binding_request_id,
        binding: &binding,
    };
    let mut cuts = Vec::with_capacity(partition_count);
    let mut observations = Vec::with_capacity(member_count);
    let mut member_offset = 0_usize;
    for (partition_ordinal, raw_partition) in raw_partitions.into_iter().enumerate() {
        let partition_member_count = usize::try_from(positive(
            raw_partition.member_count,
            "mission evidence observation partition member count",
        )?)
        .map_err(|_| {
            OperationsError::Corrupt("mission evidence observation partition member count")
        })?;
        let end = member_offset
            .checked_add(partition_member_count)
            .filter(|end| *end <= raw_members.len())
            .ok_or(OperationsError::Corrupt(
                "mission evidence observation partition member count",
            ))?;
        let decoded = decode_observation_partition(
            &context,
            partition_ordinal,
            raw_partition,
            &raw_members[member_offset..end],
        )?;
        cuts.push(decoded.partition);
        observations.extend(decoded.members);
        member_offset = end;
    }
    if member_offset != raw_members.len() {
        return Err(OperationsError::Corrupt(
            "mission evidence unpartitioned observation members",
        ));
    }
    let cuts = super::MissionEvidenceObservationBundle::new(cuts)?;
    let measures =
        super::evaluate::calculate_measures(&observations, binding.t1_submission_cutoff_ms())?;
    Ok(NewObservationParts {
        request_id,
        binding_request_id,
        workspace_id,
        plan_id,
        plan_revision,
        revision,
        expected_head_revision,
        cuts,
        members: observations,
        measures,
        actor: request.actor_receipt,
        recorded_at_ms,
    }
    .into())
}

fn load_observation_binding(
    connection: &Connection,
    destination_id: Uuid,
    binding_request_id: Uuid,
    workspace_id: WorkspaceId,
    mission_id: crate::MissionId,
    plan_id: crate::MissionEvaluationPlanId,
    plan_revision: u64,
) -> Result<MissionEvidenceManifestBindingRecord, OperationsError> {
    let binding = load_binding(connection, destination_id, binding_request_id)?.ok_or(
        OperationsError::Corrupt("mission evidence observation binding"),
    )?;
    if binding.request_id() != binding_request_id
        || binding.workspace_id() != workspace_id
        || binding.mission_id() != mission_id
        || binding.plan_id() != plan_id
        || binding.plan_revision() != plan_revision
    {
        return Err(OperationsError::Corrupt(
            "mission evidence observation binding scope",
        ));
    }
    Ok(binding)
}

struct ObservationDecodeContext<'a> {
    connection: &'a Connection,
    request_id: Uuid,
    binding_request_id: Uuid,
    binding: &'a MissionEvidenceManifestBindingRecord,
}

struct DecodedObservationSource {
    t1_member: EvaluationT1CoverageMember,
    review_member: EvaluationReviewCutMember,
}

struct DecodedObservationPartition {
    partition: super::MissionEvidenceObservationPartition,
    members: Vec<MissionEvidenceObservationMember>,
}

fn decode_observation_partition(
    context: &ObservationDecodeContext<'_>,
    partition_ordinal: usize,
    raw_partition: RawObservationPartition,
    raw_members: &[RawObservationMember],
) -> Result<DecodedObservationPartition, OperationsError> {
    if raw_partition.ordinal
        != i64::try_from(partition_ordinal)
            .map_err(|_| OperationsError::Corrupt("mission evidence observation partition"))?
    {
        return Err(OperationsError::Corrupt(
            "mission evidence observation partition ordinal",
        ));
    }
    let activity_id = forge_domain::ActivityId::from_uuid(decode_uuid(
        raw_partition.activity_id.clone(),
        "mission evidence observation Activity",
    )?);
    let manifest = context
        .binding
        .manifest_bundle()
        .manifests()
        .get(partition_ordinal)
        .ok_or(OperationsError::Corrupt(
            "mission evidence observation manifest partition",
        ))?;
    let proposal = manifest.proposal();
    if activity_id != proposal.source().activity_id()
        || EvaluationManifestId::new(decode_uuid(
            raw_partition.manifest_id.clone(),
            "mission evidence observation manifest",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evidence observation manifest"))?
            != proposal.manifest_id()
        || EvaluationRevision::new(raw_partition.manifest_revision).map_err(|_| {
            OperationsError::Corrupt("mission evidence observation manifest revision")
        })? != proposal.revision()
        || decode_evaluation_digest(
            raw_partition.manifest_digest.clone(),
            "mission evidence observation manifest digest",
        )? != proposal.digest()
        || raw_partition.review_t1_cut_id.as_slice() != raw_partition.t1_cut_id.as_slice()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence observation source partition",
        ));
    }
    let bound_members = context
        .binding
        .members()
        .iter()
        .filter(|member| member.partition_ordinal() as usize == partition_ordinal)
        .collect::<Vec<_>>();
    if raw_members.len() != manifest.members().len()
        || bound_members.len() != manifest.members().len()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence observation partition member count",
        ));
    }
    let mut t1_members = Vec::with_capacity(raw_members.len());
    let mut review_members = Vec::with_capacity(raw_members.len());
    let mut observations = Vec::with_capacity(raw_members.len());
    for (ordinal, (raw_member, bound)) in raw_members.iter().zip(bound_members).enumerate() {
        let decoded = decode_observation_member(
            context,
            partition_ordinal,
            ordinal,
            activity_id,
            manifest,
            raw_member,
            bound,
        )?;
        t1_members.push(decoded.source.t1_member);
        review_members.push(decoded.source.review_member);
        observations.push(decoded.observation);
    }
    let (t1_cut, review_cut) =
        decode_observation_cuts(raw_partition, proposal, t1_members, review_members)?;
    validate_observation_partition_source_members(
        context,
        partition_ordinal,
        &t1_cut,
        &review_cut,
    )?;
    Ok(DecodedObservationPartition {
        partition: super::MissionEvidenceObservationPartition::new(
            activity_id,
            t1_cut,
            review_cut,
        )?,
        members: observations,
    })
}

fn validate_observation_partition_source_members(
    context: &ObservationDecodeContext<'_>,
    partition_ordinal: usize,
    t1_cut: &EvaluationT1CoverageCut,
    review_cut: &EvaluationReviewCut,
) -> Result<(), OperationsError> {
    for bound in context
        .binding
        .members()
        .iter()
        .filter(|member| member.partition_ordinal() as usize == partition_ordinal)
    {
        let source = review_cut
            .members()
            .iter()
            .find(|member| member.member() == bound.declaration())
            .ok_or(OperationsError::Corrupt(
                "mission evidence observation cut member",
            ))?;
        super::evaluate::validate_member_source(bound, source, t1_cut, review_cut)?;
    }
    Ok(())
}

struct DecodedObservationMember {
    source: DecodedObservationSource,
    observation: MissionEvidenceObservationMember,
}

fn decode_observation_member(
    context: &ObservationDecodeContext<'_>,
    partition_ordinal: usize,
    member_ordinal: usize,
    activity_id: forge_domain::ActivityId,
    manifest: &EvaluationRegisteredManifest,
    raw_member: &RawObservationMember,
    bound: &MissionEvidenceBoundMember,
) -> Result<DecodedObservationMember, OperationsError> {
    validate_observation_member_scope(
        context,
        partition_ordinal,
        member_ordinal,
        raw_member,
        bound,
    )?;
    let source = decode_observation_source(raw_member, bound)?;
    let (target_identity, author_identity, reviewer_identity) =
        decode_observation_identities(context, bound, &source.review_member)?;
    let disposition = decode_disposition(raw_member.disposition, raw_member.unresolved_reason)?;
    let computed_disposition = super::evaluate::disposition(
        manifest,
        bound,
        &target_identity,
        &source.review_member,
        author_identity.as_ref(),
        reviewer_identity.as_ref(),
    )?;
    if disposition != computed_disposition {
        return Err(OperationsError::Corrupt("mission evidence disposition"));
    }
    let observation =
        MissionEvidenceObservationMember::from_parts(MissionEvidenceObservationMemberParts {
            partition_ordinal: u32::try_from(partition_ordinal)
                .map_err(|_| OperationsError::Bounds("mission evidence partition ordinal"))?,
            source_activity_id: activity_id,
            source: source.review_member.clone(),
            participation_id: bound.participation_id(),
            weight: bound.weight(),
            due_at_ms: bound.due_at_ms(),
            target_identity,
            author_identity,
            reviewer_identity,
            disposition,
        })?;
    Ok(DecodedObservationMember {
        source,
        observation,
    })
}

fn validate_observation_member_scope(
    context: &ObservationDecodeContext<'_>,
    partition_ordinal: usize,
    member_ordinal: usize,
    raw_member: &RawObservationMember,
    bound: &MissionEvidenceBoundMember,
) -> Result<(), OperationsError> {
    if raw_member.partition_ordinal
        != i64::try_from(partition_ordinal)
            .map_err(|_| OperationsError::Corrupt("mission evidence observation partition"))?
        || raw_member.ordinal
            != i64::try_from(member_ordinal)
                .map_err(|_| OperationsError::Corrupt("mission evidence observation ordinal"))?
        || decode_uuid(
            raw_member.binding_request_id.clone(),
            "mission evidence member binding",
        )? != context.binding_request_id
        || decode_uuid(
            raw_member.obligation_id.clone(),
            "mission evidence member obligation",
        )? != bound.obligation_id().into_uuid()
        || decode_participation(raw_member.participation_id.clone())? != bound.participation_id()
        || positive(raw_member.weight, "mission evidence member weight")? != bound.weight()
        || nonnegative_i64(raw_member.due_at_ms, "mission evidence member due time")?
            != bound.due_at_ms()
        || decode_uuid(
            raw_member.assignment_id.clone(),
            "mission evidence member assignment",
        )? != bound.declaration().assignment_id().into_uuid()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence observation member scope",
        ));
    }
    Ok(())
}

fn decode_observation_source(
    raw_member: &RawObservationMember,
    bound: &MissionEvidenceBoundMember,
) -> Result<DecodedObservationSource, OperationsError> {
    let coverage = super::codec::decode_coverage(super::codec::CoverageParts {
        state: raw_member.coverage_state,
        unknown_reason: raw_member.coverage_unknown_reason,
        submitted_revision: raw_member.eligible_submitted_revision,
        receipt_sequence: raw_member.eligible_receipt_sequence,
        admitted_at_ms: raw_member.admitted_at_ms,
        result_digest: raw_member
            .eligible_result_digest
            .clone()
            .map(|value| decode_digest(value, "mission evidence eligible result digest"))
            .transpose()?,
        sources_digest: raw_member
            .eligible_sources_digest
            .clone()
            .map(|value| decode_digest(value, "mission evidence eligible sources digest"))
            .transpose()?,
    })?;
    let submission = decode_submission(raw_member)?;
    let review_receipt = decode_review_receipt(raw_member)?;
    let review_state = super::codec::decode_review_state(
        super::codec::ReviewStateParts {
            state: raw_member.review_state,
            unknown_reason: raw_member.review_unknown_reason,
            pending_submitted_revision: raw_member.pending_review_submitted_revision,
            pending_receipt_sequence: raw_member.pending_review_receipt_sequence,
        },
        review_receipt,
    )?;
    let declaration = bound.declaration();
    Ok(DecodedObservationSource {
        t1_member: EvaluationT1CoverageMember::new(declaration, coverage),
        review_member: EvaluationReviewCutMember::new(
            declaration,
            coverage,
            submission,
            review_state,
        ),
    })
}

fn decode_observation_identities(
    context: &ObservationDecodeContext<'_>,
    bound: &MissionEvidenceBoundMember,
    source: &EvaluationReviewCutMember,
) -> Result<
    (
        MissionEvidenceIdentityResolution,
        Option<MissionEvidenceIdentityResolution>,
        Option<MissionEvidenceIdentityResolution>,
    ),
    OperationsError,
> {
    let declaration = bound.declaration();
    let target_identity = load_identity_resolution(
        context.connection,
        context.request_id,
        0,
        declaration.obligation_id().into_uuid(),
    )?
    .ok_or(OperationsError::Corrupt(
        "mission evidence observation target identity proof",
    ))?;
    if !resolution_matches_actor(
        &target_identity,
        declaration.target_agent_id(),
        declaration.target_session_id(),
    ) || matches!(
        &target_identity,
        MissionEvidenceIdentityResolution::Verified(proof)
            if proof.participation_id().is_some_and(|value| value != bound.participation_id())
    ) {
        return Err(OperationsError::Corrupt(
            "mission evidence observation target identity tuple",
        ));
    }
    let author_identity =
        decode_observation_author_identity(context, declaration, source, &target_identity)?;
    let reviewer_identity = decode_observation_reviewer_identity(context, declaration, source)?;
    Ok((target_identity, author_identity, reviewer_identity))
}

fn decode_observation_author_identity(
    context: &ObservationDecodeContext<'_>,
    declaration: EvaluationManifestMember,
    source: &EvaluationReviewCutMember,
    target_identity: &MissionEvidenceIdentityResolution,
) -> Result<Option<MissionEvidenceIdentityResolution>, OperationsError> {
    if let Some(receipt) = source.submission() {
        let identity = load_identity_resolution(
            context.connection,
            context.request_id,
            1,
            declaration.obligation_id().into_uuid(),
        )?
        .ok_or(OperationsError::Corrupt(
            "mission evidence author identity proof",
        ))?;
        if !resolution_matches_actor(
            &identity,
            receipt.author_agent_id(),
            receipt.author_session_id(),
        ) || &identity != target_identity
        {
            return Err(OperationsError::Corrupt(
                "mission evidence author identity tuple",
            ));
        }
        Ok(Some(identity))
    } else {
        if load_identity_resolution(
            context.connection,
            context.request_id,
            1,
            declaration.obligation_id().into_uuid(),
        )?
        .is_some()
        {
            return Err(OperationsError::Corrupt(
                "mission evidence absent author proof",
            ));
        }
        Ok(None)
    }
}

fn decode_observation_reviewer_identity(
    context: &ObservationDecodeContext<'_>,
    declaration: EvaluationManifestMember,
    source: &EvaluationReviewCutMember,
) -> Result<Option<MissionEvidenceIdentityResolution>, OperationsError> {
    match source.review() {
        EvaluationReviewState::Reviewed(receipt) => {
            let identity = load_identity_resolution(
                context.connection,
                context.request_id,
                2,
                declaration.obligation_id().into_uuid(),
            )?
            .ok_or(OperationsError::Corrupt(
                "mission evidence reviewer identity proof",
            ))?;
            if !resolution_matches_actor(
                &identity,
                receipt.reviewer_agent_id(),
                receipt.reviewer_session_id(),
            ) {
                return Err(OperationsError::Corrupt(
                    "mission evidence reviewer identity tuple",
                ));
            }
            Ok(Some(identity))
        }
        EvaluationReviewState::NotApplicable
        | EvaluationReviewState::Pending { .. }
        | EvaluationReviewState::Unknown(_) => {
            if load_identity_resolution(
                context.connection,
                context.request_id,
                2,
                declaration.obligation_id().into_uuid(),
            )?
            .is_some()
            {
                return Err(OperationsError::Corrupt(
                    "mission evidence absent reviewer proof",
                ));
            }
            Ok(None)
        }
    }
}

fn decode_observation_cuts(
    raw_partition: RawObservationPartition,
    proposal: &forge_domain::EvaluationManifestProposal,
    t1_members: Vec<EvaluationT1CoverageMember>,
    review_members: Vec<EvaluationReviewCutMember>,
) -> Result<(EvaluationT1CoverageCut, EvaluationReviewCut), OperationsError> {
    let t1_cut_id = forge_domain::EvaluationCutId::new(decode_uuid(
        raw_partition.t1_cut_id,
        "mission evidence T1 cut",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evidence T1 cut"))?;
    let deadline = TimestampMillis::new(raw_partition.deadline_ms)
        .map_err(|_| OperationsError::Corrupt("mission evidence observation deadline"))?;
    let closed_at = TimestampMillis::new(raw_partition.closed_at_ms)
        .map_err(|_| OperationsError::Corrupt("mission evidence observation closure time"))?;
    let t1_source_high_water = EvaluationSourceSequence::new(raw_partition.t1_source_high_water)
        .map_err(|_| OperationsError::Corrupt("mission evidence T1 high-water"))?;
    let t1_cut = EvaluationT1CoverageCut::from_source_records(
        t1_cut_id,
        proposal,
        deadline,
        closed_at,
        t1_source_high_water,
        t1_members,
    )
    .map_err(|_| OperationsError::Corrupt("mission evidence T1 cut"))?;
    let review_cut_id = EvaluationReviewCutId::new(decode_uuid(
        raw_partition.review_cut_id,
        "mission evidence review cut",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evidence review cut"))?;
    let review_t1_cut_id = forge_domain::EvaluationCutId::new(decode_uuid(
        raw_partition.review_t1_cut_id,
        "mission evidence review T1 cut",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evidence review T1 cut"))?;
    if review_t1_cut_id != t1_cut_id {
        return Err(OperationsError::Corrupt(
            "mission evidence review T1 cut identity",
        ));
    }
    let review_source_high_water =
        EvaluationSourceSequence::new(raw_partition.review_source_high_water)
            .map_err(|_| OperationsError::Corrupt("mission evidence review high-water"))?;
    let review_cut = EvaluationReviewCut::from_source_records(
        review_cut_id,
        &t1_cut,
        review_source_high_water,
        review_members,
    )
    .map_err(|_| OperationsError::Corrupt("mission evidence review cut"))?;
    Ok((t1_cut, review_cut))
}

fn resolution_matches_actor(
    resolution: &MissionEvidenceIdentityResolution,
    agent_id: forge_domain::AgentInstanceId,
    session_id: forge_domain::AgentSessionId,
) -> bool {
    match resolution {
        MissionEvidenceIdentityResolution::Verified(proof) => {
            proof.actor_agent_id() == agent_id && proof.actor_session_id() == session_id
        }
        MissionEvidenceIdentityResolution::Pending {
            agent_id: stored_agent,
            session_id: stored_session,
            ..
        } => *stored_agent == agent_id && *stored_session == session_id,
    }
}

fn decode_disposition(
    disposition: i64,
    reason: Option<i64>,
) -> Result<MissionEvidenceDisposition, OperationsError> {
    match (disposition, reason) {
        (0, None) => Ok(MissionEvidenceDisposition::AcceptedOriginal),
        (1, Some(reason)) => Ok(MissionEvidenceDisposition::Unresolved(
            super::codec::decode_unresolved_reason(reason)?,
        )),
        _ => Err(OperationsError::Corrupt("mission evidence disposition")),
    }
}

fn decode_submission(
    raw: &RawObservationMember,
) -> Result<Option<EvaluationSubmissionReceipt>, OperationsError> {
    match raw.submission_present {
        0 => {
            if raw.submission_revision.is_some()
                || raw.author_agent_id.is_some()
                || raw.author_session_id.is_some()
                || raw.original_binding_digest.is_some()
                || raw.submission_criteria_digest.is_some()
                || raw.submission_sequence.is_some()
                || raw.submission_timing_kind.is_some()
                || raw.submission_timing_ms.is_some()
                || raw.submission_clock_uncertainty.is_some()
                || raw.submission_result_digest.is_some()
                || raw.submission_sources_digest.is_some()
            {
                return Err(OperationsError::Corrupt(
                    "mission evidence absent submission",
                ));
            }
            Ok(None)
        }
        1 => {
            let timing = super::codec::decode_timing(
                raw.submission_timing_kind.ok_or(OperationsError::Corrupt(
                    "mission evidence submission timing",
                ))?,
                raw.submission_timing_ms,
                raw.submission_clock_uncertainty,
            )?;
            let receipt = EvaluationSubmissionReceipt::from_source_record(
                forge_domain::AgentAssignmentId::from_uuid(decode_uuid(
                    raw.assignment_id.clone(),
                    "mission evidence submission assignment",
                )?),
                forge_domain::AgentAssignmentRevision::new(raw.submission_revision.ok_or(
                    OperationsError::Corrupt("mission evidence submission revision"),
                )?)
                .map_err(|_| OperationsError::Corrupt("mission evidence submission revision"))?,
                forge_domain::AgentInstanceId::from_uuid(decode_uuid(
                    raw.author_agent_id.clone().ok_or(OperationsError::Corrupt(
                        "mission evidence submission Agent",
                    ))?,
                    "mission evidence submission Agent",
                )?),
                forge_domain::AgentSessionId::from_uuid(decode_uuid(
                    raw.author_session_id
                        .clone()
                        .ok_or(OperationsError::Corrupt(
                            "mission evidence submission Session",
                        ))?,
                    "mission evidence submission Session",
                )?),
                raw.original_binding_digest
                    .clone()
                    .map(|value| {
                        decode_evaluation_digest(value, "mission evidence original binding digest")
                    })
                    .transpose()?,
                raw.submission_criteria_digest
                    .clone()
                    .map(|value| {
                        decode_evaluation_digest(value, "mission evidence submission criteria")
                    })
                    .transpose()?,
                EvaluationSourceSequence::new(raw.submission_sequence.ok_or(
                    OperationsError::Corrupt("mission evidence submission sequence"),
                )?)
                .map_err(|_| OperationsError::Corrupt("mission evidence submission sequence"))?,
                timing,
                decode_evaluation_digest(
                    raw.submission_result_digest
                        .clone()
                        .ok_or(OperationsError::Corrupt(
                            "mission evidence submission result",
                        ))?,
                    "mission evidence submission result",
                )?,
                raw.submission_sources_digest
                    .clone()
                    .map(|value| {
                        decode_evaluation_digest(value, "mission evidence submission sources")
                    })
                    .transpose()?,
            )
            .map_err(|_| OperationsError::Corrupt("mission evidence submission receipt"))?;
            Ok(Some(receipt))
        }
        _ => Err(OperationsError::Corrupt(
            "mission evidence submission presence",
        )),
    }
}

fn decode_review_receipt(
    raw: &RawObservationMember,
) -> Result<Option<EvaluationReviewReceipt>, OperationsError> {
    if raw.review_state != 2 {
        if raw.review_assignment_id.is_some()
            || raw.review_submitted_revision.is_some()
            || raw.review_revision.is_some()
            || raw.review_sequence.is_some()
            || raw.review_outcome.is_some()
            || raw.review_author_agent_id.is_some()
            || raw.review_author_session_id.is_some()
            || raw.reviewer_agent_id.is_some()
            || raw.reviewer_session_id.is_some()
            || raw.review_result_digest.is_some()
        {
            return Err(OperationsError::Corrupt(
                "mission evidence absent review receipt",
            ));
        }
        return Ok(None);
    }
    let receipt = EvaluationReviewReceipt::from_source_record(
        forge_domain::AgentAssignmentId::from_uuid(decode_uuid(
            raw.review_assignment_id
                .clone()
                .ok_or(OperationsError::Corrupt(
                    "mission evidence review assignment",
                ))?,
            "mission evidence review assignment",
        )?),
        forge_domain::AgentAssignmentRevision::new(raw.review_submitted_revision.ok_or(
            OperationsError::Corrupt("mission evidence review submission revision"),
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evidence review submission revision"))?,
        forge_domain::AgentAssignmentRevision::new(
            raw.review_revision
                .ok_or(OperationsError::Corrupt("mission evidence review revision"))?,
        )
        .map_err(|_| OperationsError::Corrupt("mission evidence review revision"))?,
        EvaluationSourceSequence::new(
            raw.review_sequence
                .ok_or(OperationsError::Corrupt("mission evidence review sequence"))?,
        )
        .map_err(|_| OperationsError::Corrupt("mission evidence review sequence"))?,
        super::codec::decode_review_outcome(
            raw.review_outcome
                .ok_or(OperationsError::Corrupt("mission evidence review outcome"))?,
        )?,
        forge_domain::AgentInstanceId::from_uuid(decode_uuid(
            raw.review_author_agent_id
                .clone()
                .ok_or(OperationsError::Corrupt(
                    "mission evidence review author Agent",
                ))?,
            "mission evidence review author Agent",
        )?),
        forge_domain::AgentSessionId::from_uuid(decode_uuid(
            raw.review_author_session_id
                .clone()
                .ok_or(OperationsError::Corrupt(
                    "mission evidence review author Session",
                ))?,
            "mission evidence review author Session",
        )?),
        forge_domain::AgentInstanceId::from_uuid(decode_uuid(
            raw.reviewer_agent_id
                .clone()
                .ok_or(OperationsError::Corrupt("mission evidence reviewer Agent"))?,
            "mission evidence reviewer Agent",
        )?),
        forge_domain::AgentSessionId::from_uuid(decode_uuid(
            raw.reviewer_session_id
                .clone()
                .ok_or(OperationsError::Corrupt(
                    "mission evidence reviewer Session",
                ))?,
            "mission evidence reviewer Session",
        )?),
        decode_evaluation_digest(
            raw.review_result_digest
                .clone()
                .ok_or(OperationsError::Corrupt("mission evidence review result"))?,
            "mission evidence review result",
        )?,
    )
    .map_err(|_| OperationsError::Corrupt("mission evidence review receipt"))?;
    Ok(Some(receipt))
}

fn decode_actor_kind(value: i64) -> Result<MissionEvidenceActorKind, OperationsError> {
    match value {
        0 => Ok(MissionEvidenceActorKind::LocalOperator),
        1 => Ok(MissionEvidenceActorKind::ProfileParticipant),
        2 => Ok(MissionEvidenceActorKind::NativeParticipant),
        _ => Err(OperationsError::Corrupt("mission evidence actor kind")),
    }
}

fn decode_uuid(bytes: impl AsRef<[u8]>, label: &'static str) -> Result<Uuid, OperationsError> {
    decode_uuid_ref(bytes.as_ref(), label)
}

fn decode_uuid_ref(bytes: &[u8], label: &'static str) -> Result<Uuid, OperationsError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    let value = Uuid::from_bytes(bytes);
    if value.is_nil() {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn decode_workspace(bytes: Vec<u8>) -> Result<WorkspaceId, OperationsError> {
    Ok(WorkspaceId::from_uuid(decode_uuid(
        bytes,
        "mission evidence Workspace",
    )?))
}

fn decode_participation(bytes: Vec<u8>) -> Result<WorkspaceParticipationId, OperationsError> {
    WorkspaceParticipationId::from_uuid(decode_uuid(bytes, "mission evidence participation")?)
        .map_err(|_| OperationsError::Corrupt("mission evidence participation"))
}

fn decode_session(bytes: Vec<u8>) -> Result<forge_domain::AgentSessionId, OperationsError> {
    Ok(forge_domain::AgentSessionId::from_uuid(decode_uuid(
        bytes,
        "mission evidence Session",
    )?))
}

fn decode_text(
    bytes: Vec<u8>,
    label: &'static str,
    maximum: usize,
) -> Result<String, OperationsError> {
    let value = String::from_utf8(bytes).map_err(|_| OperationsError::Corrupt(label))?;
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn decode_digest(bytes: Vec<u8>, label: &'static str) -> Result<[u8; 32], OperationsError> {
    let value: [u8; 32] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    if value == [0; 32] {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn signed(value: u64, label: &'static str) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds(label))
}

fn positive(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OperationsError::Corrupt(label))
}

fn nonnegative(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(label))
}

fn nonnegative_i64(value: i64, label: &'static str) -> Result<i64, OperationsError> {
    if value < 0 {
        Err(OperationsError::Corrupt(label))
    } else {
        Ok(value)
    }
}

fn read_request_for_actor(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    actor_input: OperationalActor,
) -> Result<Option<MissionEvidenceMutationRecord>, OperationsError> {
    if request_id.is_nil() || workspace_id.into_uuid().is_nil() {
        return Err(OperationsError::Conflict("mission evidence read identity"));
    }
    let transaction = catalog.connection.unchecked_transaction()?;
    let (actor, _resolved) =
        MissionActorIdentity::resolve(&transaction, workspace_id, actor_input)?;
    let Some(stored) = load_request(&transaction, request_id)? else {
        crate::catalog::commit_read(transaction, catalog.destination)?;
        return Ok(None);
    };
    if stored.workspace_id != workspace_id || !stored.actor_identity.agrees_with(&actor) {
        return Err(OperationsError::Conflict(
            "mission evidence request actor scope",
        ));
    }
    let record = match stored.operation {
        BIND_OPERATION => MissionEvidenceMutationRecord::ManifestBinding(
            load_binding(&transaction, catalog.destination, request_id)?
                .ok_or(OperationsError::Corrupt("mission evidence binding receipt"))?,
        ),
        OBSERVE_OPERATION => MissionEvidenceMutationRecord::Observation(
            load_observation(&transaction, catalog.destination, request_id)?.ok_or(
                OperationsError::Corrupt("mission evidence observation receipt"),
            )?,
        ),
        _ => {
            return Err(OperationsError::Corrupt(
                "mission evidence request operation",
            ));
        }
    };
    crate::catalog::commit_read(transaction, catalog.destination)?;
    Ok(Some(record))
}

fn commit_write(
    transaction: Transaction<'_>,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id,
            request_id,
            source,
        })
}

fn prepare_binding(
    destination_id: Uuid,
    connection: &Connection,
    command: &MissionEvidenceManifestBindingCommand,
    actor: &MissionActorSnapshot,
) -> Result<MissionEvidenceManifestBindingRecord, OperationsError> {
    let workspace_id = command.workspace_id();
    let bundle = command.bundle();
    let common_source = bundle
        .manifests()
        .first()
        .ok_or(OperationsError::Conflict(
            "mission evidence manifest bundle",
        ))?
        .proposal()
        .source();
    let (frozen, digests) =
        validate_binding_local_terms(destination_id, connection, command, bundle, common_source)?;
    let (members, pending_binding) = bind_bundle_members(
        connection,
        destination_id,
        workspace_id,
        bundle,
        &frozen.obligations,
        digests.member_criteria_digests(),
    )?;
    Ok(binding_from_parts(NewBindingParts {
        request_id: command.request_id(),
        workspace_id,
        mission_id: frozen.version.mission_id(),
        plan_id: frozen.version.plan_id(),
        plan_version_id: frozen.version.version_id(),
        plan_revision: frozen.version.revision(),
        plan_start_request_id: frozen.start.request_id(),
        plan_started_at_ms: frozen.start.started_at_ms(),
        t0_commit_at_ms: frozen.version.t0_commit_at_ms(),
        t1_submission_cutoff_ms: frozen.version.t1_submission_cutoff_ms(),
        t2_checkpoint_ms: frozen.version.t2_checkpoint_ms(),
        manifest_bundle: bundle.clone(),
        plan_digests: digests,
        state: if pending_binding {
            MissionEvidenceBindingState::Pending
        } else {
            MissionEvidenceBindingState::Verified
        },
        members,
        actor: MissionEvidenceActorReceipt::from_snapshot(actor)?,
        recorded_at_ms: command.recorded_at_ms(),
    }))
}

fn validate_binding_local_terms(
    destination_id: Uuid,
    connection: &Connection,
    command: &MissionEvidenceManifestBindingCommand,
    bundle: &super::MissionEvidenceManifestBundle,
    common_source: EvaluationSourceIdentity,
) -> Result<
    (
        crate::mission_evaluation::FrozenMissionEvaluationPlan,
        super::MissionEvidencePlanDigests,
    ),
    OperationsError,
> {
    let workspace_id = command.workspace_id();
    if bundle.workspace_id() != workspace_id
        || bundle.plan_binding().plan_id().into_uuid() != command.plan_id().into_uuid()
        || bundle.plan_binding().revision().get()
            != i64::try_from(command.plan_revision())
                .map_err(|_| OperationsError::Bounds("mission evidence plan revision"))?
    {
        return Err(OperationsError::Conflict(
            "mission evidence manifest bundle scope",
        ));
    }
    require_verified_source_import(connection, destination_id, workspace_id, common_source)?;
    let frozen = frozen_plan_in_transaction(
        connection,
        workspace_id,
        command.plan_id(),
        command.plan_revision(),
    )?;
    let digests = super::digest::derive(&frozen.version, &frozen.obligations)?;
    if frozen.version.plan_id() != command.plan_id()
        || frozen.version.revision() != command.plan_revision()
        || bundle.plan_binding().digest() != digests.plan_digest()
        || frozen.version.t1_submission_cutoff_ms() != bundle.deadline_ms()
        || digests.plan_id().into_uuid() != command.plan_id().into_uuid()
        || digests.revision() != command.plan_revision()
    {
        return Err(OperationsError::Conflict(
            "mission evidence local plan digest",
        ));
    }
    Ok((frozen, digests))
}

struct BoundManifestPartition {
    members: Vec<MissionEvidenceBoundMember>,
    obligations: std::collections::BTreeSet<Uuid>,
    units: std::collections::BTreeSet<Uuid>,
    pending: bool,
}

fn bind_bundle_members(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    bundle: &super::MissionEvidenceManifestBundle,
    obligations: &[MissionObligationRecord],
    criteria_digests: &[super::MissionEvidenceMemberCriteriaDigest],
) -> Result<(Vec<MissionEvidenceBoundMember>, bool), OperationsError> {
    let obligations_by_id = obligations
        .iter()
        .map(|record| (record.obligation_id(), record))
        .collect::<BTreeMap<_, _>>();
    if obligations_by_id.len() != obligations.len() || criteria_digests.len() != obligations.len() {
        return Err(OperationsError::Corrupt(
            "mission evidence local obligation or criteria duplicates",
        ));
    }
    let criteria = criteria_digests
        .iter()
        .map(|value| (value.obligation_id(), *value))
        .collect::<BTreeMap<_, _>>();
    if criteria.len() != criteria_digests.len() {
        return Err(OperationsError::Corrupt(
            "mission evidence local criteria duplicates",
        ));
    }
    let member_count = bundle
        .manifests()
        .iter()
        .try_fold(0_usize, |total, manifest| {
            total
                .checked_add(manifest.members().len())
                .ok_or(OperationsError::Bounds("mission evidence manifest members"))
        })?;
    if member_count != obligations.len() {
        return Err(OperationsError::Conflict(
            "mission evidence incomplete manifest cohort",
        ));
    }
    let mut members = Vec::with_capacity(member_count);
    let mut seen_obligations = std::collections::BTreeSet::new();
    let mut seen_units = std::collections::BTreeSet::new();
    let mut pending = false;
    for (partition_ordinal, manifest) in bundle.manifests().iter().enumerate() {
        let partition = bind_manifest_partition(
            connection,
            destination_id,
            workspace_id,
            partition_ordinal,
            manifest,
            &obligations_by_id,
            &criteria,
        )?;
        if partition
            .obligations
            .iter()
            .any(|obligation| !seen_obligations.insert(*obligation))
            || partition.units.iter().any(|unit| !seen_units.insert(*unit))
        {
            return Err(OperationsError::Conflict(
                "mission evidence duplicate or changed source member",
            ));
        }
        pending |= partition.pending;
        members.extend(partition.members);
    }
    if seen_obligations.len() != obligations.len() || seen_units.len() != member_count {
        return Err(OperationsError::Conflict(
            "mission evidence incomplete local cohort",
        ));
    }
    Ok((members, pending))
}

fn bind_manifest_partition(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    partition_ordinal: usize,
    manifest: &EvaluationRegisteredManifest,
    obligations: &BTreeMap<crate::MissionObligationId, &MissionObligationRecord>,
    criteria: &BTreeMap<crate::MissionObligationId, super::MissionEvidenceMemberCriteriaDigest>,
) -> Result<BoundManifestPartition, OperationsError> {
    let proposal = manifest.proposal();
    let source = proposal.source();
    let mut pending = matches!(
        manifest.seal_timing(),
        EvaluationAdmissionTiming::Unknown(_)
    ) || matches!(manifest.seal_timing(), EvaluationAdmissionTiming::Trusted(time) if time.get() > proposal.deadline().get());
    if manifest.source_sequence().get() <= 0 || manifest.members().len() != proposal.members().len()
    {
        return Err(OperationsError::Conflict(
            "mission evidence registered partition",
        ));
    }
    let mut members = Vec::with_capacity(manifest.members().len());
    let mut seen_obligations = std::collections::BTreeSet::new();
    let mut seen_units = std::collections::BTreeSet::new();
    for (declaration, registered) in proposal.members().iter().zip(manifest.members()) {
        if *declaration != registered.declaration()
            || !seen_obligations.insert(declaration.obligation_id().into_uuid())
            || !seen_units.insert(declaration.work_unit_id().into_uuid())
        {
            return Err(OperationsError::Conflict(
                "mission evidence duplicate or changed source member",
            ));
        }
        let obligation_id =
            crate::MissionObligationId::from_uuid(declaration.obligation_id().into_uuid())?;
        let obligation =
            obligations
                .get(&obligation_id)
                .copied()
                .ok_or(OperationsError::Conflict(
                    "mission evidence foreign obligation",
                ))?;
        let criterion = criteria
            .get(&obligation_id)
            .copied()
            .ok_or(OperationsError::Corrupt(
                "mission evidence criteria digest missing",
            ))?;
        if declaration.work_unit_id().into_uuid() != obligation.work_unit_id().into_uuid()
            || declaration.criteria_digest().as_bytes() != criterion.digest().as_bytes()
        {
            return Err(OperationsError::Conflict(
                "mission evidence manifest local terms",
            ));
        }
        if let EvaluationOriginalBindingStatus::Bound(original) = registered.binding()
            && (original.assignment_revision() != declaration.assignment_revision()
                || original.source_sequence().get() > manifest.source_sequence().get())
        {
            return Err(OperationsError::Conflict(
                "mission evidence original binding revision",
            ));
        }
        let target_identity = super::historical_identity::resolve(
            connection,
            destination_id,
            workspace_id,
            Some(source.activity_id()),
            declaration.target_agent_id(),
            declaration.target_session_id(),
        )?;
        if !target_identity_matches_participation(&target_identity, obligation.participation_id())?
            || registered.binding() == EvaluationOriginalBindingStatus::UnknownHistorical
        {
            pending = true;
        }
        members.push(MissionEvidenceBoundMember::from_parts(
            MissionEvidenceBoundMemberParts {
                partition_ordinal: u32::try_from(partition_ordinal)
                    .map_err(|_| OperationsError::Bounds("mission evidence partition ordinal"))?,
                source_activity_id: source.activity_id(),
                declaration: *declaration,
                original_binding: registered.binding(),
                obligation_id: obligation.obligation_id(),
                work_unit_id: obligation.work_unit_id(),
                participation_id: obligation.participation_id(),
                weight: obligation.weight().get(),
                due_at_ms: obligation.due_at_ms(),
                criteria_digest: criterion,
                target_identity,
            },
        )?);
    }
    Ok(BoundManifestPartition {
        members,
        obligations: seen_obligations,
        units: seen_units,
        pending,
    })
}

pub(super) fn target_identity_matches_participation(
    identity: &MissionEvidenceIdentityResolution,
    expected: WorkspaceParticipationId,
) -> Result<bool, OperationsError> {
    match identity {
        MissionEvidenceIdentityResolution::Pending { .. } => Ok(false),
        MissionEvidenceIdentityResolution::Verified(proof) => match proof.participation_id() {
            Some(actual) if actual == expected => Ok(true),
            Some(_) => Err(OperationsError::Conflict(
                "mission evidence target participation foreign",
            )),
            None => Ok(false),
        },
    }
}

fn require_verified_source_import(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    source: EvaluationSourceIdentity,
) -> Result<ImportProgress, OperationsError> {
    let progress = persisted_source_import(connection, destination_id, workspace_id, source)?;
    if progress.phase() != ImportPhase::Verified {
        return Err(OperationsError::Conflict(
            "mission evidence source import is not verified",
        ));
    }
    Ok(progress)
}

fn persisted_source_import(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    source: EvaluationSourceIdentity,
) -> Result<ImportProgress, OperationsError> {
    let progress = crate::catalog::load_progress(connection, source.delegation_migration_id())?
        .ok_or(OperationsError::Corrupt(
            "mission evidence verified import missing",
        ))?;
    let source_revision = u64::try_from(source.delegation_freeze_revision().get())
        .map_err(|_| OperationsError::Corrupt("mission evidence freeze revision"))?;
    if !matches!(
        progress.phase(),
        ImportPhase::Verified | ImportPhase::Aborted
    ) || progress.workspace_id() != workspace_id
        || progress.workspace_id() != source.workspace_id()
        || progress.destination_id() != destination_id
        || source.delegation_destination_id() != destination_id
        || progress.source_revision() != source_revision
        || progress.digest().as_slice() != source.delegation_manifest_digest().as_bytes()
        || progress.verified_pages() != progress.page_count()
        || progress.verify_digest() != progress.digest()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence source import identity",
        ));
    }
    Ok(progress)
}

fn binding_exists_for_plan(
    connection: &Connection,
    workspace_id: WorkspaceId,
    plan_id: crate::MissionEvaluationPlanId,
    revision: u64,
) -> Result<bool, OperationsError> {
    let revision = i64::try_from(revision)
        .map_err(|_| OperationsError::Bounds("mission evidence plan revision"))?;
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evidence_bindings WHERE workspace_id=?1 AND plan_id=?2 AND plan_revision=?3)",
        params![workspace_id.into_uuid().as_bytes(), plan_id.into_uuid().as_bytes(), revision],
        |row| row.get(0),
    )?)
}

fn binding_for_plan(
    connection: &Connection,
    workspace_id: WorkspaceId,
    plan_id: crate::MissionEvaluationPlanId,
    revision: u64,
) -> Result<Option<Uuid>, OperationsError> {
    let revision = i64::try_from(revision)
        .map_err(|_| OperationsError::Bounds("mission evidence plan revision"))?;
    let request_id = connection
        .query_row(
            "SELECT substr(request_id,1,17) FROM ops_mission_evidence_bindings WHERE workspace_id=?1 AND plan_id=?2 AND plan_revision=?3 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), plan_id.into_uuid().as_bytes(), revision],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    request_id
        .map(|value| decode_uuid(value, "mission evidence binding request"))
        .transpose()
}

fn insert_binding(
    transaction: &Transaction<'_>,
    binding: &MissionEvidenceManifestBindingRecord,
) -> Result<(), OperationsError> {
    let first_source = binding
        .manifest_bundle()
        .manifests()
        .first()
        .ok_or(OperationsError::Corrupt("mission evidence manifest bundle"))?
        .proposal()
        .source();
    insert_binding_header(transaction, binding, first_source)?;
    for (ordinal, manifest) in binding.manifest_bundle().manifests().iter().enumerate() {
        insert_binding_partition(transaction, binding.request_id(), ordinal, manifest)?;
    }
    let mut member_ordinals = BTreeMap::<u32, i64>::new();
    for member in binding.members() {
        let ordinal = member_ordinals
            .entry(member.partition_ordinal())
            .or_insert(0);
        let member_ordinal = *ordinal;
        *ordinal = ordinal
            .checked_add(1)
            .ok_or(OperationsError::Bounds("mission evidence member ordinal"))?;
        insert_binding_member(transaction, binding.request_id(), member, member_ordinal)?;
    }
    Ok(())
}

fn insert_binding_header(
    transaction: &Transaction<'_>,
    binding: &MissionEvidenceManifestBindingRecord,
    first_source: EvaluationSourceIdentity,
) -> Result<(), OperationsError> {
    let binding_state = match binding.state() {
        MissionEvidenceBindingState::Verified => 0_i64,
        MissionEvidenceBindingState::Pending => 1_i64,
    };
    let member_count = i64::try_from(binding.members().len())
        .map_err(|_| OperationsError::Bounds("mission evidence binding member count"))?;
    let partition_count = i64::try_from(binding.manifest_bundle().manifests().len())
        .map_err(|_| OperationsError::Bounds("mission evidence partition count"))?;
    transaction.execute(
        "INSERT INTO ops_mission_evidence_bindings(request_id,workspace_id,mission_id,plan_id,plan_version_id,plan_revision,plan_start_request_id,plan_started_at_ms,source_backend_id,source_workspace_id,source_migration_id,source_destination_id,source_freeze_revision,source_delegation_digest,local_plan_digest,local_criteria_digest,t0_commit_at_ms,t1_submission_cutoff_ms,t2_checkpoint_ms,member_count,partition_count,binding_state,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23)",
        params![
            binding.request_id().as_bytes(),
            binding.workspace_id().into_uuid().as_bytes(),
            binding.mission_id().into_uuid().as_bytes(),
            binding.plan_id().into_uuid().as_bytes(),
            binding.plan_version_id().into_uuid().as_bytes(),
            signed(binding.plan_revision(), "mission evidence plan revision")?,
            binding.plan_start_request_id().as_bytes(),
            binding.plan_started_at_ms(),
            first_source.backend_id().into_uuid().as_bytes(),
            first_source.workspace_id().into_uuid().as_bytes(),
            first_source.delegation_migration_id().as_bytes(),
            first_source.delegation_destination_id().as_bytes(),
            first_source.delegation_freeze_revision().get(),
            first_source.delegation_manifest_digest().as_bytes(),
            binding.local_plan_digest().as_bytes(),
            binding.local_criteria_digest().as_bytes(),
            binding.t0_commit_at_ms(),
            binding.t1_submission_cutoff_ms(),
            binding.t2_checkpoint_ms(),
            member_count,
            partition_count,
            binding_state,
            binding.recorded_at_ms(),
        ],
    )?;
    Ok(())
}

fn insert_binding_partition(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    ordinal: usize,
    manifest: &EvaluationRegisteredManifest,
) -> Result<(), OperationsError> {
    let partition_ordinal = i64::try_from(ordinal)
        .map_err(|_| OperationsError::Bounds("mission evidence partition ordinal"))?;
    let proposal = manifest.proposal();
    let source = proposal.source();
    let (seal_timing_kind, seal_timing_ms, seal_uncertainty) =
        super::codec::encode_timing(manifest.seal_timing());
    let member_count = i64::try_from(manifest.members().len())
        .map_err(|_| OperationsError::Bounds("mission evidence partition members"))?;
    transaction.execute(
        "INSERT INTO ops_mission_evidence_binding_partitions(binding_request_id,partition_ordinal,activity_id,manifest_id,manifest_revision,manifest_digest,source_sequence,seal_timing_kind,seal_timing_ms,seal_uncertainty,member_count) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            request_id.as_bytes(),
            partition_ordinal,
            source.activity_id().into_uuid().as_bytes(),
            proposal.manifest_id().into_uuid().as_bytes(),
            proposal.revision().get(),
            proposal.digest().as_bytes(),
            manifest.source_sequence().get(),
            seal_timing_kind,
            seal_timing_ms,
            seal_uncertainty,
            member_count,
        ],
    )?;
    Ok(())
}

fn insert_binding_member(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    member: &MissionEvidenceBoundMember,
    ordinal: i64,
) -> Result<(), OperationsError> {
    let (binding_state, digest, assignment_revision, source_sequence) =
        super::codec::encode_original_binding(member.original_binding());
    transaction.execute(
        "INSERT INTO ops_mission_evidence_binding_members(binding_request_id,partition_ordinal,ordinal,obligation_id,work_unit_id,participation_id,weight,due_at_ms,assignment_id,assignment_revision,target_agent_id,target_session_id,criteria_digest,original_binding_state,original_binding_digest,original_binding_revision,original_binding_sequence) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![
            request_id.as_bytes(),
            i64::from(member.partition_ordinal()),
            ordinal,
            member.obligation_id().into_uuid().as_bytes(),
            member.work_unit_id().into_uuid().as_bytes(),
            member.participation_id().into_uuid().as_bytes(),
            signed(member.weight(), "mission evidence obligation weight")?,
            member.due_at_ms(),
            member.declaration().assignment_id().into_uuid().as_bytes(),
            member.declaration().assignment_revision().get(),
            member.declaration().target_agent_id().into_uuid().as_bytes(),
            member.declaration().target_session_id().into_uuid().as_bytes(),
            member.criteria_digest().digest().as_bytes(),
            binding_state,
            digest.as_ref().map(<[u8; 32]>::as_slice),
            assignment_revision,
            source_sequence,
        ],
    )?;
    insert_identity_proof(
        transaction,
        request_id,
        0,
        member.obligation_id().into_uuid(),
        member.target_identity(),
    )
}

fn check_prior_source_binding_history(
    transaction: &Transaction<'_>,
    binding: &MissionEvidenceManifestBindingRecord,
) -> Result<(), OperationsError> {
    let common_source = binding
        .manifest_bundle()
        .manifests()
        .first()
        .ok_or(OperationsError::Corrupt("mission evidence manifest bundle"))?
        .proposal()
        .source();
    for member in binding.members() {
        let expectation = super::history::SourceMemberHistoryExpectation {
            source: common_source,
            activity_id: member.source_activity_id(),
            declaration: member.declaration(),
            participation_id: member.participation_id(),
            weight: member.weight(),
            due_at_ms: member.due_at_ms(),
            criteria_digest: member.criteria_digest().digest(),
            expected_original_binding: Some(member.original_binding()),
        };
        super::history::check_prior_source_member_history(transaction, &expectation)?;
    }
    Ok(())
}

fn insert_identity_proof(
    transaction: &Transaction<'_>,
    owner_request_id: Uuid,
    role: i64,
    obligation_id: Uuid,
    resolution: &MissionEvidenceIdentityResolution,
) -> Result<(), OperationsError> {
    let values = identity_proof_values(owner_request_id, role, obligation_id, resolution)?;
    transaction.execute(
        "INSERT INTO ops_mission_evidence_identity_proofs(owner_request_id,role,obligation_id,agent_id,session_id,resolution_state,pending_reason,candidate_registration_request_id,candidate_activation_receipt_id,participant_id,profile_id,participation_id,registration_kind,registration_request_id,migration_id,destination_id,freeze_revision,membership_commit_digest,activation_receipt_id,activated_at_ms,profile_mapping_request_id,profile_revision,session_revision,participation_mapping_request_id,participant_membership_revision,participation_mapped_at_ms,rebind_request_id,rebind_revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28)",
        params_from_iter(values.iter()),
    )?;
    Ok(())
}

fn identity_proof_values(
    owner_request_id: Uuid,
    role: i64,
    obligation_id: Uuid,
    resolution: &MissionEvidenceIdentityResolution,
) -> Result<Vec<Value>, OperationsError> {
    Ok(match resolution {
        MissionEvidenceIdentityResolution::Pending {
            agent_id,
            session_id,
            reason,
            registration_request_id,
            activation_receipt_id,
        } => vec![
            blob(owner_request_id.as_bytes()),
            Value::Integer(role),
            blob(obligation_id.as_bytes()),
            blob(agent_id.into_uuid().as_bytes()),
            blob(session_id.into_uuid().as_bytes()),
            Value::Integer(1),
            Value::Integer(super::codec::encode_identity_pending_reason(*reason)),
            optional_uuid_value(*registration_request_id),
            optional_uuid_value(*activation_receipt_id),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        MissionEvidenceIdentityResolution::Verified(proof) => vec![
            blob(owner_request_id.as_bytes()),
            Value::Integer(role),
            blob(obligation_id.as_bytes()),
            blob(proof.actor_agent_id().into_uuid().as_bytes()),
            blob(proof.actor_session_id().into_uuid().as_bytes()),
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Null,
            blob(proof.participant_id().as_bytes()),
            optional_uuid_value(proof.profile_id()),
            optional_uuid_value(
                proof
                    .participation_id()
                    .map(WorkspaceParticipationId::into_uuid),
            ),
            Value::Integer(super::codec::encode_registration_kind(
                proof.registration_kind(),
            )),
            blob(proof.registration_request_id().as_bytes()),
            blob(proof.migration_id().as_bytes()),
            blob(proof.destination_id().as_bytes()),
            Value::Integer(signed(
                proof.freeze_revision(),
                "mission evidence freeze revision",
            )?),
            blob(proof.membership_commit_digest().as_slice()),
            blob(proof.activation_receipt_id().as_bytes()),
            Value::Integer(proof.activated_at_ms()),
            optional_uuid_value(proof.profile_mapping_request_id()),
            optional_u64_value(
                proof.profile_revision(),
                "mission evidence Profile revision",
            )?,
            optional_u64_value(
                proof.session_revision(),
                "mission evidence Session revision",
            )?,
            optional_uuid_value(proof.participation_mapping_request_id()),
            optional_u64_value(
                proof.participant_membership_revision(),
                "mission evidence membership revision",
            )?,
            proof
                .participation_mapped_at_ms()
                .map_or(Value::Null, Value::Integer),
            optional_uuid_value(proof.rebind_request_id()),
            optional_u64_value(proof.rebind_revision(), "mission evidence rebind revision")?,
        ],
    })
}

fn blob(bytes: &[u8]) -> Value {
    Value::Blob(bytes.to_vec())
}

fn optional_uuid_value(value: Option<Uuid>) -> Value {
    value.map_or(Value::Null, |value| blob(value.as_bytes()))
}

fn optional_u64_value(value: Option<u64>, label: &'static str) -> Result<Value, OperationsError> {
    value.map_or(Ok(Value::Null), |value| {
        signed(value, label).map(Value::Integer)
    })
}

#[derive(Debug)]
struct RawBinding {
    workspace_id: Vec<u8>,
    mission_id: Vec<u8>,
    plan_id: Vec<u8>,
    plan_version_id: Vec<u8>,
    plan_revision: i64,
    plan_start_request_id: Vec<u8>,
    plan_started_at_ms: i64,
    source_backend_id: Vec<u8>,
    source_workspace_id: Vec<u8>,
    source_migration_id: Vec<u8>,
    source_destination_id: Vec<u8>,
    source_freeze_revision: i64,
    source_delegation_digest: Vec<u8>,
    local_plan_digest: Vec<u8>,
    local_criteria_digest: Vec<u8>,
    t0_commit_at_ms: i64,
    t1_submission_cutoff_ms: i64,
    t2_checkpoint_ms: i64,
    member_count: i64,
    partition_count: i64,
    binding_state: i64,
    recorded_at_ms: i64,
}

#[derive(Debug)]
struct RawBindingPartition {
    ordinal: i64,
    activity_id: Vec<u8>,
    manifest_id: Vec<u8>,
    manifest_revision: i64,
    manifest_digest: Vec<u8>,
    source_sequence: i64,
    seal_timing_kind: i64,
    seal_timing_ms: Option<i64>,
    seal_uncertainty: Option<i64>,
    member_count: i64,
}

#[derive(Debug)]
struct RawBoundMember {
    partition_ordinal: i64,
    ordinal: i64,
    obligation_id: Vec<u8>,
    work_unit_id: Vec<u8>,
    participation_id: Vec<u8>,
    weight: i64,
    due_at_ms: i64,
    assignment_id: Vec<u8>,
    assignment_revision: i64,
    target_agent_id: Vec<u8>,
    target_session_id: Vec<u8>,
    criteria_digest: Vec<u8>,
    original_binding_state: i64,
    original_binding_digest: Option<Vec<u8>>,
    original_binding_revision: Option<i64>,
    original_binding_sequence: Option<i64>,
}

fn load_binding(
    connection: &Connection,
    destination_id: Uuid,
    request_id: Uuid,
) -> Result<Option<MissionEvidenceManifestBindingRecord>, OperationsError> {
    let Some(request) = load_request(connection, request_id)? else {
        return Ok(None);
    };
    if request.operation != BIND_OPERATION {
        return Err(OperationsError::Corrupt("mission evidence binding request"));
    }
    let raw = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),substr(mission_id,1,17),substr(plan_id,1,17),substr(plan_version_id,1,17),plan_revision,substr(plan_start_request_id,1,17),plan_started_at_ms,substr(source_backend_id,1,17),substr(source_workspace_id,1,17),substr(source_migration_id,1,17),substr(source_destination_id,1,17),source_freeze_revision,substr(source_delegation_digest,1,33),substr(local_plan_digest,1,33),substr(local_criteria_digest,1,33),t0_commit_at_ms,t1_submission_cutoff_ms,t2_checkpoint_ms,member_count,partition_count,binding_state,recorded_at_ms \
             FROM ops_mission_evidence_bindings WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawBinding {
                    workspace_id: row.get(0)?,
                    mission_id: row.get(1)?,
                    plan_id: row.get(2)?,
                    plan_version_id: row.get(3)?,
                    plan_revision: row.get(4)?,
                    plan_start_request_id: row.get(5)?,
                    plan_started_at_ms: row.get(6)?,
                    source_backend_id: row.get(7)?,
                    source_workspace_id: row.get(8)?,
                    source_migration_id: row.get(9)?,
                    source_destination_id: row.get(10)?,
                    source_freeze_revision: row.get(11)?,
                    source_delegation_digest: row.get(12)?,
                    local_plan_digest: row.get(13)?,
                    local_criteria_digest: row.get(14)?,
                    t0_commit_at_ms: row.get(15)?,
                    t1_submission_cutoff_ms: row.get(16)?,
                    t2_checkpoint_ms: row.get(17)?,
                    member_count: row.get(18)?,
                    partition_count: row.get(19)?,
                    binding_state: row.get(20)?,
                    recorded_at_ms: row.get(21)?,
                })
            },
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    decode_binding(connection, destination_id, request_id, request, &raw).map(Some)
}

fn load_raw_binding_partitions(
    connection: &Connection,
    binding_request_id: Uuid,
) -> Result<Vec<RawBindingPartition>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT partition_ordinal,substr(activity_id,1,17),substr(manifest_id,1,17),manifest_revision,substr(manifest_digest,1,33),source_sequence,seal_timing_kind,seal_timing_ms,seal_uncertainty,member_count FROM ops_mission_evidence_binding_partitions WHERE binding_request_id=?1 ORDER BY partition_ordinal LIMIT 257",
    )?;
    let rows = statement.query_map(params![binding_request_id.as_bytes()], |row| {
        Ok(RawBindingPartition {
            ordinal: row.get(0)?,
            activity_id: row.get(1)?,
            manifest_id: row.get(2)?,
            manifest_revision: row.get(3)?,
            manifest_digest: row.get(4)?,
            source_sequence: row.get(5)?,
            seal_timing_kind: row.get(6)?,
            seal_timing_ms: row.get(7)?,
            seal_uncertainty: row.get(8)?,
            member_count: row.get(9)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_raw_binding_members(
    connection: &Connection,
    binding_request_id: Uuid,
) -> Result<Vec<RawBoundMember>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT partition_ordinal,ordinal,substr(obligation_id,1,17),substr(work_unit_id,1,17),substr(participation_id,1,17),weight,due_at_ms,substr(assignment_id,1,17),assignment_revision,substr(target_agent_id,1,17),substr(target_session_id,1,17),substr(criteria_digest,1,33),original_binding_state,substr(original_binding_digest,1,33),original_binding_revision,original_binding_sequence FROM ops_mission_evidence_binding_members WHERE binding_request_id=?1 ORDER BY partition_ordinal,ordinal LIMIT 257",
    )?;
    let rows = statement.query_map(params![binding_request_id.as_bytes()], |row| {
        Ok(RawBoundMember {
            partition_ordinal: row.get(0)?,
            ordinal: row.get(1)?,
            obligation_id: row.get(2)?,
            work_unit_id: row.get(3)?,
            participation_id: row.get(4)?,
            weight: row.get(5)?,
            due_at_ms: row.get(6)?,
            assignment_id: row.get(7)?,
            assignment_revision: row.get(8)?,
            target_agent_id: row.get(9)?,
            target_session_id: row.get(10)?,
            criteria_digest: row.get(11)?,
            original_binding_state: row.get(12)?,
            original_binding_digest: row.get(13)?,
            original_binding_revision: row.get(14)?,
            original_binding_sequence: row.get(15)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn decode_declaration(raw: &RawBoundMember) -> Result<EvaluationManifestMember, OperationsError> {
    EvaluationManifestMember::new(
        forge_domain::EvaluationWorkUnitId::new(decode_uuid(
            raw.work_unit_id.clone(),
            "mission evidence source work unit",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evidence source work unit"))?,
        forge_domain::EvaluationObligationId::new(decode_uuid(
            raw.obligation_id.clone(),
            "mission evidence source obligation",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evidence source obligation"))?,
        forge_domain::AgentAssignmentId::from_uuid(decode_uuid(
            raw.assignment_id.clone(),
            "mission evidence source assignment",
        )?),
        forge_domain::AgentAssignmentRevision::new(raw.assignment_revision)
            .map_err(|_| OperationsError::Corrupt("mission evidence source assignment revision"))?,
        forge_domain::AgentInstanceId::from_uuid(decode_uuid(
            raw.target_agent_id.clone(),
            "mission evidence target Agent",
        )?),
        forge_domain::AgentSessionId::from_uuid(decode_uuid(
            raw.target_session_id.clone(),
            "mission evidence target Session",
        )?),
        decode_evaluation_digest(
            raw.criteria_digest.clone(),
            "mission evidence source criteria",
        )?,
    )
    .map_err(|_| OperationsError::Corrupt("mission evidence source declaration"))
}

fn decode_evaluation_digest(
    bytes: Vec<u8>,
    label: &'static str,
) -> Result<EvaluationDigest, OperationsError> {
    let digest: [u8; 32] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    EvaluationDigest::new(digest).map_err(|_| OperationsError::Corrupt(label))
}

fn decode_binding_state(value: i64) -> Result<MissionEvidenceBindingState, OperationsError> {
    match value {
        0 => Ok(MissionEvidenceBindingState::Verified),
        1 => Ok(MissionEvidenceBindingState::Pending),
        _ => Err(OperationsError::Corrupt("mission evidence binding state")),
    }
}

#[derive(Debug)]
struct RawIdentityProof {
    agent_id: Vec<u8>,
    session_id: Vec<u8>,
    resolution_state: i64,
    pending_reason: Option<i64>,
    candidate_registration_request_id: Option<Vec<u8>>,
    candidate_activation_receipt_id: Option<Vec<u8>>,
    participant_id: Option<Vec<u8>>,
    profile_id: Option<Vec<u8>>,
    participation_id: Option<Vec<u8>>,
    registration_kind: Option<i64>,
    registration_request_id: Option<Vec<u8>>,
    migration_id: Option<Vec<u8>>,
    destination_id: Option<Vec<u8>>,
    freeze_revision: Option<i64>,
    membership_commit_digest: Option<Vec<u8>>,
    activation_receipt_id: Option<Vec<u8>>,
    activated_at_ms: Option<i64>,
    profile_mapping_request_id: Option<Vec<u8>>,
    profile_revision: Option<i64>,
    session_revision: Option<i64>,
    participation_mapping_request_id: Option<Vec<u8>>,
    participant_membership_revision: Option<i64>,
    participation_mapped_at_ms: Option<i64>,
    rebind_request_id: Option<Vec<u8>>,
    rebind_revision: Option<i64>,
}

fn load_identity_resolution(
    connection: &Connection,
    owner_request_id: Uuid,
    role: i64,
    obligation_id: Uuid,
) -> Result<Option<MissionEvidenceIdentityResolution>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(agent_id,1,17),substr(session_id,1,17),resolution_state,pending_reason,substr(candidate_registration_request_id,1,17),substr(candidate_activation_receipt_id,1,17),substr(participant_id,1,17),substr(profile_id,1,17),substr(participation_id,1,17),registration_kind,substr(registration_request_id,1,17),substr(migration_id,1,17),substr(destination_id,1,17),freeze_revision,substr(membership_commit_digest,1,33),substr(activation_receipt_id,1,17),activated_at_ms,substr(profile_mapping_request_id,1,17),profile_revision,session_revision,substr(participation_mapping_request_id,1,17),participant_membership_revision,participation_mapped_at_ms,substr(rebind_request_id,1,17),rebind_revision FROM ops_mission_evidence_identity_proofs WHERE owner_request_id=?1 AND role=?2 AND obligation_id=?3 LIMIT 1",
            params![owner_request_id.as_bytes(), role, obligation_id.as_bytes()],
            |row| {
                Ok(RawIdentityProof {
                    agent_id: row.get(0)?,
                    session_id: row.get(1)?,
                    resolution_state: row.get(2)?,
                    pending_reason: row.get(3)?,
                    candidate_registration_request_id: row.get(4)?,
                    candidate_activation_receipt_id: row.get(5)?,
                    participant_id: row.get(6)?,
                    profile_id: row.get(7)?,
                    participation_id: row.get(8)?,
                    registration_kind: row.get(9)?,
                    registration_request_id: row.get(10)?,
                    migration_id: row.get(11)?,
                    destination_id: row.get(12)?,
                    freeze_revision: row.get(13)?,
                    membership_commit_digest: row.get(14)?,
                    activation_receipt_id: row.get(15)?,
                    activated_at_ms: row.get(16)?,
                    profile_mapping_request_id: row.get(17)?,
                    profile_revision: row.get(18)?,
                    session_revision: row.get(19)?,
                    participation_mapping_request_id: row.get(20)?,
                    participant_membership_revision: row.get(21)?,
                    participation_mapped_at_ms: row.get(22)?,
                    rebind_request_id: row.get(23)?,
                    rebind_revision: row.get(24)?,
                })
            },
        )
        .optional()?;
    raw.map(decode_identity_resolution).transpose()
}

fn decode_identity_resolution(
    raw: RawIdentityProof,
) -> Result<MissionEvidenceIdentityResolution, OperationsError> {
    let agent_id = forge_domain::AgentInstanceId::from_uuid(decode_uuid_ref(
        &raw.agent_id,
        "mission evidence identity Agent",
    )?);
    let session_id = forge_domain::AgentSessionId::from_uuid(decode_uuid_ref(
        &raw.session_id,
        "mission evidence identity Session",
    )?);
    match raw.resolution_state {
        1 => {
            let reason = super::codec::decode_identity_pending_reason(raw.pending_reason.ok_or(
                OperationsError::Corrupt("mission evidence pending identity reason"),
            )?)?;
            Ok(MissionEvidenceIdentityResolution::Pending {
                agent_id,
                session_id,
                reason,
                registration_request_id: raw
                    .candidate_registration_request_id
                    .map(|value| decode_uuid(value, "mission evidence candidate registration"))
                    .transpose()?,
                activation_receipt_id: raw
                    .candidate_activation_receipt_id
                    .map(|value| decode_uuid(value, "mission evidence candidate activation"))
                    .transpose()?,
            })
        }
        0 => {
            if raw.pending_reason.is_some()
                || raw.candidate_registration_request_id.is_some()
                || raw.candidate_activation_receipt_id.is_some()
            {
                return Err(OperationsError::Corrupt("mission evidence identity state"));
            }
            let proof = decode_identity_proof(raw, agent_id, session_id)?;
            Ok(MissionEvidenceIdentityResolution::Verified(Box::new(proof)))
        }
        _ => Err(OperationsError::Corrupt(
            "mission evidence identity resolution",
        )),
    }
}

fn decode_identity_proof(
    raw: RawIdentityProof,
    agent_id: forge_domain::AgentInstanceId,
    session_id: forge_domain::AgentSessionId,
) -> Result<MissionEvidenceIdentityProof, OperationsError> {
    let actor = MissionEvidenceIdentityActor {
        agent: agent_id,
        session: session_id,
        participant: decode_uuid(
            raw.participant_id
                .ok_or(OperationsError::Corrupt("mission evidence participant"))?,
            "mission evidence participant",
        )?,
    };
    let registration = MissionEvidenceRegistrationProvenance {
        kind: super::codec::decode_registration_kind(raw.registration_kind.ok_or(
            OperationsError::Corrupt("mission evidence registration kind"),
        )?)?,
        request_id: decode_uuid(
            raw.registration_request_id
                .ok_or(OperationsError::Corrupt("mission evidence registration"))?,
            "mission evidence registration",
        )?,
        migration_id: decode_uuid(
            raw.migration_id
                .ok_or(OperationsError::Corrupt("mission evidence migration"))?,
            "mission evidence migration",
        )?,
        destination_id: decode_uuid(
            raw.destination_id
                .ok_or(OperationsError::Corrupt("mission evidence destination"))?,
            "mission evidence destination",
        )?,
        freeze_revision: positive(
            raw.freeze_revision
                .ok_or(OperationsError::Corrupt("mission evidence freeze revision"))?,
            "mission evidence freeze revision",
        )?,
        membership_commit_digest: decode_digest(
            raw.membership_commit_digest
                .ok_or(OperationsError::Corrupt(
                    "mission evidence membership digest",
                ))?,
            "mission evidence membership digest",
        )?,
        activation_receipt_id: decode_uuid(
            raw.activation_receipt_id.ok_or(OperationsError::Corrupt(
                "mission evidence activation receipt",
            ))?,
            "mission evidence activation receipt",
        )?,
        activated_at_ms: nonnegative_i64(
            raw.activated_at_ms
                .ok_or(OperationsError::Corrupt("mission evidence activation time"))?,
            "mission evidence activation time",
        )?,
    };
    let profile = decode_profile_provenance(
        raw.profile_id,
        raw.profile_mapping_request_id,
        raw.profile_revision,
        raw.session_revision,
    )?;
    let participation = decode_participation_provenance(
        raw.participation_id,
        raw.participation_mapping_request_id,
        raw.participant_membership_revision,
        raw.participation_mapped_at_ms,
    )?;
    let replacement = match (raw.rebind_request_id, raw.rebind_revision) {
        (Some(request_id), Some(revision)) => Some(MissionEvidenceReplacementProvenance {
            rebind_request_id: decode_uuid(request_id, "mission evidence rebind request")?,
            rebind_revision: positive(revision, "mission evidence rebind revision")?,
        }),
        (None, None) => None,
        _ => {
            return Err(OperationsError::Corrupt(
                "mission evidence rebind provenance",
            ));
        }
    };
    MissionEvidenceIdentityProof::from_parts(&MissionEvidenceIdentityProofParts {
        actor,
        registration,
        profile,
        participation,
        replacement,
    })
    .map_err(|_| OperationsError::Corrupt("mission evidence identity proof"))
}

fn decode_profile_provenance(
    profile_id: Option<Vec<u8>>,
    mapping_request_id: Option<Vec<u8>>,
    profile_revision: Option<i64>,
    session_revision: Option<i64>,
) -> Result<Option<MissionEvidenceProfileProvenance>, OperationsError> {
    match (
        profile_id,
        mapping_request_id,
        profile_revision,
        session_revision,
    ) {
        (
            Some(profile_id),
            Some(mapping_request_id),
            Some(profile_revision),
            Some(session_revision),
        ) => Ok(Some(MissionEvidenceProfileProvenance {
            profile_id: decode_uuid(profile_id, "mission evidence Profile")?,
            mapping_request_id: decode_uuid(mapping_request_id, "mission evidence Profile link")?,
            profile_revision: positive(profile_revision, "mission evidence Profile revision")?,
            session_revision: positive(session_revision, "mission evidence Session revision")?,
        })),
        (None, None, None, None) => Ok(None),
        _ => Err(OperationsError::Corrupt(
            "mission evidence Profile provenance",
        )),
    }
}

fn decode_participation_provenance(
    participation_id: Option<Vec<u8>>,
    mapping_request_id: Option<Vec<u8>>,
    membership_revision: Option<i64>,
    mapped_at_ms: Option<i64>,
) -> Result<Option<MissionEvidenceParticipationProvenance>, OperationsError> {
    match (
        participation_id,
        mapping_request_id,
        membership_revision,
        mapped_at_ms,
    ) {
        (Some(participation_id), mapping_request_id, Some(membership_revision), mapped_at_ms) => {
            Ok(Some(MissionEvidenceParticipationProvenance {
                participation_id: decode_participation(participation_id)?,
                mapping_request_id: mapping_request_id
                    .map(|value| decode_uuid(value, "mission evidence participation map"))
                    .transpose()?,
                participant_membership_revision: positive(
                    membership_revision,
                    "mission evidence membership revision",
                )?,
                mapped_at_ms: mapped_at_ms
                    .map(|value| nonnegative_i64(value, "mission evidence participation map time"))
                    .transpose()?,
            }))
        }
        (None, None, None, None) => Ok(None),
        _ => Err(OperationsError::Corrupt(
            "mission evidence participation provenance",
        )),
    }
}

struct BindingSourceScope {
    backend_id: ForgeBackendId,
    workspace_id: WorkspaceId,
    migration_id: Uuid,
    destination_id: Uuid,
    freeze_revision: EvaluationRevision,
    delegation_digest: EvaluationDigest,
}

struct BindingDecodeMetadata {
    workspace_id: WorkspaceId,
    mission_id: crate::MissionId,
    plan_id: crate::MissionEvaluationPlanId,
    plan_version_id: crate::MissionEvaluationPlanVersionId,
    plan_revision: u64,
    plan_start_request_id: Uuid,
    plan_started_at_ms: i64,
    local_plan_digest: EvaluationDigest,
    local_criteria_digest: EvaluationDigest,
    recorded_at_ms: i64,
    member_count: usize,
    partition_count: usize,
    binding_state: MissionEvidenceBindingState,
    t0_commit_at_ms: i64,
    t1_submission_cutoff_ms: i64,
    t2_checkpoint_ms: i64,
}

struct BindingDecodeContext {
    request_id: Uuid,
    destination_id: Uuid,
    metadata: BindingDecodeMetadata,
    source: BindingSourceScope,
    frozen: crate::mission_evaluation::FrozenMissionEvaluationPlan,
    digests: super::MissionEvidencePlanDigests,
    forge_plan: EvaluationPlanBinding,
    deadline: TimestampMillis,
}

struct DecodedBindingManifests {
    bundle: super::MissionEvidenceManifestBundle,
    raw_members: Vec<RawBoundMember>,
    source_pending: bool,
}

struct DecodedBindingPartition {
    activity_id: forge_domain::ActivityId,
    manifest: EvaluationRegisteredManifest,
    pending: bool,
}

struct DecodedBoundTargetIdentity {
    identity: MissionEvidenceIdentityResolution,
    pending: bool,
}

fn decode_binding(
    connection: &Connection,
    destination_id: Uuid,
    request_id: Uuid,
    request: StoredRequest,
    raw: &RawBinding,
) -> Result<MissionEvidenceManifestBindingRecord, OperationsError> {
    let context = decode_binding_context(connection, destination_id, request_id, &request, raw)?;
    let (raw_partitions, raw_members) = load_binding_rows(connection, &context)?;
    let decoded = decode_binding_manifests(connection, &context, raw_partitions, raw_members)?;
    let (members, binding_state) = decode_binding_members(
        connection,
        &context,
        &decoded.bundle,
        &decoded.raw_members,
        decoded.source_pending,
    )?;
    if binding_state != context.metadata.binding_state {
        return Err(OperationsError::Corrupt("mission evidence binding state"));
    }
    Ok(binding_from_parts(NewBindingParts {
        request_id,
        workspace_id: context.metadata.workspace_id,
        mission_id: context.metadata.mission_id,
        plan_id: context.metadata.plan_id,
        plan_version_id: context.metadata.plan_version_id,
        plan_revision: context.metadata.plan_revision,
        plan_start_request_id: context.metadata.plan_start_request_id,
        plan_started_at_ms: context.metadata.plan_started_at_ms,
        t0_commit_at_ms: context.metadata.t0_commit_at_ms,
        t1_submission_cutoff_ms: context.metadata.t1_submission_cutoff_ms,
        t2_checkpoint_ms: context.metadata.t2_checkpoint_ms,
        manifest_bundle: decoded.bundle,
        plan_digests: context.digests,
        state: binding_state,
        members,
        actor: request.actor_receipt,
        recorded_at_ms: context.metadata.recorded_at_ms,
    }))
}

fn decode_binding_context(
    connection: &Connection,
    destination_id: Uuid,
    request_id: Uuid,
    request: &StoredRequest,
    raw: &RawBinding,
) -> Result<BindingDecodeContext, OperationsError> {
    let metadata = decode_binding_metadata(request, raw)?;
    let frozen = frozen_plan_in_transaction(
        connection,
        metadata.workspace_id,
        metadata.plan_id,
        metadata.plan_revision,
    )?;
    let digests = super::digest::derive(&frozen.version, &frozen.obligations)?;
    validate_binding_snapshot(&metadata, &frozen, &digests)?;
    let source = decode_binding_source_scope(raw, metadata.workspace_id, destination_id)?;
    let forge_plan = EvaluationPlanBinding::new(
        EvaluationPlanId::new(metadata.plan_id.into_uuid())
            .map_err(|_| OperationsError::Corrupt("mission evidence source plan"))?,
        EvaluationRevision::new(
            i64::try_from(metadata.plan_revision)
                .map_err(|_| OperationsError::Corrupt("mission evidence source plan revision"))?,
        )
        .map_err(|_| OperationsError::Corrupt("mission evidence source plan revision"))?,
        metadata.local_plan_digest,
    );
    let deadline = TimestampMillis::new(metadata.t1_submission_cutoff_ms)
        .map_err(|_| OperationsError::Corrupt("mission evidence T1"))?;
    Ok(BindingDecodeContext {
        request_id,
        destination_id,
        metadata,
        source,
        frozen,
        digests,
        forge_plan,
        deadline,
    })
}

fn decode_binding_metadata(
    request: &StoredRequest,
    raw: &RawBinding,
) -> Result<BindingDecodeMetadata, OperationsError> {
    let workspace_id = decode_workspace(raw.workspace_id.clone())?;
    let mission_id = crate::MissionId::from_uuid(decode_uuid(
        raw.mission_id.clone(),
        "mission evidence Mission",
    )?)?;
    let plan_id = crate::MissionEvaluationPlanId::from_uuid(decode_uuid(
        raw.plan_id.clone(),
        "mission evidence plan",
    )?)?;
    let plan_version_id = crate::MissionEvaluationPlanVersionId::from_uuid(decode_uuid(
        raw.plan_version_id.clone(),
        "mission evidence plan version",
    )?)?;
    let plan_revision = positive(raw.plan_revision, "mission evidence plan revision")?;
    let plan_start_request_id = decode_uuid(
        raw.plan_start_request_id.clone(),
        "mission evidence plan start request",
    )?;
    let plan_started_at_ms =
        nonnegative_i64(raw.plan_started_at_ms, "mission evidence plan start time")?;
    let local_plan_digest = decode_evaluation_digest(
        raw.local_plan_digest.clone(),
        "mission evidence local plan digest",
    )?;
    let local_criteria_digest = decode_evaluation_digest(
        raw.local_criteria_digest.clone(),
        "mission evidence local criteria digest",
    )?;
    let recorded_at_ms = nonnegative_i64(raw.recorded_at_ms, "mission evidence binding time")?;
    let member_count = usize::try_from(positive(
        raw.member_count,
        "mission evidence binding member count",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evidence binding member count"))?;
    let partition_count = usize::try_from(positive(
        raw.partition_count,
        "mission evidence partition count",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evidence partition count"))?;
    if recorded_at_ms != request.recorded_at_ms
        || workspace_id != request.workspace_id
        || plan_id != request.plan_id
        || plan_revision != request.plan_revision
        || raw.t0_commit_at_ms < 0
        || raw.t1_submission_cutoff_ms <= raw.t0_commit_at_ms
        || raw.t2_checkpoint_ms < raw.t1_submission_cutoff_ms
        || member_count > 256
        || partition_count > 256
    {
        return Err(OperationsError::Corrupt("mission evidence binding scope"));
    }
    Ok(BindingDecodeMetadata {
        workspace_id,
        mission_id,
        plan_id,
        plan_version_id,
        plan_revision,
        plan_start_request_id,
        plan_started_at_ms,
        local_plan_digest,
        local_criteria_digest,
        recorded_at_ms,
        member_count,
        partition_count,
        binding_state: decode_binding_state(raw.binding_state)?,
        t0_commit_at_ms: raw.t0_commit_at_ms,
        t1_submission_cutoff_ms: raw.t1_submission_cutoff_ms,
        t2_checkpoint_ms: raw.t2_checkpoint_ms,
    })
}

fn validate_binding_snapshot(
    metadata: &BindingDecodeMetadata,
    frozen: &crate::mission_evaluation::FrozenMissionEvaluationPlan,
    digests: &super::MissionEvidencePlanDigests,
) -> Result<(), OperationsError> {
    if frozen.version.mission_id() != metadata.mission_id
        || frozen.version.version_id() != metadata.plan_version_id
        || frozen.start.request_id() != metadata.plan_start_request_id
        || frozen.start.started_at_ms() != metadata.plan_started_at_ms
        || frozen.version.t0_commit_at_ms() != metadata.t0_commit_at_ms
        || frozen.version.t1_submission_cutoff_ms() != metadata.t1_submission_cutoff_ms
        || frozen.version.t2_checkpoint_ms() != metadata.t2_checkpoint_ms
        || digests.plan_digest() != metadata.local_plan_digest
        || digests.criteria_digest() != metadata.local_criteria_digest
    {
        return Err(OperationsError::Corrupt(
            "mission evidence frozen plan snapshot",
        ));
    }
    Ok(())
}

fn decode_binding_source_scope(
    raw: &RawBinding,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
) -> Result<BindingSourceScope, OperationsError> {
    let scope = BindingSourceScope {
        backend_id: ForgeBackendId::from_uuid(decode_uuid(
            raw.source_backend_id.clone(),
            "mission evidence source backend",
        )?),
        workspace_id: decode_workspace(raw.source_workspace_id.clone())?,
        migration_id: decode_uuid(
            raw.source_migration_id.clone(),
            "mission evidence source migration",
        )?,
        destination_id: decode_uuid(
            raw.source_destination_id.clone(),
            "mission evidence source destination",
        )?,
        freeze_revision: EvaluationRevision::new(raw.source_freeze_revision)
            .map_err(|_| OperationsError::Corrupt("mission evidence source freeze revision"))?,
        delegation_digest: decode_evaluation_digest(
            raw.source_delegation_digest.clone(),
            "mission evidence delegation digest",
        )?,
    };
    if scope.workspace_id != workspace_id || scope.destination_id != destination_id {
        return Err(OperationsError::Corrupt(
            "mission evidence source destination",
        ));
    }
    Ok(scope)
}

fn load_binding_rows(
    connection: &Connection,
    context: &BindingDecodeContext,
) -> Result<(Vec<RawBindingPartition>, Vec<RawBoundMember>), OperationsError> {
    let mut partitions = load_raw_binding_partitions(connection, context.request_id)?;
    let mut members = load_raw_binding_members(connection, context.request_id)?;
    if members.len() != context.metadata.member_count
        || members.len() != context.frozen.obligations.len()
        || partitions.len() != context.metadata.partition_count
    {
        return Err(OperationsError::Corrupt(
            "mission evidence binding partition or member count",
        ));
    }
    partitions.sort_by_key(|partition| partition.ordinal);
    members.sort_by_key(|member| (member.partition_ordinal, member.ordinal));
    Ok((partitions, members))
}

fn decode_binding_manifests(
    connection: &Connection,
    context: &BindingDecodeContext,
    raw_partitions: Vec<RawBindingPartition>,
    raw_members: Vec<RawBoundMember>,
) -> Result<DecodedBindingManifests, OperationsError> {
    let mut manifests = Vec::with_capacity(context.metadata.partition_count);
    let mut member_offset = 0_usize;
    let mut source_pending = false;
    let mut previous_activity = None;
    for (partition_ordinal, partition) in raw_partitions.into_iter().enumerate() {
        if partition.ordinal
            != i64::try_from(partition_ordinal)
                .map_err(|_| OperationsError::Corrupt("mission evidence partition ordinal"))?
        {
            return Err(OperationsError::Corrupt(
                "mission evidence partition ordinals",
            ));
        }
        let count = usize::try_from(positive(
            partition.member_count,
            "mission evidence partition member count",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evidence partition member count"))?;
        let end = member_offset
            .checked_add(count)
            .filter(|end| *end <= raw_members.len())
            .ok_or(OperationsError::Corrupt(
                "mission evidence partition member count",
            ))?;
        let decoded = decode_binding_partition(
            context,
            partition_ordinal,
            partition,
            &raw_members[member_offset..end],
        )?;
        if previous_activity.is_some_and(|previous| previous >= decoded.activity_id) {
            return Err(OperationsError::Corrupt(
                "mission evidence partition Activity order",
            ));
        }
        previous_activity = Some(decoded.activity_id);
        source_pending |= decoded.pending;
        manifests.push(decoded.manifest);
        member_offset = end;
    }
    if member_offset != raw_members.len() {
        return Err(OperationsError::Corrupt(
            "mission evidence unpartitioned binding members",
        ));
    }
    let bundle = super::MissionEvidenceManifestBundle::new(manifests)
        .map_err(|_| OperationsError::Corrupt("mission evidence manifest bundle"))?;
    let source = bundle
        .manifests()
        .first()
        .ok_or(OperationsError::Corrupt(
            "mission evidence empty manifest bundle",
        ))?
        .proposal()
        .source();
    persisted_source_import(
        connection,
        context.destination_id,
        context.metadata.workspace_id,
        source,
    )?;
    Ok(DecodedBindingManifests {
        bundle,
        raw_members,
        source_pending,
    })
}

fn decode_binding_partition(
    context: &BindingDecodeContext,
    partition_ordinal: usize,
    partition: RawBindingPartition,
    raw_members: &[RawBoundMember],
) -> Result<DecodedBindingPartition, OperationsError> {
    let activity_id = forge_domain::ActivityId::from_uuid(decode_uuid(
        partition.activity_id,
        "mission evidence source Activity",
    )?);
    let source_identity = EvaluationSourceIdentity::new(
        context.source.backend_id,
        context.source.workspace_id,
        activity_id,
        context.source.migration_id,
        context.source.destination_id,
        context.source.freeze_revision,
        context.source.delegation_digest,
    )
    .map_err(|_| OperationsError::Corrupt("mission evidence source identity"))?;
    let manifest_id = EvaluationManifestId::new(decode_uuid(
        partition.manifest_id,
        "mission evidence manifest",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evidence manifest"))?;
    let manifest_revision = EvaluationRevision::new(partition.manifest_revision)
        .map_err(|_| OperationsError::Corrupt("mission evidence manifest revision"))?;
    let manifest_digest = decode_evaluation_digest(
        partition.manifest_digest,
        "mission evidence manifest digest",
    )?;
    let manifest_source_sequence = EvaluationSourceSequence::new(partition.source_sequence)
        .map_err(|_| OperationsError::Corrupt("mission evidence manifest source sequence"))?;
    let seal_timing = super::codec::decode_timing(
        partition.seal_timing_kind,
        partition.seal_timing_ms,
        partition.seal_uncertainty,
    )?;
    let mut pending = matches!(seal_timing, EvaluationAdmissionTiming::Unknown(_))
        || matches!(seal_timing, EvaluationAdmissionTiming::Trusted(at) if at.get() > context.deadline.get());
    if raw_members.is_empty() {
        return Err(OperationsError::Corrupt(
            "mission evidence empty source partition",
        ));
    }
    let mut declarations = Vec::with_capacity(raw_members.len());
    let mut registered_members = Vec::with_capacity(raw_members.len());
    for (member_ordinal, member) in raw_members.iter().enumerate() {
        if member.partition_ordinal
            != i64::try_from(partition_ordinal)
                .map_err(|_| OperationsError::Corrupt("mission evidence member partition"))?
            || member.ordinal
                != i64::try_from(member_ordinal)
                    .map_err(|_| OperationsError::Corrupt("mission evidence member ordinal"))?
        {
            return Err(OperationsError::Corrupt(
                "mission evidence binding member ordinals",
            ));
        }
        let declaration = decode_declaration(member)?;
        let original_binding = super::codec::decode_original_binding(
            member.original_binding_state,
            member.original_binding_digest.clone(),
            member.original_binding_revision,
            member.original_binding_sequence,
        )?;
        pending |= original_binding == EvaluationOriginalBindingStatus::UnknownHistorical;
        declarations.push(declaration);
        registered_members.push(EvaluationRegisteredManifestMember::new(
            declaration,
            original_binding,
        ));
    }
    let proposal = forge_domain::EvaluationManifestProposal::new(
        manifest_id,
        manifest_revision,
        source_identity,
        context.forge_plan,
        context.deadline,
        declarations,
    )
    .map_err(|_| OperationsError::Corrupt("mission evidence manifest proposal"))?;
    if proposal.digest() != manifest_digest {
        return Err(OperationsError::Corrupt("mission evidence manifest digest"));
    }
    let manifest = EvaluationRegisteredManifest::from_source_records(
        proposal,
        registered_members,
        manifest_source_sequence,
        seal_timing,
    )
    .map_err(|_| OperationsError::Corrupt("mission evidence registered manifest"))?;
    Ok(DecodedBindingPartition {
        activity_id,
        manifest,
        pending,
    })
}

fn decode_binding_members(
    connection: &Connection,
    context: &BindingDecodeContext,
    bundle: &super::MissionEvidenceManifestBundle,
    raw_members: &[RawBoundMember],
    source_pending: bool,
) -> Result<(Vec<MissionEvidenceBoundMember>, MissionEvidenceBindingState), OperationsError> {
    let local_terms = context
        .frozen
        .obligations
        .iter()
        .map(|value| (value.obligation_id(), value))
        .collect::<BTreeMap<_, _>>();
    let local_criteria = context
        .digests
        .member_criteria_digests()
        .iter()
        .map(|value| (value.obligation_id(), *value))
        .collect::<BTreeMap<_, _>>();
    let mut members = Vec::with_capacity(raw_members.len());
    let mut seen_obligations = std::collections::BTreeSet::new();
    let mut seen_assignments = std::collections::BTreeSet::new();
    let mut pending = source_pending;
    for raw_member in raw_members {
        let (member, member_pending) = decode_bound_member(
            connection,
            context,
            bundle,
            raw_member,
            &local_terms,
            &local_criteria,
        )?;
        if !seen_obligations.insert(member.obligation_id())
            || !seen_assignments.insert(member.declaration().assignment_id())
        {
            return Err(OperationsError::Corrupt(
                "mission evidence duplicate bound member",
            ));
        }
        pending |= member_pending;
        members.push(member);
    }
    if seen_obligations.len() != context.frozen.obligations.len()
        || seen_assignments.len() != members.len()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence bound member cohort",
        ));
    }
    let state = if pending {
        MissionEvidenceBindingState::Pending
    } else {
        MissionEvidenceBindingState::Verified
    };
    Ok((members, state))
}

fn decode_bound_member(
    connection: &Connection,
    context: &BindingDecodeContext,
    bundle: &super::MissionEvidenceManifestBundle,
    raw_member: &RawBoundMember,
    local_terms: &BTreeMap<crate::MissionObligationId, &MissionObligationRecord>,
    local_criteria: &BTreeMap<
        crate::MissionObligationId,
        super::MissionEvidenceMemberCriteriaDigest,
    >,
) -> Result<(MissionEvidenceBoundMember, bool), OperationsError> {
    let declaration = decode_declaration(raw_member)?;
    let partition_ordinal = usize::try_from(raw_member.partition_ordinal)
        .map_err(|_| OperationsError::Corrupt("mission evidence member partition"))?;
    let activity_id = bundle
        .manifests()
        .get(partition_ordinal)
        .ok_or(OperationsError::Corrupt(
            "mission evidence member partition",
        ))?
        .proposal()
        .source()
        .activity_id();
    let registered_member = bundle
        .manifests()
        .get(partition_ordinal)
        .and_then(|manifest| {
            manifest
                .members()
                .iter()
                .find(|registered| registered.declaration() == declaration)
        })
        .ok_or(OperationsError::Corrupt(
            "mission evidence registered bound member",
        ))?;
    let obligation_id =
        crate::MissionObligationId::from_uuid(declaration.obligation_id().into_uuid())?;
    let work_unit_id = crate::MissionWorkUnitId::from_uuid(declaration.work_unit_id().into_uuid())?;
    let obligation = local_terms
        .get(&obligation_id)
        .copied()
        .ok_or(OperationsError::Corrupt(
            "mission evidence local obligation",
        ))?;
    let participation_id = obligation.participation_id();
    let weight = positive(raw_member.weight, "mission evidence member weight")?;
    let due_at_ms = nonnegative_i64(raw_member.due_at_ms, "mission evidence member due time")?;
    let criteria_digest = local_criteria
        .get(&obligation_id)
        .copied()
        .ok_or(OperationsError::Corrupt("mission evidence local criteria"))?;
    validate_bound_member_terms(
        raw_member,
        declaration,
        obligation,
        criteria_digest,
        weight,
        due_at_ms,
    )?;
    let target = decode_bound_target_identity(
        connection,
        context,
        obligation_id,
        declaration,
        participation_id,
        registered_member.binding(),
    )?;
    let member = MissionEvidenceBoundMember::from_parts(MissionEvidenceBoundMemberParts {
        partition_ordinal: u32::try_from(partition_ordinal)
            .map_err(|_| OperationsError::Bounds("mission evidence partition ordinal"))?,
        source_activity_id: activity_id,
        declaration,
        original_binding: registered_member.binding(),
        obligation_id,
        work_unit_id,
        participation_id,
        weight,
        due_at_ms,
        criteria_digest,
        target_identity: target.identity,
    })?;
    Ok((member, target.pending))
}

fn decode_bound_target_identity(
    connection: &Connection,
    context: &BindingDecodeContext,
    obligation_id: crate::MissionObligationId,
    declaration: EvaluationManifestMember,
    participation_id: WorkspaceParticipationId,
    original_binding: EvaluationOriginalBindingStatus,
) -> Result<DecodedBoundTargetIdentity, OperationsError> {
    let identity =
        load_identity_resolution(connection, context.request_id, 0, obligation_id.into_uuid())?
            .ok_or(OperationsError::Corrupt(
                "mission evidence target identity proof",
            ))?;
    if !resolution_matches_actor(
        &identity,
        declaration.target_agent_id(),
        declaration.target_session_id(),
    ) {
        return Err(OperationsError::Corrupt(
            "mission evidence target identity tuple",
        ));
    }
    if let MissionEvidenceIdentityResolution::Verified(proof) = &identity
        && proof
            .participation_id()
            .is_some_and(|value| value != participation_id)
    {
        return Err(OperationsError::Corrupt(
            "mission evidence target identity scope",
        ));
    }
    let pending = !matches!(&identity,
        MissionEvidenceIdentityResolution::Verified(proof)
            if proof.participation_id() == Some(participation_id))
        || original_binding == EvaluationOriginalBindingStatus::UnknownHistorical;
    Ok(DecodedBoundTargetIdentity { identity, pending })
}

fn validate_bound_member_terms(
    raw: &RawBoundMember,
    declaration: EvaluationManifestMember,
    obligation: &MissionObligationRecord,
    criteria_digest: super::MissionEvidenceMemberCriteriaDigest,
    weight: u64,
    due_at_ms: i64,
) -> Result<(), OperationsError> {
    if decode_uuid(raw.obligation_id.clone(), "mission evidence obligation")?
        != obligation.obligation_id().into_uuid()
        || decode_uuid(raw.work_unit_id.clone(), "mission evidence work unit")?
            != obligation.work_unit_id().into_uuid()
        || decode_participation(raw.participation_id.clone())? != obligation.participation_id()
        || weight != obligation.weight().get()
        || due_at_ms != obligation.due_at_ms()
        || decode_digest(
            raw.criteria_digest.clone(),
            "mission evidence criteria digest",
        )? != *criteria_digest.digest().as_bytes()
        || declaration.obligation_id().into_uuid() != obligation.obligation_id().into_uuid()
        || declaration.work_unit_id().into_uuid() != obligation.work_unit_id().into_uuid()
    {
        return Err(OperationsError::Corrupt(
            "mission evidence bound member terms",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredRequest {
    operation: i64,
    workspace_id: WorkspaceId,
    plan_id: crate::MissionEvaluationPlanId,
    plan_revision: u64,
    expected_head_revision: u64,
    payload_digest: [u8; 32],
    recorded_at_ms: i64,
    actor_identity: MissionActorIdentity,
    actor_receipt: MissionEvidenceActorReceipt,
}

#[derive(Clone, Copy)]
struct StoredRequestInsert<'a> {
    operation: i64,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    plan_id: Uuid,
    plan_revision: u64,
    expected_head_revision: u64,
    payload_digest: [u8; 32],
    actor: &'a MissionActorSnapshot,
    recorded_at_ms: i64,
}

fn insert_request(
    transaction: &Transaction<'_>,
    request: StoredRequestInsert<'_>,
) -> Result<(), OperationsError> {
    let StoredRequestInsert {
        operation,
        request_id,
        workspace_id,
        plan_id,
        plan_revision,
        expected_head_revision,
        payload_digest,
        actor,
        recorded_at_ms,
    } = request;
    if request_id.is_nil() || plan_id.is_nil() || payload_digest == [0; 32] {
        return Err(OperationsError::Conflict(
            "mission evidence request identity",
        ));
    }
    let plan_revision = i64::try_from(plan_revision)
        .map_err(|_| OperationsError::Bounds("mission evidence plan revision"))?;
    let expected_head_revision = i64::try_from(expected_head_revision)
        .map_err(|_| OperationsError::Bounds("mission evidence expected head"))?;
    actor.identity.validate()?;
    let identity = &actor.identity;
    let session_bytes = actor.session_id.map(|value| value.into_uuid().into_bytes());
    transaction.execute(
        "INSERT INTO ops_mission_evidence_requests(request_id,operation,workspace_id,plan_id,plan_revision,expected_head_revision,payload_digest,actor_kind,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,expected_leadership_revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            request_id.as_bytes(),
            operation,
            workspace_id.into_uuid().as_bytes(),
            plan_id.as_bytes(),
            plan_revision,
            expected_head_revision,
            payload_digest.as_slice(),
            identity.kind,
            identity.key.as_str(),
            actor.display_name.as_str(),
            identity
                .participation_id
                .map(|value| value.into_uuid().into_bytes()),
            identity.profile_id.map(Uuid::into_bytes),
            identity.participant_id.map(Uuid::into_bytes),
            session_bytes,
            actor.leadership_revision,
            recorded_at_ms,
        ],
    )?;
    Ok(())
}

fn load_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<StoredRequest>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),operation,substr(workspace_id,1,17),substr(plan_id,1,17),plan_revision,expected_head_revision,substr(payload_digest,1,33),actor_kind,substr(CAST(actor_key AS BLOB),1,257),substr(CAST(actor_display_name AS BLOB),1,129),substr(actor_participation_id,1,17),substr(actor_profile_id,1,17),substr(actor_participant_id,1,17),substr(actor_session_id,1,17),expected_leadership_revision,recorded_at_ms FROM ops_mission_evidence_requests WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, Vec<u8>>(9)?,
                    row.get::<_, Option<Vec<u8>>>(10)?,
                    row.get::<_, Option<Vec<u8>>>(11)?,
                    row.get::<_, Option<Vec<u8>>>(12)?,
                    row.get::<_, Option<Vec<u8>>>(13)?,
                    row.get::<_, Option<i64>>(14)?,
                    row.get::<_, i64>(15)?,
                ))
            },
        )
        .optional()?;
    let Some((
        stored_request_id,
        operation,
        workspace,
        plan,
        revision,
        expected_head,
        payload,
        actor_kind,
        actor_key,
        actor_display,
        participation,
        profile,
        participant,
        session,
        leadership_revision,
        recorded_at,
    )) = raw
    else {
        return Ok(None);
    };
    let stored_request_id = decode_uuid(stored_request_id, "mission evidence request id")?;
    if stored_request_id != request_id || recorded_at < 0 {
        return Err(OperationsError::Corrupt("mission evidence request row"));
    }
    let actor_key = decode_text(actor_key, "mission evidence actor key", 256)?;
    let actor_display = decode_text(actor_display, "mission evidence actor display", 128)?;
    let actor_identity = MissionActorIdentity {
        kind: actor_kind,
        key: actor_key,
        participation_id: participation.map(decode_participation).transpose()?,
        profile_id: profile
            .map(|value| decode_uuid(value, "mission evidence actor Profile"))
            .transpose()?,
        participant_id: participant
            .map(|value| decode_uuid(value, "mission evidence actor participant"))
            .transpose()?,
    };
    actor_identity.validate()?;
    let actor_receipt =
        MissionEvidenceActorReceipt::from_parts(MissionEvidenceActorReceiptParts {
            kind: decode_actor_kind(actor_kind)?,
            semantic_key: actor_identity.key.clone(),
            participation_id: actor_identity.participation_id,
            profile_id: actor_identity.profile_id,
            participant_id: actor_identity.participant_id,
            session_id: session.map(decode_session).transpose()?,
            display_name: actor_display,
            leadership_revision,
        })?;
    Ok(Some(StoredRequest {
        operation,
        workspace_id: decode_workspace(workspace)?,
        plan_id: crate::MissionEvaluationPlanId::from_uuid(decode_uuid(
            plan,
            "mission evidence request plan",
        )?)?,
        plan_revision: positive(revision, "mission evidence request plan revision")?,
        expected_head_revision: nonnegative(expected_head, "mission evidence request head")?,
        payload_digest: decode_digest(payload, "mission evidence request fingerprint")?,
        recorded_at_ms: recorded_at,
        actor_identity,
        actor_receipt,
    }))
}

#[derive(Clone, Copy)]
struct StoredRequestReplay {
    operation: i64,
    workspace_id: WorkspaceId,
    plan_id: Uuid,
    plan_revision: u64,
    expected_head_revision: u64,
    payload_digest: [u8; 32],
}

fn require_replay(
    existing: &StoredRequest,
    expected: StoredRequestReplay,
    actor: &MissionActorIdentity,
) -> Result<(), OperationsError> {
    if existing.operation != expected.operation
        || existing.workspace_id != expected.workspace_id
        || existing.plan_id.into_uuid() != expected.plan_id
        || existing.plan_revision != expected.plan_revision
        || existing.expected_head_revision != expected.expected_head_revision
        || existing.payload_digest != expected.payload_digest
        || !existing.actor_identity.agrees_with(actor)
    {
        return Err(OperationsError::Conflict("mission evidence request replay"));
    }
    Ok(())
}
