//! Convert validated Forge wire observations into the existing operations input port.

use forge_backend::ops::effect_facts::wire::{
    AssignmentReviewedOutcomeWire, ChangeSetStateWire, DiffEvidenceStateWire,
    EffectFactOutboxEntryWire, EffectFactOutboxResponse, EffectFactWire, ExecutionStateWire,
    FileMutationFailureCodeWire, GoalEvidenceKindWire, SourceKindWire, VerificationEvidenceWire,
    VerificationOutputIntegrityWire, VerificationRunDispositionWire, WorkCreditBudgetWire,
};
use forge_domain::{
    ActivityId, AgentAssignmentId, AgentInstanceId, AgentSessionId, ChangeSetId, ChangeSetState,
    DagNodeId, DiffEvidenceState, DiffId, ExecutionId, ExecutionState, FileMutationFailureCode,
    GoalEvidenceRef, GoalId, RuntimeInstanceId, WorkCreditBudget, WorkspaceId,
};
use heterarchy_orchestration_operations::AssignmentSubmittedSourceRef;
use heterarchy_orchestration_operations::{
    AssignmentOfferedFact, AssignmentReviewedFact, AssignmentSubmittedFact, ChangeSetTerminalFact,
    EffectFactConsumerProgress, EffectFactInput, ExecutionTerminalFact, OperationsCatalog,
    OperationsError,
};
use uuid::Uuid;

const MAX_PAGE_SIZE: usize = 64;

/// Convert one validated source page and use the sole atomic Operations consumer.
///
/// # Errors
/// Rejects invalid bounds, source data and catalog commit/replay conflicts.
pub fn apply_page(
    catalog: &mut OperationsCatalog,
    page: &EffectFactOutboxResponse,
) -> Result<EffectFactConsumerProgress, OperationsError> {
    with_inputs(page, |migration, inputs| {
        catalog.apply_effect_fact_inputs(migration, inputs)
    })
}

/// Apply wire observations through the shared Runtime consumer owner.
///
/// # Errors
/// Rejects invalid observations and owner persistence failures.
pub fn apply_runtime_page(
    runtime: &heterarchy_orchestration_runtime::OrchestrationRuntime,
    page: &EffectFactOutboxResponse,
) -> Result<EffectFactConsumerProgress, heterarchy_orchestration_runtime::RuntimeError> {
    with_inputs(page, |migration, inputs| {
        runtime.apply_coordination_effect_inputs(migration, inputs)
    })
}

fn with_inputs<F, E>(
    page: &EffectFactOutboxResponse,
    apply: F,
) -> Result<EffectFactConsumerProgress, E>
where
    E: From<OperationsError>,
    F: FnOnce(Uuid, &[EffectFactInput<'_>]) -> Result<EffectFactConsumerProgress, E>,
{
    if page.entries.len() > MAX_PAGE_SIZE {
        return Err(OperationsError::Bounds("effect fact page").into());
    }
    let owned_source_refs = page
        .entries
        .iter()
        .map(owned_source_refs)
        .collect::<Result<Vec<_>, _>>()?;
    let inputs = page
        .entries
        .iter()
        .zip(owned_source_refs.iter())
        .map(|(entry, source_refs)| input_from_entry(entry, source_refs.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;
    apply(page.migration_id, &inputs)
}

fn input_from_entry<'a>(
    entry: &'a EffectFactOutboxEntryWire,
    submitted_source_refs: Option<&'a [AssignmentSubmittedSourceRef]>,
) -> Result<EffectFactInput<'a>, OperationsError> {
    match &entry.fact {
        EffectFactWire::ExecutionTerminal { .. } => execution_fact_input(entry),
        EffectFactWire::ChangeSetTerminal { .. } => change_set_fact_input(entry),
        EffectFactWire::AssignmentReviewed { .. } => reviewed_fact_input(entry),
        EffectFactWire::AssignmentOffered { .. } => offered_fact_input(entry),
        EffectFactWire::AssignmentSubmitted { .. } => {
            submitted_fact_input(entry, submitted_source_refs)
        }
    }
}

fn shared_fact_headers(entry: &EffectFactOutboxEntryWire) -> (u64, Uuid, Uuid, WorkspaceId) {
    (
        entry.sequence,
        entry.migration_id,
        entry.destination_id,
        WorkspaceId::from_uuid(entry.workspace_id),
    )
}

fn execution_fact_input(
    entry: &EffectFactOutboxEntryWire,
) -> Result<EffectFactInput<'_>, OperationsError> {
    let EffectFactWire::ExecutionTerminal {
        execution_id,
        activity_id,
        execution_revision,
        state,
        observed_at_ms,
        agent_id,
        agent_revision,
        session_id,
        goal_id,
        goal_revision,
        dag_revision,
        node_id,
        node_revision,
        verification,
    } = &entry.fact
    else {
        return Err(OperationsError::Conflict("effect fact kind"));
    };
    let (
        verification_input_id,
        verification_input_fingerprint,
        verification_disposition,
        verification_output_integrity,
        verification_output_fingerprint,
    ) = verification_fields(verification.as_ref())?;
    let (sequence, migration_id, destination_id, workspace_id) = shared_fact_headers(entry);
    Ok(EffectFactInput::Execution(ExecutionTerminalFact {
        sequence,
        migration_id,
        destination_id,
        workspace_id,
        execution_id: ExecutionId::from_uuid(*execution_id),
        activity_id: ActivityId::from_uuid(*activity_id),
        execution_revision: *execution_revision,
        execution_state: execution_state(*state),
        observed_at_ms: *observed_at_ms,
        agent_id: AgentInstanceId::from_uuid(*agent_id),
        agent_revision: *agent_revision,
        session_id: session_id.map(AgentSessionId::from_uuid),
        goal_id: GoalId::from_uuid(*goal_id),
        goal_revision: *goal_revision,
        dag_revision: *dag_revision,
        node_id: DagNodeId::from_uuid(*node_id),
        node_revision: *node_revision,
        verification_input_id,
        verification_input_fingerprint,
        verification_disposition,
        verification_output_integrity,
        verification_output_fingerprint,
    }))
}
fn change_set_fact_input(
    entry: &EffectFactOutboxEntryWire,
) -> Result<EffectFactInput<'_>, OperationsError> {
    let EffectFactWire::ChangeSetTerminal {
        change_set_id,
        workspace_id,
        workspace_revision,
        activity_id,
        runtime_instance_id,
        activity_lease_revision,
        change_set_revision,
        state,
        failure_code,
        diff_id,
        diff_state,
        diff_revision,
        observed_at_ms,
        producer_agent_id,
        producer_session_id,
    } = &entry.fact
    else {
        return Err(OperationsError::Conflict("effect fact kind"));
    };
    let (sequence, migration_id, destination_id, _) = shared_fact_headers(entry);
    Ok(EffectFactInput::ChangeSet(ChangeSetTerminalFact {
        sequence,
        migration_id,
        destination_id,
        workspace_id: WorkspaceId::from_uuid(*workspace_id),
        change_set_id: ChangeSetId::from_uuid(*change_set_id),
        workspace_revision: *workspace_revision,
        activity_id: ActivityId::from_uuid(*activity_id),
        runtime_instance_id: RuntimeInstanceId::from_uuid(*runtime_instance_id),
        activity_lease_revision: *activity_lease_revision,
        change_set_revision: *change_set_revision,
        state: change_set_state(*state),
        failure_code: failure_code.map(file_failure),
        diff_id: DiffId::from_uuid(*diff_id),
        diff_state: decode_diff_state(*diff_state),
        diff_revision: *diff_revision,
        observed_at_ms: *observed_at_ms,
        producer_agent_id: producer_agent_id.map(AgentInstanceId::from_uuid),
        producer_session_id: producer_session_id.map(AgentSessionId::from_uuid),
    }))
}

fn reviewed_fact_input(
    entry: &EffectFactOutboxEntryWire,
) -> Result<EffectFactInput<'_>, OperationsError> {
    let EffectFactWire::AssignmentReviewed {
        assignment_id,
        workspace_id: _,
        activity_id,
        goal_id,
        node_id,
        target_agent_id,
        target_session_id,
        outcome,
        credit_budget,
        reviewer_agent_id,
        reviewer_session_id,
        result_digest,
        observed_at_ms,
    } = &entry.fact
    else {
        return Err(OperationsError::Conflict("effect fact kind"));
    };
    let (sequence, migration_id, destination_id, workspace_id) = shared_fact_headers(entry);
    Ok(EffectFactInput::AssignmentReviewed(
        AssignmentReviewedFact {
            sequence,
            migration_id,
            destination_id,
            workspace_id,
            assignment_id: AgentAssignmentId::from_uuid(*assignment_id),
            activity_id: ActivityId::from_uuid(*activity_id),
            goal_id: GoalId::from_uuid(*goal_id),
            node_id: DagNodeId::from_uuid(*node_id),
            target_agent_id: AgentInstanceId::from_uuid(*target_agent_id),
            target_session_id: target_session_id.map(AgentSessionId::from_uuid),
            outcome: review_outcome(*outcome),
            credit_budget: credit_budget.map(decode_credit_budget),
            reviewer_agent_id: AgentInstanceId::from_uuid(*reviewer_agent_id),
            reviewer_session_id: reviewer_session_id.map(AgentSessionId::from_uuid),
            result_digest: result_digest.as_deref().map(decode_digest).transpose()?,
            observed_at_ms: *observed_at_ms,
        },
    ))
}

fn offered_fact_input(
    entry: &EffectFactOutboxEntryWire,
) -> Result<EffectFactInput<'_>, OperationsError> {
    let EffectFactWire::AssignmentOffered {
        assignment_id,
        workspace_id: _,
        activity_id,
        goal_id,
        node_id,
        target_agent_id,
        target_session_id,
        credit_budget,
        offered_at_ms,
    } = &entry.fact
    else {
        return Err(OperationsError::Conflict("effect fact kind"));
    };
    let (sequence, migration_id, destination_id, workspace_id) = shared_fact_headers(entry);
    Ok(EffectFactInput::AssignmentOffered(AssignmentOfferedFact {
        sequence,
        migration_id,
        destination_id,
        workspace_id,
        assignment_id: AgentAssignmentId::from_uuid(*assignment_id),
        activity_id: ActivityId::from_uuid(*activity_id),
        goal_id: GoalId::from_uuid(*goal_id),
        node_id: DagNodeId::from_uuid(*node_id),
        target_agent_id: AgentInstanceId::from_uuid(*target_agent_id),
        target_session_id: target_session_id.map(AgentSessionId::from_uuid),
        credit_budget: credit_budget.map(decode_credit_budget),
        offered_at_ms: *offered_at_ms,
    }))
}

fn submitted_fact_input<'a>(
    entry: &'a EffectFactOutboxEntryWire,
    submitted_source_refs: Option<&'a [AssignmentSubmittedSourceRef]>,
) -> Result<EffectFactInput<'a>, OperationsError> {
    let EffectFactWire::AssignmentSubmitted {
        assignment_id,
        workspace_id: _,
        activity_id,
        goal_id,
        node_id,
        target_agent_id,
        target_session_id,
        source_count,
        source_digests,
        source_kinds,
        result_digest,
        submitted_at_ms,
    } = &entry.fact
    else {
        return Err(OperationsError::Conflict("effect fact kind"));
    };
    let (sequence, migration_id, destination_id, workspace_id) = shared_fact_headers(entry);
    Ok(EffectFactInput::AssignmentSubmitted(
        AssignmentSubmittedFact {
            sequence,
            migration_id,
            destination_id,
            workspace_id,
            assignment_id: AgentAssignmentId::from_uuid(*assignment_id),
            activity_id: ActivityId::from_uuid(*activity_id),
            goal_id: GoalId::from_uuid(*goal_id),
            node_id: DagNodeId::from_uuid(*node_id),
            target_agent_id: AgentInstanceId::from_uuid(*target_agent_id),
            target_session_id: target_session_id.map(AgentSessionId::from_uuid),
            source_count: *source_count,
            source_digests: source_digests.as_deref().map(decode_digest).transpose()?,
            source_kinds: source_kinds
                .as_deref()
                .map(|kinds| source_kinds_array(kinds, *source_count))
                .transpose()?,
            source_refs: submitted_source_refs,
            result_digest: result_digest.as_deref().map(decode_digest).transpose()?,
            submitted_at_ms: *submitted_at_ms,
        },
    ))
}

fn owned_source_refs(
    entry: &EffectFactOutboxEntryWire,
) -> Result<Option<Box<[AssignmentSubmittedSourceRef]>>, OperationsError> {
    entry
        .submitted_source_refs
        .as_ref()
        .map(|refs| {
            refs.iter()
                .map(|source_ref| {
                    AssignmentSubmittedSourceRef::new(
                        source_ref.ordinal,
                        evidence_ref(&source_ref.evidence),
                    )
                    .map_err(|_| OperationsError::Bounds("effect fact source reference"))
                })
                .collect::<Result<Vec<_>, OperationsError>>()
                .map(Vec::into_boxed_slice)
        })
        .transpose()
}

/// Durable verification evidence columns decoded from the wire evidence:
/// input id, input fingerprint, disposition, output integrity, output fingerprint.
type VerificationFields = (
    Option<Uuid>,
    Option<[u8; 32]>,
    Option<i64>,
    Option<i64>,
    Option<[u8; 32]>,
);

fn verification_fields(
    value: Option<&VerificationEvidenceWire>,
) -> Result<VerificationFields, OperationsError> {
    let Some(value) = value else {
        return Ok((None, None, None, None, None));
    };
    Ok((
        Some(value.input_id),
        Some(decode_digest(&value.input_fingerprint)?),
        Some(verification_disposition(value.disposition)),
        value.output_integrity.map(verification_integrity),
        value
            .output_fingerprint
            .as_deref()
            .map(decode_digest)
            .transpose()?,
    ))
}

fn source_kinds_array(values: &[SourceKindWire], count: u8) -> Result<[u8; 32], OperationsError> {
    if values.len() != usize::from(count) || values.len() > 32 {
        return Err(OperationsError::Conflict("effect fact source kinds"));
    }
    let mut result = [0_u8; 32];
    for (index, value) in values.iter().enumerate() {
        result[index] = match value {
            SourceKindWire::Url => 0,
            SourceKindWire::Provenance => 1,
        };
    }
    Ok(result)
}

fn evidence_ref(
    value: &forge_backend::ops::effect_facts::wire::GoalEvidenceWire,
) -> GoalEvidenceRef {
    match value.kind {
        GoalEvidenceKindWire::ChangeSet => {
            GoalEvidenceRef::ChangeSet(ChangeSetId::from_uuid(value.id))
        }
        GoalEvidenceKindWire::Execution => {
            GoalEvidenceRef::Execution(ExecutionId::from_uuid(value.id))
        }
        GoalEvidenceKindWire::Approval => {
            GoalEvidenceRef::Approval(forge_domain::ApprovalRequestId::from_uuid(value.id))
        }
        GoalEvidenceKindWire::Checkpoint => {
            GoalEvidenceRef::Checkpoint(forge_domain::CheckpointId::from_uuid(value.id))
        }
    }
}

fn execution_state(value: ExecutionStateWire) -> ExecutionState {
    match value {
        ExecutionStateWire::Starting => ExecutionState::Starting,
        ExecutionStateWire::Running => ExecutionState::Running,
        ExecutionStateWire::Completed => ExecutionState::Completed,
        ExecutionStateWire::Failed => ExecutionState::Failed,
        ExecutionStateWire::TimedOut => ExecutionState::TimedOut,
        ExecutionStateWire::Cancelled => ExecutionState::Cancelled,
        ExecutionStateWire::Interrupted => ExecutionState::Interrupted,
    }
}

fn change_set_state(value: ChangeSetStateWire) -> ChangeSetState {
    match value {
        ChangeSetStateWire::Starting => ChangeSetState::Starting,
        ChangeSetStateWire::Applied => ChangeSetState::Applied,
        ChangeSetStateWire::Failed => ChangeSetState::Failed,
    }
}

fn decode_diff_state(value: DiffEvidenceStateWire) -> DiffEvidenceState {
    match value {
        DiffEvidenceStateWire::Intent => DiffEvidenceState::Intent,
        DiffEvidenceStateWire::Ready => DiffEvidenceState::Ready,
        DiffEvidenceStateWire::SummaryOnly => DiffEvidenceState::SummaryOnly,
        DiffEvidenceStateWire::Abandoned => DiffEvidenceState::Abandoned,
        DiffEvidenceStateWire::PurgePending => DiffEvidenceState::PurgePending,
        DiffEvidenceStateWire::Tombstoned => DiffEvidenceState::Tombstoned,
    }
}

fn file_failure(value: FileMutationFailureCodeWire) -> FileMutationFailureCode {
    match value {
        FileMutationFailureCodeWire::InvalidPath => FileMutationFailureCode::InvalidPath,
        FileMutationFailureCodeWire::ProtectedPath => FileMutationFailureCode::ProtectedPath,
        FileMutationFailureCodeWire::PathConflict => FileMutationFailureCode::PathConflict,
        FileMutationFailureCodeWire::StaleWrite => FileMutationFailureCode::StaleWrite,
        FileMutationFailureCodeWire::CapacityExceeded => FileMutationFailureCode::CapacityExceeded,
        FileMutationFailureCodeWire::IoFailure => FileMutationFailureCode::IoFailure,
    }
}

fn review_outcome(
    value: AssignmentReviewedOutcomeWire,
) -> heterarchy_orchestration_operations::AssignmentReviewedOutcome {
    match value {
        AssignmentReviewedOutcomeWire::Accepted => {
            heterarchy_orchestration_operations::AssignmentReviewedOutcome::Accepted
        }
        AssignmentReviewedOutcomeWire::Rejected => {
            heterarchy_orchestration_operations::AssignmentReviewedOutcome::Rejected
        }
    }
}

fn decode_credit_budget(value: WorkCreditBudgetWire) -> WorkCreditBudget {
    match value {
        WorkCreditBudgetWire::One => WorkCreditBudget::One,
        WorkCreditBudgetWire::Three => WorkCreditBudget::Three,
        WorkCreditBudgetWire::Five => WorkCreditBudget::Five,
    }
}

fn verification_disposition(value: VerificationRunDispositionWire) -> i64 {
    match value {
        VerificationRunDispositionWire::Accepted => 0,
        VerificationRunDispositionWire::Rejected => 1,
        VerificationRunDispositionWire::Incomplete => 2,
        VerificationRunDispositionWire::RecoveryRequired => 3,
    }
}

fn verification_integrity(value: VerificationOutputIntegrityWire) -> i64 {
    match value {
        VerificationOutputIntegrityWire::Complete => 0,
        VerificationOutputIntegrityWire::Gapped => 1,
        VerificationOutputIntegrityWire::Truncated => 2,
        VerificationOutputIntegrityWire::GappedAndTruncated => 3,
    }
}

fn decode_digest(value: &str) -> Result<[u8; 32], OperationsError> {
    if value.len() != 64 {
        return Err(OperationsError::Conflict("effect fact digest"));
    }
    let bytes = value.as_bytes();
    let mut result = [0_u8; 32];
    for (index, [high, low]) in bytes.as_chunks::<2>().0.iter().enumerate() {
        let high = hex_nibble(*high).ok_or(OperationsError::Conflict("effect fact digest"))?;
        let low = hex_nibble(*low).ok_or(OperationsError::Conflict("effect fact digest"))?;
        result[index] = (high << 4) | low;
    }
    Ok(result)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
