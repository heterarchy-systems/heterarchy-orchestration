//! Pure validation for transport-neutral source candidates.

use forge_domain::{ChangeSetState, GoalEvidenceRef};
use uuid::Uuid;

use crate::{EffectFactInput, OperationsError};

pub(crate) fn validate_effect_fact_inputs(
    facts: &[EffectFactInput<'_>],
) -> Result<(), OperationsError> {
    if facts.len() > super::MAX_PAGE_SIZE {
        return Err(OperationsError::Bounds("effect fact page"));
    }
    for fact in facts {
        validate_header(fact.sequence(), fact.migration_id(), fact.destination_id())?;
        match fact {
            EffectFactInput::Execution(fact) => validate_execution(fact)?,
            EffectFactInput::ChangeSet(fact) => validate_change_set(fact)?,
            EffectFactInput::AssignmentReviewed(fact) => validate_review(fact)?,
            EffectFactInput::AssignmentOffered(fact) => validate_offer(fact)?,
            EffectFactInput::AssignmentSubmitted(fact) => validate_submission(fact)?,
        }
    }
    Ok(())
}

fn validate_header(
    sequence: u64,
    migration_id: Uuid,
    destination_id: Uuid,
) -> Result<(), OperationsError> {
    require_sequence(sequence)?;
    require_id(migration_id, "effect fact migration id")?;
    require_id(destination_id, "effect fact destination id")?;
    Ok(())
}

fn validate_execution(fact: &super::ExecutionTerminalFact) -> Result<(), OperationsError> {
    require_id(fact.workspace_id.into_uuid(), "execution workspace id")?;
    require_id(fact.execution_id.into_uuid(), "execution id")?;
    require_id(fact.activity_id.into_uuid(), "execution activity id")?;
    require_id(fact.agent_id.into_uuid(), "execution agent id")?;
    require_id(fact.goal_id.into_uuid(), "execution goal id")?;
    require_id(fact.node_id.into_uuid(), "execution node id")?;
    if let Some(session_id) = fact.session_id {
        require_id(session_id.into_uuid(), "execution session id")?;
    }
    require_positive(fact.execution_revision, "execution revision")?;
    require_positive(fact.agent_revision, "execution agent revision")?;
    require_positive(fact.goal_revision, "execution goal revision")?;
    require_positive(fact.dag_revision, "execution DAG revision")?;
    require_positive(fact.node_revision, "execution node revision")?;
    require_timestamp(fact.observed_at_ms)?;
    if fact.execution_state.is_active() {
        return Err(OperationsError::Conflict("execution fact is not terminal"));
    }
    validate_verification(
        fact.verification_input_id,
        fact.verification_input_fingerprint,
        fact.verification_disposition,
        fact.verification_output_integrity,
        fact.verification_output_fingerprint,
    )
}

fn validate_change_set(fact: &super::ChangeSetTerminalFact) -> Result<(), OperationsError> {
    require_id(fact.workspace_id.into_uuid(), "change set workspace id")?;
    require_id(fact.change_set_id.into_uuid(), "change set id")?;
    require_id(fact.activity_id.into_uuid(), "change set activity id")?;
    require_id(
        fact.runtime_instance_id.into_uuid(),
        "change set runtime id",
    )?;
    require_id(fact.diff_id.into_uuid(), "change set diff id")?;
    require_positive(fact.workspace_revision, "change set workspace revision")?;
    require_positive(
        fact.activity_lease_revision,
        "change set activity lease revision",
    )?;
    require_positive(fact.change_set_revision, "change set revision")?;
    require_positive(fact.diff_revision, "change set diff revision")?;
    require_timestamp(fact.observed_at_ms)?;
    if fact.state == ChangeSetState::Starting {
        return Err(OperationsError::Conflict("change set fact is not terminal"));
    }
    match (fact.state, fact.failure_code) {
        (ChangeSetState::Applied, None) | (ChangeSetState::Failed, Some(_)) => {}
        _ => return Err(OperationsError::Conflict("change set failure pairing")),
    }
    if !matches!(
        fact.diff_state,
        forge_domain::DiffEvidenceState::Ready
            | forge_domain::DiffEvidenceState::SummaryOnly
            | forge_domain::DiffEvidenceState::Abandoned
    ) {
        return Err(OperationsError::Conflict("change set Diff state"));
    }
    validate_producer_pair(fact.producer_agent_id, fact.producer_session_id)
}

fn validate_review(fact: &super::AssignmentReviewedFact) -> Result<(), OperationsError> {
    validate_assignment_ids(
        fact.workspace_id,
        fact.assignment_id,
        fact.activity_id,
        fact.goal_id,
        fact.node_id,
        fact.target_agent_id,
        fact.target_session_id,
    )?;
    require_id(fact.reviewer_agent_id.into_uuid(), "reviewer agent id")?;
    if let Some(session_id) = fact.reviewer_session_id {
        require_id(session_id.into_uuid(), "reviewer session id")?;
    }
    require_timestamp(fact.observed_at_ms)
}

fn validate_offer(fact: &super::AssignmentOfferedFact) -> Result<(), OperationsError> {
    validate_assignment_ids(
        fact.workspace_id,
        fact.assignment_id,
        fact.activity_id,
        fact.goal_id,
        fact.node_id,
        fact.target_agent_id,
        fact.target_session_id,
    )?;
    require_timestamp(fact.offered_at_ms)
}

fn validate_submission(fact: &super::AssignmentSubmittedFact<'_>) -> Result<(), OperationsError> {
    validate_assignment_ids(
        fact.workspace_id,
        fact.assignment_id,
        fact.activity_id,
        fact.goal_id,
        fact.node_id,
        fact.target_agent_id,
        fact.target_session_id,
    )?;
    require_timestamp(fact.submitted_at_ms)?;
    let count = usize::from(fact.source_count);
    if count > 32 {
        return Err(OperationsError::Bounds("effect fact source count"));
    }
    match (count, fact.source_digests, fact.source_kinds) {
        (0, None, None) => {}
        (0, _, _) => return Err(OperationsError::Conflict("effect fact source shape")),
        (_, Some(_), Some(kinds)) => {
            if kinds[..count].iter().any(|kind| *kind > 1)
                || kinds[count..].iter().any(|kind| *kind != 0)
            {
                return Err(OperationsError::Corrupt("effect fact source kinds"));
            }
        }
        (_, _, _) => return Err(OperationsError::Conflict("effect fact source shape")),
    }
    if let Some(source_refs) = fact.source_refs {
        validate_source_refs(source_refs, fact.source_count, fact.source_kinds)?;
    }
    Ok(())
}

fn validate_source_refs(
    refs: &[super::AssignmentSubmittedSourceRef],
    source_count: u8,
    source_kinds: Option<[u8; 32]>,
) -> Result<(), OperationsError> {
    let count = usize::from(source_count);
    let Some(kinds) = source_kinds else {
        if refs.is_empty() && count == 0 {
            return Ok(());
        }
        return Err(OperationsError::Conflict("effect fact source refs"));
    };
    let expected = kinds[..count]
        .iter()
        .copied()
        .filter(|kind| *kind == 1)
        .count();
    if refs.len() != expected {
        return Err(OperationsError::Conflict("effect fact source refs"));
    }
    let mut index = 0_usize;
    let mut previous = None;
    for (ordinal, kind) in kinds[..count].iter().enumerate() {
        if *kind != 1 {
            continue;
        }
        let source_ref = refs
            .get(index)
            .ok_or(OperationsError::Conflict("effect fact source refs"))?;
        if usize::from(source_ref.ordinal()) != ordinal
            || previous.is_some_and(|value| value >= source_ref.ordinal())
        {
            return Err(OperationsError::Conflict("effect fact source refs"));
        }
        validate_evidence(source_ref.evidence())?;
        previous = Some(source_ref.ordinal());
        index += 1;
    }
    if index != refs.len() {
        return Err(OperationsError::Conflict("effect fact source refs"));
    }
    Ok(())
}

fn validate_assignment_ids(
    workspace_id: forge_domain::WorkspaceId,
    assignment_id: forge_domain::AgentAssignmentId,
    activity_id: forge_domain::ActivityId,
    goal_id: forge_domain::GoalId,
    node_id: forge_domain::DagNodeId,
    target_agent_id: forge_domain::AgentInstanceId,
    target_session_id: Option<forge_domain::AgentSessionId>,
) -> Result<(), OperationsError> {
    require_id(workspace_id.into_uuid(), "assignment workspace id")?;
    require_id(assignment_id.into_uuid(), "assignment id")?;
    require_id(activity_id.into_uuid(), "assignment activity id")?;
    require_id(goal_id.into_uuid(), "assignment goal id")?;
    require_id(node_id.into_uuid(), "assignment node id")?;
    require_id(target_agent_id.into_uuid(), "assignment target agent id")?;
    if let Some(session_id) = target_session_id {
        require_id(session_id.into_uuid(), "assignment target session id")?;
    }
    Ok(())
}

fn validate_verification(
    input_id: Option<Uuid>,
    input_fingerprint: Option<[u8; 32]>,
    disposition: Option<i64>,
    output_integrity: Option<i64>,
    output_fingerprint: Option<[u8; 32]>,
) -> Result<(), OperationsError> {
    match (
        input_id,
        input_fingerprint,
        disposition,
        output_integrity,
        output_fingerprint,
    ) {
        (None, None, None, None, None) => Ok(()),
        (Some(input_id), Some(_), Some(disposition), integrity, fingerprint) => {
            require_id(input_id, "verification input id")?;
            if !(0..=3).contains(&disposition) {
                return Err(OperationsError::Corrupt("verification disposition"));
            }
            if disposition == 3 {
                if integrity.is_some() != fingerprint.is_some() {
                    return Err(OperationsError::Conflict("verification output pairing"));
                }
            } else if integrity.is_none() || fingerprint.is_none() {
                return Err(OperationsError::Conflict("verification output required"));
            }
            if integrity.is_some_and(|value| !(0..=3).contains(&value)) {
                return Err(OperationsError::Corrupt("verification output integrity"));
            }
            Ok(())
        }
        _ => Err(OperationsError::Conflict("verification evidence shape")),
    }
}

fn validate_producer_pair(
    agent_id: Option<forge_domain::AgentInstanceId>,
    session_id: Option<forge_domain::AgentSessionId>,
) -> Result<(), OperationsError> {
    if let Some(agent_id) = agent_id {
        require_id(agent_id.into_uuid(), "producer agent id")?;
    }
    if let Some(session_id) = session_id {
        require_id(session_id.into_uuid(), "producer session id")?;
    }
    if agent_id.is_none() && session_id.is_some() {
        return Err(OperationsError::Conflict("effect fact producer pair"));
    }
    Ok(())
}

fn validate_evidence(value: GoalEvidenceRef) -> Result<(), OperationsError> {
    let id = match value {
        GoalEvidenceRef::ChangeSet(id) => id.into_uuid(),
        GoalEvidenceRef::Execution(id) => id.into_uuid(),
        GoalEvidenceRef::Approval(id) => id.into_uuid(),
        GoalEvidenceRef::Checkpoint(id) => id.into_uuid(),
    };
    require_id(id, "effect fact evidence id")
}

fn require_sequence(value: u64) -> Result<(), OperationsError> {
    if value == 0 || i64::try_from(value).is_err() {
        Err(OperationsError::Conflict("effect fact sequence"))
    } else {
        Ok(())
    }
}

fn require_positive(value: i64, label: &'static str) -> Result<(), OperationsError> {
    if value < 1 {
        Err(OperationsError::Conflict(label))
    } else {
        Ok(())
    }
}

fn require_timestamp(value: i64) -> Result<(), OperationsError> {
    if value < 0 {
        Err(OperationsError::Conflict("effect fact timestamp"))
    } else {
        Ok(())
    }
}

fn require_id(value: Uuid, label: &'static str) -> Result<(), OperationsError> {
    if value.is_nil() {
        Err(OperationsError::Conflict(label))
    } else {
        Ok(())
    }
}
