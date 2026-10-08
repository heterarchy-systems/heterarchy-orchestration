use super::*;
use forge_domain::{
    ActivityId, AgentFocusNode, DagHeadRevision, DagNodeId, DagNodeRevision, DagRevisionNumber,
    DirectTokenId, GoalAttribution, GoalId, GoalRevision, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::DepartureObservationParts;

#[test]
fn handoff_readback_rejects_changed_fences_and_historical_source_only_digest()
-> Result<(), Box<dyn std::error::Error>> {
    let (target, parts) = fixture()?;
    let source = target.source;
    let plan = target.expected_plan;
    let principal = AuthenticatedPrincipal::TrustedStdio;
    let receipt = DepartureObservation::new(parts)?;
    target.validate_observation(principal, &receipt)?;
    let changed_plan = CoordinationSnapshot::new(
        plan.goal(),
        Some(DagRevisionNumber::new(2)?),
        Some(DagHeadRevision::new(2)?),
        None,
    )?;
    let node_plan = CoordinationSnapshot::new(
        plan.goal(),
        plan.dag_revision(),
        plan.dag_head_revision(),
        Some(AgentFocusNode::new(
            DagNodeId::from_uuid(Uuid::from_u128(14)),
            DagNodeRevision::new(1)?,
        )),
    )?;
    let goal_plan = CoordinationSnapshot::new(
        Some(GoalAttribution::new(
            GoalId::from_uuid(Uuid::from_u128(15)),
            GoalRevision::new(2)?,
        )),
        plan.dag_revision(),
        plan.dag_head_revision(),
        None,
    )?;
    for changed in [
        HandoffTarget {
            expected_target_revision: AgentInstanceRevision::new(2)?,
            ..target
        },
        HandoffTarget {
            expected_coordinator_revision: ActivityCoordinatorRevision::new(2)?,
            ..target
        },
        HandoffTarget {
            expected_plan: changed_plan,
            ..target
        },
        HandoffTarget {
            expected_plan: node_plan,
            ..target
        },
        HandoffTarget {
            expected_plan: goal_plan,
            ..target
        },
        HandoffTarget {
            source: DepartureTarget {
                expected_agent_revision: AgentInstanceRevision::new(2)?,
                ..source
            },
            ..target
        },
        HandoffTarget {
            request_id: Uuid::from_u128(16),
            ..target
        },
        HandoffTarget {
            target_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(12)),
            ..target
        },
        HandoffTarget {
            target_session_id: AgentSessionId::from_uuid(Uuid::from_u128(13)),
            ..target
        },
    ] {
        assert!(matches!(
            changed.validate_observation(principal, &receipt),
            Err(RuntimeError::Conflict)
        ));
    }
    assert!(matches!(
        target.validate_observation(
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(17))),
            &receipt
        ),
        Err(RuntimeError::Conflict)
    ));
    let historical = DepartureObservation::new(DepartureObservationParts {
        identity_digest: departure_identity_digest(
            source.workspace_id,
            source.activity_id,
            source.agent_id,
            source.session_id,
            principal,
            target.request_id,
        ),
        ..parts
    })?;
    assert!(matches!(
        target.validate_observation(principal, &historical),
        Err(RuntimeError::Conflict)
    ));
    Ok(())
}

fn fixture() -> Result<(HandoffTarget, DepartureObservationParts), Box<dyn std::error::Error>> {
    let source = DepartureTarget::new(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        Uuid::from_u128(3),
        Uuid::from_u128(4),
        1,
    )?;
    let plan = CoordinationSnapshot::new(
        Some(GoalAttribution::new(
            GoalId::from_uuid(Uuid::from_u128(9)),
            GoalRevision::new(1)?,
        )),
        Some(DagRevisionNumber::new(1)?),
        Some(DagHeadRevision::new(1)?),
        None,
    )?;
    let target = HandoffTarget::new(
        source,
        Uuid::from_u128(5),
        Uuid::from_u128(6),
        1,
        1,
        plan,
        Uuid::from_u128(7),
    )?;
    let principal = AuthenticatedPrincipal::TrustedStdio;
    let parts = DepartureObservationParts {
        request_id: target.request_id,
        migration_id: Uuid::from_u128(10),
        destination_id: Uuid::from_u128(11),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(1)),
        activity_id: ActivityId::from_uuid(Uuid::from_u128(2)),
        kind: AgentDepartureKind::Handoff,
        agent_id: source.agent_id,
        session_id: source.session_id,
        expected_agent_revision: source.expected_agent_revision,
        result_agent_revision: AgentInstanceRevision::new(2)?,
        target_agent_id: Some(target.target_agent_id),
        target_session_id: Some(target.target_session_id),
        result_coordinator_revision: Some(ActivityCoordinatorRevision::new(2)?),
        identity_digest: target.identity_digest(principal),
        reconciled_at: TimestampMillis::new(100)?,
    };
    Ok((target, parts))
}
