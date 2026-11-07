//! Caller regressions with synthetic Home reservation rows. These prove the
//! existing mutation boundaries, not authenticated Home admission or Source Apply.

use super::*;

const HELD: &str = "Node has a pending or active Home acceptance work reservation";

#[test]
fn original_terminal_does_not_release_home_successor_reservation() -> Result<(), Box<dyn Error>> {
    let mut fixture = fixture()?;
    let original = command(&fixture, 0xB010, Some(fixture.team))?;
    let original_receipt = fixture.catalog.record_assignment_affinity(&original)?;
    assert_eq!(
        original_receipt.disposition(),
        AssignmentAffinityDisposition::Pending
    );
    let acceptance = crate::home_acceptance::tests::seed_pending_work_reservation(
        &fixture.catalog.connection,
        fixture.scope,
        original.assignment_id,
        AgentAssignmentId::from_uuid(Uuid::from_u128(0xB011)),
    )?;
    let cancelled = registered_assignment(
        &fixture,
        original.assignment_id,
        AgentAssignmentState::Cancelled,
    )?;
    let terminal = fixture
        .catalog
        .reconcile_assignment_affinity(&cancelled, fixture.scope)?;
    assert_eq!(
        terminal.disposition(),
        AssignmentAffinityDisposition::Terminal
    );
    // Historical replay remains available; it must not perform fresh admission.
    let replay = fixture.catalog.record_assignment_affinity(&original)?;
    assert!(replay.replayed());
    assert_eq!(
        replay.disposition(),
        AssignmentAffinityDisposition::Terminal
    );

    let competitor = command(&fixture, 0xB012, Some(fixture.team))?;
    assert!(matches!(
        fixture.catalog.record_assignment_affinity(&competitor),
        Err(OperationsError::Conflict(HELD))
    ));
    assert!(matches!(
        fixture
            .catalog
            .set_node_team_binding(binding_command(fixture.scope, Some(1), None, 101,)?),
        Err(OperationsError::Conflict(HELD))
    ));
    assert!(
        fixture
            .catalog
            .read_assignment_affinity(fixture.workspace, competitor.assignment_id)?
            .is_none()
    );
    let state: i64 = fixture.catalog.connection.query_row(
        "SELECT state FROM ops_home_acceptance_work_reservations WHERE acceptance_id=?1",
        params![acceptance.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(state, 0);

    // Simulate the owner transition after confirmed Applied. Qualification
    // settlement does not change the work guard's lifetime.
    fixture.catalog.connection.execute(
        "UPDATE ops_home_acceptance_work_reservations SET state=1,updated_at_ms=102 WHERE acceptance_id=?1",
        params![acceptance.as_bytes()],
    )?;
    assert!(matches!(
        fixture.catalog.record_assignment_affinity(&competitor),
        Err(OperationsError::Conflict(HELD))
    ));
    assert!(matches!(
        fixture
            .catalog
            .set_node_team_binding(binding_command(fixture.scope, Some(1), None, 103,)?),
        Err(OperationsError::Conflict(HELD))
    ));
    drop(fixture.catalog);
    fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn home_reservation_is_exact_to_goal_and_dag_generation() -> Result<(), Box<dyn Error>> {
    let mut fixture = fixture()?;
    crate::home_acceptance::tests::seed_pending_work_reservation(
        &fixture.catalog.connection,
        fixture.scope,
        AgentAssignmentId::from_uuid(Uuid::from_u128(0xB020)),
        AgentAssignmentId::from_uuid(Uuid::from_u128(0xB021)),
    )?;
    let original = fixture.scope;
    let next_generation = NodeTeamBindingScope::new(
        original.workspace_id(),
        original.activity_id(),
        original.goal_id(),
        TimestampMillis::new(original.goal_created_at().get() + 1)?,
        original.dag_revision(),
        original.dag_base_goal_revision(),
        original.node_id(),
    )?;
    let next_dag = NodeTeamBindingScope::new(
        original.workspace_id(),
        original.activity_id(),
        original.goal_id(),
        original.goal_created_at(),
        DagRevisionNumber::new(2)?,
        original.dag_base_goal_revision(),
        original.node_id(),
    )?;
    for (scope, assignment) in [(next_generation, 0xB022), (next_dag, 0xB023)] {
        let bound = fixture.catalog.set_node_team_binding(binding_command(
            scope,
            None,
            Some(fixture.team),
            101,
        )?)?;
        assert_eq!(bound.revision(), 1);
        let mut fresh = command(&fixture, assignment, Some(fixture.team))?;
        fresh.node_scope = scope;
        let admitted = fixture.catalog.record_assignment_affinity(&fresh)?;
        assert_eq!(
            admitted.disposition(),
            AssignmentAffinityDisposition::Pending
        );
    }
    assert!(matches!(
        fixture
            .catalog
            .set_node_team_binding(binding_command(original, Some(1), None, 102,)?),
        Err(OperationsError::Conflict(HELD))
    ));
    drop(fixture.catalog);
    fs::remove_dir_all(fixture.root)?;
    Ok(())
}
