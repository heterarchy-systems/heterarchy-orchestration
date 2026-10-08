use super::*;
use crate::{NodeTeamBindingScope, OperationalActor, SetNodeTeamBindingCommand};
use forge_domain::{
    AgentSessionId, DagNodeId, DagRevisionNumber, GoalId, GoalRevision, TimestampMillis,
};
use sha2::{Digest, Sha256};

const SEED: u128 = 0xB000;

fn mapping_command(
    fixture: &Fixture,
    node: u128,
    team: Option<Uuid>,
) -> Result<SetNodeTeamBindingCommand, Box<dyn std::error::Error>> {
    Ok(SetNodeTeamBindingCommand {
        scope: NodeTeamBindingScope::new(
            fixture.workspace,
            fixture.activity,
            GoalId::from_uuid(Uuid::from_u128(node + 1)),
            TimestampMillis::new(1)?,
            DagRevisionNumber::INITIAL,
            GoalRevision::INITIAL,
            DagNodeId::from_uuid(Uuid::from_u128(node + 2)),
        )?,
        expected_revision: None,
        team_id: team,
        actor: OperationalActor::LocalOperator,
        expected_sender_leadership_revision: None,
        observed_at: TimestampMillis::new(T0)?,
    })
}

fn team_election_fixture()
-> Result<(Fixture, LeadershipScope, Uuid, i64), Box<dyn std::error::Error>> {
    let (mut fixture, _, _, candidate) = seeded_workspace_fixture("team-roster", SEED)?;
    let team = Uuid::from_u128(SEED + 0x6500);
    let assignment = SEED + 0x6800;
    let node = mapping_command(&fixture, assignment, Some(team))?;
    let _ = fixture.catalog.set_node_team_binding(node)?;
    let _ = fixture
        .catalog
        .record_assignment_affinity(&crate::AssignmentAffinityCommand {
            workspace_id: fixture.workspace,
            assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(assignment)),
            activity_id: fixture.activity,
            target_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(SEED + 0x202)),
            target_session_id: AgentSessionId::from_uuid(Uuid::from_u128(SEED + 0x201)),
            requester: OperationalActor::LocalOperator,
            team_id: Some(team),
            node_scope: node.scope,
            expected_binding_revision: Some(1),
            assignment_payload_fingerprint: [7; 32],
            original_offer_payload: b"fixture-offer".to_vec(),
            observed_at: TimestampMillis::new(T0)?,
        })?;
    let _ = insert_offer(
        &fixture,
        500,
        fixture.activity,
        assignment,
        SEED + 0x202,
        T0,
    )?;
    insert_credit(&fixture, assignment, candidate, SEED + 0x202, 5, T0)?;
    let scope = LeadershipScope::team(fixture.workspace, team);
    let (election, deadline) = run_majority_election(
        &mut fixture,
        scope,
        candidate,
        T0 + STABILIZATION_WINDOW_MS + 2,
    )?;
    Ok((fixture, scope, election, deadline))
}

fn prepare_roster(
    fixture: &mut Fixture,
    scope: LeadershipScope,
    election: Uuid,
    now: i64,
) -> Result<work_roster::LeadershipWorkRoster, Box<dyn std::error::Error>> {
    let revision = fixture
        .catalog
        .leadership_scope_view(scope, now)?
        .ok_or("leadership missing")?
        .leadership_revision();
    match fixture.catalog.prepare_leadership_work_roster(
        scope,
        election,
        revision,
        Uuid::from_u128(SEED + 0x6900),
        now,
    )? {
        work_roster::LeadershipWorkRosterPrepareOutcome::Recorded(roster) => Ok(*roster),
        other => Err(format!("unexpected roster outcome: {other:?}").into()),
    }
}

#[test]
fn team_roster_fences_mapping_changes_until_unbound_release()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, scope, election, deadline) = team_election_fixture()?;
    let team = scope.team_id().ok_or("Team missing")?;
    let other = Uuid::from_u128(SEED + 0x6501);
    let mut existing = mapping_command(&fixture, SEED + 0x6A00, Some(team))?;
    existing.scope = NodeTeamBindingScope::new(
        fixture.workspace,
        ActivityId::from_uuid(Uuid::from_u128(SEED + 0x6D00)),
        existing.scope.goal_id(),
        TimestampMillis::new(42)?,
        DagRevisionNumber::new(2)?,
        GoalRevision::new(3)?,
        existing.scope.node_id(),
    )?;
    let bound = fixture.catalog.set_node_team_binding(existing)?;
    let roster = prepare_roster(&mut fixture, scope, election, deadline + 1)?;
    let snapshot = roster.forge_work_cut_scope()?;
    let forge_domain::WorkCutScope::Team { team_id, nodes } = &snapshot else {
        return Err("Team roster widened to another scope".into());
    };
    assert_eq!(*team_id, team);
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains(&forge_domain::WorkCutNodeScope::new(
        existing.scope.activity_id(),
        existing.scope.goal_id(),
        existing.scope.goal_created_at(),
        existing.scope.dag_base_goal_revision(),
        existing.scope.dag_revision(),
        existing.scope.node_id(),
    )?));
    existing.expected_revision = Some(bound.revision());
    existing.team_id = None;
    assert!(
        fixture.catalog.set_node_team_binding(existing).is_err(),
        "a prepared Team roster must fence mapping removal"
    );
    existing.team_id = Some(other);
    assert!(fixture.catalog.set_node_team_binding(existing).is_err());
    existing.team_id = None;
    let incoming = mapping_command(&fixture, SEED + 0x6B00, Some(team))?;
    assert!(fixture.catalog.set_node_team_binding(incoming).is_err());
    let unrelated = mapping_command(&fixture, SEED + 0x6C00, Some(other))?;
    let unrelated_binding = fixture.catalog.set_node_team_binding(unrelated)?;
    let mut move_in = unrelated;
    move_in.expected_revision = Some(unrelated_binding.revision());
    move_in.team_id = Some(team);
    assert!(fixture.catalog.set_node_team_binding(move_in).is_err());
    let _ = fixture.catalog.release_unbound_leadership_work_roster(
        scope,
        roster.cut_id(),
        roster.revision(),
        deadline + 2,
    )?;
    let _ = fixture.catalog.set_node_team_binding(existing)?;
    let _ = fixture.catalog.set_node_team_binding(incoming)?;
    drop(fixture.catalog);
    fixture.catalog =
        OperationsCatalog::open(&fixture.root.join("catalog.sqlite3"), fixture.destination)?;
    let replay = prepare_roster(&mut fixture, scope, election, deadline + 3)?;
    assert_eq!(replay.digest(), roster.digest());
    assert_eq!(replay.forge_work_cut_scope()?, snapshot);
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn team_roster_rejects_unresolved_legacy_and_overflow_without_partial_barriers()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, scope, election, deadline) = team_election_fixture()?;
    let team = scope.team_id().ok_or("Team missing")?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_dag_node_team_bindings(workspace_id,goal_id,dag_revision,node_id,team_id,bound_by,bound_at_ms,revision) VALUES(?1,?2,1,?3,?4,'operator',1,1)",
        params![fixture.workspace.into_uuid().as_bytes(), Uuid::from_u128(SEED + 0x7001).as_bytes(), Uuid::from_u128(SEED + 0x7002).as_bytes(), team.as_bytes()],
    )?;
    assert!(prepare_roster(&mut fixture, scope, election, deadline + 1).is_err());
    assert_eq!(work_roster_barrier_count(&fixture)?, 0);
    // A legacy row of a different Team must not prevent this Team's snapshot.
    fixture.catalog.connection.execute(
        "UPDATE ops_dag_node_team_bindings SET team_id=?1 WHERE workspace_id=?2 AND activity_id IS NULL",
        params![Uuid::from_u128(SEED + 0x6501).as_bytes(), fixture.workspace.into_uuid().as_bytes()],
    )?;
    for index in 0..255 {
        let command = mapping_command(&fixture, SEED + 0x10000 + index * 3, Some(team))?;
        let _ = fixture.catalog.set_node_team_binding(command)?;
    }
    assert!(prepare_roster(&mut fixture, scope, election, deadline + 1).is_err());
    assert_eq!(work_roster_barrier_count(&fixture)?, 0);
    let mut remove = mapping_command(&fixture, SEED + 0x10000, None)?;
    remove.expected_revision = Some(1);
    let _ = fixture.catalog.set_node_team_binding(remove)?;
    let roster = prepare_roster(&mut fixture, scope, election, deadline + 1)?;
    let forge_domain::WorkCutScope::Team { nodes, .. } = roster.forge_work_cut_scope()? else {
        return Err("Team witness missing".into());
    };
    assert_eq!(nodes.len(), 256);
    let _ = fixture.catalog.dispatch_leadership_work_roster(
        scope,
        roster.cut_id(),
        roster.revision(),
        deadline + 2,
    )?;
    let incoming = mapping_command(&fixture, SEED + 0x20000, Some(team))?;
    assert!(fixture.catalog.set_node_team_binding(incoming).is_err());
    assert!(
        fixture
            .catalog
            .release_unbound_leadership_work_roster(scope, roster.cut_id(), 2, deadline + 3)
            .is_err()
    );
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

fn roster_payload(fixture: &Fixture, cut: Uuid) -> Result<Vec<u8>, rusqlite::Error> {
    fixture.catalog.connection.query_row(
        "SELECT payload FROM ops_leadership_work_rosters WHERE cut_id=?1",
        params![cut.as_bytes()],
        |row| row.get(0),
    )
}

#[test]
fn legacy_team_roster_preserves_bytes_and_only_replays_prior_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, scope, election, deadline) = team_election_fixture()?;
    let roster = prepare_roster(&mut fixture, scope, election, deadline + 1)?;
    // Simulate an already persisted v1 payload, preserving its exact byte digest.
    let mut wire: serde_json::Value =
        serde_json::from_slice(&roster_payload(&fixture, roster.cut_id())?)?;
    let fields = wire.as_object_mut().ok_or("roster object missing")?;
    fields.insert("version".to_owned(), serde_json::json!(1));
    assert!(fields.remove("team_nodes").is_some());
    let bytes = serde_json::to_vec_pretty(&wire)?;
    let mut hash = Sha256::new();
    hash.update(b"heterarchy-leadership-work-roster-v1\0");
    hash.update(&bytes);
    let digest: [u8; 32] = hash.finalize().into();
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership_work_rosters SET payload=?1,digest=?2 WHERE cut_id=?3",
        params![bytes, digest.as_slice(), roster.cut_id().as_bytes()],
    )?;
    drop(fixture.catalog);
    fixture.catalog =
        OperationsCatalog::open(&fixture.root.join("catalog.sqlite3"), fixture.destination)?;
    let historical = prepare_roster(&mut fixture, scope, election, deadline + 2)?;
    assert_eq!(historical.digest(), digest);
    assert!(historical.forge_work_cut_scope().is_err());
    assert!(
        fixture
            .catalog
            .dispatch_leadership_work_roster(scope, roster.cut_id(), 1, deadline + 2)
            .is_err()
    );
    assert_eq!(roster_payload(&fixture, roster.cut_id())?, bytes);
    assert_eq!(
        fixture
            .catalog
            .leadership_work_roster(scope, roster.cut_id())?
            .ok_or("historical roster missing")?
            .state(),
        work_roster::LeadershipWorkRosterState::Prepared
    );
    // Existing old dispatches keep their same-ID readback; no fresh authority.
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership_work_rosters SET state=1,revision=2,updated_at_ms=?1 WHERE cut_id=?2",
        params![deadline + 2, roster.cut_id().as_bytes()],
    )?;
    assert!(matches!(
        fixture
            .catalog
            .dispatch_leadership_work_roster(scope, roster.cut_id(), 1, deadline + 3)?,
        work_roster::LeadershipWorkRosterDispatch::Readback(_)
    ));
    assert_eq!(roster_payload(&fixture, roster.cut_id())?, bytes);
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}
