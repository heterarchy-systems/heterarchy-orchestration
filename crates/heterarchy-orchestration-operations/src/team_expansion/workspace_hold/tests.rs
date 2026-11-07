use std::{
    collections::BTreeSet,
    error::Error,
    path::Path,
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use forge_domain::WorkspaceId;
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, TransactionBehavior, params};
use uuid::Uuid;

use super::{
    WorkspaceHoldPlanSlot, WorkspaceHoldTarget, WorkspaceIdentityKey, schema,
    validate_plan_ordinals, validate_slot_assignment_target, validate_workspace_hold_history,
    workspace_capacity_snapshot, workspace_key_union,
};
use crate::OperationsError;

type TestResult<T> = Result<T, Box<dyn Error>>;

#[test]
fn existing_workspace_profile_hold_deduplicates_and_fits_at_full_capacity() {
    let workspace = (1_u128..=12)
        .map(|id| WorkspaceIdentityKey::Profile(Uuid::from_u128(id)))
        .collect::<BTreeSet<_>>();
    let holds = [WorkspaceIdentityKey::Profile(Uuid::from_u128(12))]
        .into_iter()
        .collect();

    assert_eq!(workspace_key_union(&workspace, &holds).len(), 12);
}

#[test]
fn new_target_hold_counts_once_against_workspace_capacity() {
    let workspace = (1_u128..=11)
        .map(|id| WorkspaceIdentityKey::Profile(Uuid::from_u128(id)))
        .collect::<BTreeSet<_>>();
    let holds = [WorkspaceIdentityKey::UnboundSlot {
        decision_id: Uuid::from_u128(90),
        ordinal: 0,
    }]
    .into_iter()
    .collect();

    assert_eq!(workspace_key_union(&workspace, &holds).len(), 12);
}

#[test]
fn eleven_agents_plus_one_hold_blocks_an_unrelated_registration() -> TestResult<()> {
    let (connection, workspace_id, decision_id, request_id, team_id) = hold_catalog()?;
    for seed in 1_u128..=11 {
        connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id)
             VALUES(?1,?2,?3)",
            params![
                workspace_id.into_uuid().as_bytes(),
                Uuid::from_u128(0x100 + seed).as_bytes(),
                Uuid::from_u128(0x200 + seed).as_bytes(),
            ],
        )?;
    }
    insert_decision(
        &connection,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        "APPROVED",
    )?;
    insert_new_target_hold_plan(
        &connection,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        100,
    )?;
    assert_eq!(
        workspace_capacity_snapshot(&connection, workspace_id)?.used(),
        12
    );
    assert!(matches!(
        super::ensure_registration_capacity_with_holds(
            &connection,
            workspace_id,
            None,
            Uuid::from_u128(0x999),
        ),
        Err(OperationsError::Bounds("workspace logical Agent capacity"))
    ));
    super::ensure_registration_capacity_with_holds(
        &connection,
        workspace_id,
        Some(AgentProfileId::from_uuid(Uuid::from_u128(0x101))?),
        Uuid::from_u128(0x201),
    )?;
    Ok(())
}

#[test]
fn approval_hold_and_registration_serialize_the_twelfth_workspace_seat() -> TestResult<()> {
    let path = std::env::temp_dir().join(format!("workspace-hold-race-{}.sqlite", Uuid::now_v7()));
    let (setup, workspace_id, decision_id, request_id, team_id) = hold_catalog_at(Some(&path))?;
    for seed in 1_u128..=11 {
        setup.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id) VALUES(?1,?2,?3)",
            params![workspace_id.into_uuid().as_bytes(),Uuid::from_u128(0x300+seed).as_bytes(),Uuid::from_u128(0x400+seed).as_bytes()],
        )?;
    }
    drop(setup);

    let barrier = Arc::new(Barrier::new(3));
    let hold_barrier = Arc::clone(&barrier);
    let hold_path = path.clone();
    let hold = thread::spawn(move || -> Result<(), OperationsError> {
        let mut connection = Connection::open(hold_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        hold_barrier.wait();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_decision(
            &transaction,
            workspace_id,
            team_id,
            decision_id,
            request_id,
            "APPROVED",
        )
        .map_err(|_| OperationsError::Conflict("test approval insert"))?;
        super::reserve_approved_workspace_holds(
            &transaction,
            super::WorkspaceHoldReservation::new(
                super::super::TeamExpansionScope::new(workspace_id, team_id),
                decision_id,
                request_id,
                1,
                forge_domain::TimestampMillis::new(100)
                    .map_err(|_| OperationsError::Bounds("test timestamp"))?,
            ),
            &[WorkspaceHoldPlanSlot::new(0, WorkspaceHoldTarget::New)],
        )?;
        transaction.commit()?;
        Ok(())
    });
    let registration_barrier = Arc::clone(&barrier);
    let registration_path = path.clone();
    let registration = thread::spawn(move || -> Result<(), OperationsError> {
        let mut connection = Connection::open(registration_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        registration_barrier.wait();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let participant = Uuid::from_u128(0x999);
        super::ensure_registration_capacity_with_holds(
            &transaction,
            workspace_id,
            None,
            participant,
        )?;
        transaction.execute(
            "INSERT INTO ops_pending_participants(workspace_id,participant_id) VALUES(?1,?2)",
            params![workspace_id.into_uuid().as_bytes(), participant.as_bytes()],
        )?;
        transaction.commit()?;
        Ok(())
    });
    barrier.wait();
    let results = [
        hold.join().map_err(|_| "hold thread panicked")?,
        registration
            .join()
            .map_err(|_| "registration thread panicked")?,
    ];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(results.iter().any(|result| matches!(
        result,
        Err(OperationsError::Bounds("workspace logical Agent capacity"))
    )));
    let check = Connection::open(&path)?;
    let holds: i64 = check.query_row(
        "SELECT COUNT(*) FROM ops_team_expansion_workspace_hold_manifests",
        [],
        |row| row.get(0),
    )?;
    let pending: i64 =
        check.query_row("SELECT COUNT(*) FROM ops_pending_participants", [], |row| {
            row.get(0)
        })?;
    assert_eq!(holds + pending, 1);
    drop(check);
    let _ = std::fs::remove_file(path);
    Ok(())
}

#[test]
fn two_distinct_pending_new_slot_holds_compete_for_two_workspace_seats() {
    let workspace = (1_u128..=11)
        .map(|id| WorkspaceIdentityKey::Profile(Uuid::from_u128(id)))
        .collect::<BTreeSet<_>>();
    let holds = [
        WorkspaceIdentityKey::UnboundSlot {
            decision_id: Uuid::from_u128(90),
            ordinal: 0,
        },
        WorkspaceIdentityKey::UnboundSlot {
            decision_id: Uuid::from_u128(91),
            ordinal: 0,
        },
    ]
    .into_iter()
    .collect();

    assert_eq!(workspace_key_union(&workspace, &holds).len(), 13);
}

#[test]
fn participant_and_profile_ids_are_distinct_workspace_keys() {
    let identity = Uuid::from_u128(1);
    let workspace = [WorkspaceIdentityKey::Participant(identity)]
        .into_iter()
        .collect();
    let holds = [WorkspaceIdentityKey::Profile(identity)]
        .into_iter()
        .collect();

    assert_eq!(workspace_key_union(&workspace, &holds).len(), 2);
}

#[test]
fn approval_requires_exact_contiguous_target_plan_for_every_ordinal() {
    let plans = [
        WorkspaceHoldPlanSlot::new(
            0,
            WorkspaceHoldTarget::ExistingWorkspace {
                profile_id: Uuid::from_u128(1),
                participation_id: Uuid::from_u128(2),
            },
        ),
        WorkspaceHoldPlanSlot::new(1, WorkspaceHoldTarget::New),
    ];

    assert!(validate_plan_ordinals(&plans, 2).is_ok());
    assert!(matches!(
        validate_plan_ordinals(&plans[..1], 2),
        Err(OperationsError::Conflict(
            "Team expansion Workspace hold slot plan"
        ))
    ));
    let gaps = [
        WorkspaceHoldPlanSlot::new(0, WorkspaceHoldTarget::New),
        WorkspaceHoldPlanSlot::new(2, WorkspaceHoldTarget::New),
    ];
    assert!(matches!(
        validate_plan_ordinals(&gaps, 2),
        Err(OperationsError::Conflict(
            "Team expansion Workspace hold slot plan"
        ))
    ));
}

#[test]
fn one_profile_cannot_be_selected_by_two_approved_ordinals() {
    let plans = [
        WorkspaceHoldPlanSlot::new(
            0,
            WorkspaceHoldTarget::Unregistered {
                profile_id: Uuid::from_u128(10),
            },
        ),
        WorkspaceHoldPlanSlot::new(
            1,
            WorkspaceHoldTarget::ExistingWorkspace {
                profile_id: Uuid::from_u128(10),
                participation_id: Uuid::from_u128(20),
            },
        ),
    ];

    assert!(matches!(
        validate_plan_ordinals(&plans, 2),
        Err(OperationsError::Conflict(
            "Profile is selected by multiple Team expansion slots"
        ))
    ));
}

#[test]
fn post_hold_approval_without_manifest_or_pre_hold_marker_fails_closed() -> TestResult<()> {
    let (connection, workspace_id, decision_id, request_id, team_id) = hold_catalog()?;
    insert_decision(
        &connection,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        "APPROVED",
    )?;

    assert!(matches!(
        validate_workspace_hold_history(&connection, workspace_id, None),
        Err(OperationsError::Corrupt(
            "Team expansion Workspace hold history incomplete"
        ))
    ));
    Ok(())
}

#[test]
fn pre_hold_marker_remains_valid_after_cooldown_anchor_is_added() -> TestResult<()> {
    let (connection, workspace_id, decision_id, request_id, team_id) = hold_catalog()?;
    insert_decision(
        &connection,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        "APPROVED",
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_pre_hold_decisions(decision_id,request_id,workspace_id,team_id,decision_state,classification) VALUES(?1,?2,?3,?4,'APPROVED','PRE_HOLD')",
        params![
            decision_id.as_bytes(),
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_cooldown_anchors(decision_id,request_id,workspace_id,team_id,cooldown_until_ms) VALUES(?1,?2,?3,?4,60000)",
        params![
            decision_id.as_bytes(),
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;

    validate_workspace_hold_history(&connection, workspace_id, None)?;
    let persisted_state: String = connection.query_row(
        "SELECT state FROM ops_team_expansion_decisions WHERE decision_id=?1",
        params![decision_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(persisted_state, "APPROVED");
    Ok(())
}

#[test]
fn departed_existing_target_keeps_its_hold_without_blocking_other_capacity_reads() -> TestResult<()>
{
    let (connection, workspace_id, decision_id, request_id, team_id) = hold_catalog()?;
    let profile_id = Uuid::from_u128(50);
    let participation_id = Uuid::from_u128(51);
    connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id) VALUES(?1,?2,?3)",
        params![
            workspace_id.into_uuid().as_bytes(),
            profile_id.as_bytes(),
            participation_id.as_bytes(),
        ],
    )?;
    insert_decision(
        &connection,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        "APPROVED",
    )?;
    insert_existing_hold_plan(
        &connection,
        HoldFixtureIdentity {
            workspace: workspace_id,
            team: team_id,
            decision: decision_id,
            request: request_id,
        },
        profile_id,
        participation_id,
        100,
    )?;

    assert_eq!(
        workspace_capacity_snapshot(&connection, workspace_id)?.used(),
        1
    );
    connection.execute(
        "DELETE FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2",
        params![workspace_id.into_uuid().as_bytes(), profile_id.as_bytes()],
    )?;
    assert_eq!(
        workspace_capacity_snapshot(&connection, workspace_id)?.used(),
        1
    );
    assert!(matches!(
        validate_slot_assignment_target(
            &connection,
            workspace_id,
            super::TeamExpansionSlotRef::new(decision_id, 0),
            profile_id,
            participation_id,
        ),
        Err(OperationsError::Conflict(
            "Existing Workspace Profile target is not exact"
        ))
    ));

    let second_decision = Uuid::from_u128(60);
    let second_request = Uuid::from_u128(61);
    let second_team = Uuid::from_u128(62);
    insert_decision(
        &connection,
        workspace_id,
        second_team,
        second_decision,
        second_request,
        "APPROVED",
    )?;
    insert_new_target_hold_plan(
        &connection,
        workspace_id,
        second_team,
        second_decision,
        second_request,
        110,
    )?;
    assert_eq!(
        workspace_capacity_snapshot(&connection, workspace_id)?.used(),
        2
    );
    Ok(())
}

#[test]
fn new_rejected_decision_needs_no_workspace_hold() -> TestResult<()> {
    let (connection, workspace_id, decision_id, request_id, team_id) = hold_catalog()?;
    insert_decision(
        &connection,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        "REJECTED",
    )?;

    validate_workspace_hold_history(&connection, workspace_id, None)?;
    Ok(())
}

#[test]
fn legacy_marker_cannot_certify_a_decision_with_a_new_hold_manifest() -> TestResult<()> {
    let (connection, workspace_id, decision_id, request_id, team_id) = hold_catalog()?;
    insert_decision(
        &connection,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        "APPROVED",
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_pre_hold_decisions(decision_id,request_id,workspace_id,team_id,decision_state,classification) VALUES(?1,?2,?3,?4,'APPROVED','PRE_HOLD')",
        params![
            decision_id.as_bytes(),
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_manifests(decision_id,request_id,workspace_id,team_id,plan_version,slot_count,recorded_at_ms) VALUES(?1,?2,?3,?4,1,1,100)",
        params![
            decision_id.as_bytes(),
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_plans(decision_id,ordinal,workspace_id,team_id,target_kind) VALUES(?1,0,?2,?3,2)",
        params![
            decision_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;

    assert!(matches!(
        validate_workspace_hold_history(&connection, workspace_id, None),
        Err(OperationsError::Corrupt(
            "Team expansion Workspace hold history incomplete"
        ))
    ));
    Ok(())
}

fn hold_catalog() -> TestResult<(Connection, WorkspaceId, Uuid, Uuid, Uuid)> {
    hold_catalog_at(None)
}

fn hold_catalog_at(path: Option<&Path>) -> TestResult<(Connection, WorkspaceId, Uuid, Uuid, Uuid)> {
    let mut connection = path.map_or_else(Connection::open_in_memory, Connection::open)?;
    connection.execute_batch(
        "CREATE TABLE ops_team_expansion_decisions(
            decision_id BLOB PRIMARY KEY,request_id BLOB NOT NULL,workspace_id BLOB NOT NULL,
            team_id BLOB NOT NULL,state TEXT NOT NULL,approved_count INTEGER NOT NULL,
            UNIQUE(workspace_id,team_id,decision_id,request_id)
         );
         CREATE TABLE ops_team_expansion_slots(
            decision_id BLOB NOT NULL,ordinal INTEGER NOT NULL,workspace_id BLOB NOT NULL,
            team_id BLOB NOT NULL,request_id BLOB NOT NULL,
            UNIQUE(workspace_id,team_id,decision_id,ordinal)
         );
         CREATE TABLE ops_team_expansion_slot_uses(
            decision_id BLOB NOT NULL,ordinal INTEGER NOT NULL,join_request_id BLOB NOT NULL,
            workspace_id BLOB NOT NULL,team_id BLOB NOT NULL,profile_id BLOB NOT NULL,
            participation_id BLOB NOT NULL
         );
         CREATE TABLE ops_team_expansion_cooldown_anchors(
            decision_id BLOB NOT NULL,request_id BLOB NOT NULL,workspace_id BLOB NOT NULL,
            team_id BLOB NOT NULL,cooldown_until_ms INTEGER NOT NULL
         );
         CREATE TABLE ops_workspace_participations(
            workspace_id BLOB NOT NULL,agent_profile_id BLOB NOT NULL,participation_id BLOB NOT NULL,
            PRIMARY KEY(workspace_id,agent_profile_id),UNIQUE(workspace_id,participation_id)
         );
         CREATE TABLE ops_workspace_participation_mappings(
            workspace_id BLOB NOT NULL,participant_id BLOB NOT NULL,agent_profile_id BLOB NOT NULL
         );
         CREATE TABLE ops_registration_profile_scopes(
            request_id BLOB NOT NULL,workspace_id BLOB NOT NULL,profile_id BLOB NOT NULL
         );
         CREATE TABLE ops_registration_requests(
            request_id BLOB NOT NULL,workspace_id BLOB NOT NULL,participant_id BLOB NOT NULL
         );
         CREATE TABLE ops_logical_members(
            workspace_id BLOB NOT NULL,participant_id BLOB NOT NULL
         );
         CREATE TABLE ops_pending_participants(
            workspace_id BLOB NOT NULL,participant_id BLOB NOT NULL
         );",
    )?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    schema::create_schema(&transaction)?;
    schema::create_recovery_schema(&transaction)?;
    transaction.commit()?;
    Ok((
        connection,
        WorkspaceId::from_uuid(Uuid::from_u128(1)),
        Uuid::from_u128(2),
        Uuid::from_u128(3),
        Uuid::from_u128(4),
    ))
}

fn insert_decision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    state: &str,
) -> TestResult<()> {
    let approved_count = i32::from(state == "APPROVED");
    connection.execute(
        "INSERT INTO ops_team_expansion_decisions(decision_id,request_id,workspace_id,team_id,state,approved_count) VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            decision_id.as_bytes(),
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            state,
            approved_count,
        ],
    )?;
    if state == "APPROVED" {
        connection.execute(
            "INSERT INTO ops_team_expansion_slots(decision_id,ordinal,workspace_id,team_id,request_id) VALUES(?1,0,?2,?3,?4)",
            params![
                decision_id.as_bytes(),
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                request_id.as_bytes(),
            ],
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct HoldFixtureIdentity {
    workspace: WorkspaceId,
    team: Uuid,
    decision: Uuid,
    request: Uuid,
}

fn insert_existing_hold_plan(
    connection: &Connection,
    identity: HoldFixtureIdentity,
    profile_id: Uuid,
    participation_id: Uuid,
    recorded_at_ms: i64,
) -> TestResult<()> {
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_manifests(decision_id,request_id,workspace_id,team_id,plan_version,slot_count,recorded_at_ms) VALUES(?1,?2,?3,?4,1,1,?5)",
        params![
            identity.decision.as_bytes(),
            identity.request.as_bytes(),
            identity.workspace.into_uuid().as_bytes(),
            identity.team.as_bytes(),
            recorded_at_ms,
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_plans(decision_id,ordinal,workspace_id,team_id,target_kind,target_profile_id,target_participation_id) VALUES(?1,0,?2,?3,0,?4,?5)",
        params![
            identity.decision.as_bytes(),
            identity.workspace.into_uuid().as_bytes(),
            identity.team.as_bytes(),
            profile_id.as_bytes(),
            participation_id.as_bytes(),
        ],
    )?;
    Ok(())
}

fn insert_new_target_hold_plan(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    recorded_at_ms: i64,
) -> TestResult<()> {
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_manifests(decision_id,request_id,workspace_id,team_id,plan_version,slot_count,recorded_at_ms) VALUES(?1,?2,?3,?4,1,1,?5)",
        params![
            decision_id.as_bytes(),
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            recorded_at_ms,
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_plans(decision_id,ordinal,workspace_id,team_id,target_kind,target_profile_id,target_participation_id) VALUES(?1,0,?2,?3,2,NULL,NULL)",
        params![
            decision_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;
    Ok(())
}
