use std::{
    error::Error,
    fs::OpenOptions,
    io::{Error as IoError, ErrorKind},
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use rusqlite::params;

use super::*;
use crate::TeamJoinCommand;
use crate::team_expansion::{
    TeamExpansionApprovalSlotPlan, TeamExpansionDecision, TeamExpansionDecisionCommand,
    TeamExpansionDecisionV2Command, TeamExpansionRequestCommand, TeamExpansionRevisions,
    TeamExpansionScope, TeamExpansionSlotAssignmentCommand, TeamExpansionSlotRef,
    TeamExpansionWorkspaceTarget,
};

const TEAM_SEAT_CAPACITY: i64 = 8;
static DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TemporaryDatabase {
    path: PathBuf,
}

impl TemporaryDatabase {
    fn new() -> Result<Self, IoError> {
        for _ in 0..64 {
            let sequence = DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "temporary-dispatch-seat-race-{}-{sequence}.sqlite",
                std::process::id(),
            ));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    drop(file);
                    std::fs::remove_file(&path)?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(IoError::new(
            ErrorKind::AlreadyExists,
            "could not allocate unique temporary dispatch race database",
        ))
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.path.as_os_str().to_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(path));
        }
    }
}

struct LastSeatSetup {
    initial_team_revision: u64,
    join_request_id: Uuid,
    join_command: TeamJoinCommand,
    alias_reservation_command: TemporaryDispatchReserveCommand,
    distinct_target_reservation_command: TemporaryDispatchReserveCommand,
    binding_a: TemporaryDispatchAdmissionBinding,
    binding_b: TemporaryDispatchAdmissionBinding,
    target_a_identity: Actor,
    target_a_alias: Actor,
    target_b: Actor,
}

struct DispatchBindings {
    target_a_identity: Actor,
    target_a_alias: Actor,
    target_b: Actor,
    binding_a: TemporaryDispatchAdmissionBinding,
    binding_b: TemporaryDispatchAdmissionBinding,
}

struct JoinReservationSetup {
    initial_team_revision: u64,
    join_request_id: Uuid,
    join_command: TeamJoinCommand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Contender {
    GeneralJoin,
    Reservation(Uuid),
}

enum OwnerCommand {
    Join(Box<TeamJoinCommand>),
    Reserve(Box<TemporaryDispatchReserveCommand>),
}

#[derive(Debug, PartialEq, Eq)]
enum OwnerOutcome {
    Joined,
    Reserved(Uuid),
    CapacityRejected,
    Unexpected(String),
}

struct OwnerAttempt {
    contender: Contender,
    outcome: OwnerOutcome,
}

struct RaceWinner {
    general_join: bool,
    reservation_request_id: Option<Uuid>,
}

#[test]
fn file_backed_team_join_and_two_tf_reservations_contend_for_one_seat() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let setup = prepare_last_seat_setup(&database.path)?;

    let mut connections = [
        crate::teams::tests::create_connection(Some(&database.path))?,
        crate::teams::tests::create_connection(Some(&database.path))?,
        crate::teams::tests::create_connection(Some(&database.path))?,
    ];
    for connection in &mut connections {
        connection.busy_timeout(Duration::from_secs(5))?;
    }
    let [
        join_connection,
        alias_reservation_connection,
        distinct_reservation_connection,
    ] = connections;
    let barrier = Arc::new(Barrier::new(4));
    let workers = [
        start_contender(
            OperationsCatalog {
                connection: join_connection,
                destination: DESTINATION,
            },
            Contender::GeneralJoin,
            OwnerCommand::Join(Box::new(setup.join_command.clone())),
            Arc::clone(&barrier),
        ),
        start_contender(
            OperationsCatalog {
                connection: alias_reservation_connection,
                destination: DESTINATION,
            },
            Contender::Reservation(setup.binding_a.request_id()),
            OwnerCommand::Reserve(Box::new(setup.alias_reservation_command.clone())),
            Arc::clone(&barrier),
        ),
        start_contender(
            OperationsCatalog {
                connection: distinct_reservation_connection,
                destination: DESTINATION,
            },
            Contender::Reservation(setup.binding_b.request_id()),
            OwnerCommand::Reserve(Box::new(setup.distinct_target_reservation_command.clone())),
            Arc::clone(&barrier),
        ),
    ];
    barrier.wait();
    let attempts = workers
        .into_iter()
        .map(|worker| {
            worker
                .join()
                .map_err(|_| IoError::other("last-seat owner worker panicked"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let winner = classify_race(&attempts)?;

    let read_connection = crate::teams::tests::create_connection(Some(&database.path))?;
    let catalog = OperationsCatalog {
        connection: read_connection,
        destination: DESTINATION,
    };
    verify_final_capacity(&catalog, &setup, &winner)?;
    Ok(())
}

fn prepare_last_seat_setup(path: &PathBuf) -> Result<LastSeatSetup, Box<dyn Error>> {
    // These are synthetic native-test import and leadership facts. Team
    // creation, admission, consent, join, and reservation still use their
    // actual OperationsCatalog owner methods; this is not Forge/Broker or
    // HTTP/Runtime authorization evidence.
    let connection = crate::teams::tests::create_connection(Some(path))?;
    let mut fixture = fixture_from_connection(connection, true, 7, false)?;
    remove_unused_default_team_test_identities(&fixture.catalog.connection)?;
    assert_eq!(
        crate::team_expansion::workspace_hold::workspace_capacity_used(
            &fixture.catalog.connection,
            WORKSPACE,
        )?,
        12,
        "last-seat fixture must satisfy Workspace logical Agent capacity",
    );
    let dispatch = prepare_dispatch_bindings(&mut fixture)?;
    let join = reserve_last_seat_for_join(&mut fixture)?;
    let alias_reservation_command = TemporaryDispatchReserveCommand::new(
        dispatch.binding_a.clone(),
        0,
        fixture.orchestrator.input(),
    )?;
    let distinct_target_reservation_command = TemporaryDispatchReserveCommand::new(
        dispatch.binding_b.clone(),
        0,
        fixture.orchestrator.input(),
    )?;
    drop(fixture);
    Ok(LastSeatSetup {
        initial_team_revision: join.initial_team_revision,
        join_request_id: join.join_request_id,
        join_command: join.join_command,
        alias_reservation_command,
        distinct_target_reservation_command,
        binding_a: dispatch.binding_a,
        binding_b: dispatch.binding_b,
        target_a_identity: dispatch.target_a_identity,
        target_a_alias: dispatch.target_a_alias,
        target_b: dispatch.target_b,
    })
}

fn remove_unused_default_team_test_identities(connection: &rusqlite::Connection) -> TestResult {
    let profiles = [id(0x3000), id(0x3100), id(0x3200)];
    for profile in profiles {
        let participant_bytes: Vec<u8> = connection.query_row(
            "SELECT participant_id FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND agent_profile_id=?2",
            params![WORKSPACE.into_uuid().as_bytes(), profile.as_bytes()],
            |row| row.get(0),
        )?;
        let participant = Uuid::from_slice(&participant_bytes)?;
        connection.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
            params![WORKSPACE.into_uuid().as_bytes(), participant.as_bytes()],
        )?;
        connection.execute(
            "DELETE FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2",
            params![WORKSPACE.into_uuid().as_bytes(), participant.as_bytes()],
        )?;
        connection.execute(
            "DELETE FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND agent_profile_id=?2",
            params![WORKSPACE.into_uuid().as_bytes(), profile.as_bytes()],
        )?;
        connection.execute(
            "DELETE FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2",
            params![WORKSPACE.into_uuid().as_bytes(), profile.as_bytes()],
        )?;
    }
    Ok(())
}

fn prepare_dispatch_bindings(fixture: &mut Fixture) -> Result<DispatchBindings, Box<dyn Error>> {
    let target_a_identity = fixture.target_a;
    let target_a_alias = Actor {
        participant: id(0xDDD0),
        activity: ActivityId::from_uuid(id(0xDDD1)),
        agent: AgentInstanceId::from_uuid(id(0xDDD2)),
        session: AgentSessionId::from_uuid(id(0xDDD3)),
        ..fixture.target_a
    };
    seed_profile_session_alias(&fixture.catalog.connection, target_a_alias)?;
    let target_b = Actor::new(12);
    let receiving_leader = fixture.receiver_a_leader;
    let binding_a = admit(
        fixture,
        0xDDE0,
        RECEIVING_TEAM_A,
        receiving_leader,
        target_a_alias,
    )?;
    assert_eq!(
        binding_a.target_participant_id(),
        target_a_alias.participant
    );
    assert_eq!(binding_a.target_home_team_id(), HOME_TEAM);
    record_request_consents(fixture, &binding_a, target_a_alias, 0xDDE8)?;
    let binding_b = admit(
        fixture,
        0xDDF0,
        RECEIVING_TEAM_A,
        receiving_leader,
        target_b,
    )?;
    record_request_consents(fixture, &binding_b, target_b, 0xDDF8)?;
    assert_last_seat_is_open(fixture)?;
    Ok(DispatchBindings {
        target_a_identity,
        target_a_alias,
        target_b,
        binding_a,
        binding_b,
    })
}

fn record_request_consents(
    fixture: &mut Fixture,
    binding: &TemporaryDispatchAdmissionBinding,
    target: Actor,
    seed: u128,
) -> TestResult {
    let _receipts = all_consents(fixture, binding, target, seed)?;
    Ok(())
}

fn assert_last_seat_is_open(fixture: &Fixture) -> TestResult {
    let joined = fixture
        .catalog
        .team_member_counts(WORKSPACE)?
        .get(&RECEIVING_TEAM_A)
        .copied()
        .ok_or_else(|| IoError::other("receiving Team joined count missing before race"))?;
    assert_eq!(joined, u64::try_from(TEAM_SEAT_CAPACITY - 1)?);
    let reservations: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE workspace_id=?1 AND receiving_team_id=?2 AND state IN('RESERVED','SOURCE_READY','ACTIVE','DRAINING')",
        params![WORKSPACE.into_uuid().as_bytes(), RECEIVING_TEAM_A.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(reservations, 0);
    Ok(())
}

fn reserve_last_seat_for_join(
    fixture: &mut Fixture,
) -> Result<JoinReservationSetup, Box<dyn Error>> {
    let team = fixture
        .catalog
        .team(WORKSPACE, RECEIVING_TEAM_A)?
        .ok_or_else(|| IoError::other("receiving Team fixture missing"))?;
    let receiving_leader = fixture.receiver_a_leader;
    let orchestrator = fixture.orchestrator;
    let initial_team_revision = team.revision();
    let join_request_id = id(0xDE10);
    let expansion_request_id = id(0xDE08);
    let expansion_decision_id = id(0xDE09);
    let expansion_request = TeamExpansionRequestCommand::new(
        expansion_request_id,
        TeamExpansionScope::new(WORKSPACE, RECEIVING_TEAM_A),
        TeamExpansionRevisions::new(initial_team_revision, 1, 1),
        1,
        "one reserved seat for join contention".to_owned(),
        receiving_leader.input(),
    );
    let expansion_request_receipt = fixture
        .catalog
        .request_team_expansion(&expansion_request, || time(220))?;
    let decision = TeamExpansionDecisionCommand::new(
        expansion_decision_id,
        expansion_request_id,
        TeamExpansionScope::new(WORKSPACE, RECEIVING_TEAM_A),
        TeamExpansionRevisions::new(initial_team_revision, 1, 2),
        expansion_request_receipt.requester_leadership_revision(),
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator.input(),
    );
    let join_actor = Actor::new(14);
    let decision_v2 = TeamExpansionDecisionV2Command::new(
        decision,
        vec![TeamExpansionApprovalSlotPlan::new(
            0,
            TeamExpansionWorkspaceTarget::ExistingWorkspaceProfile {
                profile_id: join_actor.profile,
                participation_id: crate::WorkspaceParticipationId::from_uuid(
                    join_actor.participation,
                )?,
            },
        )],
    );
    let pending = fixture
        .catalog
        .decide_team_expansion_v2(&decision_v2, || time(230))?;
    assert_eq!(
        pending.state(),
        crate::team_expansion::TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    let anchored = fixture.catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        expansion_request_id,
        expansion_decision_id,
        2,
        orchestrator.input(),
        || time(240),
    )?;
    assert_eq!(
        anchored.state(),
        crate::team_expansion::TeamExpansionDecisionState::Approved
    );
    let expansion_slot = TeamExpansionSlotRef::new(expansion_decision_id, 0);
    fixture.catalog.assign_team_expansion_slot(
        &TeamExpansionSlotAssignmentCommand::new(
            id(0xDE0A),
            expansion_slot,
            TeamExpansionScope::new(WORKSPACE, RECEIVING_TEAM_A),
            TeamExpansionRevisions::new(initial_team_revision, 1, 2),
            join_actor.input(),
            orchestrator.input(),
        ),
        || time(245),
    )?;
    let join_command = TeamJoinCommand {
        request_id: join_request_id,
        workspace_id: WORKSPACE,
        team_id: RECEIVING_TEAM_A,
        expected_team_revision: initial_team_revision,
        actor: join_actor.input(),
        role: "worker".to_owned(),
        expansion_slot: Some(expansion_slot),
        observed_at: time(250)?,
    };
    Ok(JoinReservationSetup {
        initial_team_revision,
        join_request_id,
        join_command,
    })
}

fn start_contender(
    mut catalog: OperationsCatalog,
    contender: Contender,
    command: OwnerCommand,
    barrier: Arc<Barrier>,
) -> std::thread::JoinHandle<OwnerAttempt> {
    std::thread::spawn(move || {
        barrier.wait();
        let outcome = match command {
            OwnerCommand::Join(command) => match catalog.join_team(*command) {
                Ok(_) => OwnerOutcome::Joined,
                Err(OperationsError::Bounds("team membership capacity")) => {
                    OwnerOutcome::CapacityRejected
                }
                Err(error) => OwnerOutcome::Unexpected(error.to_string()),
            },
            OwnerCommand::Reserve(command) => {
                match catalog.reserve_temporary_dispatch(command.as_ref(), || time(300)) {
                    Ok(receipt)
                        if receipt.state() == TemporaryDispatchReservationState::Reserved =>
                    {
                        OwnerOutcome::Reserved(receipt.request_id())
                    }
                    Ok(receipt) => OwnerOutcome::Unexpected(format!(
                        "unexpected temporary dispatch reservation state {:?}",
                        receipt.state()
                    )),
                    Err(OperationsError::Bounds("team membership capacity")) => {
                        OwnerOutcome::CapacityRejected
                    }
                    Err(error) => OwnerOutcome::Unexpected(error.to_string()),
                }
            }
        };
        OwnerAttempt { contender, outcome }
    })
}

fn classify_race(attempts: &[OwnerAttempt]) -> Result<RaceWinner, IoError> {
    let mut general_join = false;
    let mut reservation_request_id = None;
    let mut capacity_rejections = 0;
    for attempt in attempts {
        match (attempt.contender, &attempt.outcome) {
            (Contender::GeneralJoin, OwnerOutcome::Joined) if !general_join => {
                general_join = true;
            }
            (Contender::Reservation(request_id), OwnerOutcome::Reserved(receipt_id))
                if request_id == *receipt_id && reservation_request_id.is_none() =>
            {
                reservation_request_id = Some(request_id);
            }
            (_, OwnerOutcome::CapacityRejected) => capacity_rejections += 1,
            (_, OwnerOutcome::Unexpected(message)) => return Err(IoError::other(message.clone())),
            _ => return Err(IoError::other("unexpected or duplicate last-seat outcome")),
        }
    }
    let winner_count = i32::from(general_join) + i32::from(reservation_request_id.is_some());
    if winner_count != 1 || capacity_rejections != 2 {
        return Err(IoError::other(
            "last-seat race did not yield exactly one winner and two capacity rejections",
        ));
    }
    Ok(RaceWinner {
        general_join,
        reservation_request_id,
    })
}

fn verify_final_capacity(
    catalog: &OperationsCatalog,
    setup: &LastSeatSetup,
    winner: &RaceWinner,
) -> TestResult {
    let joined = catalog
        .team_member_counts(WORKSPACE)?
        .get(&RECEIVING_TEAM_A)
        .copied()
        .ok_or_else(|| IoError::other("receiving Team joined count missing"))?;
    assert_eq!(
        joined,
        if winner.general_join {
            u64::try_from(TEAM_SEAT_CAPACITY)?
        } else {
            u64::try_from(TEAM_SEAT_CAPACITY - 1)?
        }
    );
    let team = catalog
        .team(WORKSPACE, RECEIVING_TEAM_A)?
        .ok_or_else(|| IoError::other("receiving Team disappeared"))?;
    assert_eq!(
        team.revision(),
        setup.initial_team_revision + u64::from(winner.general_join)
    );
    assert_eq!(
        catalog
            .team_mutation(WORKSPACE, RECEIVING_TEAM_A, setup.join_request_id)?
            .is_some(),
        winner.general_join
    );

    let occupied_seats: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM (
             SELECT participation_id AS subject_id
             FROM ops_team_members
             WHERE workspace_id=?1 AND team_id=?2 AND state='JOINED'
             UNION
             SELECT target_participation_id AS subject_id
             FROM ops_temporary_dispatch_reservations
             WHERE workspace_id=?1 AND receiving_team_id=?2
               AND state IN('RESERVED','SOURCE_READY','ACTIVE','DRAINING')
         )",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            RECEIVING_TEAM_A.as_bytes()
        ],
        |row| row.get(0),
    )?;
    assert_eq!(occupied_seats, TEAM_SEAT_CAPACITY);

    let (admission_count, last_sequence, consent_count, reservation_count): (i64, i64, i64, i64) =
        catalog.connection.query_row(
            "SELECT (SELECT COUNT(*) FROM ops_temporary_dispatch_requests WHERE request_id IN(?1,?2)),(SELECT last_sequence FROM ops_temporary_dispatch_quota_state WHERE workspace_id=?3 AND requesting_team_id=?4),(SELECT COUNT(*) FROM ops_temporary_dispatch_consent_receipts WHERE request_id IN(?1,?2)),(SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE request_id IN(?1,?2) AND state='RESERVED')",
            params![
                setup.binding_a.request_id().as_bytes(),
                setup.binding_b.request_id().as_bytes(),
                WORKSPACE.into_uuid().as_bytes(),
                RECEIVING_TEAM_A.as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    assert_eq!((admission_count, last_sequence, consent_count), (2, 2, 6));
    assert_eq!(
        reservation_count,
        i64::from(winner.reservation_request_id.is_some())
    );
    verify_reservation_target(catalog, setup, winner)?;
    Ok(())
}

fn verify_reservation_target(
    catalog: &OperationsCatalog,
    setup: &LastSeatSetup,
    winner: &RaceWinner,
) -> TestResult {
    let Some(request_id) = winner.reservation_request_id else {
        return Ok(());
    };
    let (participant, participation, profile) = if request_id == setup.binding_a.request_id() {
        (
            setup.target_a_alias.participant,
            setup.target_a_alias.participation,
            setup.target_a_alias.profile,
        )
    } else if request_id == setup.binding_b.request_id() {
        (
            setup.target_b.participant,
            setup.target_b.participation,
            setup.target_b.profile,
        )
    } else {
        return Err(IoError::other("reservation winner did not match either request").into());
    };
    let exact_target: bool = catalog.connection.query_row(
        "SELECT target_participant_id=?1 AND target_participation_id=?2 AND target_profile_id=?3 FROM ops_temporary_dispatch_reservations WHERE request_id=?4 AND state='RESERVED'",
        params![
            participant.as_bytes(),
            participation.as_bytes(),
            profile.as_bytes(),
            request_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    assert!(exact_target);
    if request_id == setup.binding_a.request_id() {
        assert_eq!(
            setup.target_a_alias.participation,
            setup.target_a_identity.participation
        );
        assert_eq!(
            setup.target_a_alias.profile,
            setup.target_a_identity.profile
        );
    }
    Ok(())
}
