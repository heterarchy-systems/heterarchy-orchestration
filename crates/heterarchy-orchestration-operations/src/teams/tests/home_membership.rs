use super::*;
use rusqlite::{OptionalExtension, params};
use std::io::Error as IoError;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;

const TEAM_ONE: Uuid = Uuid::from_u128(0xE1000);
const TEAM_TWO: Uuid = Uuid::from_u128(0xE2000);
const TEAM_THREE: Uuid = Uuid::from_u128(0xE3000);

#[derive(Clone, Copy)]
struct AgentIdentity {
    profile: Uuid,
    participation: Uuid,
    participant: Uuid,
    activity: Uuid,
    agent: Uuid,
    session: Uuid,
}

impl AgentIdentity {
    fn new(index: u128) -> Self {
        let base = 0xD0000 + index * 0x10;
        Self {
            profile: Uuid::from_u128(base),
            participation: Uuid::from_u128(base + 1),
            participant: Uuid::from_u128(base + 2),
            activity: Uuid::from_u128(base + 3),
            agent: Uuid::from_u128(base + 4),
            session: Uuid::from_u128(base + 5),
        }
    }

    fn seed(self, connection: &Connection) -> Result<(), Box<dyn Error>> {
        seed_participant_identity(
            connection,
            workspace(WORKSPACE),
            self.profile,
            self.participation,
            self.participant,
            self.activity,
            self.agent,
            self.session,
            Uuid::from_u128(self.profile.as_u128() + 8),
        )
    }

    fn actor(self) -> OperationalActor {
        OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(self.activity),
            agent_id: AgentInstanceId::from_uuid(self.agent),
            session_id: AgentSessionId::from_uuid(self.session),
        }
    }

    fn roster_member(self, role: &str) -> TeamRosterMemberInput {
        TeamRosterMemberInput {
            activity_id: ActivityId::from_uuid(self.activity),
            agent_id: AgentInstanceId::from_uuid(self.agent),
            session_id: AgentSessionId::from_uuid(self.session),
            role: role.to_owned(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct MutationCounts {
    teams: i64,
    members: i64,
    requests: i64,
    events: i64,
    create_roster: i64,
}

fn mutation_counts(connection: &Connection) -> Result<MutationCounts, rusqlite::Error> {
    connection.query_row(
        "SELECT (SELECT COUNT(*) FROM ops_teams),\
                (SELECT COUNT(*) FROM ops_team_members),\
                (SELECT COUNT(*) FROM ops_team_requests),\
                (SELECT COUNT(*) FROM ops_team_member_events),\
                (SELECT COUNT(*) FROM ops_team_create_roster)",
        [],
        |row| {
            Ok(MutationCounts {
                teams: row.get(0)?,
                members: row.get(1)?,
                requests: row.get(2)?,
                events: row.get(3)?,
                create_roster: row.get(4)?,
            })
        },
    )
}

fn seed_agents(connection: &Connection, agents: &[AgentIdentity]) -> Result<(), Box<dyn Error>> {
    for agent in agents {
        agent.seed(connection)?;
    }
    Ok(())
}

fn team_create_command(
    request_id: Uuid,
    team_id: Uuid,
    members: &[AgentIdentity],
) -> Result<TeamCreateCommand, OperationsError> {
    let mut command = create_command(request_id, workspace(WORKSPACE))?;
    command.team_id = team_id;
    command.initial_members = members
        .iter()
        .enumerate()
        .map(|(index, agent)| agent.roster_member(&format!("member-{index}")))
        .collect();
    Ok(command)
}

fn create_team_with_members(
    connection: &mut Connection,
    request_id: Uuid,
    team_id: Uuid,
    members: &[AgentIdentity],
) -> Result<TeamMutationRecord, Box<dyn Error>> {
    Ok(create_team(
        connection,
        team_create_command(request_id, team_id, members)?,
    )?)
}

fn join_command(
    request_id: Uuid,
    team_id: Uuid,
    expected_team_revision: u64,
    actor: AgentIdentity,
    observed_at_ms: i64,
) -> Result<TeamJoinCommand, OperationsError> {
    Ok(TeamJoinCommand {
        request_id,
        workspace_id: workspace(WORKSPACE),
        team_id,
        expected_team_revision,
        actor: actor.actor(),
        role: "worker".to_owned(),
        expansion_slot: None,
        observed_at: timestamp(observed_at_ms)?,
    })
}

fn leave_command(
    request_id: Uuid,
    team_id: Uuid,
    expected_team_revision: u64,
    actor: AgentIdentity,
    observed_at_ms: i64,
) -> Result<TeamLeaveCommand, OperationsError> {
    Ok(TeamLeaveCommand {
        request_id,
        workspace_id: workspace(WORKSPACE),
        team_id,
        expected_team_revision,
        actor: actor.actor(),
        observed_at: timestamp(observed_at_ms)?,
    })
}

fn team_revision(connection: &Connection, team_id: Uuid) -> Result<u64, Box<dyn Error>> {
    let revision: i64 = connection.query_row(
        "SELECT revision FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
        params![
            workspace(WORKSPACE).into_uuid().as_bytes(),
            team_id.as_bytes()
        ],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(revision)?)
}

fn membership_state(
    connection: &Connection,
    team_id: Uuid,
    actor: AgentIdentity,
) -> Result<Option<String>, rusqlite::Error> {
    connection
        .query_row(
            "SELECT state FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
            params![
                workspace(WORKSPACE).into_uuid().as_bytes(),
                team_id.as_bytes(),
                actor.participation.as_bytes(),
            ],
            |row| row.get(0),
        )
        .optional()
}

fn joined_membership_count(
    connection: &Connection,
    actor: AgentIdentity,
) -> Result<i64, rusqlite::Error> {
    connection.query_row(
        "SELECT COUNT(*) FROM ops_team_members WHERE workspace_id=?1 AND participation_id=?2 AND state='JOINED'",
        params![
            workspace(WORKSPACE).into_uuid().as_bytes(),
            actor.participation.as_bytes(),
        ],
        |row| row.get(0),
    )
}

fn joined_on_team(
    connection: &Connection,
    team_id: Uuid,
    actor: AgentIdentity,
) -> Result<bool, OperationsError> {
    let members = list_team_members(connection, workspace(WORKSPACE), team_id, 256)?;
    Ok(members.items().iter().any(|member| {
        member.participation_id().into_uuid() == actor.participation
            && member.state() == TeamMembershipState::Joined
    }))
}

fn race_join(
    mut connection: Connection,
    command: TeamJoinCommand,
    barrier: &Barrier,
) -> Result<bool, String> {
    let _wait = barrier.wait();
    match join_team(&mut connection, command) {
        Ok(_) => Ok(true),
        Err(OperationsError::Conflict(
            "participant already has a home Team"
            | "participant already has an outstanding Team expansion slot",
        )) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

#[test]
fn join_admission_keeps_one_home_and_checks_left_membership_rejoin() -> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let first = AgentIdentity::new(1);
    let second = AgentIdentity::new(2);
    let third = AgentIdentity::new(3);
    let fourth = AgentIdentity::new(4);
    seed_agents(&connection, &[first, second, third, fourth])?;
    create_team_with_members(
        &mut connection,
        Uuid::from_u128(0xE1100),
        TEAM_ONE,
        &[first, second],
    )?;
    create_team_with_members(
        &mut connection,
        Uuid::from_u128(0xE1200),
        TEAM_TWO,
        &[third, fourth],
    )?;

    let before_rejections = mutation_counts(&connection)?;
    assert!(matches!(
        join_team(
            &mut connection,
            join_command(Uuid::from_u128(0xE1300), TEAM_ONE, 1, first, 20)?,
        ),
        Err(OperationsError::Conflict("team member already joined"))
    ));
    assert_eq!(mutation_counts(&connection)?, before_rejections);

    assert!(matches!(
        join_team(
            &mut connection,
            join_command(Uuid::from_u128(0xE1310), TEAM_TWO, 1, first, 21)?,
        ),
        Err(OperationsError::Conflict(
            "participant already has a home Team"
        ))
    ));
    assert_eq!(mutation_counts(&connection)?, before_rejections);
    assert_eq!(team_revision(&connection, TEAM_ONE)?, 1);
    assert_eq!(team_revision(&connection, TEAM_TWO)?, 1);

    let left = leave_team(
        &mut connection,
        leave_command(Uuid::from_u128(0xE1320), TEAM_ONE, 1, first, 30)?,
    )?;
    assert_eq!(left.team().revision(), 2);
    assert_eq!(
        membership_state(&connection, TEAM_ONE, first)?.as_deref(),
        Some("LEFT")
    );
    let mut join = join_command(Uuid::from_u128(0xE1330), TEAM_TWO, 1, first, 40)?;
    join.expansion_slot = Some(
        seed_synthetic_expansion_slots(
            &connection,
            SyntheticExpansionSlotSeed {
                team_id: TEAM_TWO,
                decision_id: Uuid::from_u128(0xE1331),
                request_id: Uuid::from_u128(0xE1332),
                assignment_request_seed: Uuid::from_u128(0xE1333),
                expected_team_revision: 1,
                targets: &[first.actor()],
                base_time_ms: 35,
            },
        )?
        .into_iter()
        .next()
        .ok_or_else(|| IoError::other("synthetic Team expansion slot"))?,
    );
    let joined = join_team(&mut connection, join)?;
    assert_eq!(joined.team().revision(), 2);
    assert_eq!(
        membership_state(&connection, TEAM_TWO, first)?.as_deref(),
        Some("JOINED")
    );

    let before_rejoin = mutation_counts(&connection)?;
    assert!(matches!(
        join_team(
            &mut connection,
            join_command(Uuid::from_u128(0xE1340), TEAM_ONE, 2, first, 50)?,
        ),
        Err(OperationsError::Conflict(
            "participant already has a home Team"
        ))
    ));
    assert_eq!(mutation_counts(&connection)?, before_rejoin);
    assert_eq!(
        membership_state(&connection, TEAM_ONE, first)?.as_deref(),
        Some("LEFT")
    );
    assert_eq!(
        membership_state(&connection, TEAM_TWO, first)?.as_deref(),
        Some("JOINED")
    );
    assert_eq!(joined_membership_count(&connection, first)?, 1);
    Ok(())
}

#[test]
fn team_create_rejects_a_conflicting_initial_member_atomically() -> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let first = AgentIdentity::new(11);
    let second = AgentIdentity::new(12);
    let third = AgentIdentity::new(13);
    seed_agents(&connection, &[first, second, third])?;
    create_team_with_members(
        &mut connection,
        Uuid::from_u128(0xE2100),
        TEAM_ONE,
        &[first, second],
    )?;
    let before = mutation_counts(&connection)?;
    let conflicting = team_create_command(Uuid::from_u128(0xE2110), TEAM_TWO, &[first, third])?;

    assert!(matches!(
        create_team(&mut connection, conflicting),
        Err(OperationsError::Conflict(
            "participant already has a home Team"
        ))
    ));
    assert_eq!(mutation_counts(&connection)?, before);
    assert_eq!(membership_state(&connection, TEAM_TWO, first)?, None);
    assert_eq!(team_revision(&connection, TEAM_ONE)?, 1);
    Ok(())
}

#[test]
fn legacy_duplicate_homes_remain_readable_and_exact_replays_are_preserved()
-> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let first = AgentIdentity::new(31);
    let second = AgentIdentity::new(32);
    let third = AgentIdentity::new(33);
    let fourth = AgentIdentity::new(34);
    let fifth = AgentIdentity::new(35);
    seed_agents(&connection, &[first, second, third, fourth, fifth])?;
    let create_home = team_create_command(Uuid::from_u128(0xE4100), TEAM_ONE, &[first, second])?;
    create_team(&mut connection, create_home.clone())?;
    create_team_with_members(
        &mut connection,
        Uuid::from_u128(0xE4200),
        TEAM_TWO,
        &[third, fourth],
    )?;
    let mut old_join = join_command(Uuid::from_u128(0xE4300), TEAM_ONE, 1, fifth, 20)?;
    old_join.expansion_slot = Some(
        seed_synthetic_expansion_slots(
            &connection,
            SyntheticExpansionSlotSeed {
                team_id: TEAM_ONE,
                decision_id: Uuid::from_u128(0xE4301),
                request_id: Uuid::from_u128(0xE4302),
                assignment_request_seed: Uuid::from_u128(0xE4303),
                expected_team_revision: 1,
                targets: &[fifth.actor()],
                base_time_ms: 15,
            },
        )?
        .into_iter()
        .next()
        .ok_or_else(|| IoError::other("synthetic Team expansion slot"))?,
    );
    join_team(&mut connection, old_join.clone())?;

    let retired = retire_team(
        &mut connection,
        TeamRetireCommand {
            request_id: Uuid::from_u128(0xE4310),
            workspace_id: workspace(WORKSPACE),
            team_id: TEAM_ONE,
            expected_team_revision: 2,
            actor: local(),
            observed_at: timestamp(30)?,
        },
    )?;
    assert_eq!(retired.team().state(), TeamState::Retired);
    let before_retired_join = mutation_counts(&connection)?;
    assert!(matches!(
        join_team(
            &mut connection,
            join_command(Uuid::from_u128(0xE4320), TEAM_TWO, 1, first, 40)?,
        ),
        Err(OperationsError::Conflict(
            "participant already has a home Team"
        ))
    ));
    assert_eq!(mutation_counts(&connection)?, before_retired_join);

    for actor in [first, fifth] {
        connection.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,?4,NULL,'legacy-home')",
            params![
                workspace(WORKSPACE).into_uuid().as_bytes(),
                TEAM_TWO.as_bytes(),
                actor.participation.as_bytes(),
                50_i64,
            ],
        )?;
    }
    connection.execute(
        "UPDATE ops_teams SET revision=3,updated_at_ms=50 WHERE workspace_id=?1 AND team_id=?2",
        params![
            workspace(WORKSPACE).into_uuid().as_bytes(),
            TEAM_TWO.as_bytes()
        ],
    )?;

    let before_replays = mutation_counts(&connection)?;
    assert_duplicate_home_replays(
        &mut connection,
        &create_home,
        &old_join,
        first,
        fifth,
        &before_replays,
    )?;
    Ok(())
}

fn assert_duplicate_home_replays(
    connection: &mut Connection,
    create_home: &TeamCreateCommand,
    old_join: &TeamJoinCommand,
    first: AgentIdentity,
    fifth: AgentIdentity,
    before_replays: &MutationCounts,
) -> Result<(), Box<dyn Error>> {
    assert!(create_team(connection, create_home.clone())?.replayed());
    assert!(join_team(connection, old_join.clone())?.replayed());
    assert_eq!(&mutation_counts(connection)?, before_replays);
    assert_eq!(joined_membership_count(connection, first)?, 2);
    assert_eq!(joined_membership_count(connection, fifth)?, 2);
    for actor in [first, fifth] {
        for team_id in [TEAM_ONE, TEAM_TWO] {
            assert_eq!(
                membership_state(connection, team_id, actor)?.as_deref(),
                Some("JOINED")
            );
            assert!(joined_on_team(connection, team_id, actor)?);
        }
    }
    Ok(())
}

#[test]
fn initial_team_roster_cannot_claim_a_profile_with_an_outstanding_foreign_slot()
-> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let target = AgentIdentity::new(71);
    let first_peer = AgentIdentity::new(72);
    let second_peer = AgentIdentity::new(73);
    let new_peer = AgentIdentity::new(74);
    seed_agents(&connection, &[target, first_peer, second_peer, new_peer])?;
    create_team_with_members(
        &mut connection,
        Uuid::from_u128(0xE7100),
        TEAM_ONE,
        &[first_peer, second_peer],
    )?;

    seed_synthetic_expansion_slots(
        &connection,
        SyntheticExpansionSlotSeed {
            team_id: TEAM_ONE,
            decision_id: Uuid::from_u128(0xE7110),
            request_id: Uuid::from_u128(0xE7111),
            assignment_request_seed: Uuid::from_u128(0xE7112),
            expected_team_revision: 1,
            targets: &[target.actor()],
            base_time_ms: 20,
        },
    )?;
    let before = mutation_counts(&connection)?;
    let command = team_create_command(Uuid::from_u128(0xE7120), TEAM_THREE, &[target, new_peer])?;

    assert!(matches!(
        create_team(&mut connection, command),
        Err(OperationsError::Conflict(
            "participant already has an outstanding Team expansion slot"
        ))
    ));
    assert_eq!(mutation_counts(&connection)?, before);
    Ok(())
}

#[test]
fn a_replacement_session_for_the_same_profile_keeps_its_home_team() -> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let first = AgentIdentity::new(41);
    let second = AgentIdentity::new(42);
    let third = AgentIdentity::new(43);
    let fourth = AgentIdentity::new(44);
    seed_agents(&connection, &[first, second, third, fourth])?;
    create_team_with_members(
        &mut connection,
        Uuid::from_u128(0xE5100),
        TEAM_ONE,
        &[first, second],
    )?;
    create_team_with_members(
        &mut connection,
        Uuid::from_u128(0xE5200),
        TEAM_TWO,
        &[third, fourth],
    )?;

    let replacement_session = Uuid::from_u128(0xD9001);
    connection.execute(
        "UPDATE ops_workspace_participation_mappings SET session_id=?1,session_revision=2 WHERE workspace_id=?2 AND participation_id=?3",
        params![
            replacement_session.as_bytes(),
            workspace(WORKSPACE).into_uuid().as_bytes(),
            first.participation.as_bytes(),
        ],
    )?;
    connection.execute(
        "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3",
        params![
            replacement_session.as_bytes(),
            workspace(WORKSPACE).into_uuid().as_bytes(),
            first.participant.as_bytes(),
        ],
    )?;
    connection.execute(
        "UPDATE ops_current_effect_bindings SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3 AND activity_id=?4 AND agent_id=?5",
        params![
            replacement_session.as_bytes(),
            workspace(WORKSPACE).into_uuid().as_bytes(),
            first.participant.as_bytes(),
            first.activity.as_bytes(),
            first.agent.as_bytes(),
        ],
    )?;

    let before = mutation_counts(&connection)?;
    let replacement_actor = AgentIdentity {
        session: replacement_session,
        ..first
    };
    assert!(matches!(
        join_team(
            &mut connection,
            join_command(Uuid::from_u128(0xE5300), TEAM_TWO, 1, replacement_actor, 30)?,
        ),
        Err(OperationsError::Conflict(
            "participant already has a home Team"
        ))
    ));
    assert_eq!(mutation_counts(&connection)?, before);
    assert_eq!(
        membership_state(&connection, TEAM_ONE, first)?.as_deref(),
        Some("JOINED")
    );
    assert_eq!(membership_state(&connection, TEAM_TWO, first)?, None);
    Ok(())
}

#[test]
fn concurrent_joins_fail_closed_on_synthetic_cross_team_slot_overlap() -> Result<(), Box<dyn Error>>
{
    let root =
        std::env::temp_dir().join(format!("orchestration-team-home-race-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let database_path = root.join("operations.sqlite3");
    let first = AgentIdentity::new(51);
    let second = AgentIdentity::new(52);
    let third = AgentIdentity::new(53);
    let fourth = AgentIdentity::new(54);
    let fifth = AgentIdentity::new(55);
    let (left_command, right_command) = prepare_overlapping_slot_join_commands(
        &database_path,
        &[first, second, third, fourth, fifth],
    )?;

    let left_connection = create_connection(Some(&database_path))?;
    let right_connection = create_connection(Some(&database_path))?;
    left_connection.busy_timeout(Duration::from_secs(5))?;
    right_connection.busy_timeout(Duration::from_secs(5))?;
    let barrier = Arc::new(Barrier::new(2));
    let left_worker = {
        let barrier = Arc::clone(&barrier);
        let command = left_command;
        std::thread::spawn(move || race_join(left_connection, command, barrier.as_ref()))
    };
    let right_worker = {
        let barrier = Arc::clone(&barrier);
        let command = right_command;
        std::thread::spawn(move || race_join(right_connection, command, barrier.as_ref()))
    };
    let left_joined = left_worker
        .join()
        .map_err(|_| std::io::Error::other("left Team join worker panicked"))?
        .map_err(std::io::Error::other)?;
    let right_joined = right_worker
        .join()
        .map_err(|_| std::io::Error::other("right Team join worker panicked"))?
        .map_err(std::io::Error::other)?;
    assert!(!left_joined && !right_joined);

    let verification = create_connection(Some(&database_path))?;
    assert_no_membership_effects(&verification, first)?;
    drop(verification);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

fn prepare_overlapping_slot_join_commands(
    database_path: &PathBuf,
    agents: &[AgentIdentity; 5],
) -> Result<(TeamJoinCommand, TeamJoinCommand), Box<dyn Error>> {
    let [target, one, two, three, four] = *agents;
    let mut setup = create_connection(Some(database_path))?;
    seed_agents(&setup, agents)?;
    create_team_with_members(&mut setup, Uuid::from_u128(0xE6100), TEAM_ONE, &[one, two])?;
    create_team_with_members(
        &mut setup,
        Uuid::from_u128(0xE6200),
        TEAM_TWO,
        &[three, four],
    )?;

    // This deliberately seeds two conflicting synthetic owner slot
    // assignments. The public assignment API rejects the second one; this
    // race verifies that JOIN cannot consume either overlapping hold.
    let mut left_command = join_command(Uuid::from_u128(0xE6300), TEAM_ONE, 1, target, 30)?;
    left_command.expansion_slot = Some(synthetic_slot_for_join(
        &setup,
        TEAM_ONE,
        (0xE6301, 0xE6302, 0xE6303),
        target,
    )?);
    let mut right_command = join_command(Uuid::from_u128(0xE6310), TEAM_TWO, 1, target, 30)?;
    right_command.expansion_slot = Some(synthetic_slot_for_join(
        &setup,
        TEAM_TWO,
        (0xE6311, 0xE6312, 0xE6313),
        target,
    )?);
    Ok((left_command, right_command))
}

fn synthetic_slot_for_join(
    connection: &Connection,
    team_id: Uuid,
    ids: (u128, u128, u128),
    target: AgentIdentity,
) -> Result<crate::team_expansion::TeamExpansionSlotRef, Box<dyn Error>> {
    seed_synthetic_expansion_slots(
        connection,
        crate::teams::tests::SyntheticExpansionSlotSeed {
            team_id,
            decision_id: Uuid::from_u128(ids.0),
            request_id: Uuid::from_u128(ids.1),
            assignment_request_seed: Uuid::from_u128(ids.2),
            expected_team_revision: 1,
            targets: &[target.actor()],
            base_time_ms: 25,
        },
    )?
    .into_iter()
    .next()
    .ok_or_else(|| IoError::other("synthetic Team expansion slot").into())
}

fn assert_no_membership_effects(
    connection: &Connection,
    target: AgentIdentity,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(joined_membership_count(connection, target)?, 0);
    assert_eq!(
        connection.query_row(
            "SELECT COUNT(*) FROM ops_team_members WHERE workspace_id=?1 AND participation_id=?2",
            params![
                workspace(WORKSPACE).into_uuid().as_bytes(),
                target.participation.as_bytes()
            ],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    assert_eq!(
        connection.query_row(
            "SELECT COUNT(*) FROM ops_team_requests WHERE request_id IN (?1,?2)",
            params![
                Uuid::from_u128(0xE6300).as_bytes(),
                Uuid::from_u128(0xE6310).as_bytes()
            ],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    assert_eq!(team_revision(connection, TEAM_ONE)?, 1);
    assert_eq!(team_revision(connection, TEAM_TWO)?, 1);
    Ok(())
}
