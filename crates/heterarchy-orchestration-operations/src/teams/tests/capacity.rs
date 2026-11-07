use super::*;
use rusqlite::params;
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[derive(Clone, Copy)]
struct ParticipantIdentity {
    profile: Uuid,
    participation: Uuid,
    participant: Uuid,
    activity: Uuid,
    agent: Uuid,
    session: Uuid,
}

impl ParticipantIdentity {
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

    fn seed_if_missing(self, connection: &Connection) -> Result<(), Box<dyn Error>> {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2)",
            params![
                workspace(WORKSPACE).into_uuid().as_bytes(),
                self.profile.as_bytes(),
            ],
            |row| row.get(0),
        )?;
        if !exists {
            self.seed(connection)?;
        }
        Ok(())
    }

    fn actor(self) -> OperationalActor {
        OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(self.activity),
            agent_id: AgentInstanceId::from_uuid(self.agent),
            session_id: AgentSessionId::from_uuid(self.session),
        }
    }

    fn roster_member(self, role: String) -> TeamRosterMemberInput {
        TeamRosterMemberInput {
            activity_id: ActivityId::from_uuid(self.activity),
            agent_id: AgentInstanceId::from_uuid(self.agent),
            session_id: AgentSessionId::from_uuid(self.session),
            role,
        }
    }
}

fn seeded_participants() -> [ParticipantIdentity; 3] {
    [
        ParticipantIdentity {
            profile: PROFILE,
            participation: PARTICIPATION,
            participant: PARTICIPANT,
            activity: ACTIVITY,
            agent: AGENT,
            session: SESSION,
        },
        ParticipantIdentity {
            profile: PROFILE_TWO,
            participation: PARTICIPATION_TWO,
            participant: PARTICIPANT_TWO,
            activity: ACTIVITY_TWO,
            agent: AGENT_TWO,
            session: SESSION_TWO,
        },
        ParticipantIdentity {
            profile: PROFILE_THREE,
            participation: PARTICIPATION_THREE,
            participant: PARTICIPANT_THREE,
            activity: ACTIVITY_THREE,
            agent: AGENT_THREE,
            session: SESSION_THREE,
        },
    ]
}

fn create_command_for(
    request_id: Uuid,
    team_id: Uuid,
) -> Result<TeamCreateCommand, OperationsError> {
    let mut command = create_command(request_id, workspace(WORKSPACE))?;
    command.team_id = team_id;
    Ok(command)
}

fn create_command_for_roster(
    connection: &Connection,
    request_id: Uuid,
    team_id: Uuid,
    members: &[ParticipantIdentity; 2],
) -> Result<TeamCreateCommand, Box<dyn Error>> {
    for member in members.iter().copied() {
        member.seed_if_missing(connection)?;
    }
    let mut command = create_command_for(request_id, team_id)?;
    command.initial_members = members
        .iter()
        .copied()
        .enumerate()
        .map(|(index, member)| member.roster_member(format!("capacity-member-{index}")))
        .collect();
    Ok(command)
}

fn append_roster_members(
    connection: &Connection,
    command: &mut TeamCreateCommand,
    indices: std::ops::Range<u128>,
) -> Result<(), Box<dyn Error>> {
    for index in indices {
        let member = ParticipantIdentity::new(index);
        member.seed_if_missing(connection)?;
        command
            .initial_members
            .push(member.roster_member(format!("capacity-member-{index}")));
    }
    Ok(())
}

fn create_eight_member_team(
    connection: &mut Connection,
    team_id: Uuid,
    request_seed: u128,
) -> Result<(TeamCreateCommand, Vec<TeamJoinCommand>, u64), Box<dyn Error>> {
    let create = create_command_for(Uuid::from_u128(request_seed), team_id)?;
    create_team(connection, create.clone())?;

    let mut actors = Vec::with_capacity(6);
    actors.push(participant());
    for index in 1_u128..6 {
        let member = ParticipantIdentity::new(index + 3);
        member.seed(connection)?;
        actors.push(member.actor());
    }
    let slots = seed_synthetic_expansion_slots(
        connection,
        SyntheticExpansionSlotSeed {
            team_id,
            decision_id: Uuid::from_u128(request_seed + 100),
            request_id: Uuid::from_u128(request_seed + 101),
            assignment_request_seed: Uuid::from_u128(request_seed + 102),
            expected_team_revision: 1,
            targets: &actors,
            base_time_ms: 15,
        },
    )?;
    let mut joins = Vec::with_capacity(6);
    let mut revision = 1;
    for (index, actor) in actors.into_iter().enumerate() {
        let ordinal = u128::try_from(index)?;
        let request_id = Uuid::from_u128(request_seed + ordinal + 1);
        let command = TeamJoinCommand {
            request_id,
            workspace_id: workspace(WORKSPACE),
            team_id,
            expected_team_revision: revision,
            actor,
            role: format!("joined-{index}"),
            expansion_slot: Some(slots[index]),
            observed_at: timestamp(20 + i64::try_from(index)?)?,
        };
        revision = join_team(connection, command.clone())?.team().revision();
        joins.push(command);
    }
    Ok((create, joins, revision))
}

fn list_members(
    connection: &Connection,
    team_id: Uuid,
) -> Result<TeamReadSet<TeamMemberRecord>, OperationsError> {
    list_team_members(connection, workspace(WORKSPACE), team_id, 256)
}

fn joined_count(members: &TeamReadSet<TeamMemberRecord>) -> usize {
    members
        .items()
        .iter()
        .filter(|member| member.state() == TeamMembershipState::Joined)
        .count()
}

fn count_workspace_teams(connection: &Connection) -> Result<i64, rusqlite::Error> {
    connection.query_row(
        "SELECT COUNT(*) FROM ops_teams WHERE workspace_id=?1",
        params![workspace(WORKSPACE).into_uuid().as_bytes()],
        |row| row.get(0),
    )
}

fn count_workspace_participations(connection: &Connection) -> Result<i64, rusqlite::Error> {
    connection.query_row(
        "SELECT COUNT(*) FROM ops_workspace_participations WHERE workspace_id=?1",
        params![workspace(WORKSPACE).into_uuid().as_bytes()],
        |row| row.get(0),
    )
}

fn count_workspace_team_requests(connection: &Connection) -> Result<i64, rusqlite::Error> {
    connection.query_row(
        "SELECT COUNT(*) FROM ops_team_requests WHERE workspace_id=?1",
        params![workspace(WORKSPACE).into_uuid().as_bytes()],
        |row| row.get(0),
    )
}

fn count_team_members(connection: &Connection, team_id: Uuid) -> Result<i64, rusqlite::Error> {
    connection.query_row(
        "SELECT COUNT(*) FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2",
        params![
            workspace(WORKSPACE).into_uuid().as_bytes(),
            team_id.as_bytes()
        ],
        |row| row.get(0),
    )
}

fn count_team_member_events(connection: &Connection) -> Result<i64, rusqlite::Error> {
    connection.query_row(
        "SELECT COUNT(*) FROM ops_team_member_events WHERE workspace_id=?1",
        params![workspace(WORKSPACE).into_uuid().as_bytes()],
        |row| row.get(0),
    )
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

fn seed_workspace_team_limit_fixture(
    connection: &mut Connection,
) -> Result<TeamCreateCommand, Box<dyn Error>> {
    let mut first_create = None;
    let [first, second, third] = seeded_participants();
    let new_one = ParticipantIdentity::new(100);
    let new_two = ParticipantIdentity::new(101);
    let new_three = ParticipantIdentity::new(102);
    let new_four = ParticipantIdentity::new(103);
    let new_five = ParticipantIdentity::new(104);
    let rosters = [
        [first, second],
        [third, new_one],
        [new_two, new_three],
        [new_four, new_five],
    ];

    for (index, roster) in (0_u128..4).zip(rosters) {
        let request_id = Uuid::from_u128(0xB1000 + index);
        let team_id = Uuid::from_u128(0xB2000 + index);
        let command = create_command_for_roster(connection, request_id, team_id, &roster)?;
        create_team(connection, command.clone())?;
        if index == 0 {
            retire_team(
                connection,
                TeamRetireCommand {
                    request_id: Uuid::from_u128(0xB3000),
                    workspace_id: workspace(WORKSPACE),
                    team_id,
                    expected_team_revision: 1,
                    actor: local(),
                    observed_at: timestamp(20)?,
                },
            )?;
            first_create = Some(command);
        }
    }
    assert!(count_workspace_participations(connection)? <= 12);
    Ok(first_create.ok_or_else(|| std::io::Error::other("first Team create command"))?)
}

fn insert_and_read_legacy_team_row(connection: &Connection) -> Result<Uuid, Box<dyn Error>> {
    let legacy_team_id = Uuid::from_u128(0xB2020);
    connection.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms) SELECT ?1,workspace_id,'Legacy Team','Imported before the workspace cap','ACTIVE',1,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,30,30,30 FROM ops_teams WHERE workspace_id=?2 AND team_id=?3",
        params![
            legacy_team_id.as_bytes(),
            workspace(WORKSPACE).into_uuid().as_bytes(),
            Uuid::from_u128(0xB2000).as_bytes(),
        ],
    )?;

    let legacy_page = list_teams(connection, workspace(WORKSPACE), 256)?;
    assert_eq!(legacy_page.items().len(), 5);
    assert!(!legacy_page.truncated());
    let legacy = legacy_page
        .items()
        .iter()
        .find(|team| team.team_id() == legacy_team_id)
        .ok_or_else(|| std::io::Error::other("legacy Team row"))?;
    assert_eq!(legacy.state(), TeamState::Active);
    Ok(legacy_team_id)
}

#[test]
fn team_capacity_workspace_limit_counts_retired_rows_replays_and_preserves_legacy_rows()
-> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let first_create = seed_workspace_team_limit_fixture(&mut connection)?;

    let teams = list_teams(&connection, workspace(WORKSPACE), 256)?;
    assert_eq!(teams.items().len(), 4);
    assert_eq!(
        teams
            .items()
            .iter()
            .filter(|team| team.state() == TeamState::Retired)
            .count(),
        1
    );

    let replay = create_team(&mut connection, first_create)?;
    assert!(replay.replayed());

    let fifth = create_command_for_roster(
        &connection,
        Uuid::from_u128(0xB1010),
        Uuid::from_u128(0xB2010),
        &[ParticipantIdentity::new(105), ParticipantIdentity::new(106)],
    )?;
    assert!(count_workspace_participations(&connection)? <= 12);
    assert!(matches!(
        create_team(&mut connection, fifth),
        Err(OperationsError::Bounds(_))
    ));
    assert_eq!(count_workspace_teams(&connection)?, 4);
    assert_eq!(count_workspace_team_requests(&connection)?, 5);
    assert_eq!(count_team_member_events(&connection)?, 8);

    let legacy_team_id = insert_and_read_legacy_team_row(&connection)?;

    let retired = retire_team(
        &mut connection,
        TeamRetireCommand {
            request_id: Uuid::from_u128(0xB3010),
            workspace_id: workspace(WORKSPACE),
            team_id: legacy_team_id,
            expected_team_revision: 1,
            actor: local(),
            observed_at: timestamp(40)?,
        },
    )?;
    assert_eq!(retired.team().state(), TeamState::Retired);
    assert_eq!(
        list_teams(&connection, workspace(WORKSPACE), 256)?
            .items()
            .len(),
        5
    );

    let over_capacity = create_command_for_roster(
        &connection,
        Uuid::from_u128(0xB1020),
        Uuid::from_u128(0xB2030),
        &[ParticipantIdentity::new(107), ParticipantIdentity::new(108)],
    )?;
    assert!(matches!(
        create_team(&mut connection, over_capacity),
        Err(OperationsError::Bounds(_))
    ));
    assert_eq!(count_workspace_participations(&connection)?, 12);
    assert_eq!(count_workspace_teams(&connection)?, 5);
    assert_eq!(count_workspace_team_requests(&connection)?, 6);
    Ok(())
}

#[test]
fn team_capacity_initial_roster_accepts_eight_and_rejects_nine_without_partial_effects()
-> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let mut valid = create_command_for(Uuid::from_u128(0xB4000), Uuid::from_u128(0xB4100))?;
    append_roster_members(&connection, &mut valid, 4..10)?;
    assert_eq!(valid.initial_members.len(), 8);
    let created = create_team(&mut connection, valid)?;
    assert_eq!(created.initial_members().len(), 8);

    let mut oversized = create_command_for(Uuid::from_u128(0xB4010), Uuid::from_u128(0xB4110))?;
    append_roster_members(&connection, &mut oversized, 4..11)?;
    assert_eq!(oversized.initial_members.len(), 9);
    assert!(matches!(
        create_team(&mut connection, oversized),
        Err(OperationsError::Bounds(_))
    ));

    assert_eq!(count_workspace_teams(&connection)?, 1);
    assert_eq!(count_workspace_team_requests(&connection)?, 1);
    assert_eq!(
        count_team_members(&connection, created.team().team_id())?,
        8
    );
    assert_eq!(count_team_member_events(&connection)?, 8);
    assert_eq!(
        list_teams(&connection, workspace(WORKSPACE), 256)?
            .items()
            .len(),
        1
    );
    Ok(())
}

#[test]
fn team_capacity_join_limit_rejects_ninth_replays_and_leave_frees_a_slot()
-> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let team_id = Uuid::from_u128(0xB5000);
    let (_, joins, revision) = create_eight_member_team(&mut connection, team_id, 0xB5100)?;
    assert_eq!(revision, 7);
    assert_eq!(joined_count(&list_members(&connection, team_id)?), 8);

    let replay = join_team(
        &mut connection,
        joins
            .last()
            .cloned()
            .ok_or_else(|| std::io::Error::other("last Team join command"))?,
    )?;
    assert!(replay.replayed());

    let ninth = ParticipantIdentity::new(9);
    ninth.seed(&connection)?;
    let rejected = TeamJoinCommand {
        request_id: Uuid::from_u128(0xB5200),
        workspace_id: workspace(WORKSPACE),
        team_id,
        expected_team_revision: revision,
        actor: ninth.actor(),
        role: "ninth".to_owned(),
        expansion_slot: None,
        observed_at: timestamp(40)?,
    };
    assert!(matches!(
        join_team(&mut connection, rejected),
        Err(OperationsError::Conflict(
            "new Team join requires approved expansion slot"
        ))
    ));
    assert_eq!(team_revision(&connection, team_id)?, revision);
    assert_eq!(joined_count(&list_members(&connection, team_id)?), 8);
    assert_eq!(count_workspace_team_requests(&connection)?, 7);
    assert_eq!(count_team_member_events(&connection)?, 8);

    let left = leave_team(
        &mut connection,
        TeamLeaveCommand {
            request_id: Uuid::from_u128(0xB5210),
            workspace_id: workspace(WORKSPACE),
            team_id,
            expected_team_revision: revision,
            actor: participant(),
            observed_at: timestamp(50)?,
        },
    )?;
    assert_eq!(
        left.membership()
            .ok_or_else(|| std::io::Error::other("left membership"))?
            .state(),
        TeamMembershipState::Left
    );
    let next_revision = left.team().revision();
    let next_slot = seed_synthetic_expansion_slots(
        &connection,
        SyntheticExpansionSlotSeed {
            team_id,
            decision_id: Uuid::from_u128(0xB5221),
            request_id: Uuid::from_u128(0xB5222),
            assignment_request_seed: Uuid::from_u128(0xB5223),
            expected_team_revision: next_revision,
            targets: &[ninth.actor()],
            base_time_ms: 3_600_100,
        },
    )?
    .into_iter()
    .next()
    .ok_or_else(|| std::io::Error::other("post-cooldown expansion slot"))?;
    let joined = join_team(
        &mut connection,
        TeamJoinCommand {
            request_id: Uuid::from_u128(0xB5220),
            workspace_id: workspace(WORKSPACE),
            team_id,
            expected_team_revision: next_revision,
            actor: ninth.actor(),
            role: "ninth".to_owned(),
            expansion_slot: Some(next_slot),
            observed_at: timestamp(3_600_200)?,
        },
    )?;
    assert_eq!(
        joined.membership().map(TeamMemberRecord::state),
        Some(TeamMembershipState::Joined)
    );
    let members = list_members(&connection, team_id)?;
    assert_eq!(members.items().len(), 9);
    assert_eq!(joined_count(&members), 8);
    assert!(!members.truncated());
    Ok(())
}

#[test]
fn team_capacity_legacy_overcap_team_is_readable_replayable_and_can_leave_without_truncation()
-> Result<(), Box<dyn Error>> {
    let mut connection = create_connection(None)?;
    let team_id = Uuid::from_u128(0xB6000);
    let (create, joins, current_revision) =
        create_eight_member_team(&mut connection, team_id, 0xB6100)?;
    let legacy = ParticipantIdentity::new(9);
    legacy.seed(&connection)?;
    let legacy_joined_at = 40;
    connection.execute(
        "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,?4,NULL,'legacy')",
        params![
            workspace(WORKSPACE).into_uuid().as_bytes(),
            team_id.as_bytes(),
            legacy.participation.as_bytes(),
            legacy_joined_at,
        ],
    )?;
    let overcap_revision = current_revision + 1;
    connection.execute(
        "UPDATE ops_teams SET revision=?1,updated_at_ms=?2 WHERE workspace_id=?3 AND team_id=?4",
        params![
            i64::try_from(overcap_revision)?,
            legacy_joined_at,
            workspace(WORKSPACE).into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;

    let overcap_page = list_members(&connection, team_id)?;
    assert_eq!(overcap_page.items().len(), 9);
    assert_eq!(joined_count(&overcap_page), 9);
    assert!(!overcap_page.truncated());
    assert!(create_team(&mut connection, create)?.replayed());
    assert!(
        join_team(
            &mut connection,
            joins
                .last()
                .cloned()
                .ok_or_else(|| std::io::Error::other("last Team join command"))?,
        )?
        .replayed()
    );

    let left = leave_team(
        &mut connection,
        TeamLeaveCommand {
            request_id: Uuid::from_u128(0xB6200),
            workspace_id: workspace(WORKSPACE),
            team_id,
            expected_team_revision: overcap_revision,
            actor: legacy.actor(),
            observed_at: timestamp(50)?,
        },
    )?;
    assert_eq!(
        left.membership()
            .ok_or_else(|| std::io::Error::other("legacy left membership"))?
            .state(),
        TeamMembershipState::Left
    );
    let after_leave = list_members(&connection, team_id)?;
    assert_eq!(after_leave.items().len(), 9);
    assert_eq!(joined_count(&after_leave), 8);
    assert_eq!(
        after_leave
            .items()
            .iter()
            .filter(|member| member.state() == TeamMembershipState::Left)
            .count(),
        1
    );
    assert!(!after_leave.truncated());
    assert_eq!(count_team_members(&connection, team_id)?, 9);
    Ok(())
}

fn race_workspace_team_create(
    mut connection: Connection,
    command: TeamCreateCommand,
    barrier: &Barrier,
) -> Result<bool, String> {
    let _wait_result = barrier.wait();
    match create_team(&mut connection, command) {
        Ok(_) => Ok(true),
        Err(OperationsError::Bounds("workspace team capacity")) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

fn seed_three_persistent_teams(database_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let mut seeded = create_connection(Some(database_path))?;
    let [first, second, third] = seeded_participants();
    let rosters = [
        [second, third],
        [first, ParticipantIdentity::new(200)],
        [ParticipantIdentity::new(201), ParticipantIdentity::new(202)],
    ];
    for (index, roster) in (0_u128..3).zip(rosters) {
        let command = create_command_for_roster(
            &seeded,
            Uuid::from_u128(0xC1000 + index),
            Uuid::from_u128(0xC2000 + index),
            &roster,
        )?;
        create_team(&mut seeded, command)?;
    }
    Ok(())
}

fn race_two_team_creates(
    database_path: &PathBuf,
    left_command: &TeamCreateCommand,
    right_command: &TeamCreateCommand,
) -> Result<(bool, bool), Box<dyn Error>> {
    let left_connection = create_connection(Some(database_path))?;
    let right_connection = create_connection(Some(database_path))?;
    left_connection.busy_timeout(Duration::from_secs(5))?;
    right_connection.busy_timeout(Duration::from_secs(5))?;
    let barrier = Arc::new(Barrier::new(2));

    let left_worker = {
        let barrier = Arc::clone(&barrier);
        let command = left_command.clone();
        std::thread::spawn(move || race_workspace_team_create(left_connection, command, &barrier))
    };
    let right_worker = {
        let barrier = Arc::clone(&barrier);
        let command = right_command.clone();
        std::thread::spawn(move || race_workspace_team_create(right_connection, command, &barrier))
    };

    let left_result = left_worker.join();
    let right_result = right_worker.join();
    let left_created = left_result
        .map_err(|_| std::io::Error::other("left Team create worker panicked"))?
        .map_err(std::io::Error::other)?;
    let right_created = right_result
        .map_err(|_| std::io::Error::other("right Team create worker panicked"))?
        .map_err(std::io::Error::other)?;
    Ok((left_created, right_created))
}

fn assert_rejected_team_create_has_no_rows(
    connection: &Connection,
    command: &TeamCreateCommand,
) -> Result<(), Box<dyn Error>> {
    let request_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_requests WHERE request_id=?1",
        params![command.request_id.as_bytes()],
        |row| row.get(0),
    )?;
    let team_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
        params![
            workspace(WORKSPACE).into_uuid().as_bytes(),
            command.team_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    let roster_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_create_roster WHERE request_id=?1",
        params![command.request_id.as_bytes()],
        |row| row.get(0),
    )?;
    let event_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_member_events WHERE request_id=?1",
        params![command.request_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(request_count, 0);
    assert_eq!(team_count, 0);
    assert_eq!(count_team_members(connection, command.team_id)?, 0);
    assert_eq!(roster_count, 0);
    assert_eq!(event_count, 0);
    Ok(())
}

#[test]
fn team_capacity_concurrent_creates_serialize_the_last_workspace_slot() -> Result<(), Box<dyn Error>>
{
    let root = std::env::temp_dir().join(format!(
        "orchestration-team-capacity-race-{}",
        Uuid::now_v7()
    ));
    std::fs::create_dir(&root)?;
    let database_path = root.join("operations.sqlite3");
    seed_three_persistent_teams(&database_path)?;
    let (left_command, right_command) = {
        let seeding = create_connection(Some(&database_path))?;
        let left = create_command_for_roster(
            &seeding,
            Uuid::from_u128(0xC3000),
            Uuid::from_u128(0xC4000),
            &[ParticipantIdentity::new(203), ParticipantIdentity::new(204)],
        )?;
        let right = create_command_for_roster(
            &seeding,
            Uuid::from_u128(0xC3001),
            Uuid::from_u128(0xC4001),
            &[ParticipantIdentity::new(205), ParticipantIdentity::new(206)],
        )?;
        assert!(count_workspace_participations(&seeding)? <= 12);
        (left, right)
    };
    let (left_created, right_created) =
        race_two_team_creates(&database_path, &left_command, &right_command)?;
    assert_ne!(left_created, right_created);

    let (winner, loser) = if left_created {
        (left_command, right_command)
    } else {
        (right_command, left_command)
    };
    let mut verification = create_connection(Some(&database_path))?;
    let teams = list_teams(&verification, workspace(WORKSPACE), 256)?;
    assert_eq!(teams.items().len(), 4);
    assert!(!teams.truncated());
    assert_eq!(count_workspace_teams(&verification)?, 4);
    assert_eq!(count_workspace_team_requests(&verification)?, 4);
    assert_eq!(count_team_member_events(&verification)?, 8);
    assert_eq!(count_team_members(&verification, winner.team_id)?, 2);
    assert_rejected_team_create_has_no_rows(&verification, &loser)?;

    let replay = create_team(&mut verification, winner.clone())?;
    assert!(replay.replayed());
    assert_eq!(count_workspace_teams(&verification)?, 4);
    assert_eq!(count_workspace_team_requests(&verification)?, 4);

    drop(verification);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
