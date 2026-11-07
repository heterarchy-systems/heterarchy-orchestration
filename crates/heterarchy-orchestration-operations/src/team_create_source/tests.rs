use std::error::Error;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::params;
use uuid::Uuid;

use super::{TeamCreateSourceStatus, validate};
use crate::{
    OperationalActor, OperationsError, TeamCreateCommand, TeamRosterMemberInput,
    teams::tests::{create_connection, create_team},
};

type TestResult = Result<(), Box<dyn Error>>;

const WORKSPACE: Uuid = Uuid::from_u128(0x1000);
const TEAM: Uuid = Uuid::from_u128(0xCA01);
const REQUEST: Uuid = Uuid::from_u128(0xCA02);
const ACTIVITY: Uuid = Uuid::from_u128(0x6000);
const AGENT: Uuid = Uuid::from_u128(0x7000);
const SESSION: Uuid = Uuid::from_u128(0x8000);
const ACTIVITY_TWO: Uuid = Uuid::from_u128(0x6100);
const AGENT_TWO: Uuid = Uuid::from_u128(0x7100);
const SESSION_TWO: Uuid = Uuid::from_u128(0x8100);
const PROFILE_THREE: Uuid = Uuid::from_u128(0x3A00);
const PARTICIPATION_THREE: Uuid = Uuid::from_u128(0x4A00);
const PARTICIPANT_THREE: Uuid = Uuid::from_u128(0x5A00);
const ACTIVITY_THREE: Uuid = Uuid::from_u128(0x6A00);
const AGENT_THREE: Uuid = Uuid::from_u128(0x7A00);
const SESSION_THREE: Uuid = Uuid::from_u128(0x8A00);

fn command() -> Result<TeamCreateCommand, OperationsError> {
    Ok(TeamCreateCommand {
        request_id: REQUEST,
        team_id: TEAM,
        workspace_id: WorkspaceId::from_uuid(WORKSPACE),
        actor: OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(ACTIVITY),
            agent_id: AgentInstanceId::from_uuid(AGENT),
            session_id: AgentSessionId::from_uuid(SESSION),
        },
        expected_sender_leadership_revision: Some(1),
        name: "Source Team".to_owned(),
        purpose: "Preserve exact creation source".to_owned(),
        initial_members: vec![
            TeamRosterMemberInput {
                activity_id: ActivityId::from_uuid(ACTIVITY),
                agent_id: AgentInstanceId::from_uuid(AGENT),
                session_id: AgentSessionId::from_uuid(SESSION),
                role: "leader".to_owned(),
            },
            TeamRosterMemberInput {
                activity_id: ActivityId::from_uuid(ACTIVITY_TWO),
                agent_id: AgentInstanceId::from_uuid(AGENT_TWO),
                session_id: AgentSessionId::from_uuid(SESSION_TWO),
                role: "member".to_owned(),
            },
        ],
        observed_at: TimestampMillis::new(20)
            .map_err(|_| OperationsError::Conflict("test timestamp"))?,
    })
}

#[test]
fn new_create_records_verified_source_and_replay_survives_member_rebind() -> TestResult {
    let mut connection = create_connection(None)?;
    let intent = command()?;
    let created = create_team(&mut connection, intent.clone())?;
    assert!(created.created());
    assert_eq!(
        validate(
            &connection,
            REQUEST,
            WorkspaceId::from_uuid(WORKSPACE),
            TEAM,
        )?,
        TeamCreateSourceStatus::Verified
    );

    connection.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND agent_id=?2",
        params![WORKSPACE.as_bytes(), AGENT_TWO.as_bytes()],
    )?;
    let replay = create_team(&mut connection, intent)?;
    assert!(replay.replayed());
    assert_eq!(
        validate(
            &connection,
            REQUEST,
            WorkspaceId::from_uuid(WORKSPACE),
            TEAM,
        )?,
        TeamCreateSourceStatus::Verified
    );
    Ok(())
}

#[test]
fn verified_source_rejects_changed_replay_and_source_mutation() -> TestResult {
    let mut connection = create_connection(None)?;
    let original = command()?;
    create_team(&mut connection, original.clone())?;

    let mut changed = original;
    changed.initial_members[1].role = "reviewer".to_owned();
    assert!(matches!(
        create_team(&mut connection, changed),
        Err(OperationsError::Conflict("team request replay"))
    ));
    assert!(
        connection
            .execute(
                "UPDATE ops_team_create_roster SET role='changed' WHERE request_id=?1",
                params![REQUEST.as_bytes()],
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "DELETE FROM ops_team_create_roster WHERE request_id=?1",
                params![REQUEST.as_bytes()],
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "UPDATE ops_team_member_events SET role='changed' WHERE request_id=?1",
                params![REQUEST.as_bytes()],
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "DELETE FROM ops_team_member_events WHERE request_id=?1",
                params![REQUEST.as_bytes()],
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "UPDATE ops_team_create_source_receipts SET member_count=3 WHERE request_id=?1",
                params![REQUEST.as_bytes()],
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "DELETE FROM ops_team_create_source_receipts WHERE request_id=?1",
                params![REQUEST.as_bytes()],
            )
            .is_err()
    );
    seed_extra_member(&connection, PROFILE_THREE, PARTICIPATION_THREE)?;
    assert!(
        insert_roster_member(
            &connection,
            2,
            PARTICIPATION_THREE,
            PARTICIPANT_THREE,
            PROFILE_THREE,
            ACTIVITY_THREE,
            AGENT_THREE,
            SESSION_THREE,
        )
        .is_err()
    );
    assert!(insert_member_event(&connection, PARTICIPATION_THREE).is_err());
    Ok(())
}

#[test]
fn digest_validation_rejects_equal_count_but_different_participation_sets() -> TestResult {
    let mut connection = create_connection(None)?;
    create_team(&mut connection, command()?)?;
    connection.execute_batch(
        "DROP TRIGGER ops_team_create_roster_insert_closed;
         DROP TRIGGER ops_team_member_events_insert_closed;",
    )?;
    let fourth_profile = Uuid::from_u128(0x3300);
    let fourth_participation = Uuid::from_u128(0x4300);
    seed_extra_member(&connection, PROFILE_THREE, PARTICIPATION_THREE)?;
    seed_extra_member(&connection, fourth_profile, fourth_participation)?;
    insert_roster_member(
        &connection,
        2,
        PARTICIPATION_THREE,
        PARTICIPANT_THREE,
        PROFILE_THREE,
        ACTIVITY_THREE,
        AGENT_THREE,
        SESSION_THREE,
    )?;
    insert_member_event(&connection, fourth_participation)?;
    assert!(matches!(
        validate(
            &connection,
            REQUEST,
            WorkspaceId::from_uuid(WORKSPACE),
            TEAM,
        ),
        Err(OperationsError::Corrupt(
            "Team create source roster/event cardinality"
        ))
    ));
    Ok(())
}

#[test]
fn missing_receipt_rolls_back_the_entire_new_team_create() -> TestResult {
    let mut connection = create_connection(None)?;
    connection.execute_batch(
        "DROP TRIGGER ops_team_create_roster_insert_closed;
         DROP TRIGGER ops_team_member_events_insert_closed;
         DROP TRIGGER ops_team_create_source_receipts_no_update;
         DROP TRIGGER ops_team_create_source_receipts_no_delete;
         DROP TABLE ops_team_create_source_receipts;",
    )?;
    assert!(create_team(&mut connection, command()?).is_err());
    for table in [
        "ops_teams",
        "ops_team_requests",
        "ops_team_members",
        "ops_team_member_events",
        "ops_team_create_roster",
        "ops_team_capacity_events",
    ] {
        let count: i64 =
            connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        assert_eq!(count, 0, "{table} must roll back");
    }
    Ok(())
}

fn seed_extra_member(
    connection: &rusqlite::Connection,
    profile_id: Uuid,
    participation_id: Uuid,
) -> TestResult {
    connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision)
         VALUES(?1,?2,?3,1,1)",
        params![
            WORKSPACE.as_bytes(),
            profile_id.as_bytes(),
            participation_id.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role)
         VALUES(?1,?2,?3,'JOINED',1,20,NULL,'member')",
        params![
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            participation_id.as_bytes(),
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_roster_member(
    connection: &rusqlite::Connection,
    ordinal: i64,
    participation_id: Uuid,
    participant_id: Uuid,
    profile_id: Uuid,
    activity_id: Uuid,
    agent_id: Uuid,
    session_id: Uuid,
) -> Result<usize, rusqlite::Error> {
    connection.execute(
        "INSERT INTO ops_team_create_roster(request_id,ordinal,workspace_id,team_id,
            participation_id,activity_id,agent_id,session_id,participant_id,profile_id,
            role,membership_revision,joined_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'member',1,20)",
        params![
            REQUEST.as_bytes(),
            ordinal,
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            participation_id.as_bytes(),
            activity_id.as_bytes(),
            agent_id.as_bytes(),
            session_id.as_bytes(),
            participant_id.as_bytes(),
            profile_id.as_bytes(),
        ],
    )
}

fn insert_member_event(
    connection: &rusqlite::Connection,
    participation_id: Uuid,
) -> Result<usize, rusqlite::Error> {
    connection.execute(
        "INSERT INTO ops_team_member_events(request_id,workspace_id,team_id,participation_id,
            state,membership_revision,actor_key,actor_display_name,actor_participation_id,
            actor_profile_id,actor_participant_id,actor_session_id,observed_at_ms,role)
         VALUES(?1,?2,?3,?4,'JOINED',1,?5,'Participant',?6,?7,?8,?9,20,'member')",
        params![
            REQUEST.as_bytes(),
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            participation_id.as_bytes(),
            format!("participant:{}", Uuid::from_u128(0x4000)),
            Uuid::from_u128(0x4000).as_bytes(),
            Uuid::from_u128(0x3000).as_bytes(),
            Uuid::from_u128(0x5000).as_bytes(),
            SESSION.as_bytes(),
        ],
    )
}
