use super::super::*;
use crate::team_create_source::{TeamCreateSourceStatus, validate};
use forge_domain::WorkspaceId;
use rusqlite::{Connection, params};
use std::{error::Error, fs, path::PathBuf};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error>>;

const DESTINATION: Uuid = Uuid::from_u128(0x5901);
const WORKSPACE: Uuid = Uuid::from_u128(0x5902);
const TEAM: Uuid = Uuid::from_u128(0x5903);
const REQUEST: Uuid = Uuid::from_u128(0x5904);

struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "operations-team-create-source-v58-{}.sqlite",
                Uuid::now_v7()
            )),
        }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(self.path.with_extension("sqlite-shm"));
    }
}

#[test]
fn v58_upgrade_preserves_roster_without_authenticating_it() -> TestResult {
    let database = TestDatabase::new();
    let catalog = OperationsCatalog::open(&database.path, DESTINATION)?;
    seed_v58_create_history(&catalog.connection)?;
    let roster_before = roster_rows(&catalog.connection)?;
    assert_eq!(roster_before.len(), 2);
    drop_v59_team_create_source_for_v58_fixture(&catalog.connection)?;
    catalog.connection.pragma_update(None, "user_version", 58)?;
    validate_schema_v58(&catalog.connection)?;
    drop(catalog);

    let upgraded = OperationsCatalog::open(&database.path, DESTINATION)?;
    assert_v58_upgrade_source_state(&upgraded, &roster_before)?;
    assert_legacy_source_is_closed(&upgraded.connection)?;
    assert_no_foreign_key_violations(&upgraded.connection)
}

#[test]
fn v59_upgrade_classifies_existing_approval_without_backfilling_holds() -> TestResult {
    let database = TestDatabase::new();
    let catalog = OperationsCatalog::open(&database.path, DESTINATION)?;
    seed_v58_create_history(&catalog.connection)?;
    seed_legacy_expansion_approval(&catalog.connection)?;
    drop_v60_workspace_holds_for_v59_fixture(&catalog.connection)?;
    catalog.connection.pragma_update(None, "user_version", 59)?;
    validate_schema_v59(&catalog.connection)?;
    drop(catalog);

    let upgraded = OperationsCatalog::open(&database.path, DESTINATION)?;
    assert_eq!(catalog_version(&upgraded.connection, DESTINATION)?, VERSION);
    validate_schema(&upgraded.connection)?;
    assert_pre_hold_upgrade(&upgraded.connection)?;
    assert_no_foreign_key_violations(&upgraded.connection)?;
    drop(upgraded);

    let read_only = OperationsCatalog::open_existing_read_only(&database.path, DESTINATION)?;
    validate_schema(&read_only.connection)?;
    assert_pre_hold_upgrade(&read_only.connection)
}

#[test]
fn v60_upgrade_adds_empty_recovery_source_without_touching_pre_hold_history() -> TestResult {
    let database = TestDatabase::new();
    let catalog = OperationsCatalog::open(&database.path, DESTINATION)?;
    seed_v58_create_history(&catalog.connection)?;
    seed_legacy_expansion_approval(&catalog.connection)?;
    let transaction = catalog.connection.unchecked_transaction()?;
    crate::team_expansion::classify_pre_hold_decisions(&transaction)?;
    transaction.commit()?;
    drop_v61_recovery_for_v60_fixture(&catalog.connection)?;
    catalog.connection.pragma_update(None, "user_version", 60)?;
    validate_schema_v60(&catalog.connection)?;
    drop(catalog);

    let upgraded = OperationsCatalog::open(&database.path, DESTINATION)?;
    validate_schema(&upgraded.connection)?;
    assert_pre_hold_upgrade(&upgraded.connection)?;
    let recoveries: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_team_expansion_legacy_slot_recoveries",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(recoveries, 0);
    assert_no_foreign_key_violations(&upgraded.connection)?;
    drop(upgraded);
    let read_only = OperationsCatalog::open_existing_read_only(&database.path, DESTINATION)?;
    validate_schema(&read_only.connection)?;
    assert_pre_hold_upgrade(&read_only.connection)
}

fn assert_pre_hold_upgrade(connection: &Connection) -> TestResult {
    let legacy_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_expansion_workspace_hold_pre_hold_decisions
         WHERE decision_id=?1 AND classification='PRE_HOLD'",
        params![Uuid::from_u128(0x5910).as_bytes()],
        |row| row.get(0),
    )?;
    let manifest_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_expansion_workspace_hold_manifests",
        [],
        |row| row.get(0),
    )?;
    let plan_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_expansion_workspace_hold_plans",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(legacy_count, 1);
    assert_eq!(
        manifest_count, 0,
        "legacy approval gets no retroactive hold"
    );
    assert_eq!(plan_count, 0, "legacy approval gets no target plan");
    Ok(())
}

fn seed_legacy_expansion_approval(connection: &Connection) -> TestResult {
    let creator = member(0x10);
    let expansion_request = Uuid::from_u128(0x590F);
    let decision = Uuid::from_u128(0x5910);
    connection.execute(
        "INSERT INTO ops_team_expansion_requests(request_id,workspace_id,team_id,
            expected_team_revision,capacity_revision,requester_leadership_revision,
            requested_count,reason,actor_activity_id,actor_agent_id,actor_session_id,
            actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,
            recorded_at_ms)
         VALUES(?1,?2,?3,1,1,1,1,'legacy approval',?4,?5,?6,?7,?8,?9,
            'Legacy creator',20)",
        params![
            expansion_request.as_bytes(),
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            creator.activity.as_bytes(),
            creator.agent.as_bytes(),
            creator.session.as_bytes(),
            creator.participation.as_bytes(),
            creator.profile.as_bytes(),
            creator.participant.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_decisions(decision_id,request_id,workspace_id,team_id,
            state,approved_count,expected_team_revision,capacity_revision,leadership_revision,
            actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,
            actor_profile_id,actor_participant_id,actor_display_name,decided_at_ms)
         VALUES(?1,?2,?3,?4,'APPROVED',1,1,1,1,?5,?6,?7,?8,?9,?10,
            'Legacy creator',30)",
        params![
            decision.as_bytes(),
            expansion_request.as_bytes(),
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            creator.activity.as_bytes(),
            creator.agent.as_bytes(),
            creator.session.as_bytes(),
            creator.participation.as_bytes(),
            creator.profile.as_bytes(),
            creator.participant.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_slots(decision_id,request_id,ordinal,workspace_id,team_id)
         VALUES(?1,?2,0,?3,?4)",
        params![
            decision.as_bytes(),
            expansion_request.as_bytes(),
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
        ],
    )?;
    Ok(())
}

fn assert_v58_upgrade_source_state(
    upgraded: &OperationsCatalog,
    roster_before: &[Vec<rusqlite::types::Value>],
) -> TestResult {
    assert_eq!(catalog_version(&upgraded.connection, DESTINATION)?, VERSION);
    validate_schema(&upgraded.connection)?;
    assert_eq!(roster_rows(&upgraded.connection)?, roster_before);
    assert_eq!(
        validate(
            &upgraded.connection,
            REQUEST,
            WorkspaceId::from_uuid(WORKSPACE),
            TEAM,
        )?,
        TeamCreateSourceStatus::LegacyUnverified
    );
    let receipt_count: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_team_create_source_receipts",
        [],
        |row| row.get(0),
    )?;
    let legacy_count: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_team_create_source_legacy WHERE request_id=?1",
        params![REQUEST.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(
        receipt_count, 0,
        "migration must not backfill a source digest"
    );
    assert_eq!(legacy_count, 1, "migration records provenance status only");
    Ok(())
}

fn assert_legacy_source_is_closed(connection: &Connection) -> TestResult {
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
                "UPDATE ops_team_member_events SET role='changed' WHERE request_id=?1",
                params![REQUEST.as_bytes()],
            )
            .is_err()
    );
    let extra = member(0x30);
    connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision)
         VALUES(?1,?2,?3,1,1)",
        params![
            WORKSPACE.as_bytes(),
            extra.profile.as_bytes(),
            extra.participation.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role)
         VALUES(?1,?2,?3,'JOINED',1,10,NULL,'member')",
        params![
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            extra.participation.as_bytes(),
        ],
    )?;
    assert!(
        connection
            .execute(
                "INSERT INTO ops_team_create_roster(request_id,ordinal,workspace_id,team_id,
                    participation_id,activity_id,agent_id,session_id,participant_id,profile_id,
                    role,membership_revision,joined_at_ms)
                 VALUES(?1,2,?2,?3,?4,?5,?6,?7,?8,?9,'member',1,10)",
                params![
                    REQUEST.as_bytes(),
                    WORKSPACE.as_bytes(),
                    TEAM.as_bytes(),
                    extra.participation.as_bytes(),
                    extra.activity.as_bytes(),
                    extra.agent.as_bytes(),
                    extra.session.as_bytes(),
                    extra.participant.as_bytes(),
                    extra.profile.as_bytes(),
                ],
            )
            .is_err()
    );
    let creator = member(0x10);
    assert!(
        connection
            .execute(
                "INSERT INTO ops_team_member_events(request_id,workspace_id,team_id,participation_id,
                    state,membership_revision,actor_key,actor_display_name,actor_participation_id,
                    actor_profile_id,actor_participant_id,actor_session_id,observed_at_ms,role)
                 VALUES(?1,?2,?3,?4,'JOINED',1,'legacy','Legacy creator',?5,?6,?7,?8,10,'member')",
                params![
                    REQUEST.as_bytes(),
                    WORKSPACE.as_bytes(),
                    TEAM.as_bytes(),
                    extra.participation.as_bytes(),
                    creator.participation.as_bytes(),
                    creator.profile.as_bytes(),
                    creator.participant.as_bytes(),
                    creator.session.as_bytes(),
                ],
            )
            .is_err()
    );
    Ok(())
}

fn seed_v58_create_history(connection: &Connection) -> TestResult {
    let first = member(0x10);
    let second = member(0x20);
    seed_v58_participations(connection, &[first, second])?;
    seed_v58_team_request_and_capacity(connection, first)?;
    seed_v58_roster(connection, first, &[first, second])
}

fn seed_v58_participations(connection: &Connection, members: &[Member]) -> TestResult {
    for member in members {
        connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision)
             VALUES(?1,?2,?3,1,1)",
            params![
                WORKSPACE.as_bytes(),
                member.profile.as_bytes(),
                member.participation.as_bytes(),
            ],
        )?;
    }
    Ok(())
}

fn seed_v58_team_request_and_capacity(connection: &Connection, creator: Member) -> TestResult {
    connection.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,
            created_by_display_name,created_by_participation_id,created_by_profile_id,
            created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms)
         VALUES(?1,?2,'Legacy Team','v58 source','ACTIVE',1,?3,'Legacy creator',?4,?5,?6,?7,10,10,10)",
        params![
            TEAM.as_bytes(),
            WORKSPACE.as_bytes(),
            format!("participant:{}", creator.participation),
            creator.participation.as_bytes(),
            creator.profile.as_bytes(),
            creator.participant.as_bytes(),
            creator.session.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_requests(request_id,operation,workspace_id,team_id,actor_key,
            actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,
            actor_session_id,name,purpose,expected_team_revision,result_team_state,
            result_team_revision,result_team_updated_at_ms,result_membership_state,
            result_membership_participation_id,result_membership_revision,
            result_membership_joined_at_ms,result_membership_left_at_ms,created,observed_at_ms,
            request_actor_kind,request_actor_activity_id,request_actor_agent_id,
            request_leadership_revision,role,result_membership_role)
         VALUES(?1,'CREATE',?2,?3,?4,'Legacy creator',?5,?6,?7,?8,'Legacy Team',
            'v58 source',NULL,'ACTIVE',1,10,NULL,NULL,NULL,NULL,NULL,1,10,
            'PARTICIPANT',?9,?10,1,NULL,NULL)",
        params![
            REQUEST.as_bytes(),
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            format!("participant:{}", creator.participation),
            creator.participation.as_bytes(),
            creator.profile.as_bytes(),
            creator.participant.as_bytes(),
            creator.session.as_bytes(),
            creator.activity.as_bytes(),
            creator.agent.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_capacity_events(request_id,workspace_id,team_id,capacity_revision,
            expected_capacity_revision,expected_team_revision,result_team_revision,max_members,
            operation,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,
            actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms,
            leadership_revision)
         VALUES(?1,?2,?3,1,0,0,1,2,'CREATE',?4,?5,?6,?7,?8,?9,'Legacy creator',10,1)",
        params![
            REQUEST.as_bytes(),
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            creator.activity.as_bytes(),
            creator.agent.as_bytes(),
            creator.session.as_bytes(),
            creator.participation.as_bytes(),
            creator.profile.as_bytes(),
            creator.participant.as_bytes(),
        ],
    )?;
    Ok(())
}

fn seed_v58_roster(connection: &Connection, creator: Member, members: &[Member]) -> TestResult {
    for (ordinal, member) in members.iter().copied().enumerate() {
        connection.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,
                joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,10,NULL,?4)",
            params![
                WORKSPACE.as_bytes(),
                TEAM.as_bytes(),
                member.participation.as_bytes(),
                member.role,
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_team_create_roster(request_id,ordinal,workspace_id,team_id,
                participation_id,activity_id,agent_id,session_id,participant_id,profile_id,
                role,membership_revision,joined_at_ms)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,1,10)",
            params![
                REQUEST.as_bytes(),
                i64::try_from(ordinal)?,
                WORKSPACE.as_bytes(),
                TEAM.as_bytes(),
                member.participation.as_bytes(),
                member.activity.as_bytes(),
                member.agent.as_bytes(),
                member.session.as_bytes(),
                member.participant.as_bytes(),
                member.profile.as_bytes(),
                member.role,
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_team_member_events(request_id,workspace_id,team_id,participation_id,
                state,membership_revision,actor_key,actor_display_name,actor_participation_id,
                actor_profile_id,actor_participant_id,actor_session_id,observed_at_ms,role)
             VALUES(?1,?2,?3,?4,'JOINED',1,?5,'Legacy creator',?6,?7,?8,?9,10,?10)",
            params![
                REQUEST.as_bytes(),
                WORKSPACE.as_bytes(),
                TEAM.as_bytes(),
                member.participation.as_bytes(),
                format!("participant:{}", creator.participation),
                creator.participation.as_bytes(),
                creator.profile.as_bytes(),
                creator.participant.as_bytes(),
                creator.session.as_bytes(),
                member.role,
            ],
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Member {
    participation: Uuid,
    profile: Uuid,
    participant: Uuid,
    activity: Uuid,
    agent: Uuid,
    session: Uuid,
    role: &'static str,
}

fn member(seed: u128) -> Member {
    Member {
        participation: Uuid::from_u128(0x1000 + seed),
        profile: Uuid::from_u128(0x2000 + seed),
        participant: Uuid::from_u128(0x3000 + seed),
        activity: Uuid::from_u128(0x4000 + seed),
        agent: Uuid::from_u128(0x5000 + seed),
        session: Uuid::from_u128(0x6000 + seed),
        role: if seed == 0x10 { "leader" } else { "member" },
    }
}

fn roster_rows(
    connection: &Connection,
) -> Result<Vec<Vec<rusqlite::types::Value>>, rusqlite::Error> {
    let mut statement = connection.prepare(
        "SELECT request_id,ordinal,workspace_id,team_id,participation_id,activity_id,agent_id,
                session_id,participant_id,profile_id,role,membership_revision,joined_at_ms
         FROM ops_team_create_roster ORDER BY request_id,ordinal",
    )?;
    statement
        .query_map([], |row| {
            (0..13)
                .map(|index| row.get(index))
                .collect::<Result<Vec<rusqlite::types::Value>, _>>()
        })?
        .collect()
}

fn assert_no_foreign_key_violations(connection: &Connection) -> TestResult {
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    let mut rows = statement.query([])?;
    assert!(
        rows.next()?.is_none(),
        "Team create source migration left a foreign-key violation"
    );
    Ok(())
}
