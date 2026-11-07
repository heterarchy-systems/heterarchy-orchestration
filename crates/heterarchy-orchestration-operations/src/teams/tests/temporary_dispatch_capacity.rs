use forge_domain::WorkspaceId;
use rusqlite::{Connection, TransactionBehavior, params};
use uuid::Uuid;

use crate::OperationsError;

const WORKSPACE: Uuid = Uuid::from_u128(0xCE100);
const TEAM: Uuid = Uuid::from_u128(0xCE200);

fn capacity_fixture() -> Result<Connection, rusqlite::Error> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE ops_team_members(workspace_id BLOB,team_id BLOB,participation_id BLOB,revision INTEGER,state TEXT);
         CREATE TABLE ops_workspace_participation_mappings(request_id BLOB,workspace_id BLOB,participation_id BLOB,agent_profile_id BLOB,participant_id BLOB);
         CREATE TABLE ops_workspace_participations(workspace_id BLOB,participation_id BLOB,agent_profile_id BLOB);
         CREATE TABLE ops_temporary_dispatch_reservations(workspace_id BLOB,receiving_team_id BLOB,target_participant_id BLOB,target_participation_id BLOB,target_profile_id BLOB,state TEXT);
         CREATE TABLE ops_team_capacity_events(workspace_id BLOB,team_id BLOB,capacity_revision INTEGER,max_members INTEGER,recorded_at_ms INTEGER,actor_activity_id BLOB,actor_agent_id BLOB,actor_session_id BLOB,actor_participation_id BLOB,actor_profile_id BLOB,actor_participant_id BLOB,actor_display_name TEXT,leadership_revision INTEGER);
         CREATE TABLE ops_team_expansion_slots(decision_id BLOB,ordinal INTEGER,workspace_id BLOB,team_id BLOB);
         CREATE TABLE ops_team_expansion_slot_uses(decision_id BLOB,ordinal INTEGER);",
    )?;
    connection.execute(
        "INSERT INTO ops_team_capacity_events(workspace_id,team_id,capacity_revision,max_members,recorded_at_ms,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,leadership_revision) VALUES(?1,?2,1,8,1,?3,?4,?5,?6,?7,?8,'Synthetic Orchestrator',1)",
        params![
            WORKSPACE.as_bytes(),
            TEAM.as_bytes(),
            Uuid::from_u128(0xCE901).as_bytes(),
            Uuid::from_u128(0xCE902).as_bytes(),
            Uuid::from_u128(0xCE903).as_bytes(),
            Uuid::from_u128(0xCE904).as_bytes(),
            Uuid::from_u128(0xCE905).as_bytes(),
            Uuid::from_u128(0xCE906).as_bytes(),
        ],
    )?;
    Ok(connection)
}

fn add_joined_member(
    connection: &Connection,
    ordinal: u128,
) -> Result<(Uuid, Uuid, Uuid), rusqlite::Error> {
    let participation = Uuid::from_u128(0xCE300 + ordinal);
    let profile = Uuid::from_u128(0xCE400 + ordinal);
    let participant = Uuid::from_u128(0xCE500 + ordinal);
    connection.execute(
        "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,revision,state) VALUES(?1,?2,?3,1,'JOINED')",
        params![WORKSPACE.as_bytes(), TEAM.as_bytes(), participation.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,participation_id,agent_profile_id) VALUES(?1,?2,?3)",
        params![WORKSPACE.as_bytes(), participation.as_bytes(), profile.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,participant_id) VALUES(?1,?2,?3,?4,?5)",
        params![
            Uuid::from_u128(0xCE600 + ordinal).as_bytes(),
            WORKSPACE.as_bytes(),
            participation.as_bytes(),
            profile.as_bytes(),
            participant.as_bytes(),
        ],
    )?;
    Ok((participation, profile, participant))
}

fn check_capacity(
    connection: &mut Connection,
) -> Result<Result<(), OperationsError>, rusqlite::Error> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = crate::teams::ensure_team_member_capacity(
        &transaction,
        WorkspaceId::from_uuid(WORKSPACE),
        TEAM,
    );
    transaction.rollback()?;
    Ok(result)
}

#[test]
fn missing_joined_member_mapping_does_not_return_a_team_seat()
-> Result<(), Box<dyn std::error::Error>> {
    // Synthetic legacy source rows intentionally omit one mapping. This tests
    // the Operations capacity fallback only; it is not identity/source proof.
    let mut connection = capacity_fixture()?;
    for ordinal in 1..=8 {
        let _member = add_joined_member(&connection, ordinal)?;
    }
    connection.execute(
        "DELETE FROM ops_workspace_participation_mappings WHERE participation_id=?1",
        params![Uuid::from_u128(0xCE300 + 8).as_bytes()],
    )?;

    assert!(matches!(
        check_capacity(&mut connection)?,
        Err(OperationsError::Bounds("team membership capacity"))
    ));
    Ok(())
}

#[test]
fn multiple_session_aliases_for_one_participation_count_one_seat()
-> Result<(), Box<dyn std::error::Error>> {
    // The synthetic legacy mapping table omits current UNIQUE constraints so
    // the fixture can represent an ambiguous participation alias.
    let mut connection = capacity_fixture()?;
    let mut ambiguous = None;
    for ordinal in 1..=7 {
        let member = add_joined_member(&connection, ordinal)?;
        if ordinal == 7 {
            ambiguous = Some(member);
        }
    }
    let (participation, _profile, _participant) =
        ambiguous.ok_or("ambiguous Team member was not inserted")?;
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,participant_id) VALUES(?1,?2,?3,?4,?5)",
        params![
            Uuid::from_u128(0xCE700).as_bytes(),
            WORKSPACE.as_bytes(),
            participation.as_bytes(),
            Uuid::from_u128(0xCE800).as_bytes(),
            Uuid::from_u128(0xCE900).as_bytes(),
        ],
    )?;

    assert!(check_capacity(&mut connection)?.is_ok());
    Ok(())
}

#[test]
fn shared_legacy_participant_id_does_not_collapse_distinct_participations()
-> Result<(), Box<dyn std::error::Error>> {
    // Synthetic legacy rows reuse one participant ID across two participation
    // and Profile identities. The capacity query must fail closed on the alias.
    let mut connection = capacity_fixture()?;
    let first = add_joined_member(&connection, 1)?;
    let second = add_joined_member(&connection, 2)?;
    connection.execute(
        "UPDATE ops_workspace_participation_mappings SET participant_id=?1 WHERE participation_id=?2",
        params![Uuid::from_u128(0xCE777).as_bytes(), first.0.as_bytes()],
    )?;
    connection.execute(
        "UPDATE ops_workspace_participation_mappings SET participant_id=?1 WHERE participation_id=?2",
        params![Uuid::from_u128(0xCE777).as_bytes(), second.0.as_bytes()],
    )?;
    for ordinal in 3..=8 {
        let _member = add_joined_member(&connection, ordinal)?;
    }

    assert!(matches!(
        check_capacity(&mut connection)?,
        Err(OperationsError::Bounds("team membership capacity"))
    ));
    Ok(())
}

#[test]
fn repeated_inbound_reservations_for_one_subject_count_one_team_seat()
-> Result<(), Box<dyn std::error::Error>> {
    // Synthetic reservation rows exercise capacity deduplication only. The
    // lifecycle suite tests authenticated owner transactions that create them.
    let mut connection = capacity_fixture()?;
    let mut first_member = None;
    for ordinal in 1..=7 {
        let member = add_joined_member(&connection, ordinal)?;
        if ordinal == 1 {
            first_member = Some(member);
        }
    }
    let (participation, profile, target) = first_member.ok_or("first Team subject missing")?;
    for _ in 0..2 {
        connection.execute(
            "INSERT INTO ops_temporary_dispatch_reservations(workspace_id,receiving_team_id,target_participant_id,target_participation_id,target_profile_id,state) VALUES(?1,?2,?3,?4,?5,'RESERVED')",
            params![
                WORKSPACE.as_bytes(), TEAM.as_bytes(), target.as_bytes(),
                participation.as_bytes(), profile.as_bytes(),
            ],
        )?;
    }

    assert!(check_capacity(&mut connection)?.is_ok());
    Ok(())
}
