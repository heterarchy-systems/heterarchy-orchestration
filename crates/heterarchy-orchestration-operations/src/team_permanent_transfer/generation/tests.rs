use super::*;
use std::error::Error;

fn database() -> Result<Connection, Box<dyn Error>> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "PRAGMA foreign_keys=ON;
         CREATE TABLE ops_team_permanent_transfer_intents(
             workspace_id BLOB NOT NULL,
             transfer_id BLOB NOT NULL,
             destination_decision_id BLOB NOT NULL,
             destination_ordinal INTEGER NOT NULL,
             recorded_at_ms INTEGER NOT NULL,
             PRIMARY KEY(workspace_id,transfer_id)
         );
         CREATE TABLE ops_team_permanent_transfer_consents(
             workspace_id BLOB NOT NULL,
             transfer_id BLOB NOT NULL
         );",
    )?;
    create_schema(&connection)?;
    Ok(connection)
}

fn insert_intent(
    connection: &Connection,
    transfer_id: Uuid,
    decision_id: Uuid,
    ordinal: u8,
    recorded_at_ms: i64,
) -> rusqlite::Result<()> {
    let workspace_id = Uuid::from_u128(0xF001);
    connection.execute(
        "INSERT INTO ops_team_permanent_transfer_intents(
             workspace_id,transfer_id,destination_decision_id,destination_ordinal,recorded_at_ms
         ) VALUES(?1,?2,?3,?4,?5)",
        params![
            workspace_id.as_bytes(),
            transfer_id.as_bytes(),
            decision_id.as_bytes(),
            i64::from(ordinal),
            recorded_at_ms,
        ],
    )?;
    Ok(())
}

fn workspace_id() -> WorkspaceId {
    WorkspaceId::from_uuid(Uuid::from_u128(0xF001))
}

#[test]
fn schema_v65_backfill_uses_original_intent_times_for_roots() -> Result<(), Box<dyn Error>> {
    let connection = database()?;
    let decision_a = Uuid::from_u128(0xF010);
    let decision_b = Uuid::from_u128(0xF011);
    let transfer_a = Uuid::from_u128(0xF020);
    let transfer_b = Uuid::from_u128(0xF021);
    insert_intent(&connection, transfer_a, decision_a, 0, 123)?;
    insert_intent(&connection, transfer_b, decision_b, 3, 456)?;

    backfill_v65_roots(&connection)?;

    let root_a = read_generation(&connection, workspace_id(), transfer_a)?.ok_or(
        OperationsError::Corrupt("test permanent transfer root generation missing"),
    )?;
    let root_b = read_generation(&connection, workspace_id(), transfer_b)?.ok_or(
        OperationsError::Corrupt("test permanent transfer root generation missing"),
    )?;
    assert_eq!(root_a.generation(), 1);
    assert_eq!(root_a.predecessor_transfer_id(), None);
    assert_eq!(root_a.recorded_at().get(), 123);
    assert_eq!(root_a.head_transfer_id(), transfer_a);
    assert_eq!(root_a.head_generation(), 1);
    assert!(root_a.active());
    assert_eq!(root_b.recorded_at().get(), 456);
    assert!(root_b.active());
    assert!(
        connection
            .execute(
                "UPDATE ops_team_permanent_transfer_generation_ancestry
             SET recorded_at_ms=124 WHERE workspace_id=?1 AND transfer_id=?2",
                params![workspace_id().into_uuid().as_bytes(), transfer_a.as_bytes()],
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "DELETE FROM ops_team_permanent_transfer_generation_heads
             WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=0",
                params![workspace_id().into_uuid().as_bytes(), decision_a.as_bytes(),],
            )
            .is_err()
    );
    Ok(())
}

#[test]
fn replacement_compares_head_and_advances_one_generation_at_a_time() -> Result<(), Box<dyn Error>> {
    let connection = database()?;
    let workspace = workspace_id();
    let decision = Uuid::from_u128(0xF030);
    let root_id = Uuid::from_u128(0xF040);
    let replacement_id = Uuid::from_u128(0xF041);
    let regressing_id = Uuid::from_u128(0xF042);
    let next_id = Uuid::from_u128(0xF043);
    insert_intent(&connection, root_id, decision, 2, 100)?;

    let transaction = connection.unchecked_transaction()?;
    create_root(&transaction, workspace, root_id)?;
    transaction.commit()?;
    let root = read_generation(&connection, workspace, root_id)?.ok_or(
        OperationsError::Corrupt("test permanent transfer root generation missing"),
    )?;
    assert!(root.active());

    let transaction = connection.unchecked_transaction()?;
    insert_intent(&transaction, replacement_id, decision, 2, 150)?;
    assert!(matches!(
        replace_head(&transaction, workspace, replacement_id, root_id, 2),
        Err(OperationsError::Conflict(_))
    ));
    drop(transaction);

    let transaction = connection.unchecked_transaction()?;
    insert_intent(&transaction, replacement_id, decision, 2, 150)?;
    assert!(matches!(
        replace_head(
            &transaction,
            workspace,
            replacement_id,
            Uuid::from_u128(0xF099),
            1
        ),
        Err(OperationsError::Conflict(_))
    ));
    drop(transaction);

    let transaction = connection.unchecked_transaction()?;
    insert_intent(&transaction, replacement_id, decision, 2, 150)?;
    assert!(matches!(
        read_generation(&transaction, workspace, root_id),
        Err(OperationsError::Corrupt(_))
    ));
    let replacement = replace_head(&transaction, workspace, replacement_id, root_id, 1)?;
    assert_eq!(replacement.generation(), 2);
    assert_eq!(replacement.predecessor_transfer_id(), Some(root_id));
    transaction.commit()?;
    let superseded_root = read_generation(&connection, workspace, root_id)?.ok_or(
        OperationsError::Corrupt("test permanent transfer root generation missing"),
    )?;
    assert!(superseded_root.superseded());
    assert_eq!(superseded_root.head_transfer_id(), replacement_id);
    assert_eq!(superseded_root.head_generation(), 2);
    assert!(
        read_generation(&connection, workspace, replacement_id)?
            .ok_or(OperationsError::Corrupt(
                "test permanent transfer replacement generation missing",
            ))?
            .active()
    );

    let transaction = connection.unchecked_transaction()?;
    insert_intent(&transaction, regressing_id, decision, 2, 149)?;
    assert!(matches!(
        replace_head(&transaction, workspace, regressing_id, replacement_id, 2),
        Err(OperationsError::Conflict(_))
    ));
    drop(transaction);

    let transaction = connection.unchecked_transaction()?;
    insert_intent(&transaction, next_id, decision, 2, 151)?;
    let next = replace_head(&transaction, workspace, next_id, replacement_id, 2)?;
    assert_eq!(next.generation(), 3);
    transaction.commit()?;
    assert_eq!(
        read_generation(&connection, workspace, next_id)?
            .ok_or(OperationsError::Corrupt(
                "test permanent transfer next generation missing",
            ))?
            .generation(),
        3
    );
    Ok(())
}

#[test]
fn readback_rejects_broken_ancestry() -> Result<(), Box<dyn Error>> {
    let connection = database()?;
    let workspace = workspace_id();
    let decision = Uuid::from_u128(0xF050);
    let root_id = Uuid::from_u128(0xF060);
    let replacement_id = Uuid::from_u128(0xF061);
    insert_intent(&connection, root_id, decision, 1, 100)?;
    let transaction = connection.unchecked_transaction()?;
    create_root(&transaction, workspace, root_id)?;
    transaction.commit()?;
    let transaction = connection.unchecked_transaction()?;
    insert_intent(&transaction, replacement_id, decision, 1, 101)?;
    replace_head(&transaction, workspace, replacement_id, root_id, 1)?;
    transaction.commit()?;

    connection
        .execute_batch("DROP TRIGGER ops_team_permanent_transfer_generation_ancestry_no_update")?;
    connection.execute(
        "UPDATE ops_team_permanent_transfer_generation_ancestry
         SET recorded_at_ms=99 WHERE workspace_id=?1 AND transfer_id=?2",
        params![workspace.into_uuid().as_bytes(), replacement_id.as_bytes(),],
    )?;
    assert!(matches!(
        read_generation(&connection, workspace, replacement_id),
        Err(OperationsError::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn replacement_allows_only_its_exact_unclassified_candidate() -> Result<(), Box<dyn Error>> {
    let connection = database()?;
    let workspace = workspace_id();
    let decision = Uuid::from_u128(0xF070);
    let root_id = Uuid::from_u128(0xF080);
    let candidate_id = Uuid::from_u128(0xF081);
    let other_candidate_id = Uuid::from_u128(0xF082);
    insert_intent(&connection, root_id, decision, 0, 100)?;
    let transaction = connection.unchecked_transaction()?;
    create_root(&transaction, workspace, root_id)?;
    transaction.commit()?;

    let transaction = connection.unchecked_transaction()?;
    insert_intent(&transaction, candidate_id, decision, 0, 101)?;
    insert_intent(&transaction, other_candidate_id, decision, 0, 102)?;
    assert!(matches!(
        read_generation(&transaction, workspace, root_id),
        Err(OperationsError::Corrupt(_))
    ));
    assert!(matches!(
        replace_head(&transaction, workspace, candidate_id, root_id, 1),
        Err(OperationsError::Corrupt(_))
    ));
    drop(transaction);
    Ok(())
}
