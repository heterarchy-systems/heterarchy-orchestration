use super::*;
use crate::TeamPermanentTransferPreparationCommand;
use rusqlite::TransactionBehavior;

struct PreparedTransferHistory {
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    preparation_id: Uuid,
    actor: OperationalActor,
    transfer: crate::TeamPermanentTransferSnapshot,
    preparation: crate::TeamPermanentTransferPreparationReceipt,
    generation: TeamPermanentTransferGenerationSnapshot,
    transfer_rows: TransferRows,
    preparation_rows: TableRows,
}

#[test]
fn schema_67_migration_preserves_preparation_and_transfer_history() -> TestResult {
    let database = TestDatabase::new();
    let destination_id = id(DESTINATION_ID);
    let workspace_id = WorkspaceId::from_uuid(id(WORKSPACE_ID));
    let mut catalog = OperationsCatalog::open(&database.path, destination_id)?;
    let history = prepare_transfer_history(&mut catalog, workspace_id)?;
    downgrade_empty_source_schema_to_v67(&mut catalog, destination_id)?;
    drop(catalog);

    let mut upgraded = OperationsCatalog::open(&database.path, destination_id)?;
    assert_migrated_transfer_history(&mut upgraded, destination_id, &history)?;
    drop(upgraded);

    let mut read_only = OperationsCatalog::open_existing_read_only(&database.path, destination_id)?;
    assert_migrated_transfer_history(&mut read_only, destination_id, &history)?;
    Ok(())
}

fn prepare_transfer_history(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
) -> FixtureResult<PreparedTransferHistory> {
    let owners = seed_synthetic_v57_owner_facts(catalog, workspace_id)?;
    let transfer = create_transfer_with_two_consents(catalog, workspace_id, &owners)?;
    let transfer_id = transfer.intent().transfer_id();
    let intent_generation = read_generation_snapshot(
        catalog,
        workspace_id,
        transfer_id,
        owners.orchestrator.actor(),
    )?;
    let source_consent_id = transfer
        .source_home_leader_consent()
        .ok_or_else(|| std::io::Error::other("missing source leader consent"))?
        .receipt_id();
    let target_acceptance_id = transfer
        .target_agent_acceptance()
        .ok_or_else(|| std::io::Error::other("missing target acceptance"))?
        .receipt_id();
    let preparation_id = id(0xCC36);
    let preparation_command = TeamPermanentTransferPreparationCommand::new(
        preparation_id,
        workspace_id,
        transfer_id,
        intent_generation.generation().number(),
        transfer
            .intent()
            .destination_workspace_leadership_revision(),
        source_consent_id,
        target_acceptance_id,
        owners.orchestrator.actor(),
    )?;
    let preparation =
        catalog.prepare_team_permanent_transfer(&preparation_command, || timestamp(1_000_800))?;
    assert_eq!(preparation.sessions().count(), 1);
    assert_eq!(preparation.bindings().len(), 1);
    let preparation_readback = catalog
        .read_team_permanent_transfer_preparation(
            workspace_id,
            preparation_id,
            owners.orchestrator.actor(),
        )?
        .ok_or_else(|| std::io::Error::other("missing preparation readback"))?;
    assert_eq!(preparation_readback, preparation);
    let generation = read_generation_snapshot(
        catalog,
        workspace_id,
        transfer_id,
        owners.orchestrator.actor(),
    )?;
    assert_eq!(
        generation.generation().state(),
        TeamPermanentTransferGenerationState::Preparing
    );

    let transfer_rows = transfer_rows(&catalog.connection, workspace_id, transfer_id)?;
    let preparation_rows = preparation_table_rows(&catalog.connection)?;
    assert_eq!(transfer_rows.intents.len(), 1);
    assert_eq!(transfer_rows.consents.len(), 2);
    assert_eq!(
        preparation_rows["ops_team_permanent_transfer_preparations"].len(),
        1
    );
    assert_eq!(
        preparation_rows["ops_team_permanent_transfer_preparation_barriers"].len(),
        1
    );
    assert_eq!(
        preparation_rows["ops_team_permanent_transfer_preparation_sessions"].len(),
        1
    );
    assert_source_schema_empty(&catalog.connection)?;
    Ok(PreparedTransferHistory {
        workspace_id,
        transfer_id,
        preparation_id,
        actor: owners.orchestrator.actor(),
        transfer,
        preparation,
        generation,
        transfer_rows,
        preparation_rows,
    })
}

fn downgrade_empty_source_schema_to_v67(
    catalog: &mut OperationsCatalog,
    destination_id: Uuid,
) -> TestResult {
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    drop_empty_source_schema_for_legacy_fixture(&transaction)?;
    transaction.pragma_update(None, "user_version", 67_i64)?;
    assert_eq!(catalog_version(&transaction, destination_id)?, 67);
    validate_schema_v67(&transaction)?;
    assert_no_foreign_key_violations(&transaction)?;
    transaction.commit()?;
    Ok(())
}

fn assert_migrated_transfer_history(
    catalog: &mut OperationsCatalog,
    destination_id: Uuid,
    history: &PreparedTransferHistory,
) -> TestResult {
    assert_eq!(
        catalog_version(&catalog.connection, destination_id)?,
        VERSION
    );
    validate_schema(&catalog.connection)?;
    assert_no_foreign_key_violations(&catalog.connection)?;
    assert_source_schema_empty(&catalog.connection)?;
    assert_eq!(
        transfer_rows(
            &catalog.connection,
            history.workspace_id,
            history.transfer_id
        )?,
        history.transfer_rows
    );
    assert_eq!(
        preparation_table_rows(&catalog.connection)?,
        history.preparation_rows
    );
    assert_eq!(
        catalog.read_team_permanent_transfer_snapshot(
            history.workspace_id,
            history.transfer_id,
            history.actor,
        )?,
        Some(history.transfer.clone())
    );
    assert_eq!(
        catalog.read_team_permanent_transfer_preparation(
            history.workspace_id,
            history.preparation_id,
            history.actor,
        )?,
        Some(history.preparation.clone())
    );
    assert_eq!(
        read_generation_snapshot(
            catalog,
            history.workspace_id,
            history.transfer_id,
            history.actor,
        )?,
        history.generation
    );
    Ok(())
}

#[test]
fn version_67_and_current_schema_validators_reject_source_schema_drift() -> TestResult {
    let database = TestDatabase::new();
    let destination_id = id(DESTINATION_ID);
    let mut catalog = OperationsCatalog::open(&database.path, destination_id)?;
    assert!(validate_schema_v67(&catalog.connection).is_err());

    let (_, trigger_name, trigger_sql) =
        crate::team_permanent_transfer::source::schema::schema_objects()
            .iter()
            .find(|(kind, _, _)| *kind == "trigger")
            .ok_or_else(|| std::io::Error::other("Source schema has no trigger"))?;
    catalog
        .connection
        .execute_batch(&format!("DROP TRIGGER {trigger_name}"))?;
    assert!(validate_schema(&catalog.connection).is_err());
    catalog.connection.execute_batch(trigger_sql)?;
    validate_schema(&catalog.connection)?;

    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    drop_empty_source_schema_for_legacy_fixture(&transaction)?;
    transaction.pragma_update(None, "user_version", 67_i64)?;
    validate_schema_v67(&transaction)?;
    transaction.commit()?;

    let malformed_v67 = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (_, preparation_trigger, _) =
        crate::team_permanent_transfer::preparation::schema::schema_objects()
            .iter()
            .find(|(kind, _, _)| *kind == "trigger")
            .ok_or_else(|| std::io::Error::other("preparation schema has no trigger"))?;
    malformed_v67.execute_batch(&format!("DROP TRIGGER {preparation_trigger}"))?;
    assert!(validate_schema_v67(&malformed_v67).is_err());
    malformed_v67.rollback()?;
    Ok(())
}

pub(super) fn drop_empty_source_schema_for_legacy_fixture(
    connection: &Connection,
) -> FixtureResult<()> {
    let mut objects = crate::team_permanent_transfer::source::schema::schema_objects().to_vec();
    objects.extend(crate::home_acceptance::schema::schema_objects());
    for &(kind, name, _) in &objects {
        if kind == "table" {
            let count: i64 =
                connection.query_row(&format!("SELECT COUNT(*) FROM {name}"), [], |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0, "cannot downgrade populated fixture table {name}");
        }
    }
    for &(kind, name, _) in objects.iter().rev() {
        let statement = match kind {
            "trigger" => format!("DROP TRIGGER {name}"),
            "table" => format!("DROP TABLE {name}"),
            "index" => format!("DROP INDEX {name}"),
            _ => return Err(std::io::Error::other("unexpected Source schema object").into()),
        };
        connection.execute_batch(&statement)?;
    }
    Ok(())
}

fn assert_source_schema_empty(connection: &Connection) -> FixtureResult<()> {
    for &(kind, name, _) in crate::team_permanent_transfer::source::schema::schema_objects() {
        if kind != "table" {
            continue;
        }
        let count: i64 =
            connection.query_row(&format!("SELECT COUNT(*) FROM {name}"), [], |row| {
                row.get(0)
            })?;
        assert_eq!(count, 0, "unexpected Source history in {name}");
    }
    Ok(())
}

fn preparation_table_rows(connection: &Connection) -> FixtureResult<TableRows> {
    let mut tables = TableRows::new();
    for &(kind, name, _) in crate::team_permanent_transfer::preparation::schema::schema_objects() {
        if kind != "table" {
            continue;
        }
        let mut statement = connection.prepare(&format!("SELECT * FROM {name} ORDER BY rowid"))?;
        let column_count = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..column_count)
                    .map(|column| row.get::<_, rusqlite::types::Value>(column))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<Result<Vec<_>, _>>()?;
        tables.insert(name.to_owned(), rows);
    }
    Ok(tables)
}
