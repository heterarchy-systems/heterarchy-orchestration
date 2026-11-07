use std::{collections::BTreeMap, error::Error, fs};

use super::super::*;

type LegacyCatalogRows = BTreeMap<String, super::CatalogTableSnapshot>;

fn rows_for_schema(
    connection: &Connection,
    objects: Vec<(&'static str, &'static str, String)>,
) -> Result<LegacyCatalogRows, rusqlite::Error> {
    let mut tables = BTreeMap::new();
    for (kind, name, _) in objects {
        if kind != "table" {
            continue;
        }
        let mut statement = connection.prepare(&format!("SELECT * FROM {name}"))?;
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|column| row.get::<_, rusqlite::types::Value>(column))
                    .collect::<Result<Vec<_>, _>>()
            })?
            .collect::<Result<Vec<_>, _>>()?;
        tables.insert(name.to_owned(), rows);
    }
    Ok(tables)
}

fn legacy_rows(connection: &Connection) -> Result<LegacyCatalogRows, rusqlite::Error> {
    rows_for_schema(connection, schema_v52_objects())
}

fn seed_mission_plan(
    catalog: &mut OperationsCatalog,
    workspace: WorkspaceId,
) -> Result<crate::MissionEvaluationPlanStartRecord, Box<dyn Error>> {
    use crate::{
        MissionCreateCommand, MissionEvaluationAuthority, MissionEvaluationPlanId,
        MissionEvaluationPlanPublishCommand, MissionEvaluationPlanStartCommand,
        MissionEvaluationPlanVersionId, MissionId, OperationalActor,
    };
    let authority = MissionEvaluationAuthority::new(OperationalActor::LocalOperator, None)?;
    let team_id = Uuid::now_v7();
    // This is a historical Team row, not a new empty-roster admission. The
    // migration must preserve old planning facts without creating evidence.
    catalog.connection.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'migration fixture','preserve historical planning','ACTIVE',1,'operator','Local operator',1,1,1)",
        params![team_id.as_bytes(), workspace.into_uuid().as_bytes()],
    )?;
    let mission = catalog.create_mission(&MissionCreateCommand::new(
        Uuid::now_v7(),
        MissionId::from_uuid(Uuid::now_v7())?,
        workspace,
        team_id,
        "Preserved Mission".to_owned(),
        2,
        authority,
    )?)?;
    let plan =
        catalog.publish_mission_evaluation_plan(&MissionEvaluationPlanPublishCommand::new(
            Uuid::now_v7(),
            MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?,
            MissionEvaluationPlanVersionId::from_uuid(Uuid::now_v7())?,
            workspace,
            mission.mission().mission_id(),
            0,
            3,
            10,
            20,
            Vec::new(),
            authority,
        )?)?;
    let started = catalog.start_mission_evaluation_plan(
        &MissionEvaluationPlanStartCommand::new(
            Uuid::now_v7(),
            workspace,
            mission.mission().mission_id(),
            plan.version().plan_id(),
            plan.version().revision(),
            authority,
        )?,
        || Ok(4),
    )?;
    Ok(started.start().clone())
}

#[test]
fn mission_evidence_v53_upgrade_preserves_planning_without_fabricating_source_evidence()
-> Result<(), Box<dyn Error>> {
    let path = std::env::temp_dir().join(format!("ops-evidence-upgrade-{}.sqlite", Uuid::now_v7()));
    let destination = Uuid::now_v7();
    let workspace = WorkspaceId::from_uuid(Uuid::now_v7());
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![Uuid::now_v7().as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes(), [0x61_u8;32].as_slice()],
    )?;
    let started = seed_mission_plan(&mut catalog, workspace)?;
    drop_v54_mission_evidence_schema_for_legacy_fixture(&catalog.connection)?;
    catalog.connection.pragma_update(None, "user_version", 53)?;
    validate_schema_v53(&catalog.connection)?;
    let before = rows_for_schema(&catalog.connection, schema_v53_objects())?;
    drop(catalog);

    let reopened = OperationsCatalog::open(&path, destination)?;
    assert_eq!(catalog_version(&reopened.connection, destination)?, VERSION);
    assert_eq!(
        before,
        rows_for_schema(&reopened.connection, schema_v53_objects())?
    );
    assert_eq!(
        reopened.mission_evaluation_plan_start(workspace, started.plan_id())?,
        Some(started)
    );
    for (kind, name, _) in crate::mission_evidence::schema_objects() {
        if kind == "table" {
            let count: i64 = reopened.connection.query_row(
                &format!("SELECT COUNT(*) FROM {name}"),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 0, "migration must not manufacture {name}");
        }
    }
    assert!(
        reopened
            .connection
            .query_row("PRAGMA foreign_key_check", [], |row| row
                .get::<_, String>(0))
            .optional()?
            .is_none()
    );
    drop(reopened);
    let read_only = OperationsCatalog::open_existing_read_only(&path, destination)?;
    assert_eq!(
        before,
        rows_for_schema(&read_only.connection, schema_v53_objects())?
    );
    drop(read_only);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn mission_evaluation_v52_upgrade_preserves_rows_and_does_not_backfill()
-> Result<(), Box<dyn Error>> {
    let path = std::env::temp_dir().join(format!("ops-mission-upgrade-{}.sqlite", Uuid::now_v7()));
    let destination = Uuid::now_v7();
    let catalog = OperationsCatalog::open(&path, destination)?;
    let migration = Uuid::now_v7();
    let workspace = Uuid::now_v7();
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,3,NULL,0,0,zeroblob(32),0,zeroblob(32))",
        params![migration.as_bytes(), workspace.as_bytes(), destination.as_bytes()],
    )?;
    drop_v53_mission_schema_for_legacy_fixture(&catalog.connection)?;
    catalog.connection.pragma_update(None, "user_version", 52)?;
    validate_schema_v52(&catalog.connection)?;
    let before = legacy_rows(&catalog.connection)?;
    drop(catalog);

    let reopened = OperationsCatalog::open(&path, destination)?;
    assert_eq!(
        reopened
            .connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
        VERSION
    );
    assert_eq!(before, legacy_rows(&reopened.connection)?);
    for (kind, name, _) in crate::mission_evaluation::schema_objects() {
        if kind == "table" {
            let count: i64 = reopened.connection.query_row(
                &format!("SELECT COUNT(*) FROM {name}"),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 0, "migration must not manufacture {name}");
        }
    }
    assert!(
        reopened
            .connection
            .query_row("PRAGMA foreign_key_check", [], |row| row
                .get::<_, String>(0))
            .optional()?
            .is_none()
    );
    drop(reopened);
    let read_only = OperationsCatalog::open_existing_read_only(&path, destination)?;
    assert_eq!(before, legacy_rows(&read_only.connection)?);
    drop(read_only);
    fs::remove_file(&path)?;
    Ok(())
}
