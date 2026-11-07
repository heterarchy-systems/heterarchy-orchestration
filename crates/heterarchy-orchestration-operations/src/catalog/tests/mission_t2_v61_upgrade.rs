use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use super::super::*;
use crate::{
    MissionCreateCommand, MissionEvaluationAuthority, MissionEvaluationPlanId,
    MissionEvaluationPlanPublishCommand, MissionEvaluationPlanStartCommand,
    MissionEvaluationPlanStartRecord, MissionEvaluationPlanVersionId, MissionId, OperationalActor,
};
use forge_domain::WorkspaceId;
use rusqlite::{Connection, params};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error>>;
type TableRows = BTreeMap<String, Vec<Vec<rusqlite::types::Value>>>;

struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "mission-t2-v61-upgrade-{}-{}.sqlite",
                std::process::id(),
                Uuid::now_v7()
            )),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = self.path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let _ = fs::remove_file(PathBuf::from(sidecar));
        }
    }
}

fn rows_for_schema(
    connection: &Connection,
    objects: Vec<(&'static str, &'static str, String)>,
) -> Result<TableRows, rusqlite::Error> {
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

fn seed_nonempty_mission(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
) -> Result<MissionEvaluationPlanStartRecord, Box<dyn Error>> {
    let team_id = Uuid::now_v7();
    catalog.connection.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'T2 upgrade','preserve Mission','ACTIVE',1,'operator','Local operator',1,1,1)",
        params![team_id.as_bytes(), workspace_id.into_uuid().as_bytes()],
    )?;
    let mission_id = MissionId::from_uuid(Uuid::now_v7())?;
    let created = catalog.create_mission(&MissionCreateCommand::new(
        Uuid::now_v7(),
        mission_id,
        workspace_id,
        team_id,
        "preserve an in-progress Mission".to_owned(),
        10,
        MissionEvaluationAuthority::new(OperationalActor::LocalOperator, None)?,
    )?)?;
    assert_eq!(created.mission().mission_id(), mission_id);
    let plan_id = MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?;
    let version = catalog
        .publish_mission_evaluation_plan(&MissionEvaluationPlanPublishCommand::new(
            Uuid::now_v7(),
            plan_id,
            MissionEvaluationPlanVersionId::from_uuid(Uuid::now_v7())?,
            workspace_id,
            mission_id,
            0,
            20,
            30,
            40,
            Vec::new(),
            MissionEvaluationAuthority::new(OperationalActor::LocalOperator, None)?,
        )?)?
        .version()
        .clone();
    let started = catalog
        .start_mission_evaluation_plan(
            &MissionEvaluationPlanStartCommand::new(
                Uuid::now_v7(),
                workspace_id,
                mission_id,
                plan_id,
                version.revision(),
                MissionEvaluationAuthority::new(OperationalActor::LocalOperator, None)?,
            )?,
            || Ok(25),
        )?
        .start()
        .clone();
    Ok(started)
}

fn drop_t2_checkpoint_schema(connection: &Connection) -> Result<(), Box<dyn Error>> {
    let objects = crate::mission_evaluation::checkpoint_schema_objects();
    for (kind, name, _) in objects.into_iter().rev() {
        let statement = match kind {
            "trigger" => format!("DROP TRIGGER IF EXISTS {name}"),
            "table" => format!("DROP TABLE IF EXISTS {name}"),
            _ => return Err("unexpected T2 schema object kind".into()),
        };
        connection.execute_batch(&statement)?;
    }
    Ok(())
}

#[test]
fn legacy_fixture_drop_removes_empty_t2_schema_before_mission_requests() -> TestResult {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE ops_mission_evaluation_requests(request_id BLOB PRIMARY KEY);
         CREATE TABLE ops_mission_evaluation_t2_deferred_checkpoints(checkpoint_id BLOB PRIMARY KEY);",
    )?;
    for (kind, name, _) in crate::mission_evaluation::checkpoint_schema_objects() {
        if kind == "trigger" {
            let target = if name == "ops_mission_evaluation_requests_checkpoint_id_guard" {
                "ops_mission_evaluation_requests"
            } else {
                "ops_mission_evaluation_t2_deferred_checkpoints"
            };
            connection.execute_batch(&format!(
                "CREATE TRIGGER {name} BEFORE INSERT ON {target} BEGIN SELECT 1; END;"
            ))?;
        }
    }

    let old_mission_objects = [("table", "ops_mission_evaluation_requests", "")];
    super::super::drop_empty_mission_schema_for_legacy_fixture(&connection, &old_mission_objects)?;

    for (_, name, _) in crate::mission_evaluation::checkpoint_schema_objects() {
        let remains: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
            params![name],
            |row| row.get(0),
        )?;
        assert!(!remains, "legacy fixture retained T2 schema object {name}");
    }
    let requests_remain: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_mission_evaluation_requests')",
        [],
        |row| row.get(0),
    )?;
    assert!(!requests_remain);
    Ok(())
}

#[test]
fn legacy_fixture_drop_preserves_nonempty_t2_checkpoint_sources() -> TestResult {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE ops_mission_evaluation_requests(request_id BLOB PRIMARY KEY);
         CREATE TABLE ops_mission_evaluation_t2_deferred_checkpoints(checkpoint_id BLOB PRIMARY KEY);
         INSERT INTO ops_mission_evaluation_t2_deferred_checkpoints(checkpoint_id) VALUES(1);",
    )?;

    let old_mission_objects = [("table", "ops_mission_evaluation_requests", "")];
    let result = super::super::drop_empty_mission_schema_for_legacy_fixture(
        &connection,
        &old_mission_objects,
    );
    assert!(matches!(result, Err(crate::OperationsError::Conflict(_))));
    let source_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_mission_evaluation_t2_deferred_checkpoints",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(source_count, 1, "fixture setup discarded a durable source");
    let requests_remain: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_mission_evaluation_requests')",
        [],
        |row| row.get(0),
    )?;
    assert!(
        requests_remain,
        "fixture setup partially removed Mission schema"
    );
    Ok(())
}

fn assert_no_foreign_key_violations(connection: &Connection) -> TestResult {
    assert!(
        connection
            .query_row("PRAGMA foreign_key_check", [], |row| row
                .get::<_, String>(0))
            .optional()?
            .is_none()
    );
    Ok(())
}

#[test]
fn populated_v61_catalog_migrates_through_v62_without_t2_backfill() -> TestResult {
    let database = TestDatabase::new();
    let destination = Uuid::now_v7();
    let workspace_id = WorkspaceId::from_uuid(Uuid::now_v7());
    let mut catalog = OperationsCatalog::open(database.path(), destination)?;
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            Uuid::now_v7().as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            destination.as_bytes(),
            [0x61_u8; 32].as_slice(),
        ],
    )?;
    let started = seed_nonempty_mission(&mut catalog, workspace_id)?;
    let plan_id = started.plan_id();
    let v61_objects = schema_v61_objects();
    let before = rows_for_schema(&catalog.connection, v61_objects.clone())?;
    assert!(
        !before
            .get("ops_mission_evaluation_missions")
            .is_none_or(Vec::is_empty)
    );
    assert!(
        !before
            .get("ops_mission_evaluation_plan_starts")
            .is_none_or(Vec::is_empty)
    );
    drop_v63_existing_home_transfer_for_v62_fixture(&catalog.connection)?;
    drop_t2_checkpoint_schema(&catalog.connection)?;
    catalog.connection.pragma_update(None, "user_version", 61)?;
    validate_schema_v61(&catalog.connection)?;
    assert_eq!(catalog_version(&catalog.connection, destination)?, 61);
    drop(catalog);

    let upgraded = OperationsCatalog::open(database.path(), destination)?;
    assert_eq!(catalog_version(&upgraded.connection, destination)?, VERSION);
    assert_eq!(VERSION, 69);
    validate_schema(&upgraded.connection)?;
    assert_eq!(
        rows_for_schema(&upgraded.connection, v61_objects.clone())?,
        before
    );
    let checkpoint_rows: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_mission_evaluation_t2_deferred_checkpoints",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(checkpoint_rows, 0, "migration must not invent T2 outcomes");
    assert_eq!(
        upgraded.mission_evaluation_plan_start(workspace_id, plan_id)?,
        Some(started.clone())
    );
    assert_no_foreign_key_violations(&upgraded.connection)?;
    drop(upgraded);

    let read_only = OperationsCatalog::open_existing_read_only(database.path(), destination)?;
    validate_schema(&read_only.connection)?;
    assert_eq!(rows_for_schema(&read_only.connection, v61_objects)?, before);
    let read_only_count: i64 = read_only.connection.query_row(
        "SELECT COUNT(*) FROM ops_mission_evaluation_t2_deferred_checkpoints",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(read_only_count, 0);
    assert_no_foreign_key_violations(&read_only.connection)?;
    Ok(())
}
