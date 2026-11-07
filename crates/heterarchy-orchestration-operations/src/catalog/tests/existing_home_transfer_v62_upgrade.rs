use super::super::{
    OperationsCatalog, VERSION, catalog_version, drop_v63_existing_home_transfer_for_v62_fixture,
    drop_v64_permanent_transfer_for_v63_fixture, validate_schema, validate_schema_v62,
    validate_schema_v63,
};
use forge_domain::WorkspaceId;
use rusqlite::params;
use std::{
    error::Error,
    fs::OpenOptions,
    io::Error as IoError,
    path::{Path, PathBuf},
};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error>>;
struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn new() -> Result<Self, IoError> {
        let path = std::env::temp_dir().join(format!(
            "existing-home-transfer-v62-upgrade-{}.sqlite",
            Uuid::now_v7()
        ));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        drop(file);
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let path = PathBuf::from(format!("{}{suffix}", self.path.display()));
            let _ = std::fs::remove_file(path);
        }
    }
}

fn assert_catalog_file(path: &Path) -> Result<(), IoError> {
    OpenOptions::new().read(true).open(path).map(drop)
}

#[test]
fn populated_v62_catalog_migrates_without_transfer_sources() -> TestResult {
    let database = TestDatabase::new()?;
    let destination = Uuid::now_v7();
    let workspace_id = WorkspaceId::from_uuid(Uuid::now_v7());
    let catalog = OperationsCatalog::open(database.path(), destination)?;
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            Uuid::now_v7().as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            destination.as_bytes(),
            [0x62_u8; 32].as_slice(),
        ],
    )?;
    drop_v63_existing_home_transfer_for_v62_fixture(&catalog.connection)?;
    catalog.connection.pragma_update(None, "user_version", 62)?;
    validate_schema_v62(&catalog.connection)?;
    assert_eq!(catalog_version(&catalog.connection, destination)?, 62);
    drop(catalog);

    let upgraded = OperationsCatalog::open(database.path(), destination)?;
    assert_eq!(VERSION, 69);
    assert_eq!(catalog_version(&upgraded.connection, destination)?, VERSION);
    validate_schema(&upgraded.connection)?;
    let import_count: i64 =
        upgraded
            .connection
            .query_row("SELECT COUNT(*) FROM ops_imports", [], |row| row.get(0))?;
    let manifest_count: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_team_expansion_v3_manifests",
        [],
        |row| row.get(0),
    )?;
    let transfer_count: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_team_expansion_existing_home_transfers",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(import_count, 1);
    assert_eq!(manifest_count, 0);
    assert_eq!(transfer_count, 0);
    let home_count: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_home_acceptance_intents",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(home_count, 0, "migration must not invent Home acceptance");
    drop(upgraded);

    assert_catalog_file(database.path())?;
    let read_only = OperationsCatalog::open_existing_read_only(database.path(), destination)?;
    validate_schema(&read_only.connection)?;
    Ok(())
}

#[test]
fn populated_v63_catalog_migrates_without_permanent_transfer_backfill() -> TestResult {
    let database = TestDatabase::new()?;
    let destination = Uuid::now_v7();
    let workspace_id = WorkspaceId::from_uuid(Uuid::now_v7());
    let catalog = OperationsCatalog::open(database.path(), destination)?;
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            Uuid::now_v7().as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            destination.as_bytes(),
            [0x63_u8; 32].as_slice(),
        ],
    )?;
    drop_v64_permanent_transfer_for_v63_fixture(&catalog.connection)?;
    catalog.connection.pragma_update(None, "user_version", 63)?;
    validate_schema_v63(&catalog.connection)?;
    assert_eq!(catalog_version(&catalog.connection, destination)?, 63);
    drop(catalog);

    let upgraded = OperationsCatalog::open(database.path(), destination)?;
    assert_eq!(catalog_version(&upgraded.connection, destination)?, VERSION);
    validate_schema(&upgraded.connection)?;
    let import_count: i64 =
        upgraded
            .connection
            .query_row("SELECT COUNT(*) FROM ops_imports", [], |row| row.get(0))?;
    let intent_count: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_team_permanent_transfer_intents",
        [],
        |row| row.get(0),
    )?;
    let consent_count: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM ops_team_permanent_transfer_consents",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(import_count, 1);
    assert_eq!(intent_count, 0);
    assert_eq!(consent_count, 0);
    Ok(())
}

#[test]
fn v64_catalog_opens_current_without_synthesizing_transfer_history() -> TestResult {
    let database = TestDatabase::new()?;
    let destination = Uuid::now_v7();
    let catalog = OperationsCatalog::open(database.path(), destination)?;
    drop_v64_permanent_transfer_for_v63_fixture(&catalog.connection)?;
    for &(_, _, sql) in crate::team_permanent_transfer::schema::schema_objects_v64() {
        catalog.connection.execute_batch(sql)?;
    }
    catalog.connection.pragma_update(None, "user_version", 64)?;
    assert_eq!(catalog_version(&catalog.connection, destination)?, 64);
    drop(catalog);
    let upgraded = OperationsCatalog::open(database.path(), destination)?;
    assert_eq!(catalog_version(&upgraded.connection, destination)?, VERSION);
    validate_schema(&upgraded.connection)?;
    let count: i64 = upgraded.connection.query_row(
        "SELECT (SELECT COUNT(*) FROM ops_team_permanent_transfer_intents)+(SELECT COUNT(*) FROM ops_team_permanent_transfer_consents)",
        [], |row| row.get(0),
    )?;
    assert_eq!(count, 0);
    drop(upgraded);
    let reopened = OperationsCatalog::open_existing_read_only(database.path(), destination)?;
    validate_schema(&reopened.connection)?;
    Ok(())
}

#[test]
fn v65_catalog_opens_current_with_empty_generation_heads() -> TestResult {
    let database = TestDatabase::new()?;
    let destination = Uuid::now_v7();
    let catalog = OperationsCatalog::open(database.path(), destination)?;
    drop_v64_permanent_transfer_for_v63_fixture(&catalog.connection)?;
    for &(_, _, sql) in crate::team_permanent_transfer::schema::schema_objects_v65() {
        catalog.connection.execute_batch(sql)?;
    }
    catalog.connection.pragma_update(None, "user_version", 65)?;
    assert_eq!(catalog_version(&catalog.connection, destination)?, 65);
    drop(catalog);
    let upgraded = OperationsCatalog::open(database.path(), destination)?;
    assert_eq!(catalog_version(&upgraded.connection, destination)?, VERSION);
    validate_schema(&upgraded.connection)?;
    let count: i64 = upgraded.connection.query_row(
        "SELECT (SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_ancestry)+(SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_heads)",
        [], |row| row.get(0),
    )?;
    assert_eq!(count, 0);
    drop(upgraded);
    let reopened = OperationsCatalog::open_existing_read_only(database.path(), destination)?;
    validate_schema(&reopened.connection)?;
    Ok(())
}
