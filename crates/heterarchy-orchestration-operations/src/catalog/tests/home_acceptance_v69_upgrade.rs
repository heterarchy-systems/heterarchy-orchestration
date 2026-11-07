use super::super::{
    OperationsCatalog, VERSION, catalog_version, validate_schema, validate_schema_v68,
};
use rusqlite::params;
use uuid::Uuid;

#[test]
fn populated_v68_requires_migration_then_upgrades_without_inventing_acceptance()
-> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("home-acceptance-v69-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let path = root.join("operations.sqlite3");
    let destination = Uuid::now_v7();
    let workspace = Uuid::now_v7();
    let migration = Uuid::now_v7();
    let catalog = OperationsCatalog::open(&path, destination)?;
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![migration.as_bytes(),workspace.as_bytes(),destination.as_bytes(),[0x68_u8;32].as_slice()],
    )?;
    let objects = crate::home_acceptance::schema::schema_objects();
    for (kind, name, _) in objects.iter().rev() {
        // This is a freshly-created, empty additive schema in a test-only DB.
        // Refuse to erase an acceptance if the fixture ever changes.
        if *kind == "table" {
            let rows: i64 = catalog.connection.query_row(
                &format!("SELECT COUNT(*) FROM {name}"),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(rows, 0);
        }
        catalog
            .connection
            .execute_batch(&format!("DROP {kind} {name}"))?;
    }
    catalog.connection.pragma_update(None, "user_version", 68)?;
    validate_schema_v68(&catalog.connection)?;
    drop(catalog);

    assert!(matches!(
        OperationsCatalog::open_existing_read_only(&path, destination),
        Err(crate::OperationsError::Conflict(
            "catalog migration required"
        ))
    ));
    let historical =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    assert_eq!(catalog_version(&historical, destination)?, 68);
    drop(historical);
    let upgraded = OperationsCatalog::open(&path, destination)?;
    assert_eq!(VERSION, 69);
    assert_eq!(catalog_version(&upgraded.connection, destination)?, VERSION);
    validate_schema(&upgraded.connection)?;
    let retained: Vec<u8> = upgraded.connection.query_row(
        "SELECT import_digest FROM ops_imports WHERE migration_id=?1 AND workspace_id=?2 AND destination_id=?3 AND source_revision=1 AND phase=2",
        params![migration.as_bytes(),workspace.as_bytes(),destination.as_bytes()], |row| row.get(0),
    )?;
    assert_eq!(retained, [0x68_u8; 32]);
    for (kind, name, _) in &objects {
        if *kind == "table" {
            let rows: i64 = upgraded.connection.query_row(
                &format!("SELECT COUNT(*) FROM {name}"),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(rows, 0, "migration must not invent rows in {name}");
        }
    }
    let violations: i64 = upgraded.connection.query_row(
        "SELECT COUNT(*) FROM pragma_foreign_key_check",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(violations, 0);
    drop(upgraded);
    let reopened = OperationsCatalog::open_existing_read_only(&path, destination)?;
    validate_schema(&reopened.connection)?;
    drop(reopened);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
