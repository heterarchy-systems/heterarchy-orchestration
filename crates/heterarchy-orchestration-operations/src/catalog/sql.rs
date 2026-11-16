//! Domain SQL definitions for the Operations catalog.

use crate::static_sql::StaticSql;

pub(super) const IDENTITY_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_catalog_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), destination_id BLOB NOT NULL UNIQUE CHECK(length(destination_id)=16))",
)
.as_str();

pub(super) const IMPORTS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_imports (migration_id BLOB PRIMARY KEY NOT NULL CHECK(length(migration_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), destination_id BLOB NOT NULL CHECK(length(destination_id)=16), source_revision INTEGER NOT NULL CHECK(source_revision>=1), phase INTEGER NOT NULL CHECK(phase IN(0,1,2,3)), next_cursor BLOB CHECK(next_cursor IS NULL OR length(next_cursor) BETWEEN 1 AND 512), page_count INTEGER NOT NULL CHECK(page_count>=0), record_count INTEGER NOT NULL CHECK(record_count>=0), import_digest BLOB NOT NULL CHECK(length(import_digest)=32), verified_pages INTEGER NOT NULL CHECK(verified_pages BETWEEN 0 AND page_count), verify_digest BLOB NOT NULL CHECK(length(verify_digest)=32), FOREIGN KEY(destination_id) REFERENCES ops_catalog_identity(destination_id), CHECK(phase NOT IN(1,2) OR page_count>=5), CHECK((phase=0 AND next_cursor IS NOT NULL) OR (phase IN(1,2) AND next_cursor IS NULL) OR phase=3), CHECK(phase!=2 OR (verified_pages=page_count AND import_digest=verify_digest)))",
)
.as_str();

pub(super) const IMPORT_INDEX_SQL: &str = StaticSql::ddl(
    "CREATE UNIQUE INDEX ops_imports_workspace_owner_idx ON ops_imports(workspace_id) WHERE phase!=3",
)
.as_str();

pub(super) const PAGES_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_import_pages (migration_id BLOB NOT NULL REFERENCES ops_imports(migration_id) CHECK(length(migration_id)=16), ordinal INTEGER NOT NULL CHECK(ordinal>=0), cursor BLOB NOT NULL CHECK(length(cursor) BETWEEN 1 AND 512), next_cursor BLOB CHECK(next_cursor IS NULL OR length(next_cursor) BETWEEN 1 AND 512), table_kind INTEGER NOT NULL CHECK(table_kind BETWEEN 0 AND 4), record_count INTEGER NOT NULL CHECK(record_count BETWEEN 0 AND 64), digest BLOB NOT NULL CHECK(length(digest)=32), PRIMARY KEY(migration_id,ordinal), UNIQUE(migration_id,cursor))",
)
.as_str();

pub(super) const SELECT_VERIFIED_IMPORT_MIGRATION_SQL: &str = StaticSql::dml(
    "SELECT substr(migration_id,1,17) FROM ops_imports WHERE workspace_id=?1 AND destination_id=?2 AND phase=2",
)
.as_str();

pub(super) const EXISTS_ACTIVE_WORKSPACE_IMPORT_SQL: &str =
    StaticSql::dml("SELECT EXISTS(SELECT 1 FROM ops_imports WHERE workspace_id=?1 AND phase!=3)")
        .as_str();

pub(super) const INSERT_IMPORT_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,?4,0,?5,0,0,?6,0,?6)",
)
.as_str();

pub(super) const COUNT_NON_INTERNAL_SCHEMA_OBJECTS_SQL: &str =
    StaticSql::dml("SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").as_str();

pub(super) const SELECT_CATALOG_DESTINATION_SQL: &str = StaticSql::dml(
    "SELECT substr(destination_id,1,17) FROM ops_catalog_identity WHERE singleton=1",
)
.as_str();

pub(super) const INSERT_CATALOG_IDENTITY_SQL: &str =
    StaticSql::dml("INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)")
        .as_str();

pub(super) const SELECT_IMPORT_PROGRESS_SQL: &str = StaticSql::dml(
    "SELECT substr(workspace_id,1,17),substr(destination_id,1,17),source_revision,phase,substr(next_cursor,1,513),page_count,record_count,substr(import_digest,1,33),verified_pages,substr(verify_digest,1,33) FROM ops_imports WHERE migration_id=?1",
)
.as_str();

pub(super) const SELECT_SCHEMA_OBJECT_SQL: &str = StaticSql::dml(
    "SELECT CASE WHEN length(CAST(sql AS BLOB))<=16384 THEN sql END FROM sqlite_schema WHERE type=?1 AND name=?2",
)
.as_str();
