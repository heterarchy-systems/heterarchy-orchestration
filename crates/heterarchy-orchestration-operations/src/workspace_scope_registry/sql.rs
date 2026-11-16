//! Domain SQL definitions for the Workspace scope registry.

use crate::static_sql::StaticSql;

pub(super) const REGISTRY_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_workspace_scope_registry (workspace_id BLOB PRIMARY KEY NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), state TEXT NOT NULL CHECK(state IN('ACTIVE','UNREGISTERED')), location_id BLOB NOT NULL CHECK(length(location_id)=16 AND location_id!=zeroblob(16)), location_revision INTEGER NOT NULL CHECK(location_revision>=1), workspace_revision INTEGER NOT NULL CHECK(workspace_revision>=1), locator TEXT NOT NULL CHECK(length(locator) BETWEEN 1 AND 4096 AND length(CAST(locator AS BLOB)) BETWEEN 1 AND 4096), revision INTEGER NOT NULL CHECK(revision>=1), generation INTEGER NOT NULL CHECK(generation>=1), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0))",
)
.as_str();

pub(super) const MUTATIONS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_workspace_scope_mutations (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), kind TEXT NOT NULL CHECK(kind IN('REGISTER','UNREGISTER')), expected_revision INTEGER NOT NULL CHECK(expected_revision>=0), resulting_revision INTEGER NOT NULL CHECK(resulting_revision>=1), resulting_generation INTEGER NOT NULL CHECK(resulting_generation>=1), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32 AND payload_digest!=zeroblob(32)), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), FOREIGN KEY(workspace_id) REFERENCES ops_workspace_scope_registry(workspace_id), CHECK(resulting_revision=expected_revision+1), CHECK((kind='REGISTER' AND expected_revision>=0) OR (kind='UNREGISTER' AND expected_revision>=1)))",
)
.as_str();

pub(super) const REGISTRY_STATE_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_workspace_scope_registry_state_idx ON ops_workspace_scope_registry(state,workspace_id)",
)
.as_str();

pub(super) const MUTATIONS_WORKSPACE_REVISION_IDX_SQL: &str = StaticSql::ddl(
    "CREATE UNIQUE INDEX ops_workspace_scope_mutations_workspace_revision_idx ON ops_workspace_scope_mutations(workspace_id,resulting_revision)",
)
.as_str();

pub(super) const REGISTRY_NO_DELETE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_workspace_scope_registry_no_delete BEFORE DELETE ON ops_workspace_scope_registry BEGIN SELECT RAISE(ABORT,'workspace scope registry rows cannot be deleted'); END",
)
.as_str();

pub(super) const MUTATIONS_NO_UPDATE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_workspace_scope_mutations_no_update BEFORE UPDATE ON ops_workspace_scope_mutations BEGIN SELECT RAISE(ABORT,'workspace scope mutations are immutable'); END",
)
.as_str();

pub(super) const MUTATIONS_NO_DELETE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_workspace_scope_mutations_no_delete BEFORE DELETE ON ops_workspace_scope_mutations BEGIN SELECT RAISE(ABORT,'workspace scope mutations are immutable'); END",
)
.as_str();

pub(super) const INSERT_WORKSPACE_SCOPE_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_workspace_scope_registry(workspace_id,state,location_id,location_revision,workspace_revision,locator,revision,generation,updated_at_ms) VALUES(?1,'ACTIVE',?2,?3,?4,?5,?6,?7,?8)",
)
.as_str();

pub(super) const UPDATE_WORKSPACE_SCOPE_ACTIVE_SQL: &str = StaticSql::dml(
    "UPDATE ops_workspace_scope_registry SET state='ACTIVE',location_id=?2,location_revision=?3,workspace_revision=?4,locator=?5,revision=?6,generation=?7,updated_at_ms=?8 WHERE workspace_id=?1 AND revision=?9",
)
.as_str();

pub(super) const UPDATE_WORKSPACE_SCOPE_UNREGISTERED_SQL: &str = StaticSql::dml(
    "UPDATE ops_workspace_scope_registry SET state='UNREGISTERED',revision=?2,generation=?3,updated_at_ms=?4 WHERE workspace_id=?1 AND state='ACTIVE' AND revision=?5",
)
.as_str();

pub(super) const INSERT_WORKSPACE_SCOPE_MUTATION_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_workspace_scope_mutations(request_id,workspace_id,kind,expected_revision,resulting_revision,resulting_generation,payload_digest,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
)
.as_str();

pub(super) const COUNT_ACTIVE_WORKSPACE_SCOPES_SQL: &str =
    StaticSql::dml("SELECT COUNT(*) FROM ops_workspace_scope_registry WHERE state='ACTIVE'")
        .as_str();

pub(super) const SELECT_WORKSPACE_SCOPE_MUTATION_SQL: &str = StaticSql::dml(
    "SELECT substr(workspace_id,1,17),kind,resulting_revision,resulting_generation,substr(payload_digest,1,33),recorded_at_ms FROM ops_workspace_scope_mutations WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_WORKSPACE_SCOPE_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(workspace_id,1,17),state,substr(location_id,1,17),location_revision,workspace_revision,substr(locator,1,4097),revision,generation,updated_at_ms FROM ops_workspace_scope_registry WHERE workspace_id=?1",
)
.as_str();

pub(super) const SELECT_ACTIVE_WORKSPACE_SCOPES_SQL: &str = StaticSql::dml(
    "SELECT substr(workspace_id,1,17),state,substr(location_id,1,17),location_revision,workspace_revision,substr(locator,1,4097),revision,generation,updated_at_ms FROM ops_workspace_scope_registry WHERE state='ACTIVE' ORDER BY workspace_id ASC LIMIT ?1",
)
.as_str();
