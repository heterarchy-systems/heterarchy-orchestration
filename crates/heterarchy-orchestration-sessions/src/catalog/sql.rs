//! Canonical compile-time validated SQL statements for the logical Agent Session catalog.

use crate::static_sql::StaticSql;

pub(super) const AGENT_SESSIONS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_sessions (
    session_id BLOB PRIMARY KEY NOT NULL CHECK(length(session_id) = 16),
    principal_kind INTEGER NOT NULL CHECK(principal_kind IN (0, 1, 2)),
    principal_id BLOB NOT NULL CHECK(length(principal_id) = 16),
    request_id BLOB NOT NULL CHECK(length(request_id) = 16),
    state INTEGER NOT NULL CHECK(state IN (1, 2)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms),
    revision INTEGER NOT NULL CHECK(revision > 0),
    UNIQUE(principal_kind, principal_id, request_id),
    CHECK((principal_kind = 0 AND principal_id = zeroblob(16)) OR principal_kind IN (1, 2))
);",
)
.as_str();

pub(super) const AGENT_SESSIONS_INDEX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX agent_sessions_state_updated_idx
ON agent_sessions(state, updated_at_ms DESC, session_id);",
)
.as_str();

pub(super) const SELECT_SESSION_ID_BY_REQUEST_SQL: &str = StaticSql::dml(
    "SELECT session_id
FROM agent_sessions
WHERE principal_kind = ?1 AND principal_id = ?2 AND request_id = ?3",
)
.as_str();

pub(super) const SELECT_SESSION_ID_BY_REQUEST_ANY_PRINCIPAL_SQL: &str =
    StaticSql::dml("SELECT session_id FROM agent_sessions WHERE request_id = ?1 LIMIT 1").as_str();

pub(super) const CLOSE_SESSION_SQL: &str = StaticSql::dml(
    "UPDATE agent_sessions
SET state = ?1, updated_at_ms = ?2, revision = ?3
WHERE session_id = ?4 AND principal_kind = ?5 AND principal_id = ?6
  AND revision = ?7 AND state = ?8",
)
.as_str();

pub(super) const COUNT_SCHEMA_OBJECTS_SQL: &str =
    StaticSql::dml("SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").as_str();

pub(super) const LIST_SCHEMA_OBJECTS_SQL: &str = StaticSql::dml(
    "SELECT type, name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
)
.as_str();

pub(super) const SELECT_SCHEMA_SQL_LENGTH_SQL: &str = StaticSql::dml(
    "SELECT length(CAST(sql AS BLOB))
FROM sqlite_schema WHERE type = ?1 AND name = ?2",
)
.as_str();

pub(super) const SELECT_SCHEMA_SQL: &str =
    StaticSql::dml("SELECT sql FROM sqlite_schema WHERE type = ?1 AND name = ?2").as_str();

pub(super) const INSERT_SESSION_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_sessions(
    session_id, principal_kind, principal_id, request_id, state,
    created_at_ms, updated_at_ms, revision
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
)
.as_str();

pub(super) const LOAD_SESSION_SQL: &str = StaticSql::dml(
    "SELECT session_id, principal_kind, principal_id, request_id, state,
       created_at_ms, updated_at_ms, revision
FROM agent_sessions WHERE session_id = ?1",
)
.as_str();

pub(super) const QUICK_CHECK_SQL: &str = StaticSql::dml("PRAGMA quick_check(1)").as_str();

pub(super) const TABLE_INFO_AGENT_SESSIONS_SQL: &str =
    StaticSql::dml("PRAGMA table_info(agent_sessions)").as_str();

pub(super) const INDEX_LIST_AGENT_SESSIONS_SQL: &str =
    StaticSql::dml("PRAGMA index_list(agent_sessions)").as_str();

pub(super) const INDEX_INFO_STATE_UPDATED_SQL: &str =
    StaticSql::dml("PRAGMA index_info(agent_sessions_state_updated_idx)").as_str();

pub(super) const FOREIGN_KEY_CHECK_SQL: &str = StaticSql::dml("PRAGMA foreign_key_check").as_str();

pub(super) const HOME_ACCEPTANCE_PINS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE home_acceptance_session_pins (
    acceptance_id BLOB PRIMARY KEY NOT NULL CHECK(length(acceptance_id)=16),
    issuer_id BLOB NOT NULL CHECK(length(issuer_id)=16),
    session_id BLOB NOT NULL CHECK(length(session_id)=16),
    held_session_id BLOB CHECK(held_session_id IS NULL OR length(held_session_id)=16)
        REFERENCES agent_sessions(session_id),
    principal_kind INTEGER NOT NULL CHECK(principal_kind IN (0,1,2)),
    principal_id BLOB NOT NULL CHECK(length(principal_id)=16),
    session_revision INTEGER NOT NULL CHECK(session_revision>0),
    request_digest BLOB NOT NULL CHECK(length(request_digest)=32),
    request_snapshot BLOB NOT NULL CHECK(length(request_snapshot) BETWEEN 1 AND 320),
    state INTEGER NOT NULL CHECK(state IN (0,1,2)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0),
    held_at_ms INTEGER CHECK(held_at_ms IS NULL OR held_at_ms>=0),
    source_outcome_kind INTEGER CHECK(source_outcome_kind IS NULL OR source_outcome_kind IN (1,2)),
    source_payload_digest BLOB CHECK(source_payload_digest IS NULL OR length(source_payload_digest)=32),
    source_observed_at_ms INTEGER CHECK(source_observed_at_ms IS NULL OR source_observed_at_ms>=0),
    operations_resolution_digest BLOB CHECK(operations_resolution_digest IS NULL OR length(operations_resolution_digest)=32),
    operations_admission_id BLOB CHECK(operations_admission_id IS NULL OR length(operations_admission_id)=16),
    operations_admission_digest BLOB CHECK(operations_admission_digest IS NULL OR length(operations_admission_digest)=32),
    operations_held_at_ms INTEGER CHECK(operations_held_at_ms IS NULL OR operations_held_at_ms>=0),
    resolved_at_ms INTEGER CHECK(resolved_at_ms IS NULL OR resolved_at_ms>=created_at_ms),
    CHECK((principal_kind=0 AND principal_id=zeroblob(16)) OR principal_kind IN (1,2)),
    CHECK((operations_admission_id IS NULL AND operations_admission_digest IS NULL AND operations_held_at_ms IS NULL)
       OR (operations_admission_id IS NOT NULL AND operations_admission_digest IS NOT NULL AND operations_held_at_ms IS NOT NULL)),
    CHECK(
      (state=0 AND held_session_id IS NOT NULL AND held_session_id=session_id
        AND held_at_ms IS NOT NULL AND held_at_ms=created_at_ms
        AND source_outcome_kind IS NULL AND source_payload_digest IS NULL AND source_observed_at_ms IS NULL
        AND operations_resolution_digest IS NULL AND operations_admission_id IS NULL
        AND operations_admission_digest IS NULL AND operations_held_at_ms IS NULL AND resolved_at_ms IS NULL)
      OR
      (state=1 AND held_session_id IS NOT NULL AND held_session_id=session_id
        AND held_at_ms IS NOT NULL AND held_at_ms=created_at_ms
        AND source_outcome_kind=1 AND source_payload_digest IS NOT NULL AND source_observed_at_ms IS NOT NULL
        AND operations_resolution_digest IS NOT NULL AND operations_admission_id IS NOT NULL
        AND operations_admission_digest IS NOT NULL AND operations_held_at_ms=held_at_ms
        AND resolved_at_ms IS NOT NULL)
      OR
      (state=2 AND source_outcome_kind=2 AND source_payload_digest IS NOT NULL
        AND source_observed_at_ms IS NOT NULL AND operations_resolution_digest IS NOT NULL
        AND resolved_at_ms IS NOT NULL
        AND ((held_session_id IS NULL AND held_at_ms IS NULL AND operations_held_at_ms IS NULL)
          OR (held_session_id IS NOT NULL AND held_session_id=session_id
            AND held_at_ms IS NOT NULL AND held_at_ms=created_at_ms
            AND (operations_admission_id IS NULL OR operations_held_at_ms=held_at_ms))))
    )
);",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_ACTIVE_SESSION_INDEX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX home_acceptance_session_active_idx
ON home_acceptance_session_pins(session_id, acceptance_id) WHERE state=0;",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_PENDING_ISSUER_INDEX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX home_acceptance_session_pending_idx
ON home_acceptance_session_pins(issuer_id, acceptance_id) WHERE state=0;",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_PIN_TRANSITION_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER home_acceptance_session_pin_transition
BEFORE UPDATE ON home_acceptance_session_pins
WHEN OLD.state!=0 OR NEW.state NOT IN (1,2)
  OR NEW.acceptance_id IS NOT OLD.acceptance_id
  OR NEW.issuer_id IS NOT OLD.issuer_id
  OR NEW.session_id IS NOT OLD.session_id
  OR NEW.held_session_id IS NOT OLD.held_session_id
  OR NEW.principal_kind IS NOT OLD.principal_kind
  OR NEW.principal_id IS NOT OLD.principal_id
  OR NEW.session_revision IS NOT OLD.session_revision
  OR NEW.request_digest IS NOT OLD.request_digest
  OR NEW.request_snapshot IS NOT OLD.request_snapshot
  OR NEW.created_at_ms IS NOT OLD.created_at_ms
  OR NEW.held_at_ms IS NOT OLD.held_at_ms
  OR NEW.resolved_at_ms IS NULL
BEGIN SELECT RAISE(ABORT, 'Home acceptance Session pin is immutable'); END;",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_PIN_DELETE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER home_acceptance_session_pin_no_delete
BEFORE DELETE ON home_acceptance_session_pins
BEGIN SELECT RAISE(ABORT, 'Home acceptance Session history is permanent'); END;",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_SESSION_UPDATE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_sessions_block_home_acceptance_update
BEFORE UPDATE OF session_id,principal_kind,principal_id,request_id,state,revision ON agent_sessions
WHEN EXISTS(SELECT 1 FROM home_acceptance_session_pins h
            WHERE h.session_id=OLD.session_id AND h.state=0)
  AND (NEW.session_id IS NOT OLD.session_id OR NEW.principal_kind IS NOT OLD.principal_kind
    OR NEW.principal_id IS NOT OLD.principal_id OR NEW.request_id IS NOT OLD.request_id
    OR NEW.state IS NOT OLD.state OR NEW.revision IS NOT OLD.revision)
BEGIN SELECT RAISE(ABORT, 'logical Session has an active Home acceptance'); END;",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_SESSION_DELETE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_sessions_block_home_acceptance_delete
BEFORE DELETE ON agent_sessions
WHEN EXISTS(SELECT 1 FROM home_acceptance_session_pins h
            WHERE h.session_id=OLD.session_id AND h.state=0)
BEGIN SELECT RAISE(ABORT, 'logical Session has an active Home acceptance'); END;",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_PENDING_IDS_SQL: &str = StaticSql::dml(
    "SELECT acceptance_id FROM home_acceptance_session_pins
WHERE issuer_id=?1 AND state=0 AND (?2 IS NULL OR acceptance_id>?2)
ORDER BY acceptance_id LIMIT ?3",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_ACTIVE_ID_FOR_SESSION_SQL: &str = StaticSql::dml(
    "SELECT substr(acceptance_id,1,17)
FROM home_acceptance_session_pins
WHERE session_id=?1 AND state=0 ORDER BY acceptance_id LIMIT 1",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_INSERT_HELD_SQL: &str = StaticSql::dml(
    "INSERT INTO home_acceptance_session_pins(
    acceptance_id,issuer_id,session_id,held_session_id,
    principal_kind,principal_id,session_revision,request_digest,request_snapshot,
    state,created_at_ms,held_at_ms,source_outcome_kind,source_payload_digest,
    source_observed_at_ms,operations_resolution_digest,operations_admission_id,
    operations_admission_digest,operations_held_at_ms,resolved_at_ms
) VALUES(?1,?2,?3,?3,?4,?5,?6,?7,?8,0,?9,?9,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_INSERT_RESOLVED_ABSENT_SQL: &str = StaticSql::dml(
    "INSERT INTO home_acceptance_session_pins(
    acceptance_id,issuer_id,session_id,held_session_id,
    principal_kind,principal_id,session_revision,request_digest,request_snapshot,
    state,created_at_ms,held_at_ms,source_outcome_kind,source_payload_digest,
    source_observed_at_ms,operations_resolution_digest,operations_admission_id,
    operations_admission_digest,operations_held_at_ms,resolved_at_ms
) VALUES(?1,?2,?3,NULL,?4,?5,?6,?7,?8,2,?9,NULL,2,?10,?11,?12,?13,?14,?15,?16)",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_UPDATE_RESOLUTION_SQL: &str = StaticSql::dml(
    "UPDATE home_acceptance_session_pins SET
    state=?1, source_outcome_kind=?2, source_payload_digest=?3,
    source_observed_at_ms=?4, operations_resolution_digest=?5,
    operations_admission_id=?6, operations_admission_digest=?7,
    operations_held_at_ms=?8, resolved_at_ms=?9
WHERE acceptance_id=?10 AND state=0 AND request_digest=?11",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_LOAD_PIN_ROW_SQL: &str = StaticSql::dml(
    "SELECT substr(acceptance_id,1,17),length(acceptance_id),
       substr(issuer_id,1,17),length(issuer_id),
       substr(session_id,1,17),length(session_id),
       substr(held_session_id,1,17),length(held_session_id),
       principal_kind,substr(principal_id,1,17),length(principal_id),
       session_revision,substr(request_digest,1,33),length(request_digest),
       substr(request_snapshot,1,321),length(request_snapshot),state,
       created_at_ms,held_at_ms,source_outcome_kind,
       substr(source_payload_digest,1,33),length(source_payload_digest),
       source_observed_at_ms,
       substr(operations_resolution_digest,1,33),length(operations_resolution_digest),
       substr(operations_admission_id,1,17),length(operations_admission_id),
       substr(operations_admission_digest,1,33),length(operations_admission_digest),
       operations_held_at_ms,resolved_at_ms
FROM home_acceptance_session_pins WHERE acceptance_id=?1",
)
.as_str();

pub(super) const HOME_ACCEPTANCE_OBJECT_SQL: [(&str, &str, &str); 7] = [
    (
        "table",
        "home_acceptance_session_pins",
        HOME_ACCEPTANCE_PINS_TABLE_SQL,
    ),
    (
        "index",
        "home_acceptance_session_active_idx",
        HOME_ACCEPTANCE_ACTIVE_SESSION_INDEX_SQL,
    ),
    (
        "index",
        "home_acceptance_session_pending_idx",
        HOME_ACCEPTANCE_PENDING_ISSUER_INDEX_SQL,
    ),
    (
        "trigger",
        "home_acceptance_session_pin_transition",
        HOME_ACCEPTANCE_PIN_TRANSITION_SQL,
    ),
    (
        "trigger",
        "home_acceptance_session_pin_no_delete",
        HOME_ACCEPTANCE_PIN_DELETE_SQL,
    ),
    (
        "trigger",
        "agent_sessions_block_home_acceptance_update",
        HOME_ACCEPTANCE_SESSION_UPDATE_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_sessions_block_home_acceptance_delete",
        HOME_ACCEPTANCE_SESSION_DELETE_GUARD_SQL,
    ),
];
