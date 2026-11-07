//! Canonical additive schema for logical Session acceptance pins.

use rusqlite::{Connection, OptionalExtension};

use super::HomeAcceptanceSessionError;

pub(super) const PINS_TABLE_SQL: &str = r"
CREATE TABLE home_acceptance_session_pins (
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
);
";

pub(super) const ACTIVE_SESSION_INDEX_SQL: &str = r"
CREATE INDEX home_acceptance_session_active_idx
ON home_acceptance_session_pins(session_id, acceptance_id) WHERE state=0;
";

pub(super) const PENDING_ISSUER_INDEX_SQL: &str = r"
CREATE INDEX home_acceptance_session_pending_idx
ON home_acceptance_session_pins(issuer_id, acceptance_id) WHERE state=0;
";

pub(super) const PIN_TRANSITION_SQL: &str = r"
CREATE TRIGGER home_acceptance_session_pin_transition
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
BEGIN SELECT RAISE(ABORT, 'Home acceptance Session pin is immutable'); END;
";

pub(super) const PIN_DELETE_SQL: &str = r"
CREATE TRIGGER home_acceptance_session_pin_no_delete
BEFORE DELETE ON home_acceptance_session_pins
BEGIN SELECT RAISE(ABORT, 'Home acceptance Session history is permanent'); END;
";

pub(super) const SESSION_UPDATE_GUARD_SQL: &str = r"
CREATE TRIGGER agent_sessions_block_home_acceptance_update
BEFORE UPDATE OF session_id,principal_kind,principal_id,request_id,state,revision ON agent_sessions
WHEN EXISTS(SELECT 1 FROM home_acceptance_session_pins h
            WHERE h.session_id=OLD.session_id AND h.state=0)
  AND (NEW.session_id IS NOT OLD.session_id OR NEW.principal_kind IS NOT OLD.principal_kind
    OR NEW.principal_id IS NOT OLD.principal_id OR NEW.request_id IS NOT OLD.request_id
    OR NEW.state IS NOT OLD.state OR NEW.revision IS NOT OLD.revision)
BEGIN SELECT RAISE(ABORT, 'logical Session has an active Home acceptance'); END;
";

pub(super) const SESSION_DELETE_GUARD_SQL: &str = r"
CREATE TRIGGER agent_sessions_block_home_acceptance_delete
BEFORE DELETE ON agent_sessions
WHEN EXISTS(SELECT 1 FROM home_acceptance_session_pins h
            WHERE h.session_id=OLD.session_id AND h.state=0)
BEGIN SELECT RAISE(ABORT, 'logical Session has an active Home acceptance'); END;
";

pub(super) const OBJECT_SQL: [(&str, &str, &str); 7] = [
    ("table", "home_acceptance_session_pins", PINS_TABLE_SQL),
    (
        "index",
        "home_acceptance_session_active_idx",
        ACTIVE_SESSION_INDEX_SQL,
    ),
    (
        "index",
        "home_acceptance_session_pending_idx",
        PENDING_ISSUER_INDEX_SQL,
    ),
    (
        "trigger",
        "home_acceptance_session_pin_transition",
        PIN_TRANSITION_SQL,
    ),
    (
        "trigger",
        "home_acceptance_session_pin_no_delete",
        PIN_DELETE_SQL,
    ),
    (
        "trigger",
        "agent_sessions_block_home_acceptance_update",
        SESSION_UPDATE_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_sessions_block_home_acceptance_delete",
        SESSION_DELETE_GUARD_SQL,
    ),
];

pub(in crate::catalog) fn create_schema(
    connection: &Connection,
) -> Result<(), HomeAcceptanceSessionError> {
    for (_, _, sql) in OBJECT_SQL {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(in crate::catalog) fn validate_schema(
    connection: &Connection,
) -> Result<(), HomeAcceptanceSessionError> {
    for (object_type, name, expected) in OBJECT_SQL {
        let actual: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type=?1 AND name=?2",
                rusqlite::params![object_type, name],
                |row| row.get(0),
            )
            .optional()?;
        let Some(actual) = actual else {
            return Err(HomeAcceptanceSessionError::Schema(name));
        };
        if actual.len() > 16 * 1024 || normalize_sql(&actual) != normalize_sql(expected) {
            return Err(HomeAcceptanceSessionError::Schema(name));
        }
    }
    Ok(())
}

pub(in crate::catalog) fn object_names() -> Vec<(&'static str, &'static str)> {
    OBJECT_SQL
        .iter()
        .map(|(object_type, name, _)| (*object_type, *name))
        .collect()
}

fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}
