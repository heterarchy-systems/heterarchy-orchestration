//! Additive schema owned by the temporary-dispatch admission boundary.

use rusqlite::Connection;
#[cfg(test)]
use rusqlite::{OptionalExtension, params};

use crate::OperationsError;

const REQUESTS_SQL: &str = "CREATE TABLE ops_temporary_dispatch_requests (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), admission_sequence INTEGER NOT NULL CHECK(admission_sequence>=1), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), requesting_team_id BLOB NOT NULL CHECK(length(requesting_team_id)=16 AND requesting_team_id != zeroblob(16)), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id != zeroblob(16)), actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id != zeroblob(16)), actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id != zeroblob(16)), actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id != zeroblob(16)), actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id != zeroblob(16)), actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id != zeroblob(16)), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), target_participant_id BLOB NOT NULL CHECK(length(target_participant_id)=16 AND target_participant_id != zeroblob(16)), target_home_team_id BLOB NOT NULL CHECK(length(target_home_team_id)=16 AND target_home_team_id != zeroblob(16)), requested_scope TEXT NOT NULL CHECK(length(CAST(requested_scope AS BLOB)) BETWEEN 1 AND 2048 AND length(trim(requested_scope))>0), requested_end_at_ms INTEGER NOT NULL CHECK(requested_end_at_ms>=0), lock_created_until_ms INTEGER CHECK(lock_created_until_ms IS NULL OR lock_created_until_ms>=0), admitted_at_ms INTEGER NOT NULL CHECK(admitted_at_ms>=0), CHECK(lock_created_until_ms IS NULL OR lock_created_until_ms>admitted_at_ms))";
const ADMISSION_WINDOW_INDEX_SQL: &str = "CREATE INDEX ops_temporary_dispatch_admission_window_idx ON ops_temporary_dispatch_requests(workspace_id,requesting_team_id,admitted_at_ms,request_id)";
const SEQUENCE_INDEX_SQL: &str = "CREATE UNIQUE INDEX ops_temporary_dispatch_sequence_idx ON ops_temporary_dispatch_requests(workspace_id,requesting_team_id,admission_sequence)";
const LOCK_HISTORY_INDEX_SQL: &str = "CREATE INDEX ops_temporary_dispatch_lock_history_idx ON ops_temporary_dispatch_requests(workspace_id,requesting_team_id,lock_created_until_ms DESC) WHERE lock_created_until_ms IS NOT NULL";
const RECEIPT_UPDATE_TRIGGER_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_receipt_no_update BEFORE UPDATE ON ops_temporary_dispatch_requests BEGIN SELECT RAISE(ABORT,'temporary dispatch admission receipts are immutable'); END";
const RECEIPT_DELETE_TRIGGER_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_receipt_no_delete BEFORE DELETE ON ops_temporary_dispatch_requests BEGIN SELECT RAISE(ABORT,'temporary dispatch admission receipts are immutable'); END";
const QUOTA_STATE_SQL: &str = "CREATE TABLE ops_temporary_dispatch_quota_state (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), requesting_team_id BLOB NOT NULL CHECK(length(requesting_team_id)=16 AND requesting_team_id != zeroblob(16)), last_admitted_at_ms INTEGER NOT NULL CHECK(last_admitted_at_ms>=0), last_sequence INTEGER NOT NULL CHECK(last_sequence>=1), recent_1_request_id BLOB NOT NULL CHECK(length(recent_1_request_id)=16 AND recent_1_request_id != zeroblob(16)), recent_1_sequence INTEGER NOT NULL CHECK(recent_1_sequence>=1), recent_1_admitted_at_ms INTEGER NOT NULL CHECK(recent_1_admitted_at_ms>=0), recent_2_request_id BLOB CHECK(recent_2_request_id IS NULL OR (length(recent_2_request_id)=16 AND recent_2_request_id != zeroblob(16))), recent_2_sequence INTEGER CHECK(recent_2_sequence IS NULL OR recent_2_sequence>=1), recent_2_admitted_at_ms INTEGER CHECK(recent_2_admitted_at_ms IS NULL OR recent_2_admitted_at_ms>=0), recent_3_request_id BLOB CHECK(recent_3_request_id IS NULL OR (length(recent_3_request_id)=16 AND recent_3_request_id != zeroblob(16))), recent_3_sequence INTEGER CHECK(recent_3_sequence IS NULL OR recent_3_sequence>=1), recent_3_admitted_at_ms INTEGER CHECK(recent_3_admitted_at_ms IS NULL OR recent_3_admitted_at_ms>=0), locked_until_ms INTEGER CHECK(locked_until_ms IS NULL OR locked_until_ms>=0), lock_request_id BLOB CHECK(lock_request_id IS NULL OR (length(lock_request_id)=16 AND lock_request_id != zeroblob(16))), PRIMARY KEY(workspace_id,requesting_team_id), CHECK(last_sequence=recent_1_sequence), CHECK(last_admitted_at_ms=recent_1_admitted_at_ms), CHECK((recent_2_request_id IS NULL AND recent_2_sequence IS NULL AND recent_2_admitted_at_ms IS NULL) OR (recent_2_request_id IS NOT NULL AND recent_2_sequence IS NOT NULL AND recent_2_admitted_at_ms IS NOT NULL)), CHECK((recent_3_request_id IS NULL AND recent_3_sequence IS NULL AND recent_3_admitted_at_ms IS NULL) OR (recent_3_request_id IS NOT NULL AND recent_3_sequence IS NOT NULL AND recent_3_admitted_at_ms IS NOT NULL)), CHECK(recent_2_sequence IS NULL OR recent_2_sequence=recent_1_sequence-1), CHECK(recent_3_sequence IS NULL OR (recent_2_sequence IS NOT NULL AND recent_3_sequence=recent_2_sequence-1)), CHECK((locked_until_ms IS NULL AND lock_request_id IS NULL) OR (locked_until_ms IS NOT NULL AND lock_request_id IS NOT NULL)), FOREIGN KEY(recent_1_request_id) REFERENCES ops_temporary_dispatch_requests(request_id), FOREIGN KEY(recent_2_request_id) REFERENCES ops_temporary_dispatch_requests(request_id), FOREIGN KEY(recent_3_request_id) REFERENCES ops_temporary_dispatch_requests(request_id), FOREIGN KEY(lock_request_id) REFERENCES ops_temporary_dispatch_requests(request_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_temporary_dispatch_requests", REQUESTS_SQL),
        (
            "index",
            "ops_temporary_dispatch_admission_window_idx",
            ADMISSION_WINDOW_INDEX_SQL,
        ),
        (
            "index",
            "ops_temporary_dispatch_sequence_idx",
            SEQUENCE_INDEX_SQL,
        ),
        (
            "index",
            "ops_temporary_dispatch_lock_history_idx",
            LOCK_HISTORY_INDEX_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_receipt_no_update",
            RECEIPT_UPDATE_TRIGGER_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_receipt_no_delete",
            RECEIPT_DELETE_TRIGGER_SQL,
        ),
        (
            "table",
            "ops_temporary_dispatch_quota_state",
            QUOTA_STATE_SQL,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn create_immutable_request_triggers(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(RECEIPT_UPDATE_TRIGGER_SQL)?;
    connection.execute_batch(RECEIPT_DELETE_TRIGGER_SQL)?;
    Ok(())
}

/// Validate this module's exact owned objects in its unit fixtures.
#[cfg(test)]
pub(super) fn validate_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (kind, name, expected_sql) in schema_objects() {
        let actual_sql = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type=?1 AND name=?2",
                params![kind, name],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or(OperationsError::Corrupt(
                "temporary dispatch schema object missing",
            ))?;
        if actual_sql != expected_sql {
            return Err(OperationsError::Corrupt(
                "temporary dispatch schema object changed",
            ));
        }
    }
    Ok(())
}
