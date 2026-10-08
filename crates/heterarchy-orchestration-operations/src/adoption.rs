//! Historical schema ownership for retired legacy-adoption evidence.
//!
//! Phase G removed every production adoption writer and readback API. Existing
//! rows remain immutable migration evidence so older catalogs reopen exactly;
//! no new `ops_legacy_adoptions` row can be created through Operations.

use crate::OperationsError;
use rusqlite::Connection;

const ADOPTIONS_SQL: &str = "CREATE TABLE ops_legacy_adoptions (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), migration_id BLOB NOT NULL CHECK(length(migration_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), destination_id BLOB NOT NULL CHECK(length(destination_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), agent_id BLOB NOT NULL CHECK(length(agent_id)=16 AND agent_id != zeroblob(16)), expected_agent_revision INTEGER NOT NULL CHECK(expected_agent_revision>=1), result_agent_revision INTEGER NOT NULL CHECK(result_agent_revision=expected_agent_revision+1), identity_digest BLOB NOT NULL CHECK(length(identity_digest)=32 AND identity_digest != zeroblob(32)), recipient_participant_id BLOB CHECK(recipient_participant_id IS NULL OR (length(recipient_participant_id)=16 AND recipient_participant_id != zeroblob(16))), joined_event_id BLOB NOT NULL CHECK(length(joined_event_id)=16), source_reconciled_at_ms INTEGER NOT NULL CHECK(source_reconciled_at_ms>=0), committed_at_ms INTEGER NOT NULL CHECK(committed_at_ms>=source_reconciled_at_ms), FOREIGN KEY(migration_id) REFERENCES ops_imports(migration_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id), FOREIGN KEY(workspace_id,joined_event_id) REFERENCES ops_logical_events(workspace_id,event_id), FOREIGN KEY(workspace_id,recipient_participant_id) REFERENCES ops_logical_members(workspace_id,participant_id), UNIQUE(workspace_id,agent_id), UNIQUE(workspace_id,activity_id,session_id), UNIQUE(workspace_id,participant_id,activity_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![("table", "ops_legacy_adoptions", ADOPTIONS_SQL)]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(ADOPTIONS_SQL)?;
    Ok(())
}
