//! DDL authority for first-attempt Source dispatch and original receipt rows.

use rusqlite::{Connection, OptionalExtension, params};

use crate::OperationsError;

pub(crate) const MAX_DISPATCH_PAYLOAD_BYTES: usize = 1_048_576;
pub(crate) const MAX_SOURCE_REQUEST_ENVELOPE_BYTES: usize = 128 * 1024;
pub(crate) const MAX_SOURCE_SNAPSHOT_BYTES: usize = 1_048_576;

const DISPATCHES: &str = "CREATE TABLE ops_team_permanent_transfer_source_dispatches (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), preparation_id BLOB NOT NULL CHECK(length(preparation_id)=16 AND preparation_id!=zeroblob(16)), transfer_id BLOB NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)), generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 9223372036854775807), mutation_request_id BLOB NOT NULL CHECK(length(mutation_request_id)=16 AND mutation_request_id!=zeroblob(16)), actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)), actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)), transport_request_id BLOB NOT NULL CHECK(length(transport_request_id)=16 AND transport_request_id!=zeroblob(16)), envelope_digest BLOB NOT NULL CHECK(length(envelope_digest)=32), body_sha256 BLOB NOT NULL CHECK(length(body_sha256)=32), body_byte_count INTEGER NOT NULL CHECK(body_byte_count BETWEEN 1 AND 65536), source_request_envelope BLOB NOT NULL CHECK(length(source_request_envelope) BETWEEN 1 AND 131072), payload BLOB NOT NULL CHECK(length(payload) BETWEEN 1 AND 1048576), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), PRIMARY KEY(workspace_id,preparation_id), UNIQUE(workspace_id,mutation_request_id), UNIQUE(workspace_id,transport_request_id), FOREIGN KEY(workspace_id,preparation_id) REFERENCES ops_team_permanent_transfer_preparations(workspace_id,preparation_id))";
const DISPATCHES_NO_UPDATE: &str = "CREATE TRIGGER ops_team_permanent_transfer_source_dispatches_no_update BEFORE UPDATE ON ops_team_permanent_transfer_source_dispatches BEGIN SELECT RAISE(ABORT,'permanent transfer Source dispatches are immutable'); END";
const DISPATCHES_NO_DELETE: &str = "CREATE TRIGGER ops_team_permanent_transfer_source_dispatches_no_delete BEFORE DELETE ON ops_team_permanent_transfer_source_dispatches BEGIN SELECT RAISE(ABORT,'permanent transfer Source dispatches are immutable'); END";

const RECEIPTS: &str = "CREATE TABLE ops_team_permanent_transfer_source_receipts (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), preparation_id BLOB NOT NULL CHECK(length(preparation_id)=16 AND preparation_id!=zeroblob(16)), envelope_digest BLOB NOT NULL CHECK(length(envelope_digest)=32), source_outcome_digest BLOB NOT NULL CHECK(length(source_outcome_digest)=32), snapshot_payload BLOB NOT NULL CHECK(length(snapshot_payload) BETWEEN 1 AND 1048576), snapshot_sha256 BLOB NOT NULL CHECK(length(snapshot_sha256)=32), local_recorded_at_ms INTEGER NOT NULL CHECK(local_recorded_at_ms>=0), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32), PRIMARY KEY(workspace_id,preparation_id), FOREIGN KEY(workspace_id,preparation_id) REFERENCES ops_team_permanent_transfer_source_dispatches(workspace_id,preparation_id))";
const RECEIPTS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_permanent_transfer_source_receipts_no_update BEFORE UPDATE ON ops_team_permanent_transfer_source_receipts BEGIN SELECT RAISE(ABORT,'permanent transfer Source receipts are immutable'); END";
const RECEIPTS_NO_DELETE: &str = "CREATE TRIGGER ops_team_permanent_transfer_source_receipts_no_delete BEFORE DELETE ON ops_team_permanent_transfer_source_receipts BEGIN SELECT RAISE(ABORT,'permanent transfer Source receipts are immutable'); END";

const OBJECTS: &[(&str, &str, &str)] = &[
    (
        "table",
        "ops_team_permanent_transfer_source_dispatches",
        DISPATCHES,
    ),
    (
        "trigger",
        "ops_team_permanent_transfer_source_dispatches_no_update",
        DISPATCHES_NO_UPDATE,
    ),
    (
        "trigger",
        "ops_team_permanent_transfer_source_dispatches_no_delete",
        DISPATCHES_NO_DELETE,
    ),
    (
        "table",
        "ops_team_permanent_transfer_source_receipts",
        RECEIPTS,
    ),
    (
        "trigger",
        "ops_team_permanent_transfer_source_receipts_no_update",
        RECEIPTS_NO_UPDATE,
    ),
    (
        "trigger",
        "ops_team_permanent_transfer_source_receipts_no_delete",
        RECEIPTS_NO_DELETE,
    ),
];

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    OBJECTS
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in OBJECTS {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(crate) fn validate_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (kind, name, expected) in OBJECTS {
        let actual: Option<String> = connection
            .query_row(
                "SELECT CASE WHEN length(CAST(sql AS BLOB))<=16384 THEN sql END
                   FROM sqlite_schema WHERE type=?1 AND name=?2",
                params![kind, name],
                |row| row.get(0),
            )
            .optional()?;
        if actual.as_deref().map(normalize_sql) != Some(normalize_sql(expected)) {
            return Err(OperationsError::Corrupt("permanent transfer Source schema"));
        }
    }
    Ok(())
}

fn normalize_sql(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}
