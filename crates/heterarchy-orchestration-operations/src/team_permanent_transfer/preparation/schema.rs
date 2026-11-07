//! Single DDL authority for immutable permanent-transfer preparation records.

use crate::OperationsError;
use rusqlite::Connection;

pub(crate) const MAX_PAYLOAD: usize = 262_144;

const PREPARATIONS: &str = "CREATE TABLE ops_team_permanent_transfer_preparations (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), preparation_id BLOB NOT NULL CHECK(length(preparation_id)=16 AND preparation_id!=zeroblob(16)), transfer_id BLOB NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)), generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 9223372036854775807), intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32), target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id!=zeroblob(16)), target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id!=zeroblob(16)), actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)), actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)), payload BLOB NOT NULL CHECK(length(payload) BETWEEN 1 AND 262144), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), PRIMARY KEY(workspace_id,preparation_id), UNIQUE(workspace_id,transfer_id), UNIQUE(workspace_id,preparation_id,target_profile_id,target_participation_id), FOREIGN KEY(workspace_id,transfer_id,intent_digest) REFERENCES ops_team_permanent_transfer_intents(workspace_id,transfer_id,intent_digest), FOREIGN KEY(workspace_id,target_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id))";
const PREPARATIONS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_permanent_transfer_preparations_no_update BEFORE UPDATE ON ops_team_permanent_transfer_preparations BEGIN SELECT RAISE(ABORT,'permanent transfer preparation records are immutable'); END";
const PREPARATIONS_NO_DELETE: &str = "CREATE TRIGGER ops_team_permanent_transfer_preparations_no_delete BEFORE DELETE ON ops_team_permanent_transfer_preparations BEGIN SELECT RAISE(ABORT,'permanent transfer preparation records are immutable'); END";

const BARRIERS: &str = "CREATE TABLE ops_team_permanent_transfer_preparation_barriers (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), preparation_id BLOB NOT NULL CHECK(length(preparation_id)=16 AND preparation_id!=zeroblob(16)), target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id!=zeroblob(16)), target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id!=zeroblob(16)), PRIMARY KEY(workspace_id,target_profile_id), UNIQUE(workspace_id,target_participation_id), UNIQUE(workspace_id,preparation_id), FOREIGN KEY(workspace_id,preparation_id,target_profile_id,target_participation_id) REFERENCES ops_team_permanent_transfer_preparations(workspace_id,preparation_id,target_profile_id,target_participation_id))";
const BARRIERS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_permanent_transfer_preparation_barriers_no_update BEFORE UPDATE ON ops_team_permanent_transfer_preparation_barriers BEGIN SELECT RAISE(ABORT,'permanent transfer preparation barriers are immutable'); END";
const BARRIERS_NO_DELETE: &str = "CREATE TRIGGER ops_team_permanent_transfer_preparation_barriers_no_delete BEFORE DELETE ON ops_team_permanent_transfer_preparation_barriers BEGIN SELECT RAISE(ABORT,'permanent transfer preparation barriers are immutable'); END";

const SESSIONS: &str = "CREATE TABLE ops_team_permanent_transfer_preparation_sessions (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), preparation_id BLOB NOT NULL CHECK(length(preparation_id)=16 AND preparation_id!=zeroblob(16)), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id!=zeroblob(16)), PRIMARY KEY(workspace_id,preparation_id,session_id), FOREIGN KEY(workspace_id,preparation_id) REFERENCES ops_team_permanent_transfer_preparations(workspace_id,preparation_id))";
const SESSIONS_INDEX: &str = "CREATE INDEX ops_team_permanent_transfer_preparation_sessions_lookup ON ops_team_permanent_transfer_preparation_sessions(workspace_id,session_id)";
const SESSIONS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_permanent_transfer_preparation_sessions_no_update BEFORE UPDATE ON ops_team_permanent_transfer_preparation_sessions BEGIN SELECT RAISE(ABORT,'permanent transfer preparation session rows are immutable'); END";
const SESSIONS_NO_DELETE: &str = "CREATE TRIGGER ops_team_permanent_transfer_preparation_sessions_no_delete BEFORE DELETE ON ops_team_permanent_transfer_preparation_sessions BEGIN SELECT RAISE(ABORT,'permanent transfer preparation session rows are immutable'); END";

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "table",
            "ops_team_permanent_transfer_preparations",
            PREPARATIONS,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_preparations_no_update",
            PREPARATIONS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_preparations_no_delete",
            PREPARATIONS_NO_DELETE,
        ),
        (
            "table",
            "ops_team_permanent_transfer_preparation_barriers",
            BARRIERS,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_preparation_barriers_no_update",
            BARRIERS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_preparation_barriers_no_delete",
            BARRIERS_NO_DELETE,
        ),
        (
            "table",
            "ops_team_permanent_transfer_preparation_sessions",
            SESSIONS,
        ),
        (
            "index",
            "ops_team_permanent_transfer_preparation_sessions_lookup",
            SESSIONS_INDEX,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_preparation_sessions_no_update",
            SESSIONS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_preparation_sessions_no_delete",
            SESSIONS_NO_DELETE,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}
