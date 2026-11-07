use rusqlite::{Transaction, params};

use crate::OperationsError;

const TENURE_ANCHORS: &str = "CREATE TABLE ops_team_membership_tenure_anchors (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id != zeroblob(16)), membership_revision INTEGER NOT NULL CHECK(membership_revision>=1), membership_request_id BLOB NOT NULL CHECK(length(membership_request_id)=16 AND membership_request_id != zeroblob(16)), membership_state TEXT NOT NULL CHECK(membership_state='JOINED'), membership_observed_at_ms INTEGER NOT NULL CHECK(membership_observed_at_ms>=0), membership_event_digest BLOB NOT NULL CHECK(length(membership_event_digest)=32 AND membership_event_digest != zeroblob(32)), source_kind TEXT NOT NULL CHECK(source_kind IN('CAPACITY_CREATE','APPROVED_SLOT_JOIN')), source_digest BLOB NOT NULL CHECK(length(source_digest)=32 AND source_digest != zeroblob(32)), anchored_at_ms INTEGER NOT NULL CHECK(anchored_at_ms>=membership_observed_at_ms), actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id != zeroblob(16)), actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id != zeroblob(16)), actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id != zeroblob(16)), actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id != zeroblob(16)), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), PRIMARY KEY(workspace_id,team_id,participation_id,membership_revision), UNIQUE(workspace_id,team_id,participation_id,membership_request_id), FOREIGN KEY(workspace_id,team_id,participation_id) REFERENCES ops_team_members(workspace_id,team_id,participation_id), FOREIGN KEY(membership_request_id,participation_id) REFERENCES ops_team_member_events(request_id,participation_id))";

const TENURE_ANCHORS_TIME_INDEX: &str = "CREATE INDEX ops_team_membership_tenure_anchors_time_idx ON ops_team_membership_tenure_anchors(workspace_id,team_id,participation_id,anchored_at_ms)";
const TENURE_ANCHORS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_membership_tenure_anchors_no_update BEFORE UPDATE ON ops_team_membership_tenure_anchors BEGIN SELECT RAISE(ABORT,'team membership tenure anchors are immutable'); END";
const TENURE_ANCHORS_NO_DELETE: &str = "CREATE TRIGGER ops_team_membership_tenure_anchors_no_delete BEFORE DELETE ON ops_team_membership_tenure_anchors BEGIN SELECT RAISE(ABORT,'team membership tenure anchors are retained'); END";

pub(super) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "table",
            "ops_team_membership_tenure_anchors",
            TENURE_ANCHORS,
        ),
        (
            "index",
            "ops_team_membership_tenure_anchors_time_idx",
            TENURE_ANCHORS_TIME_INDEX,
        ),
        (
            "trigger",
            "ops_team_membership_tenure_anchors_no_update",
            TENURE_ANCHORS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_membership_tenure_anchors_no_delete",
            TENURE_ANCHORS_NO_DELETE,
        ),
    ]
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, statement) in schema_objects() {
        transaction.execute_batch(statement)?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn create_schema_for_test(
    connection: &rusqlite::Connection,
) -> Result<(), OperationsError> {
    for (kind, name, _) in schema_objects() {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type=?1 AND name=?2)",
            params![kind, name],
            |row| row.get(0),
        )?;
        if !exists {
            let sql = schema_objects()
                .iter()
                .find(|(object_kind, object_name, _)| object_kind == kind && object_name == name)
                .map(|(_, _, sql)| *sql)
                .ok_or(OperationsError::Corrupt("team tenure test schema object"))?;
            connection.execute_batch(sql)?;
        }
    }
    Ok(())
}
