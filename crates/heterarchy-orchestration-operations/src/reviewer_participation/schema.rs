use crate::OperationsError;
use rusqlite::Transaction;

const RECEIPTS_SQL: &str = "CREATE TABLE ops_reviewer_participation_receipts (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id != zeroblob(16)), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), revision INTEGER NOT NULL CHECK(revision>=1), expected_revision INTEGER NOT NULL CHECK(expected_revision>=0), statement TEXT NOT NULL CHECK(statement IN('INTERESTED','WITHDRAW_INTEREST')), withdraws_request_id BLOB CHECK(withdraws_request_id IS NULL OR (length(withdraws_request_id)=16 AND withdraws_request_id != zeroblob(16))), withdraws_revision INTEGER CHECK(withdraws_revision IS NULL OR withdraws_revision>=1), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id != zeroblob(16)), agent_id BLOB NOT NULL CHECK(length(agent_id)=16 AND agent_id != zeroblob(16)), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), UNIQUE(workspace_id,participation_id,revision), UNIQUE(workspace_id,participation_id,revision,request_id), FOREIGN KEY(workspace_id,participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id), FOREIGN KEY(workspace_id,participation_id,withdraws_revision,withdraws_request_id) REFERENCES ops_reviewer_participation_receipts(workspace_id,participation_id,revision,request_id), CHECK(revision=expected_revision+1), CHECK((statement='INTERESTED' AND withdraws_request_id IS NULL AND withdraws_revision IS NULL) OR (statement='WITHDRAW_INTEREST' AND withdraws_request_id IS NOT NULL AND withdraws_revision IS NOT NULL AND withdraws_revision=expected_revision)))";

const HEADS_SQL: &str = "CREATE TABLE ops_reviewer_participation_heads (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), current_revision INTEGER NOT NULL CHECK(current_revision>=1), request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), PRIMARY KEY(workspace_id,participation_id), FOREIGN KEY(workspace_id,participation_id,current_revision,request_id) REFERENCES ops_reviewer_participation_receipts(workspace_id,participation_id,revision,request_id))";

const RECEIPTS_NO_UPDATE_SQL: &str = "CREATE TRIGGER ops_reviewer_participation_receipts_no_update BEFORE UPDATE ON ops_reviewer_participation_receipts BEGIN SELECT RAISE(ABORT,'reviewer participation receipts are immutable'); END";
const RECEIPTS_NO_DELETE_SQL: &str = "CREATE TRIGGER ops_reviewer_participation_receipts_no_delete BEFORE DELETE ON ops_reviewer_participation_receipts BEGIN SELECT RAISE(ABORT,'reviewer participation receipts are immutable'); END";

pub(super) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_reviewer_participation_receipts", RECEIPTS_SQL),
        ("table", "ops_reviewer_participation_heads", HEADS_SQL),
        (
            "trigger",
            "ops_reviewer_participation_receipts_no_update",
            RECEIPTS_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_reviewer_participation_receipts_no_delete",
            RECEIPTS_NO_DELETE_SQL,
        ),
    ]
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}
