use crate::OperationsError;
use rusqlite::Transaction;

#[cfg(test)]
use rusqlite::{Connection, OptionalExtension};

/// Exact objects added by the qualification-rule migration.
///
/// The catalog owner includes these in its versioned schema manifest. Existing
/// catalogs receive only these empty objects; this module performs no backfill.
pub(super) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "table",
            "ops_reviewer_qualification_rule_generations",
            RULE_GENERATIONS_SQL,
        ),
        (
            "table",
            "ops_reviewer_qualification_rule_heads",
            RULE_HEADS_SQL,
        ),
        (
            "trigger",
            "ops_reviewer_qualification_rule_generations_no_update",
            RULE_GENERATIONS_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_reviewer_qualification_rule_generations_no_delete",
            RULE_GENERATIONS_NO_DELETE_SQL,
        ),
    ]
}

const RULE_GENERATIONS_SQL: &str = "CREATE TABLE ops_reviewer_qualification_rule_generations (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), rule_id BLOB NOT NULL CHECK(length(rule_id)=16), generation INTEGER NOT NULL CHECK(generation>=1), expected_generation INTEGER CHECK(expected_generation IS NULL OR expected_generation>=1), request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16), requester_session_id BLOB NOT NULL CHECK(length(requester_session_id)=16), actor_direct_token_id BLOB NOT NULL CHECK(length(actor_direct_token_id)=16), state INTEGER NOT NULL CHECK(state IN(0,1)), domain TEXT NOT NULL CHECK(length(CAST(domain AS BLOB)) BETWEEN 1 AND 128), permitted_scope TEXT NOT NULL CHECK(length(CAST(permitted_scope AS BLOB)) BETWEEN 1 AND 128), policy_json TEXT NOT NULL CHECK(length(CAST(policy_json AS BLOB)) BETWEEN 2 AND 16384), change_json TEXT NOT NULL CHECK(length(CAST(change_json AS BLOB)) BETWEEN 2 AND 16384), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), PRIMARY KEY(workspace_id,rule_id,generation), CHECK((generation=1 AND expected_generation IS NULL) OR (generation>1 AND expected_generation=generation-1)))";
const RULE_HEADS_SQL: &str = "CREATE TABLE ops_reviewer_qualification_rule_heads (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), rule_id BLOB NOT NULL CHECK(length(rule_id)=16), current_generation INTEGER NOT NULL CHECK(current_generation>=1), PRIMARY KEY(workspace_id,rule_id), FOREIGN KEY(workspace_id,rule_id,current_generation) REFERENCES ops_reviewer_qualification_rule_generations(workspace_id,rule_id,generation))";
const RULE_GENERATIONS_NO_UPDATE_SQL: &str = "CREATE TRIGGER ops_reviewer_qualification_rule_generations_no_update BEFORE UPDATE ON ops_reviewer_qualification_rule_generations BEGIN SELECT RAISE(ABORT,'qualification rule generations are immutable'); END";
const RULE_GENERATIONS_NO_DELETE_SQL: &str = "CREATE TRIGGER ops_reviewer_qualification_rule_generations_no_delete BEFORE DELETE ON ops_reviewer_qualification_rule_generations BEGIN SELECT RAISE(ABORT,'qualification rule generations are immutable'); END";

/// Create the additive rule-history schema inside the catalog owner's existing
/// transaction. No prior Workspace data is read or backfilled.
///
/// # Errors
/// Returns the underlying SQLite schema error.
pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}

/// Validate that each qualification-rule object matches its canonical SQL.
/// The owning catalog validator remains responsible for the complete catalog
/// object set and schema version.
///
/// # Errors
/// Returns storage errors or a conflict for a missing or altered object.
#[cfg(test)]
pub(super) fn validate_schema(connection: &Connection) -> Result<(), OperationsError> {
    for &(kind, name, expected_sql) in schema_objects() {
        let actual_sql = connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type=?1 AND name=?2",
                [kind, name],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if actual_sql.as_deref().map(normalize_sql) != Some(normalize_sql(expected_sql)) {
            return Err(OperationsError::Conflict("qualification rule schema"));
        }
    }
    Ok(())
}

#[cfg(test)]
fn normalize_sql(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .trim_end_matches(';')
        .to_owned()
}
