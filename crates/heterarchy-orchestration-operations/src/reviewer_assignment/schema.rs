use crate::OperationsError;
use rusqlite::Transaction;

use super::sql::{
    ASSIGNMENTS_NO_DELETE_SQL, ASSIGNMENTS_SQL, ASSIGNMENTS_WORKSPACE_IDX_SQL,
    VERDICT_RECEIPTS_NO_DELETE_SQL, VERDICT_RECEIPTS_NO_UPDATE_SQL, VERDICT_RECEIPTS_SQL,
};

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_reviewer_assignments", ASSIGNMENTS_SQL),
        (
            "table",
            "ops_reviewer_verdict_receipts",
            VERDICT_RECEIPTS_SQL,
        ),
        (
            "index",
            "ops_reviewer_assignments_workspace_idx",
            ASSIGNMENTS_WORKSPACE_IDX_SQL,
        ),
        (
            "trigger",
            "ops_reviewer_assignments_no_delete",
            ASSIGNMENTS_NO_DELETE_SQL,
        ),
        (
            "trigger",
            "ops_reviewer_verdict_receipts_no_update",
            VERDICT_RECEIPTS_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_reviewer_verdict_receipts_no_delete",
            VERDICT_RECEIPTS_NO_DELETE_SQL,
        ),
    ]
}

pub(crate) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}
