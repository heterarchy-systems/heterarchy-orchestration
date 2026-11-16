use crate::OperationsError;
use rusqlite::Transaction;

use super::sql::{
    LAUNCH_LEDGER_NO_DELETE_SQL, LAUNCH_LEDGER_SQL, LAUNCH_LEDGER_STATE_IDX_SQL,
    SNAPSHOTS_NO_DELETE_SQL, SNAPSHOTS_NO_UPDATE_SQL, SNAPSHOTS_SQL,
};

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_assignment_execution_snapshots", SNAPSHOTS_SQL),
        ("table", "ops_assignment_launch_ledger", LAUNCH_LEDGER_SQL),
        (
            "index",
            "ops_assignment_launch_ledger_state_idx",
            LAUNCH_LEDGER_STATE_IDX_SQL,
        ),
        (
            "trigger",
            "ops_assignment_execution_snapshots_no_update",
            SNAPSHOTS_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_assignment_execution_snapshots_no_delete",
            SNAPSHOTS_NO_DELETE_SQL,
        ),
        (
            "trigger",
            "ops_assignment_launch_ledger_no_delete",
            LAUNCH_LEDGER_NO_DELETE_SQL,
        ),
    ]
}

pub(crate) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}
