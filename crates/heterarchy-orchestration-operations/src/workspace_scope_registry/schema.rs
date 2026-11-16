use crate::OperationsError;
use rusqlite::Transaction;

use super::sql::{
    MUTATIONS_NO_DELETE_SQL, MUTATIONS_NO_UPDATE_SQL, MUTATIONS_SQL,
    MUTATIONS_WORKSPACE_REVISION_IDX_SQL, REGISTRY_NO_DELETE_SQL, REGISTRY_SQL,
    REGISTRY_STATE_IDX_SQL,
};

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_workspace_scope_registry", REGISTRY_SQL),
        ("table", "ops_workspace_scope_mutations", MUTATIONS_SQL),
        (
            "index",
            "ops_workspace_scope_registry_state_idx",
            REGISTRY_STATE_IDX_SQL,
        ),
        (
            "index",
            "ops_workspace_scope_mutations_workspace_revision_idx",
            MUTATIONS_WORKSPACE_REVISION_IDX_SQL,
        ),
        (
            "trigger",
            "ops_workspace_scope_registry_no_delete",
            REGISTRY_NO_DELETE_SQL,
        ),
        (
            "trigger",
            "ops_workspace_scope_mutations_no_update",
            MUTATIONS_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_workspace_scope_mutations_no_delete",
            MUTATIONS_NO_DELETE_SQL,
        ),
    ]
}

pub(crate) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}
