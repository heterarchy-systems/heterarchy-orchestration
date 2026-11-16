//! Schema registration hooks for the Swarm tables, indexes, and triggers.

use crate::OperationsError;
use rusqlite::Transaction;

use super::sql::{
    SWARM_CHANNEL_MESSAGES_RUN_SEQ_IDX_SQL, SWARM_CHANNEL_MESSAGES_SQL,
    SWARM_CHANNEL_SUBSCRIPTIONS_MEMBER_IDX_SQL, SWARM_CHANNEL_SUBSCRIPTIONS_SQL,
    SWARM_CHANNELS_SQL, SWARM_DOORBELLS_PENDING_IDX_SQL, SWARM_DOORBELLS_SQL,
    SWARM_MEMBERS_COORDINATOR_IDX_SQL, SWARM_MEMBERS_PARENT_IDX_SQL, SWARM_MEMBERS_SQL,
    SWARM_MEMBERS_STATE_IDX_SQL, SWARM_RUNS_SQL, SWARM_RUNS_WORKSPACE_IDX_SQL,
    SWARM_VERDICTS_NO_DELETE_SQL, SWARM_VERDICTS_NO_UPDATE_SQL, SWARM_VERDICTS_PANEL_IDX_SQL,
    SWARM_VERDICTS_SQL, SWARM_VERIFICATION_PANEL_SEATS_SQL, SWARM_VERIFICATION_PANELS_RUN_IDX_SQL,
    SWARM_VERIFICATION_PANELS_SQL,
};

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_swarm_runs", SWARM_RUNS_SQL),
        (
            "index",
            "ops_swarm_runs_workspace_idx",
            SWARM_RUNS_WORKSPACE_IDX_SQL,
        ),
        ("table", "ops_swarm_members", SWARM_MEMBERS_SQL),
        (
            "index",
            "ops_swarm_members_coordinator_idx",
            SWARM_MEMBERS_COORDINATOR_IDX_SQL,
        ),
        (
            "index",
            "ops_swarm_members_state_idx",
            SWARM_MEMBERS_STATE_IDX_SQL,
        ),
        (
            "index",
            "ops_swarm_members_parent_idx",
            SWARM_MEMBERS_PARENT_IDX_SQL,
        ),
        ("table", "ops_swarm_channels", SWARM_CHANNELS_SQL),
        (
            "table",
            "ops_swarm_channel_subscriptions",
            SWARM_CHANNEL_SUBSCRIPTIONS_SQL,
        ),
        (
            "index",
            "ops_swarm_channel_subscriptions_member_idx",
            SWARM_CHANNEL_SUBSCRIPTIONS_MEMBER_IDX_SQL,
        ),
        (
            "table",
            "ops_swarm_channel_messages",
            SWARM_CHANNEL_MESSAGES_SQL,
        ),
        (
            "index",
            "ops_swarm_channel_messages_run_seq_idx",
            SWARM_CHANNEL_MESSAGES_RUN_SEQ_IDX_SQL,
        ),
        ("table", "ops_swarm_doorbells", SWARM_DOORBELLS_SQL),
        (
            "index",
            "ops_swarm_doorbells_pending_idx",
            SWARM_DOORBELLS_PENDING_IDX_SQL,
        ),
        (
            "table",
            "ops_swarm_verification_panels",
            SWARM_VERIFICATION_PANELS_SQL,
        ),
        (
            "index",
            "ops_swarm_verification_panels_run_idx",
            SWARM_VERIFICATION_PANELS_RUN_IDX_SQL,
        ),
        (
            "table",
            "ops_swarm_verification_panel_seats",
            SWARM_VERIFICATION_PANEL_SEATS_SQL,
        ),
        ("table", "ops_swarm_verdicts", SWARM_VERDICTS_SQL),
        (
            "index",
            "ops_swarm_verdicts_panel_idx",
            SWARM_VERDICTS_PANEL_IDX_SQL,
        ),
        (
            "trigger",
            "ops_swarm_verdicts_no_update",
            SWARM_VERDICTS_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_swarm_verdicts_no_delete",
            SWARM_VERDICTS_NO_DELETE_SQL,
        ),
    ]
}

pub(crate) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}
