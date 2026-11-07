//! Versioned Workspace capacity hold objects added alongside Team approval.

use rusqlite::Transaction;

use crate::OperationsError;

const MANIFESTS: &str = "CREATE TABLE ops_team_expansion_workspace_hold_manifests(decision_id BLOB PRIMARY KEY NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),plan_version INTEGER NOT NULL CHECK(plan_version=1),slot_count INTEGER NOT NULL CHECK(slot_count BETWEEN 1 AND 8),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),UNIQUE(workspace_id,team_id,decision_id),UNIQUE(workspace_id,team_id,decision_id,request_id),FOREIGN KEY(workspace_id,team_id,decision_id,request_id) REFERENCES ops_team_expansion_decisions(workspace_id,team_id,decision_id,request_id))";
const MANIFESTS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_workspace_hold_manifests_no_update BEFORE UPDATE ON ops_team_expansion_workspace_hold_manifests BEGIN SELECT RAISE(ABORT,'Team expansion Workspace hold manifests are immutable'); END";
const MANIFESTS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_workspace_hold_manifests_no_delete BEFORE DELETE ON ops_team_expansion_workspace_hold_manifests BEGIN SELECT RAISE(ABORT,'Team expansion Workspace hold manifests are immutable'); END";

const PLANS: &str = "CREATE TABLE ops_team_expansion_workspace_hold_plans(decision_id BLOB NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 7),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),target_kind INTEGER NOT NULL CHECK(target_kind IN(0,1,2)),target_profile_id BLOB CHECK(target_profile_id IS NULL OR (length(target_profile_id)=16 AND target_profile_id!=zeroblob(16))),target_participation_id BLOB CHECK(target_participation_id IS NULL OR (length(target_participation_id)=16 AND target_participation_id!=zeroblob(16))),PRIMARY KEY(decision_id,ordinal),UNIQUE(workspace_id,decision_id,ordinal),UNIQUE(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,team_id,decision_id) REFERENCES ops_team_expansion_workspace_hold_manifests(workspace_id,team_id,decision_id),FOREIGN KEY(workspace_id,team_id,decision_id,ordinal) REFERENCES ops_team_expansion_slots(workspace_id,team_id,decision_id,ordinal),CHECK((target_kind=0 AND target_profile_id IS NOT NULL AND target_participation_id IS NOT NULL) OR (target_kind=1 AND target_profile_id IS NOT NULL AND target_participation_id IS NULL) OR (target_kind=2 AND target_profile_id IS NULL AND target_participation_id IS NULL)))";
const PLANS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_workspace_hold_plans_no_update BEFORE UPDATE ON ops_team_expansion_workspace_hold_plans BEGIN SELECT RAISE(ABORT,'Team expansion Workspace hold plans are immutable'); END";
const PLANS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_workspace_hold_plans_no_delete BEFORE DELETE ON ops_team_expansion_workspace_hold_plans BEGIN SELECT RAISE(ABORT,'Team expansion Workspace hold plans are immutable'); END";

const BINDINGS: &str = "CREATE TABLE ops_team_expansion_workspace_hold_bindings(decision_id BLOB NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 7),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),binding_request_id BLOB NOT NULL UNIQUE CHECK(length(binding_request_id)=16 AND binding_request_id!=zeroblob(16)),profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id!=zeroblob(16)),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),PRIMARY KEY(decision_id,ordinal),FOREIGN KEY(workspace_id,decision_id,ordinal) REFERENCES ops_team_expansion_workspace_hold_plans(workspace_id,decision_id,ordinal))";
const BINDINGS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_workspace_hold_bindings_no_update BEFORE UPDATE ON ops_team_expansion_workspace_hold_bindings BEGIN SELECT RAISE(ABORT,'Team expansion Workspace hold bindings are immutable'); END";
const BINDINGS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_workspace_hold_bindings_no_delete BEFORE DELETE ON ops_team_expansion_workspace_hold_bindings BEGIN SELECT RAISE(ABORT,'Team expansion Workspace hold bindings are immutable'); END";

const HOLD_PROFILE_INDEX: &str = "CREATE INDEX ops_team_expansion_workspace_hold_profile_idx ON ops_team_expansion_workspace_hold_plans(workspace_id,target_profile_id)";

const PRE_HOLD_DECISIONS: &str = "CREATE TABLE ops_team_expansion_workspace_hold_pre_hold_decisions(decision_id BLOB PRIMARY KEY NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),decision_state TEXT NOT NULL CHECK(decision_state IN('APPROVED','REJECTED')),classification TEXT NOT NULL CHECK(classification='PRE_HOLD'),UNIQUE(workspace_id,team_id,decision_id,request_id),FOREIGN KEY(workspace_id,team_id,decision_id,request_id) REFERENCES ops_team_expansion_decisions(workspace_id,team_id,decision_id,request_id))";
const PRE_HOLD_DECISIONS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_workspace_hold_pre_hold_no_update BEFORE UPDATE ON ops_team_expansion_workspace_hold_pre_hold_decisions BEGIN SELECT RAISE(ABORT,'pre-hold Team expansion classifications are immutable'); END";
const PRE_HOLD_DECISIONS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_workspace_hold_pre_hold_no_delete BEFORE DELETE ON ops_team_expansion_workspace_hold_pre_hold_decisions BEGIN SELECT RAISE(ABORT,'pre-hold Team expansion classifications are immutable'); END";

const LEGACY_RECOVERIES: &str = "CREATE TABLE ops_team_expansion_legacy_slot_recoveries(request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),decision_id BLOB NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 7),approval_request_id BLOB NOT NULL CHECK(length(approval_request_id)=16 AND approval_request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),expected_team_revision INTEGER NOT NULL CHECK(expected_team_revision>=1),capacity_revision INTEGER NOT NULL CHECK(capacity_revision>=1),leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1),target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id!=zeroblob(16)),target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id!=zeroblob(16)),actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id!=zeroblob(16)),actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id!=zeroblob(16)),actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id!=zeroblob(16)),actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)),actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)),actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16)),actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),UNIQUE(workspace_id,team_id,decision_id,ordinal),UNIQUE(workspace_id,request_id),FOREIGN KEY(workspace_id,team_id,decision_id,ordinal) REFERENCES ops_team_expansion_slots(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,team_id,decision_id,approval_request_id) REFERENCES ops_team_expansion_workspace_hold_pre_hold_decisions(workspace_id,team_id,decision_id,request_id))";
const LEGACY_RECOVERIES_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_legacy_slot_recoveries_no_update BEFORE UPDATE ON ops_team_expansion_legacy_slot_recoveries BEGIN SELECT RAISE(ABORT,'legacy Team expansion slot recoveries are immutable'); END";
const LEGACY_RECOVERIES_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_legacy_slot_recoveries_no_delete BEFORE DELETE ON ops_team_expansion_legacy_slot_recoveries BEGIN SELECT RAISE(ABORT,'legacy Team expansion slot recoveries are retained'); END";

/// Migration-only classification for decisions that predate Workspace hold
/// admission. It claims no particular catalog source version and records no
/// target, slot hold, occupancy or target authority.
pub(crate) const PRE_HOLD_DECISION_BACKFILL_SQL: &str = "INSERT INTO ops_team_expansion_workspace_hold_pre_hold_decisions(decision_id,request_id,workspace_id,team_id,decision_state,classification) SELECT decision_id,request_id,workspace_id,team_id,state,'PRE_HOLD' FROM ops_team_expansion_decisions";

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "table",
            "ops_team_expansion_workspace_hold_manifests",
            MANIFESTS,
        ),
        (
            "trigger",
            "ops_team_expansion_workspace_hold_manifests_no_update",
            MANIFESTS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_workspace_hold_manifests_no_delete",
            MANIFESTS_NO_DELETE,
        ),
        ("table", "ops_team_expansion_workspace_hold_plans", PLANS),
        (
            "index",
            "ops_team_expansion_workspace_hold_profile_idx",
            HOLD_PROFILE_INDEX,
        ),
        (
            "trigger",
            "ops_team_expansion_workspace_hold_plans_no_update",
            PLANS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_workspace_hold_plans_no_delete",
            PLANS_NO_DELETE,
        ),
        (
            "table",
            "ops_team_expansion_workspace_hold_bindings",
            BINDINGS,
        ),
        (
            "trigger",
            "ops_team_expansion_workspace_hold_bindings_no_update",
            BINDINGS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_workspace_hold_bindings_no_delete",
            BINDINGS_NO_DELETE,
        ),
        (
            "table",
            "ops_team_expansion_workspace_hold_pre_hold_decisions",
            PRE_HOLD_DECISIONS,
        ),
        (
            "trigger",
            "ops_team_expansion_workspace_hold_pre_hold_no_update",
            PRE_HOLD_DECISIONS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_workspace_hold_pre_hold_no_delete",
            PRE_HOLD_DECISIONS_NO_DELETE,
        ),
    ]
}

pub(crate) const fn recovery_schema_objects()
-> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "table",
            "ops_team_expansion_legacy_slot_recoveries",
            LEGACY_RECOVERIES,
        ),
        (
            "trigger",
            "ops_team_expansion_legacy_slot_recoveries_no_update",
            LEGACY_RECOVERIES_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_legacy_slot_recoveries_no_delete",
            LEGACY_RECOVERIES_NO_DELETE,
        ),
    ]
}

/// Add the new hold objects to the schema version that owns this module.
/// Existing Team expansion approvals are not backfilled or certified here.
pub(crate) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}

pub(crate) fn create_recovery_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, sql) in recovery_schema_objects() {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}
