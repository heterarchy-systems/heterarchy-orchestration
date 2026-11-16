//! Domain SQL definitions for `AssignmentExecutionSnapshot` and Launch Ledger persistence.

use crate::static_sql::StaticSql;

pub(super) const SNAPSHOTS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_assignment_execution_snapshots (snapshot_id BLOB PRIMARY KEY NOT NULL CHECK(length(snapshot_id)=16 AND snapshot_id!=zeroblob(16)), request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32 AND payload_digest!=zeroblob(32)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id!=zeroblob(16)), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), node_id BLOB NOT NULL CHECK(length(node_id)=16 AND node_id!=zeroblob(16)), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16 AND assignment_id!=zeroblob(16)), attempt INTEGER NOT NULL CHECK(attempt>=1), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id!=zeroblob(16)), profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id!=zeroblob(16)), profile_revision INTEGER NOT NULL CHECK(profile_revision>=1), resolved_engine TEXT NOT NULL CHECK(resolved_engine IN('codex','agy')), resolved_model TEXT NOT NULL CHECK(length(trim(resolved_model))>=1 AND length(CAST(resolved_model AS BLOB)) BETWEEN 1 AND 128), resolved_reasoning TEXT CHECK(resolved_reasoning IS NULL OR (length(trim(resolved_reasoning))>=1 AND length(CAST(resolved_reasoning AS BLOB)) BETWEEN 1 AND 64)), role_prompt_digest BLOB CHECK(role_prompt_digest IS NULL OR (length(role_prompt_digest)=32 AND role_prompt_digest!=zeroblob(32))), worktree_path TEXT NOT NULL CHECK(length(worktree_path) BETWEEN 2 AND 4096 AND length(CAST(worktree_path AS BLOB)) BETWEEN 2 AND 4096 AND substr(worktree_path,1,1)='/' AND substr(worktree_path,1,27)!='/Volumes/256.SSD/RustBuild/' AND worktree_path!='/Volumes/256.SSD/RustBuild'), lease_id BLOB NOT NULL CHECK(length(lease_id)=16 AND lease_id!=zeroblob(16)), lease_revision INTEGER NOT NULL CHECK(lease_revision>=1), lease_expires_at_ms INTEGER NOT NULL CHECK(lease_expires_at_ms>=0), mcp_grant_token_id BLOB CHECK(mcp_grant_token_id IS NULL OR (length(mcp_grant_token_id)=16 AND mcp_grant_token_id!=zeroblob(16))), mcp_grant_issued_at_ms INTEGER CHECK(mcp_grant_issued_at_ms IS NULL OR mcp_grant_issued_at_ms>=0), mcp_grant_expires_at_ms INTEGER CHECK(mcp_grant_expires_at_ms IS NULL OR mcp_grant_expires_at_ms>=0), allow_workspace_write INTEGER NOT NULL CHECK(allow_workspace_write IN(0,1)), allow_local_shell INTEGER NOT NULL CHECK(allow_local_shell IN(0,1)), allow_network INTEGER NOT NULL CHECK(allow_network IN(0,1)), startup_timeout_ms INTEGER NOT NULL CHECK(startup_timeout_ms BETWEEN 1 AND 86400000), execution_timeout_ms INTEGER NOT NULL CHECK(execution_timeout_ms BETWEEN 1 AND 86400000), termination_grace_ms INTEGER NOT NULL CHECK(termination_grace_ms BETWEEN 1 AND 86400000), committed_at_ms INTEGER NOT NULL CHECK(committed_at_ms>=0), UNIQUE(workspace_id,goal_id,dag_revision,node_id,assignment_id,attempt), CHECK((mcp_grant_token_id IS NULL AND mcp_grant_issued_at_ms IS NULL AND mcp_grant_expires_at_ms IS NULL) OR (mcp_grant_token_id IS NOT NULL AND mcp_grant_issued_at_ms IS NOT NULL AND mcp_grant_expires_at_ms IS NOT NULL AND mcp_grant_expires_at_ms>mcp_grant_issued_at_ms)))",
)
.as_str();

pub(super) const LAUNCH_LEDGER_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_assignment_launch_ledger (snapshot_id BLOB PRIMARY KEY NOT NULL CHECK(length(snapshot_id)=16 AND snapshot_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id!=zeroblob(16)), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), node_id BLOB NOT NULL CHECK(length(node_id)=16 AND node_id!=zeroblob(16)), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16 AND assignment_id!=zeroblob(16)), attempt INTEGER NOT NULL CHECK(attempt>=1), state TEXT NOT NULL CHECK(state IN('PREPARED','NOT_STARTED','SPAWNED','EXITED','UNKNOWN')), pid INTEGER CHECK(pid IS NULL OR pid>=1), pgid INTEGER CHECK(pgid IS NULL OR pgid>=1), process_start_time TEXT CHECK(process_start_time IS NULL OR (length(trim(process_start_time))>=1 AND length(CAST(process_start_time AS BLOB)) BETWEEN 1 AND 256)), boot_id TEXT CHECK(boot_id IS NULL OR (length(trim(boot_id))>=1 AND length(CAST(boot_id AS BLOB)) BETWEEN 1 AND 256)), exit_code INTEGER, unknown_reason TEXT CHECK(unknown_reason IS NULL OR unknown_reason IN('BOOT_RECOVERY_AFTER_SPAWN','EXECUTION_TIMED_OUT','OWNERSHIP_MISMATCH','FORCED_TERMINATION')), state_recorded_at_ms INTEGER CHECK(state_recorded_at_ms IS NULL OR state_recorded_at_ms>=0), revision INTEGER NOT NULL CHECK(revision>=1), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0), UNIQUE(workspace_id,goal_id,dag_revision,node_id,assignment_id,attempt), FOREIGN KEY(snapshot_id) REFERENCES ops_assignment_execution_snapshots(snapshot_id), CHECK((pid IS NULL AND pgid IS NULL AND process_start_time IS NULL AND boot_id IS NULL) OR (pid IS NOT NULL AND pgid IS NOT NULL AND process_start_time IS NOT NULL AND boot_id IS NOT NULL)), CHECK((state='PREPARED' AND pid IS NULL AND exit_code IS NULL AND unknown_reason IS NULL AND state_recorded_at_ms IS NULL) OR (state='NOT_STARTED' AND pid IS NULL AND exit_code IS NULL AND unknown_reason IS NULL AND state_recorded_at_ms IS NOT NULL) OR (state='SPAWNED' AND pid IS NOT NULL AND exit_code IS NULL AND unknown_reason IS NULL AND state_recorded_at_ms IS NOT NULL) OR (state='EXITED' AND pid IS NOT NULL AND exit_code IS NOT NULL AND unknown_reason IS NULL AND state_recorded_at_ms IS NOT NULL) OR (state='UNKNOWN' AND exit_code IS NULL AND unknown_reason IS NOT NULL AND state_recorded_at_ms IS NOT NULL)))",
)
.as_str();

pub(super) const LAUNCH_LEDGER_STATE_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_assignment_launch_ledger_state_idx ON ops_assignment_launch_ledger(state,workspace_id)",
)
.as_str();

pub(super) const SNAPSHOTS_NO_UPDATE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_assignment_execution_snapshots_no_update BEFORE UPDATE ON ops_assignment_execution_snapshots BEGIN SELECT RAISE(ABORT,'assignment execution snapshots are immutable'); END",
)
.as_str();

pub(super) const SNAPSHOTS_NO_DELETE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_assignment_execution_snapshots_no_delete BEFORE DELETE ON ops_assignment_execution_snapshots BEGIN SELECT RAISE(ABORT,'assignment execution snapshots are immutable'); END",
)
.as_str();

pub(super) const LAUNCH_LEDGER_NO_DELETE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_assignment_launch_ledger_no_delete BEFORE DELETE ON ops_assignment_launch_ledger BEGIN SELECT RAISE(ABORT,'assignment launch ledger rows cannot be deleted'); END",
)
.as_str();

pub(super) const EXISTS_SNAPSHOT_OR_ATTEMPT_SQL: &str = StaticSql::dml(
    "SELECT EXISTS(SELECT 1 FROM ops_assignment_execution_snapshots WHERE snapshot_id=?1 OR (workspace_id=?2 AND goal_id=?3 AND dag_revision=?4 AND node_id=?5 AND assignment_id=?6 AND attempt=?7))",
)
.as_str();

pub(super) const INSERT_EXECUTION_SNAPSHOT_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_assignment_execution_snapshots(snapshot_id,request_id,payload_digest,workspace_id,goal_id,dag_revision,node_id,assignment_id,attempt,activity_id,profile_id,profile_revision,resolved_engine,resolved_model,resolved_reasoning,role_prompt_digest,worktree_path,lease_id,lease_revision,lease_expires_at_ms,mcp_grant_token_id,mcp_grant_issued_at_ms,mcp_grant_expires_at_ms,allow_workspace_write,allow_local_shell,allow_network,startup_timeout_ms,execution_timeout_ms,termination_grace_ms,committed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30)",
)
.as_str();

pub(super) const INSERT_INITIAL_LAUNCH_LEDGER_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_assignment_launch_ledger(snapshot_id,workspace_id,goal_id,dag_revision,node_id,assignment_id,attempt,state,pid,pgid,process_start_time,boot_id,exit_code,unknown_reason,state_recorded_at_ms,revision,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,'PREPARED',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,?8)",
)
.as_str();

pub(super) const UPDATE_LAUNCH_LEDGER_STATE_SQL: &str = StaticSql::dml(
    "UPDATE ops_assignment_launch_ledger SET state=?2,pid=?3,pgid=?4,process_start_time=?5,boot_id=?6,exit_code=?7,unknown_reason=?8,state_recorded_at_ms=?9,revision=?10,updated_at_ms=?11 WHERE snapshot_id=?1 AND revision=?12",
)
.as_str();

pub(super) const SELECT_LEDGER_BY_ATTEMPT_SQL: &str = StaticSql::dml(
    "SELECT substr(snapshot_id,1,17),substr(workspace_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(assignment_id,1,17),attempt,state,pid,pgid,substr(process_start_time,1,257),substr(boot_id,1,257),exit_code,unknown_reason,state_recorded_at_ms,revision,updated_at_ms FROM ops_assignment_launch_ledger WHERE workspace_id=?1 AND goal_id=?2 AND dag_revision=?3 AND node_id=?4 AND assignment_id=?5 AND attempt=?6",
)
.as_str();

pub(super) const SELECT_LEDGER_BY_SNAPSHOT_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(snapshot_id,1,17),substr(workspace_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(assignment_id,1,17),attempt,state,pid,pgid,substr(process_start_time,1,257),substr(boot_id,1,257),exit_code,unknown_reason,state_recorded_at_ms,revision,updated_at_ms FROM ops_assignment_launch_ledger WHERE snapshot_id=?1",
)
.as_str();

pub(super) const SELECT_SNAPSHOT_BY_ATTEMPT_SQL: &str = StaticSql::dml(
    "SELECT substr(snapshot_id,1,17),substr(payload_digest,1,33),substr(workspace_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(assignment_id,1,17),attempt,substr(activity_id,1,17),substr(profile_id,1,17),profile_revision,resolved_engine,substr(resolved_model,1,129),substr(resolved_reasoning,1,65),substr(role_prompt_digest,1,33),substr(worktree_path,1,4097),substr(lease_id,1,17),lease_revision,lease_expires_at_ms,substr(mcp_grant_token_id,1,17),mcp_grant_issued_at_ms,mcp_grant_expires_at_ms,allow_workspace_write,allow_local_shell,allow_network,startup_timeout_ms,execution_timeout_ms,termination_grace_ms,committed_at_ms FROM ops_assignment_execution_snapshots WHERE workspace_id=?1 AND goal_id=?2 AND dag_revision=?3 AND node_id=?4 AND assignment_id=?5 AND attempt=?6",
)
.as_str();

pub(super) const SELECT_SNAPSHOT_BY_REQUEST_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(snapshot_id,1,17),substr(payload_digest,1,33),substr(workspace_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(assignment_id,1,17),attempt,substr(activity_id,1,17),substr(profile_id,1,17),profile_revision,resolved_engine,substr(resolved_model,1,129),substr(resolved_reasoning,1,65),substr(role_prompt_digest,1,33),substr(worktree_path,1,4097),substr(lease_id,1,17),lease_revision,lease_expires_at_ms,substr(mcp_grant_token_id,1,17),mcp_grant_issued_at_ms,mcp_grant_expires_at_ms,allow_workspace_write,allow_local_shell,allow_network,startup_timeout_ms,execution_timeout_ms,termination_grace_ms,committed_at_ms FROM ops_assignment_execution_snapshots WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_IN_FLIGHT_LEDGERS_BY_WORKSPACE_SQL: &str = StaticSql::dml(
    "SELECT substr(snapshot_id,1,17),substr(workspace_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(assignment_id,1,17),attempt,state,pid,pgid,substr(process_start_time,1,257),substr(boot_id,1,257),exit_code,unknown_reason,state_recorded_at_ms,revision,updated_at_ms FROM ops_assignment_launch_ledger WHERE state IN('PREPARED','SPAWNED') AND workspace_id=?1 ORDER BY workspace_id ASC, goal_id ASC, dag_revision ASC, node_id ASC, assignment_id ASC, attempt ASC LIMIT ?2",
)
.as_str();

pub(super) const SELECT_IN_FLIGHT_LEDGERS_ALL_SQL: &str = StaticSql::dml(
    "SELECT substr(snapshot_id,1,17),substr(workspace_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(assignment_id,1,17),attempt,state,pid,pgid,substr(process_start_time,1,257),substr(boot_id,1,257),exit_code,unknown_reason,state_recorded_at_ms,revision,updated_at_ms FROM ops_assignment_launch_ledger WHERE state IN('PREPARED','SPAWNED') ORDER BY workspace_id ASC, goal_id ASC, dag_revision ASC, node_id ASC, assignment_id ASC, attempt ASC LIMIT ?1",
)
.as_str();
