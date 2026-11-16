//! Domain SQL definitions for Swarm runs, channels, members, two-phase watermarks,
//! doorbells, and lineage-separated adversarial verifier panels.

use crate::static_sql::StaticSql;

pub(super) const SWARM_RUNS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_runs (swarm_run_id BLOB PRIMARY KEY NOT NULL CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32 AND payload_digest!=zeroblob(32)), title TEXT NOT NULL CHECK(length(trim(title))>=1 AND length(CAST(title AS BLOB)) BETWEEN 1 AND 256), mode_kind INTEGER NOT NULL CHECK(mode_kind IN(0,1)), goal_id BLOB CHECK(goal_id IS NULL OR (length(goal_id)=16 AND goal_id!=zeroblob(16))), dag_revision INTEGER CHECK(dag_revision IS NULL OR dag_revision>=1), node_id BLOB CHECK(node_id IS NULL OR (length(node_id)=16 AND node_id!=zeroblob(16))), declared_write_manifest_blake3 BLOB CHECK(declared_write_manifest_blake3 IS NULL OR (length(declared_write_manifest_blake3)=32 AND declared_write_manifest_blake3!=zeroblob(32))), state TEXT NOT NULL CHECK(state IN('ACTIVE','CONVERGING','CANDIDATE_READY','SETTLED','ABORTED')), max_members INTEGER NOT NULL CHECK(max_members BETWEEN 1 AND 256), max_spawn_depth INTEGER NOT NULL CHECK(max_spawn_depth BETWEEN 0 AND 4), max_total_turns INTEGER NOT NULL CHECK(max_total_turns BETWEEN 1 AND 100000), total_turns_used INTEGER NOT NULL DEFAULT 0 CHECK(total_turns_used>=0 AND total_turns_used<=max_total_turns), required_verifier_quorum INTEGER NOT NULL CHECK(required_verifier_quorum BETWEEN 1 AND 16 AND required_verifier_quorum<=max_members), allow_worker_all_mention INTEGER NOT NULL CHECK(allow_worker_all_mention IN(0,1)), baseline_commit_oid TEXT NOT NULL CHECK(length(CAST(baseline_commit_oid AS BLOB)) BETWEEN 40 AND 64), baseline_tree_blake3 BLOB NOT NULL CHECK(length(baseline_tree_blake3)=32 AND baseline_tree_blake3!=zeroblob(32)), converged_commit_oid TEXT CHECK(converged_commit_oid IS NULL OR length(CAST(converged_commit_oid AS BLOB)) BETWEEN 40 AND 64), converged_tree_blake3 BLOB CHECK(converged_tree_blake3 IS NULL OR (length(converged_tree_blake3)=32 AND converged_tree_blake3!=zeroblob(32))), converged_diff_digest BLOB CHECK(converged_diff_digest IS NULL OR (length(converged_diff_digest)=32 AND converged_diff_digest!=zeroblob(32))), stall_nudge_count INTEGER NOT NULL DEFAULT 0 CHECK(stall_nudge_count>=0), settlement_request_id BLOB CHECK(settlement_request_id IS NULL OR (length(settlement_request_id)=16 AND settlement_request_id!=zeroblob(16))), revision INTEGER NOT NULL CHECK(revision>=1), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=created_at_ms), CHECK((mode_kind=0 AND goal_id IS NULL AND dag_revision IS NULL AND node_id IS NULL AND declared_write_manifest_blake3 IS NULL) OR (mode_kind=1 AND goal_id IS NOT NULL AND dag_revision IS NOT NULL AND node_id IS NOT NULL AND declared_write_manifest_blake3 IS NOT NULL)), CHECK(NOT(mode_kind=1 AND state='SETTLED')), CHECK((converged_commit_oid IS NULL AND converged_tree_blake3 IS NULL AND converged_diff_digest IS NULL) OR (converged_commit_oid IS NOT NULL AND converged_tree_blake3 IS NOT NULL AND converged_diff_digest IS NOT NULL)), CHECK((state IN('ACTIVE','CONVERGING') AND converged_commit_oid IS NULL) OR (state IN('CANDIDATE_READY','SETTLED') AND converged_commit_oid IS NOT NULL) OR state='ABORTED'), CHECK((state='SETTLED' AND settlement_request_id IS NOT NULL) OR (state!='SETTLED' AND settlement_request_id IS NULL)))",
)
.as_str();

pub(super) const SWARM_RUNS_WORKSPACE_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_swarm_runs_workspace_idx ON ops_swarm_runs(workspace_id,state,created_at_ms,swarm_run_id)",
)
.as_str();

pub(super) const SWARM_MEMBERS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_members (member_id BLOB PRIMARY KEY NOT NULL CHECK(length(member_id)=16 AND member_id!=zeroblob(16)), swarm_run_id BLOB NOT NULL REFERENCES ops_swarm_runs(swarm_run_id) CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), roster_name TEXT NOT NULL CHECK(length(trim(roster_name))>=1 AND length(CAST(roster_name AS BLOB)) BETWEEN 1 AND 128), role_kind TEXT NOT NULL CHECK(role_kind IN('COORDINATOR','WORKER','HELPER','VERIFIER','MONITOR')), parent_member_id BLOB CHECK(parent_member_id IS NULL OR (length(parent_member_id)=16 AND parent_member_id!=zeroblob(16))), spawn_depth INTEGER NOT NULL CHECK(spawn_depth BETWEEN 0 AND 4), workspace_mode TEXT NOT NULL CHECK(workspace_mode IN('INHERIT','SHARE','BRANCH')), worktree_path TEXT CHECK(worktree_path IS NULL OR (length(worktree_path) BETWEEN 2 AND 4096 AND length(CAST(worktree_path AS BLOB)) BETWEEN 2 AND 4096 AND substr(worktree_path,1,1)='/' AND substr(worktree_path,1,27)!='/Volumes/256.SSD/RustBuild/' AND worktree_path!='/Volumes/256.SSD/RustBuild')), member_state TEXT NOT NULL CHECK(member_state IN('WAITING','WAKE_QUEUED','WORKING','SUSPENDED_QUOTA','STOPPED','KILLED')), profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id!=zeroblob(16)), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id!=zeroblob(16)), principal_kind INTEGER NOT NULL CHECK(principal_kind IN(0,1,2)), principal_id BLOB CHECK((principal_kind=0 AND principal_id IS NULL) OR (principal_kind IN(1,2) AND principal_id IS NOT NULL AND length(principal_id)=16 AND principal_id!=zeroblob(16))), committed_seen_seq INTEGER NOT NULL DEFAULT 0 CHECK(committed_seen_seq>=0), inflight_target_seq INTEGER NOT NULL DEFAULT 0 CHECK(inflight_target_seq>=committed_seen_seq), active_snapshot_id BLOB CHECK(active_snapshot_id IS NULL OR (length(active_snapshot_id)=16 AND active_snapshot_id!=zeroblob(16))), attempt_epoch INTEGER NOT NULL DEFAULT 0 CHECK(attempt_epoch>=0), turns_used INTEGER NOT NULL DEFAULT 0 CHECK(turns_used>=0), ping_pong_peer_id BLOB CHECK(ping_pong_peer_id IS NULL OR (length(ping_pong_peer_id)=16 AND ping_pong_peer_id!=zeroblob(16))), ping_pong_streak INTEGER NOT NULL DEFAULT 0 CHECK(ping_pong_streak>=0), last_output_commit_oid TEXT CHECK(last_output_commit_oid IS NULL OR length(CAST(last_output_commit_oid AS BLOB)) BETWEEN 40 AND 64), last_output_tree_blake3 BLOB CHECK(last_output_tree_blake3 IS NULL OR (length(last_output_tree_blake3)=32 AND last_output_tree_blake3!=zeroblob(32))), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=created_at_ms), UNIQUE(swarm_run_id,roster_name), UNIQUE(swarm_run_id,member_id), FOREIGN KEY(swarm_run_id,parent_member_id) REFERENCES ops_swarm_members(swarm_run_id,member_id), CHECK((parent_member_id IS NULL AND spawn_depth=0) OR (parent_member_id IS NOT NULL AND spawn_depth BETWEEN 1 AND 4)), CHECK(role_kind!='COORDINATOR' OR (parent_member_id IS NULL AND spawn_depth=0)), CHECK(role_kind!='VERIFIER' OR workspace_mode='INHERIT'), CHECK((member_state='WORKING' AND active_snapshot_id IS NOT NULL AND inflight_target_seq>committed_seen_seq) OR (member_state!='WORKING' AND active_snapshot_id IS NULL AND inflight_target_seq=committed_seen_seq)), CHECK(workspace_mode!='INHERIT' OR (last_output_commit_oid IS NULL AND last_output_tree_blake3 IS NULL)), CHECK((last_output_commit_oid IS NULL AND last_output_tree_blake3 IS NULL) OR (last_output_commit_oid IS NOT NULL AND last_output_tree_blake3 IS NOT NULL)))",
)
.as_str();

pub(super) const SWARM_MEMBERS_COORDINATOR_IDX_SQL: &str = StaticSql::ddl(
    "CREATE UNIQUE INDEX ops_swarm_members_coordinator_idx ON ops_swarm_members(swarm_run_id) WHERE role_kind='COORDINATOR'",
)
.as_str();

pub(super) const SWARM_MEMBERS_STATE_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_swarm_members_state_idx ON ops_swarm_members(swarm_run_id,member_state,member_id)",
)
.as_str();

pub(super) const SWARM_MEMBERS_PARENT_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_swarm_members_parent_idx ON ops_swarm_members(swarm_run_id,parent_member_id)",
)
.as_str();

pub(super) const SWARM_CHANNELS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_channels (channel_id BLOB PRIMARY KEY NOT NULL CHECK(length(channel_id)=16 AND channel_id!=zeroblob(16)), swarm_run_id BLOB NOT NULL REFERENCES ops_swarm_runs(swarm_run_id) CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), name TEXT NOT NULL CHECK(length(trim(name))>=1 AND length(CAST(name AS BLOB)) BETWEEN 1 AND 64), purpose TEXT CHECK(purpose IS NULL OR (length(trim(purpose))>=1 AND length(CAST(purpose AS BLOB)) BETWEEN 1 AND 512)), created_by_member_id BLOB CHECK(created_by_member_id IS NULL OR (length(created_by_member_id)=16 AND created_by_member_id!=zeroblob(16))), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), UNIQUE(swarm_run_id,name), UNIQUE(swarm_run_id,channel_id), FOREIGN KEY(swarm_run_id,created_by_member_id) REFERENCES ops_swarm_members(swarm_run_id,member_id))",
)
.as_str();

pub(super) const SWARM_CHANNEL_SUBSCRIPTIONS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_channel_subscriptions (swarm_run_id BLOB NOT NULL CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), channel_id BLOB NOT NULL CHECK(length(channel_id)=16 AND channel_id!=zeroblob(16)), member_id BLOB NOT NULL CHECK(length(member_id)=16 AND member_id!=zeroblob(16)), subscribed_at_ms INTEGER NOT NULL CHECK(subscribed_at_ms>=0), PRIMARY KEY(channel_id,member_id), FOREIGN KEY(swarm_run_id,channel_id) REFERENCES ops_swarm_channels(swarm_run_id,channel_id), FOREIGN KEY(swarm_run_id,member_id) REFERENCES ops_swarm_members(swarm_run_id,member_id))",
)
.as_str();

pub(super) const SWARM_CHANNEL_SUBSCRIPTIONS_MEMBER_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_swarm_channel_subscriptions_member_idx ON ops_swarm_channel_subscriptions(swarm_run_id,member_id,channel_id)",
)
.as_str();

pub(super) const SWARM_CHANNEL_MESSAGES_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_channel_messages (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), sequence INTEGER NOT NULL CHECK(sequence>=1), swarm_run_id BLOB NOT NULL CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), channel_id BLOB NOT NULL CHECK(length(channel_id)=16 AND channel_id!=zeroblob(16)), author_member_id BLOB CHECK(author_member_id IS NULL OR (length(author_member_id)=16 AND author_member_id!=zeroblob(16))), author_epoch INTEGER CHECK(author_epoch IS NULL OR author_epoch>=0), PRIMARY KEY(workspace_id,sequence), FOREIGN KEY(workspace_id,sequence) REFERENCES ops_workspace_messages(workspace_id,sequence), FOREIGN KEY(swarm_run_id,channel_id) REFERENCES ops_swarm_channels(swarm_run_id,channel_id), FOREIGN KEY(swarm_run_id,author_member_id) REFERENCES ops_swarm_members(swarm_run_id,member_id), CHECK((author_member_id IS NULL AND author_epoch IS NULL) OR (author_member_id IS NOT NULL AND author_epoch IS NOT NULL)))",
)
.as_str();

pub(super) const SWARM_CHANNEL_MESSAGES_RUN_SEQ_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_swarm_channel_messages_run_seq_idx ON ops_swarm_channel_messages(swarm_run_id,sequence,channel_id)",
)
.as_str();

pub(super) const SWARM_DOORBELLS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_doorbells (swarm_run_id BLOB NOT NULL CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), target_member_id BLOB NOT NULL CHECK(length(target_member_id)=16 AND target_member_id!=zeroblob(16)), sequence INTEGER NOT NULL CHECK(sequence>=1), reason TEXT NOT NULL CHECK(reason IN('DIRECT_MENTION','CHANNEL_MENTION','ALL_MENTION','SYSTEM_STALL','SYSTEM_LIVELOCK','MANUAL_WAKE')), enqueued_at_ms INTEGER NOT NULL CHECK(enqueued_at_ms>=0), PRIMARY KEY(swarm_run_id,target_member_id,sequence), FOREIGN KEY(swarm_run_id,target_member_id) REFERENCES ops_swarm_members(swarm_run_id,member_id))",
)
.as_str();

pub(super) const SWARM_DOORBELLS_PENDING_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_swarm_doorbells_pending_idx ON ops_swarm_doorbells(swarm_run_id,enqueued_at_ms,target_member_id,sequence)",
)
.as_str();

pub(super) const SWARM_VERIFICATION_PANELS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_verification_panels (panel_id BLOB PRIMARY KEY NOT NULL CHECK(length(panel_id)=16 AND panel_id!=zeroblob(16)), request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32 AND payload_digest!=zeroblob(32)), swarm_run_id BLOB NOT NULL REFERENCES ops_swarm_runs(swarm_run_id) CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), required_quorum INTEGER NOT NULL CHECK(required_quorum BETWEEN 1 AND 16), auto_created INTEGER NOT NULL CHECK(auto_created IN(0,1)), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), UNIQUE(swarm_run_id,panel_id))",
)
.as_str();

pub(super) const SWARM_VERIFICATION_PANELS_RUN_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_swarm_verification_panels_run_idx ON ops_swarm_verification_panels(swarm_run_id,created_at_ms,panel_id)",
)
.as_str();

pub(super) const SWARM_VERIFICATION_PANEL_SEATS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_verification_panel_seats (panel_id BLOB NOT NULL CHECK(length(panel_id)=16 AND panel_id!=zeroblob(16)), swarm_run_id BLOB NOT NULL CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), member_id BLOB NOT NULL CHECK(length(member_id)=16 AND member_id!=zeroblob(16)), seat_kind TEXT NOT NULL CHECK(seat_kind IN('AUTHOR','VERIFIER')), assigned_at_ms INTEGER NOT NULL CHECK(assigned_at_ms>=0), PRIMARY KEY(panel_id,seat_kind,member_id), FOREIGN KEY(swarm_run_id,panel_id) REFERENCES ops_swarm_verification_panels(swarm_run_id,panel_id), FOREIGN KEY(swarm_run_id,member_id) REFERENCES ops_swarm_members(swarm_run_id,member_id))",
)
.as_str();

pub(super) const SWARM_VERDICTS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_swarm_verdicts (verdict_id BLOB PRIMARY KEY NOT NULL CHECK(length(verdict_id)=16 AND verdict_id!=zeroblob(16)), request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32 AND payload_digest!=zeroblob(32)), panel_id BLOB NOT NULL CHECK(length(panel_id)=16 AND panel_id!=zeroblob(16)), swarm_run_id BLOB NOT NULL CHECK(length(swarm_run_id)=16 AND swarm_run_id!=zeroblob(16)), verifier_member_id BLOB NOT NULL CHECK(length(verifier_member_id)=16 AND verifier_member_id!=zeroblob(16)), outcome TEXT NOT NULL CHECK(outcome IN('ACCEPTED','REJECTED','NEEDS_CHANGES')), reviewed_diff_digest BLOB NOT NULL CHECK(length(reviewed_diff_digest)=32 AND reviewed_diff_digest!=zeroblob(32)), rationale TEXT NOT NULL CHECK(length(trim(rationale))>=1 AND length(CAST(rationale AS BLOB)) BETWEEN 1 AND 8192), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), UNIQUE(panel_id,verifier_member_id,reviewed_diff_digest), FOREIGN KEY(swarm_run_id,panel_id) REFERENCES ops_swarm_verification_panels(swarm_run_id,panel_id), FOREIGN KEY(swarm_run_id,verifier_member_id) REFERENCES ops_swarm_members(swarm_run_id,member_id))",
)
.as_str();

pub(super) const SWARM_VERDICTS_PANEL_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_swarm_verdicts_panel_idx ON ops_swarm_verdicts(panel_id,recorded_at_ms,verdict_id)",
)
.as_str();

pub(super) const SWARM_VERDICTS_NO_UPDATE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_swarm_verdicts_no_update BEFORE UPDATE ON ops_swarm_verdicts BEGIN SELECT RAISE(ABORT,'swarm verdicts are immutable'); END",
)
.as_str();

pub(super) const SWARM_VERDICTS_NO_DELETE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_swarm_verdicts_no_delete BEFORE DELETE ON ops_swarm_verdicts BEGIN SELECT RAISE(ABORT,'swarm verdicts are immutable'); END",
)
.as_str();

// --- DML: Swarm Runs ---

pub(super) const INSERT_SWARM_RUN_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_swarm_runs(swarm_run_id,workspace_id,request_id,payload_digest,title,mode_kind,goal_id,dag_revision,node_id,declared_write_manifest_blake3,state,max_members,max_spawn_depth,max_total_turns,total_turns_used,required_verifier_quorum,allow_worker_all_mention,baseline_commit_oid,baseline_tree_blake3,converged_commit_oid,converged_tree_blake3,converged_diff_digest,stall_nudge_count,settlement_request_id,revision,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'ACTIVE',?11,?12,?13,0,?14,?15,?16,?17,NULL,NULL,NULL,0,NULL,1,?18,?18)",
)
.as_str();

pub(super) const SELECT_SWARM_RUN_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(request_id,1,17),substr(payload_digest,1,33),substr(CAST(title AS BLOB),1,257),mode_kind,substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(declared_write_manifest_blake3,1,33),state,max_members,max_spawn_depth,max_total_turns,total_turns_used,required_verifier_quorum,allow_worker_all_mention,substr(CAST(baseline_commit_oid AS BLOB),1,65),substr(baseline_tree_blake3,1,33),substr(CAST(converged_commit_oid AS BLOB),1,65),substr(converged_tree_blake3,1,33),substr(converged_diff_digest,1,33),stall_nudge_count,substr(settlement_request_id,1,17),revision,created_at_ms,updated_at_ms FROM ops_swarm_runs WHERE workspace_id=?1 AND swarm_run_id=?2",
)
.as_str();

pub(super) const SELECT_SWARM_RUN_BY_REQUEST_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(request_id,1,17),substr(payload_digest,1,33),substr(CAST(title AS BLOB),1,257),mode_kind,substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(declared_write_manifest_blake3,1,33),state,max_members,max_spawn_depth,max_total_turns,total_turns_used,required_verifier_quorum,allow_worker_all_mention,substr(CAST(baseline_commit_oid AS BLOB),1,65),substr(baseline_tree_blake3,1,33),substr(CAST(converged_commit_oid AS BLOB),1,65),substr(converged_tree_blake3,1,33),substr(converged_diff_digest,1,33),stall_nudge_count,substr(settlement_request_id,1,17),revision,created_at_ms,updated_at_ms FROM ops_swarm_runs WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_SWARM_RUNS_BY_WORKSPACE_SQL: &str = StaticSql::dml(
    "SELECT substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(request_id,1,17),substr(payload_digest,1,33),substr(CAST(title AS BLOB),1,257),mode_kind,substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(declared_write_manifest_blake3,1,33),state,max_members,max_spawn_depth,max_total_turns,total_turns_used,required_verifier_quorum,allow_worker_all_mention,substr(CAST(baseline_commit_oid AS BLOB),1,65),substr(baseline_tree_blake3,1,33),substr(CAST(converged_commit_oid AS BLOB),1,65),substr(converged_tree_blake3,1,33),substr(converged_diff_digest,1,33),stall_nudge_count,substr(settlement_request_id,1,17),revision,created_at_ms,updated_at_ms FROM ops_swarm_runs WHERE workspace_id=?1 ORDER BY created_at_ms DESC, swarm_run_id ASC LIMIT ?2",
)
.as_str();

pub(super) const UPDATE_SWARM_RUN_CONVERGING_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_runs SET state='CONVERGING',revision=revision+1,updated_at_ms=?3 WHERE workspace_id=?1 AND swarm_run_id=?2 AND state='ACTIVE' AND revision=?4",
)
.as_str();

pub(super) const UPDATE_SWARM_RUN_CANDIDATE_READY_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_runs SET state='CANDIDATE_READY',converged_commit_oid=?3,converged_tree_blake3=?4,converged_diff_digest=?5,revision=revision+1,updated_at_ms=?6 WHERE workspace_id=?1 AND swarm_run_id=?2 AND state IN('ACTIVE','CONVERGING','CANDIDATE_READY') AND revision=?7",
)
.as_str();

pub(super) const UPDATE_SWARM_RUN_SETTLED_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_runs SET state='SETTLED',settlement_request_id=?3,revision=revision+1,updated_at_ms=?4 WHERE workspace_id=?1 AND swarm_run_id=?2 AND mode_kind=0 AND state='CANDIDATE_READY' AND revision=?5",
)
.as_str();

pub(super) const UPDATE_SWARM_RUN_ABORTED_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_runs SET state='ABORTED',revision=revision+1,updated_at_ms=?3 WHERE workspace_id=?1 AND swarm_run_id=?2 AND state IN('ACTIVE','CONVERGING','CANDIDATE_READY') AND revision=?4",
)
.as_str();

pub(super) const INCREMENT_SWARM_RUN_TURNS_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_runs SET total_turns_used=total_turns_used+1,revision=revision+1,updated_at_ms=?3 WHERE workspace_id=?1 AND swarm_run_id=?2 AND total_turns_used<max_total_turns",
)
.as_str();

pub(super) const INCREMENT_SWARM_RUN_STALL_NUDGE_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_runs SET stall_nudge_count=stall_nudge_count+1,revision=revision+1,updated_at_ms=?3 WHERE workspace_id=?1 AND swarm_run_id=?2",
)
.as_str();

// --- DML: Swarm Channels & Subscriptions ---

pub(super) const INSERT_SWARM_CHANNEL_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_swarm_channels(channel_id,swarm_run_id,workspace_id,name,purpose,created_by_member_id,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
)
.as_str();

pub(super) const SELECT_SWARM_CHANNEL_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(channel_id,1,17),substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(CAST(name AS BLOB),1,65),substr(CAST(purpose AS BLOB),1,513),substr(created_by_member_id,1,17),created_at_ms FROM ops_swarm_channels WHERE swarm_run_id=?1 AND channel_id=?2",
)
.as_str();

pub(super) const SELECT_SWARM_CHANNEL_BY_NAME_SQL: &str = StaticSql::dml(
    "SELECT substr(channel_id,1,17),substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(CAST(name AS BLOB),1,65),substr(CAST(purpose AS BLOB),1,513),substr(created_by_member_id,1,17),created_at_ms FROM ops_swarm_channels WHERE swarm_run_id=?1 AND name=?2",
)
.as_str();

pub(super) const SELECT_SWARM_CHANNELS_BY_RUN_SQL: &str = StaticSql::dml(
    "SELECT substr(channel_id,1,17),substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(CAST(name AS BLOB),1,65),substr(CAST(purpose AS BLOB),1,513),substr(created_by_member_id,1,17),created_at_ms FROM ops_swarm_channels WHERE swarm_run_id=?1 ORDER BY created_at_ms ASC, name ASC LIMIT ?2",
)
.as_str();

pub(super) const INSERT_SWARM_CHANNEL_SUBSCRIPTION_SQL: &str = StaticSql::dml(
    "INSERT OR IGNORE INTO ops_swarm_channel_subscriptions(swarm_run_id,channel_id,member_id,subscribed_at_ms) VALUES(?1,?2,?3,?4)",
)
.as_str();

pub(super) const SELECT_SWARM_SUBSCRIPTIONS_BY_RUN_SQL: &str = StaticSql::dml(
    "SELECT substr(channel_id,1,17),substr(member_id,1,17) FROM ops_swarm_channel_subscriptions WHERE swarm_run_id=?1 ORDER BY channel_id ASC, member_id ASC LIMIT ?2",
)
.as_str();

pub(super) const SELECT_SWARM_CHANNEL_SUBSCRIBER_IDS_SQL: &str = StaticSql::dml(
    "SELECT substr(s.member_id,1,17) FROM ops_swarm_channel_subscriptions AS s JOIN ops_swarm_members AS m ON m.swarm_run_id=s.swarm_run_id AND m.member_id=s.member_id WHERE s.swarm_run_id=?1 AND s.channel_id=?2 AND m.member_state!='KILLED' ORDER BY s.member_id ASC LIMIT ?3",
)
.as_str();

// --- DML: Swarm Members & Two-Phase Watermarks ---

pub(super) const COUNT_ACTIVE_SWARM_MEMBERS_SQL: &str = StaticSql::dml(
    "SELECT COUNT(*) FROM ops_swarm_members WHERE swarm_run_id=?1 AND member_state!='KILLED'",
)
.as_str();

pub(super) const COUNT_TOTAL_SWARM_MEMBERS_SQL: &str =
    StaticSql::dml("SELECT COUNT(*) FROM ops_swarm_members WHERE swarm_run_id=?1").as_str();

pub(super) const COUNT_WORKING_SWARM_MEMBERS_SQL: &str = StaticSql::dml(
    "SELECT COUNT(*) FROM ops_swarm_members WHERE swarm_run_id=?1 AND member_state='WORKING'",
)
.as_str();

pub(super) const INSERT_SWARM_MEMBER_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_swarm_members(member_id,swarm_run_id,workspace_id,roster_name,role_kind,parent_member_id,spawn_depth,workspace_mode,worktree_path,member_state,profile_id,participant_id,principal_kind,principal_id,committed_seen_seq,inflight_target_seq,active_snapshot_id,attempt_epoch,turns_used,ping_pong_peer_id,ping_pong_streak,last_output_commit_oid,last_output_tree_blake3,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'WAITING',?10,?11,?12,?13,0,0,NULL,0,0,NULL,0,NULL,NULL,?14,?14)",
)
.as_str();

pub(super) const SELECT_SWARM_MEMBER_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(member_id,1,17),substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(CAST(roster_name AS BLOB),1,129),role_kind,substr(parent_member_id,1,17),spawn_depth,workspace_mode,substr(CAST(worktree_path AS BLOB),1,4097),member_state,substr(profile_id,1,17),substr(participant_id,1,17),principal_kind,substr(principal_id,1,17),committed_seen_seq,inflight_target_seq,substr(active_snapshot_id,1,17),attempt_epoch,turns_used,substr(ping_pong_peer_id,1,17),ping_pong_streak,substr(CAST(last_output_commit_oid AS BLOB),1,65),substr(last_output_tree_blake3,1,33),created_at_ms,updated_at_ms FROM ops_swarm_members WHERE swarm_run_id=?1 AND member_id=?2",
)
.as_str();

pub(super) const SELECT_SWARM_MEMBER_BY_ROSTER_SQL: &str = StaticSql::dml(
    "SELECT substr(member_id,1,17),substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(CAST(roster_name AS BLOB),1,129),role_kind,substr(parent_member_id,1,17),spawn_depth,workspace_mode,substr(CAST(worktree_path AS BLOB),1,4097),member_state,substr(profile_id,1,17),substr(participant_id,1,17),principal_kind,substr(principal_id,1,17),committed_seen_seq,inflight_target_seq,substr(active_snapshot_id,1,17),attempt_epoch,turns_used,substr(ping_pong_peer_id,1,17),ping_pong_streak,substr(CAST(last_output_commit_oid AS BLOB),1,65),substr(last_output_tree_blake3,1,33),created_at_ms,updated_at_ms FROM ops_swarm_members WHERE swarm_run_id=?1 AND roster_name=?2",
)
.as_str();

pub(super) const SELECT_SWARM_MEMBERS_BY_RUN_SQL: &str = StaticSql::dml(
    "SELECT substr(member_id,1,17),substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(CAST(roster_name AS BLOB),1,129),role_kind,substr(parent_member_id,1,17),spawn_depth,workspace_mode,substr(CAST(worktree_path AS BLOB),1,4097),member_state,substr(profile_id,1,17),substr(participant_id,1,17),principal_kind,substr(principal_id,1,17),committed_seen_seq,inflight_target_seq,substr(active_snapshot_id,1,17),attempt_epoch,turns_used,substr(ping_pong_peer_id,1,17),ping_pong_streak,substr(CAST(last_output_commit_oid AS BLOB),1,65),substr(last_output_tree_blake3,1,33),created_at_ms,updated_at_ms FROM ops_swarm_members WHERE swarm_run_id=?1 ORDER BY spawn_depth ASC, created_at_ms ASC, member_id ASC LIMIT ?2",
)
.as_str();

pub(super) const SELECT_WAKE_QUEUED_SWARM_MEMBERS_SQL: &str = StaticSql::dml(
    "SELECT substr(member_id,1,17),substr(swarm_run_id,1,17),substr(workspace_id,1,17),substr(CAST(roster_name AS BLOB),1,129),role_kind,substr(parent_member_id,1,17),spawn_depth,workspace_mode,substr(CAST(worktree_path AS BLOB),1,4097),member_state,substr(profile_id,1,17),substr(participant_id,1,17),principal_kind,substr(principal_id,1,17),committed_seen_seq,inflight_target_seq,substr(active_snapshot_id,1,17),attempt_epoch,turns_used,substr(ping_pong_peer_id,1,17),ping_pong_streak,substr(CAST(last_output_commit_oid AS BLOB),1,65),substr(last_output_tree_blake3,1,33),created_at_ms,updated_at_ms FROM ops_swarm_members WHERE swarm_run_id=?1 AND member_state='WAKE_QUEUED' ORDER BY spawn_depth DESC, updated_at_ms ASC, member_id ASC LIMIT ?2",
)
.as_str();

pub(super) const UPDATE_SWARM_MEMBER_WAKE_QUEUED_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_members SET member_state='WAKE_QUEUED',updated_at_ms=?3 WHERE swarm_run_id=?1 AND member_id=?2 AND member_state='WAITING'",
)
.as_str();

pub(super) const UPDATE_SWARM_MEMBER_CLAIM_WORKING_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_members SET member_state='WORKING',active_snapshot_id=?3,inflight_target_seq=?4,attempt_epoch=attempt_epoch+1,updated_at_ms=?5 WHERE swarm_run_id=?1 AND member_id=?2 AND member_state='WAKE_QUEUED' AND active_snapshot_id IS NULL",
)
.as_str();

pub(super) const UPDATE_SWARM_MEMBER_COMPLETE_TURN_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_members SET member_state=?4,committed_seen_seq=?5,inflight_target_seq=?5,active_snapshot_id=NULL,turns_used=turns_used+1,ping_pong_peer_id=?6,ping_pong_streak=?7,last_output_commit_oid=?8,last_output_tree_blake3=?9,updated_at_ms=?10 WHERE swarm_run_id=?1 AND member_id=?2 AND member_state='WORKING' AND active_snapshot_id=?3",
)
.as_str();

pub(super) const UPDATE_SWARM_MEMBER_FAIL_OR_SUSPEND_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_members SET member_state=?4,inflight_target_seq=committed_seen_seq,active_snapshot_id=NULL,turns_used=turns_used+?5,updated_at_ms=?6 WHERE swarm_run_id=?1 AND member_id=?2 AND member_state='WORKING' AND active_snapshot_id=?3",
)
.as_str();

pub(super) const UPDATE_SWARM_MEMBER_PROFILE_REQUEUE_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_members SET profile_id=?3,member_state=?4,attempt_epoch=attempt_epoch+1,updated_at_ms=?5 WHERE swarm_run_id=?1 AND member_id=?2 AND member_state='SUSPENDED_QUOTA'",
)
.as_str();

pub(super) const UPDATE_SWARM_MEMBER_PING_PONG_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_members SET ping_pong_peer_id=?3,ping_pong_streak=?4,updated_at_ms=?5 WHERE swarm_run_id=?1 AND member_id=?2 AND member_state!='KILLED'",
)
.as_str();

pub(super) const UPDATE_SWARM_MEMBER_KILL_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_members SET member_state='KILLED',inflight_target_seq=committed_seen_seq,active_snapshot_id=NULL,updated_at_ms=?3 WHERE swarm_run_id=?1 AND member_id=?2 AND member_state!='KILLED'",
)
.as_str();

pub(super) const UPDATE_SWARM_MEMBER_STOP_SQL: &str = StaticSql::dml(
    "UPDATE ops_swarm_members SET member_state='STOPPED',inflight_target_seq=committed_seen_seq,active_snapshot_id=NULL,updated_at_ms=?3 WHERE swarm_run_id=?1 AND member_id=?2 AND member_state IN('WAITING','WAKE_QUEUED','SUSPENDED_QUOTA')",
)
.as_str();

// --- DML: Messages & Doorbells ---

pub(super) const SELECT_NEXT_WORKSPACE_MESSAGE_SEQUENCE_SQL: &str = StaticSql::dml(
    "SELECT COALESCE(MAX(sequence),0) FROM ops_workspace_messages WHERE workspace_id=?1",
)
.as_str();

pub(super) const INSERT_SWARM_WORKSPACE_MESSAGE_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,channel_participation_id,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms,activity_id,goal_id,dag_revision,node_id,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_profile_id,provider_source_participation_id,provider_source_label,goal_created_at_ms,dag_base_goal_revision) VALUES(?1,?2,0,NULL,?3,NULL,NULL,NULL,NULL,?4,0,?5,?6,?7,?8,?9,?10,?11,NULL,NULL,NULL,NULL,NULL,NULL,?12,?13)",
)
.as_str();

pub(super) const INSERT_SWARM_CHANNEL_MESSAGE_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_swarm_channel_messages(workspace_id,sequence,swarm_run_id,channel_id,author_member_id,author_epoch) VALUES(?1,?2,?3,?4,?5,?6)",
)
.as_str();

pub(super) const SELECT_SWARM_MESSAGE_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(wm.message_id,1,17),substr(wm.workspace_id,1,17),wm.sequence,substr(cm.swarm_run_id,1,17),substr(cm.channel_id,1,17),substr(CAST(ch.name AS BLOB),1,65),substr(cm.author_member_id,1,17),substr(CAST(wm.author_display_name AS BLOB),1,129),cm.author_epoch,substr(CAST(wm.body AS BLOB),1,16385),wm.submitted_at_ms FROM ops_workspace_messages AS wm LEFT JOIN ops_swarm_channel_messages AS cm ON cm.workspace_id=wm.workspace_id AND cm.sequence=wm.sequence LEFT JOIN ops_swarm_channels AS ch ON ch.swarm_run_id=cm.swarm_run_id AND ch.channel_id=cm.channel_id WHERE wm.message_id=?1",
)
.as_str();

pub(super) const SELECT_SWARM_BOARD_SINCE_ALL_CHANNELS_SQL: &str = StaticSql::dml(
    "SELECT substr(wm.message_id,1,17),substr(wm.workspace_id,1,17),wm.sequence,substr(cm.swarm_run_id,1,17),substr(cm.channel_id,1,17),substr(CAST(ch.name AS BLOB),1,65),substr(cm.author_member_id,1,17),substr(CAST(wm.author_display_name AS BLOB),1,129),cm.author_epoch,substr(CAST(wm.body AS BLOB),1,16385),wm.submitted_at_ms FROM ops_swarm_channel_messages AS cm JOIN ops_workspace_messages AS wm ON wm.workspace_id=cm.workspace_id AND wm.sequence=cm.sequence JOIN ops_swarm_channels AS ch ON ch.swarm_run_id=cm.swarm_run_id AND ch.channel_id=cm.channel_id WHERE cm.workspace_id=?1 AND cm.swarm_run_id=?2 AND cm.sequence>?3 ORDER BY cm.sequence ASC LIMIT ?4",
)
.as_str();

pub(super) const SELECT_SWARM_BOARD_SINCE_CHANNEL_SQL: &str = StaticSql::dml(
    "SELECT substr(wm.message_id,1,17),substr(wm.workspace_id,1,17),wm.sequence,substr(cm.swarm_run_id,1,17),substr(cm.channel_id,1,17),substr(CAST(ch.name AS BLOB),1,65),substr(cm.author_member_id,1,17),substr(CAST(wm.author_display_name AS BLOB),1,129),cm.author_epoch,substr(CAST(wm.body AS BLOB),1,16385),wm.submitted_at_ms FROM ops_swarm_channel_messages AS cm JOIN ops_workspace_messages AS wm ON wm.workspace_id=cm.workspace_id AND wm.sequence=cm.sequence JOIN ops_swarm_channels AS ch ON ch.swarm_run_id=cm.swarm_run_id AND ch.channel_id=cm.channel_id WHERE cm.workspace_id=?1 AND cm.swarm_run_id=?2 AND cm.channel_id=?3 AND cm.sequence>?4 ORDER BY cm.sequence ASC LIMIT ?5",
)
.as_str();

pub(super) const SELECT_SWARM_WAKEUP_MESSAGES_SQL: &str = StaticSql::dml(
    "SELECT substr(wm.message_id,1,17),substr(wm.workspace_id,1,17),wm.sequence,substr(cm.swarm_run_id,1,17),substr(cm.channel_id,1,17),substr(CAST(ch.name AS BLOB),1,65),substr(cm.author_member_id,1,17),substr(CAST(wm.author_display_name AS BLOB),1,129),cm.author_epoch,substr(CAST(wm.body AS BLOB),1,16385),wm.submitted_at_ms FROM ops_swarm_channel_messages AS cm JOIN ops_workspace_messages AS wm ON wm.workspace_id=cm.workspace_id AND wm.sequence=cm.sequence JOIN ops_swarm_channels AS ch ON ch.swarm_run_id=cm.swarm_run_id AND ch.channel_id=cm.channel_id WHERE cm.swarm_run_id=?1 AND cm.sequence>?2 AND (EXISTS(SELECT 1 FROM ops_swarm_channel_subscriptions AS sub WHERE sub.swarm_run_id=cm.swarm_run_id AND sub.channel_id=cm.channel_id AND sub.member_id=?3) OR EXISTS(SELECT 1 FROM ops_swarm_doorbells AS db WHERE db.swarm_run_id=cm.swarm_run_id AND db.target_member_id=?3 AND db.sequence=cm.sequence)) ORDER BY cm.sequence ASC LIMIT ?4",
)
.as_str();

pub(super) const INSERT_SWARM_DOORBELL_SQL: &str = StaticSql::dml(
    "INSERT OR IGNORE INTO ops_swarm_doorbells(swarm_run_id,target_member_id,sequence,reason,enqueued_at_ms) VALUES(?1,?2,?3,?4,?5)",
)
.as_str();

pub(super) const EXISTS_PENDING_SWARM_DOORBELL_AFTER_SQL: &str = StaticSql::dml(
    "SELECT EXISTS(SELECT 1 FROM ops_swarm_doorbells WHERE swarm_run_id=?1 AND target_member_id=?2 AND sequence>?3)",
)
.as_str();

pub(super) const DELETE_CONSUMED_SWARM_DOORBELLS_SQL: &str = StaticSql::dml(
    "DELETE FROM ops_swarm_doorbells WHERE swarm_run_id=?1 AND target_member_id=?2 AND sequence<=?3",
)
.as_str();

pub(super) const SELECT_SWARM_DOORBELLS_BY_RUN_SQL: &str = StaticSql::dml(
    "SELECT substr(swarm_run_id,1,17),substr(target_member_id,1,17),sequence,reason,enqueued_at_ms FROM ops_swarm_doorbells WHERE swarm_run_id=?1 ORDER BY enqueued_at_ms ASC, target_member_id ASC, sequence ASC LIMIT ?2",
)
.as_str();

// --- DML: Verification Panels & Verdicts ---

pub(super) const INSERT_SWARM_VERIFICATION_PANEL_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_swarm_verification_panels(panel_id,request_id,payload_digest,swarm_run_id,workspace_id,required_quorum,auto_created,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
)
.as_str();

pub(super) const INSERT_SWARM_PANEL_SEAT_SQL: &str = StaticSql::dml(
    "INSERT OR IGNORE INTO ops_swarm_verification_panel_seats(panel_id,swarm_run_id,member_id,seat_kind,assigned_at_ms) VALUES(?1,?2,?3,?4,?5)",
)
.as_str();

pub(super) const SELECT_SWARM_PANEL_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(panel_id,1,17),substr(request_id,1,17),substr(payload_digest,1,33),substr(swarm_run_id,1,17),substr(workspace_id,1,17),required_quorum,auto_created,created_at_ms FROM ops_swarm_verification_panels WHERE swarm_run_id=?1 AND panel_id=?2",
)
.as_str();

pub(super) const SELECT_SWARM_PANEL_BY_REQUEST_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(panel_id,1,17),substr(request_id,1,17),substr(payload_digest,1,33),substr(swarm_run_id,1,17),substr(workspace_id,1,17),required_quorum,auto_created,created_at_ms FROM ops_swarm_verification_panels WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_LATEST_SWARM_PANEL_BY_RUN_SQL: &str = StaticSql::dml(
    "SELECT substr(panel_id,1,17),substr(request_id,1,17),substr(payload_digest,1,33),substr(swarm_run_id,1,17),substr(workspace_id,1,17),required_quorum,auto_created,created_at_ms FROM ops_swarm_verification_panels WHERE swarm_run_id=?1 ORDER BY created_at_ms DESC, panel_id DESC LIMIT 1",
)
.as_str();

pub(super) const SELECT_SWARM_PANEL_SEATS_SQL: &str = StaticSql::dml(
    "SELECT substr(member_id,1,17),seat_kind,assigned_at_ms FROM ops_swarm_verification_panel_seats WHERE panel_id=?1 ORDER BY seat_kind ASC, assigned_at_ms ASC, member_id ASC LIMIT ?2",
)
.as_str();

pub(super) const INSERT_SWARM_VERDICT_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_swarm_verdicts(verdict_id,request_id,payload_digest,panel_id,swarm_run_id,verifier_member_id,outcome,reviewed_diff_digest,rationale,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
)
.as_str();

pub(super) const SELECT_SWARM_VERDICT_BY_REQUEST_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(verdict_id,1,17),substr(request_id,1,17),substr(payload_digest,1,33),substr(panel_id,1,17),substr(swarm_run_id,1,17),substr(verifier_member_id,1,17),outcome,substr(reviewed_diff_digest,1,33),substr(CAST(rationale AS BLOB),1,8193),recorded_at_ms FROM ops_swarm_verdicts WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_SWARM_VERDICTS_BY_PANEL_SQL: &str = StaticSql::dml(
    "SELECT substr(verdict_id,1,17),substr(request_id,1,17),substr(payload_digest,1,33),substr(panel_id,1,17),substr(swarm_run_id,1,17),substr(verifier_member_id,1,17),outcome,substr(reviewed_diff_digest,1,33),substr(CAST(rationale AS BLOB),1,8193),recorded_at_ms FROM ops_swarm_verdicts WHERE panel_id=?1 ORDER BY recorded_at_ms ASC, verdict_id ASC LIMIT ?2",
)
.as_str();
