//! Domain SQL definitions for `DagSettled` projection and governance links.

use crate::static_sql::StaticSql;

pub(crate) const DAG_SETTLED_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_dag_settlement_projection (migration_id BLOB NOT NULL REFERENCES ops_effect_fact_consumers(migration_id) CHECK(length(migration_id)=16), source_sequence INTEGER NOT NULL CHECK(source_sequence>=1), fact_digest BLOB NOT NULL CHECK(length(fact_digest)=32), settlement_id BLOB NOT NULL CHECK(length(settlement_id)=16 AND settlement_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id != zeroblob(16)), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id != zeroblob(16)), goal_revision INTEGER NOT NULL CHECK(goal_revision>=1), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), settled_file_count INTEGER NOT NULL CHECK(settled_file_count>=0), settled_tree_digest BLOB NOT NULL CHECK(length(settled_tree_digest)=32 AND settled_tree_digest != zeroblob(32)), coordinator_agent_id BLOB NOT NULL CHECK(length(coordinator_agent_id)=16 AND coordinator_agent_id != zeroblob(16)), coordinator_session_id BLOB CHECK(coordinator_session_id IS NULL OR (length(coordinator_session_id)=16 AND coordinator_session_id != zeroblob(16))), settled_at_ms INTEGER NOT NULL CHECK(settled_at_ms>=0), PRIMARY KEY(migration_id,source_sequence), UNIQUE(migration_id,settlement_id), UNIQUE(migration_id,goal_id,dag_revision))",
)
.as_str();

pub(crate) const DAG_SETTLEMENT_GOVERNANCE_LINKS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_dag_settlement_governance_links (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id != zeroblob(16)), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), migration_id BLOB NOT NULL REFERENCES ops_effect_fact_consumers(migration_id) CHECK(length(migration_id)=16), source_sequence INTEGER NOT NULL CHECK(source_sequence>=1), settlement_id BLOB NOT NULL CHECK(length(settlement_id)=16 AND settlement_id != zeroblob(16)), coordinator_agent_id BLOB NOT NULL CHECK(length(coordinator_agent_id)=16 AND coordinator_agent_id != zeroblob(16)), coordinator_participant_id BLOB CHECK(coordinator_participant_id IS NULL OR (length(coordinator_participant_id)=16 AND coordinator_participant_id != zeroblob(16))), workspace_leadership_revision INTEGER CHECK(workspace_leadership_revision IS NULL OR workspace_leadership_revision>=1), accepted_work_credit_count INTEGER NOT NULL CHECK(accepted_work_credit_count>=0), evaluated_assignment_count INTEGER NOT NULL CHECK(evaluated_assignment_count>=0), started_mission_plan_id BLOB CHECK(started_mission_plan_id IS NULL OR (length(started_mission_plan_id)=16 AND started_mission_plan_id != zeroblob(16))), started_mission_plan_revision INTEGER CHECK(started_mission_plan_revision IS NULL OR started_mission_plan_revision>=1), settled_at_ms INTEGER NOT NULL CHECK(settled_at_ms>=0), PRIMARY KEY(workspace_id,goal_id,dag_revision), FOREIGN KEY(migration_id,source_sequence) REFERENCES ops_dag_settlement_projection(migration_id,source_sequence), CHECK((started_mission_plan_id IS NULL AND started_mission_plan_revision IS NULL) OR (started_mission_plan_id IS NOT NULL AND started_mission_plan_revision IS NOT NULL)))",
)
.as_str();

pub(crate) const EXISTS_DAG_SETTLEMENT_IDENTITY_SQL: &str = StaticSql::dml(
    "SELECT EXISTS(SELECT 1 FROM ops_dag_settlement_projection WHERE migration_id=?1 AND (settlement_id=?2 OR (goal_id=?3 AND dag_revision=?4)))",
)
.as_str();

pub(crate) const INSERT_DAG_SETTLEMENT_PROJECTION_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_dag_settlement_projection(migration_id,source_sequence,fact_digest,settlement_id,workspace_id,activity_id,goal_id,goal_revision,dag_revision,settled_file_count,settled_tree_digest,coordinator_agent_id,coordinator_session_id,settled_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
)
.as_str();

pub(crate) const SELECT_GOVERNANCE_LINK_SETTLEMENT_SQL: &str = StaticSql::dml(
    "SELECT substr(settlement_id,1,17) FROM ops_dag_settlement_governance_links WHERE workspace_id=?1 AND goal_id=?2 AND dag_revision=?3",
)
.as_str();

pub(crate) const SELECT_COORDINATOR_PARTICIPANT_SQL: &str = StaticSql::dml(
    "SELECT substr(participant_id,1,17) FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3",
)
.as_str();

pub(crate) const SELECT_ACTIVE_WORKSPACE_LEADERSHIP_REVISION_SQL: &str = StaticSql::dml(
    "SELECT leadership_revision FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=0 AND activity_id IS NULL AND team_id IS NULL AND state IN('STABILIZING','ACTIVE')",
)
.as_str();

pub(crate) const COUNT_ACCEPTED_WORK_CREDITS_SQL: &str = StaticSql::dml(
    "SELECT COUNT(*) FROM ops_work_credit_entries WHERE workspace_id=?1 AND goal_id=?2",
)
.as_str();

pub(crate) const COUNT_EVALUATED_ASSIGNMENTS_SQL: &str = StaticSql::dml(
    "SELECT COUNT(DISTINCT assignment_id) FROM ops_assignment_review_projection WHERE workspace_id=?1 AND goal_id=?2 AND target_agent_id<>reviewer_agent_id",
)
.as_str();

pub(crate) const SELECT_LATEST_STARTED_MISSION_PLAN_SQL: &str = StaticSql::dml(
    "SELECT substr(plan_id,1,17),revision FROM ops_mission_evaluation_plan_starts WHERE workspace_id=?1 ORDER BY started_at_ms DESC, plan_id DESC LIMIT 1",
)
.as_str();

pub(crate) const INSERT_DAG_SETTLEMENT_GOVERNANCE_LINK_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_dag_settlement_governance_links(workspace_id,goal_id,dag_revision,migration_id,source_sequence,settlement_id,coordinator_agent_id,coordinator_participant_id,workspace_leadership_revision,accepted_work_credit_count,evaluated_assignment_count,started_mission_plan_id,started_mission_plan_revision,settled_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
)
.as_str();

pub(crate) const SELECT_DAG_SETTLEMENT_PROJECTION_PAGE_SQL: &str = StaticSql::dml(
    "SELECT substr(migration_id,1,17),source_sequence,substr(fact_digest,1,33),substr(settlement_id,1,17),substr(workspace_id,1,17),substr(activity_id,1,17),substr(goal_id,1,17),goal_revision,dag_revision,settled_file_count,substr(settled_tree_digest,1,33),substr(coordinator_agent_id,1,17),substr(coordinator_session_id,1,17),settled_at_ms FROM ops_dag_settlement_projection WHERE migration_id=?1 ORDER BY source_sequence ASC LIMIT ?2",
)
.as_str();

pub(crate) const SELECT_DAG_SETTLEMENT_GOVERNANCE_LINK_SQL: &str = StaticSql::dml(
    "SELECT substr(migration_id,1,17),source_sequence,substr(settlement_id,1,17),substr(coordinator_agent_id,1,17),substr(coordinator_participant_id,1,17),workspace_leadership_revision,accepted_work_credit_count,evaluated_assignment_count,substr(started_mission_plan_id,1,17),started_mission_plan_revision,settled_at_ms FROM ops_dag_settlement_governance_links WHERE workspace_id=?1 AND goal_id=?2 AND dag_revision=?3",
)
.as_str();

pub(crate) const SELECT_WORKSPACE_ORCHESTRATOR_AUTHORITY_SQL: &str = StaticSql::dml(
    "SELECT state,substr(leader_participant_id,1,17),leadership_revision FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=0 AND activity_id IS NULL AND team_id IS NULL",
)
.as_str();

pub(crate) const SELECT_TEAM_LEADER_AUTHORITY_SQL: &str = StaticSql::dml(
    "SELECT state,substr(leader_participant_id,1,17),leadership_revision FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=2 AND activity_id IS NULL AND team_id=?2",
)
.as_str();

pub(crate) const SELECT_CALLER_EFFECT_BINDING_PARTICIPANT_SQL: &str = StaticSql::dml(
    "SELECT substr(participant_id,1,17) FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3 AND session_id=?4",
)
.as_str();
