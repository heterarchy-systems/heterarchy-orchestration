//! Domain SQL definitions for independent reviewer assignment and verdict receipts.

use crate::static_sql::StaticSql;

pub(super) const ASSIGNMENTS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_reviewer_assignments (assignment_record_id BLOB PRIMARY KEY NOT NULL CHECK(length(assignment_record_id)=16 AND assignment_record_id!=zeroblob(16)), request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32 AND payload_digest!=zeroblob(32)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id!=zeroblob(16)), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), node_id BLOB NOT NULL CHECK(length(node_id)=16 AND node_id!=zeroblob(16)), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16 AND assignment_id!=zeroblob(16)), attempt INTEGER NOT NULL CHECK(attempt>=1), reviewer_participation_id BLOB NOT NULL CHECK(length(reviewer_participation_id)=16 AND reviewer_participation_id!=zeroblob(16)), reviewer_profile_id BLOB NOT NULL CHECK(length(reviewer_profile_id)=16 AND reviewer_profile_id!=zeroblob(16)), reviewer_participant_id BLOB NOT NULL CHECK(length(reviewer_participant_id)=16 AND reviewer_participant_id!=zeroblob(16)), reviewer_principal_kind INTEGER NOT NULL CHECK(reviewer_principal_kind IN(0,1,2)), reviewer_principal_id BLOB CHECK((reviewer_principal_kind=0 AND reviewer_principal_id IS NULL) OR (reviewer_principal_kind IN(1,2) AND reviewer_principal_id IS NOT NULL AND length(reviewer_principal_id)=16 AND reviewer_principal_id!=zeroblob(16))), executor_profile_id BLOB NOT NULL CHECK(length(executor_profile_id)=16 AND executor_profile_id!=zeroblob(16)), executor_participant_id BLOB NOT NULL CHECK(length(executor_participant_id)=16 AND executor_participant_id!=zeroblob(16)), executor_principal_kind INTEGER NOT NULL CHECK(executor_principal_kind IN(0,1,2)), executor_principal_id BLOB CHECK((executor_principal_kind=0 AND executor_principal_id IS NULL) OR (executor_principal_kind IN(1,2) AND executor_principal_id IS NOT NULL AND length(executor_principal_id)=16 AND executor_principal_id!=zeroblob(16))), leader_profile_id BLOB NOT NULL CHECK(length(leader_profile_id)=16 AND leader_profile_id!=zeroblob(16)), leader_participant_id BLOB NOT NULL CHECK(length(leader_participant_id)=16 AND leader_participant_id!=zeroblob(16)), leader_principal_kind INTEGER NOT NULL CHECK(leader_principal_kind IN(0,1,2)), leader_principal_id BLOB CHECK((leader_principal_kind=0 AND leader_principal_id IS NULL) OR (leader_principal_kind IN(1,2) AND leader_principal_id IS NOT NULL AND length(leader_principal_id)=16 AND leader_principal_id!=zeroblob(16))), qualification_domain TEXT CHECK(qualification_domain IS NULL OR (length(trim(qualification_domain))>=1 AND length(CAST(qualification_domain AS BLOB)) BETWEEN 1 AND 128)), rule_generation INTEGER CHECK((qualification_domain IS NULL AND rule_generation IS NULL) OR (qualification_domain IS NOT NULL AND rule_generation IS NOT NULL AND rule_generation>=1)), revision INTEGER NOT NULL CHECK(revision>=1), assigned_at_ms INTEGER NOT NULL CHECK(assigned_at_ms>=0), current_verdict_request_id BLOB CHECK((revision=1 AND current_verdict_request_id IS NULL) OR (revision>1 AND current_verdict_request_id IS NOT NULL AND length(current_verdict_request_id)=16 AND current_verdict_request_id!=zeroblob(16))), UNIQUE(workspace_id,goal_id,dag_revision,node_id,assignment_id,attempt), FOREIGN KEY(workspace_id,reviewer_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id), CHECK(reviewer_profile_id!=executor_profile_id AND reviewer_participant_id!=executor_participant_id AND NOT(reviewer_principal_kind=executor_principal_kind AND reviewer_principal_id IS executor_principal_id)), CHECK(reviewer_profile_id!=leader_profile_id AND reviewer_participant_id!=leader_participant_id AND NOT(reviewer_principal_kind=leader_principal_kind AND reviewer_principal_id IS leader_principal_id)))",
)
.as_str();

pub(super) const VERDICT_RECEIPTS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE ops_reviewer_verdict_receipts (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), assignment_record_id BLOB NOT NULL CHECK(length(assignment_record_id)=16 AND assignment_record_id!=zeroblob(16)), expected_revision INTEGER NOT NULL CHECK(expected_revision>=1), resulting_revision INTEGER NOT NULL CHECK(resulting_revision=expected_revision+1), outcome TEXT NOT NULL CHECK(outcome IN('ACCEPTED','REJECTED','NEEDS_CHANGES')), handoff_diff_digest BLOB NOT NULL CHECK(length(handoff_diff_digest)=32 AND handoff_diff_digest!=zeroblob(32)), rationale TEXT NOT NULL CHECK(length(trim(rationale))>=1 AND length(CAST(rationale AS BLOB)) BETWEEN 1 AND 8192), payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32 AND payload_digest!=zeroblob(32)), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), UNIQUE(assignment_record_id,resulting_revision), FOREIGN KEY(assignment_record_id) REFERENCES ops_reviewer_assignments(assignment_record_id))",
)
.as_str();

pub(super) const ASSIGNMENTS_WORKSPACE_IDX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX ops_reviewer_assignments_workspace_idx ON ops_reviewer_assignments(workspace_id,assigned_at_ms,assignment_record_id)",
)
.as_str();

pub(super) const ASSIGNMENTS_NO_DELETE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_reviewer_assignments_no_delete BEFORE DELETE ON ops_reviewer_assignments BEGIN SELECT RAISE(ABORT,'reviewer assignments cannot be deleted'); END",
)
.as_str();

pub(super) const VERDICT_RECEIPTS_NO_UPDATE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_reviewer_verdict_receipts_no_update BEFORE UPDATE ON ops_reviewer_verdict_receipts BEGIN SELECT RAISE(ABORT,'reviewer verdict receipts are immutable'); END",
)
.as_str();

pub(super) const VERDICT_RECEIPTS_NO_DELETE_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER ops_reviewer_verdict_receipts_no_delete BEFORE DELETE ON ops_reviewer_verdict_receipts BEGIN SELECT RAISE(ABORT,'reviewer verdict receipts are immutable'); END",
)
.as_str();

pub(super) const SELECT_ACTIVE_RULE_GENERATION_SQL: &str = StaticSql::dml(
    "SELECT g.generation FROM ops_reviewer_qualification_rule_heads AS h JOIN ops_reviewer_qualification_rule_generations AS g ON g.workspace_id=h.workspace_id AND g.rule_id=h.rule_id AND g.generation=h.current_generation WHERE h.workspace_id=?1 AND g.domain=?2 AND g.state=0 ORDER BY g.generation DESC, h.rule_id ASC LIMIT 1",
)
.as_str();

pub(super) const SELECT_REVIEWER_PARTICIPATION_HEAD_SQL: &str = StaticSql::dml(
    "SELECT substr(r.profile_id,1,17), r.statement FROM ops_reviewer_participation_heads AS h JOIN ops_reviewer_participation_receipts AS r ON r.workspace_id=h.workspace_id AND r.participation_id=h.participation_id AND r.revision=h.current_revision AND r.request_id=h.request_id WHERE h.workspace_id=?1 AND h.participation_id=?2",
)
.as_str();

pub(super) const SELECT_REVIEWER_SESSION_BINDING_SQL: &str = StaticSql::dml(
    "SELECT r.statement, substr(r.agent_id,1,17), substr(r.session_id,1,17) FROM ops_reviewer_participation_heads AS h JOIN ops_reviewer_participation_receipts AS r ON r.workspace_id=h.workspace_id AND r.participation_id=h.participation_id AND r.revision=h.current_revision AND r.request_id=h.request_id WHERE h.workspace_id=?1 AND h.participation_id=?2",
)
.as_str();

pub(super) const INSERT_REVIEWER_ASSIGNMENT_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_reviewer_assignments(assignment_record_id,request_id,payload_digest,workspace_id,goal_id,dag_revision,node_id,assignment_id,attempt,reviewer_participation_id,reviewer_profile_id,reviewer_participant_id,reviewer_principal_kind,reviewer_principal_id,executor_profile_id,executor_participant_id,executor_principal_kind,executor_principal_id,leader_profile_id,leader_participant_id,leader_principal_kind,leader_principal_id,qualification_domain,rule_generation,revision,assigned_at_ms,current_verdict_request_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,1,?25,NULL)",
)
.as_str();

pub(super) const INSERT_REVIEWER_VERDICT_RECEIPT_SQL: &str = StaticSql::dml(
    "INSERT INTO ops_reviewer_verdict_receipts(request_id,assignment_record_id,expected_revision,resulting_revision,outcome,handoff_diff_digest,rationale,payload_digest,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
)
.as_str();

pub(super) const UPDATE_REVIEWER_ASSIGNMENT_VERDICT_SQL: &str = StaticSql::dml(
    "UPDATE ops_reviewer_assignments SET revision=?2,current_verdict_request_id=?3 WHERE assignment_record_id=?1 AND revision=?4",
)
.as_str();

pub(super) const SELECT_ASSIGNMENT_BY_CORRELATION_SQL: &str = StaticSql::dml(
    "SELECT substr(a.assignment_record_id,1,17),substr(a.payload_digest,1,33),substr(a.workspace_id,1,17),substr(a.goal_id,1,17),a.dag_revision,substr(a.node_id,1,17),substr(a.assignment_id,1,17),a.attempt,substr(a.reviewer_participation_id,1,17),substr(a.reviewer_profile_id,1,17),substr(a.reviewer_participant_id,1,17),a.reviewer_principal_kind,substr(a.reviewer_principal_id,1,17),substr(a.executor_profile_id,1,17),substr(a.executor_participant_id,1,17),a.executor_principal_kind,substr(a.executor_principal_id,1,17),substr(a.leader_profile_id,1,17),substr(a.leader_participant_id,1,17),a.leader_principal_kind,substr(a.leader_principal_id,1,17),substr(a.qualification_domain,1,129),a.rule_generation,a.revision,a.assigned_at_ms,substr(a.current_verdict_request_id,1,17),substr(v.request_id,1,17),v.outcome,substr(v.handoff_diff_digest,1,33),substr(v.rationale,1,8193),v.recorded_at_ms FROM ops_reviewer_assignments AS a LEFT JOIN ops_reviewer_verdict_receipts AS v ON v.request_id=a.current_verdict_request_id AND v.assignment_record_id=a.assignment_record_id AND v.resulting_revision=a.revision WHERE a.workspace_id=?1 AND a.goal_id=?2 AND a.dag_revision=?3 AND a.node_id=?4 AND a.assignment_id=?5 AND a.attempt=?6",
)
.as_str();

pub(super) const SELECT_ASSIGNMENT_BY_RECORD_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(a.assignment_record_id,1,17),substr(a.payload_digest,1,33),substr(a.workspace_id,1,17),substr(a.goal_id,1,17),a.dag_revision,substr(a.node_id,1,17),substr(a.assignment_id,1,17),a.attempt,substr(a.reviewer_participation_id,1,17),substr(a.reviewer_profile_id,1,17),substr(a.reviewer_participant_id,1,17),a.reviewer_principal_kind,substr(a.reviewer_principal_id,1,17),substr(a.executor_profile_id,1,17),substr(a.executor_participant_id,1,17),a.executor_principal_kind,substr(a.executor_principal_id,1,17),substr(a.leader_profile_id,1,17),substr(a.leader_participant_id,1,17),a.leader_principal_kind,substr(a.leader_principal_id,1,17),substr(a.qualification_domain,1,129),a.rule_generation,a.revision,a.assigned_at_ms,substr(a.current_verdict_request_id,1,17),substr(v.request_id,1,17),v.outcome,substr(v.handoff_diff_digest,1,33),substr(v.rationale,1,8193),v.recorded_at_ms FROM ops_reviewer_assignments AS a LEFT JOIN ops_reviewer_verdict_receipts AS v ON v.request_id=a.current_verdict_request_id AND v.assignment_record_id=a.assignment_record_id AND v.resulting_revision=a.revision WHERE a.assignment_record_id=?1",
)
.as_str();

pub(super) const SELECT_ASSIGNMENT_BY_REQUEST_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(a.assignment_record_id,1,17),substr(a.payload_digest,1,33),substr(a.workspace_id,1,17),substr(a.goal_id,1,17),a.dag_revision,substr(a.node_id,1,17),substr(a.assignment_id,1,17),a.attempt,substr(a.reviewer_participation_id,1,17),substr(a.reviewer_profile_id,1,17),substr(a.reviewer_participant_id,1,17),a.reviewer_principal_kind,substr(a.reviewer_principal_id,1,17),substr(a.executor_profile_id,1,17),substr(a.executor_participant_id,1,17),a.executor_principal_kind,substr(a.executor_principal_id,1,17),substr(a.leader_profile_id,1,17),substr(a.leader_participant_id,1,17),a.leader_principal_kind,substr(a.leader_principal_id,1,17),substr(a.qualification_domain,1,129),a.rule_generation,a.revision,a.assigned_at_ms,substr(a.current_verdict_request_id,1,17),substr(v.request_id,1,17),v.outcome,substr(v.handoff_diff_digest,1,33),substr(v.rationale,1,8193),v.recorded_at_ms FROM ops_reviewer_assignments AS a LEFT JOIN ops_reviewer_verdict_receipts AS v ON v.request_id=a.current_verdict_request_id AND v.assignment_record_id=a.assignment_record_id AND v.resulting_revision=a.revision WHERE a.request_id=?1",
)
.as_str();

pub(super) const SELECT_VERDICT_RECEIPT_BY_REQUEST_ID_SQL: &str = StaticSql::dml(
    "SELECT substr(assignment_record_id,1,17),resulting_revision,outcome,substr(handoff_diff_digest,1,33),substr(rationale,1,8193),substr(payload_digest,1,33),recorded_at_ms FROM ops_reviewer_verdict_receipts WHERE request_id=?1",
)
.as_str();
