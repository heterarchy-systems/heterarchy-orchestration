use rusqlite::Connection;

use crate::OperationsError;

const REQUESTS: &str = "CREATE TABLE ops_mission_evaluation_requests (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), operation INTEGER NOT NULL CHECK(operation BETWEEN 0 AND 3), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), target_id BLOB NOT NULL CHECK(length(target_id)=16 AND target_id!=zeroblob(16)), revision INTEGER NOT NULL CHECK(revision>=1), actor_kind INTEGER NOT NULL CHECK(actor_kind IN(0,1,2)), actor_key TEXT NOT NULL CHECK(length(CAST(actor_key AS BLOB)) BETWEEN 1 AND 256), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), actor_participation_id BLOB CHECK(actor_participation_id IS NULL OR (length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16))), actor_profile_id BLOB CHECK(actor_profile_id IS NULL OR (length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16))), actor_participant_id BLOB CHECK(actor_participant_id IS NULL OR (length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16))), actor_session_id BLOB CHECK(actor_session_id IS NULL OR (length(actor_session_id)=16 AND actor_session_id!=zeroblob(16))), expected_leadership_revision INTEGER CHECK(expected_leadership_revision IS NULL OR expected_leadership_revision>=1), CHECK((actor_kind=0 AND actor_key='operator' AND actor_participation_id IS NULL AND actor_profile_id IS NULL AND actor_participant_id IS NULL AND actor_session_id IS NULL AND expected_leadership_revision IS NULL) OR (actor_kind=1 AND actor_participation_id IS NOT NULL AND actor_profile_id IS NOT NULL AND actor_participant_id IS NOT NULL AND actor_session_id IS NOT NULL AND expected_leadership_revision IS NOT NULL AND expected_leadership_revision>=1) OR (actor_kind=2 AND actor_participation_id IS NULL AND actor_profile_id IS NULL AND actor_participant_id IS NOT NULL AND actor_session_id IS NOT NULL AND expected_leadership_revision IS NOT NULL AND expected_leadership_revision>=1))) STRICT";

const MISSIONS: &str = "CREATE TABLE ops_mission_evaluation_missions (mission_id BLOB PRIMARY KEY NOT NULL CHECK(length(mission_id)=16 AND mission_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)), create_request_id BLOB NOT NULL UNIQUE REFERENCES ops_mission_evaluation_requests(request_id) CHECK(length(create_request_id)=16 AND create_request_id!=zeroblob(16)), purpose TEXT NOT NULL CHECK(length(CAST(purpose AS BLOB)) BETWEEN 1 AND 2048 AND length(trim(purpose))>0), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), revision INTEGER NOT NULL CHECK(revision>=1), UNIQUE(workspace_id,mission_id), UNIQUE(workspace_id,team_id,mission_id), FOREIGN KEY(workspace_id,team_id) REFERENCES ops_teams(workspace_id,team_id)) STRICT";

const TEAM_MISSION_HEADS: &str = "CREATE TABLE ops_mission_evaluation_team_heads (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)), mission_id BLOB NOT NULL CHECK(length(mission_id)=16 AND mission_id!=zeroblob(16)), revision INTEGER NOT NULL CHECK(revision>=1), PRIMARY KEY(workspace_id,team_id), UNIQUE(workspace_id,mission_id), FOREIGN KEY(workspace_id,team_id,mission_id) REFERENCES ops_mission_evaluation_missions(workspace_id,team_id,mission_id)) STRICT";

const OBLIGATIONS: &str = "CREATE TABLE ops_mission_evaluation_obligations (obligation_id BLOB PRIMARY KEY NOT NULL CHECK(length(obligation_id)=16 AND obligation_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), mission_id BLOB NOT NULL CHECK(length(mission_id)=16 AND mission_id!=zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id!=zeroblob(16)), work_unit_id BLOB NOT NULL CHECK(length(work_unit_id)=16 AND work_unit_id!=zeroblob(16)), record_request_id BLOB NOT NULL UNIQUE REFERENCES ops_mission_evaluation_requests(request_id) CHECK(length(record_request_id)=16 AND record_request_id!=zeroblob(16)), weight INTEGER NOT NULL CHECK(weight>0), acceptance_criteria TEXT NOT NULL CHECK(length(CAST(acceptance_criteria AS BLOB)) BETWEEN 1 AND 8192 AND length(trim(acceptance_criteria))>0), due_at_ms INTEGER NOT NULL CHECK(due_at_ms>=0), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), revision INTEGER NOT NULL CHECK(revision>=1), UNIQUE(workspace_id,mission_id,obligation_id), UNIQUE(workspace_id,mission_id,participation_id,work_unit_id), FOREIGN KEY(workspace_id,mission_id) REFERENCES ops_mission_evaluation_missions(workspace_id,mission_id), FOREIGN KEY(workspace_id,participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id)) STRICT";

const PLANS: &str = "CREATE TABLE ops_mission_evaluation_plans (plan_id BLOB PRIMARY KEY NOT NULL CHECK(length(plan_id)=16 AND plan_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), mission_id BLOB NOT NULL CHECK(length(mission_id)=16 AND mission_id!=zeroblob(16)), head_revision INTEGER NOT NULL CHECK(head_revision>=0), UNIQUE(plan_id,workspace_id,mission_id), FOREIGN KEY(workspace_id,mission_id) REFERENCES ops_mission_evaluation_missions(workspace_id,mission_id)) STRICT";

const PLAN_VERSIONS: &str = "CREATE TABLE ops_mission_evaluation_plan_versions (plan_id BLOB NOT NULL CHECK(length(plan_id)=16 AND plan_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), mission_id BLOB NOT NULL CHECK(length(mission_id)=16 AND mission_id!=zeroblob(16)), revision INTEGER NOT NULL CHECK(revision>=1), expected_head_revision INTEGER NOT NULL CHECK(expected_head_revision>=0 AND expected_head_revision<9223372036854775807), version_id BLOB NOT NULL UNIQUE CHECK(length(version_id)=16 AND version_id!=zeroblob(16)), request_id BLOB NOT NULL UNIQUE REFERENCES ops_mission_evaluation_requests(request_id) CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), t0_commit_at_ms INTEGER NOT NULL CHECK(t0_commit_at_ms>=0), t1_submission_cutoff_ms INTEGER NOT NULL CHECK(t1_submission_cutoff_ms>t0_commit_at_ms), t2_checkpoint_ms INTEGER NOT NULL CHECK(t2_checkpoint_ms>=t1_submission_cutoff_ms), cohort_count INTEGER NOT NULL CHECK(cohort_count BETWEEN 0 AND 256), sealed INTEGER NOT NULL CHECK(sealed IN(0,1)), PRIMARY KEY(plan_id,revision), UNIQUE(plan_id,revision,workspace_id,mission_id), CHECK(revision=expected_head_revision+1), FOREIGN KEY(plan_id,workspace_id,mission_id) REFERENCES ops_mission_evaluation_plans(plan_id,workspace_id,mission_id)) STRICT";

const PLAN_COHORT: &str = "CREATE TABLE ops_mission_evaluation_plan_cohort (plan_id BLOB NOT NULL CHECK(length(plan_id)=16 AND plan_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), mission_id BLOB NOT NULL CHECK(length(mission_id)=16 AND mission_id!=zeroblob(16)), revision INTEGER NOT NULL CHECK(revision>=1), ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 255), obligation_id BLOB NOT NULL CHECK(length(obligation_id)=16 AND obligation_id!=zeroblob(16)), weight_snapshot INTEGER NOT NULL CHECK(weight_snapshot>0), PRIMARY KEY(plan_id,revision,ordinal), UNIQUE(plan_id,revision,obligation_id), FOREIGN KEY(plan_id,revision,workspace_id,mission_id) REFERENCES ops_mission_evaluation_plan_versions(plan_id,revision,workspace_id,mission_id), FOREIGN KEY(workspace_id,mission_id,obligation_id) REFERENCES ops_mission_evaluation_obligations(workspace_id,mission_id,obligation_id)) STRICT";

const PLAN_STARTS: &str = "CREATE TABLE ops_mission_evaluation_plan_starts (plan_id BLOB PRIMARY KEY NOT NULL CHECK(length(plan_id)=16 AND plan_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), mission_id BLOB NOT NULL CHECK(length(mission_id)=16 AND mission_id!=zeroblob(16)), revision INTEGER NOT NULL CHECK(revision>=1), request_id BLOB NOT NULL UNIQUE REFERENCES ops_mission_evaluation_requests(request_id) CHECK(length(request_id)=16 AND request_id!=zeroblob(16)), started_at_ms INTEGER NOT NULL CHECK(started_at_ms>=0), FOREIGN KEY(plan_id,workspace_id,mission_id) REFERENCES ops_mission_evaluation_plans(plan_id,workspace_id,mission_id), FOREIGN KEY(plan_id,revision,workspace_id,mission_id) REFERENCES ops_mission_evaluation_plan_versions(plan_id,revision,workspace_id,mission_id)) STRICT";

const REQUESTS_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_mission_evaluation_requests_immutable_update BEFORE UPDATE ON ops_mission_evaluation_requests BEGIN SELECT RAISE(ABORT,'immutable mission evaluation request'); END";
const REQUESTS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_requests_immutable_delete BEFORE DELETE ON ops_mission_evaluation_requests BEGIN SELECT RAISE(ABORT,'immutable mission evaluation request'); END";
const MISSIONS_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_mission_evaluation_missions_immutable_update BEFORE UPDATE ON ops_mission_evaluation_missions BEGIN SELECT RAISE(ABORT,'immutable mission'); END";
const MISSIONS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_missions_immutable_delete BEFORE DELETE ON ops_mission_evaluation_missions BEGIN SELECT RAISE(ABORT,'immutable mission'); END";
const TEAM_MISSION_HEADS_UPDATE: &str = "CREATE TRIGGER ops_mission_evaluation_team_heads_update BEFORE UPDATE ON ops_mission_evaluation_team_heads BEGIN SELECT RAISE(ABORT,'mission Team head custody transition required'); END";
const TEAM_MISSION_HEADS_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_team_heads_delete BEFORE DELETE ON ops_mission_evaluation_team_heads BEGIN SELECT RAISE(ABORT,'mission Team head custody transition required'); END";
const OBLIGATIONS_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_mission_evaluation_obligations_immutable_update BEFORE UPDATE ON ops_mission_evaluation_obligations BEGIN SELECT RAISE(ABORT,'immutable mission obligation'); END";
const OBLIGATIONS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_obligations_immutable_delete BEFORE DELETE ON ops_mission_evaluation_obligations BEGIN SELECT RAISE(ABORT,'immutable mission obligation'); END";
const PLANS_HEAD_REVISION: &str = "CREATE TRIGGER ops_mission_evaluation_plans_head_revision BEFORE UPDATE ON ops_mission_evaluation_plans WHEN NEW.plan_id!=OLD.plan_id OR NEW.workspace_id!=OLD.workspace_id OR NEW.mission_id!=OLD.mission_id OR NEW.head_revision!=OLD.head_revision+1 OR NOT EXISTS(SELECT 1 FROM ops_mission_evaluation_plan_versions v WHERE v.plan_id=OLD.plan_id AND v.workspace_id=OLD.workspace_id AND v.mission_id=OLD.mission_id AND v.revision=NEW.head_revision AND v.expected_head_revision=OLD.head_revision AND v.sealed=1) OR EXISTS(SELECT 1 FROM ops_mission_evaluation_plan_starts WHERE plan_id=OLD.plan_id) BEGIN SELECT RAISE(ABORT,'mission evaluation plan head transition'); END";
const PLANS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_plans_immutable_delete BEFORE DELETE ON ops_mission_evaluation_plans BEGIN SELECT RAISE(ABORT,'immutable mission evaluation plan'); END";
const PLAN_VERSION_INSERT_GUARD: &str = "CREATE TRIGGER ops_mission_evaluation_plan_versions_insert_guard BEFORE INSERT ON ops_mission_evaluation_plan_versions WHEN NOT EXISTS(SELECT 1 FROM ops_mission_evaluation_plans p WHERE p.plan_id=NEW.plan_id AND p.workspace_id=NEW.workspace_id AND p.mission_id=NEW.mission_id AND p.head_revision=NEW.expected_head_revision) OR EXISTS(SELECT 1 FROM ops_mission_evaluation_plan_starts WHERE plan_id=NEW.plan_id) BEGIN SELECT RAISE(ABORT,'mission evaluation plan version head'); END";
const PLAN_VERSIONS_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_mission_evaluation_plan_versions_immutable_update BEFORE UPDATE ON ops_mission_evaluation_plan_versions WHEN OLD.sealed!=0 OR NEW.sealed!=1 OR NEW.plan_id!=OLD.plan_id OR NEW.workspace_id!=OLD.workspace_id OR NEW.mission_id!=OLD.mission_id OR NEW.revision!=OLD.revision OR NEW.expected_head_revision!=OLD.expected_head_revision OR NEW.version_id!=OLD.version_id OR NEW.request_id!=OLD.request_id OR NEW.t0_commit_at_ms!=OLD.t0_commit_at_ms OR NEW.t1_submission_cutoff_ms!=OLD.t1_submission_cutoff_ms OR NEW.t2_checkpoint_ms!=OLD.t2_checkpoint_ms OR NEW.cohort_count!=OLD.cohort_count OR (SELECT COUNT(*) FROM ops_mission_evaluation_plan_cohort c WHERE c.plan_id=NEW.plan_id AND c.revision=NEW.revision)!=NEW.cohort_count OR (NEW.cohort_count>0 AND ((SELECT MIN(ordinal) FROM ops_mission_evaluation_plan_cohort c WHERE c.plan_id=NEW.plan_id AND c.revision=NEW.revision)!=0 OR (SELECT MAX(ordinal) FROM ops_mission_evaluation_plan_cohort c WHERE c.plan_id=NEW.plan_id AND c.revision=NEW.revision)!=NEW.cohort_count-1)) OR EXISTS(SELECT 1 FROM ops_mission_evaluation_plan_cohort a JOIN ops_mission_evaluation_plan_cohort b ON b.plan_id=a.plan_id AND b.revision=a.revision AND a.ordinal<b.ordinal WHERE a.plan_id=NEW.plan_id AND a.revision=NEW.revision AND a.obligation_id>=b.obligation_id) OR EXISTS(SELECT 1 FROM ops_mission_evaluation_plan_cohort c JOIN ops_mission_evaluation_obligations o ON o.workspace_id=c.workspace_id AND o.mission_id=c.mission_id AND o.obligation_id=c.obligation_id WHERE c.plan_id=NEW.plan_id AND c.revision=NEW.revision AND c.weight_snapshot!=o.weight) BEGIN SELECT RAISE(ABORT,'mission evaluation plan version seal'); END";
const PLAN_VERSIONS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_plan_versions_immutable_delete BEFORE DELETE ON ops_mission_evaluation_plan_versions BEGIN SELECT RAISE(ABORT,'immutable mission evaluation plan version'); END";
const PLAN_COHORT_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_mission_evaluation_plan_cohort_immutable_update BEFORE UPDATE ON ops_mission_evaluation_plan_cohort BEGIN SELECT RAISE(ABORT,'immutable mission evaluation plan cohort'); END";
const PLAN_COHORT_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_plan_cohort_immutable_delete BEFORE DELETE ON ops_mission_evaluation_plan_cohort BEGIN SELECT RAISE(ABORT,'immutable mission evaluation plan cohort'); END";
const PLAN_COHORT_INSERT_GUARD: &str = "CREATE TRIGGER ops_mission_evaluation_plan_cohort_insert_guard BEFORE INSERT ON ops_mission_evaluation_plan_cohort WHEN NOT EXISTS(SELECT 1 FROM ops_mission_evaluation_plan_versions v WHERE v.plan_id=NEW.plan_id AND v.revision=NEW.revision AND v.workspace_id=NEW.workspace_id AND v.mission_id=NEW.mission_id AND v.sealed=0 AND NEW.ordinal<v.cohort_count) BEGIN SELECT RAISE(ABORT,'mission evaluation plan cohort bound'); END";
const PLAN_START_INSERT_GUARD: &str = "CREATE TRIGGER ops_mission_evaluation_plan_starts_insert_guard BEFORE INSERT ON ops_mission_evaluation_plan_starts WHEN NOT EXISTS(SELECT 1 FROM ops_mission_evaluation_plans p JOIN ops_mission_evaluation_plan_versions v ON v.plan_id=p.plan_id AND v.workspace_id=p.workspace_id AND v.mission_id=p.mission_id WHERE p.plan_id=NEW.plan_id AND p.workspace_id=NEW.workspace_id AND p.mission_id=NEW.mission_id AND p.head_revision=NEW.revision AND v.revision=NEW.revision AND v.sealed=1) BEGIN SELECT RAISE(ABORT,'mission evaluation plan start revision'); END";
const PLAN_STARTS_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_mission_evaluation_plan_starts_immutable_update BEFORE UPDATE ON ops_mission_evaluation_plan_starts BEGIN SELECT RAISE(ABORT,'immutable mission evaluation plan start'); END";
const PLAN_STARTS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_plan_starts_immutable_delete BEFORE DELETE ON ops_mission_evaluation_plan_starts BEGIN SELECT RAISE(ABORT,'immutable mission evaluation plan start'); END";

const OBJECTS: &[(&str, &str, &str)] = &[
    ("table", "ops_mission_evaluation_requests", REQUESTS),
    ("table", "ops_mission_evaluation_missions", MISSIONS),
    (
        "table",
        "ops_mission_evaluation_team_heads",
        TEAM_MISSION_HEADS,
    ),
    ("table", "ops_mission_evaluation_obligations", OBLIGATIONS),
    ("table", "ops_mission_evaluation_plans", PLANS),
    (
        "table",
        "ops_mission_evaluation_plan_versions",
        PLAN_VERSIONS,
    ),
    ("table", "ops_mission_evaluation_plan_cohort", PLAN_COHORT),
    ("table", "ops_mission_evaluation_plan_starts", PLAN_STARTS),
    (
        "trigger",
        "ops_mission_evaluation_requests_immutable_update",
        REQUESTS_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_requests_immutable_delete",
        REQUESTS_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_missions_immutable_update",
        MISSIONS_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_missions_immutable_delete",
        MISSIONS_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_team_heads_update",
        TEAM_MISSION_HEADS_UPDATE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_team_heads_delete",
        TEAM_MISSION_HEADS_DELETE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_obligations_immutable_update",
        OBLIGATIONS_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_obligations_immutable_delete",
        OBLIGATIONS_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plans_head_revision",
        PLANS_HEAD_REVISION,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plans_immutable_delete",
        PLANS_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_versions_insert_guard",
        PLAN_VERSION_INSERT_GUARD,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_versions_immutable_update",
        PLAN_VERSIONS_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_versions_immutable_delete",
        PLAN_VERSIONS_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_cohort_immutable_update",
        PLAN_COHORT_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_cohort_immutable_delete",
        PLAN_COHORT_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_cohort_insert_guard",
        PLAN_COHORT_INSERT_GUARD,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_starts_insert_guard",
        PLAN_START_INSERT_GUARD,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_starts_immutable_update",
        PLAN_STARTS_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_plan_starts_immutable_delete",
        PLAN_STARTS_IMMUTABLE_DELETE,
    ),
];

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    OBJECTS.to_vec()
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in OBJECTS {
        connection.execute_batch(sql)?;
    }
    Ok(())
}
