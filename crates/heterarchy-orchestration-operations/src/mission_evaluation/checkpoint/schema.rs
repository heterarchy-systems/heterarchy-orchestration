use rusqlite::Connection;

use crate::OperationsError;

const CHECKPOINTS: &str = "CREATE TABLE ops_mission_evaluation_t2_deferred_checkpoints (checkpoint_id BLOB PRIMARY KEY NOT NULL CHECK(length(checkpoint_id)=16 AND checkpoint_id!=zeroblob(16)),request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)),mission_id BLOB NOT NULL CHECK(length(mission_id)=16 AND mission_id!=zeroblob(16)),plan_id BLOB NOT NULL CHECK(length(plan_id)=16 AND plan_id!=zeroblob(16)),revision INTEGER NOT NULL CHECK(revision>=1),sequence INTEGER NOT NULL CHECK(sequence>=0),previous_checkpoint_id BLOB CHECK(previous_checkpoint_id IS NULL OR (length(previous_checkpoint_id)=16 AND previous_checkpoint_id!=zeroblob(16))),reason TEXT NOT NULL CHECK(length(CAST(reason AS BLOB)) BETWEEN 1 AND 4096 AND length(trim(reason))>0),next_check_condition TEXT NOT NULL CHECK(length(CAST(next_check_condition AS BLOB)) BETWEEN 1 AND 2048 AND length(trim(next_check_condition))>0),next_check_at_ms INTEGER NOT NULL CHECK(next_check_at_ms>=0),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0 AND next_check_at_ms>recorded_at_ms),accountable_owner_activity_id BLOB NOT NULL CHECK(length(accountable_owner_activity_id)=16 AND accountable_owner_activity_id!=zeroblob(16)),accountable_owner_agent_id BLOB NOT NULL CHECK(length(accountable_owner_agent_id)=16 AND accountable_owner_agent_id!=zeroblob(16)),accountable_owner_kind INTEGER NOT NULL CHECK(accountable_owner_kind IN(1,2)),accountable_owner_key TEXT NOT NULL CHECK(length(CAST(accountable_owner_key AS BLOB)) BETWEEN 1 AND 256),accountable_owner_display_name TEXT NOT NULL CHECK(length(CAST(accountable_owner_display_name AS BLOB)) BETWEEN 1 AND 128 AND length(trim(accountable_owner_display_name))>0),accountable_owner_participation_id BLOB CHECK(accountable_owner_participation_id IS NULL OR (length(accountable_owner_participation_id)=16 AND accountable_owner_participation_id!=zeroblob(16))),accountable_owner_profile_id BLOB CHECK(accountable_owner_profile_id IS NULL OR (length(accountable_owner_profile_id)=16 AND accountable_owner_profile_id!=zeroblob(16))),accountable_owner_participant_id BLOB NOT NULL CHECK(length(accountable_owner_participant_id)=16 AND accountable_owner_participant_id!=zeroblob(16)),accountable_owner_session_id BLOB NOT NULL CHECK(length(accountable_owner_session_id)=16 AND accountable_owner_session_id!=zeroblob(16)),accountable_owner_leadership_revision INTEGER NOT NULL CHECK(accountable_owner_leadership_revision>=1),UNIQUE(plan_id,revision,sequence),UNIQUE(plan_id,revision,checkpoint_id),CHECK((sequence=0 AND previous_checkpoint_id IS NULL) OR (sequence>0 AND previous_checkpoint_id IS NOT NULL)),CHECK((accountable_owner_kind=1 AND accountable_owner_participation_id IS NOT NULL AND accountable_owner_profile_id IS NOT NULL) OR (accountable_owner_kind=2 AND accountable_owner_participation_id IS NULL AND accountable_owner_profile_id IS NULL)),FOREIGN KEY(plan_id,revision,workspace_id,mission_id) REFERENCES ops_mission_evaluation_plan_versions(plan_id,revision,workspace_id,mission_id),FOREIGN KEY(plan_id,revision,previous_checkpoint_id) REFERENCES ops_mission_evaluation_t2_deferred_checkpoints(plan_id,revision,checkpoint_id)) STRICT";

const CHECKPOINT_INSERT_GUARD: &str = "CREATE TRIGGER ops_mission_evaluation_t2_deferred_checkpoints_insert_guard BEFORE INSERT ON ops_mission_evaluation_t2_deferred_checkpoints WHEN EXISTS(SELECT 1 FROM ops_mission_evaluation_requests WHERE request_id=NEW.request_id) OR NOT EXISTS(SELECT 1 FROM ops_mission_evaluation_plan_starts s JOIN ops_mission_evaluation_plan_versions v ON v.plan_id=s.plan_id AND v.revision=s.revision AND v.workspace_id=s.workspace_id AND v.mission_id=s.mission_id WHERE s.plan_id=NEW.plan_id AND s.revision=NEW.revision AND s.workspace_id=NEW.workspace_id AND s.mission_id=NEW.mission_id AND v.sealed=1 AND NEW.recorded_at_ms>=v.t2_checkpoint_ms) OR (NEW.sequence=0 AND EXISTS(SELECT 1 FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE plan_id=NEW.plan_id AND revision=NEW.revision)) OR (NEW.sequence>0 AND NOT EXISTS(SELECT 1 FROM ops_mission_evaluation_t2_deferred_checkpoints p WHERE p.plan_id=NEW.plan_id AND p.revision=NEW.revision AND p.checkpoint_id=NEW.previous_checkpoint_id AND p.sequence=NEW.sequence-1 AND p.recorded_at_ms<=NEW.recorded_at_ms)) OR EXISTS(SELECT 1 FROM ops_mission_evaluation_t2_deferred_checkpoints p WHERE p.plan_id=NEW.plan_id AND p.revision=NEW.revision AND p.sequence>=NEW.sequence) BEGIN SELECT RAISE(ABORT,'mission T2 deferred checkpoint source/order'); END";

const REQUEST_ID_CROSS_DOMAIN_GUARD: &str = "CREATE TRIGGER ops_mission_evaluation_requests_checkpoint_id_guard BEFORE INSERT ON ops_mission_evaluation_requests WHEN EXISTS(SELECT 1 FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'mission evaluation request identity already used by checkpoint'); END";

const CHECKPOINT_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_mission_evaluation_t2_deferred_checkpoints_immutable_update BEFORE UPDATE ON ops_mission_evaluation_t2_deferred_checkpoints BEGIN SELECT RAISE(ABORT,'immutable mission T2 deferred checkpoint'); END";
const CHECKPOINT_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_mission_evaluation_t2_deferred_checkpoints_immutable_delete BEFORE DELETE ON ops_mission_evaluation_t2_deferred_checkpoints BEGIN SELECT RAISE(ABORT,'immutable mission T2 deferred checkpoint'); END";

const OBJECTS: &[(&str, &str, &str)] = &[
    (
        "table",
        "ops_mission_evaluation_t2_deferred_checkpoints",
        CHECKPOINTS,
    ),
    (
        "trigger",
        "ops_mission_evaluation_t2_deferred_checkpoints_insert_guard",
        CHECKPOINT_INSERT_GUARD,
    ),
    (
        "trigger",
        "ops_mission_evaluation_requests_checkpoint_id_guard",
        REQUEST_ID_CROSS_DOMAIN_GUARD,
    ),
    (
        "trigger",
        "ops_mission_evaluation_t2_deferred_checkpoints_immutable_update",
        CHECKPOINT_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_mission_evaluation_t2_deferred_checkpoints_immutable_delete",
        CHECKPOINT_IMMUTABLE_DELETE,
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
