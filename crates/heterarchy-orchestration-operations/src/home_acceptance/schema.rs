use rusqlite::Connection;

use crate::OperationsError;

const INTENTS: &str = "CREATE TABLE ops_home_acceptance_intents (acceptance_id BLOB PRIMARY KEY NOT NULL CHECK(length(acceptance_id)=16 AND acceptance_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), issuer_id BLOB NOT NULL CHECK(length(issuer_id)=16 AND issuer_id!=zeroblob(16)), source_backend_id BLOB NOT NULL CHECK(length(source_backend_id)=16 AND source_backend_id!=zeroblob(16)), source_destination_id BLOB NOT NULL CHECK(length(source_destination_id)=16 AND source_destination_id!=zeroblob(16)), predecessor_acceptance_id BLOB CHECK(predecessor_acceptance_id IS NULL OR (length(predecessor_acceptance_id)=16 AND predecessor_acceptance_id!=zeroblob(16))), predecessor_source_digest BLOB CHECK(predecessor_source_digest IS NULL OR (length(predecessor_source_digest)=32 AND predecessor_source_digest!=zeroblob(32))), request_digest BLOB NOT NULL CHECK(length(request_digest)=32 AND request_digest!=zeroblob(32)), request_snapshot BLOB NOT NULL CHECK(length(request_snapshot) BETWEEN 1 AND 512), proposal_snapshot BLOB CHECK(proposal_snapshot IS NULL OR length(proposal_snapshot) BETWEEN 1 AND 2048), proposal_digest BLOB CHECK(proposal_digest IS NULL OR (length(proposal_digest)=32 AND proposal_digest!=zeroblob(32))), intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32 AND intent_digest!=zeroblob(32)), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), CHECK((predecessor_acceptance_id IS NULL AND predecessor_source_digest IS NULL) OR (predecessor_acceptance_id IS NOT NULL AND predecessor_source_digest IS NOT NULL)), CHECK((proposal_snapshot IS NULL AND proposal_digest IS NULL AND predecessor_acceptance_id IS NULL) OR (proposal_snapshot IS NOT NULL AND proposal_digest IS NOT NULL))) STRICT";
const ADMISSIONS: &str = "CREATE TABLE ops_home_acceptance_admissions (acceptance_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_home_acceptance_intents(acceptance_id) CHECK(length(acceptance_id)=16 AND acceptance_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), request_digest BLOB NOT NULL CHECK(length(request_digest)=32 AND request_digest!=zeroblob(32)), issuer_id BLOB NOT NULL CHECK(length(issuer_id)=16 AND issuer_id!=zeroblob(16)), receipt_id BLOB NOT NULL UNIQUE CHECK(length(receipt_id)=16 AND receipt_id!=zeroblob(16)), receipt_digest BLOB NOT NULL CHECK(length(receipt_digest)=32 AND receipt_digest!=zeroblob(32)), admission_snapshot BLOB NOT NULL CHECK(length(admission_snapshot) BETWEEN 1 AND 8192), moving_profile_id BLOB NOT NULL CHECK(length(moving_profile_id)=16 AND moving_profile_id!=zeroblob(16)), recipient_profile_id BLOB NOT NULL CHECK(length(recipient_profile_id)=16 AND recipient_profile_id!=zeroblob(16)), recipient_participant_id BLOB NOT NULL CHECK(length(recipient_participant_id)=16 AND recipient_participant_id!=zeroblob(16)), recipient_session_id BLOB NOT NULL CHECK(length(recipient_session_id)=16 AND recipient_session_id!=zeroblob(16)), recipient_participation_id BLOB NOT NULL CHECK(length(recipient_participation_id)=16 AND recipient_participation_id!=zeroblob(16)), recipient_participation_revision INTEGER NOT NULL CHECK(recipient_participation_revision>=1), recipient_membership_revision INTEGER NOT NULL CHECK(recipient_membership_revision>=1), session_revision INTEGER NOT NULL CHECK(session_revision>=1), session_acquired_at_ms INTEGER NOT NULL CHECK(session_acquired_at_ms>=0), profile_revision INTEGER NOT NULL CHECK(profile_revision>=1), profile_link_request_id BLOB NOT NULL CHECK(length(profile_link_request_id)=16 AND profile_link_request_id!=zeroblob(16)), profile_linked_profile_revision INTEGER NOT NULL CHECK(profile_linked_profile_revision>=1), profile_linked_session_revision INTEGER NOT NULL CHECK(profile_linked_session_revision>=1), profile_acquired_at_ms INTEGER NOT NULL CHECK(profile_acquired_at_ms>=0), home_team_id BLOB CHECK(home_team_id IS NULL OR (length(home_team_id)=16 AND home_team_id!=zeroblob(16))), team_membership_revision INTEGER CHECK(team_membership_revision IS NULL OR team_membership_revision>=1), node_binding_revision INTEGER CHECK(node_binding_revision IS NULL OR node_binding_revision>=1), node_binding_digest BLOB CHECK(node_binding_digest IS NULL OR (length(node_binding_digest)=32 AND node_binding_digest!=zeroblob(32))), affinity_digest BLOB NOT NULL CHECK(length(affinity_digest)=32 AND affinity_digest!=zeroblob(32)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id!=zeroblob(16)), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id!=zeroblob(16)), goal_created_at_ms INTEGER NOT NULL CHECK(goal_created_at_ms>=0), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), dag_base_goal_revision INTEGER NOT NULL CHECK(dag_base_goal_revision>=1), node_id BLOB NOT NULL CHECK(length(node_id)=16 AND node_id!=zeroblob(16)), root_assignment_id BLOB NOT NULL CHECK(length(root_assignment_id)=16 AND root_assignment_id!=zeroblob(16)), successor_assignment_id BLOB NOT NULL CHECK(length(successor_assignment_id)=16 AND successor_assignment_id!=zeroblob(16)), authority_kind INTEGER NOT NULL CHECK(authority_kind IN(0,1)), grant_issued_at_ms INTEGER NOT NULL CHECK(grant_issued_at_ms>=0), grant_expires_at_ms INTEGER NOT NULL CHECK(grant_expires_at_ms>grant_issued_at_ms), admitted_at_ms INTEGER NOT NULL CHECK(admitted_at_ms>=grant_issued_at_ms), CHECK(moving_profile_id!=recipient_profile_id), CHECK((authority_kind=0 AND home_team_id IS NULL AND team_membership_revision IS NULL AND node_binding_digest IS NOT NULL) OR (authority_kind=1 AND home_team_id IS NOT NULL AND team_membership_revision IS NOT NULL AND node_binding_revision IS NOT NULL AND node_binding_digest IS NOT NULL))) STRICT";
const RESOLUTIONS: &str = "CREATE TABLE ops_home_acceptance_resolutions (acceptance_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_home_acceptance_intents(acceptance_id) CHECK(length(acceptance_id)=16 AND acceptance_id!=zeroblob(16)), admission_acceptance_id BLOB REFERENCES ops_home_acceptance_admissions(acceptance_id) CHECK(admission_acceptance_id IS NULL OR admission_acceptance_id=acceptance_id), request_digest BLOB NOT NULL CHECK(length(request_digest)=32 AND request_digest!=zeroblob(32)), issuer_id BLOB NOT NULL CHECK(length(issuer_id)=16 AND issuer_id!=zeroblob(16)), outcome INTEGER NOT NULL CHECK(outcome IN(1,2)), source_payload_digest BLOB NOT NULL CHECK(length(source_payload_digest)=32 AND source_payload_digest!=zeroblob(32)), source_snapshot BLOB NOT NULL CHECK(length(source_snapshot) BETWEEN 1 AND 4096), resolution_id BLOB NOT NULL UNIQUE CHECK(length(resolution_id)=16 AND resolution_id!=zeroblob(16)), resolution_digest BLOB NOT NULL CHECK(length(resolution_digest)=32 AND resolution_digest!=zeroblob(32)), resolved_at_ms INTEGER NOT NULL CHECK(resolved_at_ms>=0), CHECK(outcome!=1 OR admission_acceptance_id IS NOT NULL)) STRICT";
const WORK_RESERVATIONS: &str = "CREATE TABLE ops_home_acceptance_work_reservations (acceptance_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_home_acceptance_intents(acceptance_id) CHECK(length(acceptance_id)=16 AND acceptance_id!=zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id!=zeroblob(16)), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id!=zeroblob(16)), goal_created_at_ms INTEGER NOT NULL CHECK(goal_created_at_ms>=0), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), dag_base_goal_revision INTEGER NOT NULL CHECK(dag_base_goal_revision>=1), node_id BLOB NOT NULL CHECK(length(node_id)=16 AND node_id!=zeroblob(16)), root_assignment_id BLOB NOT NULL CHECK(length(root_assignment_id)=16 AND root_assignment_id!=zeroblob(16)), successor_assignment_id BLOB NOT NULL CHECK(length(successor_assignment_id)=16 AND successor_assignment_id!=zeroblob(16)), authority_kind INTEGER NOT NULL CHECK(authority_kind IN(0,1)), team_id BLOB CHECK(team_id IS NULL OR (length(team_id)=16 AND team_id!=zeroblob(16))), state INTEGER NOT NULL CHECK(state IN(0,1,2,3)), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=created_at_ms), terminal_source_digest BLOB CHECK(terminal_source_digest IS NULL OR (length(terminal_source_digest)=32 AND terminal_source_digest!=zeroblob(32))), terminal_assignment_state INTEGER CHECK(terminal_assignment_state IS NULL OR terminal_assignment_state IN(1,2,3,4)), terminal_assignment_revision INTEGER CHECK(terminal_assignment_revision IS NULL OR terminal_assignment_revision>=1), terminal_assignment_cancellation_requested INTEGER CHECK(terminal_assignment_cancellation_requested IS NULL OR terminal_assignment_cancellation_requested IN(0,1)), terminal_assignment_digest BLOB CHECK(terminal_assignment_digest IS NULL OR (length(terminal_assignment_digest)=32 AND terminal_assignment_digest!=zeroblob(32))), terminal_assignment_updated_at_ms INTEGER CHECK(terminal_assignment_updated_at_ms IS NULL OR terminal_assignment_updated_at_ms>=0), CHECK((state IN(0,1) AND terminal_source_digest IS NULL) OR (state IN(2,3) AND terminal_source_digest IS NOT NULL)), CHECK((state=3 AND terminal_assignment_state IS NOT NULL AND terminal_assignment_revision IS NOT NULL AND terminal_assignment_cancellation_requested IS NOT NULL AND terminal_assignment_digest IS NOT NULL AND terminal_assignment_updated_at_ms IS NOT NULL) OR (state!=3 AND terminal_assignment_state IS NULL AND terminal_assignment_revision IS NULL AND terminal_assignment_cancellation_requested IS NULL AND terminal_assignment_digest IS NULL AND terminal_assignment_updated_at_ms IS NULL)), CHECK((authority_kind=0 AND team_id IS NULL) OR (authority_kind=1 AND team_id IS NOT NULL))) STRICT";
const ACTIVE_NODE: &str = "CREATE INDEX ops_home_acceptance_active_node_idx ON ops_home_acceptance_work_reservations(workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id) WHERE state IN(0,1)";
const PENDING_PROFILE: &str = "CREATE INDEX ops_home_acceptance_pending_profile_idx ON ops_home_acceptance_admissions(workspace_id,recipient_profile_id)";
const PENDING_SESSION: &str = "CREATE INDEX ops_home_acceptance_pending_session_idx ON ops_home_acceptance_admissions(workspace_id,recipient_session_id)";
const PENDING_TEAM: &str = "CREATE INDEX ops_home_acceptance_pending_team_idx ON ops_home_acceptance_admissions(workspace_id,home_team_id) WHERE home_team_id IS NOT NULL";
const INTENTS_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_home_acceptance_intents_immutable_update BEFORE UPDATE ON ops_home_acceptance_intents BEGIN SELECT RAISE(ABORT,'immutable Home acceptance intent'); END";
const INTENTS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_home_acceptance_intents_immutable_delete BEFORE DELETE ON ops_home_acceptance_intents BEGIN SELECT RAISE(ABORT,'immutable Home acceptance intent'); END";
const ADMISSIONS_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_home_acceptance_admissions_immutable_update BEFORE UPDATE ON ops_home_acceptance_admissions BEGIN SELECT RAISE(ABORT,'immutable Home acceptance admission'); END";
const ADMISSIONS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_home_acceptance_admissions_immutable_delete BEFORE DELETE ON ops_home_acceptance_admissions BEGIN SELECT RAISE(ABORT,'immutable Home acceptance admission'); END";
const RESOLUTIONS_IMMUTABLE_UPDATE: &str = "CREATE TRIGGER ops_home_acceptance_resolutions_immutable_update BEFORE UPDATE ON ops_home_acceptance_resolutions BEGIN SELECT RAISE(ABORT,'immutable Home acceptance resolution'); END";
const RESOLUTIONS_IMMUTABLE_DELETE: &str = "CREATE TRIGGER ops_home_acceptance_resolutions_immutable_delete BEFORE DELETE ON ops_home_acceptance_resolutions BEGIN SELECT RAISE(ABORT,'immutable Home acceptance resolution'); END";
const WORK_RESERVATION_TRANSITION: &str = "CREATE TRIGGER ops_home_acceptance_work_reservation_transition BEFORE UPDATE ON ops_home_acceptance_work_reservations WHEN NEW.acceptance_id!=OLD.acceptance_id OR NEW.workspace_id!=OLD.workspace_id OR NEW.activity_id!=OLD.activity_id OR NEW.goal_id!=OLD.goal_id OR NEW.goal_created_at_ms!=OLD.goal_created_at_ms OR NEW.dag_revision!=OLD.dag_revision OR NEW.dag_base_goal_revision!=OLD.dag_base_goal_revision OR NEW.node_id!=OLD.node_id OR NEW.root_assignment_id!=OLD.root_assignment_id OR NEW.successor_assignment_id!=OLD.successor_assignment_id OR NEW.authority_kind!=OLD.authority_kind OR NEW.team_id IS NOT OLD.team_id OR NOT ((OLD.state=0 AND NEW.state IN(1,2)) OR (OLD.state=1 AND NEW.state=3)) OR NEW.updated_at_ms<OLD.updated_at_ms OR (OLD.state=0 AND NEW.state=1 AND (NEW.terminal_source_digest IS NOT NULL OR NEW.terminal_assignment_state IS NOT NULL OR NEW.terminal_assignment_revision IS NOT NULL OR NEW.terminal_assignment_cancellation_requested IS NOT NULL OR NEW.terminal_assignment_digest IS NOT NULL OR NEW.terminal_assignment_updated_at_ms IS NOT NULL)) OR (OLD.state=0 AND NEW.state=2 AND (NEW.terminal_source_digest IS NULL OR NEW.terminal_assignment_state IS NOT NULL OR NEW.terminal_assignment_revision IS NOT NULL OR NEW.terminal_assignment_cancellation_requested IS NOT NULL OR NEW.terminal_assignment_digest IS NOT NULL OR NEW.terminal_assignment_updated_at_ms IS NOT NULL)) OR (OLD.state=1 AND NEW.state=3 AND (NEW.terminal_source_digest IS NULL OR NEW.terminal_assignment_state IS NULL OR NEW.terminal_assignment_revision IS NULL OR NEW.terminal_assignment_cancellation_requested IS NULL OR NEW.terminal_assignment_digest IS NULL OR NEW.terminal_assignment_updated_at_ms IS NULL)) BEGIN SELECT RAISE(ABORT,'Home work reservation transition'); END";
const WORK_RESERVATION_NO_DELETE: &str = "CREATE TRIGGER ops_home_acceptance_work_reservation_no_delete BEFORE DELETE ON ops_home_acceptance_work_reservations BEGIN SELECT RAISE(ABORT,'immutable Home work reservation history'); END";

const OBJECTS: &[(&str, &str, &str)] = &[
    ("table", "ops_home_acceptance_intents", INTENTS),
    ("table", "ops_home_acceptance_admissions", ADMISSIONS),
    ("table", "ops_home_acceptance_resolutions", RESOLUTIONS),
    (
        "table",
        "ops_home_acceptance_work_reservations",
        WORK_RESERVATIONS,
    ),
    ("index", "ops_home_acceptance_active_node_idx", ACTIVE_NODE),
    (
        "index",
        "ops_home_acceptance_pending_profile_idx",
        PENDING_PROFILE,
    ),
    (
        "index",
        "ops_home_acceptance_pending_session_idx",
        PENDING_SESSION,
    ),
    (
        "index",
        "ops_home_acceptance_pending_team_idx",
        PENDING_TEAM,
    ),
    (
        "trigger",
        "ops_home_acceptance_intents_immutable_update",
        INTENTS_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_home_acceptance_intents_immutable_delete",
        INTENTS_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_home_acceptance_admissions_immutable_update",
        ADMISSIONS_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_home_acceptance_admissions_immutable_delete",
        ADMISSIONS_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_home_acceptance_resolutions_immutable_update",
        RESOLUTIONS_IMMUTABLE_UPDATE,
    ),
    (
        "trigger",
        "ops_home_acceptance_resolutions_immutable_delete",
        RESOLUTIONS_IMMUTABLE_DELETE,
    ),
    (
        "trigger",
        "ops_home_acceptance_work_reservation_transition",
        WORK_RESERVATION_TRANSITION,
    ),
    (
        "trigger",
        "ops_home_acceptance_work_reservation_no_delete",
        WORK_RESERVATION_NO_DELETE,
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
