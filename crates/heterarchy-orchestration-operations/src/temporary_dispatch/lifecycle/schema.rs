use rusqlite::Connection;

use crate::OperationsError;

const CONSENT_RECEIPTS_SQL: &str = "CREATE TABLE ops_temporary_dispatch_consent_receipts (receipt_id BLOB PRIMARY KEY NOT NULL CHECK(length(receipt_id)=16 AND receipt_id != zeroblob(16)), request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), consent_kind TEXT NOT NULL CHECK(consent_kind IN('TARGET_ACCEPTANCE','HOME_LEADER_CONSENT','WORKSPACE_ORCHESTRATOR_APPROVAL')), revision INTEGER NOT NULL CHECK(revision>=1), intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32), actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id != zeroblob(16)), actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id != zeroblob(16)), actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id != zeroblob(16)), actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id != zeroblob(16)), actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id != zeroblob(16)), actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id != zeroblob(16)), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), authority_revision INTEGER NOT NULL CHECK(authority_revision>=1), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), UNIQUE(request_id,consent_kind,revision), UNIQUE(request_id,consent_kind,receipt_id), FOREIGN KEY(request_id) REFERENCES ops_temporary_dispatch_requests(request_id))";

const CONSENT_CURRENT_SQL: &str = "CREATE TABLE ops_temporary_dispatch_current_consents (request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), consent_kind TEXT NOT NULL CHECK(consent_kind IN('TARGET_ACCEPTANCE','HOME_LEADER_CONSENT','WORKSPACE_ORCHESTRATOR_APPROVAL')), receipt_id BLOB CHECK(receipt_id IS NULL OR (length(receipt_id)=16 AND receipt_id != zeroblob(16))), revision INTEGER NOT NULL CHECK(revision>=1), PRIMARY KEY(request_id,consent_kind), FOREIGN KEY(request_id) REFERENCES ops_temporary_dispatch_requests(request_id), FOREIGN KEY(request_id,consent_kind,receipt_id) REFERENCES ops_temporary_dispatch_consent_receipts(request_id,consent_kind,receipt_id))";

const CONSENT_WITHDRAWALS_SQL: &str = "CREATE TABLE ops_temporary_dispatch_consent_withdrawals (receipt_id BLOB PRIMARY KEY NOT NULL CHECK(length(receipt_id)=16 AND receipt_id != zeroblob(16)), consent_receipt_id BLOB NOT NULL CHECK(length(consent_receipt_id)=16 AND consent_receipt_id != zeroblob(16)), request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), consent_kind TEXT NOT NULL CHECK(consent_kind IN('TARGET_ACCEPTANCE','HOME_LEADER_CONSENT','WORKSPACE_ORCHESTRATOR_APPROVAL')), revision INTEGER NOT NULL CHECK(revision>=1), intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32), actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id != zeroblob(16)), actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id != zeroblob(16)), actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id != zeroblob(16)), actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id != zeroblob(16)), actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id != zeroblob(16)), actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id != zeroblob(16)), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), authority_revision INTEGER NOT NULL CHECK(authority_revision>=1), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), UNIQUE(consent_receipt_id), FOREIGN KEY(request_id,consent_kind,consent_receipt_id) REFERENCES ops_temporary_dispatch_consent_receipts(request_id,consent_kind,receipt_id))";

const RESERVATION_RECEIPTS_SQL: &str = "CREATE TABLE ops_temporary_dispatch_reservation_receipts (receipt_id BLOB PRIMARY KEY NOT NULL CHECK(length(receipt_id)=16 AND receipt_id != zeroblob(16)), request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), revision INTEGER NOT NULL CHECK(revision>=1), state TEXT NOT NULL CHECK(state IN('PENDING_CONFLICT','RESERVED','SOURCE_READY','ACTIVE','DRAINING','RETURNED','DENIED','CANCELLED')), intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32), target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id != zeroblob(16)), target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id != zeroblob(16)), target_membership_revision INTEGER NOT NULL CHECK(target_membership_revision>=1), requesting_leadership_revision INTEGER NOT NULL CHECK(requesting_leadership_revision>=1), home_leadership_revision INTEGER NOT NULL CHECK(home_leadership_revision>=1), workspace_leadership_revision INTEGER NOT NULL CHECK(workspace_leadership_revision>=1), target_consent_receipt_id BLOB NOT NULL CHECK(length(target_consent_receipt_id)=16 AND target_consent_receipt_id != zeroblob(16)), home_leader_consent_receipt_id BLOB NOT NULL CHECK(length(home_leader_consent_receipt_id)=16 AND home_leader_consent_receipt_id != zeroblob(16)), orchestrator_consent_receipt_id BLOB NOT NULL CHECK(length(orchestrator_consent_receipt_id)=16 AND orchestrator_consent_receipt_id != zeroblob(16)), actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id != zeroblob(16)), actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id != zeroblob(16)), actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id != zeroblob(16)), actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id != zeroblob(16)), actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id != zeroblob(16)), actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id != zeroblob(16)), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), UNIQUE(request_id,revision), UNIQUE(request_id,receipt_id), FOREIGN KEY(request_id) REFERENCES ops_temporary_dispatch_requests(request_id), FOREIGN KEY(target_consent_receipt_id) REFERENCES ops_temporary_dispatch_consent_receipts(receipt_id), FOREIGN KEY(home_leader_consent_receipt_id) REFERENCES ops_temporary_dispatch_consent_receipts(receipt_id), FOREIGN KEY(orchestrator_consent_receipt_id) REFERENCES ops_temporary_dispatch_consent_receipts(receipt_id))";

const RESERVATIONS_SQL: &str = "CREATE TABLE ops_temporary_dispatch_reservations (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), receiving_team_id BLOB NOT NULL CHECK(length(receiving_team_id)=16 AND receiving_team_id != zeroblob(16)), home_team_id BLOB NOT NULL CHECK(length(home_team_id)=16 AND home_team_id != zeroblob(16)), target_participant_id BLOB NOT NULL CHECK(length(target_participant_id)=16 AND target_participant_id != zeroblob(16)), target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id != zeroblob(16)), target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id != zeroblob(16)), requested_scope TEXT NOT NULL CHECK(length(CAST(requested_scope AS BLOB)) BETWEEN 1 AND 2048 AND length(trim(requested_scope))>0), requested_end_at_ms INTEGER NOT NULL CHECK(requested_end_at_ms>=0), intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32), state TEXT NOT NULL CHECK(state IN('PENDING_CONFLICT','RESERVED','SOURCE_READY','ACTIVE','DRAINING','RETURNED','DENIED','CANCELLED')), revision INTEGER NOT NULL CHECK(revision>=1), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), FOREIGN KEY(request_id) REFERENCES ops_temporary_dispatch_requests(request_id), FOREIGN KEY(workspace_id,target_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id))";

const RESERVATION_RECEIPT_NO_UPDATE_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_reservation_receipt_no_update BEFORE UPDATE ON ops_temporary_dispatch_reservation_receipts BEGIN SELECT RAISE(ABORT,'temporary dispatch reservation receipts are immutable'); END";
const RESERVATION_RECEIPT_NO_DELETE_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_reservation_receipt_no_delete BEFORE DELETE ON ops_temporary_dispatch_reservation_receipts BEGIN SELECT RAISE(ABORT,'temporary dispatch reservation receipts are immutable'); END";
const CONSENT_RECEIPT_NO_UPDATE_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_consent_receipt_no_update BEFORE UPDATE ON ops_temporary_dispatch_consent_receipts BEGIN SELECT RAISE(ABORT,'temporary dispatch consent receipts are immutable'); END";
const CONSENT_RECEIPT_NO_DELETE_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_consent_receipt_no_delete BEFORE DELETE ON ops_temporary_dispatch_consent_receipts BEGIN SELECT RAISE(ABORT,'temporary dispatch consent receipts are immutable'); END";
const CONSENT_WITHDRAWAL_NO_UPDATE_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_consent_withdrawal_no_update BEFORE UPDATE ON ops_temporary_dispatch_consent_withdrawals BEGIN SELECT RAISE(ABORT,'temporary dispatch withdrawal receipts are immutable'); END";
const CONSENT_WITHDRAWAL_NO_DELETE_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_consent_withdrawal_no_delete BEFORE DELETE ON ops_temporary_dispatch_consent_withdrawals BEGIN SELECT RAISE(ABORT,'temporary dispatch withdrawal receipts are immutable'); END";
const RESERVATIONS_NO_DELETE_SQL: &str = "CREATE TRIGGER ops_temporary_dispatch_reservations_no_delete BEFORE DELETE ON ops_temporary_dispatch_reservations BEGIN SELECT RAISE(ABORT,'temporary dispatch reservation history is retained'); END";

const RESERVATION_CAPACITY_INDEX_SQL: &str = "CREATE INDEX ops_temporary_dispatch_reservations_capacity_idx ON ops_temporary_dispatch_reservations(workspace_id,receiving_team_id,state,target_participant_id)";
const RESERVATION_SUBJECT_INDEX_SQL: &str = "CREATE INDEX ops_temporary_dispatch_reservations_subject_idx ON ops_temporary_dispatch_reservations(workspace_id,target_participant_id,state,receiving_team_id)";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_temporary_dispatch_consent_receipts",
            CONSENT_RECEIPTS_SQL,
        ),
        (
            "table",
            "ops_temporary_dispatch_current_consents",
            CONSENT_CURRENT_SQL,
        ),
        (
            "table",
            "ops_temporary_dispatch_consent_withdrawals",
            CONSENT_WITHDRAWALS_SQL,
        ),
        (
            "table",
            "ops_temporary_dispatch_reservation_receipts",
            RESERVATION_RECEIPTS_SQL,
        ),
        (
            "table",
            "ops_temporary_dispatch_reservations",
            RESERVATIONS_SQL,
        ),
        (
            "index",
            "ops_temporary_dispatch_reservations_capacity_idx",
            RESERVATION_CAPACITY_INDEX_SQL,
        ),
        (
            "index",
            "ops_temporary_dispatch_reservations_subject_idx",
            RESERVATION_SUBJECT_INDEX_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_reservation_receipt_no_update",
            RESERVATION_RECEIPT_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_reservation_receipt_no_delete",
            RESERVATION_RECEIPT_NO_DELETE_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_consent_receipt_no_update",
            CONSENT_RECEIPT_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_consent_receipt_no_delete",
            CONSENT_RECEIPT_NO_DELETE_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_consent_withdrawal_no_update",
            CONSENT_WITHDRAWAL_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_consent_withdrawal_no_delete",
            CONSENT_WITHDRAWAL_NO_DELETE_SQL,
        ),
        (
            "trigger",
            "ops_temporary_dispatch_reservations_no_delete",
            RESERVATIONS_NO_DELETE_SQL,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}
