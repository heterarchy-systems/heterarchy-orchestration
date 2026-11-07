use rusqlite::Transaction;

use crate::OperationsError;

const CAPACITY_EVENTS: &str = "CREATE TABLE ops_team_capacity_events(request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),capacity_revision INTEGER NOT NULL CHECK(capacity_revision>=1),expected_capacity_revision INTEGER NOT NULL CHECK(expected_capacity_revision>=0),expected_team_revision INTEGER NOT NULL CHECK(expected_team_revision>=0),result_team_revision INTEGER NOT NULL CHECK(result_team_revision>=1),max_members INTEGER NOT NULL CHECK(max_members BETWEEN 0 AND 8),operation TEXT NOT NULL CHECK(operation IN('CREATE','SET')),actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id!=zeroblob(16)),actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id!=zeroblob(16)),actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id!=zeroblob(16)),actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)),actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)),actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16)),actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1),UNIQUE(workspace_id,team_id,capacity_revision),FOREIGN KEY(workspace_id,team_id) REFERENCES ops_teams(workspace_id,team_id))";
const CAPACITY_EVENTS_INDEX: &str = "CREATE INDEX ops_team_capacity_events_current_idx ON ops_team_capacity_events(workspace_id,team_id,capacity_revision DESC)";
const CAPACITY_NO_UPDATE: &str = "CREATE TRIGGER ops_team_capacity_events_no_update BEFORE UPDATE ON ops_team_capacity_events BEGIN SELECT RAISE(ABORT,'team capacity history is immutable'); END";
const CAPACITY_NO_DELETE: &str = "CREATE TRIGGER ops_team_capacity_events_no_delete BEFORE DELETE ON ops_team_capacity_events BEGIN SELECT RAISE(ABORT,'team capacity history is immutable'); END";

const REQUESTS: &str = "CREATE TABLE ops_team_expansion_requests(request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),expected_team_revision INTEGER NOT NULL CHECK(expected_team_revision>=1),capacity_revision INTEGER NOT NULL CHECK(capacity_revision>=1),requester_leadership_revision INTEGER NOT NULL CHECK(requester_leadership_revision>=1),requested_count INTEGER NOT NULL CHECK(requested_count BETWEEN 1 AND 8),reason TEXT NOT NULL CHECK(length(CAST(reason AS BLOB)) BETWEEN 1 AND 2048 AND length(trim(reason))>0),actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id!=zeroblob(16)),actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id!=zeroblob(16)),actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id!=zeroblob(16)),actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)),actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)),actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16)),actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),UNIQUE(workspace_id,team_id,request_id),FOREIGN KEY(workspace_id,team_id) REFERENCES ops_teams(workspace_id,team_id),FOREIGN KEY(workspace_id,team_id,capacity_revision) REFERENCES ops_team_capacity_events(workspace_id,team_id,capacity_revision))";
const REQUESTS_TEAM_INDEX: &str = "CREATE INDEX ops_team_expansion_requests_team_idx ON ops_team_expansion_requests(workspace_id,team_id,recorded_at_ms)";
const REQUESTS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_requests_no_update BEFORE UPDATE ON ops_team_expansion_requests BEGIN SELECT RAISE(ABORT,'team expansion requests are immutable'); END";
const REQUESTS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_requests_no_delete BEFORE DELETE ON ops_team_expansion_requests BEGIN SELECT RAISE(ABORT,'team expansion requests are immutable'); END";

const DECISIONS: &str = "CREATE TABLE ops_team_expansion_decisions(decision_id BLOB PRIMARY KEY NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),state TEXT NOT NULL CHECK(state IN('APPROVED','REJECTED')),approved_count INTEGER NOT NULL CHECK(approved_count BETWEEN 0 AND 8),expected_team_revision INTEGER NOT NULL CHECK(expected_team_revision>=1),capacity_revision INTEGER NOT NULL CHECK(capacity_revision>=1),leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1),actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id!=zeroblob(16)),actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id!=zeroblob(16)),actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id!=zeroblob(16)),actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)),actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)),actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16)),actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128),decided_at_ms INTEGER NOT NULL CHECK(decided_at_ms>=0),CHECK((state='APPROVED' AND approved_count>=1) OR (state='REJECTED' AND approved_count=0)),UNIQUE(workspace_id,team_id,request_id),UNIQUE(workspace_id,team_id,decision_id),UNIQUE(workspace_id,team_id,decision_id,request_id),FOREIGN KEY(workspace_id,team_id,request_id) REFERENCES ops_team_expansion_requests(workspace_id,team_id,request_id),FOREIGN KEY(workspace_id,team_id,capacity_revision) REFERENCES ops_team_capacity_events(workspace_id,team_id,capacity_revision))";
const DECISIONS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_decisions_no_update BEFORE UPDATE ON ops_team_expansion_decisions BEGIN SELECT RAISE(ABORT,'team expansion decisions are immutable'); END";
const DECISIONS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_decisions_no_delete BEFORE DELETE ON ops_team_expansion_decisions BEGIN SELECT RAISE(ABORT,'team expansion decisions are immutable'); END";

const COOLDOWN_ANCHORS: &str = "CREATE TABLE ops_team_expansion_cooldown_anchors(request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),decision_id BLOB NOT NULL UNIQUE CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),anchor_at_ms INTEGER NOT NULL CHECK(anchor_at_ms>=0),cooldown_until_ms INTEGER NOT NULL CHECK(cooldown_until_ms=anchor_at_ms+3600000),finalized_by_participation_id BLOB NOT NULL CHECK(length(finalized_by_participation_id)=16 AND finalized_by_participation_id!=zeroblob(16)),finalized_by_profile_id BLOB NOT NULL CHECK(length(finalized_by_profile_id)=16 AND finalized_by_profile_id!=zeroblob(16)),finalized_by_participant_id BLOB NOT NULL CHECK(length(finalized_by_participant_id)=16 AND finalized_by_participant_id!=zeroblob(16)),finalized_by_activity_id BLOB NOT NULL CHECK(length(finalized_by_activity_id)=16 AND finalized_by_activity_id!=zeroblob(16)),finalized_by_agent_id BLOB NOT NULL CHECK(length(finalized_by_agent_id)=16 AND finalized_by_agent_id!=zeroblob(16)),finalized_by_session_id BLOB NOT NULL CHECK(length(finalized_by_session_id)=16 AND finalized_by_session_id!=zeroblob(16)),finalized_by_display_name TEXT NOT NULL CHECK(length(CAST(finalized_by_display_name AS BLOB)) BETWEEN 1 AND 128),finalized_by_leadership_revision INTEGER NOT NULL CHECK(finalized_by_leadership_revision>=1),UNIQUE(workspace_id,team_id,request_id),FOREIGN KEY(workspace_id,team_id,decision_id,request_id) REFERENCES ops_team_expansion_decisions(workspace_id,team_id,decision_id,request_id))";
const DECISIONS_COOLDOWN_INDEX: &str = "CREATE INDEX ops_team_expansion_decisions_cooldown_idx ON ops_team_expansion_cooldown_anchors(workspace_id,team_id,cooldown_until_ms)";
const COOLDOWN_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_cooldown_anchors_no_update BEFORE UPDATE ON ops_team_expansion_cooldown_anchors BEGIN SELECT RAISE(ABORT,'team expansion cooldown anchors are immutable'); END";
const COOLDOWN_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_cooldown_anchors_no_delete BEFORE DELETE ON ops_team_expansion_cooldown_anchors BEGIN SELECT RAISE(ABORT,'team expansion cooldown anchors are immutable'); END";

const SLOTS: &str = "CREATE TABLE ops_team_expansion_slots(decision_id BLOB NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 7),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),PRIMARY KEY(decision_id,ordinal),UNIQUE(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,team_id,decision_id,request_id) REFERENCES ops_team_expansion_decisions(workspace_id,team_id,decision_id,request_id),FOREIGN KEY(workspace_id,team_id) REFERENCES ops_teams(workspace_id,team_id))";
const SLOTS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_slots_no_update BEFORE UPDATE ON ops_team_expansion_slots BEGIN SELECT RAISE(ABORT,'approved team expansion slots are immutable'); END";
const SLOTS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_slots_no_delete BEFORE DELETE ON ops_team_expansion_slots BEGIN SELECT RAISE(ABORT,'approved team expansion slots are immutable'); END";

const ASSIGNMENTS: &str = "CREATE TABLE ops_team_expansion_slot_assignments(request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),decision_id BLOB NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 7),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id!=zeroblob(16)),target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id!=zeroblob(16)),target_participant_id BLOB NOT NULL CHECK(length(target_participant_id)=16 AND target_participant_id!=zeroblob(16)),target_activity_id BLOB NOT NULL CHECK(length(target_activity_id)=16 AND target_activity_id!=zeroblob(16)),target_agent_id BLOB NOT NULL CHECK(length(target_agent_id)=16 AND target_agent_id!=zeroblob(16)),target_session_id BLOB NOT NULL CHECK(length(target_session_id)=16 AND target_session_id!=zeroblob(16)),target_display_name TEXT NOT NULL CHECK(length(CAST(target_display_name AS BLOB)) BETWEEN 1 AND 128),actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)),actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)),actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16)),actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id!=zeroblob(16)),actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id!=zeroblob(16)),actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id!=zeroblob(16)),actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128),expected_team_revision INTEGER NOT NULL CHECK(expected_team_revision>=1),capacity_revision INTEGER NOT NULL CHECK(capacity_revision>=1),leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),UNIQUE(decision_id,ordinal),UNIQUE(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,team_id,decision_id,ordinal) REFERENCES ops_team_expansion_slots(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,target_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id),FOREIGN KEY(workspace_id,team_id,capacity_revision) REFERENCES ops_team_capacity_events(workspace_id,team_id,capacity_revision))";
const ASSIGNMENTS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_slot_assignments_no_update BEFORE UPDATE ON ops_team_expansion_slot_assignments BEGIN SELECT RAISE(ABORT,'team expansion slot assignments are immutable'); END";
const ASSIGNMENTS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_slot_assignments_no_delete BEFORE DELETE ON ops_team_expansion_slot_assignments BEGIN SELECT RAISE(ABORT,'team expansion slot assignments are immutable'); END";

const SLOT_USES: &str = "CREATE TABLE ops_team_expansion_slot_uses(decision_id BLOB NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 7),join_request_id BLOB NOT NULL UNIQUE CHECK(length(join_request_id)=16 AND join_request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id!=zeroblob(16)),profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id!=zeroblob(16)),membership_revision INTEGER NOT NULL CHECK(membership_revision>=1),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),PRIMARY KEY(decision_id,ordinal),FOREIGN KEY(workspace_id,team_id,decision_id,ordinal) REFERENCES ops_team_expansion_slot_assignments(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,team_id,participation_id) REFERENCES ops_team_members(workspace_id,team_id,participation_id))";
const SLOT_USES_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_slot_uses_no_update BEFORE UPDATE ON ops_team_expansion_slot_uses BEGIN SELECT RAISE(ABORT,'team expansion slot uses are immutable'); END";
const SLOT_USES_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_slot_uses_no_delete BEFORE DELETE ON ops_team_expansion_slot_uses BEGIN SELECT RAISE(ABORT,'team expansion slot uses are immutable'); END";

pub(super) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_team_capacity_events", CAPACITY_EVENTS),
        (
            "index",
            "ops_team_capacity_events_current_idx",
            CAPACITY_EVENTS_INDEX,
        ),
        (
            "trigger",
            "ops_team_capacity_events_no_update",
            CAPACITY_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_capacity_events_no_delete",
            CAPACITY_NO_DELETE,
        ),
        ("table", "ops_team_expansion_requests", REQUESTS),
        (
            "index",
            "ops_team_expansion_requests_team_idx",
            REQUESTS_TEAM_INDEX,
        ),
        (
            "trigger",
            "ops_team_expansion_requests_no_update",
            REQUESTS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_requests_no_delete",
            REQUESTS_NO_DELETE,
        ),
        ("table", "ops_team_expansion_decisions", DECISIONS),
        (
            "trigger",
            "ops_team_expansion_decisions_no_update",
            DECISIONS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_decisions_no_delete",
            DECISIONS_NO_DELETE,
        ),
        (
            "table",
            "ops_team_expansion_cooldown_anchors",
            COOLDOWN_ANCHORS,
        ),
        (
            "index",
            "ops_team_expansion_decisions_cooldown_idx",
            DECISIONS_COOLDOWN_INDEX,
        ),
        (
            "trigger",
            "ops_team_expansion_cooldown_anchors_no_update",
            COOLDOWN_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_cooldown_anchors_no_delete",
            COOLDOWN_NO_DELETE,
        ),
        ("table", "ops_team_expansion_slots", SLOTS),
        (
            "trigger",
            "ops_team_expansion_slots_no_update",
            SLOTS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_slots_no_delete",
            SLOTS_NO_DELETE,
        ),
        ("table", "ops_team_expansion_slot_assignments", ASSIGNMENTS),
        (
            "trigger",
            "ops_team_expansion_slot_assignments_no_update",
            ASSIGNMENTS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_slot_assignments_no_delete",
            ASSIGNMENTS_NO_DELETE,
        ),
        ("table", "ops_team_expansion_slot_uses", SLOT_USES),
        (
            "trigger",
            "ops_team_expansion_slot_uses_no_update",
            SLOT_USES_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_slot_uses_no_delete",
            SLOT_USES_NO_DELETE,
        ),
    ]
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, statement) in schema_objects() {
        transaction.execute_batch(statement)?;
    }
    Ok(())
}
