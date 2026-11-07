use rusqlite::Connection;

#[cfg(test)]
use rusqlite::{OptionalExtension, params};

use crate::OperationsError;

const INTENTS_V64: &str = "CREATE TABLE ops_team_permanent_transfer_intents(transfer_id BLOB PRIMARY KEY NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)),source_team_id BLOB NOT NULL CHECK(length(source_team_id)=16 AND source_team_id!=zeroblob(16)),destination_team_id BLOB NOT NULL CHECK(length(destination_team_id)=16 AND destination_team_id!=zeroblob(16) AND destination_team_id!=source_team_id),source_membership_revision INTEGER NOT NULL CHECK(source_membership_revision>=1),source_leadership_revision INTEGER NOT NULL CHECK(source_leadership_revision>=1),source_leader_participation_id BLOB NOT NULL CHECK(length(source_leader_participation_id)=16 AND source_leader_participation_id!=zeroblob(16)),source_leader_profile_id BLOB NOT NULL CHECK(length(source_leader_profile_id)=16 AND source_leader_profile_id!=zeroblob(16)),destination_team_revision INTEGER NOT NULL CHECK(destination_team_revision>=1),destination_capacity_revision INTEGER NOT NULL CHECK(destination_capacity_revision>=1),destination_workspace_leadership_revision INTEGER NOT NULL CHECK(destination_workspace_leadership_revision>=1),target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id!=zeroblob(16)),target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id!=zeroblob(16)),destination_request_id BLOB NOT NULL CHECK(length(destination_request_id)=16 AND destination_request_id!=zeroblob(16)),destination_decision_id BLOB NOT NULL CHECK(length(destination_decision_id)=16 AND destination_decision_id!=zeroblob(16)),destination_ordinal INTEGER NOT NULL CHECK(destination_ordinal BETWEEN 0 AND 7),workspace_actor_activity_id BLOB NOT NULL CHECK(length(workspace_actor_activity_id)=16 AND workspace_actor_activity_id!=zeroblob(16)),workspace_actor_agent_id BLOB NOT NULL CHECK(length(workspace_actor_agent_id)=16 AND workspace_actor_agent_id!=zeroblob(16)),workspace_actor_session_id BLOB NOT NULL CHECK(length(workspace_actor_session_id)=16 AND workspace_actor_session_id!=zeroblob(16)),workspace_actor_participation_id BLOB NOT NULL CHECK(length(workspace_actor_participation_id)=16 AND workspace_actor_participation_id!=zeroblob(16)),workspace_actor_profile_id BLOB NOT NULL CHECK(length(workspace_actor_profile_id)=16 AND workspace_actor_profile_id!=zeroblob(16)),workspace_actor_participant_id BLOB NOT NULL CHECK(length(workspace_actor_participant_id)=16 AND workspace_actor_participant_id!=zeroblob(16)),workspace_actor_display_name TEXT NOT NULL CHECK(length(CAST(workspace_actor_display_name AS BLOB)) BETWEEN 1 AND 128),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32),UNIQUE(transfer_id,intent_digest),UNIQUE(workspace_id,destination_decision_id,destination_ordinal),FOREIGN KEY(workspace_id,source_team_id) REFERENCES ops_teams(workspace_id,team_id),FOREIGN KEY(workspace_id,destination_team_id,destination_decision_id,destination_request_id) REFERENCES ops_team_expansion_decisions(workspace_id,team_id,decision_id,request_id),FOREIGN KEY(workspace_id,destination_team_id,destination_decision_id,destination_ordinal) REFERENCES ops_team_expansion_slots(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,target_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id),FOREIGN KEY(workspace_id,source_leader_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id))";
const INTENTS_V65: &str = "CREATE TABLE ops_team_permanent_transfer_intents(transfer_id BLOB NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)),source_team_id BLOB NOT NULL CHECK(length(source_team_id)=16 AND source_team_id!=zeroblob(16)),destination_team_id BLOB NOT NULL CHECK(length(destination_team_id)=16 AND destination_team_id!=zeroblob(16) AND destination_team_id!=source_team_id),source_membership_revision INTEGER NOT NULL CHECK(source_membership_revision>=1),source_leadership_revision INTEGER NOT NULL CHECK(source_leadership_revision>=1),source_leader_participation_id BLOB NOT NULL CHECK(length(source_leader_participation_id)=16 AND source_leader_participation_id!=zeroblob(16)),source_leader_profile_id BLOB NOT NULL CHECK(length(source_leader_profile_id)=16 AND source_leader_profile_id!=zeroblob(16)),destination_team_revision INTEGER NOT NULL CHECK(destination_team_revision>=1),destination_capacity_revision INTEGER NOT NULL CHECK(destination_capacity_revision>=1),destination_workspace_leadership_revision INTEGER NOT NULL CHECK(destination_workspace_leadership_revision>=1),target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id!=zeroblob(16)),target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id!=zeroblob(16)),destination_request_id BLOB NOT NULL CHECK(length(destination_request_id)=16 AND destination_request_id!=zeroblob(16)),destination_decision_id BLOB NOT NULL CHECK(length(destination_decision_id)=16 AND destination_decision_id!=zeroblob(16)),destination_ordinal INTEGER NOT NULL CHECK(destination_ordinal BETWEEN 0 AND 7),workspace_actor_activity_id BLOB NOT NULL CHECK(length(workspace_actor_activity_id)=16 AND workspace_actor_activity_id!=zeroblob(16)),workspace_actor_agent_id BLOB NOT NULL CHECK(length(workspace_actor_agent_id)=16 AND workspace_actor_agent_id!=zeroblob(16)),workspace_actor_session_id BLOB NOT NULL CHECK(length(workspace_actor_session_id)=16 AND workspace_actor_session_id!=zeroblob(16)),workspace_actor_participation_id BLOB NOT NULL CHECK(length(workspace_actor_participation_id)=16 AND workspace_actor_participation_id!=zeroblob(16)),workspace_actor_profile_id BLOB NOT NULL CHECK(length(workspace_actor_profile_id)=16 AND workspace_actor_profile_id!=zeroblob(16)),workspace_actor_participant_id BLOB NOT NULL CHECK(length(workspace_actor_participant_id)=16 AND workspace_actor_participant_id!=zeroblob(16)),workspace_actor_display_name TEXT NOT NULL CHECK(length(CAST(workspace_actor_display_name AS BLOB)) BETWEEN 1 AND 128),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32),PRIMARY KEY(workspace_id,transfer_id),UNIQUE(workspace_id,transfer_id,intent_digest),UNIQUE(workspace_id,destination_decision_id,destination_ordinal),FOREIGN KEY(workspace_id,source_team_id) REFERENCES ops_teams(workspace_id,team_id),FOREIGN KEY(workspace_id,destination_team_id,destination_decision_id,destination_request_id) REFERENCES ops_team_expansion_decisions(workspace_id,team_id,decision_id,request_id),FOREIGN KEY(workspace_id,destination_team_id,destination_decision_id,destination_ordinal) REFERENCES ops_team_expansion_slots(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,target_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id),FOREIGN KEY(workspace_id,source_leader_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id))";
const INTENTS: &str = "CREATE TABLE ops_team_permanent_transfer_intents(transfer_id BLOB NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)),source_team_id BLOB NOT NULL CHECK(length(source_team_id)=16 AND source_team_id!=zeroblob(16)),destination_team_id BLOB NOT NULL CHECK(length(destination_team_id)=16 AND destination_team_id!=zeroblob(16) AND destination_team_id!=source_team_id),source_membership_revision INTEGER NOT NULL CHECK(source_membership_revision>=1),source_leadership_revision INTEGER NOT NULL CHECK(source_leadership_revision>=1),source_leader_participation_id BLOB NOT NULL CHECK(length(source_leader_participation_id)=16 AND source_leader_participation_id!=zeroblob(16)),source_leader_profile_id BLOB NOT NULL CHECK(length(source_leader_profile_id)=16 AND source_leader_profile_id!=zeroblob(16)),destination_team_revision INTEGER NOT NULL CHECK(destination_team_revision>=1),destination_capacity_revision INTEGER NOT NULL CHECK(destination_capacity_revision>=1),destination_workspace_leadership_revision INTEGER NOT NULL CHECK(destination_workspace_leadership_revision>=1),target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id!=zeroblob(16)),target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id!=zeroblob(16)),destination_request_id BLOB NOT NULL CHECK(length(destination_request_id)=16 AND destination_request_id!=zeroblob(16)),destination_decision_id BLOB NOT NULL CHECK(length(destination_decision_id)=16 AND destination_decision_id!=zeroblob(16)),destination_ordinal INTEGER NOT NULL CHECK(destination_ordinal BETWEEN 0 AND 7),workspace_actor_activity_id BLOB NOT NULL CHECK(length(workspace_actor_activity_id)=16 AND workspace_actor_activity_id!=zeroblob(16)),workspace_actor_agent_id BLOB NOT NULL CHECK(length(workspace_actor_agent_id)=16 AND workspace_actor_agent_id!=zeroblob(16)),workspace_actor_session_id BLOB NOT NULL CHECK(length(workspace_actor_session_id)=16 AND workspace_actor_session_id!=zeroblob(16)),workspace_actor_participation_id BLOB NOT NULL CHECK(length(workspace_actor_participation_id)=16 AND workspace_actor_participation_id!=zeroblob(16)),workspace_actor_profile_id BLOB NOT NULL CHECK(length(workspace_actor_profile_id)=16 AND workspace_actor_profile_id!=zeroblob(16)),workspace_actor_participant_id BLOB NOT NULL CHECK(length(workspace_actor_participant_id)=16 AND workspace_actor_participant_id!=zeroblob(16)),workspace_actor_display_name TEXT NOT NULL CHECK(length(CAST(workspace_actor_display_name AS BLOB)) BETWEEN 1 AND 128),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32),PRIMARY KEY(workspace_id,transfer_id),UNIQUE(workspace_id,transfer_id,intent_digest),FOREIGN KEY(workspace_id,source_team_id) REFERENCES ops_teams(workspace_id,team_id),FOREIGN KEY(workspace_id,destination_team_id,destination_decision_id,destination_request_id) REFERENCES ops_team_expansion_decisions(workspace_id,team_id,decision_id,request_id),FOREIGN KEY(workspace_id,destination_team_id,destination_decision_id,destination_ordinal) REFERENCES ops_team_expansion_slots(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,target_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id),FOREIGN KEY(workspace_id,source_leader_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id))";
const INTENTS_UPDATE: &str = "CREATE TRIGGER ops_team_permanent_transfer_intents_no_update BEFORE UPDATE ON ops_team_permanent_transfer_intents BEGIN SELECT RAISE(ABORT,'permanent Team transfer intents are immutable'); END";
const INTENTS_DELETE: &str = "CREATE TRIGGER ops_team_permanent_transfer_intents_no_delete BEFORE DELETE ON ops_team_permanent_transfer_intents BEGIN SELECT RAISE(ABORT,'permanent Team transfer intents are immutable'); END";

const CONSENTS_V64: &str = "CREATE TABLE ops_team_permanent_transfer_consents(receipt_id BLOB PRIMARY KEY NOT NULL CHECK(length(receipt_id)=16 AND receipt_id!=zeroblob(16)),transfer_id BLOB NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)),consent_kind TEXT NOT NULL CHECK(consent_kind IN('SOURCE_HOME_LEADER','TARGET_AGENT')),authority_revision INTEGER NOT NULL CHECK(authority_revision>=1),membership_revision INTEGER NOT NULL CHECK(membership_revision>=1),actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id!=zeroblob(16)),actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id!=zeroblob(16)),actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id!=zeroblob(16)),actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)),actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)),actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16)),actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32),UNIQUE(transfer_id,consent_kind),FOREIGN KEY(transfer_id,intent_digest) REFERENCES ops_team_permanent_transfer_intents(transfer_id,intent_digest))";
const CONSENTS: &str = "CREATE TABLE ops_team_permanent_transfer_consents(receipt_id BLOB NOT NULL CHECK(length(receipt_id)=16 AND receipt_id!=zeroblob(16)),transfer_id BLOB NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)),consent_kind TEXT NOT NULL CHECK(consent_kind IN('SOURCE_HOME_LEADER','TARGET_AGENT')),authority_revision INTEGER NOT NULL CHECK(authority_revision>=1),membership_revision INTEGER NOT NULL CHECK(membership_revision>=1),actor_activity_id BLOB NOT NULL CHECK(length(actor_activity_id)=16 AND actor_activity_id!=zeroblob(16)),actor_agent_id BLOB NOT NULL CHECK(length(actor_agent_id)=16 AND actor_agent_id!=zeroblob(16)),actor_session_id BLOB NOT NULL CHECK(length(actor_session_id)=16 AND actor_session_id!=zeroblob(16)),actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)),actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)),actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16)),actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32),PRIMARY KEY(workspace_id,receipt_id),UNIQUE(workspace_id,transfer_id,consent_kind),FOREIGN KEY(workspace_id,transfer_id,intent_digest) REFERENCES ops_team_permanent_transfer_intents(workspace_id,transfer_id,intent_digest))";
const CONSENTS_UPDATE: &str = "CREATE TRIGGER ops_team_permanent_transfer_consents_no_update BEFORE UPDATE ON ops_team_permanent_transfer_consents BEGIN SELECT RAISE(ABORT,'permanent Team transfer consent receipts are immutable'); END";
const CONSENTS_DELETE: &str = "CREATE TRIGGER ops_team_permanent_transfer_consents_no_delete BEFORE DELETE ON ops_team_permanent_transfer_consents BEGIN SELECT RAISE(ABORT,'permanent Team transfer consent receipts are immutable'); END";

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_team_permanent_transfer_intents", INTENTS),
        (
            "trigger",
            "ops_team_permanent_transfer_intents_no_update",
            INTENTS_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_intents_no_delete",
            INTENTS_DELETE,
        ),
        ("table", "ops_team_permanent_transfer_consents", CONSENTS),
        (
            "trigger",
            "ops_team_permanent_transfer_consents_no_update",
            CONSENTS_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_consents_no_delete",
            CONSENTS_DELETE,
        ),
    ]
}

pub(crate) const fn schema_objects_v65() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_team_permanent_transfer_intents", INTENTS_V65),
        (
            "trigger",
            "ops_team_permanent_transfer_intents_no_update",
            INTENTS_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_intents_no_delete",
            INTENTS_DELETE,
        ),
        ("table", "ops_team_permanent_transfer_consents", CONSENTS),
        (
            "trigger",
            "ops_team_permanent_transfer_consents_no_update",
            CONSENTS_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_consents_no_delete",
            CONSENTS_DELETE,
        ),
    ]
}

pub(crate) const fn schema_objects_v64() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_team_permanent_transfer_intents", INTENTS_V64),
        (
            "trigger",
            "ops_team_permanent_transfer_intents_no_update",
            INTENTS_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_intents_no_delete",
            INTENTS_DELETE,
        ),
        (
            "table",
            "ops_team_permanent_transfer_consents",
            CONSENTS_V64,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_consents_no_update",
            CONSENTS_UPDATE,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_consents_no_delete",
            CONSENTS_DELETE,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(crate) fn migrate_v64_to_v65(connection: &Connection) -> Result<(), OperationsError> {
    rebuild_base_schema(connection, schema_objects_v65())
}

pub(crate) fn migrate_v65_to_v66(connection: &Connection) -> Result<(), OperationsError> {
    rebuild_base_schema(connection, schema_objects())
}

fn rebuild_base_schema(
    connection: &Connection,
    objects: &[(&str, &str, &str)],
) -> Result<(), OperationsError> {
    // The owner invokes this inside its existing IMMEDIATE migration transaction.
    // Temporary copies do not enter the main schema inventory. Recreate the
    // canonical names directly: ALTER TABLE would quote the referenced name.
    connection.execute_batch(
        "CREATE TEMP TABLE transfer_migration_intents AS SELECT * FROM ops_team_permanent_transfer_intents;
         CREATE TEMP TABLE transfer_migration_consents AS SELECT * FROM ops_team_permanent_transfer_consents;
         DROP TRIGGER ops_team_permanent_transfer_consents_no_delete;
         DROP TRIGGER ops_team_permanent_transfer_consents_no_update;
         DROP TABLE ops_team_permanent_transfer_consents;
         DROP TRIGGER ops_team_permanent_transfer_intents_no_delete;
         DROP TRIGGER ops_team_permanent_transfer_intents_no_update;
         DROP TABLE ops_team_permanent_transfer_intents;",
    )?;
    for &(_, _, sql) in objects {
        connection.execute_batch(sql)?;
    }
    connection.execute_batch(
        "INSERT INTO ops_team_permanent_transfer_intents SELECT * FROM transfer_migration_intents;
         INSERT INTO ops_team_permanent_transfer_consents SELECT * FROM transfer_migration_consents;",
    )?;
    for (current, backup) in [
        (
            "ops_team_permanent_transfer_intents",
            "transfer_migration_intents",
        ),
        (
            "ops_team_permanent_transfer_consents",
            "transfer_migration_consents",
        ),
    ] {
        let count_matches = connection.query_row(
            &format!("SELECT (SELECT COUNT(*) FROM {current})=(SELECT COUNT(*) FROM {backup})"),
            [],
            |row| row.get::<_, bool>(0),
        )?;
        let differing: bool = connection.query_row(
            &format!("SELECT EXISTS(SELECT * FROM {current} EXCEPT SELECT * FROM {backup}) OR EXISTS(SELECT * FROM {backup} EXCEPT SELECT * FROM {current})"),
            [],
            |row| row.get(0),
        )?;
        if !count_matches || differing {
            return Err(OperationsError::Corrupt(
                "permanent transfer migration rows",
            ));
        }
    }
    connection.execute_batch(
        "DROP TABLE temp.transfer_migration_consents; DROP TABLE temp.transfer_migration_intents;",
    )?;
    Ok(())
}

#[cfg(test)]
pub(super) fn validate_schema(connection: &Connection) -> Result<(), OperationsError> {
    for &(kind, name, expected) in schema_objects() {
        let actual = connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type=?1 AND name=?2",
                params![kind, name],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or(OperationsError::Corrupt(
                "permanent Team transfer schema object missing",
            ))?;
        if actual != expected {
            return Err(OperationsError::Corrupt(
                "permanent Team transfer schema object changed",
            ));
        }
    }
    Ok(())
}
