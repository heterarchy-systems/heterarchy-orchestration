//! Immutable source receipt for newly created Team rosters.
//!
//! Schema-58 Team rows remain readable but are never promoted to verified
//! creation evidence. New creates record this receipt in the same Operations
//! transaction that writes the Team, roster, membership events, and capacity.

use std::collections::BTreeSet;

use forge_domain::{TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{OperationsError, teams, wire};

const SOURCE_VERSION: i64 = 1;
const DIGEST_DOMAIN: &[u8] = b"heterarchy-orchestration-team-create-source/v1\0";

const RECEIPTS_SQL: &str = r"
CREATE TABLE ops_team_create_source_receipts (
    request_id BLOB PRIMARY KEY NOT NULL
        CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)),
    source_version INTEGER NOT NULL CHECK(source_version=1),
    member_count INTEGER NOT NULL CHECK(member_count BETWEEN 2 AND 64),
    full_source_digest BLOB NOT NULL CHECK(length(full_source_digest)=32),
    recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),
    UNIQUE(workspace_id,team_id,request_id),
    FOREIGN KEY(request_id) REFERENCES ops_team_requests(request_id),
    FOREIGN KEY(workspace_id,team_id) REFERENCES ops_teams(workspace_id,team_id)
);
";
const LEGACY_SQL: &str = r"
CREATE TABLE ops_team_create_source_legacy (
    request_id BLOB PRIMARY KEY NOT NULL
        CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)),
    migrated_from_version INTEGER NOT NULL CHECK(migrated_from_version=58),
    UNIQUE(workspace_id,team_id,request_id),
    FOREIGN KEY(request_id) REFERENCES ops_team_requests(request_id),
    FOREIGN KEY(workspace_id,team_id) REFERENCES ops_teams(workspace_id,team_id)
);
";
const RECEIPTS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_create_source_receipts_no_update BEFORE UPDATE ON ops_team_create_source_receipts BEGIN SELECT RAISE(ABORT,'Team create source receipts are immutable'); END";
const RECEIPTS_NO_DELETE: &str = "CREATE TRIGGER ops_team_create_source_receipts_no_delete BEFORE DELETE ON ops_team_create_source_receipts BEGIN SELECT RAISE(ABORT,'Team create source receipts are retained'); END";
const LEGACY_NO_UPDATE: &str = "CREATE TRIGGER ops_team_create_source_legacy_no_update BEFORE UPDATE ON ops_team_create_source_legacy BEGIN SELECT RAISE(ABORT,'Team create legacy source markers are immutable'); END";
const LEGACY_NO_DELETE: &str = "CREATE TRIGGER ops_team_create_source_legacy_no_delete BEFORE DELETE ON ops_team_create_source_legacy BEGIN SELECT RAISE(ABORT,'Team create legacy source markers are retained'); END";
const ROSTER_NO_UPDATE: &str = "CREATE TRIGGER ops_team_create_roster_no_update BEFORE UPDATE ON ops_team_create_roster BEGIN SELECT RAISE(ABORT,'Team create roster source is immutable'); END";
const ROSTER_NO_DELETE: &str = "CREATE TRIGGER ops_team_create_roster_no_delete BEFORE DELETE ON ops_team_create_roster BEGIN SELECT RAISE(ABORT,'Team create roster source is retained'); END";
const EVENTS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_member_events_no_update BEFORE UPDATE ON ops_team_member_events BEGIN SELECT RAISE(ABORT,'Team membership events are immutable'); END";
const EVENTS_NO_DELETE: &str = "CREATE TRIGGER ops_team_member_events_no_delete BEFORE DELETE ON ops_team_member_events BEGIN SELECT RAISE(ABORT,'Team membership events are retained'); END";
const ROSTER_INSERT_CLOSED: &str = "CREATE TRIGGER ops_team_create_roster_insert_closed BEFORE INSERT ON ops_team_create_roster WHEN EXISTS(SELECT 1 FROM ops_team_create_source_receipts WHERE request_id=NEW.request_id) OR EXISTS(SELECT 1 FROM ops_team_create_source_legacy WHERE request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'Team create roster source is closed'); END";
const EVENTS_INSERT_CLOSED: &str = "CREATE TRIGGER ops_team_member_events_insert_closed BEFORE INSERT ON ops_team_member_events WHEN EXISTS(SELECT 1 FROM ops_team_create_source_receipts WHERE request_id=NEW.request_id) OR EXISTS(SELECT 1 FROM ops_team_create_source_legacy WHERE request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'Team create membership event source is closed'); END";

const SCHEMA_OBJECTS: &[(&str, &str, &str)] = &[
    ("table", "ops_team_create_source_receipts", RECEIPTS_SQL),
    ("table", "ops_team_create_source_legacy", LEGACY_SQL),
    (
        "trigger",
        "ops_team_create_source_receipts_no_update",
        RECEIPTS_NO_UPDATE,
    ),
    (
        "trigger",
        "ops_team_create_source_receipts_no_delete",
        RECEIPTS_NO_DELETE,
    ),
    (
        "trigger",
        "ops_team_create_source_legacy_no_update",
        LEGACY_NO_UPDATE,
    ),
    (
        "trigger",
        "ops_team_create_source_legacy_no_delete",
        LEGACY_NO_DELETE,
    ),
    (
        "trigger",
        "ops_team_create_roster_no_update",
        ROSTER_NO_UPDATE,
    ),
    (
        "trigger",
        "ops_team_create_roster_no_delete",
        ROSTER_NO_DELETE,
    ),
    (
        "trigger",
        "ops_team_member_events_no_update",
        EVENTS_NO_UPDATE,
    ),
    (
        "trigger",
        "ops_team_member_events_no_delete",
        EVENTS_NO_DELETE,
    ),
    (
        "trigger",
        "ops_team_create_roster_insert_closed",
        ROSTER_INSERT_CLOSED,
    ),
    (
        "trigger",
        "ops_team_member_events_insert_closed",
        EVENTS_INSERT_CLOSED,
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TeamCreateSourceStatus {
    Verified,
    LegacyUnverified,
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in SCHEMA_OBJECTS {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(crate) fn migrate_v58_to_v59(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    create_schema(transaction)?;
    transaction.execute(
        "INSERT INTO ops_team_create_source_legacy(request_id,workspace_id,team_id,migrated_from_version)
         SELECT request_id,workspace_id,team_id,58
         FROM ops_team_requests WHERE operation='CREATE'",
        [],
    )?;
    Ok(())
}

/// Test-only reconstruction of a source-less schema-58 CREATE. Production
/// code never deletes a verified receipt or converts its provenance class.
#[cfg(test)]
pub(crate) fn replace_verified_with_legacy_fixture(
    connection: &mut Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(), OperationsError> {
    let transaction = connection.transaction()?;
    transaction.execute_batch("DROP TRIGGER ops_team_create_source_receipts_no_delete;")?;
    let deleted = transaction.execute(
        "DELETE FROM ops_team_create_source_receipts
         WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3",
        params![
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;
    if deleted != 1 {
        return Err(OperationsError::Corrupt(
            "Team create source fixture receipt",
        ));
    }
    transaction.execute(
        "INSERT INTO ops_team_create_source_legacy(request_id,workspace_id,team_id,migrated_from_version)
         VALUES(?1,?2,?3,58)",
        params![
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;
    transaction.execute_batch(RECEIPTS_NO_DELETE)?;
    transaction.commit()?;
    Ok(())
}

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    SCHEMA_OBJECTS
}

pub(crate) fn record_verified(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    recorded_at: TimestampMillis,
) -> Result<(), OperationsError> {
    if legacy_marker_exists(transaction, request_id, workspace_id, team_id)? {
        return Err(OperationsError::Corrupt(
            "new Team create has legacy source marker",
        ));
    }
    let computed = compute_source(transaction, request_id, workspace_id, team_id)?;
    if computed.recorded_at != recorded_at.get() {
        return Err(OperationsError::Corrupt("Team create source timestamp"));
    }
    transaction.execute(
        "INSERT INTO ops_team_create_source_receipts(request_id,workspace_id,team_id,source_version,member_count,full_source_digest,recorded_at_ms)
         VALUES(?1,?2,?3,1,?4,?5,?6)",
        params![
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            i64::try_from(computed.member_count)
                .map_err(|_| OperationsError::Bounds("Team create source member count"))?,
            computed.digest.as_slice(),
            recorded_at.get(),
        ],
    )?;
    match validate(transaction, request_id, workspace_id, team_id)? {
        TeamCreateSourceStatus::Verified => Ok(()),
        TeamCreateSourceStatus::LegacyUnverified => {
            Err(OperationsError::Corrupt("new Team create source is legacy"))
        }
    }
}

/// Revalidate a stored source receipt against its complete persisted roster and
/// initial membership events. Schema-58 creates retain their historical replay
/// behavior but are never reported as verified source.
pub(crate) fn validate(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<TeamCreateSourceStatus, OperationsError> {
    let receipt = load_receipt(connection, request_id, workspace_id, team_id)?;
    let legacy = legacy_marker_exists(connection, request_id, workspace_id, team_id)?;
    match (receipt, legacy) {
        (Some(_), true) => Err(OperationsError::Corrupt(
            "Team create source has verified and legacy markers",
        )),
        (None, true) => Ok(TeamCreateSourceStatus::LegacyUnverified),
        (None, false) => Err(OperationsError::Corrupt(
            "Team create source receipt missing",
        )),
        (Some(receipt), false) => {
            let computed = compute_source(connection, request_id, workspace_id, team_id)?;
            if receipt.source_version != SOURCE_VERSION
                || receipt.member_count != computed.member_count
                || receipt.recorded_at != computed.recorded_at
                || receipt.digest != computed.digest
            {
                return Err(OperationsError::Corrupt(
                    "Team create source receipt mismatch",
                ));
            }
            Ok(TeamCreateSourceStatus::Verified)
        }
    }
}

/// Preserve source-less schema-58 readback while still validating any durable
/// v59 marker that exists. New capped creates call [`validate`] directly.
pub(crate) fn validate_legacy_or_absent(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<TeamCreateSourceStatus, OperationsError> {
    if load_receipt(connection, request_id, workspace_id, team_id)?.is_some()
        || legacy_marker_exists(connection, request_id, workspace_id, team_id)?
    {
        validate(connection, request_id, workspace_id, team_id)
    } else {
        Ok(TeamCreateSourceStatus::LegacyUnverified)
    }
}

struct SourceReceipt {
    source_version: i64,
    member_count: usize,
    digest: [u8; 32],
    recorded_at: i64,
}

struct ComputedSource {
    member_count: usize,
    digest: [u8; 32],
    recorded_at: i64,
}

fn load_receipt(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Option<SourceReceipt>, OperationsError> {
    connection
        .query_row(
            "SELECT source_version,member_count,full_source_digest,recorded_at_ms
             FROM ops_team_create_source_receipts
             WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3",
            params![
                request_id.as_bytes(),
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?
        .map(|(source_version, member_count, digest, recorded_at)| {
            let member_count = usize::try_from(member_count)
                .map_err(|_| OperationsError::Corrupt("Team create source member count"))?;
            let digest: [u8; 32] = digest
                .try_into()
                .map_err(|_| OperationsError::Corrupt("Team create source digest"))?;
            Ok(SourceReceipt {
                source_version,
                member_count,
                digest,
                recorded_at,
            })
        })
        .transpose()
}

fn legacy_marker_exists(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM ops_team_create_source_legacy
                WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3
             )",
            params![
                request_id.as_bytes(),
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
            ],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn compute_source(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<ComputedSource, OperationsError> {
    let request = load_create_request(connection, request_id, workspace_id, team_id)?.ok_or(
        OperationsError::Corrupt("Team create source request missing"),
    )?;
    let mut digest = Sha256::new();
    digest.update(DIGEST_DOMAIN);
    update_field(&mut digest, request_id.as_bytes())?;
    update_field(&mut digest, workspace_id.into_uuid().as_bytes())?;
    update_field(&mut digest, team_id.as_bytes())?;
    request.validate()?;
    request.update_digest(&mut digest)?;
    let capacity_source: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_team_capacity_events
            WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3
              AND operation='CREATE' AND capacity_revision=1
              AND expected_capacity_revision=0 AND expected_team_revision=0
              AND result_team_revision=1 AND recorded_at_ms=?4
         )",
        params![
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            request.recorded_at,
        ],
        |row| row.get(0),
    )?;
    if !capacity_source {
        return Err(OperationsError::Corrupt(
            "Team create capacity source missing",
        ));
    }

    let (roster_count, event_count, exact_participation_set) =
        source_cardinality(connection, request_id, workspace_id, team_id)?;
    if roster_count != event_count
        || !exact_participation_set
        || !(teams::MIN_FORMING_TEAM_MEMBERS..=teams::MAX_INITIAL_TEAM_MEMBERS)
            .contains(&roster_count)
    {
        return Err(OperationsError::Corrupt(
            "Team create source roster/event cardinality",
        ));
    }

    let mut statement = connection.prepare(
        "SELECT roster.ordinal,roster.participation_id,roster.activity_id,roster.agent_id,
                roster.session_id,roster.participant_id,roster.profile_id,roster.role,
                roster.membership_revision,roster.joined_at_ms,
                event.state,event.membership_revision,event.actor_key,event.actor_display_name,
                event.actor_participation_id,event.actor_profile_id,event.actor_participant_id,
                event.actor_session_id,event.observed_at_ms,event.role
         FROM ops_team_create_roster AS roster
         JOIN ops_team_member_events AS event
           ON event.request_id=roster.request_id
          AND event.workspace_id=roster.workspace_id
          AND event.team_id=roster.team_id
          AND event.participation_id=roster.participation_id
         WHERE roster.request_id=?1 AND roster.workspace_id=?2 AND roster.team_id=?3
         ORDER BY roster.ordinal",
    )?;
    let rows = statement.query_map(
        params![
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
        SourceMember::from_row,
    )?;
    let mut members = Vec::new();
    for row in rows {
        members.push(row?);
    }
    if members.len() != roster_count {
        return Err(OperationsError::Corrupt(
            "Team create source roster/event cardinality",
        ));
    }
    digest.update(
        u64::try_from(members.len())
            .map_err(|_| OperationsError::Bounds("Team create source member count"))?
            .to_be_bytes(),
    );
    let mut participations = BTreeSet::new();
    let mut participants = BTreeSet::new();
    for (expected_ordinal, member) in members.iter().enumerate() {
        member.validate(
            expected_ordinal,
            request.recorded_at,
            &request,
            &mut participations,
            &mut participants,
        )?;
        member.update_digest(&mut digest)?;
    }
    Ok(ComputedSource {
        member_count: members.len(),
        digest: digest.finalize().into(),
        recorded_at: request.recorded_at,
    })
}

fn source_cardinality(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(usize, usize, bool), OperationsError> {
    let (roster_count, event_count, exact_set): (i64, i64, bool) = connection.query_row(
        "SELECT
            (SELECT COUNT(*) FROM ops_team_create_roster
             WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3),
            (SELECT COUNT(*) FROM ops_team_member_events
             WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3),
            NOT EXISTS(
                SELECT 1 FROM ops_team_create_roster AS roster
                LEFT JOIN ops_team_member_events AS event
                  ON event.request_id=roster.request_id
                 AND event.workspace_id=roster.workspace_id
                 AND event.team_id=roster.team_id
                 AND event.participation_id=roster.participation_id
                WHERE roster.request_id=?1 AND roster.workspace_id=?2 AND roster.team_id=?3
                  AND event.participation_id IS NULL
            ) AND NOT EXISTS(
                SELECT 1 FROM ops_team_member_events AS event
                LEFT JOIN ops_team_create_roster AS roster
                  ON roster.request_id=event.request_id
                 AND roster.workspace_id=event.workspace_id
                 AND roster.team_id=event.team_id
                 AND roster.participation_id=event.participation_id
                WHERE event.request_id=?1 AND event.workspace_id=?2 AND event.team_id=?3
                  AND roster.participation_id IS NULL
            )",
        params![
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok((
        usize::try_from(roster_count)
            .map_err(|_| OperationsError::Corrupt("Team create source roster count"))?,
        usize::try_from(event_count)
            .map_err(|_| OperationsError::Corrupt("Team create source event count"))?,
        exact_set,
    ))
}

struct CreateRequestSource {
    actor_key: String,
    actor_display_name: String,
    actor_participation_id: Option<Vec<u8>>,
    actor_profile_id: Option<Vec<u8>>,
    actor_participant_id: Option<Vec<u8>>,
    actor_session_id: Option<Vec<u8>>,
    request_actor_kind: Option<String>,
    request_actor_activity_id: Option<Vec<u8>>,
    request_actor_agent_id: Option<Vec<u8>>,
    leadership_revision: Option<i64>,
    result_team_state: String,
    result_team_revision: i64,
    result_team_updated_at: i64,
    created: i64,
    recorded_at: i64,
}

impl CreateRequestSource {
    fn validate(&self) -> Result<(), OperationsError> {
        if self.request_actor_kind.as_deref() != Some("PARTICIPANT")
            || self.leadership_revision.is_none_or(|revision| revision < 1)
            || self.result_team_state != "ACTIVE"
            || self.result_team_revision != 1
            || self.result_team_updated_at != self.recorded_at
            || self.created != 1
            || self.recorded_at < 0
        {
            return Err(OperationsError::Corrupt("Team create request source"));
        }
        for (value, field) in [
            (
                self.actor_participation_id.as_ref(),
                "Team create actor participation",
            ),
            (self.actor_profile_id.as_ref(), "Team create actor Profile"),
            (
                self.actor_participant_id.as_ref(),
                "Team create actor participant",
            ),
            (self.actor_session_id.as_ref(), "Team create actor Session"),
            (
                self.request_actor_activity_id.as_ref(),
                "Team create request Activity",
            ),
            (
                self.request_actor_agent_id.as_ref(),
                "Team create request Agent",
            ),
        ] {
            let value = value.ok_or(OperationsError::Corrupt(field))?;
            wire::id(value.clone()).map_err(|_| OperationsError::Corrupt(field))?;
        }
        Ok(())
    }

    fn update_digest(&self, digest: &mut Sha256) -> Result<(), OperationsError> {
        for value in [
            self.actor_key.as_bytes(),
            self.actor_display_name.as_bytes(),
            self.result_team_state.as_bytes(),
        ] {
            update_field(digest, value)?;
        }
        for value in [
            self.actor_participation_id.as_deref(),
            self.actor_profile_id.as_deref(),
            self.actor_participant_id.as_deref(),
            self.actor_session_id.as_deref(),
            self.request_actor_kind.as_deref().map(str::as_bytes),
            self.request_actor_activity_id.as_deref(),
            self.request_actor_agent_id.as_deref(),
        ] {
            update_optional_field(digest, value)?;
        }
        digest.update(self.leadership_revision.unwrap_or_default().to_be_bytes());
        digest.update(self.result_team_revision.to_be_bytes());
        digest.update(self.result_team_updated_at.to_be_bytes());
        digest.update(self.created.to_be_bytes());
        digest.update(self.recorded_at.to_be_bytes());
        Ok(())
    }
}

fn load_create_request(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Option<CreateRequestSource>, OperationsError> {
    connection
        .query_row(
            "SELECT actor_key,actor_display_name,actor_participation_id,actor_profile_id,
                    actor_participant_id,actor_session_id,request_actor_kind,
                    request_actor_activity_id,request_actor_agent_id,request_leadership_revision,
                    result_team_state,result_team_revision,result_team_updated_at_ms,created,observed_at_ms
             FROM ops_team_requests
             WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3 AND operation='CREATE'",
            params![
                request_id.as_bytes(),
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
            ],
            |row| {
                Ok(CreateRequestSource {
                    actor_key: row.get(0)?,
                    actor_display_name: row.get(1)?,
                    actor_participation_id: row.get(2)?,
                    actor_profile_id: row.get(3)?,
                    actor_participant_id: row.get(4)?,
                    actor_session_id: row.get(5)?,
                    request_actor_kind: row.get(6)?,
                    request_actor_activity_id: row.get(7)?,
                    request_actor_agent_id: row.get(8)?,
                    leadership_revision: row.get(9)?,
                    result_team_state: row.get(10)?,
                    result_team_revision: row.get(11)?,
                    result_team_updated_at: row.get(12)?,
                    created: row.get(13)?,
                    recorded_at: row.get(14)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

struct SourceMember {
    ordinal: i64,
    participation_id: Vec<u8>,
    activity_id: Vec<u8>,
    agent_id: Vec<u8>,
    session_id: Vec<u8>,
    participant_id: Vec<u8>,
    profile_id: Vec<u8>,
    role: String,
    membership_revision: i64,
    joined_at: i64,
    event_state: String,
    event_revision: i64,
    event_actor_key: String,
    event_actor_display_name: String,
    event_actor_participation_id: Option<Vec<u8>>,
    event_actor_profile_id: Option<Vec<u8>>,
    event_actor_participant_id: Option<Vec<u8>>,
    event_actor_session_id: Option<Vec<u8>>,
    event_observed_at: i64,
    event_role: Option<String>,
}

impl SourceMember {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            ordinal: row.get(0)?,
            participation_id: row.get(1)?,
            activity_id: row.get(2)?,
            agent_id: row.get(3)?,
            session_id: row.get(4)?,
            participant_id: row.get(5)?,
            profile_id: row.get(6)?,
            role: row.get(7)?,
            membership_revision: row.get(8)?,
            joined_at: row.get(9)?,
            event_state: row.get(10)?,
            event_revision: row.get(11)?,
            event_actor_key: row.get(12)?,
            event_actor_display_name: row.get(13)?,
            event_actor_participation_id: row.get(14)?,
            event_actor_profile_id: row.get(15)?,
            event_actor_participant_id: row.get(16)?,
            event_actor_session_id: row.get(17)?,
            event_observed_at: row.get(18)?,
            event_role: row.get(19)?,
        })
    }

    fn validate(
        &self,
        expected_ordinal: usize,
        recorded_at: i64,
        request: &CreateRequestSource,
        participations: &mut BTreeSet<[u8; 16]>,
        participants: &mut BTreeSet<[u8; 16]>,
    ) -> Result<(), OperationsError> {
        if self.ordinal
            != i64::try_from(expected_ordinal)
                .map_err(|_| OperationsError::Bounds("Team create source ordinal"))?
            || self.membership_revision != 1
            || self.joined_at != recorded_at
            || self.event_state != "JOINED"
            || self.event_revision != 1
            || self.event_observed_at != recorded_at
            || self.event_role.as_deref() != Some(self.role.as_str())
            || self.event_actor_key != request.actor_key
            || self.event_actor_display_name != request.actor_display_name
            || self.event_actor_participation_id != request.actor_participation_id
            || self.event_actor_profile_id != request.actor_profile_id
            || self.event_actor_participant_id != request.actor_participant_id
            || self.event_actor_session_id != request.actor_session_id
        {
            return Err(OperationsError::Corrupt(
                "Team create source member generation",
            ));
        }
        teams::validate_team_role(&self.role)?;
        for (value, field) in [
            (&self.participation_id, "Team create source participation"),
            (&self.activity_id, "Team create source Activity"),
            (&self.agent_id, "Team create source Agent"),
            (&self.session_id, "Team create source Session"),
            (&self.participant_id, "Team create source participant"),
            (&self.profile_id, "Team create source Profile"),
        ] {
            wire::id(value.clone()).map_err(|_| OperationsError::Corrupt(field))?;
        }
        let participation = wire::id(self.participation_id.clone())?;
        let participant = wire::id(self.participant_id.clone())?;
        if !participations.insert(participation) || !participants.insert(participant) {
            return Err(OperationsError::Corrupt(
                "Team create source duplicate member",
            ));
        }
        Ok(())
    }

    fn update_digest(&self, digest: &mut Sha256) -> Result<(), OperationsError> {
        digest.update(self.ordinal.to_be_bytes());
        for value in [
            self.participation_id.as_slice(),
            self.activity_id.as_slice(),
            self.agent_id.as_slice(),
            self.session_id.as_slice(),
            self.participant_id.as_slice(),
            self.profile_id.as_slice(),
            self.role.as_bytes(),
            self.event_state.as_bytes(),
            self.event_actor_key.as_bytes(),
            self.event_actor_display_name.as_bytes(),
        ] {
            update_field(digest, value)?;
        }
        digest.update(self.membership_revision.to_be_bytes());
        digest.update(self.joined_at.to_be_bytes());
        digest.update(self.event_revision.to_be_bytes());
        for value in [
            self.event_actor_participation_id.as_deref(),
            self.event_actor_profile_id.as_deref(),
            self.event_actor_participant_id.as_deref(),
            self.event_actor_session_id.as_deref(),
            self.event_role.as_deref().map(str::as_bytes),
        ] {
            update_optional_field(digest, value)?;
        }
        digest.update(self.event_observed_at.to_be_bytes());
        Ok(())
    }
}

fn update_optional_field(digest: &mut Sha256, value: Option<&[u8]>) -> Result<(), OperationsError> {
    if let Some(value) = value {
        digest.update([1]);
        return update_field(digest, value);
    }
    digest.update([0]);
    Ok(())
}

fn update_field(digest: &mut Sha256, value: &[u8]) -> Result<(), OperationsError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| OperationsError::Bounds("Team create source digest field"))?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

#[cfg(test)]
mod tests;
