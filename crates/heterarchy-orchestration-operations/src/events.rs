//! Orchestration-owned immutable Agent events, presence, and inbox receipts.
//!
//! These tables deliberately contain no Forge Workspace, Activity, Goal, or
//! DAG foreign keys.  `ops_scopes` and `ops_members` are the normalized
//! Orchestration identities supplied by the parent module; this file only
//! preserves the typed snapshots and receipt revisions needed by the import
//! and cutover journal.

use forge_domain::{
    ActivityCoordinatorRevision, ActivityId, AgentEvent, AgentEventId, AgentEventKind,
    AgentInstanceId, AgentMembershipRevision, AgentParticipantRef, AgentPresenceRevision,
    AgentSessionId, AuthenticatedPrincipal, CoordinationSnapshot, CoordinatorSnapshot,
    DagHeadRevision, DagNodeId, DagNodeRevision, DagRevisionNumber, InboxReceiptRevision,
    TimestampMillis, WorkspaceId,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use sha2::{Digest as Sha2Digest, Sha256};
use uuid::Uuid;

use crate::OperationsError;

const ID_BYTES: usize = 16;
const MAX_LABEL_BYTES: usize = forge_domain::MAX_AGENT_EVENT_TEXT_BYTES;

const OPS_EVENTS_TABLE_SQL: &str = r"
CREATE TABLE ops_events (
    event_id BLOB NOT NULL CHECK(length(event_id) = 16),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id) = 16),
    activity_id BLOB NOT NULL CHECK(length(activity_id) = 16),
    membership_revision INTEGER NOT NULL CHECK(membership_revision >= 1),
    event_kind INTEGER NOT NULL CHECK(event_kind IN (0, 1, 2, 3, 4, 5)),
    from_agent_id BLOB CHECK(from_agent_id IS NULL OR length(from_agent_id) = 16),
    from_session_id BLOB CHECK(from_session_id IS NULL OR length(from_session_id) = 16),
    from_principal_kind INTEGER CHECK(from_principal_kind IS NULL OR from_principal_kind IN (0, 1, 2)),
    from_principal_id BLOB CHECK(from_principal_id IS NULL OR length(from_principal_id) = 16),
    from_display_name TEXT CHECK(from_display_name IS NULL OR length(CAST(from_display_name AS BLOB)) BETWEEN 1 AND 256),
    to_agent_id BLOB CHECK(to_agent_id IS NULL OR length(to_agent_id) = 16),
    to_session_id BLOB CHECK(to_session_id IS NULL OR length(to_session_id) = 16),
    to_principal_kind INTEGER CHECK(to_principal_kind IS NULL OR to_principal_kind IN (0, 1, 2)),
    to_principal_id BLOB CHECK(to_principal_id IS NULL OR length(to_principal_id) = 16),
    to_display_name TEXT CHECK(to_display_name IS NULL OR length(CAST(to_display_name AS BLOB)) BETWEEN 1 AND 256),
    before_goal_id BLOB CHECK(before_goal_id IS NULL OR length(before_goal_id) = 16),
    before_goal_revision INTEGER CHECK(before_goal_revision IS NULL OR before_goal_revision >= 1),
    before_dag_revision INTEGER CHECK(before_dag_revision IS NULL OR before_dag_revision >= 1),
    before_dag_head_revision INTEGER CHECK(before_dag_head_revision IS NULL OR before_dag_head_revision >= 1),
    before_head_id BLOB CHECK(before_head_id IS NULL OR length(before_head_id) = 16),
    before_head_revision INTEGER CHECK(before_head_revision IS NULL OR before_head_revision >= 1),
    after_goal_id BLOB CHECK(after_goal_id IS NULL OR length(after_goal_id) = 16),
    after_goal_revision INTEGER CHECK(after_goal_revision IS NULL OR after_goal_revision >= 1),
    after_dag_revision INTEGER CHECK(after_dag_revision IS NULL OR after_dag_revision >= 1),
    after_dag_head_revision INTEGER CHECK(after_dag_head_revision IS NULL OR after_dag_head_revision >= 1),
    after_head_id BLOB CHECK(after_head_id IS NULL OR length(after_head_id) = 16),
    after_head_revision INTEGER CHECK(after_head_revision IS NULL OR after_head_revision >= 1),
    before_coordinator_agent_id BLOB CHECK(before_coordinator_agent_id IS NULL OR length(before_coordinator_agent_id) = 16),
    before_coordinator_session_id BLOB CHECK(before_coordinator_session_id IS NULL OR length(before_coordinator_session_id) = 16),
    before_coordinator_revision INTEGER CHECK(before_coordinator_revision IS NULL OR before_coordinator_revision >= 1),
    after_coordinator_agent_id BLOB CHECK(after_coordinator_agent_id IS NULL OR length(after_coordinator_agent_id) = 16),
    after_coordinator_session_id BLOB CHECK(after_coordinator_session_id IS NULL OR length(after_coordinator_session_id) = 16),
    after_coordinator_revision INTEGER CHECK(after_coordinator_revision IS NULL OR after_coordinator_revision >= 1),
    observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms >= 0),
    PRIMARY KEY(workspace_id, event_id),
    UNIQUE(workspace_id, activity_id, membership_revision),
    FOREIGN KEY(workspace_id, activity_id) REFERENCES ops_scopes(workspace_id, activity_id)
);
";

const OPS_EVENTS_INDEX_SQL: &str = r"
CREATE INDEX ops_events_activity_revision_idx
ON ops_events(workspace_id, activity_id, membership_revision DESC, event_id DESC);
";

const OPS_PRESENCE_TABLE_SQL: &str = r"
CREATE TABLE ops_presence (
    workspace_id BLOB NOT NULL CHECK(length(workspace_id) = 16),
    activity_id BLOB NOT NULL CHECK(length(activity_id) = 16),
    agent_id BLOB NOT NULL CHECK(length(agent_id) = 16),
    session_id BLOB NOT NULL CHECK(length(session_id) = 16),
    last_authenticated_at_ms INTEGER NOT NULL CHECK(last_authenticated_at_ms >= 0),
    presence_revision INTEGER NOT NULL CHECK(presence_revision >= 1),
    PRIMARY KEY(workspace_id, agent_id),
    UNIQUE(workspace_id, activity_id, session_id),
    FOREIGN KEY(workspace_id, activity_id) REFERENCES ops_scopes(workspace_id, activity_id),
    FOREIGN KEY(workspace_id, agent_id) REFERENCES ops_members(workspace_id, agent_id)
);
";

const OPS_PRESENCE_INDEX_SQL: &str = r"
CREATE INDEX ops_presence_activity_idx
ON ops_presence(workspace_id, activity_id, last_authenticated_at_ms DESC, agent_id DESC);
";

const OPS_INBOX_TABLE_SQL: &str = r"
CREATE TABLE ops_inbox (
    workspace_id BLOB NOT NULL CHECK(length(workspace_id) = 16),
    recipient_agent_id BLOB NOT NULL CHECK(length(recipient_agent_id) = 16),
    recipient_session_id BLOB NOT NULL CHECK(length(recipient_session_id) = 16),
    event_id BLOB NOT NULL CHECK(length(event_id) = 16),
    receipt_revision INTEGER NOT NULL CHECK(receipt_revision >= 1),
    acknowledged_at_ms INTEGER CHECK(acknowledged_at_ms IS NULL OR acknowledged_at_ms >= 0),
    PRIMARY KEY(workspace_id, recipient_agent_id, recipient_session_id, event_id),
    FOREIGN KEY(workspace_id, recipient_agent_id) REFERENCES ops_members(workspace_id, agent_id),
    FOREIGN KEY(workspace_id, event_id) REFERENCES ops_events(workspace_id, event_id)
);
";

const OPS_INBOX_INDEX_SQL: &str = r"
CREATE INDEX ops_inbox_recipient_revision_idx
ON ops_inbox(
    workspace_id,
    recipient_agent_id,
    recipient_session_id,
    receipt_revision DESC,
    event_id DESC
);
";

const EVENT_SELECT: &str = "SELECT substr(event_id,1,17), substr(workspace_id,1,17), substr(activity_id,1,17), membership_revision, event_kind, substr(from_agent_id,1,17), substr(from_session_id,1,17), from_principal_kind, substr(from_principal_id,1,17), substr(CAST(from_display_name AS BLOB),1,257), substr(to_agent_id,1,17), substr(to_session_id,1,17), to_principal_kind, substr(to_principal_id,1,17), substr(CAST(to_display_name AS BLOB),1,257), substr(before_goal_id,1,17), before_goal_revision, before_dag_revision, before_dag_head_revision, substr(before_head_id,1,17), before_head_revision, substr(after_goal_id,1,17), after_goal_revision, after_dag_revision, after_dag_head_revision, substr(after_head_id,1,17), after_head_revision, substr(before_coordinator_agent_id,1,17), substr(before_coordinator_session_id,1,17), before_coordinator_revision, substr(after_coordinator_agent_id,1,17), substr(after_coordinator_session_id,1,17), after_coordinator_revision, observed_at_ms FROM ops_events WHERE workspace_id = ?1 AND event_id = ?2";

/// The canonical table/index definitions owned by this module.
pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_events", OPS_EVENTS_TABLE_SQL),
        (
            "index",
            "ops_events_activity_revision_idx",
            OPS_EVENTS_INDEX_SQL,
        ),
        ("table", "ops_presence", OPS_PRESENCE_TABLE_SQL),
        ("index", "ops_presence_activity_idx", OPS_PRESENCE_INDEX_SQL),
        ("table", "ops_inbox", OPS_INBOX_TABLE_SQL),
        (
            "index",
            "ops_inbox_recipient_revision_idx",
            OPS_INBOX_INDEX_SQL,
        ),
    ]
}

/// Create the normalized event, presence, and receipt tables.
pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Hash one event's complete semantic content in a fixed, length-framed order.
/// This is independent of SQLite encoding and is used by migration page
/// read-back; it deliberately includes all participant, coordination, and
/// Coordinator snapshots.
pub(crate) fn event_digest(event: &AgentEvent) -> Result<[u8; 32], OperationsError> {
    let mut digest = EventDigest::new();
    digest.bytes(0, b"heterarchy-orchestration-ops-event-v1");
    digest.bytes(1, event.id().into_uuid().as_bytes());
    digest.bytes(2, event.workspace_id().into_uuid().as_bytes());
    digest.bytes(3, event.activity_id().into_uuid().as_bytes());
    digest.i64(4, event.membership_revision().get());
    digest.i64(5, encode_event_kind(event.kind()));
    digest.participant(6, event.from())?;
    digest.participant(7, event.to())?;
    digest.coordination(8, event.coordination_before());
    digest.coordination(9, event.coordination_after());
    digest.coordinator(10, event.coordinator_before());
    digest.coordinator(11, event.coordinator_after());
    digest.i64(12, event.observed_at().get());
    Ok(digest.finish())
}

/// Immutable normalized event import/write. An identical existing row is a
/// successful retry; any differing payload is an identity conflict.
pub(crate) fn put_event(
    transaction: &Transaction<'_>,
    event: &AgentEvent,
) -> Result<(), OperationsError> {
    if let Some(existing) = get_event(transaction, event.workspace_id(), event.id())? {
        return if existing == *event {
            Ok(())
        } else {
            Err(OperationsError::Conflict("ops_events.identity"))
        };
    }
    let existing_revision: Option<Vec<u8>> = transaction
        .query_row(
            "SELECT event_id FROM ops_events WHERE workspace_id = ?1 AND activity_id = ?2 AND membership_revision = ?3",
            params![
                event.workspace_id().into_uuid().as_bytes().as_slice(),
                event.activity_id().into_uuid().as_bytes().as_slice(),
                event.membership_revision().get(),
            ],
            |row| row.get(0),
        )
        .optional()?;
    if existing_revision.as_deref() != Some(event.id().into_uuid().as_bytes().as_slice())
        && existing_revision.is_some()
    {
        return Err(OperationsError::Conflict("ops_events.membership_revision"));
    }

    let encoded = EncodedEvent::from_domain(event)?;
    transaction.execute(
        "INSERT INTO ops_events (event_id, workspace_id, activity_id, membership_revision, event_kind, from_agent_id, from_session_id, from_principal_kind, from_principal_id, from_display_name, to_agent_id, to_session_id, to_principal_kind, to_principal_id, to_display_name, before_goal_id, before_goal_revision, before_dag_revision, before_dag_head_revision, before_head_id, before_head_revision, after_goal_id, after_goal_revision, after_dag_revision, after_dag_head_revision, after_head_id, after_head_revision, before_coordinator_agent_id, before_coordinator_session_id, before_coordinator_revision, after_coordinator_agent_id, after_coordinator_session_id, after_coordinator_revision, observed_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34)",
        params![
            event.id().into_uuid().as_bytes().as_slice(),
            event.workspace_id().into_uuid().as_bytes().as_slice(),
            event.activity_id().into_uuid().as_bytes().as_slice(),
            event.membership_revision().get(),
            encode_event_kind(event.kind()),
            encoded.from.agent_id.as_deref(),
            encoded.from.session_id.as_deref(),
            encoded.from.principal_kind,
            encoded.from.principal_id.as_deref(),
            encoded.from.display_name.as_deref(),
            encoded.to.agent_id.as_deref(),
            encoded.to.session_id.as_deref(),
            encoded.to.principal_kind,
            encoded.to.principal_id.as_deref(),
            encoded.to.display_name.as_deref(),
            encoded.before.goal_id.as_deref(),
            encoded.before.goal_revision,
            encoded.before.dag_revision,
            encoded.before.dag_head_revision,
            encoded.before.node_id.as_deref(),
            encoded.before.node_revision,
            encoded.after.goal_id.as_deref(),
            encoded.after.goal_revision,
            encoded.after.dag_revision,
            encoded.after.dag_head_revision,
            encoded.after.node_id.as_deref(),
            encoded.after.node_revision,
            encoded.before_coordinator.0.as_deref(),
            encoded.before_coordinator.1.as_deref(),
            encoded.before_coordinator.2,
            encoded.after_coordinator.0.as_deref(),
            encoded.after_coordinator.1.as_deref(),
            encoded.after_coordinator.2,
            event.observed_at().get(),
        ],
    )?;

    let stored = get_event(transaction, event.workspace_id(), event.id())?
        .ok_or(OperationsError::Corrupt("ops_events.readback"))?;
    if stored != *event {
        return Err(OperationsError::Conflict("ops_events.readback"));
    }
    Ok(())
}

/// Read one exact event snapshot. Goal/DAG values are decoded as opaque typed
/// attribution only; no Forge repository rows are consulted.
pub(crate) fn get_event(
    connection: &Connection,
    workspace_id: WorkspaceId,
    event_id: AgentEventId,
) -> Result<Option<AgentEvent>, OperationsError> {
    let mut statement = connection.prepare(EVENT_SELECT)?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes().as_slice(),
        event_id.into_uuid().as_bytes().as_slice(),
    ])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    decode_event_checked(row).map(Some)
}

/// Read a bounded Activity-scoped page of imported event snapshots.
///
/// These rows are migration-time evidence, not current operational truth.
pub(crate) fn list_activity_events(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    limit: i64,
) -> Result<Vec<AgentEvent>, OperationsError> {
    if !(1..=65).contains(&limit) {
        return Err(OperationsError::Bounds("ops_events activity limit"));
    }
    let mut statement = connection.prepare(
        "SELECT substr(event_id,1,17) FROM ops_events WHERE workspace_id=?1 AND activity_id=?2 ORDER BY membership_revision DESC,event_id DESC LIMIT ?3",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes().as_slice(),
        activity_id.into_uuid().as_bytes().as_slice(),
        limit,
    ])?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next()? {
        let event_id: Vec<u8> = row.get(0)?;
        ids.push(AgentEventId::from_uuid(decode_uuid(
            &event_id,
            "ops_events.event_id",
        )?));
    }
    drop(rows);
    drop(statement);
    let mut events = Vec::with_capacity(ids.len());
    for event_id in ids {
        let event = get_event(connection, workspace_id, event_id)?
            .ok_or(OperationsError::Corrupt("ops_events activity readback"))?;
        if event.activity_id() != activity_id {
            return Err(OperationsError::Corrupt("ops_events activity scope"));
        }
        events.push(event);
    }
    Ok(events)
}

/// Durable normalized presence row used by import/read-back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceRecord {
    agent_id: AgentInstanceId,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    session_id: AgentSessionId,
    last_authenticated_at: TimestampMillis,
    revision: AgentPresenceRevision,
}

impl PresenceRecord {
    pub(crate) const fn new(
        agent_id: AgentInstanceId,
        workspace_id: WorkspaceId,
        activity_id: ActivityId,
        session_id: AgentSessionId,
        last_authenticated_at: TimestampMillis,
        revision: AgentPresenceRevision,
    ) -> Self {
        Self {
            agent_id,
            workspace_id,
            activity_id,
            session_id,
            last_authenticated_at,
            revision,
        }
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    #[must_use]
    pub const fn last_authenticated_at(&self) -> TimestampMillis {
        self.last_authenticated_at
    }

    pub const fn revision(&self) -> AgentPresenceRevision {
        self.revision
    }
}

/// Idempotently import one presence row.
pub(crate) fn put_presence(
    transaction: &Transaction<'_>,
    record: &PresenceRecord,
) -> Result<(), OperationsError> {
    if let Some(existing) = get_presence(transaction, record.workspace_id(), record.agent_id())? {
        return if existing == *record {
            Ok(())
        } else {
            Err(OperationsError::Conflict("ops_presence.identity"))
        };
    }
    let existing_session: Option<Vec<u8>> = transaction
        .query_row(
            "SELECT agent_id FROM ops_presence WHERE workspace_id = ?1 AND activity_id = ?2 AND session_id = ?3",
            params![
                record.workspace_id().into_uuid().as_bytes().as_slice(),
                record.activity_id().into_uuid().as_bytes().as_slice(),
                record.session_id().into_uuid().as_bytes().as_slice(),
            ],
            |row| row.get(0),
        )
        .optional()?;
    if existing_session.as_deref() != Some(record.agent_id().into_uuid().as_bytes().as_slice())
        && existing_session.is_some()
    {
        return Err(OperationsError::Conflict("ops_presence.session"));
    }
    transaction.execute(
        "INSERT INTO ops_presence (workspace_id, activity_id, agent_id, session_id, last_authenticated_at_ms, presence_revision) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            record.workspace_id().into_uuid().as_bytes().as_slice(),
            record.activity_id().into_uuid().as_bytes().as_slice(),
            record.agent_id().into_uuid().as_bytes().as_slice(),
            record.session_id().into_uuid().as_bytes().as_slice(),
            record.last_authenticated_at().get(),
            record.revision().get(),
        ],
    )?;
    let stored = get_presence(transaction, record.workspace_id(), record.agent_id())?
        .ok_or(OperationsError::Corrupt("ops_presence.readback"))?;
    if stored != *record {
        return Err(OperationsError::Conflict("ops_presence.readback"));
    }
    Ok(())
}

pub(crate) fn get_presence(
    connection: &Connection,
    workspace_id: WorkspaceId,
    agent_id: AgentInstanceId,
) -> Result<Option<PresenceRecord>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(agent_id,1,17), substr(workspace_id,1,17), substr(activity_id,1,17), substr(session_id,1,17), last_authenticated_at_ms, presence_revision FROM ops_presence WHERE workspace_id = ?1 AND agent_id = ?2",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes().as_slice(),
        agent_id.into_uuid().as_bytes().as_slice(),
    ])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    decode_presence_checked(row).map(Some)
}

/// Durable normalized inbox receipt used by import/read-back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxReceiptRecord {
    agent_id: AgentInstanceId,
    workspace_id: WorkspaceId,
    session_id: AgentSessionId,
    event_id: AgentEventId,
    receipt_revision: InboxReceiptRevision,
    acknowledged_at: Option<TimestampMillis>,
}

impl InboxReceiptRecord {
    pub(crate) const fn new(
        agent_id: AgentInstanceId,
        workspace_id: WorkspaceId,
        session_id: AgentSessionId,
        event_id: AgentEventId,
        receipt_revision: InboxReceiptRevision,
        acknowledged_at: Option<TimestampMillis>,
    ) -> Self {
        Self {
            agent_id,
            workspace_id,
            session_id,
            event_id,
            receipt_revision,
            acknowledged_at,
        }
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    pub const fn event_id(&self) -> AgentEventId {
        self.event_id
    }

    pub const fn receipt_revision(&self) -> InboxReceiptRevision {
        self.receipt_revision
    }

    #[must_use]
    pub const fn acknowledged_at(&self) -> Option<TimestampMillis> {
        self.acknowledged_at
    }
}

/// Idempotently import one inbox receipt.
pub(crate) fn put_receipt(
    transaction: &Transaction<'_>,
    record: &InboxReceiptRecord,
) -> Result<(), OperationsError> {
    if let Some(existing) = get_receipt(
        transaction,
        record.workspace_id(),
        record.agent_id(),
        record.session_id(),
        record.event_id(),
    )? {
        return if existing == *record {
            Ok(())
        } else {
            Err(OperationsError::Conflict("ops_inbox.identity"))
        };
    }
    transaction.execute(
        "INSERT INTO ops_inbox (workspace_id, recipient_agent_id, recipient_session_id, event_id, receipt_revision, acknowledged_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            record.workspace_id().into_uuid().as_bytes().as_slice(),
            record.agent_id().into_uuid().as_bytes().as_slice(),
            record.session_id().into_uuid().as_bytes().as_slice(),
            record.event_id().into_uuid().as_bytes().as_slice(),
            record.receipt_revision().get(),
            record.acknowledged_at().map(TimestampMillis::get),
        ],
    )?;
    let stored = get_receipt(
        transaction,
        record.workspace_id(),
        record.agent_id(),
        record.session_id(),
        record.event_id(),
    )?
    .ok_or(OperationsError::Corrupt("ops_inbox.readback"))?;
    if stored != *record {
        return Err(OperationsError::Conflict("ops_inbox.readback"));
    }
    Ok(())
}

pub(crate) fn get_receipt(
    connection: &Connection,
    workspace_id: WorkspaceId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    event_id: AgentEventId,
) -> Result<Option<InboxReceiptRecord>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(recipient_agent_id,1,17), substr(workspace_id,1,17), substr(recipient_session_id,1,17), substr(event_id,1,17), receipt_revision, acknowledged_at_ms FROM ops_inbox WHERE workspace_id = ?1 AND recipient_agent_id = ?2 AND recipient_session_id = ?3 AND event_id = ?4",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes().as_slice(),
        agent_id.into_uuid().as_bytes().as_slice(),
        session_id.into_uuid().as_bytes().as_slice(),
        event_id.into_uuid().as_bytes().as_slice(),
    ])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    decode_receipt_checked(row).map(Some)
}

#[derive(Default)]
struct EncodedParticipant {
    agent_id: Option<Vec<u8>>,
    session_id: Option<Vec<u8>>,
    principal_kind: Option<i64>,
    principal_id: Option<Vec<u8>>,
    display_name: Option<String>,
}

struct EncodedEvent {
    from: EncodedParticipant,
    to: EncodedParticipant,
    before: EncodedCoordination,
    after: EncodedCoordination,
    before_coordinator: EncodedCoordinator,
    after_coordinator: EncodedCoordinator,
}

struct EncodedCoordination {
    goal_id: Option<Vec<u8>>,
    goal_revision: Option<i64>,
    dag_revision: Option<i64>,
    dag_head_revision: Option<i64>,
    node_id: Option<Vec<u8>>,
    node_revision: Option<i64>,
}
type EncodedCoordinator = (Option<Vec<u8>>, Option<Vec<u8>>, Option<i64>);

impl EncodedEvent {
    fn from_domain(event: &AgentEvent) -> Result<Self, OperationsError> {
        Ok(Self {
            from: encode_participant(event.from())?,
            to: encode_participant(event.to())?,
            before: encode_coordination(event.coordination_before()),
            after: encode_coordination(event.coordination_after()),
            before_coordinator: encode_coordinator(event.coordinator_before()),
            after_coordinator: encode_coordinator(event.coordinator_after()),
        })
    }
}

struct EventDigest {
    hasher: Sha256,
}

impl EventDigest {
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    fn bytes(&mut self, field: u8, value: &[u8]) {
        self.hasher.update([field]);
        self.hasher.update((value.len() as u64).to_be_bytes());
        self.hasher.update(value);
    }

    fn optional_bytes(&mut self, field: u8, value: Option<&[u8]>) {
        self.hasher.update([field, u8::from(value.is_some())]);
        if let Some(value) = value {
            self.hasher.update((value.len() as u64).to_be_bytes());
            self.hasher.update(value);
        }
    }

    fn optional_uuid(&mut self, field: u8, value: Option<Uuid>) {
        self.hasher.update([field, u8::from(value.is_some())]);
        if let Some(value) = value {
            self.hasher.update(value.as_bytes());
        }
    }

    fn i64(&mut self, field: u8, value: i64) {
        self.hasher.update([field]);
        self.hasher.update(value.to_be_bytes());
    }

    fn optional_i64(&mut self, field: u8, value: Option<i64>) {
        self.hasher.update([field, u8::from(value.is_some())]);
        if let Some(value) = value {
            self.hasher.update(value.to_be_bytes());
        }
    }

    fn participant(
        &mut self,
        field: u8,
        participant: Option<&AgentParticipantRef>,
    ) -> Result<(), OperationsError> {
        let Some(participant) = participant else {
            self.hasher.update([field, 0]);
            return Ok(());
        };
        if participant.display_name().byte_len() > MAX_LABEL_BYTES {
            return Err(OperationsError::Bounds("ops_events.display_name"));
        }
        self.hasher.update([field, 1]);
        self.bytes(
            field.wrapping_add(1),
            participant.agent_id().into_uuid().as_bytes(),
        );
        self.optional_uuid(
            field.wrapping_add(2),
            participant.session_id().map(AgentSessionId::into_uuid),
        );
        let (principal_kind, principal_id) = encode_principal(participant.principal());
        self.i64(field.wrapping_add(3), principal_kind);
        self.optional_bytes(field.wrapping_add(4), principal_id.as_deref());
        self.bytes(
            field.wrapping_add(5),
            participant.display_name().as_str().as_bytes(),
        );
        Ok(())
    }

    fn coordination(&mut self, field: u8, snapshot: CoordinationSnapshot) {
        self.hasher.update([field]);
        self.optional_uuid(
            field.wrapping_add(1),
            snapshot.goal().map(|value| value.goal_id().into_uuid()),
        );
        self.optional_i64(
            field.wrapping_add(2),
            snapshot.goal().map(|value| value.expected_revision().get()),
        );
        self.optional_i64(
            field.wrapping_add(3),
            snapshot.dag_revision().map(DagRevisionNumber::get),
        );
        self.optional_i64(
            field.wrapping_add(4),
            snapshot.dag_head_revision().map(DagHeadRevision::get),
        );
        self.optional_uuid(
            field.wrapping_add(5),
            snapshot.node().map(|value| value.node_id().into_uuid()),
        );
        self.optional_i64(
            field.wrapping_add(6),
            snapshot.node().map(|value| value.expected_revision().get()),
        );
    }

    fn coordinator(&mut self, field: u8, snapshot: CoordinatorSnapshot) {
        self.hasher.update([field]);
        self.optional_uuid(
            field.wrapping_add(1),
            snapshot.agent_id().map(AgentInstanceId::into_uuid),
        );
        self.optional_uuid(
            field.wrapping_add(2),
            snapshot.session_id().map(AgentSessionId::into_uuid),
        );
        self.optional_i64(
            field.wrapping_add(3),
            snapshot.revision().map(ActivityCoordinatorRevision::get),
        );
    }

    fn finish(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

fn encode_participant(
    participant: Option<&AgentParticipantRef>,
) -> Result<EncodedParticipant, OperationsError> {
    let Some(participant) = participant else {
        return Ok(EncodedParticipant::default());
    };
    if participant.display_name().byte_len() > MAX_LABEL_BYTES {
        return Err(OperationsError::Bounds("ops_events.display_name"));
    }
    let (principal_kind, principal_id) = encode_principal(participant.principal());
    Ok(EncodedParticipant {
        agent_id: Some(participant.agent_id().into_uuid().as_bytes().to_vec()),
        session_id: participant
            .session_id()
            .map(|value| value.into_uuid().as_bytes().to_vec()),
        principal_kind: Some(principal_kind),
        principal_id,
        display_name: Some(participant.display_name().as_str().to_owned()),
    })
}

fn encode_coordination(snapshot: CoordinationSnapshot) -> EncodedCoordination {
    let goal = snapshot.goal().map_or((None, None), |value| {
        (
            Some(value.goal_id().into_uuid().as_bytes().to_vec()),
            Some(value.expected_revision().get()),
        )
    });
    let head = snapshot.node().map_or((None, None), |value| {
        (
            Some(value.node_id().into_uuid().as_bytes().to_vec()),
            Some(value.expected_revision().get()),
        )
    });
    EncodedCoordination {
        goal_id: goal.0,
        goal_revision: goal.1,
        dag_revision: snapshot.dag_revision().map(DagRevisionNumber::get),
        dag_head_revision: snapshot.dag_head_revision().map(DagHeadRevision::get),
        node_id: head.0,
        node_revision: head.1,
    }
}

fn encode_coordinator(snapshot: CoordinatorSnapshot) -> EncodedCoordinator {
    (
        snapshot
            .agent_id()
            .map(|value| value.into_uuid().as_bytes().to_vec()),
        snapshot
            .session_id()
            .map(|value| value.into_uuid().as_bytes().to_vec()),
        snapshot.revision().map(ActivityCoordinatorRevision::get),
    )
}

fn encode_principal(value: AuthenticatedPrincipal) -> (i64, Option<Vec<u8>>) {
    match value {
        AuthenticatedPrincipal::TrustedStdio => (0, None),
        AuthenticatedPrincipal::DirectToken(id) => (1, Some(id.into_uuid().as_bytes().to_vec())),
        AuthenticatedPrincipal::OAuthGrant(id) => (2, Some(id.into_uuid().as_bytes().to_vec())),
    }
}

fn encode_event_kind(value: AgentEventKind) -> i64 {
    match value {
        AgentEventKind::Joined => 0,
        AgentEventKind::Left => 1,
        AgentEventKind::HandoffStarted => 2,
        AgentEventKind::HandoffCompleted => 3,
        AgentEventKind::FocusChanged => 4,
        AgentEventKind::RoleChanged => 5,
    }
}

fn decode_event_checked(row: &Row<'_>) -> Result<AgentEvent, OperationsError> {
    let event_id = AgentEventId::from_uuid(decode_row_uuid(row, 0, "ops_events.event_id")?);
    let workspace_id = WorkspaceId::from_uuid(decode_row_uuid(row, 1, "ops_events.workspace_id")?);
    let activity_id = ActivityId::from_uuid(decode_row_uuid(row, 2, "ops_events.activity_id")?);
    let membership_revision = AgentMembershipRevision::new(row.get(3)?)
        .map_err(|_| OperationsError::Corrupt("ops_events.membership_revision"))?;
    let kind = decode_event_kind(row.get(4)?)?;
    let from = decode_participant(row, 5)?;
    let to = decode_participant(row, 10)?;
    let before = decode_coordination(row, 15)?;
    let after = decode_coordination(row, 21)?;
    let before_coordinator = decode_coordinator(row, 27)?;
    let after_coordinator = decode_coordinator(row, 30)?;
    let observed_at = TimestampMillis::new(row.get(33)?)
        .map_err(|_| OperationsError::Corrupt("ops_events.observed_at_ms"))?;
    Ok(AgentEvent::new(
        event_id,
        workspace_id,
        activity_id,
        membership_revision,
        kind,
        from,
        to,
        before,
        after,
        before_coordinator,
        after_coordinator,
        observed_at,
    ))
}

fn decode_participant(
    row: &Row<'_>,
    offset: usize,
) -> Result<Option<AgentParticipantRef>, OperationsError> {
    let agent: Option<Vec<u8>> = row.get(offset)?;
    let session: Option<Vec<u8>> = row.get(offset + 1)?;
    let principal_kind: Option<i64> = row.get(offset + 2)?;
    let principal_id: Option<Vec<u8>> = row.get(offset + 3)?;
    let display_name: Option<Vec<u8>> = row.get(offset + 4)?;
    let Some(agent) = agent else {
        if session.is_some()
            || principal_kind.is_some()
            || principal_id.is_some()
            || display_name.is_some()
        {
            return Err(OperationsError::Corrupt("ops_events.participant"));
        }
        return Ok(None);
    };
    let display_name = display_name.ok_or(OperationsError::Corrupt("ops_events.display_name"))?;
    if display_name.len() > MAX_LABEL_BYTES {
        return Err(OperationsError::Bounds("ops_events.display_name"));
    }
    let display_name = String::from_utf8(display_name)
        .map_err(|_| OperationsError::Corrupt("ops_events.display_name"))?;
    let principal = decode_principal(
        principal_kind.ok_or(OperationsError::Corrupt("ops_events.principal_kind"))?,
        principal_id,
    )?;
    let display_name = forge_domain::AgentDisplayName::new(display_name)
        .map_err(|_| OperationsError::Corrupt("ops_events.display_name"))?;
    Ok(Some(AgentParticipantRef::new(
        AgentInstanceId::from_uuid(decode_uuid(&agent, "ops_events.agent_id")?),
        session
            .map(|value| decode_uuid(&value, "ops_events.session_id"))
            .transpose()?
            .map(AgentSessionId::from_uuid),
        principal,
        display_name,
    )))
}

fn decode_coordination(
    row: &Row<'_>,
    offset: usize,
) -> Result<CoordinationSnapshot, OperationsError> {
    let goal_id: Option<Vec<u8>> = row.get(offset)?;
    let goal_revision: Option<i64> = row.get(offset + 1)?;
    let dag_revision: Option<i64> = row.get(offset + 2)?;
    let dag_head_revision: Option<i64> = row.get(offset + 3)?;
    let head_id: Option<Vec<u8>> = row.get(offset + 4)?;
    let head_revision: Option<i64> = row.get(offset + 5)?;
    let goal = goal_id
        .map(|value| {
            let revision = forge_domain::GoalRevision::new(
                goal_revision.ok_or(OperationsError::Corrupt("ops_events.goal_revision"))?,
            )
            .map_err(|_| OperationsError::Corrupt("ops_events.goal_revision"))?;
            Ok::<_, OperationsError>(forge_domain::GoalAttribution::new(
                forge_domain::GoalId::from_uuid(decode_uuid(&value, "ops_events.goal_id")?),
                revision,
            ))
        })
        .transpose()?;
    let dag_revision = dag_revision
        .map(|value| {
            DagRevisionNumber::new(value)
                .map_err(|_| OperationsError::Corrupt("ops_events.dag_revision"))
        })
        .transpose()?;
    let dag_head_revision = dag_head_revision
        .map(|value| {
            DagHeadRevision::new(value)
                .map_err(|_| OperationsError::Corrupt("ops_events.dag_head_revision"))
        })
        .transpose()?;
    let head = head_id
        .map(|value| {
            let revision = DagNodeRevision::new(
                head_revision.ok_or(OperationsError::Corrupt("ops_events.head_revision"))?,
            )
            .map_err(|_| OperationsError::Corrupt("ops_events.head_revision"))?;
            Ok::<_, OperationsError>(forge_domain::AgentFocusNode::new(
                DagNodeId::from_uuid(decode_uuid(&value, "ops_events.head_id")?),
                revision,
            ))
        })
        .transpose()?;
    CoordinationSnapshot::new(goal, dag_revision, dag_head_revision, head)
        .map_err(|_| OperationsError::Corrupt("ops_events.coordination"))
}

fn decode_coordinator(
    row: &Row<'_>,
    offset: usize,
) -> Result<CoordinatorSnapshot, OperationsError> {
    let agent: Option<Vec<u8>> = row.get(offset)?;
    let session: Option<Vec<u8>> = row.get(offset + 1)?;
    let revision: Option<i64> = row.get(offset + 2)?;
    match (agent, session, revision) {
        (None, None, None) => Ok(CoordinatorSnapshot::unbound()),
        (Some(agent), session, Some(revision)) => Ok(CoordinatorSnapshot::bound(
            AgentInstanceId::from_uuid(decode_uuid(&agent, "ops_events.coordinator_agent_id")?),
            session
                .map(|value| decode_uuid(&value, "ops_events.coordinator_session_id"))
                .transpose()?
                .map(AgentSessionId::from_uuid),
            ActivityCoordinatorRevision::new(revision)
                .map_err(|_| OperationsError::Corrupt("ops_events.coordinator_revision"))?,
        )),
        _ => Err(OperationsError::Corrupt("ops_events.coordinator")),
    }
}

fn decode_presence_checked(row: &Row<'_>) -> Result<PresenceRecord, OperationsError> {
    Ok(PresenceRecord::new(
        AgentInstanceId::from_uuid(decode_row_uuid(row, 0, "ops_presence.agent_id")?),
        WorkspaceId::from_uuid(decode_row_uuid(row, 1, "ops_presence.workspace_id")?),
        ActivityId::from_uuid(decode_row_uuid(row, 2, "ops_presence.activity_id")?),
        AgentSessionId::from_uuid(decode_row_uuid(row, 3, "ops_presence.session_id")?),
        TimestampMillis::new(row.get(4)?)
            .map_err(|_| OperationsError::Corrupt("ops_presence.last_authenticated_at_ms"))?,
        AgentPresenceRevision::new(row.get(5)?)
            .map_err(|_| OperationsError::Corrupt("ops_presence.presence_revision"))?,
    ))
}

fn decode_receipt_checked(row: &Row<'_>) -> Result<InboxReceiptRecord, OperationsError> {
    Ok(InboxReceiptRecord::new(
        AgentInstanceId::from_uuid(decode_row_uuid(row, 0, "ops_inbox.recipient_agent_id")?),
        WorkspaceId::from_uuid(decode_row_uuid(row, 1, "ops_inbox.workspace_id")?),
        AgentSessionId::from_uuid(decode_row_uuid(row, 2, "ops_inbox.recipient_session_id")?),
        AgentEventId::from_uuid(decode_row_uuid(row, 3, "ops_inbox.event_id")?),
        InboxReceiptRevision::new(row.get(4)?)
            .map_err(|_| OperationsError::Corrupt("ops_inbox.receipt_revision"))?,
        row.get::<_, Option<i64>>(5)?
            .map(|value| {
                TimestampMillis::new(value)
                    .map_err(|_| OperationsError::Corrupt("ops_inbox.acknowledged_at_ms"))
            })
            .transpose()?,
    ))
}

fn decode_principal(
    kind: i64,
    identifier: Option<Vec<u8>>,
) -> Result<AuthenticatedPrincipal, OperationsError> {
    match (kind, identifier) {
        (0, None) => Ok(AuthenticatedPrincipal::TrustedStdio),
        (1, Some(value)) => Ok(AuthenticatedPrincipal::DirectToken(
            forge_domain::DirectTokenId::from_uuid(decode_uuid(&value, "ops_events.principal_id")?),
        )),
        (2, Some(value)) => Ok(AuthenticatedPrincipal::OAuthGrant(
            forge_domain::OAuthGrantId::from_uuid(decode_uuid(&value, "ops_events.principal_id")?),
        )),
        _ => Err(OperationsError::Corrupt("ops_events.principal")),
    }
}

fn decode_event_kind(value: i64) -> Result<AgentEventKind, OperationsError> {
    match value {
        0 => Ok(AgentEventKind::Joined),
        1 => Ok(AgentEventKind::Left),
        2 => Ok(AgentEventKind::HandoffStarted),
        3 => Ok(AgentEventKind::HandoffCompleted),
        4 => Ok(AgentEventKind::FocusChanged),
        5 => Ok(AgentEventKind::RoleChanged),
        _ => Err(OperationsError::Corrupt("ops_events.event_kind")),
    }
}

fn decode_uuid(bytes: &[u8], field: &'static str) -> Result<Uuid, OperationsError> {
    if bytes.len() != ID_BYTES {
        return Err(OperationsError::Corrupt(field));
    }
    Uuid::from_slice(bytes).map_err(|_| OperationsError::Corrupt(field))
}

fn decode_row_uuid(
    row: &Row<'_>,
    index: usize,
    field: &'static str,
) -> Result<Uuid, OperationsError> {
    let bytes: Vec<u8> = row.get(index)?;
    decode_uuid(&bytes, field)
}
