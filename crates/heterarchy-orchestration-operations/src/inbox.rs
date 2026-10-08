//! Steady-state logical inbox and acknowledgement authority.
//!
//! `ops_logical_receipts` is the immutable delivery ledger created with a
//! logical event. This module keeps acknowledgement as separate mutable state,
//! so reading never changes a receipt and historical delivery provenance is not
//! rewritten when a consumer acknowledges it.

use crate::{
    OperationsCatalog, OperationsError,
    presence::{ReadyLogicalBinding, resolve_ready_logical_binding},
    wire,
};
use forge_domain::{
    ActivityId, AgentDisplayName, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal,
    InboxReceiptRevision, TimestampMillis, WorkspaceId,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use uuid::Uuid;

const MAX_INBOX_RESULTS: u32 = 64;
const RECEIPT_STATE_SQL: &str = "CREATE TABLE ops_logical_receipt_state (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), recipient_participant_id BLOB NOT NULL CHECK(length(recipient_participant_id)=16), event_id BLOB NOT NULL CHECK(length(event_id)=16), receipt_revision INTEGER NOT NULL CHECK(receipt_revision>=1), acknowledged_at_ms INTEGER CHECK(acknowledged_at_ms IS NULL OR acknowledged_at_ms>=0), PRIMARY KEY(workspace_id,recipient_participant_id,event_id), FOREIGN KEY(workspace_id,recipient_participant_id,event_id) REFERENCES ops_logical_receipts(workspace_id,recipient_participant_id,event_id))";
const RECEIPT_STATE_TRIGGER_SQL: &str = "CREATE TRIGGER ops_logical_receipt_state_insert AFTER INSERT ON ops_logical_receipts BEGIN INSERT INTO ops_logical_receipt_state(workspace_id,recipient_participant_id,event_id,receipt_revision,acknowledged_at_ms) VALUES(NEW.workspace_id,NEW.recipient_participant_id,NEW.event_id,1,NULL); END";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_logical_receipt_state", RECEIPT_STATE_SQL),
        (
            "trigger",
            "ops_logical_receipt_state_insert",
            RECEIPT_STATE_TRIGGER_SQL,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    connection.execute(
        "INSERT OR IGNORE INTO ops_logical_receipt_state(workspace_id,recipient_participant_id,event_id,receipt_revision,acknowledged_at_ms) SELECT workspace_id,recipient_participant_id,event_id,1,NULL FROM ops_logical_receipts",
        [],
    )?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalInboxTarget {
    workspace: WorkspaceId,
    activity: ActivityId,
    agent: AgentInstanceId,
    session: AgentSessionId,
}

impl LogicalInboxTarget {
    #[must_use]
    pub const fn new(
        workspace: WorkspaceId,
        activity: ActivityId,
        agent: AgentInstanceId,
        session: AgentSessionId,
    ) -> Self {
        Self {
            workspace,
            activity,
            agent,
            session,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalInboxQuery {
    target: LogicalInboxTarget,
    principal: AuthenticatedPrincipal,
    limit: u32,
}

impl LogicalInboxQuery {
    #[must_use]
    pub const fn new(
        target: LogicalInboxTarget,
        principal: AuthenticatedPrincipal,
        limit: u32,
    ) -> Self {
        Self {
            target,
            principal,
            limit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalInboxAck {
    target: LogicalInboxTarget,
    principal: AuthenticatedPrincipal,
    event: Uuid,
    expected_revision: InboxReceiptRevision,
    observed_at: TimestampMillis,
}

impl LogicalInboxAck {
    #[must_use]
    pub const fn new(
        target: LogicalInboxTarget,
        principal: AuthenticatedPrincipal,
        event: Uuid,
        expected_revision: InboxReceiptRevision,
        observed_at: TimestampMillis,
    ) -> Self {
        Self {
            target,
            principal,
            event,
            expected_revision,
            observed_at,
        }
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalInboxEntry {
    workspace: WorkspaceId,
    recipient_participant: Uuid,
    recipient_agent: AgentInstanceId,
    recipient_session: AgentSessionId,
    recipient_display_name: AgentDisplayName,
    event: Uuid,
    actor_participant: Uuid,
    actor_agent: AgentInstanceId,
    actor_session: AgentSessionId,
    actor_display_name: AgentDisplayName,
    activity: ActivityId,
    membership_revision: u64,
    observed_at: TimestampMillis,
    receipt_revision: InboxReceiptRevision,
    acknowledged_at: Option<TimestampMillis>,
}

impl LogicalInboxEntry {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace
    }
    #[must_use]
    pub const fn recipient_participant_id(&self) -> Uuid {
        self.recipient_participant
    }
    #[must_use]
    pub const fn recipient_agent_id(&self) -> AgentInstanceId {
        self.recipient_agent
    }
    pub const fn recipient_session_id(&self) -> AgentSessionId {
        self.recipient_session
    }
    #[must_use]
    pub const fn recipient_display_name(&self) -> &AgentDisplayName {
        &self.recipient_display_name
    }
    #[must_use]
    pub const fn event_id(&self) -> Uuid {
        self.event
    }
    #[must_use]
    pub const fn actor_participant_id(&self) -> Uuid {
        self.actor_participant
    }
    #[must_use]
    pub const fn actor_agent_id(&self) -> AgentInstanceId {
        self.actor_agent
    }
    pub const fn actor_session_id(&self) -> AgentSessionId {
        self.actor_session
    }
    #[must_use]
    pub const fn actor_display_name(&self) -> &AgentDisplayName {
        &self.actor_display_name
    }
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity
    }
    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }
    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
    pub const fn receipt_revision(&self) -> InboxReceiptRevision {
        self.receipt_revision
    }
    #[must_use]
    pub const fn acknowledged_at(&self) -> Option<TimestampMillis> {
        self.acknowledged_at
    }
}

impl OperationsCatalog {
    /// Read bounded logical inbox receipts without acknowledging them.
    ///
    /// # Errors
    /// Rejects non-Ready recipient bindings, identity/principal mismatches,
    /// invalid limits, corrupt receipt state, and storage failures.
    pub fn list_logical_inbox(
        &self,
        query: LogicalInboxQuery,
    ) -> Result<Vec<LogicalInboxEntry>, OperationsError> {
        if !(1..=MAX_INBOX_RESULTS).contains(&query.limit) {
            return Err(OperationsError::Bounds("logical inbox limit"));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let recipient = resolve_recipient(&transaction, query.target, query.principal)?;
        let entries = list_entries(&transaction, query.target, &recipient, query.limit)?;
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(entries)
    }

    /// Read one exact logical inbox event for the current Ready recipient.
    ///
    /// # Errors
    /// Rejects non-Ready recipient bindings, identity/principal mismatches,
    /// corrupt receipt state, and storage failures.
    pub fn logical_inbox_entry(
        &self,
        target: LogicalInboxTarget,
        principal: AuthenticatedPrincipal,
        event: Uuid,
    ) -> Result<Option<LogicalInboxEntry>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let recipient = resolve_recipient(&transaction, target, principal)?;
        let entry = entry_by_event(&transaction, target, &recipient, event)?;
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(entry)
    }

    /// Acknowledge one exact receipt through recipient/session/event and
    /// receipt-revision CAS. Already-acknowledged receipts do not advance.
    ///
    /// # Errors
    /// Rejects non-Ready recipient bindings, missing receipts, stale receipt
    /// revisions, duplicate acknowledgement, corrupt state, and uncertain
    /// commits.
    pub fn acknowledge_logical_inbox(
        &mut self,
        command: LogicalInboxAck,
    ) -> Result<LogicalInboxEntry, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let recipient = resolve_recipient(&transaction, command.target, command.principal)?;
        let current = entry_by_event(&transaction, command.target, &recipient, command.event)?
            .ok_or(OperationsError::Conflict("logical inbox receipt missing"))?;
        if current.receipt_revision != command.expected_revision
            || current.acknowledged_at.is_some()
        {
            return Err(OperationsError::Conflict("logical inbox receipt revision"));
        }
        let next_revision = command
            .expected_revision
            .checked_next()
            .map_err(|_| OperationsError::Bounds("logical inbox receipt revision"))?;
        let changed = transaction.execute(
            "UPDATE ops_logical_receipt_state SET receipt_revision=?1,acknowledged_at_ms=?2 WHERE workspace_id=?3 AND recipient_participant_id=?4 AND event_id=?5 AND receipt_revision=?6 AND acknowledged_at_ms IS NULL",
            params![
                next_revision.get(),
                command.observed_at.get(),
                command.target.workspace.into_uuid().as_bytes(),
                recipient.participant_id.as_bytes(),
                command.event.as_bytes(),
                command.expected_revision.get(),
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("logical inbox receipt revision"));
        }
        let acknowledged = entry_by_event(&transaction, command.target, &recipient, command.event)?
            .ok_or(OperationsError::Corrupt("logical inbox receipt readback"))?;
        transaction
            .commit()
            .map_err(|source| OperationsError::InboxCommitUnconfirmed {
                workspace_id: command.target.workspace.into_uuid(),
                participant_id: recipient.participant_id,
                event_id: command.event,
                source,
            })?;
        Ok(acknowledged)
    }
}

fn resolve_recipient(
    connection: &Connection,
    target: LogicalInboxTarget,
    principal: AuthenticatedPrincipal,
) -> Result<ReadyLogicalBinding, OperationsError> {
    resolve_ready_logical_binding(
        connection,
        target.workspace,
        target.activity,
        target.agent,
        target.session,
        principal,
    )
}

fn list_entries(
    connection: &Connection,
    target: LogicalInboxTarget,
    recipient: &ReadyLogicalBinding,
    limit: u32,
) -> Result<Vec<LogicalInboxEntry>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(e.event_id,1,17),substr(e.participant_id,1,17),substr(e.activity_id,1,17),e.membership_revision,e.observed_at_ms,substr(m.session_id,1,17),substr(CAST(m.display_name AS BLOB),1,129),substr(b.agent_id,1,17),s.receipt_revision,s.acknowledged_at_ms FROM ops_logical_receipts r JOIN ops_logical_receipt_state s ON s.workspace_id=r.workspace_id AND s.recipient_participant_id=r.recipient_participant_id AND s.event_id=r.event_id JOIN ops_logical_events e ON e.workspace_id=r.workspace_id AND e.event_id=r.event_id JOIN ops_logical_members m ON m.workspace_id=e.workspace_id AND m.participant_id=e.participant_id JOIN ops_current_effect_bindings b ON b.workspace_id=e.workspace_id AND b.participant_id=e.participant_id AND b.activity_id=e.activity_id WHERE r.workspace_id=?1 AND r.recipient_participant_id=?2 ORDER BY e.observed_at_ms DESC,e.event_id DESC LIMIT ?3",
    )?;
    let mut rows = statement.query(params![
        target.workspace.into_uuid().as_bytes(),
        recipient.participant_id.as_bytes(),
        i64::from(limit),
    ])?;
    let mut entries = Vec::new();
    while let Some(row) = rows.next()? {
        entries.push(decode_entry(row, target, recipient)?);
    }
    Ok(entries)
}

fn entry_by_event(
    connection: &Connection,
    target: LogicalInboxTarget,
    recipient: &ReadyLogicalBinding,
    event: Uuid,
) -> Result<Option<LogicalInboxEntry>, OperationsError> {
    connection
        .query_row(
            "SELECT substr(e.event_id,1,17),substr(e.participant_id,1,17),substr(e.activity_id,1,17),e.membership_revision,e.observed_at_ms,substr(m.session_id,1,17),substr(CAST(m.display_name AS BLOB),1,129),substr(b.agent_id,1,17),s.receipt_revision,s.acknowledged_at_ms FROM ops_logical_receipts r JOIN ops_logical_receipt_state s ON s.workspace_id=r.workspace_id AND s.recipient_participant_id=r.recipient_participant_id AND s.event_id=r.event_id JOIN ops_logical_events e ON e.workspace_id=r.workspace_id AND e.event_id=r.event_id JOIN ops_logical_members m ON m.workspace_id=e.workspace_id AND m.participant_id=e.participant_id JOIN ops_current_effect_bindings b ON b.workspace_id=e.workspace_id AND b.participant_id=e.participant_id AND b.activity_id=e.activity_id WHERE r.workspace_id=?1 AND r.recipient_participant_id=?2 AND e.event_id=?3 LIMIT 1",
            params![
                target.workspace.into_uuid().as_bytes(),
                recipient.participant_id.as_bytes(),
                event.as_bytes(),
            ],
            |row| decode_entry(row, target, recipient).map_err(to_sql_error),
        )
        .optional()
        .map_err(Into::into)
}

fn decode_entry(
    row: &rusqlite::Row<'_>,
    target: LogicalInboxTarget,
    recipient: &ReadyLogicalBinding,
) -> Result<LogicalInboxEntry, OperationsError> {
    let actor_display_name = String::from_utf8(row.get(6)?)
        .map_err(|_| OperationsError::Corrupt("logical inbox actor display"))?;
    Ok(LogicalInboxEntry {
        workspace: target.workspace,
        recipient_participant: recipient.participant_id,
        recipient_agent: target.agent,
        recipient_session: target.session,
        recipient_display_name: recipient.display_name.clone(),
        event: Uuid::from_bytes(wire::id(row.get(0)?)?),
        actor_participant: Uuid::from_bytes(wire::id(row.get(1)?)?),
        activity: ActivityId::from_uuid(Uuid::from_bytes(wire::id(row.get(2)?)?)),
        membership_revision: u64::try_from(row.get::<_, i64>(3)?)
            .map_err(|_| OperationsError::Corrupt("logical inbox membership revision"))?,
        observed_at: TimestampMillis::new(row.get(4)?)
            .map_err(|_| OperationsError::Corrupt("logical inbox event timestamp"))?,
        actor_session: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(row.get(5)?)?)),
        actor_display_name: AgentDisplayName::new(actor_display_name)
            .map_err(|_| OperationsError::Corrupt("logical inbox actor display"))?,
        actor_agent: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(row.get(7)?)?)),
        receipt_revision: InboxReceiptRevision::new(row.get(8)?)
            .map_err(|_| OperationsError::Corrupt("logical inbox receipt revision"))?,
        acknowledged_at: row
            .get::<_, Option<i64>>(9)?
            .map(TimestampMillis::new)
            .transpose()
            .map_err(|_| OperationsError::Corrupt("logical inbox acknowledgement timestamp"))?,
    })
}

fn to_sql_error(error: OperationsError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(error))
}
