//! Activity-scoped reads over Orchestration-owned logical state.
//!
//! Current participants, presence, JOINED events, and unread receipt facts are
//! read only from current logical tables. Forge-owned planning and legacy
//! history remain separate inputs at the MCP composition boundary.

use crate::{OperationsCatalog, OperationsError, events, registration, wire};
use forge_domain::{
    ActivityId, AgentDeclaredRole, AgentDisplayName, AgentEvent, AgentInstanceId,
    AgentPresenceRevision, AgentSessionId, AgentTransportKind, TimestampMillis, WorkspaceId,
};
use rusqlite::{Connection, params};
use std::collections::HashSet;
use uuid::Uuid;

const MAX_ACTIVITY_RESULTS: usize = 64;
const QUERY_LIMIT: i64 = 65;

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalActivityParticipant {
    participant_id: Uuid,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
    membership_revision: u64,
    presence_revision: AgentPresenceRevision,
    last_authenticated_at: Option<TimestampMillis>,
}

impl LogicalActivityParticipant {
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    #[must_use]
    pub const fn display_name(&self) -> &AgentDisplayName {
        &self.display_name
    }

    #[must_use]
    pub const fn declared_role(&self) -> &AgentDeclaredRole {
        &self.declared_role
    }

    #[must_use]
    pub const fn transport(&self) -> AgentTransportKind {
        self.transport
    }

    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }

    pub const fn presence_revision(&self) -> AgentPresenceRevision {
        self.presence_revision
    }

    #[must_use]
    pub const fn last_authenticated_at(&self) -> Option<TimestampMillis> {
        self.last_authenticated_at
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalActivityJoinedEvent {
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    event_id: Uuid,
    participant_id: Uuid,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    display_name: AgentDisplayName,
    membership_revision: u64,
    observed_at: TimestampMillis,
}

impl LogicalActivityJoinedEvent {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    #[must_use]
    pub const fn display_name(&self) -> &AgentDisplayName {
        &self.display_name
    }

    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }

    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalActivitySnapshot {
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    participants: Vec<LogicalActivityParticipant>,
    events: Vec<LogicalActivityJoinedEvent>,
    unread_count: u32,
    imported_history_events: Vec<AgentEvent>,
    imported_history_truncated: bool,
}

impl LogicalActivitySnapshot {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    pub fn participants(&self) -> &[LogicalActivityParticipant] {
        &self.participants
    }

    pub fn events(&self) -> &[LogicalActivityJoinedEvent] {
        &self.events
    }

    #[must_use]
    pub const fn unread_count(&self) -> u32 {
        self.unread_count
    }

    #[must_use]
    pub const fn unread_join_count(&self) -> u32 {
        self.unread_count
    }

    pub fn imported_history_events(&self) -> &[AgentEvent] {
        &self.imported_history_events
    }

    #[must_use]
    pub const fn imported_history_truncated(&self) -> bool {
        self.imported_history_truncated
    }
}

impl OperationsCatalog {
    /// Read one bounded Activity-scoped snapshot from current logical tables.
    /// Imported Forge rows are intentionally excluded from current truth.
    ///
    /// # Errors
    /// Rejects corrupt Ready witnesses, malformed rows, oversized current
    /// participant/event sets, or storage failures.
    pub fn logical_activity_snapshot(
        &self,
        workspace_id: WorkspaceId,
        activity_id: ActivityId,
    ) -> Result<LogicalActivitySnapshot, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let participants = load_participants(&transaction, workspace_id, activity_id)?;
        let ready_participants = participants
            .iter()
            .map(LogicalActivityParticipant::participant_id)
            .collect::<HashSet<_>>();
        let current_events =
            load_current_events(&transaction, workspace_id, activity_id, &ready_participants)?;
        let ready_events = current_events
            .iter()
            .map(LogicalActivityJoinedEvent::event_id)
            .collect::<HashSet<_>>();
        let unread_count =
            load_unread_count(&transaction, workspace_id, activity_id, &ready_events)?;
        let mut imported_history_events =
            events::list_activity_events(&transaction, workspace_id, activity_id, QUERY_LIMIT)?;
        let imported_history_truncated = imported_history_events.len() > MAX_ACTIVITY_RESULTS;
        imported_history_events.truncate(MAX_ACTIVITY_RESULTS);
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(LogicalActivitySnapshot {
            workspace_id,
            activity_id,
            participants,
            events: current_events,
            unread_count,
            imported_history_events,
            imported_history_truncated,
        })
    }
}

fn load_participants(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
) -> Result<Vec<LogicalActivityParticipant>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(b.participant_id,1,17),substr(b.agent_id,1,17),substr(b.session_id,1,17),substr(CAST(m.display_name AS BLOB),1,129),substr(CAST(m.declared_role AS BLOB),1,129),m.transport_kind,m.membership_revision,p.presence_revision,p.last_authenticated_at_ms FROM ops_current_effect_bindings b JOIN ops_logical_members m ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id AND m.session_id=b.session_id LEFT JOIN ops_logical_presence p ON p.workspace_id=b.workspace_id AND p.participant_id=b.participant_id AND p.session_id=b.session_id WHERE b.workspace_id=?1 AND b.activity_id=?2 ORDER BY b.participant_id LIMIT ?3",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        activity_id.into_uuid().as_bytes(),
        QUERY_LIMIT,
    ])?;
    let mut participants = Vec::new();
    while let Some(row) = rows.next()? {
        let participant_id = Uuid::from_bytes(wire::id(row.get(0)?)?);
        let presence_revision = row
            .get::<_, Option<i64>>(7)?
            .map(AgentPresenceRevision::new)
            .transpose()
            .map_err(|_| OperationsError::Corrupt("logical activity presence revision"))?
            .unwrap_or(AgentPresenceRevision::INITIAL);
        let last_authenticated_at = row
            .get::<_, Option<i64>>(8)?
            .map(TimestampMillis::new)
            .transpose()
            .map_err(|_| OperationsError::Corrupt("logical activity presence timestamp"))?;
        participants.push(LogicalActivityParticipant {
            participant_id,
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(row.get(1)?)?)),
            session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(row.get(2)?)?)),
            display_name: AgentDisplayName::new(
                String::from_utf8(row.get(3)?)
                    .map_err(|_| OperationsError::Corrupt("logical activity display"))?,
            )
            .map_err(|_| OperationsError::Corrupt("logical activity display"))?,
            declared_role: AgentDeclaredRole::new(
                String::from_utf8(row.get(4)?)
                    .map_err(|_| OperationsError::Corrupt("logical activity role"))?,
            )
            .map_err(|_| OperationsError::Corrupt("logical activity role"))?,
            transport: registration::decode_transport(row.get(5)?)?,
            membership_revision: u64::try_from(row.get::<_, i64>(6)?)
                .map_err(|_| OperationsError::Corrupt("logical activity membership revision"))?,
            presence_revision,
            last_authenticated_at,
        });
        if participants.len() > MAX_ACTIVITY_RESULTS {
            return Err(OperationsError::Bounds("logical activity participants"));
        }
    }
    Ok(participants)
}

fn load_current_events(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    ready_participants: &HashSet<Uuid>,
) -> Result<Vec<LogicalActivityJoinedEvent>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(e.event_id,1,17),substr(e.participant_id,1,17),substr(b.agent_id,1,17),substr(b.session_id,1,17),substr(CAST(m.display_name AS BLOB),1,129),e.membership_revision,e.observed_at_ms FROM ops_logical_events e JOIN ops_current_effect_bindings b ON b.workspace_id=e.workspace_id AND b.participant_id=e.participant_id AND b.activity_id=e.activity_id JOIN ops_logical_members m ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id AND m.session_id=b.session_id WHERE e.workspace_id=?1 AND e.activity_id=?2 ORDER BY e.observed_at_ms DESC,e.event_id DESC LIMIT ?3",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        activity_id.into_uuid().as_bytes(),
        QUERY_LIMIT,
    ])?;
    let mut current_events = Vec::new();
    while let Some(row) = rows.next()? {
        let participant_id = Uuid::from_bytes(wire::id(row.get(1)?)?);
        if !ready_participants.contains(&participant_id) {
            continue;
        }
        current_events.push(LogicalActivityJoinedEvent {
            workspace_id,
            activity_id,
            event_id: Uuid::from_bytes(wire::id(row.get(0)?)?),
            participant_id,
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(row.get(2)?)?)),
            session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(row.get(3)?)?)),
            display_name: AgentDisplayName::new(
                String::from_utf8(row.get(4)?)
                    .map_err(|_| OperationsError::Corrupt("logical activity event display"))?,
            )
            .map_err(|_| OperationsError::Corrupt("logical activity event display"))?,
            membership_revision: u64::try_from(row.get::<_, i64>(5)?)
                .map_err(|_| OperationsError::Corrupt("logical activity event revision"))?,
            observed_at: TimestampMillis::new(row.get(6)?)
                .map_err(|_| OperationsError::Corrupt("logical activity event timestamp"))?,
        });
        if current_events.len() > MAX_ACTIVITY_RESULTS {
            return Err(OperationsError::Bounds("logical activity events"));
        }
    }
    Ok(current_events)
}

fn load_unread_count(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    ready_events: &HashSet<Uuid>,
) -> Result<u32, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(s.event_id,1,17),COUNT(*) FROM ops_logical_receipt_state s JOIN ops_logical_events e ON e.workspace_id=s.workspace_id AND e.event_id=s.event_id WHERE e.workspace_id=?1 AND e.activity_id=?2 AND s.acknowledged_at_ms IS NULL GROUP BY s.event_id ORDER BY s.event_id LIMIT ?3",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        activity_id.into_uuid().as_bytes(),
        QUERY_LIMIT,
    ])?;
    let mut groups = 0_usize;
    let mut unread = 0_u32;
    while let Some(row) = rows.next()? {
        groups = groups.saturating_add(1);
        if groups > MAX_ACTIVITY_RESULTS {
            return Err(OperationsError::Bounds(
                "logical activity unread event groups",
            ));
        }
        let event_id = Uuid::from_bytes(wire::id(row.get(0)?)?);
        if ready_events.contains(&event_id) {
            let count = u32::try_from(row.get::<_, i64>(1)?)
                .map_err(|_| OperationsError::Bounds("logical activity unread count"))?;
            unread = unread
                .checked_add(count)
                .ok_or(OperationsError::Bounds("logical activity unread count"))?;
        }
    }
    Ok(unread)
}
