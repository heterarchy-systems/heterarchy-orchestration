//! Session-owned participation application operations over the existing catalog.

use crate::{OrchestrationRuntime, RuntimeError};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentPresenceRevision, AgentSessionId, AuthenticatedPrincipal,
    InboxReceiptRevision, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    LogicalInboxAck, LogicalInboxEntry, LogicalInboxQuery, LogicalInboxTarget,
    LogicalPresenceSnapshot, LogicalPresenceTarget, LogicalPresenceTouch,
};
use uuid::Uuid;

/// Exact selectors for a current activated logical binding; none grants identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParticipationTarget {
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
}
impl ParticipationTarget {
    fn validate(self) -> Result<(), RuntimeError> {
        if [
            self.workspace_id,
            self.activity_id,
            self.agent_id,
            self.session_id,
        ]
        .iter()
        .any(Uuid::is_nil)
        {
            return Err(RuntimeError::InvalidInput);
        }
        Ok(())
    }
    fn presence(self) -> LogicalPresenceTarget {
        LogicalPresenceTarget::new(
            WorkspaceId::from_uuid(self.workspace_id),
            ActivityId::from_uuid(self.activity_id),
            AgentInstanceId::from_uuid(self.agent_id),
            AgentSessionId::from_uuid(self.session_id),
        )
    }
    fn inbox(self) -> LogicalInboxTarget {
        LogicalInboxTarget::new(
            WorkspaceId::from_uuid(self.workspace_id),
            ActivityId::from_uuid(self.activity_id),
            AgentInstanceId::from_uuid(self.agent_id),
            AgentSessionId::from_uuid(self.session_id),
        )
    }
}
impl OrchestrationRuntime {
    /// Recheck an exact open Session and Workspace access. Catalog binding checks
    /// remain in each same-transaction operation, including for trusted operators.
    /// # Errors
    /// Rejects invalid selectors, foreign/closed Sessions and inaccessible scopes.
    pub fn require_participation(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<(), RuntimeError> {
        target.validate()?;
        self.require_open_session(target.session_id, principal)?;
        self.require_workspace_read(target.workspace_id, Some(target.session_id), principal)
    }
    /// Touch presence at the exact existing owner CAS; this is not nonce replay.
    /// # Errors
    /// Returns authorization, revision, storage or unconfirmed-commit failures.
    pub fn touch_presence(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        expected_revision: i64,
        now_ms: i64,
    ) -> Result<LogicalPresenceSnapshot, RuntimeError> {
        self.require_participation(target, principal)?;
        let revision = AgentPresenceRevision::new(expected_revision)
            .map_err(|_| RuntimeError::InvalidInput)?;
        let now = TimestampMillis::new(now_ms).map_err(|_| RuntimeError::InvalidInput)?;
        self.catalog()?
            .touch_logical_presence(LogicalPresenceTouch::new(
                target.presence(),
                principal,
                revision,
                now,
            ))
            .map_err(RuntimeError::Persistence)
    }
    /// Read current nullable presence without synthesizing or touching a row.
    /// # Errors
    /// Returns authorization, binding or storage failures.
    pub fn presence(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<LogicalPresenceSnapshot>, RuntimeError> {
        self.require_participation(target, principal)?;
        self.catalog()?
            .logical_presence(target.presence(), principal)
            .map_err(RuntimeError::Persistence)
    }
    /// List bounded existing receipts without acknowledging them.
    /// # Errors
    /// Returns authorization, binding, limit or storage failures.
    pub fn inbox(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        limit: u32,
    ) -> Result<Vec<LogicalInboxEntry>, RuntimeError> {
        self.require_participation(target, principal)?;
        self.catalog()?
            .list_logical_inbox(LogicalInboxQuery::new(target.inbox(), principal, limit))
            .map_err(RuntimeError::Persistence)
    }
    /// Acknowledge one receipt at the exact CAS; a repeated ACK remains a conflict.
    /// # Errors
    /// Returns authorization, binding, revision or unconfirmed-commit failures.
    pub fn acknowledge_inbox(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        event_id: Uuid,
        expected_revision: i64,
        now_ms: i64,
    ) -> Result<LogicalInboxEntry, RuntimeError> {
        self.require_participation(target, principal)?;
        if event_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let revision =
            InboxReceiptRevision::new(expected_revision).map_err(|_| RuntimeError::InvalidInput)?;
        let now = TimestampMillis::new(now_ms).map_err(|_| RuntimeError::InvalidInput)?;
        self.catalog()?
            .acknowledge_logical_inbox(LogicalInboxAck::new(
                target.inbox(),
                principal,
                event_id,
                revision,
                now,
            ))
            .map_err(RuntimeError::Persistence)
    }
    /// Read one exact event receipt after an uncertain ACK without retrying it.
    /// # Errors
    /// Returns authorization, binding or storage failures.
    pub fn inbox_entry(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        event_id: Uuid,
    ) -> Result<Option<LogicalInboxEntry>, RuntimeError> {
        self.require_participation(target, principal)?;
        if event_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        self.catalog()?
            .logical_inbox_entry(target.inbox(), principal, event_id)
            .map_err(RuntimeError::Persistence)
    }
}
