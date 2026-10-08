//! Compact typed realtime events.
//!
//! An event is the **notification of an already-committed domain change**, not
//! the change itself. Envelopes are fixed-field and free of free-text payload
//! so the fan-out can clone them cheaply; clients fetch the authoritative
//! revision through the canonical HTTP queries.

use serde::Serialize;
use uuid::Uuid;

/// Closed set of Orchestration state changes the realtime layer can observe.
///
/// Forge execution output (stdout/stderr, build/test logs) is never an
/// Orchestration event; when Orchestration projects a confirmed Forge result,
/// it publishes the corresponding domain kind instead of raw execution truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RealtimeEventKind {
    /// Agent lifecycle state changed.
    AgentStateChanged,
    /// Agent Profile lifecycle state changed.
    AgentProfileStateChanged,
    /// Logical Session catalog changed.
    LogicalSessionChanged,
    /// Team lifecycle changed.
    TeamChanged,
    /// Team membership changed.
    MembershipChanged,
    /// Election or leadership state changed.
    LeadershipChanged,
    /// Provisioning pipeline state changed.
    ProvisioningStateChanged,
    /// An approval request was created or changed.
    ApprovalRequestChanged,
    /// An approval decision was recorded.
    ApprovalDecisionChanged,
    /// The approval policy for a workspace changed.
    ApprovalPolicyChanged,
    /// An Agent Request changed state.
    AgentRequestStateChanged,
    /// Scheduling / coordination projection changed.
    SchedulingChanged,
}

/// Notification of one committed domain change.
///
/// The committing owner stamps `occurred_at_ms`; the hub never invents clocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeEvent {
    /// Which closed kind of state change occurred.
    pub kind: RealtimeEventKind,
    /// Identity of the affected resource (agent, team, session, request, ...).
    pub resource_id: Uuid,
    /// Canonical revision of the resource **after** the observed commit.
    pub resource_revision: u64,
    /// Workspace scope when the change belongs to one workspace.
    pub workspace_id: Option<Uuid>,
    /// Wall-clock milliseconds supplied by the committing boundary.
    pub occurred_at_ms: i64,
}
