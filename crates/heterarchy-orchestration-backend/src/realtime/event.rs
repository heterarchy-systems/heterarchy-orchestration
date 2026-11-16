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
    /// Workspace scope configuration or trust boundary changed.
    WorkspaceScopeChanged,
    /// Reviewer assignment state changed.
    ReviewerAssignmentChanged,
    /// A reviewer verdict was recorded.
    ReviewerVerdictRecorded,
    /// An execution snapshot was committed.
    ExecutionSnapshotCommitted,
    /// A Swarm run lifecycle state changed.
    SwarmRunChanged,
    /// A Swarm communication channel changed.
    SwarmChannelChanged,
    /// A Swarm member state changed.
    SwarmMemberStateChanged,
    /// A Swarm board message was posted.
    SwarmBoardMessagePosted,
    /// A Swarm verdict was recorded.
    SwarmVerdictRecorded,
    /// A Swarm stall check was evaluated.
    SwarmStallEvaluated,
    /// A Forge settlement was projected into Orchestration.
    SettlementProjected,
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

#[cfg(test)]
mod tests {
    use super::RealtimeEventKind;

    #[test]
    fn serializes_all_event_kinds_in_snake_case() -> Result<(), serde_json::Error> {
        let expected = [
            (
                RealtimeEventKind::AgentStateChanged,
                "\"agent_state_changed\"",
            ),
            (
                RealtimeEventKind::AgentProfileStateChanged,
                "\"agent_profile_state_changed\"",
            ),
            (
                RealtimeEventKind::LogicalSessionChanged,
                "\"logical_session_changed\"",
            ),
            (RealtimeEventKind::TeamChanged, "\"team_changed\""),
            (
                RealtimeEventKind::MembershipChanged,
                "\"membership_changed\"",
            ),
            (
                RealtimeEventKind::LeadershipChanged,
                "\"leadership_changed\"",
            ),
            (
                RealtimeEventKind::ProvisioningStateChanged,
                "\"provisioning_state_changed\"",
            ),
            (
                RealtimeEventKind::ApprovalRequestChanged,
                "\"approval_request_changed\"",
            ),
            (
                RealtimeEventKind::ApprovalDecisionChanged,
                "\"approval_decision_changed\"",
            ),
            (
                RealtimeEventKind::ApprovalPolicyChanged,
                "\"approval_policy_changed\"",
            ),
            (
                RealtimeEventKind::AgentRequestStateChanged,
                "\"agent_request_state_changed\"",
            ),
            (
                RealtimeEventKind::SchedulingChanged,
                "\"scheduling_changed\"",
            ),
            (
                RealtimeEventKind::WorkspaceScopeChanged,
                "\"workspace_scope_changed\"",
            ),
            (
                RealtimeEventKind::ReviewerAssignmentChanged,
                "\"reviewer_assignment_changed\"",
            ),
            (
                RealtimeEventKind::ReviewerVerdictRecorded,
                "\"reviewer_verdict_recorded\"",
            ),
            (
                RealtimeEventKind::ExecutionSnapshotCommitted,
                "\"execution_snapshot_committed\"",
            ),
            (RealtimeEventKind::SwarmRunChanged, "\"swarm_run_changed\""),
            (
                RealtimeEventKind::SwarmChannelChanged,
                "\"swarm_channel_changed\"",
            ),
            (
                RealtimeEventKind::SwarmMemberStateChanged,
                "\"swarm_member_state_changed\"",
            ),
            (
                RealtimeEventKind::SwarmBoardMessagePosted,
                "\"swarm_board_message_posted\"",
            ),
            (
                RealtimeEventKind::SwarmVerdictRecorded,
                "\"swarm_verdict_recorded\"",
            ),
            (
                RealtimeEventKind::SwarmStallEvaluated,
                "\"swarm_stall_evaluated\"",
            ),
            (
                RealtimeEventKind::SettlementProjected,
                "\"settlement_projected\"",
            ),
        ];

        for (kind, wire) in expected {
            assert_eq!(serde_json::to_string(&kind)?, wire);
        }
        Ok(())
    }
}
