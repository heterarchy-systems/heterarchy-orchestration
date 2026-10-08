//! Typed, source-scoped Activity coordination query contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::WorkspaceQueryScope;

/// Query the complete current and imported coordination projection for one Activity.
/// The optional Session is an explicit selector; caller identity comes from HTTP auth.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoordinationQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
}

/// Exact query echo and the complete native coordination projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoordinationQueryResponse {
    pub request: CoordinationQueryRequest,
    pub snapshot: CoordinationQuerySnapshot,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub catch_up: Option<CoordinationCatchUpWire>,
}

/// Exact bounded local consumer progress; a full page does not prove source exhaustion.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoordinationCatchUpWire {
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub requested_after: u64,
    pub applied_through: u64,
    pub page_size: u32,
    pub page_limit: u32,
    pub more_may_remain: bool,
}

/// Full `agent_coordination_get` projection, retaining native snake-case fields.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationQuerySnapshot {
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub membership_revision: Option<i64>,
    pub membership_revision_scope: CoordinationMembershipRevisionScope,
    pub coordinator: CoordinationCoordinatorWire,
    pub counts: CoordinationCountsWire,
    pub unread_count: u32,
    pub unread_join_count: u32,
    pub participants: Vec<CoordinationParticipantWire>,
    pub events: Vec<CoordinationEventWire>,
    pub legacy_history: CoordinationLegacyHistoryWire,
    pub authority: CoordinationAuthorityWire,
    pub snapshot_consistency: CoordinationSnapshotConsistency,
}

/// Native count fields, including nullable legacy counters.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationCountsWire {
    pub current_coordinator: i64,
    pub attached_participants: u32,
    pub ready_logical_participants: u32,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub unresolved_legacy: Option<u32>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub closed_activity: Option<u32>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub handed_off: Option<u32>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub detached: Option<u32>,
}

/// Forge planning coordinator joined with its matching logical participant when present.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationCoordinatorWire {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub agent_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participant_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub revision: Option<i64>,
    pub authority: CoordinationPlanningAuthority,
}

/// Current ready logical participant and presence projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationParticipantWire {
    pub participant_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub display_name: String,
    pub declared_role: String,
    pub transport: CoordinationTransport,
    pub partition: CoordinationParticipantPartition,
    pub membership_revision: u64,
    pub presence_revision: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub last_authenticated_at_ms: Option<i64>,
}

/// One native coordination event. Current joined events carry participant IDs;
/// imported legacy event participants intentionally do not.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationEventWire {
    pub event_id: Uuid,
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    pub membership_revision: u64,
    pub kind: CoordinationEventKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub from: Option<CoordinationEventParticipantWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub to: Option<CoordinationEventParticipantWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub plan_before: Option<CoordinationPlanWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub plan_after: Option<CoordinationPlanWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub coordinator_before: Option<CoordinationEventCoordinatorWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub coordinator_after: Option<CoordinationEventCoordinatorWire>,
    pub observed_at_ms: i64,
}

/// Participant endpoint embedded in an event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationEventParticipantWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant_id: Option<Uuid>,
    pub agent_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
    pub display_name: String,
}

/// Goal/DAG/node attribution captured with an imported event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationPlanWire {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub goal: Option<CoordinationGoalWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dag_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dag_head_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub node: Option<CoordinationNodeWire>,
}

/// Goal identity and frozen revision from an imported event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationGoalWire {
    pub goal_id: Uuid,
    pub revision: i64,
}

/// Node identity and frozen revision from an imported event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationNodeWire {
    pub node_id: Uuid,
    pub revision: i64,
}

/// Coordinator attribution captured with an imported event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationEventCoordinatorWire {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub agent_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub revision: Option<i64>,
}

/// Bounded imported event history, explicitly labeled as a cutover snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationLegacyHistoryWire {
    pub source: CoordinationLegacyHistorySource,
    pub freshness: CoordinationLegacyHistoryFreshness,
    pub live_after_cutover_writes_included: bool,
    pub truncated: bool,
    pub events: Vec<CoordinationEventWire>,
}

/// Fixed ownership labels emitted by the native coordination query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationAuthorityWire {
    pub participants: CoordinationParticipantsAuthority,
    pub presence: CoordinationPresenceAuthority,
    pub inbox: CoordinationInboxAuthority,
    pub planning_coordinator: CoordinationPlanningAuthority,
    pub legacy_history: CoordinationLegacyHistorySource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationMembershipRevisionScope {
    PerParticipant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationPlanningAuthority {
    ForgeActivityPlanning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationParticipantPartition {
    ReadyLogicalParticipant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum CoordinationTransport {
    Mcp,
    Stdio,
    StreamableHttp,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationEventKind {
    Joined,
    Left,
    HandoffStarted,
    HandoffCompleted,
    FocusChanged,
    RoleChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationLegacyHistorySource {
    OrchestrationImportedCutoverSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationLegacyHistoryFreshness {
    CutoverSnapshotNotLive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationParticipantsAuthority {
    OrchestrationLogicalMembership,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationPresenceAuthority {
    OrchestrationLogicalPresence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationInboxAuthority {
    OrchestrationLogicalInbox,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationSnapshotConsistency {
    CrossAuthorityRead,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
