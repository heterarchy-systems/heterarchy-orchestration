//! Shared leadership query request and response contracts.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Canonical Forge/Workspace scope supplied with a leadership read.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceQueryScope {
    pub workspace_id: Uuid,
    pub location_id: Uuid,
    pub location_revision: i64,
    pub workspace_revision: i64,
    pub locator: String,
}

/// Bounded leadership query selector. The HTTP caller's credential remains the
/// authority; `session_id` is only an explicit session selector.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Option<Uuid>,
    pub team_id: Option<Uuid>,
    pub review_after: Option<Uuid>,
    pub limit: u32,
}

/// Complete leadership view and bounded review history page.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipViewResponse {
    pub leadership: Option<LeadershipWire>,
    pub review_history: ReviewHistoryWire,
}

/// One scoped durable leadership projection.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipWire {
    pub workspace_id: Uuid,
    pub activity_id: Option<Uuid>,
    pub team_id: Option<Uuid>,
    pub state: String,
    pub leadership_revision: i64,
    pub leader_participation_id: Option<Uuid>,
    pub leader_profile_id: Option<Uuid>,
    pub leader_participant_id: Option<Uuid>,
    pub leader_display_name: Option<String>,
    pub stabilization_until_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub current_election: Option<ElectionWire>,
}

/// Election projection attached to a leadership view.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ElectionWire {
    pub election_id: Uuid,
    pub state: String,
    pub ballot_deadline_ms: i64,
    pub deadline_passed: bool,
    pub electorate_size: i64,
    pub cast_yes: i64,
    pub cast_no: i64,
    pub cast_abstain: i64,
}

/// Bounded review history page.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewHistoryWire {
    pub entries: Vec<ReviewHistoryEntryWire>,
    pub next_cursor: Option<Uuid>,
}

/// One review history entry.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewHistoryEntryWire {
    pub request_id: Uuid,
    pub election_id: Uuid,
    pub created_at_ms: i64,
    pub reason_preview: String,
    pub phase: String,
}

/// Query context is present even when no leadership row exists, so an empty
/// view cannot silently belong to a different requested scope.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipQueryResponse {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Option<Uuid>,
    pub team_id: Option<Uuid>,
    pub view: LeadershipViewResponse,
}
