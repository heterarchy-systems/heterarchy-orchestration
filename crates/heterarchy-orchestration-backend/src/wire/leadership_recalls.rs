//! Source-scoped leadership recall start contract.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;
use super::leadership_reviews::LeadershipReviewPhaseWire;

/// Closed leadership scope selected for a recall request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeadershipRecallTargetWire {
    Workspace,
    Activity,
    Team,
}

/// Start or exactly replay one caller-owned recall review.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipRecallStartRequest {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub target: LeadershipRecallTargetWire,
    pub team_id: Option<Uuid>,
    pub expected_leadership_revision: i64,
    pub request_id: Uuid,
    pub reason: String,
}

/// Exact source and actor context returned with the recall receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipRecallStartResponse {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub target: LeadershipRecallTargetWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    pub receipt: LeadershipRecallReceiptWire,
}

/// Start/replay metadata from the current Operations review receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipRecallReceiptWire {
    pub request_id: Uuid,
    pub election_id: Uuid,
    /// Current leadership revision observed with this receipt.
    pub leadership_revision: i64,
    pub electorate_size: i64,
    /// Current review phase; exact replays can be past the initial phase.
    pub phase: LeadershipReviewPhaseWire,
    pub defense_deadline_ms: i64,
    pub discussion_deadline_ms: i64,
    pub ballot_deadline_ms: i64,
    pub replay: bool,
    pub candidate_is_caller: bool,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
