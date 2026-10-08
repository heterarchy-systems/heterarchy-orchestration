//! Source-scoped leadership review query contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;
use super::leadership_ballots::LeadershipBallotChoice;

/// Maximum statement page accepted by one leadership review query.
pub const MAX_LEADERSHIP_REVIEW_QUERY_LIMIT: u32 = 64;

/// Source-scoped read selector for one review and its statement page.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipReviewQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub request_id: Option<Uuid>,
    pub election_id: Option<Uuid>,
    pub after_sequence: u64,
    pub limit: u32,
}

/// Exact source scope and optional review snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipReviewQueryResponse {
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub review: Option<LeadershipReviewWire>,
}

/// Full bounded read snapshot of the current `Operations` leadership review.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipReviewWire {
    pub request_id: Uuid,
    pub election_id: Uuid,
    pub workspace_id: Uuid,
    /// Original expected leadership revision persisted with this review.
    pub expected_leadership_revision: i64,
    /// Original proposer Activity, when the review was initiated by a member.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub proposer_activity_id: Option<Uuid>,
    /// Original proposer `AgentInstance`, when the review was initiated by a member.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub proposer_agent_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub activity_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    pub candidate_participant_id: Uuid,
    pub candidate_subject_id: Uuid,
    pub election_state: LeadershipReviewElectionStateWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub elected_participant_id: Option<Uuid>,
    pub authority_confirmed: bool,
    pub proposer: LeadershipReviewIdentityWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub incumbent: Option<LeadershipReviewIdentityWire>,
    pub reason: String,
    pub phase: LeadershipReviewPhaseWire,
    pub defense_deadline_ms: i64,
    pub discussion_deadline_ms: i64,
    pub ballot_deadline_ms: i64,
    pub created_at_ms: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub defense_closed_at_ms: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub incumbent_response: Option<LeadershipReviewStatementWire>,
    pub electorate: Vec<LeadershipReviewVoterWire>,
    pub electorate_truncated: bool,
    pub statements: Vec<LeadershipReviewStatementWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_cursor: Option<u64>,
    pub has_more: bool,
}

/// Optional participant/profile/session bindings on a review identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipReviewIdentityWire {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participant_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
    pub display_name: String,
}

/// One frozen electorate identity and its optional ballot observation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipReviewVoterWire {
    pub participant_id: Uuid,
    pub subject_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub display_name: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub choice: Option<LeadershipBallotChoice>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cast_at_ms: Option<i64>,
}

/// One immutable review statement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipReviewStatementWire {
    pub statement_id: Uuid,
    pub election_id: Uuid,
    pub kind: LeadershipReviewStatementKindWire,
    pub author: LeadershipReviewIdentityWire,
    pub body: String,
    pub sequence: u64,
    pub submitted_at_ms: i64,
    pub incumbent_final: bool,
}

/// Closed review lifecycle phase owned by `Operations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LeadershipReviewPhaseWire {
    Defense,
    Discussion,
    Ballot,
    AwaitingDecision,
    Resolved,
}

/// Closed election lifecycle state owned by `Operations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LeadershipReviewElectionStateWire {
    Electing,
    ClosedElected,
    ClosedNoMajority,
    Cancelled,
}

/// Closed review-statement kind owned by `Operations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LeadershipReviewStatementKindWire {
    Defense,
    Discussion,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
