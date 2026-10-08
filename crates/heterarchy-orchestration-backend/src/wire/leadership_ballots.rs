//! Scoped leadership ballot cast/query contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

/// Closed ballot choices carried over the HTTP boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LeadershipBallotChoice {
    Yes,
    No,
    Abstain,
}

/// Source-fenced ballot cast request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipBallotCastRequest {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub election_id: Uuid,
    pub choice: LeadershipBallotChoice,
}

/// Source-fenced ballot observation request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipBallotQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub election_id: Uuid,
}

/// Native ballot receipt projection returned by a cast.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipBallotReceiptWire {
    pub election_id: Uuid,
    pub voter_participant_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub voter_participation_id: Option<Uuid>,
    pub choice: LeadershipBallotChoice,
    pub ballot_revision: i64,
    pub replay: bool,
}

/// Scoped cast response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipBallotCastResponse {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub receipt: LeadershipBallotReceiptWire,
}

/// Immutable ballot observation projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipBallotWire {
    pub election_id: Uuid,
    pub voter_participant_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub voter_participation_id: Option<Uuid>,
    pub cast_by_participant_id: Uuid,
    pub choice: LeadershipBallotChoice,
    pub ballot_revision: i64,
    pub cast_at_ms: i64,
}

/// Scoped ballot observation response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipBallotQueryResponse {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub ballot: Option<LeadershipBallotWire>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
