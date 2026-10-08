//! Source-scoped leadership statement append and exact readback contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;
use super::leadership_reviews::{
    LeadershipReviewPhaseWire, LeadershipReviewStatementKindWire, LeadershipReviewStatementWire,
};

/// Append one exact, session-owned leadership statement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipStatementAppendRequest {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub election_id: Uuid,
    pub request_id: Uuid,
    pub message_id: Uuid,
    pub kind: LeadershipReviewStatementKindWire,
    pub body: String,
}

/// Append receipt with current review phase and exact statement identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipStatementAppendResponse {
    pub scope: WorkspaceQueryScope,
    pub receipt: LeadershipStatementReceiptWire,
}

/// Exact idempotent append receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipStatementReceiptWire {
    pub message_id: Uuid,
    pub election_id: Uuid,
    pub sequence: u64,
    pub phase: LeadershipReviewPhaseWire,
    pub replay: bool,
}

/// Exact message identity to resolve after an uncertain append exchange.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipStatementQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub message_id: Uuid,
}

/// Exact source scope and optional durable statement readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipStatementQueryResponse {
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub record: Option<LeadershipStatementReadbackWire>,
}

/// Durable request/election bindings returned with one exact statement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipStatementReadbackWire {
    pub workspace_id: Uuid,
    pub request_id: Uuid,
    pub statement: LeadershipReviewStatementWire,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
