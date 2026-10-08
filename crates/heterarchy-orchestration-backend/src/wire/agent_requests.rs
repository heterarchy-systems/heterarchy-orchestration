//! Source-scoped `AgentRequest` query contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

/// `Workspace` selector for one `AgentRequest` read.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestQueryRequest {
    pub scope: super::leadership::WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub request_id: Uuid,
}

/// Exact source scope and optional `Workspace`-owned `AgentRequest`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRequestQueryResponse {
    pub scope: super::leadership::WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub request: Option<AgentRequestWire>,
}

/// `Workspace` selector and owner-bounded newest-first `AgentRequest` page.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestListQueryRequest {
    pub scope: super::leadership::WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub limit: u32,
}

/// Exact source scope and bounded `AgentRequest` page.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRequestListQueryResponse {
    pub scope: super::leadership::WorkspaceQueryScope,
    pub items: Vec<AgentRequestWire>,
    pub truncated: bool,
}

/// The exact Runtime `AgentRequest` summary fields used by the existing detail
/// projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRequestWire {
    pub request_id: Uuid,
    pub workspace_id: String,
    pub activity_id: String,
    pub objective: String,
    pub capability: String,
    pub requested_count: u8,
    pub reason: String,
    pub state: String,
    pub created_at_ms: i64,
    pub revision: u64,
}

/// Shared request-page maximum, matching the `Operations`
/// `MAX_AGENT_REQUESTS_PER_READ` value.
pub const MAX_AGENT_REQUEST_LIST_LIMIT: u32 = 256;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
