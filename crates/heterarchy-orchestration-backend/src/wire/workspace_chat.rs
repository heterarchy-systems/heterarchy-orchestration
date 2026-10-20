//! Strict Profile-direct Workspace Chat HTTP contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::{
    WorkspaceQueryScope, deserialize_optional_non_null,
    node_chat::{ChatEntryWire, ChatReceiptWire},
    workspace_project::ProviderProjectReferenceWire,
};

/// Exact Profile-direct query. Node and leadership selectors are absent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatQueryWire {
    pub workspace_id: Uuid,
    pub agent_profile_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub runtime_session_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub before_sequence: Option<u64>,
}

/// Caller-owned Workspace/Session selector and direct query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub query: WorkspaceChatQueryWire,
}

/// Provider route fence returned by a direct query and frozen by send.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatTargetFenceWire {
    pub operation_id: Uuid,
    pub profile_id: Uuid,
    pub profile_name: String,
    pub participation_id: Uuid,
    pub participation_revision: u64,
    pub runtime_session_id: Uuid,
    pub runtime_revision: i64,
    pub provider_conversation_id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub project: Option<ProviderProjectReferenceWire>,
}

/// Immutable direct message intent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatMessageRequest {
    pub query: WorkspaceChatQueryWire,
    pub message_id: Uuid,
    pub text: String,
    pub target_fence: WorkspaceChatTargetFenceWire,
}

/// Caller-owned direct send/recovery request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatSendRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub request: WorkspaceChatMessageRequest,
}

/// Exact current direct route and bounded direct-channel history.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatSnapshotWire {
    pub workspace_id: Uuid,
    pub agent_profile_id: Uuid,
    pub targets: Vec<WorkspaceChatTargetFenceWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target: Option<WorkspaceChatTargetFenceWire>,
    pub messages: Vec<ChatEntryWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub before_sequence: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_before_sequence: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub unavailable_reason: Option<String>,
}

/// Direct query echo and projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatQueryResponse {
    pub request: WorkspaceChatQueryRequest,
    pub snapshot: WorkspaceChatSnapshotWire,
}

/// Exact direct send echo and durable outbox receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatSendResponse {
    pub request: WorkspaceChatSendRequest,
    pub receipt: ChatReceiptWire,
}

/// Pure direct recovery; absence never authorizes a resend.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChatRecoverResponse {
    pub request: WorkspaceChatSendRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<ChatReceiptWire>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
