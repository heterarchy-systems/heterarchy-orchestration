//! Scoped Agent provision operation query/cancellation contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvisionOperationQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub operation_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvisionOperationCancelRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub operation_id: Uuid,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvisionOperationWire {
    pub operation_id: Uuid,
    pub request_id: Uuid,
    pub slot_ordinal: u8,
    pub effect_kind: String,
    pub provider_family: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub runtime_session_id: Option<Uuid>,
    pub state: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub provider_conversation_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub reported_failure_code: Option<String>,
    pub prepared_at_ms: i64,
    pub updated_at_ms: i64,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvisionOperationQueryResponse {
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub operation: Option<ProvisionOperationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvisionOperationCancelResponse {
    pub scope: WorkspaceQueryScope,
    pub operation: ProvisionOperationWire,
    pub changed: bool,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
