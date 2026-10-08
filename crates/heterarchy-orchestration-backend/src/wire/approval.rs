//! Shared approval policy and request query/mutation contracts.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

/// Exact permission ceiling attached to an approval scope or request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalPermissionWire {
    pub workspace_read: bool,
    pub workspace_write: bool,
    pub network: bool,
}

/// One bounded automatic-approval scope rule.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalAutoScopeWire {
    pub scope_id: Uuid,
    pub action: String,
    pub ceiling: ApprovalPermissionWire,
    pub max_uses: Option<u16>,
    pub expires_at_ms: i64,
}

/// Canonical Workspace approval policy projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalPolicyWire {
    pub workspace_id: Uuid,
    pub enabled: bool,
    pub revision: u64,
    pub auto_scopes: Vec<ApprovalAutoScopeWire>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Canonical approval request projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequestWire {
    pub request_id: Uuid,
    pub revision: u64,
    pub workspace_id: Uuid,
    pub action: String,
    pub session_id: Uuid,
    pub state: String,
    pub requested_permissions: ApprovalPermissionWire,
    pub policy_revision: Option<u64>,
    pub auto_scope_id: Option<Uuid>,
    pub requested_at_ms: i64,
    pub expires_at_ms: i64,
    pub decided_at_ms: Option<i64>,
    pub decided_by: Option<Uuid>,
    pub request_digest: String,
}

/// Bounded policy query selector. `session_id` remains a selector; the
/// original caller credential supplies authorization.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalPolicyQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
}

/// Bounded approval request list selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalListQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub state: Option<String>,
    pub limit: u32,
}

/// Compare-and-set approval policy command.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalPolicySetRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub enabled: bool,
    pub auto_scopes: Vec<ApprovalAutoScopeWire>,
    pub expected_revision: Option<u64>,
}

/// Policy query response. An absent policy is represented by `None`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyQueryResponse {
    pub scope: WorkspaceQueryScope,
    pub policy: Option<ApprovalPolicyWire>,
}

/// Bounded request-list response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListQueryResponse {
    pub scope: WorkspaceQueryScope,
    pub requests: Vec<ApprovalRequestWire>,
}

/// Policy mutation response with the committed canonical projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetResponse {
    pub scope: WorkspaceQueryScope,
    pub policy: ApprovalPolicyWire,
}

/// Source-scoped approval submission request. The caller credential remains
/// the authority; `session_id` is an explicitly validated session selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalSubmitRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub request_id: Uuid,
    pub action: String,
    pub request_digest: String,
    pub requested_permissions: ApprovalPermissionWire,
    pub ttl_ms: i64,
}

/// Source-scoped approval decision request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalDecideRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub request_id: Uuid,
    pub decision: String,
}

/// Source-scoped exact approval request readback selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalRequestGetRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub request_id: Uuid,
}

/// Historical admission issuance metadata attached to an approval outcome.
/// This is not a live grant or execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalAdmissionWire {
    pub admission_id: Uuid,
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub issuer: String,
    pub backend_id: String,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

/// Submission outcome with the exact scoped request projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalSubmitResponse {
    pub scope: WorkspaceQueryScope,
    pub request: ApprovalRequestWire,
    pub admission: Option<ApprovalAdmissionWire>,
    pub replay: bool,
}

/// Decision outcome with the exact scoped request projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalDecideResponse {
    pub scope: WorkspaceQueryScope,
    pub request: ApprovalRequestWire,
    pub admission: Option<ApprovalAdmissionWire>,
}

/// Exact request readback; absence carries no admission metadata.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequestGetResponse {
    pub scope: WorkspaceQueryScope,
    pub request: Option<ApprovalRequestWire>,
    pub admission: Option<ApprovalAdmissionWire>,
}
