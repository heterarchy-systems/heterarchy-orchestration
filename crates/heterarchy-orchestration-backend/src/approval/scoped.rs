//! Source-fenced Approval contracts consumed by remote MCP.
//! Queries never create a policy or Workspace. Operator mutations recheck
//! their authenticated caller and owned Session after source/Broker waits.

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::approval::{
    ApprovalAutoScopeWire, ApprovalListQueryRequest, ApprovalPermissionWire,
    ApprovalPolicyQueryRequest, ApprovalPolicySetRequest, ApprovalPolicyWire, ApprovalRequestWire,
    ListQueryResponse, PolicyQueryResponse, SetResponse,
};
use crate::{BackendState, RealtimeEventKind};
use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::{ApprovalRequestState, AuthenticatedPrincipal};
use heterarchy_orchestration_runtime::{ApprovalPolicySummary, approval::ApprovalRequestSummary};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-policy/query",
            post(policy_query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-requests/query",
            post(requests_query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-policy/set",
            post(policy_set),
        )
}

async fn policy_query(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ApprovalPolicyQueryRequest>,
) -> Result<Json<PolicyQueryResponse>, LaneError> {
    validate_scope(&workspace, &body.scope, body.session_id)?;
    let workspace_id = body.scope.workspace_id;
    let policy = state
        .run_workspace_reference_query(
            body.scope.clone(),
            body.session_id,
            principal,
            move |runtime, _| runtime.approval_policy_summary(workspace_id),
        )
        .await?;
    Ok(Json(PolicyQueryResponse {
        scope: body.scope,
        policy: policy.as_ref().map(policy_wire),
    }))
}

async fn requests_query(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ApprovalListQueryRequest>,
) -> Result<Json<ListQueryResponse>, LaneError> {
    validate_scope(&workspace, &body.scope, body.session_id)?;
    if !(1..=64).contains(&body.limit) {
        return Err(LaneError::InvalidIdentity);
    }
    let query = heterarchy_orchestration_operations::ApprovalRequestQuery {
        workspace_id: super::ops_workspace(body.scope.workspace_id)?,
        state: body.state.as_deref().map(request_state).transpose()?,
        limit: usize::try_from(body.limit).map_err(|_| LaneError::InvalidIdentity)?,
    };
    let requests = state
        .run_workspace_reference_query(
            body.scope.clone(),
            body.session_id,
            principal,
            move |runtime, _| runtime.approval_requests(query),
        )
        .await?;
    Ok(Json(ListQueryResponse {
        scope: body.scope,
        requests: requests.iter().map(request_wire).collect(),
    }))
}

async fn policy_set(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ApprovalPolicySetRequest>,
) -> Result<Json<SetResponse>, LaneError> {
    validate_scope(&workspace, &body.scope, Some(body.session_id))?;
    if body.auto_scopes.len() > heterarchy_orchestration_operations::MAX_APPROVAL_AUTO_SCOPES {
        return Err(LaneError::InvalidIdentity);
    }
    let scopes = body
        .auto_scopes
        .iter()
        .map(|scope| {
            super::auto_scope(&super::AutoScopeBody {
                scope_id: scope.scope_id.to_string(),
                action: scope.action.clone(),
                ceiling: super::PermissionCeilingBody {
                    workspace_read: scope.ceiling.workspace_read,
                    workspace_write: scope.ceiling.workspace_write,
                    network: scope.ceiling.network,
                },
                max_uses: scope.max_uses,
                expires_at_ms: scope.expires_at_ms,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let workspace_id = body.scope.workspace_id;
    let scope = body.scope.clone();
    let publisher = state.clone();
    let policy = state
        .run_scoped_operator_mutation(
            body.scope,
            body.session_id,
            principal,
            move |runtime, now| {
                let policy = runtime.set_approval_policy(
                    heterarchy_orchestration_operations::SetApprovalPolicyCommand {
                        workspace_id: forge_domain::WorkspaceId::from_uuid(workspace_id),
                        enabled: body.enabled,
                        auto_scopes: scopes,
                        expected_revision: body.expected_revision,
                        now_ms: now,
                    },
                )?;
                publisher.publish_state_changed(
                    RealtimeEventKind::ApprovalPolicyChanged,
                    workspace_id,
                    policy.revision,
                    Some(workspace_id),
                );
                Ok(policy)
            },
        )
        .await?;
    Ok(Json(SetResponse {
        scope,
        policy: policy_wire(&policy),
    }))
}

pub(super) fn validate_scope(
    path: &str,
    scope: &crate::wire::WorkspaceQueryScope,
    session: Option<uuid::Uuid>,
) -> Result<(), LaneError> {
    validate_workspace_reference(scope, session)?;
    if super::canonical_uuid(Some(path))? != scope.workspace_id {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn request_state(state: &str) -> Result<ApprovalRequestState, LaneError> {
    match state {
        "pending" => Ok(ApprovalRequestState::Pending),
        "approved" => Ok(ApprovalRequestState::Approved),
        "rejected" => Ok(ApprovalRequestState::Rejected),
        "auto_approved" => Ok(ApprovalRequestState::AutoApproved),
        "denied" => Ok(ApprovalRequestState::Denied),
        "cancelled" => Ok(ApprovalRequestState::Cancelled),
        "expired" => Ok(ApprovalRequestState::Expired),
        "superseded" => Ok(ApprovalRequestState::Superseded),
        _ => Err(LaneError::InvalidIdentity),
    }
}

fn policy_wire(policy: &ApprovalPolicySummary) -> ApprovalPolicyWire {
    ApprovalPolicyWire {
        workspace_id: policy.workspace_id,
        enabled: policy.enabled,
        revision: policy.revision,
        created_at_ms: policy.created_at_ms,
        updated_at_ms: policy.updated_at_ms,
        auto_scopes: policy
            .auto_scopes
            .iter()
            .map(|scope| ApprovalAutoScopeWire {
                scope_id: scope.scope_id,
                action: scope.action.to_owned(),
                ceiling: permissions(scope.permission_ceiling),
                max_uses: scope.max_uses,
                expires_at_ms: scope.expires_at_ms,
            })
            .collect(),
    }
}

fn permissions(
    value: heterarchy_orchestration_runtime::approval::PermissionCeilingSummary,
) -> ApprovalPermissionWire {
    ApprovalPermissionWire {
        workspace_read: value.workspace_read,
        workspace_write: value.workspace_write,
        network: value.network,
    }
}

pub(super) fn request_wire(request: &ApprovalRequestSummary) -> ApprovalRequestWire {
    ApprovalRequestWire {
        request_id: request.request_id,
        revision: request.revision,
        workspace_id: request.workspace_id,
        request_digest: digest_hex(&request.request_digest),
        action: request.action.to_owned(),
        session_id: request.session_id,
        state: request.state.to_owned(),
        requested_permissions: permissions(request.requested_permissions),
        policy_revision: request.policy_revision,
        auto_scope_id: request.auto_scope_id,
        requested_at_ms: request.requested_at_ms,
        expires_at_ms: request.expires_at_ms,
        decided_at_ms: request.decided_at_ms,
        decided_by: request.decided_by,
    }
}

pub(super) fn digest_hex(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}
