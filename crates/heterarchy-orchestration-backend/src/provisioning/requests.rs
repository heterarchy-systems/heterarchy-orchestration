//! Source-scoped, read-only `AgentRequest` queries.

use axum::extract::{Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::AgentRequestSummary;

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::agent_requests::{
    AgentRequestListQueryRequest, AgentRequestListQueryResponse, AgentRequestQueryRequest,
    AgentRequestQueryResponse, AgentRequestWire, MAX_AGENT_REQUEST_LIST_LIMIT,
};
use crate::{BackendState, wire::WorkspaceQueryScope};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/agent-requests/query",
            post(query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/agent-requests/list/query",
            post(list),
        )
}

async fn query(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentRequestQueryRequest>,
) -> Result<Json<AgentRequestQueryResponse>, LaneError> {
    validate_scope(&workspace_id, &body.scope, body.session_id)?;
    if body.request_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let scope = body.scope.clone();
    let workspace_id = scope.workspace_id;
    let request_id = body.request_id;
    let request = state
        .run_workspace_reference_query(body.scope, body.session_id, principal, move |runtime, _| {
            runtime.agent_request_in_workspace(workspace_id, request_id)
        })
        .await?;
    Ok(Json(AgentRequestQueryResponse {
        scope,
        request: request.as_ref().map(AgentRequestWire::from_summary),
    }))
}

async fn list(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentRequestListQueryRequest>,
) -> Result<Json<AgentRequestListQueryResponse>, LaneError> {
    validate_scope(&workspace_id, &body.scope, body.session_id)?;
    if body.limit == 0 || body.limit > MAX_AGENT_REQUEST_LIST_LIMIT {
        return Err(LaneError::InvalidIdentity);
    }
    let scope = body.scope.clone();
    let workspace_id = scope.workspace_id;
    let limit = body.limit;
    let page = state
        .run_workspace_reference_query(body.scope, body.session_id, principal, move |runtime, _| {
            runtime.agent_requests(workspace_id, limit)
        })
        .await?;
    Ok(Json(AgentRequestListQueryResponse {
        scope,
        items: page
            .items
            .iter()
            .map(AgentRequestWire::from_summary)
            .collect(),
        truncated: page.truncated,
    }))
}

fn validate_scope(
    path_workspace_id: &str,
    scope: &WorkspaceQueryScope,
    session_id: Option<uuid::Uuid>,
) -> Result<(), LaneError> {
    validate_workspace_reference(scope, session_id)?;
    if super::canonical_uuid(Some(path_workspace_id))? != scope.workspace_id {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

impl AgentRequestWire {
    fn from_summary(value: &AgentRequestSummary) -> Self {
        Self {
            request_id: value.request_id,
            workspace_id: value.workspace_id.clone(),
            activity_id: value.activity_id.clone(),
            objective: value.objective.clone(),
            capability: value.capability.clone(),
            requested_count: value.requested_count,
            reason: value.reason.clone(),
            state: value.state.to_owned(),
            created_at_ms: value.created_at_ms,
            revision: value.revision,
        }
    }
}
