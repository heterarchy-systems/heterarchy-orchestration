//! Read the complete Source binding through the existing authorized read lane.

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::AuthenticatedPrincipal;

use crate::wire::forge_reads::{
    CoordinatorBindingWire, CoordinatorGetRequest, CoordinatorGetResponse,
};
use crate::{ApiError, BackendState, CoordinatorAuthorityError};

pub(super) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/workspaces/{workspace_id}/forge/coordinator-get",
        post(get),
    )
}

async fn get(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<CoordinatorGetRequest>,
) -> Result<Json<CoordinatorGetResponse>, ApiError> {
    let _context = super::file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    // Requiring the configured HTTP client here keeps Control-backed hosts
    // fail-closed, even though the shared observation owner supports them.
    let (permit, _client) = super::forge_source(&state).await?;
    let authority = state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let binding = authority
        .coordinator_observation(body.scope.clone(), body.activity_id, permit)
        .await
        .map_err(|error| match error {
            CoordinatorAuthorityError::Denied => {
                ApiError::Runtime(heterarchy_orchestration_runtime::RuntimeError::Forbidden)
            }
            CoordinatorAuthorityError::Stale => {
                ApiError::Runtime(heterarchy_orchestration_runtime::RuntimeError::Conflict)
            }
            CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
        })?;
    Ok(Json(CoordinatorGetResponse {
        scope: body.scope,
        activity_id: body.activity_id,
        coordinator: binding.as_ref().map(CoordinatorBindingWire::from),
    }))
}
