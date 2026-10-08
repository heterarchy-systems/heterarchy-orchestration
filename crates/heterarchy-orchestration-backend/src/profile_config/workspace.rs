//! Source-fenced Profile observation shared with the production Workbench query.

use crate::wire::profiles::{
    WorkspaceProfileWire, WorkspaceProfilesQueryRequest, WorkspaceProfilesQueryResponse,
    WorkspaceSessionProfileWire,
};
use crate::{ApiError, BackendState, canonical_uuid};
use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::RuntimeError;
use heterarchy_orchestration_sessions::AgentProfileOperationalState;

pub(super) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/workspaces/{workspace_id}/profiles/query",
        post(query),
    )
}

async fn query(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceProfilesQueryRequest>,
) -> Result<Json<WorkspaceProfilesQueryResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, body.session_id)
        .map_err(|_| ApiError::InvalidIdentity)?;
    if canonical_uuid(&workspace)? != body.scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    let workspace = body.scope.workspace_id;
    let projection = state
        .run_workspace_reference_query(
            body.scope.clone(),
            body.session_id,
            principal,
            move |runtime, _| {
                runtime
                    .workspace_profile_projection(workspace)
                    .map_err(RuntimeError::from)
            },
        )
        .await?;
    Ok(Json(WorkspaceProfilesQueryResponse {
        scope: body.scope,
        profiles: projection
            .profiles
            .into_iter()
            .map(|entry| WorkspaceProfileWire {
                profile: entry.profile.into(),
                state: state_label(entry.state).to_owned(),
            })
            .collect(),
        session_profiles: projection
            .session_profiles
            .into_iter()
            .map(|entry| WorkspaceSessionProfileWire {
                session_id: entry.session_id,
                profile_id: entry.profile_id,
            })
            .collect(),
    }))
}

fn state_label(state: AgentProfileOperationalState) -> &'static str {
    match state {
        AgentProfileOperationalState::Busy => "busy",
        AgentProfileOperationalState::Online => "online",
        AgentProfileOperationalState::Offline => "offline",
        AgentProfileOperationalState::Error => "error",
        AgentProfileOperationalState::Retired => "retired",
    }
}
