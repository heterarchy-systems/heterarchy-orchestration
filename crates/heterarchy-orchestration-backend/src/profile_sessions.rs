//! Principal-bound Profile-to-logical-Session identity links.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::profiles::ProfileSessionBindingSummary;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ApiError, BackendState, canonical_uuid, current_timestamp_millis};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/agent-profiles/{profile_id}/sessions",
            post(bind),
        )
        .route(
            "/internal/v1/agent-profiles/{profile_id}/sessions/{session_id}",
            get(read),
        )
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProfileSessionBindRequest {
    pub request_id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileSessionBindResponse {
    pub request_id: Uuid,
    pub profile_id: Uuid,
    pub profile_revision: i64,
    pub session_id: Uuid,
    pub session_revision: i64,
    pub created: bool,
}

async fn bind(
    State(state): State<BackendState>,
    Path(profile_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProfileSessionBindRequest>,
) -> Result<(StatusCode, Json<ProfileSessionBindResponse>), ApiError> {
    let profile_id = canonical_uuid(&profile_id)?;
    let request_id = canonical_uuid(&body.request_id)?;
    let session_id = canonical_uuid(&body.session_id)?;
    let now_ms = current_timestamp_millis()?;
    let outcome = state
        .run_session_mutation(session_id, principal, move |runtime| {
            runtime.bind_profile_session(request_id, profile_id, session_id, principal, now_ms)
        })
        .await?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(outcome.into())))
}

async fn read(
    State(state): State<BackendState>,
    Path((profile_id, session_id)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ProfileSessionBindResponse>, ApiError> {
    let profile_id = canonical_uuid(&profile_id)?;
    let session_id = canonical_uuid(&session_id)?;
    let binding = state
        .run_catalog(move |runtime| {
            runtime.profile_session_binding(profile_id, session_id, principal)
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(binding.into()))
}

impl From<ProfileSessionBindingSummary> for ProfileSessionBindResponse {
    fn from(summary: ProfileSessionBindingSummary) -> Self {
        Self {
            request_id: summary.request_id,
            profile_id: summary.profile_id,
            profile_revision: summary.profile_revision,
            session_id: summary.session_id,
            session_revision: summary.session_revision,
            created: summary.created,
        }
    }
}
