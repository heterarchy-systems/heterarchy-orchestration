//! Principal-bound logical session HTTP contract.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;

use crate::wire::{
    AgentSessionCloseRequest as CloseBody,
    AgentSessionOpenReadbackResponse as OpenReadbackResponse, AgentSessionOpenRequest as OpenBody,
    AgentSessionOpenResponse as OpenResponse, AgentSessionResponse as SessionResponse,
};
use crate::{ApiError, BackendState, canonical_uuid, current_timestamp_millis};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route("/internal/v1/sessions", post(open))
        .route(
            "/internal/v1/session-open-requests/{request_id}",
            get(open_readback),
        )
        .route("/internal/v1/sessions/{session_id}", get(read))
        .route("/internal/v1/sessions/{session_id}/close", post(close))
}

async fn open_readback(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(request_id): Path<String>,
) -> Result<Json<OpenReadbackResponse>, ApiError> {
    let request_id = canonical_uuid(&request_id)?;
    let session = state
        .run_catalog(move |runtime| runtime.session_open_readback(request_id, principal))
        .await?;
    Ok(Json(OpenReadbackResponse {
        request_id,
        session: session.map(Into::into),
    }))
}

async fn open(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<OpenBody>,
) -> Result<(StatusCode, Json<OpenResponse>), ApiError> {
    let request_id = canonical_uuid(&body.request_id)?;
    let now = current_timestamp_millis()?;
    let publisher = state.clone();
    let outcome = state
        .run_mutation(move |runtime| {
            let outcome = runtime.open_session(request_id, principal, now)?;
            if outcome.created {
                publisher.publish_state_changed(
                    crate::RealtimeEventKind::LogicalSessionChanged,
                    outcome.session.session_id,
                    u64::try_from(outcome.session.revision).unwrap_or_default(),
                    None,
                );
            }
            Ok(outcome)
        })
        .await?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(OpenResponse {
            session: outcome.session.into(),
            created: outcome.created,
        }),
    ))
}

async fn read(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionResponse>, ApiError> {
    let session_id = canonical_uuid(&session_id)?;
    let session = state
        .run_catalog(move |runtime| runtime.session(session_id, principal))
        .await?;
    Ok(Json(session.into()))
}

async fn close(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(session_id): Path<String>,
    Json(body): Json<CloseBody>,
) -> Result<Json<SessionResponse>, ApiError> {
    let session_id = canonical_uuid(&session_id)?;
    let now = current_timestamp_millis()?;
    let publisher = state.clone();
    let session = state
        .run_control_mutation(move |runtime| {
            let session =
                runtime.close_session(session_id, principal, body.expected_revision, now)?;
            publisher.publish_state_changed(
                crate::RealtimeEventKind::LogicalSessionChanged,
                session.session_id,
                u64::try_from(session.revision).unwrap_or_default(),
                None,
            );
            Ok(session)
        })
        .await?;
    Ok(Json(session.into()))
}
