//! Profile configuration commands share the existing runtime and operator gate.

mod workspace;

use crate::wire::profiles::{
    ProfileLifecycleRequest, ProfileMutationResponse, ProfileUpdateRequest,
};
use crate::{ApiError, BackendState, canonical_uuid, current_timestamp_millis};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::{get, post, put},
};
use forge_domain::{AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_runtime::profiles::ProfileMutationSummary;
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileRevision, ProfileConfiguration, ProfileMutation,
    ProfileMutationCommand,
};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .merge(workspace::routes())
        .route(
            "/internal/v1/agent-profiles/{profile_id}/configuration",
            put(update),
        )
        .route(
            "/internal/v1/agent-profiles/{profile_id}/disable",
            post(disable),
        )
        .route(
            "/internal/v1/agent-profiles/{profile_id}/retire",
            post(retire),
        )
        .route("/internal/v1/profile-mutations/{request_id}", get(receipt))
}

async fn update(
    State(state): State<BackendState>,
    Path(profile_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProfileUpdateRequest>,
) -> Result<Json<ProfileMutationResponse>, ApiError> {
    let configuration = ProfileConfiguration::new(
        body.display_name,
        body.preferred_model,
        body.preferred_reasoning,
        body.base_capabilities,
    )
    .map_err(crate::RuntimeError::from)
    .map_err(ApiError::Runtime)?;
    mutate(
        state,
        principal,
        profile_id,
        ProfileLifecycleRequest {
            request_id: body.request_id,
            expected_revision: body.expected_revision,
        },
        ProfileMutation::Update(configuration),
    )
    .await
}

async fn disable(
    State(state): State<BackendState>,
    Path(profile_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProfileLifecycleRequest>,
) -> Result<Json<ProfileMutationResponse>, ApiError> {
    mutate(state, principal, profile_id, body, ProfileMutation::Disable).await
}

async fn retire(
    State(state): State<BackendState>,
    Path(profile_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProfileLifecycleRequest>,
) -> Result<Json<ProfileMutationResponse>, ApiError> {
    mutate(state, principal, profile_id, body, ProfileMutation::Retire).await
}

async fn mutate(
    state: BackendState,
    principal: AuthenticatedPrincipal,
    profile_id: String,
    body: ProfileLifecycleRequest,
    operation: ProfileMutation,
) -> Result<Json<ProfileMutationResponse>, ApiError> {
    let command = ProfileMutationCommand::new(
        canonical_uuid(&body.request_id)?,
        AgentProfileId::from_uuid(canonical_uuid(&profile_id)?)
            .map_err(|_| ApiError::InvalidIdentity)?,
        AgentProfileRevision::new(body.expected_revision).map_err(|_| ApiError::InvalidIdentity)?,
        operation,
        TimestampMillis::new(current_timestamp_millis()?).map_err(|_| ApiError::Unavailable)?,
    )
    .map_err(|_| ApiError::InvalidIdentity)?;
    let publisher = state.clone();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let outcome = runtime.mutate_agent_profile(&command)?;
            if !outcome.replayed && outcome.expected_revision != Some(outcome.profile.revision) {
                publisher.publish_state_changed(
                    crate::RealtimeEventKind::AgentProfileStateChanged,
                    outcome.profile.profile_id,
                    u64::try_from(outcome.profile.revision).unwrap_or_default(),
                    None,
                );
            }
            Ok(outcome)
        })
        .await?;
    Ok(Json(outcome.into()))
}

async fn receipt(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ProfileMutationResponse>, ApiError> {
    let request = canonical_uuid(&request_id)?;
    let outcome = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.profile_mutation_receipt(request)
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(outcome.into()))
}

impl From<ProfileMutationSummary> for ProfileMutationResponse {
    fn from(receipt: ProfileMutationSummary) -> Self {
        Self {
            request_id: receipt.request_id,
            expected_revision: receipt.expected_revision,
            kind: receipt.kind.to_owned(),
            profile: receipt.profile.into(),
            replayed: receipt.replayed,
        }
    }
}
