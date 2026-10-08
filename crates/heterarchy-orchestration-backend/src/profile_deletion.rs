//! Authenticated operator routes for resumable Profile deletion.

use axum::extract::{Extension, Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::RuntimeError;

use crate::realtime::RealtimeEventKind;
use crate::wire::profile_deletion::{
    AgentProfileDeletionRequest, AgentProfileDeletionResponse, AgentProfileDeletionResumeResponse,
    AgentProfileProviderDeletionAckResponse, AgentProfileProviderDeletionDispatchResponse,
    AgentProfileProviderDeletionFenceWire, AgentProfileProviderDeletionNextResponse,
    AgentProfileProviderDeletionOperationWire, AgentProfileProviderDeletionReadbackResponse,
    AgentProfileProviderDeletionReceiptObservation, AgentProfileProviderDeletionReceiptRequest,
    AgentProfileProviderDeletionSelectionResponse, AgentProfileProviderDeletionUnknownRequest,
};
use crate::{ApiError, BackendState, canonical_uuid, current_timestamp_millis};
use heterarchy_orchestration_runtime::profile_deletion::AgentProfileDeletionSnapshot;

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/agent-profile-provider-deletions/in-flight",
            get(provider_selection),
        )
        .route(
            "/internal/v1/agent-profile-provider-deletions/{operation_id}/query",
            post(provider_readback),
        )
        .route(
            "/internal/v1/agent-profiles/{profile_id}/deletion",
            get(read_profile).post(begin),
        )
        .route(
            "/internal/v1/agent-profile-deletions/{request_id}",
            get(read_request),
        )
        .route(
            "/internal/v1/agent-profile-deletions/{request_id}/resume",
            post(resume),
        )
        .route(
            "/internal/v1/agent-profile-provider-deletions/next",
            post(next_dispatch),
        )
        .route(
            "/internal/v1/agent-profile-provider-deletions/{operation_id}/unknown",
            post(mark_unknown),
        )
        .route(
            "/internal/v1/agent-profile-provider-deletions/{operation_id}/receipt",
            post(record_receipt),
        )
}

async fn begin(
    State(state): State<BackendState>,
    Path(profile_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentProfileDeletionRequest>,
) -> Result<Json<AgentProfileDeletionResponse>, ApiError> {
    let profile_id = canonical_uuid(&profile_id)?;
    let request_id = canonical_uuid(&body.request_id)?;
    let requested_at_ms = current_timestamp_millis()?;
    let publisher = state.clone();
    let snapshot = state
        .run_operator_mutation(principal, move |runtime| {
            let before = runtime.agent_profile_deletion(profile_id)?;
            let snapshot = runtime.begin_agent_profile_deletion(
                request_id,
                profile_id,
                body.expected_profile_revision,
                requested_at_ms,
            )?;
            publish_snapshot_changes(&publisher, before.as_ref(), &snapshot);
            Ok(snapshot)
        })
        .await?;
    Ok(Json(snapshot.into()))
}

async fn provider_selection(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<AgentProfileProviderDeletionSelectionResponse>, ApiError> {
    let operation = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            Ok(runtime
                .profile_deletion_provider_selection()?
                .as_ref()
                .map(AgentProfileProviderDeletionOperationWire::from))
        })
        .await?;
    Ok(Json(AgentProfileProviderDeletionSelectionResponse {
        operation,
        retry_authorized: false,
        request_equivalence_proven: false,
    }))
}

async fn provider_readback(
    State(state): State<BackendState>,
    Path(operation_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentProfileProviderDeletionUnknownRequest>,
) -> Result<Json<AgentProfileProviderDeletionReadbackResponse>, ApiError> {
    let operation_id = canonical_uuid(&operation_id)?;
    let fence = parse_fence(body.fence)?;
    if fence.operation_id() != operation_id {
        return Err(ApiError::InvalidIdentity);
    }
    let readback = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.profile_deletion_provider_readback(&fence)
        })
        .await?;
    Ok(Json(AgentProfileProviderDeletionReadbackResponse {
        operation: (&readback.operation).into(),
        receipt: readback
            .receipt
            .as_ref()
            .map(AgentProfileProviderDeletionReceiptObservation::try_from)
            .transpose()
            .map_err(|_| ApiError::Unavailable)?,
        retry_authorized: false,
        request_equivalence_proven: false,
    }))
}

async fn read_profile(
    State(state): State<BackendState>,
    Path(profile_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<AgentProfileDeletionResponse>, ApiError> {
    let profile_id = canonical_uuid(&profile_id)?;
    let snapshot = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.agent_profile_deletion(profile_id)
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(snapshot.into()))
}

async fn read_request(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<AgentProfileDeletionResponse>, ApiError> {
    let request_id = canonical_uuid(&request_id)?;
    let snapshot = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.agent_profile_deletion_request(request_id)
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(snapshot.into()))
}

async fn resume(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<AgentProfileDeletionResumeResponse>, ApiError> {
    let request_id = canonical_uuid(&request_id)?;
    let observed_at_ms = current_timestamp_millis()?;
    let publisher = state.clone();
    let summary = state
        .run_operator_control_mutation(principal, move |runtime| {
            let before = runtime.agent_profile_deletion_request(request_id)?;
            let summary = runtime.resume_agent_profile_deletion(request_id, observed_at_ms)?;
            publish_snapshot_changes(&publisher, before.as_ref(), &summary.snapshot);
            Ok(summary)
        })
        .await?;
    Ok(Json(summary.into()))
}

async fn next_dispatch(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<AgentProfileProviderDeletionNextResponse>, ApiError> {
    let observed_at_ms = current_timestamp_millis()?;
    let publisher = state.clone();
    let dispatch = state
        .run_operator_control_mutation(principal, move |runtime| {
            let dispatch = runtime.next_agent_profile_provider_deletion_dispatch(observed_at_ms)?;
            if let Some(dispatch) = dispatch.as_ref().filter(|value| {
                value.fence.action()
                    == heterarchy_orchestration_operations::AgentProfileProviderDeletionAction::Execute
            }) {
                publisher.publish_state_changed(
                    RealtimeEventKind::AgentProfileStateChanged,
                    dispatch.fence.operation_id(),
                    dispatch.fence.expected_revision(),
                    None,
                );
            }
            Ok(dispatch)
        })
        .await?
        .map(AgentProfileProviderDeletionDispatchResponse::from);
    Ok(Json(AgentProfileProviderDeletionNextResponse { dispatch }))
}

async fn mark_unknown(
    State(state): State<BackendState>,
    Path(operation_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentProfileProviderDeletionUnknownRequest>,
) -> Result<Json<AgentProfileProviderDeletionAckResponse>, ApiError> {
    let operation_id = canonical_uuid(&operation_id)?;
    let fence = parse_fence(body.fence)?;
    if fence.operation_id() != operation_id {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let observed_at_ms = current_timestamp_millis()?;
    let publisher = state.clone();
    let receipt = state
        .run_operator_control_mutation(principal, move |runtime| {
            let receipt =
                runtime.mark_agent_profile_provider_deletion_unknown(&fence, observed_at_ms)?;
            publish_operation_change(
                &publisher,
                receipt.fence.operation_id(),
                receipt.fence.expected_revision(),
            );
            Ok(receipt)
        })
        .await?;
    Ok(Json(receipt.into()))
}

async fn record_receipt(
    State(state): State<BackendState>,
    Path(operation_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentProfileProviderDeletionReceiptRequest>,
) -> Result<Json<AgentProfileProviderDeletionAckResponse>, ApiError> {
    let operation_id = canonical_uuid(&operation_id)?;
    let fence = parse_fence(body.fence)?;
    if fence.operation_id() != operation_id {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let input = body.receipt.into();
    let observed_at_ms = current_timestamp_millis()?;
    let publisher = state.clone();
    let receipt = state
        .run_operator_control_mutation(principal, move |runtime| {
            let receipt = runtime.record_agent_profile_provider_deletion_receipt(
                &fence,
                &input,
                observed_at_ms,
            )?;
            publish_operation_change(
                &publisher,
                receipt.fence.operation_id(),
                receipt.fence.expected_revision(),
            );
            Ok(receipt)
        })
        .await?;
    Ok(Json(receipt.into()))
}

fn parse_fence(
    fence: AgentProfileProviderDeletionFenceWire,
) -> Result<
    heterarchy_orchestration_runtime::profile_deletion::AgentProfileProviderDeletionFence,
    ApiError,
> {
    fence.into_runtime().map_err(ApiError::Runtime)
}

fn publish_snapshot_changes(
    publisher: &BackendState,
    before: Option<&AgentProfileDeletionSnapshot>,
    after: &AgentProfileDeletionSnapshot,
) {
    let profile_changed = before.is_none_or(|before| {
        before.profile.revision != after.profile.revision
            || before.profile.lifecycle_state != after.profile.lifecycle_state
    });
    if profile_changed && let Ok(revision) = u64::try_from(after.profile.revision) {
        publisher.publish_state_changed(
            RealtimeEventKind::AgentProfileStateChanged,
            after.profile.profile_id,
            revision,
            None,
        );
    }

    let progress_changed = before.is_none_or(|before| before.deletion != after.deletion);
    if progress_changed
        && let Some(progress) = after.deletion.as_ref()
        && let Some(revision) = progress.revision
    {
        publisher.publish_state_changed(
            RealtimeEventKind::AgentProfileStateChanged,
            progress.request_id,
            revision,
            None,
        );
    }
}

fn publish_operation_change(
    publisher: &BackendState,
    operation_id: uuid::Uuid,
    expected_revision: u64,
) {
    if let Some(revision) = expected_revision.checked_add(1) {
        publisher.publish_state_changed(
            RealtimeEventKind::AgentProfileStateChanged,
            operation_id,
            revision,
            None,
        );
    }
}
