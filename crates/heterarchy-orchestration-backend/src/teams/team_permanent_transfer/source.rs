//! One owned HTTP lifecycle for first Source send and query-only reconciliation.
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::post,
};
use forge_backend::{ForgeHttpHomeTransferSourceError, HomeTransferSourceRecord};
use forge_control::{
    HomeTransferSourceCommand, HomeTransferSourceOperation, HomeTransferSourceResponse,
};
use forge_domain::{AuthenticatedPrincipal, HomeTransferSourceAction, TimestampMillis};
use heterarchy_orchestration_operations::{
    TeamPermanentTransferSourceDispatchOutcome, TeamPermanentTransferSourceDispatchReadback,
    TeamPermanentTransferSourceDispatchReceipt, TeamPermanentTransferSourceReceiptCommand,
    TeamPermanentTransferSourceWireIdentity, WorkspaceProjectScope,
    derive_team_permanent_transfer_source_request,
};
use heterarchy_orchestration_runtime::{
    RuntimeError,
    team_permanent_transfer::{
        TeamPermanentTransferPreparationReadSpec, TeamPermanentTransferSourceDispatchSpec,
    },
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{actor_target, participation_target, validate_actor_path};
use crate::{
    ApiError, BackendState, CoordinatorAuthorityError,
    app_support::{LaneError, validate_workspace_reference},
    coordinator_authority::ConfiguredHomeTransferSource,
    wire::{WorkspaceQueryScope, team_permanent_transfer as wire},
};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/source/dispatch",
            post(dispatch),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/source/readback",
            post(readback),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/source/reconcile",
            post(reconcile),
        )
}

async fn dispatch(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferSourceDispatchRequest>,
) -> Result<Json<wire::TeamPermanentTransferSourceDispatchResponse>, LaneError> {
    validate_actor_path(&path, &request.actor)?;
    if !wire::valid_team_permanent_transfer_source_dispatch_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let owned = state.clone();
    state
        .run_owned(Box::pin(async move {
            dispatch_owned(&owned, request, principal).await
        }))
        .await
        .map(Json)
        .map_err(Into::into)
}

async fn readback(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferSourceReadbackRequest>,
) -> Result<Json<wire::TeamPermanentTransferSourceReadbackResponse>, LaneError> {
    validate_read_request(&path, &request)?;
    let owned = state.clone();
    state
        .run_owned(async move {
            let record = load_local(&owned, &request, principal).await?;
            wire::TeamPermanentTransferSourceReadbackResponse::from_operations(
                &request,
                record.as_ref(),
            )
            .ok_or(ApiError::SourceBindingUnconfirmed)
        })
        .await
        .map(Json)
        .map_err(Into::into)
}

async fn reconcile(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferSourceReconcileRequest>,
) -> Result<Json<wire::TeamPermanentTransferSourceReconcileResponse>, LaneError> {
    validate_read_request(&path, &request)?;
    let owned = state.clone();
    state
        .run_owned(async move {
            let Some(local) = load_local(&owned, &request, principal).await? else {
                return wire::TeamPermanentTransferSourceReadbackResponse::from_operations(
                    &request, None,
                )
                .ok_or(ApiError::SourceBindingUnconfirmed);
            };
            let mut ingestion_attempted = false;
            if local.original_source_record().is_none() {
                let (scope, source) = configured_source(&owned, &request.current_actor.scope)?;
                validate_recovery_route(local.dispatch(), &request.current_actor.scope, &source)?;
                if let Some(record) =
                    source_get(&source, scope, local.dispatch().source_request()).await?
                {
                    ingestion_attempted = true;
                    ingest(&owned, local.dispatch(), record)
                        .await
                        .map_err(|error| {
                            tracing::debug!(
                                ?error,
                                "Home Source reconciliation needs receipt readback"
                            );
                            ApiError::SourceBindingUnconfirmed
                        })?;
                }
            }
            let current = load_local(&owned, &request, principal)
                .await
                .map_err(|error| {
                    if ingestion_attempted {
                        ApiError::SourceBindingUnconfirmed
                    } else {
                        error
                    }
                })?;
            wire::TeamPermanentTransferSourceReadbackResponse::from_operations(
                &request,
                current.as_ref(),
            )
            .ok_or(ApiError::SourceBindingUnconfirmed)
        })
        .await
        .map(Json)
        .map_err(Into::into)
}

fn validate_read_request(
    path: &str,
    request: &wire::TeamPermanentTransferSourceReadbackRequest,
) -> Result<(), LaneError> {
    validate_actor_path(path, &request.current_actor)?;
    if crate::canonical_uuid(path)? != request.workspace_id
        || !wire::valid_team_permanent_transfer_source_readback_request(request)
    {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn read_request(
    request: &wire::TeamPermanentTransferSourceDispatchRequest,
) -> wire::TeamPermanentTransferSourceReadbackRequest {
    wire::TeamPermanentTransferSourceReadbackRequest {
        workspace_id: request.actor.scope.workspace_id,
        preparation_id: request.command.preparation_id,
        current_actor: request.actor.clone(),
    }
}

/// Local history requires authenticated participation and owner read permission,
/// but no current remote Source availability, Broker writer or new grant.
async fn load_local(
    state: &BackendState,
    request: &wire::TeamPermanentTransferSourceReadbackRequest,
    principal: AuthenticatedPrincipal,
) -> Result<Option<TeamPermanentTransferSourceDispatchReadback>, ApiError> {
    validate_workspace_reference(
        &request.current_actor.scope,
        Some(request.current_actor.session_id),
    )?;
    let spec = TeamPermanentTransferPreparationReadSpec {
        workspace_id: request.workspace_id,
        preparation_id: request.preparation_id,
        actor: actor_target(&request.current_actor),
    };
    state
        .catalog_job(move |runtime| {
            runtime.read_team_permanent_transfer_source_dispatch(spec, principal)
        })
        .await
}

#[allow(
    clippy::too_many_lines,
    reason = "the one-shot send branches retain a single owned lifecycle and explicit commit boundaries"
)]
async fn dispatch_owned(
    state: &BackendState,
    request: wire::TeamPermanentTransferSourceDispatchRequest,
    principal: AuthenticatedPrincipal,
) -> Result<wire::TeamPermanentTransferSourceDispatchResponse, ApiError> {
    let history_request = read_request(&request);
    if let Some(local) = load_local(state, &history_request, principal).await? {
        return dispatch_response(
            &request,
            &local,
            wire::TeamPermanentTransferSourceDispatchDispositionWire::Existing,
        );
    }
    let actor = actor_target(&request.actor);
    state
        .activity_scope_proof(request.actor.scope.clone(), actor.activity_id)
        .await?;
    let prep_spec = TeamPermanentTransferPreparationReadSpec {
        workspace_id: actor.workspace_id,
        preparation_id: request.command.preparation_id,
        actor,
    };
    let preparation = state
        .catalog_job(move |runtime| {
            runtime.read_team_permanent_transfer_preparation(prep_spec, principal)
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    let (access_scope, source) = configured_source(state, &request.actor.scope)?;
    let source_scope = operations_scope(&request.actor.scope)?;
    let service = source.service();
    let source_request = derive_team_permanent_transfer_source_request(
        &preparation,
        &source_scope,
        service.backend_id,
        service.destination_id,
    )
    .map_err(RuntimeError::persistence)
    .map_err(ApiError::Runtime)?;
    // This authenticated Get is preflight, not evidence that no delayed request
    // exists. A positive original can be recorded without a redundant mutation.
    let already_source = source_get(&source, access_scope.clone(), &source_request).await?;
    let grant = source
        .grant(&source_request, HomeTransferSourceAction::Prepare, now()?)
        .map_err(authority_error)?;
    let source_command = HomeTransferSourceCommand {
        scope: access_scope,
        service,
        operation: HomeTransferSourceOperation::Prepare {
            request: source_request.clone(),
            grant,
        },
    };
    let prepared = source
        .client()
        .prepare_home_transfer_source_command(&source_command, Uuid::now_v7())
        .map_err(|error| source_error(&error))?;
    // Reserve transport capacity before committing a durable one-shot attempt.
    let reserved = source
        .client()
        .reserve_home_transfer_source_send(prepared)
        .map_err(|error| source_error(&error))?;
    let body = reserved.prepared().body_bytes();
    let wire_identity = TeamPermanentTransferSourceWireIdentity::new(
        reserved.prepared().request_id(),
        Sha256::digest(body).into(),
        body.len(),
    )
    .map_err(RuntimeError::persistence)
    .map_err(ApiError::Runtime)?;
    let spec = TeamPermanentTransferSourceDispatchSpec {
        actor,
        preparation_id: request.command.preparation_id,
        transfer_id: request.command.transfer_id,
        generation: request.command.expected_generation,
        expected_workspace_leadership_revision: request
            .command
            .expected_workspace_leadership_revision,
        request_id: request.command.request_id,
        source_route: reserved.prepared().authority(),
        source_scope,
        backend_id: service.backend_id,
        issuer_id: service.issuer,
        service_destination_id: service.destination_id,
        source_request,
        grant,
        wire_identity,
    };
    state
        .activity_scope_proof(request.actor.scope.clone(), actor.activity_id)
        .await?;
    state
        .runtime
        .ensure_broker_owner()
        .await
        .map_err(RuntimeError::BrokerOwner)
        .map_err(ApiError::Runtime)?;
    let outcome = state
        .catalog_job(move |runtime| {
            runtime.require_participation(participation_target(actor), principal)?;
            runtime.commit_team_permanent_transfer_source_dispatch(spec, principal)
        })
        .await?;
    let disposition = match outcome {
        TeamPermanentTransferSourceDispatchOutcome::Existing(_) => {
            drop(reserved);
            wire::TeamPermanentTransferSourceDispatchDispositionWire::Existing
        }
        TeamPermanentTransferSourceDispatchOutcome::Created(permit) => {
            let dispatch = permit.record();
            if dispatch.wire_identity() != wire_identity
                || dispatch.source_request()
                    != match &reserved.prepared().command().operation {
                        HomeTransferSourceOperation::Prepare { request, .. } => request,
                        _ => return Err(ApiError::SourceBindingUnconfirmed),
                    }
            {
                return Err(ApiError::SourceBindingUnconfirmed);
            }
            let original = if let Some(record) = already_source {
                drop(reserved);
                Some(record)
            } else {
                match reserved.send().await {
                    Ok(HomeTransferSourceResponse::Outcome(record)) => Some(record),
                    Ok(
                        HomeTransferSourceResponse::Record(_)
                        | HomeTransferSourceResponse::ReservationRetirementOutcome(_)
                        | HomeTransferSourceResponse::ReservationRetirementRecord(_),
                    ) => {
                        return Err(ApiError::SourceBindingUnconfirmed);
                    }
                    Err(error) => {
                        tracing::debug!(
                            ?error,
                            "Home Source attempt requires exact query reconciliation"
                        );
                        None
                    }
                }
            };
            if let Some(record) = original {
                ingest(state, dispatch, record).await.map_err(|error| {
                    tracing::debug!(?error, "Home Source result needs durable receipt readback");
                    ApiError::SourceBindingUnconfirmed
                })?;
            }
            wire::TeamPermanentTransferSourceDispatchDispositionWire::Created
        }
    };
    let local = load_local(state, &history_request, principal)
        .await
        .map_err(|_| ApiError::SourceBindingUnconfirmed)?
        .ok_or(ApiError::SourceBindingUnconfirmed)?;
    dispatch_response(&request, &local, disposition)
}

fn dispatch_response(
    request: &wire::TeamPermanentTransferSourceDispatchRequest,
    local: &TeamPermanentTransferSourceDispatchReadback,
    disposition: wire::TeamPermanentTransferSourceDispatchDispositionWire,
) -> Result<wire::TeamPermanentTransferSourceDispatchResponse, ApiError> {
    let stored =
        wire::TeamPermanentTransferSourceDispatchRecordWire::from_operations(local.dispatch())
            .ok_or(ApiError::SourceBindingUnconfirmed)?;
    if !wire::valid_team_permanent_transfer_source_dispatch_recovery(request, &stored) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let response = wire::TeamPermanentTransferSourceDispatchResponse::from_operations(
        request,
        local.dispatch(),
        disposition,
        local.original_source_record(),
    )
    .ok_or(ApiError::SourceBindingUnconfirmed)?;
    Ok(response)
}

fn configured_source(
    state: &BackendState,
    scope: &WorkspaceQueryScope,
) -> Result<
    (
        forge_control::ResolvedWorkspaceScope,
        ConfiguredHomeTransferSource,
    ),
    ApiError,
> {
    state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?
        .home_transfer_source(scope)
        .map_err(authority_error)
}

fn validate_recovery_route(
    dispatch: &TeamPermanentTransferSourceDispatchReceipt,
    read_scope: &WorkspaceQueryScope,
    source: &ConfiguredHomeTransferSource,
) -> Result<(), ApiError> {
    let original = dispatch.source_scope();
    let service = source.service();
    if dispatch.source_route() != source.client().authority()
        || dispatch.backend_id() != service.backend_id
        || dispatch.issuer_id() != service.issuer
        || dispatch.service_destination_id() != service.destination_id
        || original.workspace_id() != read_scope.workspace_id
        || original.location_id() != read_scope.location_id
        || original.location_revision() != read_scope.location_revision
        || original.locator() != read_scope.locator
        || read_scope.workspace_revision < original.workspace_revision()
    {
        return Err(ApiError::WorkspaceScopeUnavailable);
    }
    // Only the trusted outer Workspace revision may advance for historical Get.
    Ok(())
}

async fn source_get(
    source: &ConfiguredHomeTransferSource,
    scope: forge_control::ResolvedWorkspaceScope,
    request: &forge_domain::HomeTransferSourceRequest,
) -> Result<Option<HomeTransferSourceRecord>, ApiError> {
    let command = HomeTransferSourceCommand {
        scope,
        service: source.service(),
        operation: HomeTransferSourceOperation::Get {
            preparation_id: request.witness().preparation_id(),
            request_digest: request.request_digest(),
        },
    };
    let prepared = source
        .client()
        .prepare_home_transfer_source_command(&command, Uuid::now_v7())
        .map_err(|error| source_error(&error))?;
    match source
        .client()
        .send_prepared_home_transfer_source(&prepared)
        .await
        .map_err(|error| source_error(&error))?
    {
        HomeTransferSourceResponse::Record(record) => {
            if record.as_ref().is_some_and(|r| r.request() != request) {
                return Err(ApiError::SourceBindingUnconfirmed);
            }
            Ok(record)
        }
        HomeTransferSourceResponse::Outcome(_)
        | HomeTransferSourceResponse::ReservationRetirementOutcome(_)
        | HomeTransferSourceResponse::ReservationRetirementRecord(_) => {
            Err(ApiError::SourceBindingUnconfirmed)
        }
    }
}

async fn ingest(
    state: &BackendState,
    dispatch: &TeamPermanentTransferSourceDispatchReceipt,
    record: HomeTransferSourceRecord,
) -> Result<(), ApiError> {
    state
        .runtime
        .ensure_broker_owner()
        .await
        .map_err(RuntimeError::BrokerOwner)
        .map_err(ApiError::Runtime)?;
    let command = TeamPermanentTransferSourceReceiptCommand::new(
        dispatch.workspace_id(),
        dispatch.preparation_id(),
        dispatch.envelope_digest(),
        now()?,
        record,
    )
    .map_err(RuntimeError::persistence)
    .map_err(ApiError::Runtime)?;
    let _outcome = state
        .catalog_job(move |runtime| runtime.record_team_permanent_transfer_source_receipt(&command))
        .await?;
    Ok(())
}

fn operations_scope(scope: &WorkspaceQueryScope) -> Result<WorkspaceProjectScope, ApiError> {
    WorkspaceProjectScope::new(
        scope.workspace_id,
        scope.location_id,
        scope.location_revision,
        scope.workspace_revision,
        scope.locator.clone(),
    )
    .map_err(RuntimeError::persistence)
    .map_err(ApiError::Runtime)
}
fn now() -> Result<TimestampMillis, ApiError> {
    TimestampMillis::new(crate::current_timestamp_millis()?).map_err(|_| ApiError::InvalidIdentity)
}
fn authority_error(error: CoordinatorAuthorityError) -> ApiError {
    match error {
        CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
        CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
        CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
    }
}
fn source_error(error: &ForgeHttpHomeTransferSourceError) -> ApiError {
    if matches!(error.cause(), forge_backend::ForgeHttpClientError::Capacity) {
        ApiError::Capacity
    } else if error.is_unknown_outcome() {
        ApiError::SourceBindingUnconfirmed
    } else {
        ApiError::WorkspaceScopeUnavailable
    }
}
