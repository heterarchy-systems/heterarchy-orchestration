//! Operator-only HTTP adapter for the Forge operations import.
//!
//! Source reads happen before the runtime owner is entered. Every catalog
//! mutation receives the complete progress snapshot captured before the source
//! request, so a lost reply or concurrent progress cannot advance another page.

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::ImportProgress;
use heterarchy_orchestration_runtime::RuntimeError;
use uuid::Uuid;

use crate::app_support::validate_workspace_reference;
use crate::wire::operations_import::{
    ImportPrepareRequest, ImportProgressResponse, ImportProgressWire, ImportStepRequest,
};
use crate::{ApiError, BackendState, CoordinatorAuthorityError};

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route("/internal/v1/operations/imports/prepare", post(prepare))
        .route(
            "/internal/v1/operations/imports/{migration_id}/import-next-page",
            post(import_next_page),
        )
        .route(
            "/internal/v1/operations/imports/{migration_id}/verify-next-page",
            post(verify_next_page),
        )
        .route(
            "/internal/v1/operations/imports/{migration_id}/progress",
            get(progress),
        )
        .route(
            "/internal/v1/operations/imports/{migration_id}/abort",
            post(abort),
        )
}

async fn prepare(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ImportPrepareRequest>,
) -> Result<Json<ImportProgressWire>, ApiError> {
    let migration = require_migration(body.migration_id)?;
    require_operator(&state, principal).await?;
    validate_route_scope(&state, &body.scope, body.destination_id)?;
    let authority = state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let permit = state
        .forge_query_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    let freeze = authority
        .agent_operations_freeze_read(body.scope.clone(), migration, body.destination_id, permit)
        .await
        .map_err(map_authority_error)?
        .ok_or(ApiError::NotFound)?;
    if freeze.migration_id() != migration
        || freeze.workspace_id().into_uuid() != body.scope.workspace_id
        || freeze.destination_id() != body.destination_id
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let progress = state
        .catalog_job(move |runtime| runtime.import_prepare(&freeze))
        .await?;
    Ok(Json(ImportProgressWire::from(&progress)))
}

async fn import_next_page(
    State(state): State<BackendState>,
    Path(migration): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ImportStepRequest>,
) -> Result<Json<ImportProgressWire>, ApiError> {
    let migration = require_migration_text(&migration)?;
    require_operator(&state, principal).await?;
    validate_route_scope(&state, &body.scope, body.destination_id)?;
    validate_expected_counts(body.expected_pages, body.expected_verified_pages)?;
    let captured = read_expected_progress(&state, migration, &body).await?;
    let cursor = captured
        .next_cursor()
        .ok_or(ApiError::Runtime(RuntimeError::Conflict))?;
    let authority = state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let permit = state
        .forge_query_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    let page = authority
        .agent_operations_export(
            body.scope.clone(),
            migration,
            body.destination_id,
            cursor,
            permit,
        )
        .await
        .map_err(map_authority_error)?;
    let progress = state
        .catalog_job(move |runtime| runtime.import_page_expected(&page, &captured))
        .await?;
    Ok(Json(ImportProgressWire::from(&progress)))
}

async fn verify_next_page(
    State(state): State<BackendState>,
    Path(migration): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ImportStepRequest>,
) -> Result<Json<ImportProgressWire>, ApiError> {
    let migration = require_migration_text(&migration)?;
    require_operator(&state, principal).await?;
    validate_route_scope(&state, &body.scope, body.destination_id)?;
    validate_expected_counts(body.expected_pages, body.expected_verified_pages)?;
    let target = state
        .catalog_job(move |runtime| runtime.import_verification_target(migration))
        .await?;
    validate_progress_fence(target.progress(), &body, migration)?;
    let Some(cursor) = target.requested_cursor() else {
        return Ok(Json(ImportProgressWire::from(target.progress())));
    };
    let expected = target.progress().clone();
    let authority = state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let permit = state
        .forge_query_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    let page = authority
        .agent_operations_export(
            body.scope.clone(),
            migration,
            body.destination_id,
            cursor,
            permit,
        )
        .await
        .map_err(map_authority_error)?;
    let progress = state
        .catalog_job(move |runtime| runtime.import_verify_page_expected(&page, &expected))
        .await?;
    Ok(Json(ImportProgressWire::from(&progress)))
}

async fn progress(
    State(state): State<BackendState>,
    Path(migration): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ImportProgressResponse>, ApiError> {
    let migration = require_migration_text(&migration)?;
    require_operator(&state, principal).await?;
    let progress = state
        .catalog_job(move |runtime| runtime.import_progress(migration))
        .await?;
    Ok(Json(ImportProgressResponse {
        progress: progress.as_ref().map(ImportProgressWire::from),
    }))
}

async fn abort(
    State(state): State<BackendState>,
    Path(migration): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ImportStepRequest>,
) -> Result<Json<ImportProgressWire>, ApiError> {
    let migration = require_migration_text(&migration)?;
    require_operator(&state, principal).await?;
    validate_route_scope(&state, &body.scope, body.destination_id)?;
    validate_expected_counts(body.expected_pages, body.expected_verified_pages)?;
    let captured = read_expected_progress(&state, migration, &body).await?;
    let progress = state
        .catalog_job(move |runtime| runtime.import_abort_expected(migration, &captured))
        .await?;
    Ok(Json(ImportProgressWire::from(&progress)))
}

async fn require_operator(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
) -> Result<(), ApiError> {
    state
        .catalog_job(move |runtime| runtime.require_operator(principal))
        .await
}

fn validate_route_scope(
    state: &BackendState,
    scope: &crate::wire::WorkspaceQueryScope,
    destination_id: Uuid,
) -> Result<(), ApiError> {
    validate_workspace_reference(scope, None)?;
    if destination_id.is_nil() || destination_id != state.runtime.destination_id() {
        return Err(ApiError::InvalidIdentity);
    }
    state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?
        .validate_scope(scope)
        .map_err(map_authority_error)
}

fn validate_expected_counts(
    expected_pages: u64,
    expected_verified_pages: u64,
) -> Result<(), ApiError> {
    if expected_pages > i64::MAX as u64
        || expected_verified_pages > i64::MAX as u64
        || expected_verified_pages > expected_pages
    {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }
    Ok(())
}

fn validate_progress_fence(
    progress: &ImportProgress,
    body: &ImportStepRequest,
    migration: Uuid,
) -> Result<(), ApiError> {
    if progress.migration_id() != migration
        || progress.workspace_id().into_uuid() != body.scope.workspace_id
        || progress.destination_id() != body.destination_id
        || progress.page_count() != body.expected_pages
        || progress.verified_pages() != body.expected_verified_pages
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(())
}

async fn read_expected_progress(
    state: &BackendState,
    migration: Uuid,
    body: &ImportStepRequest,
) -> Result<ImportProgress, ApiError> {
    let progress = state
        .catalog_job(move |runtime| runtime.import_progress(migration))
        .await?
        .ok_or(ApiError::NotFound)?;
    validate_progress_fence(&progress, body, migration)?;
    Ok(progress)
}

fn require_migration(value: Uuid) -> Result<Uuid, ApiError> {
    if value.is_nil() {
        Err(ApiError::InvalidIdentity)
    } else {
        Ok(value)
    }
}

fn require_migration_text(value: &str) -> Result<Uuid, ApiError> {
    crate::canonical_uuid(value)
}

fn map_authority_error(error: CoordinatorAuthorityError) -> ApiError {
    match error {
        CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
        CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
        CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{AuthenticatedPrincipal, DirectTokenId};
    use heterarchy_orchestration_runtime::OrchestrationRuntime;

    fn scope() -> crate::wire::WorkspaceQueryScope {
        crate::wire::WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 1,
            workspace_revision: 1,
            locator: "workspace".to_owned(),
        }
    }

    #[test]
    fn expected_verified_pages_cannot_exceed_expected_pages() {
        assert!(validate_expected_counts(0, 1).is_err());
        assert!(validate_expected_counts(1, 1).is_ok());
    }

    #[tokio::test]
    async fn prepare_rejects_nonoperator_before_source_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("operations-import-route-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let destination = runtime.destination_id();
        let state = BackendState::new(
            runtime,
            std::sync::Arc::new(crate::realtime::RealtimeHub::new()),
        );
        let result = prepare(
            State(state.clone()),
            Extension(AuthenticatedPrincipal::DirectToken(
                DirectTokenId::from_uuid(Uuid::from_u128(3)),
            )),
            Json(ImportPrepareRequest {
                scope: scope(),
                migration_id: Uuid::from_u128(4),
                destination_id: destination,
            }),
        )
        .await;
        assert!(matches!(
            result,
            Err(ApiError::Runtime(RuntimeError::Forbidden))
        ));
        state.stop_admission();
        state.drain().await;
        drop(state);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
