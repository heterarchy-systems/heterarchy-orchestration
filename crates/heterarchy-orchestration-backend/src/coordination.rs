//! Owned bounded composition of logical Activity and Forge Coordinator observations.

use crate::wire::coordination::{CoordinationQueryRequest, CoordinationQueryResponse};
use crate::{
    ApiError, BackendState, RuntimeError, membership::source_error, wire::WorkspaceQueryScope,
};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
mod projection;

/// Produce the existing full native projection from canonical snapshots.
///
/// # Errors
/// Rejects an unrepresentable or invalid projection rather than omitting fields.
pub fn project_snapshot(
    logical: &heterarchy_orchestration_operations::LogicalActivitySnapshot,
    coordinator: Option<&forge_domain::ActivityCoordinatorBinding>,
) -> Result<crate::wire::coordination::CoordinationQuerySnapshot, RuntimeError> {
    let value = projection::snapshot_json(logical, coordinator)?;
    serde_json::from_value(value).map_err(|_| RuntimeError::InvalidInput)
}

pub(crate) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/workspaces/{workspace_id}/activities/{activity_id}/coordination/query",
        post(query),
    )
}

async fn query(
    State(state): State<BackendState>,
    Path((workspace, activity)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<CoordinationQueryRequest>,
) -> Result<Json<CoordinationQueryResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, body.session_id)?;
    if crate::canonical_uuid(&workspace)? != body.scope.workspace_id
        || crate::canonical_uuid(&activity)? != body.activity_id
        || body.activity_id.is_nil()
    {
        return Err(ApiError::InvalidIdentity);
    }
    let owner = state.clone();
    state
        .run_owned(async move {
            let (logical, coordinator, catch_up) = owner
                .coordination_snapshot(
                    body.scope.clone(),
                    body.activity_id,
                    body.session_id,
                    principal,
                )
                .await?;
            let snapshot =
                project_snapshot(&logical, coordinator.as_ref()).map_err(ApiError::Runtime)?;
            Ok(Json(CoordinationQueryResponse {
                request: body,
                snapshot,
                catch_up,
            }))
        })
        .await
}
use forge_domain::AuthenticatedPrincipal;

impl BackendState {
    pub(crate) async fn coordination_snapshot(
        &self,
        scope: WorkspaceQueryScope,
        activity_id: uuid::Uuid,
        session_id: Option<uuid::Uuid>,
        principal: AuthenticatedPrincipal,
    ) -> Result<
        (
            heterarchy_orchestration_operations::LogicalActivitySnapshot,
            Option<forge_domain::ActivityCoordinatorBinding>,
            Option<crate::wire::coordination::CoordinationCatchUpWire>,
        ),
        ApiError,
    > {
        let workspace_id = scope.workspace_id;
        let cursor = self
            .catalog_job(move |runtime| {
                runtime.coordination_source_cursor(workspace_id, session_id, principal)
            })
            .await?;
        self.workspace_scope_proof(scope.clone()).await?;
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::WorkspaceScopeUnavailable)?;
        let catch_up = if let Some(cursor) = cursor {
            let page = authority
                .coordination_effect_page(scope.clone(), cursor, self.member_source_slot().await?)
                .await
                .map_err(source_error)?;
            let page_size = u32::try_from(page.entries.len()).map_err(|_| ApiError::Unavailable)?;
            let progress = self
                .catalog_job(move |runtime| {
                    runtime.require_workspace_read(workspace_id, session_id, principal)?;
                    let current = runtime
                        .coordination_source_cursor(workspace_id, session_id, principal)?
                        .ok_or(RuntimeError::SourceAuthorityUnavailable)?;
                    if current.migration_id() != cursor.migration_id()
                        || current.destination_id() != cursor.destination_id()
                    {
                        return Err(RuntimeError::Conflict);
                    }
                    crate::effect_fact_http::apply_runtime_page(runtime, &page)
                })
                .await?;
            Some(crate::wire::coordination::CoordinationCatchUpWire {
                migration_id: cursor.migration_id(),
                destination_id: cursor.destination_id(),
                requested_after: cursor.last_sequence(),
                applied_through: progress.last_sequence(),
                page_size,
                page_limit: 64,
                more_may_remain: page_size == 64,
            })
        } else {
            None
        };
        let coordinator = authority
            .coordinator_observation(scope.clone(), activity_id, self.member_source_slot().await?)
            .await
            .map_err(source_error)?;
        self.workspace_scope_proof(scope).await?;
        let snapshot = self
            .catalog_job(move |runtime| {
                runtime.coordination_snapshot(workspace_id, activity_id, session_id, principal)
            })
            .await?;
        Ok((snapshot, coordinator, catch_up))
    }
}
