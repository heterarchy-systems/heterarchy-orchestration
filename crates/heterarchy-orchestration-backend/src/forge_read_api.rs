//! Application read operations over the configured Forge HTTP source.
//!
//! These routes expose the bounded Location/Workspace catalog source reads
//! to authenticated consumers. Every handler re-proves authorization through
//! the shared bearer layer and (for Workspace-scoped reads) the catalog
//! membership guard plus the exact source-scope proof before issuing one
//! bounded source observation. Responses are observations only: no read
//! result grants mutation rights, lease ownership, actor identity, or scope
//! membership, and no read falls back to another transport when the
//! configured HTTP source is absent.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Extension, Router};
use forge_control::WorkspaceLocator;
use forge_domain::{LocationId, LocationRevision};
use heterarchy_orchestration_runtime::RuntimeError;
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

use crate::ApiError;

mod coordinator;

use crate::BackendState;
use crate::forge_source::ForgeSourceError;
use crate::forge_source::forge_reads::file::{
    ForgeFileEntryKind, ForgeSourceEol, ForgeSourceScope,
};
use crate::forge_source::forge_reads::goal_dag::{
    ForgeDagCurrent, ForgeDagLifecycle, ForgeDagNodeStatus, ForgeGoal, ForgeGoalStatusKind,
    ForgeVerificationKind,
};
use crate::forge_source::forge_reads::leases::{
    ForgeDiffArtifactHealth, ForgeDiffChangeKind, ForgeDiffEntrySubject, ForgeDiffEvidence,
    ForgeDiffEvidenceState, ForgeDiffOrigin, ForgeDiffRead, ForgeDiffRenderKind,
    ForgeProjectCommandClass, ForgeWriterLeaseProjection, ForgeWriterLeaseScopeKind,
    ForgeWriterLeaseState, ForgeWriterLeaseStatus,
};
use crate::forge_source::forge_reads::workspace::{
    ForgeDiscoveryDisposition, ForgeDiscoveryKind, ForgeDiscoveryReason, ForgeDiscoverySignal,
    ForgeLocationRow, ForgeLocationState, ForgeWorkspaceCatalogRow, ForgeWorkspaceDiscoveryRow,
};
use crate::wire::forge_reads::{
    CommandListQueryRequest, CommandListQueryResponse, DagCurrentQueryRequest,
    DagCurrentQueryResponse, DagCurrentWire, DagEdgeWire, DagLifecycleWire, DagNodeWire,
    DagStatusWire, DiffArtifactHealthWire, DiffChangeKindWire, DiffDoctorQueryRequest,
    DiffDoctorQueryResponse, DiffEntrySubjectWire, DiffEntryWire, DiffEvidenceStateWire,
    DiffEvidenceWire, DiffListQueryRequest, DiffListQueryResponse, DiffOriginWire,
    DiffReadQueryRequest, DiffReadQueryResponse, DiffRenderKindWire, GoalGetQueryRequest,
    GoalGetQueryResponse, GoalListQueryRequest, GoalListQueryResponse, GoalStatusKindWire,
    GoalStatusWire, GoalVerificationKindWire, GoalVerificationWire, GoalWire,
    ProjectCommandClassWire, ProjectCommandWire, WriterLeaseGetQueryRequest,
    WriterLeaseGetQueryResponse, WriterLeaseListQueryRequest, WriterLeaseListQueryResponse,
    WriterLeaseProjectionWire, WriterLeaseScopeKindWire, WriterLeaseScopeWire,
    WriterLeaseStateWire, WriterLeaseStatusWire,
};
use crate::wire::forge_reads::{
    DiscoveryDispositionWire, DiscoveryKindWire, DiscoveryReasonWire, DiscoverySignalWire,
    DiscoverySignalsWire, FileEntryKindWire, FileEntryWire, FileListQueryRequest,
    FileListQueryResponse, FileReadSliceQueryRequest, FileReadSliceQueryResponse,
    LocationListRequest, LocationListResponse, LocationRowWire, LocationStateWire, SourceEolWire,
    SourceScopeWire, TextSearchMatchWire, TextSearchQueryRequest, TextSearchQueryResponse,
    WorkspaceCatalogEntryWire, WorkspaceDiscoverRequest, WorkspaceDiscoverResponse,
    WorkspaceDiscoveryEntryWire, WorkspaceListRequest, WorkspaceListResponse,
    WorkspaceResolveRequest, WorkspaceResolveResponse,
};

/// Forge source read routes; merge before the router takes its state.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .merge(coordinator::routes())
        .route("/internal/v1/forge/location-list", post(location_list))
        .route("/internal/v1/forge/workspace-list", post(workspace_list))
        .route(
            "/internal/v1/forge/workspace-discover",
            post(workspace_discover),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/workspace-resolve",
            post(workspace_resolve),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/file-list",
            post(file_list),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/text-search",
            post(text_search),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/file-read-slice",
            post(file_read_slice),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/goal-list",
            post(goal_list),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/goal-get",
            post(goal_get),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/dag-get-current",
            post(dag_get_current),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/command-list",
            post(command_list),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/diff-read",
            post(diff_read),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/writer-lease-get",
            post(writer_lease_get),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/writer-lease-list",
            post(writer_lease_list),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/diff-list",
            post(diff_list),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/forge/diff-doctor",
            post(diff_doctor),
        )
}

async fn location_list(
    State(state): State<BackendState>,
    Extension(_): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(LocationListRequest {}): Json<LocationListRequest>,
) -> Result<Json<LocationListResponse>, ApiError> {
    let (_permit, client) = forge_source(&state).await?;
    let rows = client.location_list().await.map_err(map_source_error)?;
    Ok(Json(LocationListResponse {
        entries: rows.iter().map(location_wire).collect(),
    }))
}

async fn workspace_list(
    State(state): State<BackendState>,
    Extension(_): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceListRequest>,
) -> Result<Json<WorkspaceListResponse>, ApiError> {
    let location = location_identity(body.location_id, body.expected_revision)?;
    let (_permit, client) = forge_source(&state).await?;
    let rows = client
        .workspace_list(location.0, location.1)
        .await
        .map_err(map_source_error)?;
    Ok(Json(WorkspaceListResponse {
        entries: rows.iter().map(catalog_entry_wire).collect(),
    }))
}

async fn workspace_discover(
    State(state): State<BackendState>,
    Extension(_): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceDiscoverRequest>,
) -> Result<Json<WorkspaceDiscoverResponse>, ApiError> {
    let location = location_identity(body.location_id, body.expected_revision)?;
    let (_permit, client) = forge_source(&state).await?;
    let rows = client
        .workspace_discover(location.0, location.1)
        .await
        .map_err(map_source_error)?;
    Ok(Json(WorkspaceDiscoverResponse {
        entries: rows.iter().map(discovery_entry_wire).collect(),
    }))
}

async fn workspace_resolve(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceResolveRequest>,
) -> Result<Json<WorkspaceResolveResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, body.session_id)?;
    if crate::canonical_uuid(&workspace_id)? != body.scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    let scope = body.scope.clone();
    let workspace = scope.workspace_id;
    let session = body.session_id;
    state
        .catalog_job(move |runtime| runtime.require_workspace_read(workspace, session, principal))
        .await?;
    state.workspace_scope_proof(scope.clone()).await?;

    let locator =
        WorkspaceLocator::decode(&scope.locator).map_err(|_| ApiError::InvalidIdentity)?;
    if locator.encode() != scope.locator {
        return Err(ApiError::InvalidIdentity);
    }
    let relative_root = locator
        .relative_root()
        .to_str()
        .ok_or(ApiError::InvalidIdentity)?
        .to_owned();
    let (_permit, client) = forge_source(&state).await?;
    let resolution = client
        .resolve_workspace_identity(
            LocationId::from_uuid(scope.location_id),
            LocationRevision::new(scope.location_revision)
                .map_err(|_| ApiError::InvalidIdentity)?,
            &relative_root,
        )
        .await
        .map_err(map_source_error)?;
    if resolution.workspace_id() != scope.workspace_id
        || resolution.workspace_revision() != scope.workspace_revision
    {
        // The caller's reference disagrees with the fresh source resolution:
        // a stale observation, carried as the typed conflict so callers can
        // re-read and resynchronize.
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Json(WorkspaceResolveResponse {
        workspace_id: resolution.workspace_id(),
        workspace_revision: resolution.workspace_revision(),
        locator: scope.locator,
    }))
}

/// Validated per-read context for one confined file-family observation.
struct FileReadContext {
    scope: forge_backend::ops::ScopeDto,
    activity: forge_control::FileActivityScope,
}

/// Run the shared authorization prologue for one file-family read: syntax
/// validation, catalog membership, the exact source-scope proof, and the
/// live-Activity proof. None of these steps trusts caller routing fields as
/// permission.
async fn file_read_context(
    state: &BackendState,
    workspace_id: &str,
    scope: &crate::wire::WorkspaceQueryScope,
    session_id: Option<Uuid>,
    principal: forge_domain::AuthenticatedPrincipal,
    activity_id: Uuid,
) -> Result<FileReadContext, ApiError> {
    use crate::forge_source::forge_reads::file::{activity_scope, scope_dto};
    use forge_control::WorkspaceLocator;

    crate::app_support::validate_workspace_reference(scope, session_id)?;
    if crate::canonical_uuid(workspace_id)? != scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    let owned = scope.clone();
    let workspace = scope.workspace_id;
    state
        .catalog_job(move |runtime| {
            runtime.require_workspace_read(workspace, session_id, principal)
        })
        .await?;
    state.workspace_scope_proof(owned.clone()).await?;
    state.activity_scope_proof(owned, activity_id).await?;

    let locator =
        WorkspaceLocator::decode(&scope.locator).map_err(|_| ApiError::InvalidIdentity)?;
    if locator.encode() != scope.locator {
        return Err(ApiError::InvalidIdentity);
    }
    let relative_root = locator
        .relative_root()
        .to_str()
        .ok_or(ApiError::InvalidIdentity)?
        .to_owned();
    let scope_dto = scope_dto(
        scope.workspace_id,
        scope.workspace_revision,
        scope.location_id,
        scope.location_revision,
        &relative_root,
    )
    .map_err(map_source_error)?;
    let observed_at = crate::current_timestamp_millis()?;
    let activity = activity_scope(activity_id, observed_at).map_err(map_source_error)?;
    Ok(FileReadContext {
        scope: scope_dto,
        activity,
    })
}

async fn file_list(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<FileListQueryRequest>,
) -> Result<Json<FileListQueryResponse>, ApiError> {
    const MAX_FILE_LIST_RESULTS: u32 = 512;
    if body.max_results == 0
        || body.max_results > MAX_FILE_LIST_RESULTS
        || body.max_output_bytes == 0
        || body.max_output_bytes > MAX_FILE_OUTPUT_BYTES
    {
        return Err(ApiError::InvalidIdentity);
    }
    let context = file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    if let Some(token) = body.directory.as_deref() {
        let _ = crate::forge_source::forge_reads::file::decode_path_token(token)
            .map_err(|_| ApiError::InvalidIdentity)?;
    }
    let options = forge_domain::FileListOptions::new(
        usize::try_from(body.max_results).map_err(|_| ApiError::InvalidIdentity)?,
        usize::try_from(body.max_output_bytes).map_err(|_| ApiError::InvalidIdentity)?,
    )
    .map_err(|_| ApiError::InvalidIdentity)?;
    let (_permit, client) = forge_source(&state).await?;
    let listing = client
        .file_list(
            &context.scope,
            &context.activity,
            body.directory.as_deref(),
            &options,
        )
        .await
        .map_err(map_source_error)?;
    Ok(Json(FileListQueryResponse {
        scope: scope_wire(listing.scope()),
        activity_id: listing.activity_id().to_owned(),
        directory: listing.directory_token().map(str::to_owned),
        directory_display_path: listing.directory_display_path().map(str::to_owned),
        directory_display_path_truncated: listing.directory_display_path_truncated(),
        entries: listing
            .entries()
            .iter()
            .map(|entry| FileEntryWire {
                path: entry.path_token().to_owned(),
                display_path: entry.display_path().to_owned(),
                display_path_truncated: entry.display_path_truncated(),
                kind: match entry.kind() {
                    ForgeFileEntryKind::Directory => FileEntryKindWire::Directory,
                    ForgeFileEntryKind::RegularFile => FileEntryKindWire::RegularFile,
                    ForgeFileEntryKind::SymbolicLink => FileEntryKindWire::SymbolicLink,
                    ForgeFileEntryKind::Other => FileEntryKindWire::Other,
                },
                size_bytes: entry.size_bytes(),
            })
            .collect(),
        scanned_entries: listing.scanned_entries(),
        truncated: listing.truncated(),
    }))
}

async fn text_search(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<TextSearchQueryRequest>,
) -> Result<Json<TextSearchQueryResponse>, ApiError> {
    const MAX_TEXT_SEARCH_RESULTS: u32 = 256;
    if body.max_results == 0
        || body.max_results > MAX_TEXT_SEARCH_RESULTS
        || body.max_output_bytes == 0
        || body.max_output_bytes > MAX_FILE_OUTPUT_BYTES
    {
        return Err(ApiError::InvalidIdentity);
    }
    let context = file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    if let Some(token) = body.root.as_deref() {
        let _ = crate::forge_source::forge_reads::file::decode_path_token(token)
            .map_err(|_| ApiError::InvalidIdentity)?;
    }
    let query = forge_domain::TextSearchQuery::new(body.query.clone())
        .map_err(|_| ApiError::InvalidIdentity)?;
    let options = forge_domain::TextSearchOptions::new(
        usize::try_from(body.max_results).map_err(|_| ApiError::InvalidIdentity)?,
        usize::try_from(body.max_output_bytes).map_err(|_| ApiError::InvalidIdentity)?,
    )
    .map_err(|_| ApiError::InvalidIdentity)?;
    let (_permit, client) = forge_source(&state).await?;
    let result = client
        .text_search(
            &context.scope,
            &context.activity,
            body.root.as_deref(),
            &query,
            &options,
        )
        .await
        .map_err(map_source_error)?;
    Ok(Json(TextSearchQueryResponse {
        scope: scope_wire(result.scope()),
        activity_id: result.activity_id().to_owned(),
        root: result.root_token().map(str::to_owned),
        root_display_path: result.root_display_path().map(str::to_owned),
        root_display_path_truncated: result.root_display_path_truncated(),
        matches: result
            .matches()
            .iter()
            .map(|match_row| TextSearchMatchWire {
                path: match_row.path_token().to_owned(),
                display_path: match_row.display_path().to_owned(),
                display_path_truncated: match_row.display_path_truncated(),
                line: match_row.line(),
                preview: match_row.preview().to_owned(),
            })
            .collect(),
        scanned_entries: result.scanned_entries(),
        scanned_files: result.scanned_files(),
        truncated: result.truncated(),
    }))
}

async fn file_read_slice(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<FileReadSliceQueryRequest>,
) -> Result<Json<FileReadSliceQueryResponse>, ApiError> {
    if body.start_line == 0 || body.end_line < body.start_line {
        return Err(ApiError::InvalidIdentity);
    }
    let context = file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    let line_range = forge_domain::SourceLineRange::new(body.start_line, body.end_line)
        .map_err(|_| ApiError::InvalidIdentity)?;
    let (_permit, client) = forge_source(&state).await?;
    let slice = client
        .file_read_slice(&context.scope, &context.activity, &body.path, line_range)
        .await
        .map_err(map_source_error)?;
    Ok(Json(FileReadSliceQueryResponse {
        scope: scope_wire(slice.scope()),
        activity_id: slice.activity_id().to_owned(),
        path: slice.path_token().to_owned(),
        display_path: slice.display_path().to_owned(),
        display_path_truncated: slice.display_path_truncated(),
        content: slice.content().to_owned(),
        whole_file_hash: slice.whole_file_hash().to_owned(),
        range_hash: slice.range_hash().to_owned(),
        line_hashes: slice.line_hashes().to_vec(),
        start_line: slice.start_line(),
        end_line: slice.end_line(),
        total_lines: slice.total_lines(),
        eol: match slice.eol() {
            ForgeSourceEol::Lf => SourceEolWire::Lf,
            ForgeSourceEol::CrLf => SourceEolWire::CrLf,
            ForgeSourceEol::Cr => SourceEolWire::Cr,
            ForgeSourceEol::Mixed => SourceEolWire::Mixed,
            ForgeSourceEol::None => SourceEolWire::None,
        },
        has_final_newline: slice.has_final_newline(),
    }))
}

const MAX_GOAL_LIST_RESULTS: u32 = 100;

async fn goal_list(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<GoalListQueryRequest>,
) -> Result<Json<GoalListQueryResponse>, ApiError> {
    if body.limit == 0 || body.limit > MAX_GOAL_LIST_RESULTS {
        return Err(ApiError::InvalidIdentity);
    }
    let context = file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    let limit =
        forge_domain::GoalListLimit::new(body.limit).map_err(|_| ApiError::InvalidIdentity)?;
    let (_permit, client) = forge_source(&state).await?;
    let goals = client
        .goal_list(&context.scope, body.activity_id, limit)
        .await
        .map_err(map_source_error)?;
    Ok(Json(GoalListQueryResponse {
        goals: goals.iter().map(goal_wire).collect(),
    }))
}

async fn goal_get(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<GoalGetQueryRequest>,
) -> Result<Json<GoalGetQueryResponse>, ApiError> {
    let context = file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    let (_permit, client) = forge_source(&state).await?;
    let goal = client
        .goal_get(&context.scope, body.activity_id, body.goal_id)
        .await
        .map_err(map_source_error)?;
    Ok(Json(GoalGetQueryResponse {
        goal: goal.as_ref().map(goal_wire),
    }))
}

async fn dag_get_current(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<DagCurrentQueryRequest>,
) -> Result<Json<DagCurrentQueryResponse>, ApiError> {
    let context = file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    let (_permit, client) = forge_source(&state).await?;
    let dag = client
        .dag_get_current(&context.scope, body.goal_id)
        .await
        .map_err(map_source_error)?;
    Ok(Json(DagCurrentQueryResponse {
        dag: dag.as_ref().map(dag_wire),
    }))
}

fn goal_wire(goal: &ForgeGoal) -> GoalWire {
    GoalWire {
        goal_id: goal.goal_id().to_owned(),
        workspace_id: goal.workspace_id().to_owned(),
        revision: goal.revision(),
        status: GoalStatusWire {
            kind: match goal.status().kind() {
                ForgeGoalStatusKind::Active => GoalStatusKindWire::Active,
                ForgeGoalStatusKind::Paused => GoalStatusKindWire::Paused,
                ForgeGoalStatusKind::Blocked => GoalStatusKindWire::Blocked,
                ForgeGoalStatusKind::Completed => GoalStatusKindWire::Completed,
                ForgeGoalStatusKind::Failed => GoalStatusKindWire::Failed,
                ForgeGoalStatusKind::Cancelled => GoalStatusKindWire::Cancelled,
            },
            reason: goal.status().reason().map(str::to_owned),
        },
        objective: goal.objective().to_owned(),
        success_criteria: goal.success_criteria().to_vec(),
        constraints: goal.constraints().to_vec(),
        out_of_scope: goal.out_of_scope().to_vec(),
        verification_requirements: goal
            .verification_requirements()
            .iter()
            .map(|requirement| GoalVerificationWire {
                kind: match requirement.kind() {
                    ForgeVerificationKind::ExecutionSucceeded => {
                        GoalVerificationKindWire::ExecutionSucceeded
                    }
                    ForgeVerificationKind::FilePresent => GoalVerificationKindWire::FilePresent,
                    ForgeVerificationKind::ChangeSetSealed => {
                        GoalVerificationKindWire::ChangeSetSealed
                    }
                    ForgeVerificationKind::CheckpointReady => {
                        GoalVerificationKindWire::CheckpointReady
                    }
                },
                description: requirement.description().to_owned(),
            })
            .collect(),
        stop_conditions: goal.stop_conditions().to_vec(),
        created_at_ms: goal.created_at_ms(),
        updated_at_ms: goal.updated_at_ms(),
    }
}

fn dag_wire(dag: &ForgeDagCurrent) -> DagCurrentWire {
    DagCurrentWire {
        goal_id: dag.goal_id().to_owned(),
        revision_id: dag.revision_id().to_owned(),
        base_goal_revision: dag.base_goal_revision(),
        dag_revision: dag.dag_revision(),
        parent_revision: dag.parent_revision(),
        created_at_ms: dag.created_at_ms(),
        head_revision: dag.head_revision(),
        head_updated_at_ms: dag.head_updated_at_ms(),
        nodes: dag
            .nodes()
            .iter()
            .map(|node| DagNodeWire {
                node_id: node.node_id().to_owned(),
                ordinal: node.ordinal(),
                title: node.title().to_owned(),
                objective: node.objective().to_owned(),
                acceptance: node.acceptance().to_vec(),
                lifecycle: match node.lifecycle() {
                    ForgeDagLifecycle::Pending => DagLifecycleWire::Pending,
                    ForgeDagLifecycle::Active => DagLifecycleWire::Active,
                    ForgeDagLifecycle::Blocked => DagLifecycleWire::Blocked,
                    ForgeDagLifecycle::Completed => DagLifecycleWire::Completed,
                    ForgeDagLifecycle::Failed => DagLifecycleWire::Failed,
                    ForgeDagLifecycle::Skipped => DagLifecycleWire::Skipped,
                    ForgeDagLifecycle::Cancelled => DagLifecycleWire::Cancelled,
                },
                status: match node.status() {
                    ForgeDagNodeStatus::Pending => DagStatusWire::Pending,
                    ForgeDagNodeStatus::Ready => DagStatusWire::Ready,
                    ForgeDagNodeStatus::Active => DagStatusWire::Active,
                    ForgeDagNodeStatus::Blocked => DagStatusWire::Blocked,
                    ForgeDagNodeStatus::Completed => DagStatusWire::Completed,
                    ForgeDagNodeStatus::Failed => DagStatusWire::Failed,
                    ForgeDagNodeStatus::Skipped => DagStatusWire::Skipped,
                    ForgeDagNodeStatus::Cancelled => DagStatusWire::Cancelled,
                },
                waiting_on: node.waiting_on().to_vec(),
                revision: node.revision(),
                evidence_count: node.evidence_count(),
                created_at_ms: node.created_at_ms(),
                updated_at_ms: node.updated_at_ms(),
            })
            .collect(),
        edges: dag
            .edges()
            .iter()
            .map(|edge| DagEdgeWire {
                upstream: edge.upstream().to_owned(),
                downstream: edge.downstream().to_owned(),
            })
            .collect(),
    }
}

const MAX_FILE_OUTPUT_BYTES: u32 = 65_536;

async fn command_list(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<CommandListQueryRequest>,
) -> Result<Json<CommandListQueryResponse>, ApiError> {
    let context = file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    let (_permit, client) = forge_source(&state).await?;
    let observed_at = crate::current_timestamp_millis()?;
    let list = client
        .command_list(
            &context.scope,
            body.activity_id,
            context.activity.runtime_instance_id.into_uuid(),
            observed_at,
        )
        .await
        .map_err(map_source_error)?;
    Ok(Json(CommandListQueryResponse {
        activity_id: list.activity_id().to_owned(),
        catalog_fingerprint: list.catalog_fingerprint().to_owned(),
        commands: list
            .commands()
            .iter()
            .map(|command| ProjectCommandWire {
                command_id: command.command_id().to_owned(),
                class: match command.class() {
                    ForgeProjectCommandClass::Verify => ProjectCommandClassWire::Verify,
                    ForgeProjectCommandClass::GeneratedPackage => {
                        ProjectCommandClassWire::GeneratedPackage
                    }
                },
            })
            .collect(),
    }))
}

async fn diff_read(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(body): Json<DiffReadQueryRequest>,
) -> Result<Json<DiffReadQueryResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, body.session_id)?;
    if crate::canonical_uuid(&workspace_id)? != body.scope.workspace_id || body.diff_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    let session = crate::app_support::session_selector(&headers)?;
    let scope = body.scope.clone();
    let workspace = scope.workspace_id;
    state
        .catalog_job(move |runtime| runtime.require_workspace_read(workspace, session, principal))
        .await?;
    state.workspace_scope_proof(scope.clone()).await?;
    let (_permit, client) = forge_source(&state).await?;
    let read = client
        .diff_read(&scope_dto_from_wire(&scope)?, body.diff_id)
        .await
        .map_err(map_source_error)?;
    Ok(Json(DiffReadQueryResponse {
        scope: scope_wire(read.scope()),
        diff: diff_wire(&read),
        health: match read.health() {
            ForgeDiffArtifactHealth::Available => DiffArtifactHealthWire::Available,
            ForgeDiffArtifactHealth::NotApplicable => DiffArtifactHealthWire::NotApplicable,
            ForgeDiffArtifactHealth::Archived => DiffArtifactHealthWire::Archived,
            ForgeDiffArtifactHealth::Missing => DiffArtifactHealthWire::Missing,
            ForgeDiffArtifactHealth::Corrupt => DiffArtifactHealthWire::Corrupt,
        },
        content: read.content().map(str::to_owned),
    }))
}

async fn writer_lease_get(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(body): Json<WriterLeaseGetQueryRequest>,
) -> Result<Json<WriterLeaseGetQueryResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, body.session_id)?;
    if crate::canonical_uuid(&workspace_id)? != body.scope.workspace_id || body.lease_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    let session = crate::app_support::session_selector(&headers)?;
    let scope = body.scope.clone();
    let workspace = scope.workspace_id;
    state
        .catalog_job(move |runtime| runtime.require_workspace_read(workspace, session, principal))
        .await?;
    state.workspace_scope_proof(scope.clone()).await?;
    let (_permit, client) = forge_source(&state).await?;
    let lease = client
        .writer_lease_get(
            &scope_dto_from_wire(&scope)?,
            body.lease_id,
            body.scope_offset,
            body.expected_revision,
        )
        .await
        .map_err(map_source_error)?;
    Ok(Json(WriterLeaseGetQueryResponse {
        lease: lease.as_ref().map(lease_wire),
    }))
}

async fn writer_lease_list(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(body): Json<WriterLeaseListQueryRequest>,
) -> Result<Json<WriterLeaseListQueryResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, body.session_id)?;
    if crate::canonical_uuid(&workspace_id)? != body.scope.workspace_id || body.limit == 0 {
        return Err(ApiError::InvalidIdentity);
    }
    let session = crate::app_support::session_selector(&headers)?;
    let scope = body.scope.clone();
    let workspace = scope.workspace_id;
    state
        .catalog_job(move |runtime| runtime.require_workspace_read(workspace, session, principal))
        .await?;
    state.workspace_scope_proof(scope.clone()).await?;
    let (_permit, client) = forge_source(&state).await?;
    let (leases, truncated) = client
        .writer_lease_list(&scope_dto_from_wire(&scope)?, body.limit, body.cursor)
        .await
        .map_err(map_source_error)?;
    Ok(Json(WriterLeaseListQueryResponse {
        leases: leases.iter().map(lease_wire).collect(),
        limit: body.limit,
        truncated,
        next_cursor: None,
    }))
}

async fn diff_list(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    Json(body): Json<DiffListQueryRequest>,
) -> Result<Json<DiffListQueryResponse>, ApiError> {
    let context = file_read_context(
        &state,
        &workspace_id,
        &body.scope,
        body.session_id,
        principal,
        body.activity_id,
    )
    .await?;
    let (_permit, client) = forge_source(&state).await?;
    let diffs = client
        .diff_list(&context.scope, body.activity_id, body.limit)
        .await
        .map_err(map_source_error)?;
    Ok(Json(DiffListQueryResponse {
        scope: scope_wire_for_evidence(&body.scope),
        activity_id: body.activity_id.to_string(),
        diffs: diffs.iter().map(diff_evidence_wire).collect(),
    }))
}

async fn diff_doctor(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<forge_domain::AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(body): Json<DiffDoctorQueryRequest>,
) -> Result<Json<DiffDoctorQueryResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, body.session_id)?;
    if crate::canonical_uuid(&workspace_id)? != body.scope.workspace_id || body.diff_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    let session = crate::app_support::session_selector(&headers)?;
    let scope = body.scope.clone();
    let workspace = scope.workspace_id;
    state
        .catalog_job(move |runtime| runtime.require_workspace_read(workspace, session, principal))
        .await?;
    state.workspace_scope_proof(scope.clone()).await?;
    let (_permit, client) = forge_source(&state).await?;
    let doctor = client
        .diff_doctor(&scope_dto_from_wire(&scope)?, body.diff_id)
        .await
        .map_err(map_source_error)?;
    Ok(Json(DiffDoctorQueryResponse {
        scope: scope_wire_for_evidence(&body.scope),
        diff_id: doctor.diff_id().to_owned(),
        health: match doctor.health() {
            ForgeDiffArtifactHealth::Available => DiffArtifactHealthWire::Available,
            ForgeDiffArtifactHealth::NotApplicable => DiffArtifactHealthWire::NotApplicable,
            ForgeDiffArtifactHealth::Archived => DiffArtifactHealthWire::Archived,
            ForgeDiffArtifactHealth::Missing => DiffArtifactHealthWire::Missing,
            ForgeDiffArtifactHealth::Corrupt => DiffArtifactHealthWire::Corrupt,
        },
        evidence: diff_evidence_wire(doctor.evidence()),
    }))
}

fn scope_wire_for_evidence(scope: &crate::wire::WorkspaceQueryScope) -> SourceScopeWire {
    SourceScopeWire {
        workspace_id: scope.workspace_id,
        workspace_revision: scope.workspace_revision,
        locator: scope.locator.clone(),
    }
}

fn diff_evidence_wire(evidence: &ForgeDiffEvidence) -> DiffEvidenceWire {
    use crate::forge_source::forge_reads::leases::{
        ForgeDiffChangeKind, ForgeDiffEntrySubject, ForgeDiffEvidenceState, ForgeDiffOrigin,
        ForgeDiffRenderKind,
    };
    DiffEvidenceWire {
        diff_id: evidence.diff_id().to_owned(),
        workspace_id: evidence.workspace_id().to_owned(),
        activity_id: evidence.activity_id().to_owned(),
        change_set_id: evidence.change_set_id().to_owned(),
        origin: match evidence.origin() {
            ForgeDiffOrigin::FileMutation => DiffOriginWire::FileMutation,
            ForgeDiffOrigin::TreeChangeSet => DiffOriginWire::TreeChangeSet,
        },
        mutation_id: evidence.mutation_id().map(str::to_owned),
        entries: evidence
            .entries()
            .iter()
            .map(|entry| DiffEntryWire {
                subject: match entry.subject() {
                    ForgeDiffEntrySubject::File => DiffEntrySubjectWire::File,
                    ForgeDiffEntrySubject::Directory => DiffEntrySubjectWire::Directory,
                },
                primary_path: entry.primary_path_token().to_owned(),
                secondary_path: entry.secondary_path_token().map(str::to_owned),
                change_kind: match entry.change_kind() {
                    ForgeDiffChangeKind::Created => DiffChangeKindWire::Created,
                    ForgeDiffChangeKind::Modified => DiffChangeKindWire::Modified,
                    ForgeDiffChangeKind::Deleted => DiffChangeKindWire::Deleted,
                    ForgeDiffChangeKind::Moved => DiffChangeKindWire::Moved,
                },
                additions: entry.additions(),
                deletions: entry.deletions(),
            })
            .collect(),
        state: match evidence.state() {
            ForgeDiffEvidenceState::Intent => DiffEvidenceStateWire::Intent,
            ForgeDiffEvidenceState::Ready => DiffEvidenceStateWire::Ready,
            ForgeDiffEvidenceState::SummaryOnly => DiffEvidenceStateWire::SummaryOnly,
            ForgeDiffEvidenceState::Abandoned => DiffEvidenceStateWire::Abandoned,
            ForgeDiffEvidenceState::PurgePending => DiffEvidenceStateWire::PurgePending,
            ForgeDiffEvidenceState::Tombstoned => DiffEvidenceStateWire::Tombstoned,
        },
        render_kind: match evidence.render_kind() {
            ForgeDiffRenderKind::Text => DiffRenderKindWire::Text,
            ForgeDiffRenderKind::Binary => DiffRenderKindWire::Binary,
            ForgeDiffRenderKind::Oversized => DiffRenderKindWire::Oversized,
            ForgeDiffRenderKind::Structured => DiffRenderKindWire::Structured,
        },
        compressed_hash: evidence.compressed_hash().map(str::to_owned),
        uncompressed_hash: evidence.uncompressed_hash().map(str::to_owned),
        compressed_size_bytes: evidence.compressed_size_bytes(),
        uncompressed_size_bytes: evidence.uncompressed_size_bytes(),
        renderer_version: evidence.renderer_version(),
        redaction_policy_version: evidence.redaction_policy_version(),
        created_at_ms: evidence.created_at_ms(),
        updated_at_ms: evidence.updated_at_ms(),
        revision: evidence.revision(),
    }
}

fn scope_dto_from_wire(
    scope: &crate::wire::WorkspaceQueryScope,
) -> Result<forge_backend::ops::ScopeDto, ApiError> {
    use forge_control::WorkspaceLocator;
    let locator =
        WorkspaceLocator::decode(&scope.locator).map_err(|_| ApiError::InvalidIdentity)?;
    if locator.encode() != scope.locator {
        return Err(ApiError::InvalidIdentity);
    }
    let relative_root = locator
        .relative_root()
        .to_str()
        .ok_or(ApiError::InvalidIdentity)?
        .to_owned();
    crate::forge_source::forge_reads::file::scope_dto(
        scope.workspace_id,
        scope.workspace_revision,
        scope.location_id,
        scope.location_revision,
        &relative_root,
    )
    .map_err(map_source_error)
}

fn diff_wire(read: &ForgeDiffRead) -> DiffEvidenceWire {
    DiffEvidenceWire {
        diff_id: read.diff_id().to_owned(),
        workspace_id: read.workspace_id().to_owned(),
        activity_id: read.activity_id().to_owned(),
        change_set_id: read.change_set_id().to_owned(),
        origin: match read.origin() {
            ForgeDiffOrigin::FileMutation => DiffOriginWire::FileMutation,
            ForgeDiffOrigin::TreeChangeSet => DiffOriginWire::TreeChangeSet,
        },
        mutation_id: read.mutation_id().map(str::to_owned),
        entries: read
            .entries()
            .iter()
            .map(|entry| DiffEntryWire {
                subject: match entry.subject() {
                    ForgeDiffEntrySubject::File => DiffEntrySubjectWire::File,
                    ForgeDiffEntrySubject::Directory => DiffEntrySubjectWire::Directory,
                },
                primary_path: entry.primary_path_token().to_owned(),
                secondary_path: entry.secondary_path_token().map(str::to_owned),
                change_kind: match entry.change_kind() {
                    ForgeDiffChangeKind::Created => DiffChangeKindWire::Created,
                    ForgeDiffChangeKind::Modified => DiffChangeKindWire::Modified,
                    ForgeDiffChangeKind::Deleted => DiffChangeKindWire::Deleted,
                    ForgeDiffChangeKind::Moved => DiffChangeKindWire::Moved,
                },
                additions: entry.additions(),
                deletions: entry.deletions(),
            })
            .collect(),
        state: match read.state() {
            ForgeDiffEvidenceState::Intent => DiffEvidenceStateWire::Intent,
            ForgeDiffEvidenceState::Ready => DiffEvidenceStateWire::Ready,
            ForgeDiffEvidenceState::SummaryOnly => DiffEvidenceStateWire::SummaryOnly,
            ForgeDiffEvidenceState::Abandoned => DiffEvidenceStateWire::Abandoned,
            ForgeDiffEvidenceState::PurgePending => DiffEvidenceStateWire::PurgePending,
            ForgeDiffEvidenceState::Tombstoned => DiffEvidenceStateWire::Tombstoned,
        },
        render_kind: match read.render_kind() {
            ForgeDiffRenderKind::Text => DiffRenderKindWire::Text,
            ForgeDiffRenderKind::Binary => DiffRenderKindWire::Binary,
            ForgeDiffRenderKind::Oversized => DiffRenderKindWire::Oversized,
            ForgeDiffRenderKind::Structured => DiffRenderKindWire::Structured,
        },
        compressed_hash: read.compressed_hash().map(str::to_owned),
        uncompressed_hash: read.uncompressed_hash().map(str::to_owned),
        compressed_size_bytes: read.compressed_size_bytes(),
        uncompressed_size_bytes: read.uncompressed_size_bytes(),
        renderer_version: read.renderer_version(),
        redaction_policy_version: read.redaction_policy_version(),
        created_at_ms: read.created_at_ms(),
        updated_at_ms: read.updated_at_ms(),
        revision: read.revision(),
    }
}

fn lease_wire(lease: &ForgeWriterLeaseProjection) -> WriterLeaseProjectionWire {
    WriterLeaseProjectionWire {
        lease_id: lease.lease_id().to_owned(),
        owner_activity_id: lease.owner_activity_id().to_owned(),
        agent_id: lease.agent_id().to_owned(),
        owner_session_id: lease.owner_session_id().map(str::to_owned),
        revision: lease.revision(),
        lease_until_ms: lease.lease_until_ms(),
        observed_at_ms: lease.observed_at_ms(),
        state: match lease.state() {
            ForgeWriterLeaseState::Active => WriterLeaseStateWire::Active,
            ForgeWriterLeaseState::Released => WriterLeaseStateWire::Released,
        },
        status: match lease.status() {
            ForgeWriterLeaseStatus::Active => WriterLeaseStatusWire::Active,
            ForgeWriterLeaseStatus::ExpiredRecoveryRequired => {
                WriterLeaseStatusWire::ExpiredRecoveryRequired
            }
            ForgeWriterLeaseStatus::Released => WriterLeaseStatusWire::Released,
        },
        blocking_reason: lease.blocking_reason().map(str::to_owned),
        recovery_action: lease.recovery_action().to_owned(),
        total_scope_count: u64::try_from(lease.total_scope_count()).unwrap_or(u64::MAX),
        scope_offset: u64::try_from(lease.scope_offset()).unwrap_or(u64::MAX),
        next_scope_offset: lease
            .next_scope_offset()
            .map(|offset| u64::try_from(offset).unwrap_or(u64::MAX)),
        scope_page_limit: u64::try_from(lease.scope_page_limit()).unwrap_or(u64::MAX),
        scopes: lease
            .scopes()
            .iter()
            .map(|entry| WriterLeaseScopeWire {
                kind: match entry.kind() {
                    ForgeWriterLeaseScopeKind::ExactPath => WriterLeaseScopeKindWire::ExactPath,
                    ForgeWriterLeaseScopeKind::DirectorySubtree => {
                        WriterLeaseScopeKindWire::DirectorySubtree
                    }
                },
                path_base64: entry.path_token().to_owned(),
                display_path: entry.display_path().to_owned(),
                display_path_truncated: entry.display_path_truncated(),
            })
            .collect(),
    }
}

fn scope_wire(scope: &ForgeSourceScope) -> SourceScopeWire {
    SourceScopeWire {
        workspace_id: scope.workspace_id(),
        workspace_revision: scope.workspace_revision(),
        locator: scope.locator_token().to_owned(),
    }
}

/// The bounded configured HTTP source with one shared source-query slot.
///
/// A host without the configured HTTP source (including Control-backed
/// hosts) stays fail-closed for source reads.
async fn forge_source(
    state: &BackendState,
) -> Result<
    (
        OwnedSemaphorePermit,
        crate::forge_source::ForgeSourceHttpClient,
    ),
    ApiError,
> {
    let authority = state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let client = authority
        .http_source()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?
        .clone();
    let permit = state
        .forge_query_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Capacity)?;
    Ok((permit, client))
}

fn location_identity(
    location_id: Uuid,
    expected_revision: i64,
) -> Result<(LocationId, LocationRevision), ApiError> {
    let revision =
        LocationRevision::new(expected_revision).map_err(|_| ApiError::InvalidIdentity)?;
    if location_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    Ok((LocationId::from_uuid(location_id), revision))
}

/// Classify a bounded source-read failure onto the shared response contract.
/// Peer text, credentials, and endpoint details are never retained.
fn map_source_error(error: ForgeSourceError) -> ApiError {
    match error {
        ForgeSourceError::InvalidScope => ApiError::InvalidIdentity,
        ForgeSourceError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
        ForgeSourceError::Stale => ApiError::Runtime(RuntimeError::Conflict),
        ForgeSourceError::NotFound => ApiError::NotFound,
        ForgeSourceError::Unavailable => ApiError::WorkspaceScopeUnavailable,
        ForgeSourceError::Protocol => ApiError::Unavailable,
    }
}

fn location_wire(row: &ForgeLocationRow) -> LocationRowWire {
    LocationRowWire {
        location_id: row.location_id(),
        display_name: row.display_name().to_owned(),
        revision: row.revision(),
        state: match row.state() {
            ForgeLocationState::Enabled => LocationStateWire::Enabled,
            ForgeLocationState::Disabled => LocationStateWire::Disabled,
        },
    }
}

fn catalog_entry_wire(row: &ForgeWorkspaceCatalogRow) -> WorkspaceCatalogEntryWire {
    WorkspaceCatalogEntryWire {
        locator: row.locator().to_owned(),
        display_name: row.display_name().to_owned(),
        workspace_id: row.workspace_id(),
        workspace_revision: row.workspace_revision(),
    }
}

fn discovery_entry_wire(row: &ForgeWorkspaceDiscoveryRow) -> WorkspaceDiscoveryEntryWire {
    WorkspaceDiscoveryEntryWire {
        locator: row.locator().to_owned(),
        kind: match row.kind() {
            ForgeDiscoveryKind::LocationRoot => DiscoveryKindWire::LocationRoot,
            ForgeDiscoveryKind::DirectChild => DiscoveryKindWire::DirectChild,
        },
        display_name: row.display_name().to_owned(),
        signals: DiscoverySignalsWire {
            present: row
                .present()
                .iter()
                .map(|signal| match *signal {
                    ForgeDiscoverySignal::CargoWorkspace => DiscoverySignalWire::CargoWorkspace,
                    ForgeDiscoverySignal::GitRepository => DiscoverySignalWire::GitRepository,
                    ForgeDiscoverySignal::AgentInstructions => {
                        DiscoverySignalWire::AgentInstructions
                    }
                    ForgeDiscoverySignal::ForgeState => DiscoverySignalWire::ForgeState,
                })
                .collect(),
        },
        disposition: match row.disposition() {
            ForgeDiscoveryDisposition::Candidate => DiscoveryDispositionWire::Candidate,
            ForgeDiscoveryDisposition::Ignored => DiscoveryDispositionWire::Ignored,
            ForgeDiscoveryDisposition::NotWorkspace => DiscoveryDispositionWire::NotWorkspace,
        },
        reason: match row.reason() {
            ForgeDiscoveryReason::RegisteredLocationRoot => {
                DiscoveryReasonWire::RegisteredLocationRoot
            }
            ForgeDiscoveryReason::ForgeState => DiscoveryReasonWire::ForgeState,
            ForgeDiscoveryReason::CargoWorkspace => DiscoveryReasonWire::CargoWorkspace,
            ForgeDiscoveryReason::GitRepository => DiscoveryReasonWire::GitRepository,
            ForgeDiscoveryReason::AgentInstructions => DiscoveryReasonWire::AgentInstructions,
            ForgeDiscoveryReason::VcsMetadata => DiscoveryReasonWire::VcsMetadata,
            ForgeDiscoveryReason::ForgeInternalState => DiscoveryReasonWire::ForgeInternalState,
            ForgeDiscoveryReason::BuildOutput => DiscoveryReasonWire::BuildOutput,
            ForgeDiscoveryReason::DependencyCache => DiscoveryReasonWire::DependencyCache,
            ForgeDiscoveryReason::FrameworkCache => DiscoveryReasonWire::FrameworkCache,
            ForgeDiscoveryReason::CoverageOutput => DiscoveryReasonWire::CoverageOutput,
            ForgeDiscoveryReason::Cache => DiscoveryReasonWire::Cache,
            ForgeDiscoveryReason::Temporary => DiscoveryReasonWire::Temporary,
            ForgeDiscoveryReason::PythonCache => DiscoveryReasonWire::PythonCache,
            ForgeDiscoveryReason::VirtualEnvironment => DiscoveryReasonWire::VirtualEnvironment,
            ForgeDiscoveryReason::HiddenMetadata => DiscoveryReasonWire::HiddenMetadata,
            ForgeDiscoveryReason::NoWorkspaceSignals => DiscoveryReasonWire::NoWorkspaceSignals,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn source_errors_map_onto_the_shared_response_contract() {
        assert!(matches!(
            map_source_error(ForgeSourceError::InvalidScope),
            ApiError::InvalidIdentity
        ));
        assert!(matches!(
            map_source_error(ForgeSourceError::Denied),
            ApiError::Runtime(RuntimeError::Forbidden)
        ));
        assert!(matches!(
            map_source_error(ForgeSourceError::Stale),
            ApiError::Runtime(RuntimeError::Conflict)
        ));
        assert!(matches!(
            map_source_error(ForgeSourceError::Unavailable),
            ApiError::WorkspaceScopeUnavailable
        ));
        assert!(matches!(
            map_source_error(ForgeSourceError::Protocol),
            ApiError::Unavailable
        ));
    }

    #[test]
    fn catalog_and_discovery_rows_project_with_exact_wire_vocabulary() -> TestResult {
        let location_id = LocationId::from_uuid(Uuid::from_u128(0x601));
        let revision = LocationRevision::new(5)?;
        let location_readback = serde_json::json!({
            "entries": [
                {
                    "location_id": Uuid::from_u128(0x601),
                    "display_name": "Local Disk",
                    "revision": 5,
                    "state": "disabled",
                }
            ]
        });
        let root = crate::forge_source::forge_reads::workspace::location_list_output_from_wire(
            &location_readback,
        )?
        .into_iter()
        .next()
        .ok_or("expected one location row")?;
        let location = location_wire(&root);
        let encoded = serde_json::to_value(&location)?;
        assert_eq!(encoded["state"], serde_json::json!("disabled"));
        assert_eq!(
            encoded["locationId"],
            serde_json::json!(Uuid::from_u128(0x601))
        );
        assert_eq!(encoded["displayName"], serde_json::json!("Local Disk"));
        assert_eq!(encoded["revision"], serde_json::json!(5));

        let catalog_readback = serde_json::json!({
            "entries": [
                {
                    "locator": forge_control::WorkspaceLocator::new(
                        location_id,
                        revision,
                        std::path::Path::new("."),
                    )?
                    .encode(),
                    "display_name": "Root",
                    "workspace_id": Uuid::from_u128(0x602),
                    "workspace_revision": 8,
                }
            ]
        });
        let catalog = crate::forge_source::forge_reads::workspace::workspace_list_output_from_wire(
            &catalog_readback,
            location_id,
            revision,
        )?
        .into_iter()
        .next()
        .ok_or("expected one catalog row")?;
        let entry = catalog_entry_wire(&catalog);
        let encoded = serde_json::to_value(&entry)?;
        assert_eq!(encoded["locator"], serde_json::json!(catalog.locator()));
        assert_eq!(
            encoded["workspaceId"],
            serde_json::json!(Uuid::from_u128(0x602))
        );

        let discovery_readback = serde_json::json!({
            "entries": [
                {
                    "locator": forge_control::WorkspaceLocator::new(
                        location_id,
                        revision,
                        std::path::Path::new("alpha"),
                    )?
                    .encode(),
                    "kind": "direct_child",
                    "display_name": "alpha",
                    "signals": {"present": ["cargo_workspace", "forge_state"]},
                    "disposition": "ignored",
                    "reason": "build_output",
                }
            ]
        });
        let discovery =
            crate::forge_source::forge_reads::workspace::workspace_discover_output_from_wire(
                &discovery_readback,
                location_id,
                revision,
            )?
            .into_iter()
            .next()
            .ok_or("expected one discovery row")?;
        let entry = discovery_entry_wire(&discovery);
        let encoded = serde_json::to_value(&entry)?;
        assert_eq!(encoded["kind"], serde_json::json!("direct_child"));
        assert_eq!(
            encoded["signals"]["present"],
            serde_json::json!(["cargo_workspace", "forge_state"])
        );
        assert_eq!(encoded["disposition"], serde_json::json!("ignored"));
        assert_eq!(encoded["reason"], serde_json::json!("build_output"));
        Ok(())
    }

    #[test]
    fn location_identity_requests_validate_revision_and_identity() -> TestResult {
        let valid = WorkspaceListRequest {
            location_id: Uuid::from_u128(0x603),
            expected_revision: 3,
        };
        let Ok((location, revision)) =
            location_identity(valid.location_id, valid.expected_revision)
        else {
            return Err("expected a valid location identity".into());
        };
        assert_eq!(location.into_uuid(), Uuid::from_u128(0x603));
        assert_eq!(revision.get(), 3);

        let nil = WorkspaceListRequest {
            location_id: Uuid::nil(),
            expected_revision: 3,
        };
        assert!(matches!(
            location_identity(nil.location_id, nil.expected_revision),
            Err(ApiError::InvalidIdentity)
        ));

        let zero = WorkspaceListRequest {
            location_id: Uuid::from_u128(0x603),
            expected_revision: 0,
        };
        assert!(matches!(
            location_identity(zero.location_id, zero.expected_revision),
            Err(ApiError::InvalidIdentity)
        ));
        Ok(())
    }
}
