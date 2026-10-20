//! Session-free operator API for Workspace `ChatGPT` Project configuration.

use axum::extract::DefaultBodyLimit;
use axum::extract::{Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::WorkspaceProjectScope;
use uuid::Uuid;

use crate::wire::workspace_project::{
    WorkspaceProjectBindingWire, WorkspaceProjectConfigureRequest,
    WorkspaceProjectConfigureResponse, WorkspaceProjectQueryRequest, WorkspaceProjectQueryResponse,
    WorkspaceProjectReadbackRequest, WorkspaceProjectReadbackResponse,
};
use crate::{ApiError, BackendState, OrchestrationRuntime, RuntimeError};
use forge_control::ResolvedWorkspaceScope;
use heterarchy_orchestration_operations::WorkspaceProjectConfigureOutcome;

const MAX_WORKSPACE_PROJECT_BODY_BYTES: usize = 8192;

enum ConfigureResult {
    Confirmed(Box<WorkspaceProjectConfigureOutcome>, bool),
    CommittedButUnconfirmed,
}

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/chatgpt-project/query",
            post(query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/chatgpt-project/configure",
            post(configure),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/chatgpt-project/readback",
            post(readback),
        )
        .layer(DefaultBodyLimit::max(MAX_WORKSPACE_PROJECT_BODY_BYTES))
}

async fn query(
    State(state): State<BackendState>,
    Path(path_workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceProjectQueryRequest>,
) -> Result<Json<WorkspaceProjectQueryResponse>, ApiError> {
    let workspace_id = validate_path_scope(&path_workspace, &body.scope)?;
    let _scope = WorkspaceProjectScope::try_from(&body.scope)
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    state
        .runtime
        .require_operator(principal)
        .map_err(ApiError::Runtime)?;
    let proven_scope = state.workspace_candidate_proof(body.scope).await?;
    let authority = state
        .coordinator_authority
        .clone()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let (binding, work_authorized) = state
        .catalog_job(move |runtime| {
            let binding = runtime.workspace_project_binding(principal, workspace_id)?;
            let work_authorized = authority
                .workspace_authorized(&proven_scope)
                .map_err(|_| RuntimeError::SourceAuthorityUnavailable)?;
            Ok((binding, work_authorized))
        })
        .await?;
    Ok(Json(WorkspaceProjectQueryResponse {
        workspace_id,
        binding: binding.as_ref().map(WorkspaceProjectBindingWire::from),
        work_authorized,
    }))
}

async fn configure(
    State(state): State<BackendState>,
    Path(path_workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceProjectConfigureRequest>,
) -> Result<Json<WorkspaceProjectConfigureResponse>, ApiError> {
    let workspace_id = validate_path_scope(&path_workspace, &body.scope)?;
    let scope = WorkspaceProjectScope::try_from(&body.scope)
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    if scope.workspace_id() != workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    let command = body
        .into_command(
            forge_domain::TimestampMillis::new(crate::current_timestamp_millis()?)
                .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?,
        )
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    state
        .runtime
        .require_operator(principal)
        .map_err(ApiError::Runtime)?;
    let wire_scope = crate::wire::workspace_project::scope_wire(&scope);
    let proven_scope = state.workspace_candidate_proof(wire_scope).await?;
    let authority = state
        .coordinator_authority
        .clone()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let static_workspace_ids = authority.static_workspace_ids();
    let result = state
        .run_operator_mutation(principal, move |runtime| {
            configure_and_publish(
                runtime,
                principal,
                command,
                &static_workspace_ids,
                &authority,
                &proven_scope,
                workspace_id,
            )
        })
        .await?;
    configure_response(result)
}

fn configure_and_publish(
    runtime: &OrchestrationRuntime,
    principal: forge_domain::AuthenticatedPrincipal,
    command: heterarchy_orchestration_operations::WorkspaceProjectConfigureCommand,
    static_workspace_ids: &[Uuid],
    authority: &crate::CoordinatorAuthority,
    proven_scope: &ResolvedWorkspaceScope,
    workspace_id: Uuid,
) -> Result<ConfigureResult, RuntimeError> {
    let outcome = match runtime.configure_workspace_project(
        principal,
        command,
        static_workspace_ids,
    ) {
        Ok(outcome) => outcome,
        Err(error)
            if matches!(
                &error,
                RuntimeError::Persistence(inner)
                    if matches!(
                        inner.as_ref(),
                        heterarchy_orchestration_operations::OperationsError::CatalogCommitUnconfirmed { .. }
                    )
            ) =>
        {
            authority.mark_workspace_project_projection_unavailable(workspace_id);
            return Ok(ConfigureResult::CommittedButUnconfirmed);
        }
        Err(error) => return Err(error),
    };

    if !outcome.replayed()
        && !authority.publish_workspace_project_enrollment(
            proven_scope,
            outcome.current_binding().revision(),
        )
    {
        authority.mark_workspace_project_projection_unavailable(workspace_id);
        return Ok(ConfigureResult::CommittedButUnconfirmed);
    }
    let Ok(work_authorized) = authority.workspace_authorized(proven_scope) else {
        authority.mark_workspace_project_projection_unavailable(workspace_id);
        return Ok(ConfigureResult::CommittedButUnconfirmed);
    };
    Ok(ConfigureResult::Confirmed(
        Box::new(outcome),
        work_authorized,
    ))
}

fn configure_response(
    result: ConfigureResult,
) -> Result<Json<WorkspaceProjectConfigureResponse>, ApiError> {
    match result {
        ConfigureResult::Confirmed(outcome, work_authorized) => Ok(Json(
            WorkspaceProjectConfigureResponse::from((&*outcome, work_authorized)),
        )),
        ConfigureResult::CommittedButUnconfirmed => Err(ApiError::SourceBindingUnconfirmed),
    }
}

async fn readback(
    State(state): State<BackendState>,
    Path(path_workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceProjectReadbackRequest>,
) -> Result<Json<WorkspaceProjectReadbackResponse>, ApiError> {
    let workspace_id = validate_path_scope(&path_workspace, &body.scope)?;
    if body.request_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    let scope = WorkspaceProjectScope::try_from(&body.scope)
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    state
        .runtime
        .require_operator(principal)
        .map_err(ApiError::Runtime)?;
    let authority = state
        .coordinator_authority
        .clone()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let current_scope = authority
        .workspace_reference_for_operator_readback(workspace_id, &body.scope)
        .map_err(map_workspace_scope_error)?;
    let _ = state.workspace_candidate_proof(current_scope).await?;
    let receipt = state
        .catalog_job(move |runtime| {
            runtime.workspace_project_receipt(principal, body.request_id, &scope)
        })
        .await?;
    if receipt
        .as_ref()
        .is_some_and(|receipt| receipt.scope().workspace_id() != workspace_id)
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Json(WorkspaceProjectReadbackResponse {
        receipt: receipt
            .as_ref()
            .map(crate::wire::workspace_project::WorkspaceProjectBindingReceiptWire::from),
        retry_authorized: false,
    }))
}

fn map_workspace_scope_error(error: crate::CoordinatorAuthorityError) -> ApiError {
    match error {
        crate::CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
        crate::CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
        crate::CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
    }
}

fn validate_path_scope(
    path_workspace: &str,
    scope: &crate::wire::WorkspaceQueryScope,
) -> Result<Uuid, ApiError> {
    let workspace_id = Uuid::parse_str(path_workspace).map_err(|_| ApiError::InvalidIdentity)?;
    if workspace_id.is_nil()
        || workspace_id.to_string() != path_workspace
        || workspace_id != scope.workspace_id
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(workspace_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use forge_control::{ResolvedWorkspaceScope, WorkspaceLocator};
    use forge_domain::{
        AuthenticatedPrincipal, DirectTokenId, LocationId, LocationRevision, TimestampMillis,
        WorkspaceId, WorkspaceRevision,
    };
    use heterarchy_orchestration_operations::{
        ChatGptProjectId, WorkspaceProjectBindingRevision, WorkspaceProjectConfigureCommand,
    };
    use heterarchy_orchestration_runtime::access::OperatorPolicy;
    use serde_json::{Value, json};
    use std::{error::Error, path::Path, time::Duration};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn post_commit_projection_failure_is_recoverable_by_exact_receipt_readback()
    -> Result<(), Box<dyn Error>> {
        let workspace_id = Uuid::from_u128(0xA901);
        let location_id = Uuid::from_u128(0xA902);
        let location_revision = 5;
        let workspace_revision = 8;
        let locator = WorkspaceLocator::new(
            LocationId::from_uuid(location_id),
            LocationRevision::new(location_revision)?,
            Path::new("recoverable-workspace"),
        )?;
        let wire_scope = crate::wire::WorkspaceQueryScope {
            workspace_id,
            location_id,
            location_revision,
            workspace_revision,
            locator: locator.encode(),
        };
        let proven_scope = ResolvedWorkspaceScope::from_observation(
            WorkspaceId::from_uuid(workspace_id),
            WorkspaceRevision::new(workspace_revision)?,
            locator,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let source_address = listener.local_addr()?;
        let commands = crate::forge_commands::ForgeCommandHttpClient::new(
            source_address,
            "workspace-project-recovery-test",
            Duration::from_secs(2),
        )?;
        let authority = crate::CoordinatorAuthority::from_http(
            crate::forge_source::ForgeSourceHttpClient::new(commands),
            Vec::new(),
        )?;
        assert!(authority.publish_workspace_project_enrollment(
            &proven_scope,
            WorkspaceProjectBindingRevision::new(2)?,
        ));

        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xA903)));
        let data_root = std::env::temp_dir().join(format!(
            "workspace-project-recovery-{}-{}",
            std::process::id(),
            Uuid::now_v7()
        ));
        std::fs::create_dir_all(&data_root)?;
        let runtime = OrchestrationRuntime::open(&data_root)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?);
        let request_id = Uuid::from_u128(0xA904);
        let project_scope = WorkspaceProjectScope::try_from(&wire_scope)?;
        let command = WorkspaceProjectConfigureCommand::new(
            request_id,
            project_scope.clone(),
            None,
            ChatGptProjectId::try_from("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
            TimestampMillis::new(100)?,
        )?;

        let result = configure_and_publish(
            &runtime,
            principal,
            command,
            &[],
            &authority,
            &proven_scope,
            workspace_id,
        )?;
        let Err(api_error) = configure_response(result) else {
            return Err("post-commit projection failure was reported as confirmed".into());
        };
        let error_response = api_error.into_response();
        assert_eq!(
            error_response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        let error_body = axum::body::to_bytes(error_response.into_body(), 4096).await?;
        assert_eq!(
            serde_json::from_slice::<Value>(&error_body)?["code"],
            "COMMITTED_BUT_UNCONFIRMED"
        );

        let binding = runtime
            .workspace_project_binding(principal, workspace_id)?
            .ok_or("committed Project binding missing")?;
        assert_eq!(binding.revision().as_i64(), 1);
        let enrollments = runtime.workspace_project_enrollments()?;
        assert_eq!(enrollments.len(), 1);
        assert_eq!(enrollments[0].scope(), &project_scope);
        assert_eq!(enrollments[0].binding_revision().as_i64(), 1);
        let receipt = runtime
            .workspace_project_receipt(principal, request_id, &project_scope)?
            .ok_or("committed Project receipt missing")?;
        let expected_receipt =
            crate::wire::workspace_project::WorkspaceProjectBindingReceiptWire::from(&receipt);
        assert_eq!(
            authority.validate_scope(&wire_scope),
            Err(crate::CoordinatorAuthorityError::Unavailable)
        );

        let source = tokio::spawn(serve_workspace_resolution_once(
            listener,
            workspace_id,
            workspace_revision,
        ));
        let mut state = crate::BackendState::new(
            runtime,
            std::sync::Arc::new(crate::realtime::RealtimeHub::new()),
        );
        state.coordinator_authority = Some(authority.clone());
        let response = readback(
            State(state.clone()),
            Path(workspace_id.to_string()),
            Extension(principal),
            Json(WorkspaceProjectReadbackRequest {
                request_id,
                scope: wire_scope.clone(),
            }),
        )
        .await
        .map_err(|error| std::io::Error::other(format!("receipt readback failed: {error:?}")))?;
        assert!(!response.retry_authorized);
        assert_eq!(response.receipt.as_ref(), Some(&expected_receipt));
        source.await??;
        assert_eq!(
            authority.validate_scope(&wire_scope),
            Err(crate::CoordinatorAuthorityError::Unavailable)
        );
        drop(state);
        std::fs::remove_dir_all(data_root)?;
        Ok(())
    }

    async fn serve_workspace_resolution_once(
        listener: tokio::net::TcpListener,
        workspace_id: Uuid,
        workspace_revision: i64,
    ) -> Result<(), std::io::Error> {
        let (mut socket, _) = listener.accept().await?;
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        let (body_start, body_length) = loop {
            let count = socket.read(&mut chunk).await?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Forge command request ended early",
                ));
            }
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.len() > 64 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Forge command request exceeded the test bound",
                ));
            }
            if let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let header_text = String::from_utf8_lossy(&bytes[..header_end]);
                let body_length = header_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Forge command content length missing",
                        )
                    })?;
                let body_start = header_end + 4;
                if bytes.len() >= body_start + body_length {
                    break (body_start, body_length);
                }
            }
        };
        let request: Value = serde_json::from_slice(&bytes[body_start..body_start + body_length])
            .map_err(std::io::Error::other)?;
        let response = serde_json::to_vec(&json!({
            "forge_api": request["forge_api"],
            "request_id": request["request_id"],
            "ok": true,
            "result": {
                "workspace_id": workspace_id,
                "workspace_revision": workspace_revision,
            },
        }))
        .map_err(std::io::Error::other)?;
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response.len(),
        );
        socket.write_all(head.as_bytes()).await?;
        socket.write_all(&response).await?;
        socket.shutdown().await
    }
}
