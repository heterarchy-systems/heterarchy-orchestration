//! Source-fenced HTTP adapter for temporary-dispatch request admission.

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Extension, Path, State},
    routing::post,
};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    TemporaryDispatchAdmissionResult, TemporaryDispatchRequestRecord,
};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError,
    temporary_dispatch::{TemporaryDispatchAdmissionSpec, TemporaryDispatchReadbackSpec},
};
use uuid::Uuid;

use crate::{
    ApiError, BackendState,
    app_support::{LaneError, validate_workspace_reference},
    wire::{WorkspaceQueryScope, temporary_dispatch as wire},
};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/temporary-dispatch/admit",
            post(admit),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/temporary-dispatch/readback",
            post(readback),
        )
        .layer(DefaultBodyLimit::max(
            wire::MAX_TEMPORARY_DISPATCH_BODY_BYTES,
        ))
}

async fn admit(
    State(state): State<BackendState>,
    Path((workspace_path, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TemporaryDispatchAdmissionRequest>,
) -> Result<Json<wire::TemporaryDispatchAdmissionResponse>, LaneError> {
    validate_admission_path(&workspace_path, &team_path, &request)?;
    let scope = request.scope.clone();
    let activity_id = request.activity_id;
    let session_id = request.session_id;
    let spec = admission_spec(&request);
    let result = run_scoped_temporary_dispatch(
        &state,
        scope,
        activity_id,
        session_id,
        principal,
        move |runtime| runtime.admit_temporary_dispatch(&spec, principal),
    )
    .await?;
    Ok(Json(wire::TemporaryDispatchAdmissionResponse {
        request,
        outcome: admission_outcome(result),
    }))
}

async fn readback(
    State(state): State<BackendState>,
    Path((workspace_path, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TemporaryDispatchReadbackRequest>,
) -> Result<Json<wire::TemporaryDispatchReadbackResponse>, LaneError> {
    validate_readback_path(&workspace_path, &team_path, &request)?;
    let scope = request.current_scope.clone();
    let current_activity_id = request.current_actor.activity_id;
    let current_session_id = request.current_actor.session_id;
    let spec = TemporaryDispatchReadbackSpec {
        intent: admission_spec(&request.intent),
        current_activity_id,
        current_session_id,
    };
    let receipt = run_scoped_temporary_dispatch(
        &state,
        scope,
        current_activity_id,
        current_session_id,
        principal,
        move |runtime| runtime.read_temporary_dispatch_request(&spec, principal),
    )
    .await?;
    let receipt = receipt.as_ref().map(receipt_wire);
    if receipt
        .as_ref()
        .is_some_and(|value| !wire::receipt_matches_intent(value, &request.intent))
    {
        return Err(ApiError::Unavailable.into());
    }
    Ok(Json(wire::TemporaryDispatchReadbackResponse {
        request,
        receipt,
    }))
}

fn validate_admission_path(
    workspace_path: &str,
    team_path: &str,
    request: &wire::TemporaryDispatchAdmissionRequest,
) -> Result<(), LaneError> {
    validate_workspace_reference(&request.scope, Some(request.session_id))?;
    if crate::canonical_uuid(workspace_path)? != request.scope.workspace_id
        || crate::canonical_uuid(team_path)? != request.requesting_team_id
        || !wire::valid_admission_request(request)
    {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn validate_readback_path(
    workspace_path: &str,
    team_path: &str,
    request: &wire::TemporaryDispatchReadbackRequest,
) -> Result<(), LaneError> {
    validate_workspace_reference(
        &request.current_scope,
        Some(request.current_actor.session_id),
    )?;
    if crate::canonical_uuid(workspace_path)? != request.current_scope.workspace_id
        || request.current_scope.workspace_id != request.intent.scope.workspace_id
        || crate::canonical_uuid(team_path)? != request.intent.requesting_team_id
        || !wire::valid_readback_request(request)
    {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn admission_spec(
    request: &wire::TemporaryDispatchAdmissionRequest,
) -> TemporaryDispatchAdmissionSpec {
    TemporaryDispatchAdmissionSpec {
        request_id: request.request_id,
        workspace_id: request.scope.workspace_id,
        requesting_team_id: request.requesting_team_id,
        activity_id: request.activity_id,
        session_id: request.session_id,
        expected_leadership_revision: request.expected_leadership_revision,
        target_participant_id: request.target_participant_id,
        requested_scope: request.requested_scope.clone(),
        requested_end_at_ms: request.requested_end_at_ms,
    }
}

fn admission_outcome(
    result: TemporaryDispatchAdmissionResult,
) -> wire::TemporaryDispatchAdmissionOutcomeWire {
    match result {
        TemporaryDispatchAdmissionResult::Admitted(record) => {
            wire::TemporaryDispatchAdmissionOutcomeWire::Admitted {
                receipt: Box::new(receipt_wire(record.as_ref())),
            }
        }
        TemporaryDispatchAdmissionResult::QuotaRejected {
            request_id,
            workspace_id,
            requesting_team_id,
            retry_at,
        } => wire::TemporaryDispatchAdmissionOutcomeWire::QuotaRejected {
            request_id,
            workspace_id: workspace_id.into_uuid(),
            requesting_team_id,
            retry_at_ms: retry_at.get(),
        },
    }
}

fn receipt_wire(record: &TemporaryDispatchRequestRecord) -> wire::TemporaryDispatchReceiptWire {
    let actor = record.actor();
    wire::TemporaryDispatchReceiptWire {
        request_id: record.request_id(),
        workspace_id: record.workspace_id().into_uuid(),
        requesting_team_id: record.requesting_team_id(),
        leadership_revision: record.leadership_revision(),
        actor: wire::TemporaryDispatchActorReceiptWire {
            activity_id: actor.activity_id().into_uuid(),
            agent_id: actor.agent_id().into_uuid(),
            session_id: actor.session_id().into_uuid(),
            participation_id: actor.participation_id().into_uuid(),
            profile_id: actor.profile_id(),
            participant_id: actor.participant_id(),
            display_name: actor.display_name().to_owned(),
        },
        target_home_team_id: record.target_home_team_id(),
        target_participant_id: record.target_participant_id(),
        requested_scope: record.requested_scope().as_str().to_owned(),
        requested_end_at_ms: record.requested_end_at().get(),
        admitted_at_ms: record.admitted_at().get(),
        lock_until_ms: record.lock_until().map(forge_domain::TimestampMillis::get),
        replayed: record.replayed(),
    }
}

pub(super) async fn run_scoped_temporary_dispatch<T, F>(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    activity_id: Uuid,
    session_id: Uuid,
    principal: AuthenticatedPrincipal,
    operation: F,
) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
{
    let owned_state = state.clone();
    state
        .run_owned(async move {
            let workspace_id = scope.workspace_id;
            owned_state
                .catalog_job(move |runtime| {
                    authorize_actor(runtime, workspace_id, session_id, principal)
                })
                .await?;
            owned_state
                .activity_scope_proof(scope.clone(), activity_id)
                .await?;
            owned_state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            owned_state.activity_scope_proof(scope, activity_id).await?;
            owned_state
                .catalog_job(move |runtime| {
                    authorize_actor(runtime, workspace_id, session_id, principal)?;
                    operation(runtime)
                })
                .await
        })
        .await
}

fn authorize_actor(
    runtime: &OrchestrationRuntime,
    workspace_id: Uuid,
    session_id: Uuid,
    principal: AuthenticatedPrincipal,
) -> Result<(), RuntimeError> {
    runtime.require_open_session(session_id, principal)?;
    runtime.require_workspace_read(workspace_id, Some(session_id), principal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Extension;
    use forge_domain::DirectTokenId;
    use http_body_util::{BodyExt, Full};
    use hyper::{
        Request, StatusCode,
        body::Bytes,
        header::{CONTENT_LENGTH, CONTENT_TYPE, HOST},
    };
    use hyper_util::rt::TokioIo;
    use std::{error::Error, sync::Arc};
    use tokio::net::{TcpListener, TcpStream};

    #[test]
    fn receipt_correlation_rejects_substituted_original_session_or_payload() {
        let intent = wire::TemporaryDispatchAdmissionRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            requesting_team_id: Uuid::from_u128(5),
            request_id: Uuid::from_u128(6),
            activity_id: Uuid::from_u128(7),
            session_id: Uuid::from_u128(8),
            expected_leadership_revision: 9,
            target_participant_id: Uuid::from_u128(10),
            requested_scope: "bounded work".to_owned(),
            requested_end_at_ms: 100,
        };
        let receipt = wire::TemporaryDispatchReceiptWire {
            request_id: intent.request_id,
            workspace_id: intent.scope.workspace_id,
            requesting_team_id: intent.requesting_team_id,
            leadership_revision: intent.expected_leadership_revision,
            actor: wire::TemporaryDispatchActorReceiptWire {
                activity_id: intent.activity_id,
                agent_id: Uuid::from_u128(11),
                session_id: intent.session_id,
                participation_id: Uuid::from_u128(12),
                profile_id: Uuid::from_u128(13),
                participant_id: Uuid::from_u128(14),
                display_name: "Leader".to_owned(),
            },
            target_home_team_id: Uuid::from_u128(15),
            target_participant_id: intent.target_participant_id,
            requested_scope: intent.requested_scope.clone(),
            requested_end_at_ms: intent.requested_end_at_ms,
            admitted_at_ms: 50,
            lock_until_ms: None,
            replayed: true,
        };
        assert!(wire::receipt_matches_intent(&receipt, &intent));

        let mut changed_session = intent.clone();
        changed_session.session_id = Uuid::from_u128(16);
        assert!(!wire::receipt_matches_intent(&receipt, &changed_session));

        let mut changed_payload = intent;
        changed_payload.requested_scope.push('!');
        assert!(!wire::receipt_matches_intent(&receipt, &changed_payload));
    }

    #[test]
    fn readback_proves_current_scope_and_rejects_a_foreign_workspace() {
        let mut intent = wire::TemporaryDispatchAdmissionRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "original-workspace-binding".to_owned(),
            },
            requesting_team_id: Uuid::from_u128(5),
            request_id: Uuid::from_u128(6),
            activity_id: Uuid::from_u128(7),
            session_id: Uuid::from_u128(8),
            expected_leadership_revision: 9,
            target_participant_id: Uuid::from_u128(10),
            requested_scope: "bounded work".to_owned(),
            requested_end_at_ms: 100,
        };
        intent.scope.location_revision = 1;
        intent.scope.workspace_revision = 1;
        let request = wire::TemporaryDispatchReadbackRequest {
            intent,
            current_scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(11),
                location_revision: 12,
                workspace_revision: 13,
                locator: "current-workspace-binding".to_owned(),
            },
            current_actor: wire::TemporaryDispatchCurrentActorWire {
                activity_id: Uuid::from_u128(14),
                session_id: Uuid::from_u128(15),
            },
        };
        assert!(
            validate_readback_path(
                &Uuid::from_u128(1).to_string(),
                &Uuid::from_u128(5).to_string(),
                &request,
            )
            .is_ok()
        );

        let mut foreign_scope = request;
        foreign_scope.current_scope.workspace_id = Uuid::from_u128(16);
        assert!(
            validate_readback_path(
                &Uuid::from_u128(16).to_string(),
                &Uuid::from_u128(5).to_string(),
                &foreign_scope,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn route_rejects_oversized_json_before_source_work() -> Result<(), Box<dyn Error>> {
        let data_root =
            std::env::temp_dir().join(format!("temporary-dispatch-body-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&data_root)?;
        let runtime = OrchestrationRuntime::open(&data_root)?;
        let state = BackendState::new(runtime, Arc::new(crate::RealtimeHub::new()));
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xE801)));
        let app = routes()
            .with_state(state.clone())
            .layer(Extension(principal));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let stream = TcpStream::connect(address).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        let connection_task = tokio::spawn(connection);

        let workspace_id = Uuid::from_u128(0xE802);
        let team_id = Uuid::from_u128(0xE803);
        let request = wire::TemporaryDispatchAdmissionRequest {
            scope: WorkspaceQueryScope {
                workspace_id,
                location_id: Uuid::from_u128(0xE804),
                location_revision: 1,
                workspace_revision: 1,
                locator: "isolated".to_owned(),
            },
            requesting_team_id: team_id,
            request_id: Uuid::from_u128(0xE805),
            activity_id: Uuid::from_u128(0xE806),
            session_id: Uuid::from_u128(0xE807),
            expected_leadership_revision: 1,
            target_participant_id: Uuid::from_u128(0xE808),
            requested_scope: "bounded work".to_owned(),
            requested_end_at_ms: 1,
        };
        let json_body = serde_json::to_string(&request)?;
        let body = format!(
            "{json_body}{}",
            " ".repeat(
                wire::MAX_TEMPORARY_DISPATCH_BODY_BYTES
                    .saturating_add(1)
                    .saturating_sub(json_body.len())
            )
        );
        assert!(body.len() > wire::MAX_TEMPORARY_DISPATCH_BODY_BYTES);
        let body_length = body.len();
        let http_request = Request::builder()
            .method(hyper::Method::POST)
            .uri(format!(
                "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/temporary-dispatch/admit"
            ))
            .header(HOST, address.to_string())
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, body_length)
            .body(Full::new(Bytes::from(body)))?;
        let response = sender.send_request(http_request).await?;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let _body = response.into_body().collect().await?.to_bytes();

        connection_task.abort();
        let _connection_result = connection_task.await;
        server.abort();
        let _server_result = server.await;
        state.catalog_execution.stop_admission();
        state.catalog_execution.drain().await;
        drop(state);
        std::fs::remove_dir_all(data_root)?;
        Ok(())
    }
}
