//! Authenticated operator HTTP bridge for the frozen provider-worker runtime.
//!
//! The runtime and durable Operations catalog remain the only owners of
//! provider admission, dispatch fences, and reconciliation. These handlers
//! translate typed requests, stamp server time, and project durable readback.

use axum::extract::{DefaultBodyLimit, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    AgentProvisionDispatchAction, AgentProvisionFailureCode, AgentProvisionOperation,
    ProviderMessageDispatchAction, ProviderMessageOutbox, ProviderMessageState,
};
use heterarchy_orchestration_runtime::provider_chat::{
    ProviderIncomingCommand, ProviderIncomingRole,
};
use heterarchy_orchestration_runtime::{OrchestrationRuntime, RuntimeError};
use uuid::Uuid;

use crate::BackendState;
use crate::app_support::{LaneError, now_millis};
use crate::realtime::RealtimeEventKind;
use crate::wire::provider_worker::{
    ProviderDeliveryConfirmation, ProviderDispatchRequest, ProviderIncomingMessage,
    ProviderIncomingMessageResponse, ProviderIncomingReadbackResponse,
    ProviderMessageDispatchResponse, ProviderMessageFence, ProviderMessageInspection,
    ProviderMessageInspectionResponse, ProviderMessageMutationResponse, ProviderMessageOutboxWire,
    ProviderMessageSelectionObservation, ProviderMessageSelectionQuery, ProviderMessageTargetWire,
    ProviderOperationFence, ProviderProvisionConfirmation, ProviderProvisionDispatchResponse,
    ProviderProvisionDispatchWire, ProviderProvisionInspection, ProviderProvisionMutationResponse,
    ProviderProvisionQueryResponse, ProviderProvisionSelectionObservation,
    ProviderProvisionTargetWire, ProviderWorkerActionWire,
};
use crate::wire::provisioning_operations::ProvisionOperationWire;

const PROVIDER_WORKER_BODY_LIMIT: usize = 48 * 1024;

/// Merge these authenticated operator-worker routes before applying backend state.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/provider-worker/provision/in-flight",
            get(in_flight_provision),
        )
        .route(
            "/internal/v1/provider-worker/messages/in-flight",
            post(in_flight_messages),
        )
        .route(
            "/internal/v1/provider-worker/messages/query",
            post(query_message),
        )
        .route(
            "/internal/v1/provider-worker/messages/incoming/query",
            post(query_incoming),
        )
        .route(
            "/internal/v1/provider-worker/provision/dispatch",
            post(dispatch_provision),
        )
        .route(
            "/internal/v1/provider-worker/provision/query",
            post(query_provision),
        )
        .route(
            "/internal/v1/provider-worker/provision/unknown",
            post(unknown_provision),
        )
        .route(
            "/internal/v1/provider-worker/provision/fail",
            post(fail_provision),
        )
        .route(
            "/internal/v1/provider-worker/provision/confirm",
            post(confirm_provision),
        )
        .route(
            "/internal/v1/provider-worker/messages/dispatch",
            post(dispatch_message),
        )
        .route(
            "/internal/v1/provider-worker/messages/inspect",
            post(inspect_message),
        )
        .route(
            "/internal/v1/provider-worker/messages/unknown",
            post(unknown_message),
        )
        .route(
            "/internal/v1/provider-worker/messages/fail",
            post(fail_message),
        )
        .route(
            "/internal/v1/provider-worker/messages/delivered",
            post(delivered_message),
        )
        .route(
            "/internal/v1/provider-worker/messages/incoming",
            post(incoming_message),
        )
        .layer(DefaultBodyLimit::max(PROVIDER_WORKER_BODY_LIMIT))
}

async fn dispatch_provision(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(_body): Json<ProviderDispatchRequest>,
) -> Result<Json<ProviderProvisionDispatchResponse>, LaneError> {
    let observed_at_ms = now_millis()?;
    let publisher = state.clone();
    let response = state
        .run_operator_control_mutation(principal, move |runtime| {
            let Some(outcome) =
                runtime.provider_worker_next_provision(principal, observed_at_ms)?
            else {
                return Ok(ProviderProvisionDispatchResponse {
                    dispatch: None,
                    reconciled: None,
                });
            };
            let operation = outcome.operation();
            if outcome
                .dispatch()
                .is_none_or(|fence| fence.action() == AgentProvisionDispatchAction::Execute)
            {
                // The commit is known even if a subsequent projection fails.
                publisher.require_snapshot_resync();
            }
            let workspace_id = request_workspace_id(runtime, operation.request_id())?;
            let Some(fence) = outcome.dispatch() else {
                publisher.publish_state_changed(
                    RealtimeEventKind::ProvisioningStateChanged,
                    operation.operation_id(),
                    operation.revision(),
                    Some(workspace_id),
                );
                return Ok(ProviderProvisionDispatchResponse {
                    dispatch: None,
                    reconciled: Some(ProviderProvisionMutationResponse {
                        workspace_id,
                        operation: ProvisionOperationWire::from(operation),
                    }),
                });
            };
            if fence.action() == AgentProvisionDispatchAction::Execute {
                publisher.publish_state_changed(
                    RealtimeEventKind::ProvisioningStateChanged,
                    operation.operation_id(),
                    operation.revision(),
                    Some(workspace_id),
                );
            }
            let provider_conversation_id = runtime.provider_worker_provision_conversation(
                principal,
                fence.operation_id(),
                fence.operation_revision(),
            )?;
            Ok(ProviderProvisionDispatchResponse {
                dispatch: Some(ProviderProvisionDispatchWire {
                    workspace_id,
                    operation: ProvisionOperationWire::from(operation),
                    operation_revision: fence.operation_revision(),
                    action: provision_action_wire(fence.action()),
                    target: ProviderProvisionTargetWire {
                        profile_id: fence.profile_id().into_uuid(),
                        runtime_session_id: fence.runtime_session_id().into_uuid(),
                        effect_kind: effect_kind_name(fence),
                        provider_family: "chatgpt".to_owned(),
                        provider_conversation_id,
                    },
                }),
                reconciled: None,
            })
        })
        .await?;
    Ok(Json(response))
}

async fn query_provision(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderProvisionInspection>,
) -> Result<Json<ProviderProvisionQueryResponse>, LaneError> {
    if body.operation_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let operation_id = body.operation_id;
    let operation = state
        .run_catalog(move |runtime| {
            runtime
                .provider_worker_inspect_provision(principal, operation_id)
                .map(|operation| operation.as_ref().map(ProvisionOperationWire::from))
        })
        .await?;
    Ok(Json(ProviderProvisionQueryResponse { operation }))
}

async fn in_flight_provision(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ProviderProvisionSelectionObservation>, LaneError> {
    let operation = state
        .run_catalog(move |runtime| {
            runtime
                .provider_worker_in_flight_provision(principal)
                .map(|operation| operation.as_ref().map(ProvisionOperationWire::from))
        })
        .await?;
    Ok(Json(ProviderProvisionSelectionObservation {
        operation,
        retry_authorized: false,
        request_equivalence_proven: false,
    }))
}

async fn in_flight_messages(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderMessageSelectionQuery>,
) -> Result<Json<ProviderMessageSelectionObservation>, LaneError> {
    let page = state
        .run_catalog(move |runtime| {
            runtime.provider_worker_in_flight_messages(principal, body.after_message_id, body.limit)
        })
        .await?;
    Ok(Json(ProviderMessageSelectionObservation {
        messages: page
            .outboxes
            .iter()
            .map(ProviderMessageOutboxWire::from)
            .collect(),
        next_message_id: page.next_message_id,
        retry_authorized: false,
        request_equivalence_proven: false,
    }))
}

async fn query_message(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderMessageInspection>,
) -> Result<Json<ProviderMessageInspectionResponse>, LaneError> {
    let readback = state
        .run_catalog(move |runtime| {
            runtime.provider_worker_message_readback(principal, body.message_id)
        })
        .await?;
    Ok(Json(ProviderMessageInspectionResponse {
        message: ProviderMessageOutboxWire::from(&readback.outbox),
        action: None,
        body: readback.body,
        expires_at_ms: readback.expires_at_ms,
    }))
}

async fn query_incoming(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderIncomingMessage>,
) -> Result<Json<ProviderIncomingReadbackResponse>, LaneError> {
    let message = state
        .run_catalog(move |runtime| {
            let command = incoming_command(&body);
            let Some(message) = runtime.provider_worker_incoming_readback(principal, &command)?
            else {
                return Ok(None);
            };
            let reply =
                runtime.provider_worker_message_readback(principal, body.reply_to_message_id)?;
            Ok(Some(ProviderIncomingMessageResponse {
                created: false,
                message_id: message.message_id(),
                workspace_id: message.workspace_id().into_uuid(),
                target: ProviderMessageTargetWire::from(reply.outbox.target()),
                provider_turn_id: body.provider_turn_id,
                role: body.role,
                body: body.body,
                sequence: message.sequence(),
                submitted_at_ms: message.submitted_at().get(),
            }))
        })
        .await?;
    Ok(Json(ProviderIncomingReadbackResponse {
        message,
        retry_authorized: false,
        request_equivalence_proven: false,
    }))
}

fn incoming_command(body: &ProviderIncomingMessage) -> ProviderIncomingCommand {
    ProviderIncomingCommand {
        reply_to_message_id: body.reply_to_message_id,
        provider_conversation_id: body.provider_conversation_id.clone(),
        provider_turn_id: body.provider_turn_id.clone(),
        body: body.body.clone(),
        role: match body.role {
            crate::wire::provider_worker::ProviderIncomingRole::Assistant => {
                ProviderIncomingRole::Assistant
            }
            crate::wire::provider_worker::ProviderIncomingRole::User => ProviderIncomingRole::User,
        },
    }
}

async fn unknown_provision(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderOperationFence>,
) -> Result<Json<ProviderProvisionMutationResponse>, LaneError> {
    if body.operation_id.is_nil() || body.expected_revision == 0 {
        return Err(LaneError::InvalidIdentity);
    }
    let observed_at_ms = now_millis()?;
    mutate_provision(state, principal, body.operation_id, move |runtime| {
        runtime.provider_worker_mark_provision_unknown(
            principal,
            body.operation_id,
            body.expected_revision,
            observed_at_ms,
        )
    })
    .await
}

async fn fail_provision(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<crate::wire::provider_worker::ProviderProvisionFailure>,
) -> Result<Json<ProviderProvisionMutationResponse>, LaneError> {
    if body.operation_id.is_nil() || body.expected_revision == 0 {
        return Err(LaneError::InvalidIdentity);
    }
    let failure_code = body
        .failure_code
        .as_deref()
        .map(AgentProvisionFailureCode::from_wire_name)
        .transpose()
        .map_err(|_| LaneError::InvalidIdentity)?;
    let observed_at_ms = now_millis()?;
    mutate_provision(state, principal, body.operation_id, move |runtime| {
        runtime.provider_worker_fail_provision(
            principal,
            body.operation_id,
            body.expected_revision,
            observed_at_ms,
            failure_code,
        )
    })
    .await
}

async fn confirm_provision(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderProvisionConfirmation>,
) -> Result<Json<ProviderProvisionMutationResponse>, LaneError> {
    if body.operation_id.is_nil() || body.expected_revision == 0 {
        return Err(LaneError::InvalidIdentity);
    }
    let observed_at_ms = now_millis()?;
    mutate_provision(state, principal, body.operation_id, move |runtime| {
        runtime.provider_worker_confirm_provision(
            principal,
            body.operation_id,
            body.expected_revision,
            &body.provider_conversation_id,
            observed_at_ms,
        )
    })
    .await
}

async fn mutate_provision<F>(
    state: BackendState,
    principal: AuthenticatedPrincipal,
    operation_id: Uuid,
    mutation: F,
) -> Result<Json<ProviderProvisionMutationResponse>, LaneError>
where
    F: FnOnce(&OrchestrationRuntime) -> Result<AgentProvisionOperation, RuntimeError>
        + Send
        + 'static,
{
    let publisher = state.clone();
    let response = state
        .run_operator_control_mutation(principal, move |runtime| {
            let before = runtime
                .provider_worker_inspect_provision(principal, operation_id)?
                .ok_or(RuntimeError::NotFound)?;
            let operation = mutation(runtime)?;
            let workspace_id = request_workspace_id(runtime, operation.request_id())?;
            if before.revision() != operation.revision() {
                publisher.publish_state_changed(
                    RealtimeEventKind::ProvisioningStateChanged,
                    operation.operation_id(),
                    operation.revision(),
                    Some(workspace_id),
                );
            }
            Ok(ProviderProvisionMutationResponse {
                workspace_id,
                operation: ProvisionOperationWire::from(&operation),
            })
        })
        .await?;
    Ok(Json(response))
}

async fn dispatch_message(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(_body): Json<ProviderDispatchRequest>,
) -> Result<Json<ProviderMessageDispatchResponse>, LaneError> {
    let now_ms = now_millis()?;
    let publisher = state.clone();
    let dispatch = state
        .run_operator_control_mutation(principal, move |runtime| {
            let dispatch = runtime.provider_worker_next_message(principal, now_ms)?;
            if let Some(dispatch) = dispatch.as_ref()
                && dispatch
                    .action
                    .is_none_or(|action| action == ProviderMessageDispatchAction::Execute)
            {
                publish_message_change(&publisher, &dispatch.outbox);
            }
            Ok(dispatch.map(|dispatch| ProviderMessageDispatchResponse {
                message: Some(ProviderMessageOutboxWire::from(&dispatch.outbox)),
                action: dispatch.action.map(message_action_wire),
                body: dispatch.body,
                expires_at_ms: dispatch.expires_at_ms,
            }))
        })
        .await?;
    Ok(Json(dispatch.unwrap_or(ProviderMessageDispatchResponse {
        message: None,
        action: None,
        body: None,
        expires_at_ms: None,
    })))
}

async fn inspect_message(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderMessageInspection>,
) -> Result<Json<ProviderMessageInspectionResponse>, LaneError> {
    if body.message_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let now_ms = now_millis()?;
    let publisher = state.clone();
    let dispatch = state
        .run_operator_control_mutation(principal, move |runtime| {
            let dispatch =
                runtime.provider_worker_inspect_message(principal, body.message_id, now_ms)?;
            if dispatch.action.is_none() && dispatch.outbox.state() == ProviderMessageState::Unknown
            {
                publish_message_change(&publisher, &dispatch.outbox);
            }
            Ok(ProviderMessageInspectionResponse {
                message: ProviderMessageOutboxWire::from(&dispatch.outbox),
                action: dispatch.action.map(message_action_wire),
                body: dispatch.body,
                expires_at_ms: dispatch.expires_at_ms,
            })
        })
        .await?;
    Ok(Json(dispatch))
}

async fn unknown_message(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderMessageFence>,
) -> Result<Json<ProviderMessageMutationResponse>, LaneError> {
    if body.message_id.is_nil() || body.expected_revision == 0 {
        return Err(LaneError::InvalidIdentity);
    }
    let now_ms = now_millis()?;
    mutate_message(state, principal, body.message_id, move |runtime| {
        runtime.provider_worker_mark_message_unknown(
            principal,
            body.message_id,
            body.expected_revision,
            now_ms,
        )
    })
    .await
}

async fn fail_message(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderMessageFence>,
) -> Result<Json<ProviderMessageMutationResponse>, LaneError> {
    if body.message_id.is_nil() || body.expected_revision == 0 {
        return Err(LaneError::InvalidIdentity);
    }
    let now_ms = now_millis()?;
    mutate_message(state, principal, body.message_id, move |runtime| {
        runtime.provider_worker_fail_message(
            principal,
            body.message_id,
            body.expected_revision,
            now_ms,
        )
    })
    .await
}

async fn delivered_message(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderDeliveryConfirmation>,
) -> Result<Json<ProviderMessageMutationResponse>, LaneError> {
    if body.message_id.is_nil() || body.expected_revision == 0 {
        return Err(LaneError::InvalidIdentity);
    }
    let now_ms = now_millis()?;
    mutate_message(state, principal, body.message_id, move |runtime| {
        runtime.provider_worker_confirm_message_delivery(
            principal,
            body.message_id,
            body.expected_revision,
            &body.provider_conversation_id,
            body.provider_turn_id,
            body.body,
            now_ms,
        )
    })
    .await
}

async fn mutate_message<F>(
    state: BackendState,
    principal: AuthenticatedPrincipal,
    message_id: Uuid,
    mutation: F,
) -> Result<Json<ProviderMessageMutationResponse>, LaneError>
where
    F: FnOnce(&OrchestrationRuntime) -> Result<ProviderMessageOutbox, RuntimeError>
        + Send
        + 'static,
{
    let publisher = state.clone();
    let response = state
        .run_operator_control_mutation(principal, move |runtime| {
            let before = runtime.provider_worker_message_readback(principal, message_id)?;
            let before_revision = before.outbox.revision();
            let before_body = before.body;
            let expires_at_ms = before.expires_at_ms;
            let updated = mutation(runtime)?;
            if updated.revision() != before_revision {
                publish_message_change(&publisher, &updated);
            }
            Ok(ProviderMessageMutationResponse {
                message: ProviderMessageOutboxWire::from(&updated),
                body: before_body,
                expires_at_ms,
            })
        })
        .await?;
    Ok(Json(response))
}

async fn incoming_message(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProviderIncomingMessage>,
) -> Result<Json<ProviderIncomingMessageResponse>, LaneError> {
    if body.reply_to_message_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let now_ms = now_millis()?;
    let publisher = state.clone();
    let response = state
        .run_operator_control_mutation(principal, move |runtime| {
            let reply =
                runtime.provider_worker_message_readback(principal, body.reply_to_message_id)?;
            let target = ProviderMessageTargetWire::from(reply.outbox.target());
            let role = match body.role {
                crate::wire::provider_worker::ProviderIncomingRole::Assistant => {
                    ProviderIncomingRole::Assistant
                }
                crate::wire::provider_worker::ProviderIncomingRole::User => {
                    ProviderIncomingRole::User
                }
            };
            let outcome = runtime.provider_worker_record_incoming(
                principal,
                ProviderIncomingCommand {
                    reply_to_message_id: body.reply_to_message_id,
                    provider_conversation_id: body.provider_conversation_id.clone(),
                    provider_turn_id: body.provider_turn_id.clone(),
                    role,
                    body: body.body.clone(),
                },
                now_ms,
            )?;
            let message = outcome.message();
            if outcome.created() {
                publisher.publish_state_changed(
                    RealtimeEventKind::SchedulingChanged,
                    message.message_id(),
                    message.sequence(),
                    Some(message.workspace_id().into_uuid()),
                );
            }
            Ok(ProviderIncomingMessageResponse {
                created: outcome.created(),
                message_id: message.message_id(),
                workspace_id: message.workspace_id().into_uuid(),
                target,
                provider_turn_id: body.provider_turn_id,
                role: body.role,
                body: body.body,
                sequence: message.sequence(),
                submitted_at_ms: message.submitted_at().get(),
            })
        })
        .await?;
    Ok(Json(response))
}

fn request_workspace_id(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
) -> Result<Uuid, RuntimeError> {
    let request = runtime
        .agent_request(request_id)?
        .ok_or(RuntimeError::NotFound)?;
    Uuid::parse_str(&request.workspace_id).map_err(|_| {
        RuntimeError::Persistence(
            heterarchy_orchestration_operations::OperationsError::Corrupt(
                "provider worker request Workspace identity",
            ),
        )
    })
}

fn provision_action_wire(action: AgentProvisionDispatchAction) -> ProviderWorkerActionWire {
    match action {
        AgentProvisionDispatchAction::Execute => ProviderWorkerActionWire::Execute,
        AgentProvisionDispatchAction::Inspect => ProviderWorkerActionWire::Inspect,
    }
}

fn effect_kind_name(
    dispatch: heterarchy_orchestration_operations::AgentProvisionDispatch,
) -> String {
    match dispatch.effect_kind() {
        heterarchy_orchestration_operations::AgentProvisionEffectKind::WakeExistingRuntime => {
            "wake_existing_runtime"
        }
        heterarchy_orchestration_operations::AgentProvisionEffectKind::WakeNewRuntime => {
            "wake_new_runtime"
        }
        heterarchy_orchestration_operations::AgentProvisionEffectKind::ProvisionNew => {
            "provision_new"
        }
        heterarchy_orchestration_operations::AgentProvisionEffectKind::ReuseReadyRuntime => {
            "reuse_ready_runtime"
        }
    }
    .to_owned()
}

fn message_action_wire(
    action: heterarchy_orchestration_operations::ProviderMessageDispatchAction,
) -> ProviderWorkerActionWire {
    match action {
        heterarchy_orchestration_operations::ProviderMessageDispatchAction::Execute => {
            ProviderWorkerActionWire::Execute
        }
        heterarchy_orchestration_operations::ProviderMessageDispatchAction::Inspect => {
            ProviderWorkerActionWire::Inspect
        }
    }
}

fn publish_message_change(publisher: &BackendState, outbox: &ProviderMessageOutbox) {
    publisher.publish_state_changed(
        RealtimeEventKind::SchedulingChanged,
        outbox.message_id(),
        outbox.revision(),
        Some(outbox.target().workspace_id().into_uuid()),
    );
}

#[cfg(test)]
mod tests;
