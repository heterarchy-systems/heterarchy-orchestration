//! Authenticated exact node conversations over the canonical Runtime operation.

use crate::{
    ApiError, BackendState, RuntimeError,
    wire::node_chat::{
        ChatMessageRequest, ChatQueryWire, NodeChatQueryRequest, NodeChatQueryResponse,
        NodeChatRecoverResponse, NodeChatSendRequest, NodeChatSendResponse,
    },
};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_runtime::{
    chat::{ChatError, ChatQuery, ChatSend},
    participation::ParticipationTarget,
};
mod projection;
pub use projection::{project_query, project_receipt, project_send, project_snapshot};

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/node-chat/query",
            post(query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/node-chat/send",
            post(send),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/node-chat/recover",
            post(recover),
        )
}

fn caller(
    path: &str,
    scope: &crate::wire::WorkspaceQueryScope,
    session: uuid::Uuid,
    activity: uuid::Uuid,
) -> Result<ParticipationTarget, ApiError> {
    crate::app_support::validate_workspace_reference(scope, Some(session))?;
    if crate::canonical_uuid(path)? != scope.workspace_id || activity.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(ParticipationTarget {
        workspace_id: scope.workspace_id,
        activity_id: activity,
        session_id: session,
        agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
            scope.workspace_id,
            activity,
            session,
        )
        .into_uuid(),
    })
}

/// Parse the transport selectors without treating them as caller authority.
///
/// # Errors
/// Rejects nil identities, invalid generation, or non-node query selectors.
pub fn domain_query(value: &ChatQueryWire) -> Result<ChatQuery, RuntimeError> {
    if value.workspace_id.is_nil()
        || value.agent_profile_id.is_some()
        || value.runtime_session_id.is_some_and(|id| id.is_nil())
        || value.before_sequence == Some(0)
    {
        return Err(RuntimeError::InvalidInput);
    }
    let node = value
        .node_scope
        .as_ref()
        .ok_or(RuntimeError::InvalidInput)?;
    let node_scope = crate::node_team::node_scope(value.workspace_id, node)
        .map_err(|_| RuntimeError::InvalidInput)?;
    Ok(ChatQuery {
        workspace_id: forge_domain::WorkspaceId::from_uuid(value.workspace_id),
        agent_profile_id: None,
        runtime: value.runtime_session_id,
        node_scope: Some(node_scope),
        before_sequence: value.before_sequence,
    })
}

/// Convert the complete frozen node-message intent at the HTTP boundary.
///
/// # Errors
/// Rejects an invalid message identity, send cursor or destination fence.
pub fn domain_send(value: &ChatMessageRequest) -> Result<ChatSend, RuntimeError> {
    let query = domain_query(&value.query)?;
    if value.message_id.is_nil() || query.before_sequence.is_some() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(ChatSend {
        query,
        message_id: value.message_id,
        text: value.text.clone(),
        target_fence: projection::domain_fence(&value.target_fence)?,
    })
}

async fn query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<NodeChatQueryRequest>,
) -> Result<Json<NodeChatQueryResponse>, ApiError> {
    let target = caller(&path, &body.scope, body.session_id, body.sender_activity_id)?;
    if body.query.workspace_id != body.scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    let query = domain_query(&body.query).map_err(ApiError::Runtime)?;
    let owner = state.clone();
    state
        .run_owned(async move {
            let ws = query.workspace_id;
            owner
                .catalog_job(move |runtime| {
                    runtime
                        .authorize_chat_sender(target, principal, ws)
                        .map_err(chat_runtime_error)
                })
                .await?;
            owner.workspace_scope_proof(body.scope.clone()).await?;
            let source = source(&owner, &body.scope, &query).await?;
            let now = now()?;
            let snapshot = owner
                .catalog_job(move |runtime| {
                    runtime
                        .read_chat(target, principal, &query, source.as_ref(), now)
                        .map_err(chat_runtime_error)
                })
                .await?;
            Ok(Json(NodeChatQueryResponse {
                request: body,
                snapshot: project_snapshot(&snapshot),
            }))
        })
        .await
}

async fn send(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<NodeChatSendRequest>,
) -> Result<Json<NodeChatSendResponse>, ApiError> {
    let target = caller(&path, &body.scope, body.session_id, body.sender_activity_id)?;
    validate_send(&body)?;
    let request = domain_send(&body.request).map_err(ApiError::Runtime)?;
    let owner = state.clone();
    state
        .run_owned(async move {
            let frozen = request.clone();
            let fence = body.sender_leadership_revision;
            let existing = owner
                .catalog_job(move |runtime| {
                    runtime
                        .recover_chat(target, principal, &frozen, fence)
                        .map_err(chat_runtime_error)
                })
                .await?;
            if let Some(receipt) = existing {
                return Ok(Json(NodeChatSendResponse {
                    request: body,
                    receipt: project_receipt(&receipt),
                }));
            }
            let ws = request.query.workspace_id;
            owner
                .catalog_job(move |runtime| {
                    runtime
                        .authorize_chat_sender(target, principal, ws)
                        .map_err(chat_runtime_error)
                })
                .await?;
            owner.workspace_scope_proof(body.scope.clone()).await?;
            let source = source(&owner, &body.scope, &request.query).await?;
            let now = now()?;
            let receipt = owner
                .catalog_job(move |runtime| {
                    runtime
                        .send_chat(target, principal, &request, source.as_ref(), fence, now)
                        .map_err(chat_runtime_error)
                })
                .await?;
            Ok(Json(NodeChatSendResponse {
                request: body,
                receipt: project_receipt(&receipt),
            }))
        })
        .await
}

async fn recover(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<NodeChatSendRequest>,
) -> Result<Json<NodeChatRecoverResponse>, ApiError> {
    let target = caller(&path, &body.scope, body.session_id, body.sender_activity_id)?;
    validate_send(&body)?;
    let request = domain_send(&body.request).map_err(ApiError::Runtime)?;
    let fence = body.sender_leadership_revision;
    let receipt = state
        .run_catalog(move |runtime| {
            runtime
                .recover_chat(target, principal, &request, fence)
                .map_err(chat_runtime_error)
        })
        .await?;
    Ok(Json(NodeChatRecoverResponse {
        request: body,
        receipt: receipt.as_ref().map(project_receipt),
    }))
}

fn validate_send(body: &NodeChatSendRequest) -> Result<(), ApiError> {
    if body.request.query.workspace_id != body.scope.workspace_id
        || body.sender_leadership_revision <= 0
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

async fn source(
    state: &BackendState,
    scope: &crate::wire::WorkspaceQueryScope,
    query: &ChatQuery,
) -> Result<Option<heterarchy_orchestration_runtime::chat::ChatSourceObservation>, ApiError> {
    let authority = state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    let node = query.node_scope.ok_or(ApiError::InvalidIdentity)?;
    let permit = state
        .forge_query_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    authority
        .node_assignment_observation(scope.clone(), node, permit)
        .await
        .map_err(|error| match error {
            crate::CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
            crate::CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
            crate::CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
        })?
        .map(|value| {
            value
                .chat_evidence()
                .map_err(|_| ApiError::WorkspaceScopeUnavailable)
        })
        .transpose()
}

fn now() -> Result<TimestampMillis, ApiError> {
    TimestampMillis::new(crate::app_support::now_millis().map_err(|_| ApiError::Unavailable)?)
        .map_err(|_| ApiError::Unavailable)
}

// Keep recovery failures distinct from definite append rejection.
fn chat_runtime_error(error: ChatError) -> RuntimeError {
    RuntimeError::Chat(Box::new(error))
}
