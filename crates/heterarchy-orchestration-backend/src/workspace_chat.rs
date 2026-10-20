//! Profile-direct Workspace conversations over the canonical Runtime owner.

use crate::wire::workspace_project::ProviderProjectReferenceWire;
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{AuthenticatedPrincipal, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_operations::{
    MessageAuthor, ProviderMessageSource, ProviderMessageState, WorkspaceProjectReference,
};
use heterarchy_orchestration_runtime::{
    chat::{ChatEntry, ChatError, ChatQuery, ChatSend, ChatSnapshot, ChatTargetFence},
    participation::ParticipationTarget,
};

use crate::{
    ApiError, BackendState, RuntimeError,
    wire::workspace_chat::{
        WorkspaceChatMessageRequest, WorkspaceChatQueryRequest, WorkspaceChatQueryResponse,
        WorkspaceChatQueryWire, WorkspaceChatRecoverResponse, WorkspaceChatSendRequest,
        WorkspaceChatSendResponse, WorkspaceChatSnapshotWire, WorkspaceChatTargetFenceWire,
    },
};

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/workspace-chat/query",
            post(query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/workspace-chat/send",
            post(send),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/workspace-chat/recover",
            post(recover),
        )
}

async fn query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceChatQueryRequest>,
) -> Result<Json<WorkspaceChatQueryResponse>, ApiError> {
    let caller = caller(&path, &body.scope, body.session_id, body.sender_activity_id)?;
    let query = domain_query(&body.query).map_err(ApiError::Runtime)?;
    let owner = state.clone();
    state
        .run_owned(async move {
            owner
                .catalog_job(move |runtime| {
                    runtime
                        .authorize_chat_sender(caller, principal, query.workspace_id)
                        .map_err(chat_runtime_error)
                })
                .await?;
            owner.workspace_scope_proof(body.scope.clone()).await?;
            let now = now()?;
            let snapshot = owner
                .catalog_job(move |runtime| {
                    runtime
                        .read_chat(caller, principal, &query, None, now)
                        .map_err(chat_runtime_error)
                })
                .await?;
            Ok(Json(WorkspaceChatQueryResponse {
                request: body,
                snapshot: project_snapshot(&snapshot)?,
            }))
        })
        .await
}

async fn send(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceChatSendRequest>,
) -> Result<Json<WorkspaceChatSendResponse>, ApiError> {
    let caller = caller(&path, &body.scope, body.session_id, body.sender_activity_id)?;
    let request = domain_send(&body.request).map_err(ApiError::Runtime)?;
    let owner = state.clone();
    state
        .run_owned(async move {
            let frozen = request.clone();
            let existing = owner
                .catalog_job(move |runtime| {
                    runtime
                        .recover_workspace_chat(caller, principal, &frozen)
                        .map_err(chat_runtime_error)
                })
                .await?;
            if let Some(receipt) = existing {
                return Ok(Json(WorkspaceChatSendResponse {
                    request: body,
                    receipt: crate::node_chat::project_receipt(&receipt),
                }));
            }
            owner
                .catalog_job(move |runtime| {
                    runtime
                        .authorize_chat_sender(caller, principal, request.query.workspace_id)
                        .map_err(chat_runtime_error)
                })
                .await?;
            owner.workspace_scope_proof(body.scope.clone()).await?;
            let now = now()?;
            let receipt = owner
                .catalog_job(move |runtime| {
                    runtime
                        .send_workspace_chat(caller, principal, &request, now)
                        .map_err(chat_runtime_error)
                })
                .await?;
            Ok(Json(WorkspaceChatSendResponse {
                request: body,
                receipt: crate::node_chat::project_receipt(&receipt),
            }))
        })
        .await
}

async fn recover(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<WorkspaceChatSendRequest>,
) -> Result<Json<WorkspaceChatRecoverResponse>, ApiError> {
    let caller = caller(&path, &body.scope, body.session_id, body.sender_activity_id)?;
    let request = domain_send(&body.request).map_err(ApiError::Runtime)?;
    let receipt = state
        .run_catalog(move |runtime| {
            runtime
                .recover_workspace_chat(caller, principal, &request)
                .map_err(chat_runtime_error)
        })
        .await?;
    Ok(Json(WorkspaceChatRecoverResponse {
        request: body,
        receipt: receipt.as_ref().map(crate::node_chat::project_receipt),
    }))
}

fn caller(
    path: &str,
    scope: &crate::wire::WorkspaceQueryScope,
    session_id: uuid::Uuid,
    activity_id: uuid::Uuid,
) -> Result<ParticipationTarget, ApiError> {
    crate::app_support::validate_workspace_reference(scope, Some(session_id))?;
    if crate::canonical_uuid(path)? != scope.workspace_id || activity_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(ParticipationTarget {
        workspace_id: scope.workspace_id,
        activity_id,
        agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
            scope.workspace_id,
            activity_id,
            session_id,
        )
        .into_uuid(),
        session_id,
    })
}

fn domain_query(value: &WorkspaceChatQueryWire) -> Result<ChatQuery, RuntimeError> {
    if value.workspace_id.is_nil()
        || value.agent_profile_id.is_nil()
        || value.runtime_session_id.is_some_and(|id| id.is_nil())
        || value.before_sequence == Some(0)
    {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(ChatQuery {
        workspace_id: WorkspaceId::from_uuid(value.workspace_id),
        agent_profile_id: Some(
            heterarchy_orchestration_sessions::AgentProfileId::from_uuid(value.agent_profile_id)
                .map_err(|_| RuntimeError::InvalidInput)?,
        ),
        runtime: value.runtime_session_id,
        node_scope: None,
        before_sequence: value.before_sequence,
    })
}

fn domain_send(value: &WorkspaceChatMessageRequest) -> Result<ChatSend, RuntimeError> {
    let query = domain_query(&value.query)?;
    if value.message_id.is_nil() || query.before_sequence.is_some() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(ChatSend {
        query,
        message_id: value.message_id,
        text: value.text.clone(),
        target_fence: domain_direct_fence(&value.target_fence)?,
    })
}

fn project_snapshot(value: &ChatSnapshot) -> Result<WorkspaceChatSnapshotWire, ApiError> {
    let profile = value
        .query
        .agent_profile_id
        .ok_or(ApiError::Runtime(RuntimeError::Conflict))?;
    Ok(WorkspaceChatSnapshotWire {
        workspace_id: value.query.workspace_id.into_uuid(),
        agent_profile_id: profile.into_uuid(),
        targets: value.targets.iter().map(project_fence).collect(),
        target: value.target.as_ref().map(project_fence),
        messages: value.entries.iter().map(project_entry).collect(),
        before_sequence: value.query.before_sequence,
        next_before_sequence: value.next_before_sequence,
        unavailable_reason: value.unavailable_reason.map(str::to_owned),
    })
}

fn project_fence(
    value: &heterarchy_orchestration_runtime::chat::ChatTargetFence,
) -> WorkspaceChatTargetFenceWire {
    WorkspaceChatTargetFenceWire {
        operation_id: value.operation_id,
        profile_id: value.profile_id,
        profile_name: value.profile_name.clone(),
        participation_id: value.participation_id,
        participation_revision: value.participation_revision,
        runtime_session_id: value.runtime_session_id,
        runtime_revision: value.runtime_revision,
        provider_conversation_id: value.provider_conversation_id.clone(),
        project: value.project.map(ProviderProjectReferenceWire::from),
    }
}

fn domain_direct_fence(
    value: &WorkspaceChatTargetFenceWire,
) -> Result<ChatTargetFence, RuntimeError> {
    let project = value
        .project
        .clone()
        .map(WorkspaceProjectReference::try_from)
        .transpose()
        .map_err(|_| RuntimeError::InvalidInput)?;
    if [
        value.operation_id,
        value.profile_id,
        value.participation_id,
        value.runtime_session_id,
    ]
    .iter()
    .any(uuid::Uuid::is_nil)
        || value.participation_revision == 0
        || value.runtime_revision <= 0
        || value.profile_name.trim().is_empty()
        || value.provider_conversation_id.trim().is_empty()
    {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(ChatTargetFence {
        operation_id: value.operation_id,
        profile_id: value.profile_id,
        profile_name: value.profile_name.clone(),
        participation_id: value.participation_id,
        participation_revision: value.participation_revision,
        runtime_session_id: value.runtime_session_id,
        runtime_revision: value.runtime_revision,
        provider_conversation_id: value.provider_conversation_id.clone(),
        project,
        leadership_revision: None,
        node_target: None,
    })
}

pub(crate) fn project_entry(value: &ChatEntry) -> crate::wire::node_chat::ChatEntryWire {
    use crate::wire::node_chat::{ChatDeliveryWire, ChatEntryWire, ChatRoleWire};
    let message = &value.message;
    let context = message.context();
    let delivery = match value.provider_state {
        Some(ProviderMessageState::Queued | ProviderMessageState::EffectPending) => {
            ChatDeliveryWire::Pending
        }
        Some(ProviderMessageState::Unknown) => ChatDeliveryWire::Unknown,
        Some(ProviderMessageState::Failed) => ChatDeliveryWire::Failed,
        Some(ProviderMessageState::Delivered) | None => ChatDeliveryWire::Confirmed,
    };
    let role = match message.author() {
        MessageAuthor::Operator => ChatRoleWire::User,
        MessageAuthor::System
        | MessageAuthor::Provider {
            source: ProviderMessageSource::Unknown { .. },
        } => ChatRoleWire::System,
        MessageAuthor::Participant { .. }
        | MessageAuthor::NativeParticipant { .. }
        | MessageAuthor::Provider {
            source: ProviderMessageSource::Known { .. },
        } => ChatRoleWire::Assistant,
    };
    ChatEntryWire {
        id: message.message_id(),
        role,
        actor_label: message.author_display_name().to_owned(),
        body: message.body().to_owned(),
        delivery,
        activity_id: context
            .activity_id()
            .map(forge_domain::ActivityId::into_uuid),
        goal_id: context.goal_id().map(forge_domain::GoalId::into_uuid),
        goal_created_at_ms: context
            .goal_created_at()
            .map(forge_domain::TimestampMillis::get),
        dag_revision: context
            .dag_revision()
            .map(forge_domain::DagRevisionNumber::get),
        dag_base_goal_revision: context
            .dag_base_goal_revision()
            .map(forge_domain::GoalRevision::get),
        node_id: context.node_id().map(forge_domain::DagNodeId::into_uuid),
        sequence: message.sequence(),
        submitted_at_ms: message.submitted_at().get(),
        provider_state: value.provider_state.map(provider_state),
    }
}

pub(crate) fn provider_state(
    value: ProviderMessageState,
) -> crate::wire::node_chat::ChatProviderStateWire {
    use crate::wire::node_chat::ChatProviderStateWire;
    match value {
        ProviderMessageState::Queued => ChatProviderStateWire::Queued,
        ProviderMessageState::EffectPending => ChatProviderStateWire::EffectPending,
        ProviderMessageState::Delivered => ChatProviderStateWire::Delivered,
        ProviderMessageState::Failed => ChatProviderStateWire::Failed,
        ProviderMessageState::Unknown => ChatProviderStateWire::Unknown,
    }
}

fn now() -> Result<TimestampMillis, ApiError> {
    TimestampMillis::new(crate::current_timestamp_millis()?).map_err(|_| ApiError::Unavailable)
}

fn chat_runtime_error(error: ChatError) -> RuntimeError {
    RuntimeError::Chat(Box::new(error))
}
