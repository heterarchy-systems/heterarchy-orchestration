//! Official no-retry client for Profile-direct Workspace Chat.

use hyper::Method;
use hyper::header::HeaderValue;

use crate::wire::workspace_chat::{
    WorkspaceChatQueryRequest, WorkspaceChatQueryResponse, WorkspaceChatQueryWire,
    WorkspaceChatRecoverResponse, WorkspaceChatSendRequest, WorkspaceChatSendResponse,
    WorkspaceChatSnapshotWire, WorkspaceChatTargetFenceWire,
};
use crate::wire::workspace_project::valid_provider_project_reference;

use super::membership::validate_scope;
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_TARGETS: usize = 8;
const MAX_MESSAGES: usize = 64;

impl OrchestrationHttpClient {
    /// Read one exact Profile-direct route and bounded direct history.
    ///
    /// # Errors
    /// Rejects malformed requests, typed service failures and foreign or
    /// malformed projections.
    pub async fn workspace_chat_read(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceChatQueryRequest,
    ) -> Result<WorkspaceChatQueryResponse, ClientError> {
        validate_query_request(request)?;
        let response: WorkspaceChatQueryResponse = self
            .call(
                authorization,
                Method::POST,
                path(request.scope.workspace_id, "query"),
                Some(request),
                None,
            )
            .await?;
        if response.request != *request || !valid_snapshot(&response.snapshot, &request.query) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Append and enqueue one immutable Profile-direct message without retry.
    ///
    /// # Errors
    /// Rejects malformed requests and service failures. Transport uncertainty
    /// retains the complete original request for pure recovery.
    pub async fn workspace_chat_send(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceChatSendRequest,
    ) -> Result<WorkspaceChatSendResponse, ClientError> {
        validate_send_request(request)?;
        let readback = ReadbackTarget::WorkspaceChat(Box::new(request.clone()));
        let response: WorkspaceChatSendResponse = self
            .call(
                authorization,
                Method::POST,
                path(request.scope.workspace_id, "send"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.request != *request || response.receipt.message_id != request.request.message_id
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Recover one exact direct message without resolving the current route.
    ///
    /// # Errors
    /// Rejects malformed requests, typed service failures and foreign or
    /// malformed immutable receipts.
    pub async fn workspace_chat_recover(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceChatSendRequest,
    ) -> Result<WorkspaceChatRecoverResponse, ClientError> {
        validate_send_request(request)?;
        let response: WorkspaceChatRecoverResponse = self
            .call(
                authorization,
                Method::POST,
                path(request.scope.workspace_id, "recover"),
                Some(request),
                None,
            )
            .await?;
        if response.request != *request
            || response
                .receipt
                .as_ref()
                .is_some_and(|receipt| receipt.message_id != request.request.message_id)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn path(workspace_id: uuid::Uuid, endpoint: &str) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/workspace-chat/{endpoint}")
}

fn validate_query_request(request: &WorkspaceChatQueryRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.sender_activity_id)?;
    if request.query.workspace_id != request.scope.workspace_id {
        return Err(ClientError::InvalidRequest);
    }
    validate_query(&request.query)
}

fn validate_send_request(request: &WorkspaceChatSendRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.sender_activity_id)?;
    if request.request.query.workspace_id != request.scope.workspace_id
        || request.request.message_id.is_nil()
        || request.request.query.before_sequence.is_some()
        || request.request.text.trim().is_empty()
    {
        return Err(ClientError::InvalidRequest);
    }
    validate_query(&request.request.query)?;
    validate_fence(
        &request.request.target_fence,
        request.request.query.agent_profile_id,
    )
}

fn validate_query(query: &WorkspaceChatQueryWire) -> Result<(), ClientError> {
    require_identity(query.workspace_id)?;
    require_identity(query.agent_profile_id)?;
    if query.runtime_session_id.is_some_and(|id| id.is_nil()) || query.before_sequence == Some(0) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_fence(
    fence: &WorkspaceChatTargetFenceWire,
    profile_id: uuid::Uuid,
) -> Result<(), ClientError> {
    for id in [
        fence.operation_id,
        fence.profile_id,
        fence.participation_id,
        fence.runtime_session_id,
    ] {
        require_identity(id)?;
    }
    if fence.profile_id != profile_id
        || fence.participation_revision == 0
        || fence.runtime_revision <= 0
        || fence.profile_name.trim().is_empty()
        || fence.provider_conversation_id.trim().is_empty()
        || fence
            .project
            .as_ref()
            .is_some_and(|project| !valid_provider_project_reference(project))
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_snapshot(snapshot: &WorkspaceChatSnapshotWire, query: &WorkspaceChatQueryWire) -> bool {
    snapshot.workspace_id == query.workspace_id
        && snapshot.agent_profile_id == query.agent_profile_id
        && snapshot.before_sequence == query.before_sequence
        && snapshot.targets.len() <= MAX_TARGETS
        && snapshot.messages.len() <= MAX_MESSAGES
        && snapshot.targets.iter().all(|fence| {
            validate_fence(fence, query.agent_profile_id).is_ok()
                && query
                    .runtime_session_id
                    .is_none_or(|runtime| fence.runtime_session_id == runtime)
        })
        && snapshot.target.as_ref().is_none_or(|target| {
            snapshot.targets.contains(target)
                && query
                    .runtime_session_id
                    .is_none_or(|runtime| target.runtime_session_id == runtime)
        })
        && snapshot.messages.iter().all(|entry| {
            !entry.id.is_nil()
                && entry.activity_id.is_none()
                && entry.goal_id.is_none()
                && entry.goal_created_at_ms.is_none()
                && entry.dag_revision.is_none()
                && entry.dag_base_goal_revision.is_none()
                && entry.node_id.is_none()
                && entry.sequence > 0
                && entry.submitted_at_ms >= 0
        })
        && snapshot
            .messages
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
}
