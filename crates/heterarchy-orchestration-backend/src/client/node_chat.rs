//! Caller-scoped Node-Chat queries, sends, and exact-intent recovery.

use heterarchy_orchestration_operations::{
    MAX_MESSAGE_BODY_BYTES, MAX_PROVIDER_ID_BYTES, MAX_PROVIDER_MESSAGE_JSON_BYTES,
};
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::node_chat::{
    ChatEntryWire, ChatMessageRequest, ChatQueryWire, ChatReceiptWire, ChatSnapshotWire,
    ChatTargetFenceWire, NodeChatQueryRequest, NodeChatQueryResponse, NodeChatRecoverResponse,
    NodeChatSendRequest, NodeChatSendResponse, NodeChatTargetWire,
};
use crate::wire::node_team::NodeTeamScopeWire;
use crate::wire::workspace_project::valid_provider_project_reference;

use super::membership::validate_scope;
use super::{
    ClientError, MAX_REVIEW_RESPONSE_BYTES, OrchestrationHttpClient, ReadbackTarget,
    require_identity,
};

const MAX_NODE_CHAT_MESSAGES: usize = 64;
const MAX_NODE_CHAT_TARGETS: usize = 8;
const MAX_PROFILE_NAME_BYTES: usize = 128;
const MAX_CHAT_ACTOR_LABEL_BYTES: usize = 128;
const MAX_CHAT_UNAVAILABLE_REASON_BYTES: usize = 512;

impl OrchestrationHttpClient {
    /// Read the native 64-message Node Chat page and its bounded target set.
    ///
    /// The response limit matches the already-bounded review projection limit:
    /// a full page of escaped 16 KiB messages can exceed the ordinary response
    /// bound while remaining below that established 8 MiB limit.
    ///
    /// # Errors
    /// Rejects invalid exact scopes, malformed or foreign snapshots, typed
    /// service rejections, and bounded transport failures.
    pub async fn node_chat_read(
        &self,
        authorization: &HeaderValue,
        request: &NodeChatQueryRequest,
    ) -> Result<NodeChatQueryResponse, ClientError> {
        validate_query_request(request)?;
        let response: NodeChatQueryResponse = self
            .call_with_response_limit(
                authorization,
                Method::POST,
                node_chat_path(request.scope.workspace_id, "query"),
                Some(request),
                None,
                MAX_REVIEW_RESPONSE_BYTES,
            )
            .await?;
        if !valid_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Send one exact Node Chat intent without retrying an uncertain exchange.
    ///
    /// A lost or malformed success response retains the complete caller intent
    /// for a later explicit recovery read.
    ///
    /// # Errors
    /// Rejects invalid scopes, fences, or message text; returns a typed service
    /// rejection or an unknown outcome carrying the complete original request.
    pub async fn node_chat_send(
        &self,
        authorization: &HeaderValue,
        request: &NodeChatSendRequest,
    ) -> Result<NodeChatSendResponse, ClientError> {
        validate_send_request(request)?;
        let readback = ReadbackTarget::NodeChat(Box::new(request.clone()));
        let response: NodeChatSendResponse = self
            .call(
                authorization,
                Method::POST,
                node_chat_path(request.scope.workspace_id, "send"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_send_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the durable result of the exact original Node Chat intent.
    ///
    /// An absent receipt remains `None`; this read never authorizes a retry.
    /// It does not need a currently resolvable Node destination.
    ///
    /// # Errors
    /// Rejects invalid intent, malformed recovery projections, typed service
    /// rejections, and bounded query transport failures.
    pub async fn node_chat_recover(
        &self,
        authorization: &HeaderValue,
        request: &NodeChatSendRequest,
    ) -> Result<NodeChatRecoverResponse, ClientError> {
        validate_send_request(request)?;
        let response: NodeChatRecoverResponse = self
            .call(
                authorization,
                Method::POST,
                node_chat_path(request.scope.workspace_id, "recover"),
                Some(request),
                None,
            )
            .await?;
        if !valid_recover_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn node_chat_path(workspace_id: Uuid, endpoint: &str) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/node-chat/{endpoint}")
}

fn validate_query_request(request: &NodeChatQueryRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.sender_activity_id)?;
    validate_query(&request.query, request.scope.workspace_id, true)
}

fn validate_send_request(request: &NodeChatSendRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.sender_activity_id)?;
    if request.sender_leadership_revision <= 0 {
        return Err(ClientError::InvalidRequest);
    }
    validate_query(&request.request.query, request.scope.workspace_id, false)?;
    validate_message_request(&request.request)
}

fn validate_query(
    query: &ChatQueryWire,
    expected_workspace_id: Uuid,
    allow_before_sequence: bool,
) -> Result<(), ClientError> {
    if query.workspace_id != expected_workspace_id
        || query.agent_profile_id.is_some()
        || query.node_scope.is_none()
        || query
            .runtime_session_id
            .is_some_and(|session_id| session_id.is_nil())
        || query
            .before_sequence
            .is_some_and(|sequence| sequence == 0 || i64::try_from(sequence).is_err())
        || (!allow_before_sequence && query.before_sequence.is_some())
    {
        return Err(ClientError::InvalidRequest);
    }
    let node_scope = query.node_scope.ok_or(ClientError::InvalidRequest)?;
    if !valid_node_scope(node_scope) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_node_scope(scope: NodeTeamScopeWire) -> bool {
    !scope.activity_id.is_nil()
        && !scope.goal_id.is_nil()
        && scope.goal_created_at_ms >= 0
        && scope.dag_revision > 0
        && scope.dag_base_goal_revision > 0
        && !scope.node_id.is_nil()
}

fn validate_message_request(request: &ChatMessageRequest) -> Result<(), ClientError> {
    require_identity(request.message_id)?;
    if request.text.trim().is_empty()
        || request.text.len() > MAX_MESSAGE_BODY_BYTES
        || serde_json::to_vec(&request.text)
            .map_err(|_| ClientError::InvalidRequest)?
            .len()
            > MAX_PROVIDER_MESSAGE_JSON_BYTES
        || !valid_target_fence(&request.target_fence)
        || request
            .query
            .runtime_session_id
            .is_some_and(|runtime| runtime != request.target_fence.runtime_session_id)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_target_fence(fence: &ChatTargetFenceWire) -> bool {
    if fence.operation_id.is_nil()
        || fence.profile_id.is_nil()
        || !bounded_nonempty(&fence.profile_name, MAX_PROFILE_NAME_BYTES)
        || fence.participation_id.is_nil()
        || fence.participation_revision == 0
        || fence.runtime_session_id.is_nil()
        || fence.runtime_revision <= 0
        || !bounded_nonempty(&fence.provider_conversation_id, MAX_PROVIDER_ID_BYTES)
        || fence
            .project
            .as_ref()
            .is_some_and(|project| !valid_provider_project_reference(project))
    {
        return false;
    }

    match &fence.node_target {
        Some(NodeChatTargetWire::Agent {
            assignment_id,
            assignment_revision,
            agent_id,
            session_id,
        }) => {
            !assignment_id.is_nil()
                && *assignment_revision > 0
                && !agent_id.is_nil()
                && !session_id.is_nil()
                && fence.leadership_revision.is_none()
        }
        Some(NodeChatTargetWire::Team {
            team_id,
            binding_revision,
        }) => {
            !team_id.is_nil()
                && *binding_revision > 0
                && fence
                    .leadership_revision
                    .is_some_and(|revision| revision > 0)
        }
        None => false,
    }
}

fn valid_query_response(response: &NodeChatQueryResponse, request: &NodeChatQueryRequest) -> bool {
    response.request == *request && valid_snapshot(&response.snapshot, request)
}

fn valid_snapshot(snapshot: &ChatSnapshotWire, request: &NodeChatQueryRequest) -> bool {
    let Some(node_scope) = request.query.node_scope else {
        return false;
    };
    if snapshot.workspace_id != request.scope.workspace_id
        || snapshot.agent_profile_id.is_some()
        || snapshot.node_scope != Some(node_scope)
        || snapshot.before_sequence != request.query.before_sequence
        || snapshot.targets.len() > MAX_NODE_CHAT_TARGETS
        || snapshot.messages.len() > MAX_NODE_CHAT_MESSAGES
        || snapshot
            .targets
            .iter()
            .any(|target| !valid_target_fence(target))
        || snapshot
            .target
            .as_ref()
            .is_some_and(|target| !valid_target_fence(target))
        || snapshot
            .unavailable_reason
            .as_ref()
            .is_some_and(|reason| !bounded_nonempty(reason, MAX_CHAT_UNAVAILABLE_REASON_BYTES))
        || snapshot.target.is_some() != snapshot.unavailable_reason.is_none()
    {
        return false;
    }

    // A runtime selector resolves the first exact target; without one, native
    // Workbench selects only an unambiguous singleton target set.
    for (index, target) in snapshot.targets.iter().enumerate() {
        if snapshot.targets[..index]
            .iter()
            .any(|previous| previous.runtime_session_id == target.runtime_session_id)
        {
            return false;
        }
    }
    let selected = match request.query.runtime_session_id {
        Some(runtime_session_id) => snapshot
            .targets
            .iter()
            .find(|target| target.runtime_session_id == runtime_session_id),
        None if snapshot.targets.len() == 1 => snapshot.targets.first(),
        None => None,
    };
    if snapshot.target.as_ref() != selected {
        return false;
    }

    if !snapshot
        .messages
        .iter()
        .all(|message| valid_entry(message, node_scope))
        || !snapshot
            .messages
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
        || request.query.before_sequence.is_some_and(|before| {
            snapshot
                .messages
                .iter()
                .any(|message| message.sequence >= before)
        })
    {
        return false;
    }

    match snapshot.next_before_sequence {
        None => true,
        Some(cursor) => {
            snapshot.messages.len() == MAX_NODE_CHAT_MESSAGES
                && snapshot
                    .messages
                    .first()
                    .is_some_and(|message| message.sequence == cursor)
        }
    }
}

fn valid_entry(entry: &ChatEntryWire, node_scope: NodeTeamScopeWire) -> bool {
    !entry.id.is_nil()
        && bounded_nonempty(&entry.actor_label, MAX_CHAT_ACTOR_LABEL_BYTES)
        && bounded_nonempty(&entry.body, MAX_MESSAGE_BODY_BYTES)
        && entry.activity_id == Some(node_scope.activity_id)
        && entry.goal_id == Some(node_scope.goal_id)
        && entry.goal_created_at_ms == Some(node_scope.goal_created_at_ms)
        && entry.dag_revision == Some(node_scope.dag_revision)
        && entry.dag_base_goal_revision == Some(node_scope.dag_base_goal_revision)
        && entry.node_id == Some(node_scope.node_id)
        && entry.sequence > 0
        && entry.submitted_at_ms >= 0
}

fn valid_send_response(response: &NodeChatSendResponse, request: &NodeChatSendRequest) -> bool {
    response.request == *request && valid_receipt(&response.receipt, request.request.message_id)
}

fn valid_recover_response(
    response: &NodeChatRecoverResponse,
    request: &NodeChatSendRequest,
) -> bool {
    response.request == *request
        && response
            .receipt
            .as_ref()
            .is_none_or(|receipt| valid_receipt(receipt, request.request.message_id))
}

fn valid_receipt(receipt: &ChatReceiptWire, expected_message_id: Uuid) -> bool {
    // The closed enum already enforces every native Provider state during
    // deserialization; replay may legitimately expose any later state.
    receipt.message_id == expected_message_id
}

fn bounded_nonempty(value: &str, maximum_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= maximum_bytes
}

#[cfg(test)]
mod tests {
    use hyper::header::HeaderValue;
    use serde_json::json;

    use super::*;
    use crate::wire::node_chat::{ChatDeliveryWire, ChatRoleWire, NodeChatTargetWire};
    use crate::wire::{WorkspaceQueryScope, node_chat::ChatProviderStateWire};

    const CREDENTIAL: &str = "Bearer node-chat-fixture-secret";

    fn scope() -> NodeTeamScopeWire {
        NodeTeamScopeWire {
            activity_id: Uuid::from_u128(3),
            goal_id: Uuid::from_u128(4),
            goal_created_at_ms: 5,
            dag_revision: 6,
            dag_base_goal_revision: 7,
            node_id: Uuid::from_u128(8),
        }
    }

    fn target_fence() -> ChatTargetFenceWire {
        ChatTargetFenceWire {
            operation_id: Uuid::from_u128(9),
            profile_id: Uuid::from_u128(10),
            profile_name: "profile".to_owned(),
            participation_id: Uuid::from_u128(11),
            participation_revision: 12,
            runtime_session_id: Uuid::from_u128(13),
            runtime_revision: 14,
            provider_conversation_id: "conversation".to_owned(),
            project: None,
            leadership_revision: None,
            node_target: Some(NodeChatTargetWire::Agent {
                assignment_id: Uuid::from_u128(15),
                assignment_revision: 16,
                agent_id: Uuid::from_u128(17),
                session_id: Uuid::from_u128(18),
            }),
        }
    }

    fn query(before_sequence: Option<u64>) -> ChatQueryWire {
        ChatQueryWire {
            workspace_id: Uuid::from_u128(1),
            agent_profile_id: None,
            runtime_session_id: Some(Uuid::from_u128(13)),
            node_scope: Some(scope()),
            before_sequence,
        }
    }

    fn query_request(before_sequence: Option<u64>) -> NodeChatQueryRequest {
        NodeChatQueryRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            session_id: Uuid::from_u128(19),
            // Sender Activity is independent of the historical Node Activity.
            sender_activity_id: Uuid::from_u128(20),
            query: query(before_sequence),
        }
    }

    fn send_request() -> NodeChatSendRequest {
        let query = query(None);
        NodeChatSendRequest {
            scope: WorkspaceQueryScope {
                workspace_id: query.workspace_id,
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            session_id: Uuid::from_u128(19),
            sender_activity_id: Uuid::from_u128(20),
            sender_leadership_revision: 21,
            request: ChatMessageRequest {
                query,
                message_id: Uuid::from_u128(22),
                text: "hello".to_owned(),
                target_fence: target_fence(),
            },
        }
    }

    fn entry(node_scope: NodeTeamScopeWire, sequence: u64, body: String) -> ChatEntryWire {
        ChatEntryWire {
            id: Uuid::from_u128(u128::from(sequence) + 100),
            role: ChatRoleWire::User,
            actor_label: "Operator".to_owned(),
            body,
            delivery: ChatDeliveryWire::Pending,
            activity_id: Some(node_scope.activity_id),
            goal_id: Some(node_scope.goal_id),
            goal_created_at_ms: Some(node_scope.goal_created_at_ms),
            dag_revision: Some(node_scope.dag_revision),
            dag_base_goal_revision: Some(node_scope.dag_base_goal_revision),
            node_id: Some(node_scope.node_id),
            sequence,
            submitted_at_ms: i64::try_from(sequence).unwrap_or(1),
            provider_state: Some(ChatProviderStateWire::Queued),
        }
    }

    fn snapshot_for(
        request: &NodeChatQueryRequest,
        messages: Vec<ChatEntryWire>,
        next_before_sequence: Option<u64>,
    ) -> ChatSnapshotWire {
        let fence = target_fence();
        ChatSnapshotWire {
            workspace_id: request.scope.workspace_id,
            agent_profile_id: None,
            targets: vec![fence.clone()],
            target: Some(fence),
            messages,
            node_scope: request.query.node_scope,
            before_sequence: request.query.before_sequence,
            next_before_sequence,
            unavailable_reason: None,
        }
    }

    #[test]
    fn exact_scope_validation_keeps_sender_activity_independent_and_rejects_non_node_queries() {
        let request = query_request(None);
        assert_ne!(request.sender_activity_id, scope().activity_id);
        assert!(validate_query_request(&request).is_ok());

        let mut invalid = request.clone();
        invalid.query.agent_profile_id = Some(Uuid::from_u128(23));
        assert_eq!(
            validate_query_request(&invalid),
            Err(ClientError::InvalidRequest)
        );

        let mut invalid = request;
        invalid.query.before_sequence = Some(0);
        assert_eq!(
            validate_query_request(&invalid),
            Err(ClientError::InvalidRequest)
        );
    }

    #[test]
    fn send_validation_binds_runtime_and_target_role_and_uses_native_text_limits() {
        let request = send_request();
        assert!(validate_send_request(&request).is_ok());

        let mut invalid = request.clone();
        invalid.request.target_fence.runtime_session_id = Uuid::from_u128(24);
        assert_eq!(
            validate_send_request(&invalid),
            Err(ClientError::InvalidRequest)
        );

        let mut team = request.clone();
        team.request.target_fence.node_target = Some(NodeChatTargetWire::Team {
            team_id: Uuid::from_u128(25),
            binding_revision: 26,
        });
        assert_eq!(
            validate_send_request(&team),
            Err(ClientError::InvalidRequest)
        );
        team.request.target_fence.leadership_revision = Some(27);
        assert!(validate_send_request(&team).is_ok());

        let mut blank = request.clone();
        blank.request.text = " \n\t".to_owned();
        assert_eq!(
            validate_send_request(&blank),
            Err(ClientError::InvalidRequest)
        );

        let mut encoded_over_limit = request;
        // The body is below 16 KiB, but its existing serde JSON encoding is
        // above the Provider payload owner limit because controls are escaped.
        encoded_over_limit.request.text = "\0".repeat(MAX_PROVIDER_MESSAGE_JSON_BYTES / 6 + 1);
        assert!(encoded_over_limit.request.text.len() <= MAX_MESSAGE_BODY_BYTES);
        assert_eq!(
            validate_send_request(&encoded_over_limit),
            Err(ClientError::InvalidRequest)
        );
    }

    #[test]
    fn snapshot_validation_preserves_exact_context_and_native_oldest_cursor() {
        let request = query_request(Some(1_000));
        let messages = (100_u64..164)
            .map(|sequence| entry(scope(), sequence, "x".to_owned()))
            .collect();
        let snapshot = snapshot_for(&request, messages, Some(100));
        assert!(valid_snapshot(&snapshot, &request));

        let mut invalid = snapshot.clone();
        invalid.next_before_sequence = Some(101);
        assert!(!valid_snapshot(&invalid, &request));

        let mut invalid = snapshot.clone();
        invalid.messages[0].activity_id = Some(Uuid::from_u128(28));
        assert!(!valid_snapshot(&invalid, &request));

        let mut invalid = snapshot;
        invalid.messages.swap(0, 1);
        assert!(!valid_snapshot(&invalid, &request));
    }

    #[tokio::test]
    async fn malformed_send_success_keeps_full_intent_for_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = send_request();
        let payload = json!({
            "request": request.clone(),
            "receipt": {
                "messageId": Uuid::from_u128(29),
                "providerState": "queued"
            }
        });
        let body = payload.to_string();
        let reply = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (client, server) = super::super::tests::fixture(Some(reply)).await?;
        let result = client
            .node_chat_send(&HeaderValue::from_static(CREDENTIAL), &request)
            .await;
        assert_eq!(
            result.err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::NodeChat(Box::new(request.clone()))
            })
        );
        let captured = server.await??;
        let captured = String::from_utf8(captured)?;
        assert!(captured.starts_with(
            "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000001/node-chat/send HTTP/1.1"
        ));
        assert!(captured.contains("authorization: Bearer node-chat-fixture-secret"));
        Ok(())
    }

    #[tokio::test]
    async fn full_native_chat_page_can_exceed_ordinary_response_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = query_request(None);
        let messages = (1_u64..=64)
            .map(|sequence| entry(scope(), sequence, "x".repeat(5_000)))
            .collect();
        let response = NodeChatQueryResponse {
            request: request.clone(),
            snapshot: snapshot_for(&request, messages, None),
        };
        let body = serde_json::to_string(&response)?;
        assert!(body.len() > 256 * 1024);
        let reply = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (client, server) = super::super::tests::fixture(Some(reply)).await?;
        let decoded = client
            .node_chat_read(&HeaderValue::from_static(CREDENTIAL), &request)
            .await?;
        assert_eq!(decoded.snapshot.messages.len(), MAX_NODE_CHAT_MESSAGES);
        let _captured = server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn recovery_preserves_absence_and_accepts_a_later_provider_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = send_request();
        for receipt in [
            serde_json::Value::Null,
            json!({
                "messageId": request.request.message_id,
                "providerState": "delivered"
            }),
        ] {
            let payload = json!({
                "request": request.clone(),
                "receipt": receipt.clone()
            });
            let body = payload.to_string();
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let (client, server) = super::super::tests::fixture(Some(reply)).await?;
            let decoded = client
                .node_chat_recover(&HeaderValue::from_static(CREDENTIAL), &request)
                .await?;
            assert_eq!(decoded.request, request);
            assert_eq!(decoded.receipt.is_some(), !receipt.is_null());
            let captured = server.await??;
            let captured = String::from_utf8(captured)?;
            assert!(captured.starts_with(
                "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000001/node-chat/recover HTTP/1.1"
            ));
        }
        Ok(())
    }
}
