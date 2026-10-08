//! Typed Node-Chat HTTP contracts shared by the backend and official client.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::{WorkspaceQueryScope, node_team::NodeTeamScopeWire};

/// Exact caller-owned Workspace/Session selector and Node Chat query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeChatQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub query: ChatQueryWire,
}

/// Native Workbench Chat query expressed with typed backend identities.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChatQueryWire {
    pub workspace_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub agent_profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub runtime_session_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub node_scope: Option<NodeTeamScopeWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub before_sequence: Option<u64>,
}

/// Exact caller-owned Chat intent. The query and target fence are frozen for
/// both send and recovery so recovery never substitutes current provider state.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeChatSendRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub sender_leadership_revision: i64,
    pub request: ChatMessageRequest,
}

/// One immutable Chat message intent and the exact selected Node destination.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChatMessageRequest {
    pub query: ChatQueryWire,
    pub message_id: Uuid,
    pub text: String,
    pub target_fence: ChatTargetFenceWire,
}

/// Native Workbench Chat destination observation, retained as a send fence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChatTargetFenceWire {
    pub operation_id: Uuid,
    pub profile_id: Uuid,
    pub profile_name: String,
    pub participation_id: Uuid,
    pub participation_revision: u64,
    pub runtime_session_id: Uuid,
    pub runtime_revision: i64,
    pub provider_conversation_id: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub leadership_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub node_target: Option<NodeChatTargetWire>,
}

/// Exact Agent assignment or Team binding named by a Node Chat target fence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum NodeChatTargetWire {
    Agent {
        assignment_id: Uuid,
        assignment_revision: i64,
        agent_id: Uuid,
        session_id: Uuid,
    },
    Team {
        team_id: Uuid,
        binding_revision: i64,
    },
}

/// Exact query echo and native Node Chat snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeChatQueryResponse {
    pub request: NodeChatQueryRequest,
    pub snapshot: ChatSnapshotWire,
}

/// Native Workbench Chat snapshot for the exact requested Node scope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChatSnapshotWire {
    pub workspace_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub agent_profile_id: Option<Uuid>,
    pub targets: Vec<ChatTargetFenceWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target: Option<ChatTargetFenceWire>,
    pub messages: Vec<ChatEntryWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub node_scope: Option<NodeTeamScopeWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub before_sequence: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_before_sequence: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub unavailable_reason: Option<String>,
}

/// One flattened native Workbench Chat message and Operations sequence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChatEntryWire {
    pub id: Uuid,
    pub role: ChatRoleWire,
    pub actor_label: String,
    pub body: String,
    pub delivery: ChatDeliveryWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub activity_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub goal_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub goal_created_at_ms: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dag_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dag_base_goal_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub node_id: Option<Uuid>,
    pub sequence: u64,
    pub submitted_at_ms: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub provider_state: Option<ChatProviderStateWire>,
}

/// Closed native message role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatRoleWire {
    User,
    Assistant,
    System,
}

/// Closed native delivery projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatDeliveryWire {
    Confirmed,
    Pending,
    Unknown,
    Failed,
}

/// Closed Provider outbox state returned by native Chat read/send/recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatProviderStateWire {
    Queued,
    EffectPending,
    Delivered,
    Failed,
    Unknown,
}

/// Exact send-intent echo and resulting durable receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeChatSendResponse {
    pub request: NodeChatSendRequest,
    pub receipt: ChatReceiptWire,
}

/// Read-only recovery result for the original full send intent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeChatRecoverResponse {
    pub request: NodeChatSendRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<ChatReceiptWire>,
}

/// Durable message identity and current Provider state.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChatReceiptWire {
    pub message_id: Uuid,
    pub provider_state: ChatProviderStateWire,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn node_scope() -> NodeTeamScopeWire {
        NodeTeamScopeWire {
            activity_id: Uuid::from_u128(1),
            goal_id: Uuid::from_u128(2),
            goal_created_at_ms: 3,
            dag_revision: 4,
            dag_base_goal_revision: 5,
            node_id: Uuid::from_u128(6),
        }
    }

    fn query() -> ChatQueryWire {
        ChatQueryWire {
            workspace_id: Uuid::from_u128(7),
            agent_profile_id: None,
            runtime_session_id: None,
            node_scope: Some(node_scope()),
            before_sequence: None,
        }
    }

    fn target_fence() -> ChatTargetFenceWire {
        ChatTargetFenceWire {
            operation_id: Uuid::from_u128(8),
            profile_id: Uuid::from_u128(9),
            profile_name: "agent-profile".to_owned(),
            participation_id: Uuid::from_u128(10),
            participation_revision: 11,
            runtime_session_id: Uuid::from_u128(12),
            runtime_revision: 13,
            provider_conversation_id: "conversation".to_owned(),
            leadership_revision: None,
            node_target: Some(NodeChatTargetWire::Agent {
                assignment_id: Uuid::from_u128(14),
                assignment_revision: 15,
                agent_id: Uuid::from_u128(16),
                session_id: Uuid::from_u128(17),
            }),
        }
    }

    fn request() -> NodeChatSendRequest {
        NodeChatSendRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(7),
                location_id: Uuid::from_u128(18),
                location_revision: 19,
                workspace_revision: 20,
                locator: "workspace".to_owned(),
            },
            session_id: Uuid::from_u128(21),
            sender_activity_id: Uuid::from_u128(22),
            sender_leadership_revision: 23,
            request: ChatMessageRequest {
                query: query(),
                message_id: Uuid::from_u128(24),
                text: "hello".to_owned(),
                target_fence: target_fence(),
            },
        }
    }

    fn entry() -> ChatEntryWire {
        ChatEntryWire {
            id: Uuid::from_u128(24),
            role: ChatRoleWire::User,
            actor_label: "Operator".to_owned(),
            body: "hello".to_owned(),
            delivery: ChatDeliveryWire::Pending,
            activity_id: Some(Uuid::from_u128(1)),
            goal_id: Some(Uuid::from_u128(2)),
            goal_created_at_ms: Some(3),
            dag_revision: Some(4),
            dag_base_goal_revision: Some(5),
            node_id: Some(Uuid::from_u128(6)),
            sequence: 25,
            submitted_at_ms: 26,
            provider_state: Some(ChatProviderStateWire::Queued),
        }
    }

    fn snapshot() -> ChatSnapshotWire {
        let fence = target_fence();
        ChatSnapshotWire {
            workspace_id: Uuid::from_u128(7),
            agent_profile_id: None,
            targets: vec![fence.clone()],
            target: Some(fence),
            messages: vec![entry()],
            node_scope: Some(node_scope()),
            before_sequence: None,
            next_before_sequence: None,
            unavailable_reason: None,
        }
    }

    fn without_key(mut value: Value, key: &str) -> Value {
        let removed = value.as_object_mut().and_then(|object| object.remove(key));
        assert!(removed.is_some(), "fixture must contain the omitted field");
        value
    }

    #[test]
    fn chat_wire_round_trips_native_camel_case_and_flattened_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let encoded = serde_json::to_value(&request)?;
        assert_eq!(encoded["request"]["query"]["agentProfileId"], Value::Null);
        assert_eq!(encoded["request"]["query"]["runtimeSessionId"], Value::Null);
        assert_eq!(encoded["request"]["query"]["beforeSequence"], Value::Null);
        assert_eq!(
            encoded["request"]["targetFence"]["nodeTarget"]["kind"],
            "agent"
        );
        assert_eq!(
            encoded["request"]["targetFence"]["nodeTarget"]["assignmentRevision"],
            15
        );
        assert_eq!(
            serde_json::from_value::<NodeChatSendRequest>(encoded)?,
            request
        );

        let response = NodeChatQueryResponse {
            request: NodeChatQueryRequest {
                scope: request.scope,
                session_id: request.session_id,
                sender_activity_id: request.sender_activity_id,
                query: query(),
            },
            snapshot: snapshot(),
        };
        let encoded = serde_json::to_value(&response)?;
        let message = &encoded["snapshot"]["messages"][0];
        assert!(message.get("message").is_none());
        assert_eq!(message["role"], "user");
        assert_eq!(message["delivery"], "pending");
        assert_eq!(message["providerState"], "queued");
        assert_eq!(message["activityId"], Uuid::from_u128(1).to_string());
        assert_eq!(
            serde_json::from_value::<NodeChatQueryResponse>(encoded)?,
            response
        );
        Ok(())
    }

    #[test]
    fn nullable_contract_fields_must_be_present_and_unknown_fields_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = query();
        let query_value = serde_json::to_value(&query)?;
        for field in [
            "agentProfileId",
            "runtimeSessionId",
            "nodeScope",
            "beforeSequence",
        ] {
            assert!(
                serde_json::from_value::<ChatQueryWire>(without_key(query_value.clone(), field))
                    .is_err()
            );
        }

        let fence_value = serde_json::to_value(target_fence())?;
        for field in ["leadershipRevision", "nodeTarget"] {
            assert!(
                serde_json::from_value::<ChatTargetFenceWire>(without_key(
                    fence_value.clone(),
                    field
                ))
                .is_err()
            );
        }

        let snapshot_value = serde_json::to_value(snapshot())?;
        for field in [
            "agentProfileId",
            "target",
            "nodeScope",
            "beforeSequence",
            "nextBeforeSequence",
            "unavailableReason",
        ] {
            assert!(
                serde_json::from_value::<ChatSnapshotWire>(without_key(
                    snapshot_value.clone(),
                    field
                ))
                .is_err()
            );
        }

        let entry_value = serde_json::to_value(entry())?;
        for field in [
            "activityId",
            "goalId",
            "goalCreatedAtMs",
            "dagRevision",
            "dagBaseGoalRevision",
            "nodeId",
            "providerState",
        ] {
            assert!(
                serde_json::from_value::<ChatEntryWire>(without_key(entry_value.clone(), field))
                    .is_err()
            );
        }

        let receipt = json!({
            "messageId": Uuid::from_u128(24),
            "providerState": "effect_pending",
            "unexpected": true
        });
        assert!(serde_json::from_value::<ChatReceiptWire>(receipt).is_err());
        let mut unknown_query = query_value;
        unknown_query["unexpected"] = json!(true);
        assert!(serde_json::from_value::<ChatQueryWire>(unknown_query).is_err());
        Ok(())
    }

    #[test]
    fn recovery_receipt_null_is_required_and_target_enum_is_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let response = NodeChatRecoverResponse {
            request,
            receipt: None,
        };
        let encoded = serde_json::to_value(&response)?;
        assert_eq!(encoded["receipt"], Value::Null);
        assert_eq!(
            serde_json::from_value::<NodeChatRecoverResponse>(encoded.clone())?,
            response
        );
        assert!(
            serde_json::from_value::<NodeChatRecoverResponse>(without_key(encoded, "receipt"))
                .is_err()
        );

        let unknown_target = json!({
            "kind": "agent",
            "assignmentId": Uuid::from_u128(1),
            "assignmentRevision": 2,
            "agentId": Uuid::from_u128(3),
            "sessionId": Uuid::from_u128(4),
            "extra": true
        });
        assert!(serde_json::from_value::<NodeChatTargetWire>(unknown_target).is_err());
        assert!(serde_json::from_value::<ChatProviderStateWire>(json!("sending")).is_err());
        Ok(())
    }
}
