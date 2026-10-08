//! Source-scoped logical presence and inbox contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

/// Exact Workspace, Activity, Agent, and logical Session selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ParticipantTarget {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
}

/// Compare-and-set one logical participant's presence revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PresenceTouchRequest {
    pub target: ParticipantTarget,
    pub expected_presence_revision: i64,
}

/// Read the current presence for one exact participant target.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PresenceQueryRequest {
    pub target: ParticipantTarget,
}

/// Exact participant presence projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceWire {
    pub workspace_id: Uuid,
    pub participant_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub display_name: String,
    pub membership_revision: u64,
    pub presence_revision: i64,
    pub last_authenticated_at_ms: i64,
}

/// Successful compare-and-set receipt for one presence touch.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceTouchResponse {
    pub target: ParticipantTarget,
    pub presence: PresenceWire,
}

/// Exact source target and explicit nullable current presence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceQueryResponse {
    pub target: ParticipantTarget,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub presence: Option<PresenceWire>,
}

/// Read a bounded page of the target participant's logical inbox.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InboxListRequest {
    pub target: ParticipantTarget,
    pub limit: u32,
}

/// One participant identity as retained on an immutable inbox event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxParticipantWire {
    pub participant_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub display_name: String,
}

/// One immutable event together with this recipient's exact receipt state.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxEntryWire {
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    pub event_id: Uuid,
    pub recipient: InboxParticipantWire,
    pub actor: InboxParticipantWire,
    pub membership_revision: u64,
    pub observed_at_ms: i64,
    pub receipt_revision: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub acknowledged_at_ms: Option<i64>,
}

/// Exact source target and bounded inbox page.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxListResponse {
    pub target: ParticipantTarget,
    pub entries: Vec<InboxEntryWire>,
}

/// Compare-and-set one exact event receipt to acknowledged.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InboxAckRequest {
    pub target: ParticipantTarget,
    pub event_id: Uuid,
    pub expected_receipt_revision: i64,
}

/// Read one exact event receipt for the target participant.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InboxQueryRequest {
    pub target: ParticipantTarget,
    pub event_id: Uuid,
}

/// Acknowledgement result with the updated exact event receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxAckResponse {
    pub target: ParticipantTarget,
    pub entry: InboxEntryWire,
}

/// Exact source target and explicit nullable event receipt readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxQueryResponse {
    pub target: ParticipantTarget,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub entry: Option<InboxEntryWire>,
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
    use super::{InboxEntryWire, InboxQueryResponse, PresenceQueryResponse};
    use serde_json::{Value, json};

    fn target() -> Value {
        json!({
            "scope": {
                "workspaceId": "00000000-0000-0000-0000-000000000001",
                "locationId": "00000000-0000-0000-0000-000000000002",
                "locationRevision": 1,
                "workspaceRevision": 1,
                "locator": "workspace"
            },
            "activityId": "00000000-0000-0000-0000-000000000003",
            "agentId": "00000000-0000-0000-0000-000000000004",
            "sessionId": "00000000-0000-0000-0000-000000000005"
        })
    }

    fn entry() -> Value {
        json!({
            "workspaceId": "00000000-0000-0000-0000-000000000001",
            "activityId": "00000000-0000-0000-0000-000000000003",
            "eventId": "00000000-0000-0000-0000-000000000006",
            "recipient": {
                "participantId": "00000000-0000-0000-0000-000000000007",
                "agentId": "00000000-0000-0000-0000-000000000004",
                "sessionId": "00000000-0000-0000-0000-000000000005",
                "displayName": "recipient"
            },
            "actor": {
                "participantId": "00000000-0000-0000-0000-000000000008",
                "agentId": "00000000-0000-0000-0000-000000000009",
                "sessionId": "00000000-0000-0000-0000-00000000000a",
                "displayName": "actor"
            },
            "membershipRevision": 1,
            "observedAtMs": 1,
            "receiptRevision": 1,
            "acknowledgedAtMs": null
        })
    }

    #[test]
    fn nullable_presence_receipt_and_ack_fields_are_required()
    -> Result<(), Box<dyn std::error::Error>> {
        let presence = json!({"target": target(), "presence": null});
        assert!(serde_json::from_value::<PresenceQueryResponse>(presence.clone()).is_ok());
        let mut missing_presence = presence;
        missing_presence
            .as_object_mut()
            .ok_or("presence response must be an object")?
            .remove("presence");
        assert!(serde_json::from_value::<PresenceQueryResponse>(missing_presence).is_err());

        let receipt = entry();
        assert!(serde_json::from_value::<InboxEntryWire>(receipt.clone()).is_ok());
        let mut missing_ack = receipt;
        missing_ack
            .as_object_mut()
            .ok_or("inbox entry must be an object")?
            .remove("acknowledgedAtMs");
        assert!(serde_json::from_value::<InboxEntryWire>(missing_ack).is_err());

        let query = json!({"target": target(), "entry": null});
        assert!(serde_json::from_value::<InboxQueryResponse>(query.clone()).is_ok());
        let mut missing_entry = query;
        missing_entry
            .as_object_mut()
            .ok_or("inbox response must be an object")?
            .remove("entry");
        assert!(serde_json::from_value::<InboxQueryResponse>(missing_entry).is_err());
        Ok(())
    }
}
