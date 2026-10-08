//! Exact detach and historical departure readback wire contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::{WorkspaceQueryScope, membership::MemberTransportWire};

/// Exact Workspace, Activity, Agent, Session and revision fence for a detach.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DepartureTarget {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub expected_agent_revision: i64,
}

/// Detach one exact attached Agent. Its replay identity is derived from target.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DepartureRequest {
    pub target: DepartureTarget,
}

/// Select one historical departure receipt by its exact target and nonce.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DepartureQueryRequest {
    pub target: DepartureTarget,
    pub request_id: Uuid,
}

/// Immutable detach result committed by Orchestration Operations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DepartureResponse {
    pub target: DepartureTarget,
    pub request_id: Uuid,
    pub receipt: DepartureReceiptWire,
    pub replayed: bool,
}

/// Historical readback. `receipt` is always present, including when absent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DepartureQueryResponse {
    pub target: DepartureTarget,
    pub request_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<DepartureReceiptWire>,
}

/// Closed logical departure kind carried in the Operations receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DepartureKindWire {
    Detach,
    Handoff,
}

/// Full immutable projection of an Operations logical departure receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DepartureReceiptWire {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub participant_id: Uuid,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub kind: DepartureKindWire,
    pub expected_agent_revision: i64,
    pub result_agent_revision: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_participant_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_agent_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_session_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result_coordinator_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub departed_event_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub handoff_started_event_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub handoff_completed_event_id: Option<Uuid>,
    pub membership_revision: u64,
    pub presence_cleared: bool,
    pub source_reconciled_at_ms: i64,
    pub committed_at_ms: i64,
    pub display_name: String,
    pub declared_role: String,
    pub transport: MemberTransportWire,
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
    use super::*;
    use std::error::Error;

    fn target() -> DepartureTarget {
        DepartureTarget {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            agent_id: Uuid::from_u128(6),
            session_id: Uuid::from_u128(7),
            expected_agent_revision: 8,
        }
    }

    fn receipt() -> DepartureReceiptWire {
        DepartureReceiptWire {
            request_id: Uuid::from_u128(9),
            workspace_id: Uuid::from_u128(1),
            participant_id: Uuid::from_u128(10),
            activity_id: Uuid::from_u128(5),
            agent_id: Uuid::from_u128(6),
            session_id: Uuid::from_u128(7),
            kind: DepartureKindWire::Detach,
            expected_agent_revision: 8,
            result_agent_revision: 9,
            target_participant_id: None,
            target_agent_id: None,
            target_session_id: None,
            result_coordinator_revision: None,
            departed_event_id: Some(Uuid::from_u128(11)),
            handoff_started_event_id: None,
            handoff_completed_event_id: None,
            membership_revision: 2,
            presence_cleared: true,
            source_reconciled_at_ms: 12,
            committed_at_ms: 13,
            display_name: "Ada".to_owned(),
            declared_role: "reviewer".to_owned(),
            transport: MemberTransportWire::Mcp,
        }
    }

    #[test]
    fn detach_receipt_round_trip_keeps_every_nullable_field_explicit() -> Result<(), Box<dyn Error>>
    {
        let encoded = serde_json::to_value(receipt())?;
        assert_eq!(encoded["kind"], "detach");
        for field in [
            "targetParticipantId",
            "targetAgentId",
            "targetSessionId",
            "resultCoordinatorRevision",
            "handoffStartedEventId",
            "handoffCompletedEventId",
        ] {
            assert_eq!(encoded.get(field), Some(&serde_json::Value::Null));
        }
        assert_eq!(
            serde_json::from_value::<DepartureReceiptWire>(encoded.clone())?,
            receipt()
        );

        for field in [
            "targetParticipantId",
            "targetAgentId",
            "targetSessionId",
            "resultCoordinatorRevision",
            "departedEventId",
            "handoffStartedEventId",
            "handoffCompletedEventId",
        ] {
            let mut omitted = encoded.clone();
            omitted
                .as_object_mut()
                .ok_or("receipt JSON object")?
                .remove(field);
            assert!(serde_json::from_value::<DepartureReceiptWire>(omitted).is_err());
        }
        Ok(())
    }

    #[test]
    fn query_receipt_requires_an_explicit_null_and_rejects_unknown_fields()
    -> Result<(), Box<dyn Error>> {
        let query = DepartureQueryResponse {
            target: target(),
            request_id: Uuid::from_u128(9),
            receipt: None,
        };
        let encoded = serde_json::to_value(&query)?;
        assert_eq!(encoded["receipt"], serde_json::Value::Null);
        assert_eq!(
            serde_json::from_value::<DepartureQueryResponse>(encoded.clone())?,
            query
        );

        let mut omitted = encoded.clone();
        omitted
            .as_object_mut()
            .ok_or("query JSON object")?
            .remove("receipt");
        assert!(serde_json::from_value::<DepartureQueryResponse>(omitted).is_err());

        let mut unknown = encoded;
        unknown
            .as_object_mut()
            .ok_or("query JSON object")?
            .insert("principal".to_owned(), serde_json::Value::Null);
        assert!(serde_json::from_value::<DepartureQueryResponse>(unknown).is_err());
        Ok(())
    }

    #[test]
    fn departure_kind_accepts_only_supported_departures() {
        assert!(serde_json::from_str::<DepartureKindWire>("\"detach\"").is_ok());
        assert_eq!(
            serde_json::from_str::<DepartureKindWire>("\"handoff\"").ok(),
            Some(DepartureKindWire::Handoff)
        );
        assert!(serde_json::from_str::<DepartureKindWire>("\"unknown\"").is_err());
    }
}
