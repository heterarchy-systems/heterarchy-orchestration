//! Caller-scoped join, activation, and readback contracts for Agent requests.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::AgentSessionResponse;
use super::leadership::WorkspaceQueryScope;
use super::membership::{MemberActivationWire, MemberRegistrationQueryResponse, MemberTarget};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestJoinRequest {
    pub operation_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestActivateRequest {
    pub operation_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestParticipationQueryRequest {
    pub operation_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
}

/// Validated operation/profile projection visible to the operation-linked caller.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestParticipationTargetWire {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub agent_request_id: Uuid,
    pub agent_request_state: String,
    pub agent_request_revision: u64,
    pub profile_id: Uuid,
    pub runtime_session_id: Uuid,
}

/// Ready membership produced by one join or activation mutation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestParticipationResponse {
    pub target: MemberTarget,
    pub operation_id: Uuid,
    pub agent_request_id: Uuid,
    pub agent_request_state: String,
    pub agent_request_revision: u64,
    pub profile_id: Uuid,
    pub runtime_session_id: Uuid,
    pub membership: MemberActivationWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_created: Option<bool>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_revision: Option<i64>,
}

/// Session-scoped view of an operation's participation state.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestParticipationQueryResponse {
    pub operation_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session: Option<AgentSessionResponse>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target: Option<AgentRequestParticipationTargetWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub membership: Option<MemberRegistrationQueryResponse>,
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

    fn target() -> MemberTarget {
        MemberTarget {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
        }
    }

    #[test]
    fn request_contracts_use_camel_case_and_close_unknown_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = AgentRequestParticipationQueryRequest {
            operation_id: Uuid::from_u128(7),
            session_id: None,
        };
        let value = serde_json::to_value(&request)?;
        assert_eq!(value["operationId"], Uuid::from_u128(7).to_string());
        assert!(value["sessionId"].is_null());

        let mut unknown = value.clone();
        unknown["future"] = true.into();
        assert!(serde_json::from_value::<AgentRequestParticipationQueryRequest>(unknown).is_err());

        let mut missing_session = value;
        missing_session
            .as_object_mut()
            .ok_or("query request object")?
            .remove("sessionId");
        assert!(
            serde_json::from_value::<AgentRequestParticipationQueryRequest>(missing_session)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn mutation_and_query_nullable_receipts_are_required_on_the_wire()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = AgentRequestParticipationResponse {
            target: target(),
            operation_id: Uuid::from_u128(7),
            agent_request_id: Uuid::from_u128(8),
            agent_request_state: "fulfilled".to_owned(),
            agent_request_revision: 3,
            profile_id: Uuid::from_u128(9),
            runtime_session_id: Uuid::from_u128(10),
            membership: MemberActivationWire {
                request_id: Uuid::from_u128(11),
                participant_id: Uuid::from_u128(12),
                agent_id: Uuid::from_u128(13),
                membership_digest: "ab".repeat(32),
                activated_at_ms: 14,
                phase: super::super::membership::MemberPhaseWire::Ready,
            },
            session_created: None,
            session_revision: None,
        };
        let baseline = serde_json::to_value(&response)?;
        for field in ["sessionCreated", "sessionRevision"] {
            let mut missing = baseline.clone();
            let _removed = missing
                .as_object_mut()
                .ok_or("mutation response object")?
                .remove(field);
            assert!(serde_json::from_value::<AgentRequestParticipationResponse>(missing).is_err());
        }

        let query = AgentRequestParticipationQueryResponse {
            operation_id: Uuid::from_u128(7),
            session: None,
            target: None,
            membership: None,
        };
        let baseline = serde_json::to_value(query)?;
        for field in ["session", "target", "membership"] {
            let mut missing = baseline.clone();
            let _removed = missing
                .as_object_mut()
                .ok_or("query response object")?
                .remove(field);
            assert!(
                serde_json::from_value::<AgentRequestParticipationQueryResponse>(missing).is_err()
            );
        }
        Ok(())
    }
}
