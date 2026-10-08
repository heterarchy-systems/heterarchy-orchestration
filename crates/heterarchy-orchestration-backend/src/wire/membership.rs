//! Typed HTTP contracts for logical Workspace membership registration.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

/// Exact Workspace, Activity, and caller-owned Session registration target.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberTarget {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
}

/// Declared client transport metadata. This value never grants permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberTransportWire {
    Mcp,
    Stdio,
    StreamableHttp,
    Other,
}

/// Request to prepare or exactly replay the caller's membership registration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberRegisterRequest {
    pub target: MemberTarget,
    pub display_name: String,
    pub declared_role: String,
    pub transport: MemberTransportWire,
}

/// Prepared membership proposal and its full native registration projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberRegisterResponse {
    pub target: MemberTarget,
    pub progress: MemberProgressWire,
}

/// Request to atomically confirm one exact membership proposal and recipients.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberConfirmRequest {
    pub target: MemberTarget,
    pub request_id: Uuid,
    pub recipients: Vec<Uuid>,
}

/// Immutable logical membership commit receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberConfirmResponse {
    pub target: MemberTarget,
    pub commit: MemberCommitWire,
}

/// Request to activate one exactly committed membership binding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberActivateRequest {
    pub target: MemberTarget,
    pub request_id: Uuid,
}

/// Immutable activation receipt accepted from the source binding lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberActivateResponse {
    pub target: MemberTarget,
    pub activation: MemberActivationWire,
}

/// Exact logical registration readback selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberRegistrationQueryRequest {
    pub target: MemberTarget,
    pub request_id: Uuid,
}

/// Full typed readback for one membership registration, including explicit
/// absence for every stage not yet recorded.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberRegistrationQueryResponse {
    pub target: MemberTarget,
    pub request_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub progress: Option<MemberProgressWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub commit: Option<MemberCommitWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub activation: Option<MemberActivationWire>,
}

/// Closed native registration phase vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberPhaseWire {
    Pending,
    Reserved,
    MembershipCommitted,
    Ready,
}

/// Published native fields for a complete registration progress observation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberProgressWire {
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub agent_id: Uuid,
    pub request_id: Uuid,
    pub participant_id: Uuid,
    pub migration_id: Uuid,
    pub phase: MemberPhaseWire,
    pub reserved: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub reserved_at_ms: Option<i64>,
}

/// Published native fields for the immutable logical membership commit.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberCommitWire {
    pub request_id: Uuid,
    pub participant_id: Uuid,
    pub commit_digest: String,
    pub joined_event_id: Uuid,
    pub committed_at_ms: i64,
}

/// Published native fields for one source-witness-backed activation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberActivationWire {
    pub request_id: Uuid,
    pub participant_id: Uuid,
    pub agent_id: Uuid,
    pub membership_digest: String,
    pub activated_at_ms: i64,
    pub phase: MemberPhaseWire,
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
                location_revision: 1,
                workspace_revision: 2,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(3),
            session_id: Uuid::from_u128(4),
        }
    }

    fn progress() -> MemberProgressWire {
        MemberProgressWire {
            workspace_id: Uuid::from_u128(1),
            activity_id: Uuid::from_u128(3),
            session_id: Uuid::from_u128(4),
            agent_id: Uuid::from_u128(5),
            request_id: Uuid::from_u128(6),
            participant_id: Uuid::from_u128(7),
            migration_id: Uuid::from_u128(8),
            phase: MemberPhaseWire::Pending,
            reserved: false,
            reserved_at_ms: None,
        }
    }

    #[test]
    fn member_wire_uses_frozen_camel_case_and_closed_phase_tokens()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_value(MemberRegistrationQueryResponse {
            target: target(),
            request_id: Uuid::from_u128(6),
            progress: Some(progress()),
            commit: None,
            activation: None,
        })?;
        assert_eq!(
            value["target"]["activityId"],
            Uuid::from_u128(3).to_string()
        );
        assert_eq!(value["progress"]["phase"], "pending");
        assert!(value["commit"].is_null());
        assert!(value["activation"].is_null());

        let mut unknown_phase = value.clone();
        unknown_phase["progress"]["phase"] = "future_phase".into();
        assert!(serde_json::from_value::<MemberRegistrationQueryResponse>(unknown_phase).is_err());
        Ok(())
    }

    #[test]
    fn query_and_nested_nullable_fields_are_required_on_the_wire()
    -> Result<(), Box<dyn std::error::Error>> {
        let baseline = serde_json::to_value(MemberRegistrationQueryResponse {
            target: target(),
            request_id: Uuid::from_u128(6),
            progress: Some(progress()),
            commit: None,
            activation: None,
        })?;
        for field in ["progress", "commit", "activation"] {
            let mut missing = baseline.clone();
            let _removed = missing.as_object_mut().ok_or("query object")?.remove(field);
            assert!(serde_json::from_value::<MemberRegistrationQueryResponse>(missing).is_err());
        }

        let mut missing_reserved_at = baseline["progress"].clone();
        let _removed = missing_reserved_at
            .as_object_mut()
            .ok_or("progress object")?
            .remove("reservedAtMs");
        assert!(serde_json::from_value::<MemberProgressWire>(missing_reserved_at).is_err());
        Ok(())
    }

    #[test]
    fn member_transport_tokens_are_closed_snake_case() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(serde_json::to_string(&MemberTransportWire::Mcp)?, "\"mcp\"");
        assert_eq!(
            serde_json::to_string(&MemberTransportWire::StreamableHttp)?,
            "\"streamable_http\""
        );
        assert!(serde_json::from_str::<MemberTransportWire>("\"custom\"").is_err());
        Ok(())
    }
}
