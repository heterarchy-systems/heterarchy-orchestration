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

/// Operator request to settle the exact stored binding as terminally rejected.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberBindingRejectRequest {
    pub target: MemberTarget,
    pub request_id: Uuid,
}

/// Exact terminal rejection accepted from the Source binding lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberBindingRejectResponse {
    pub target: MemberTarget,
    pub rejection: MemberBindingRejectionWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberBindingRejectionReconcileRequest {
    pub target: MemberTarget,
    pub request_id: Uuid,
    pub resolution_request_id: Uuid,
    pub expected_attempt_request_digest: String,
    pub expected_legacy_artifact_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberBindingRejectionReconcileResponse {
    pub target: MemberTarget,
    pub request_id: Uuid,
    pub resolution_request_id: Uuid,
    pub attempt_request_digest: String,
    pub legacy_artifact_digest: String,
    pub legacy_envelope_request_id_policy: LegacyEnvelopeRequestIdPolicyWire,
    pub durable_request_id_policy: DurableRequestIdPolicyWire,
    pub source_request_id_policy: SourceRequestIdPolicyWire,
    pub source_get_observed_at_ms: i64,
    pub resolved_at_ms: i64,
    pub consumed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyEnvelopeRequestIdPolicyWire {
    GeneratedUuidV7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableRequestIdPolicyWire {
    PersistedUuidV8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRequestIdPolicyWire {
    BodyEqualsEnvelope,
}

/// Historical owner readback selector for one terminal rejection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberBindingRejectionQueryRequest {
    pub target: MemberTarget,
    pub request_id: Uuid,
}

/// Explicit nullable terminal rejection evidence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberBindingRejectionQueryResponse {
    pub target: MemberTarget,
    pub request_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub rejection: Option<MemberBindingRejectionWire>,
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

/// Published immutable Orchestration observation of a Source rejection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberBindingRejectionWire {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub participant_id: Uuid,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub principal: MemberPrincipalWire,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub expected_freeze_revision: u64,
    pub manifest_digest: String,
    pub issuer_id: Uuid,
    pub workspace_root_fingerprint: String,
    pub binding_digest: String,
    pub delegation_digest: String,
    pub membership_digest: String,
    pub display_name: String,
    pub declared_role: String,
    pub transport: MemberTransportWire,
    pub reason: MemberBindingRejectionReasonWire,
    pub reserved_at_ms: i64,
    pub requested_at_ms: i64,
    pub observed_at_ms: i64,
    pub rejected_at_ms: i64,
    pub request_digest: String,
    pub receipt_digest: String,
}

/// Authenticated principal echoed only in immutable rejection evidence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemberPrincipalWire {
    TrustedStdio,
    DirectToken { token_id: Uuid },
    OAuthGrant { grant_id: Uuid },
}

/// Closed terminal Source rejection vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberBindingRejectionReasonWire {
    PrincipalTransportMismatch,
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

    fn rejection() -> MemberBindingRejectionWire {
        MemberBindingRejectionWire {
            request_id: Uuid::from_u128(6),
            workspace_id: Uuid::from_u128(1),
            participant_id: Uuid::from_u128(7),
            activity_id: Uuid::from_u128(3),
            agent_id: Uuid::from_u128(8),
            session_id: Uuid::from_u128(4),
            principal: MemberPrincipalWire::DirectToken {
                token_id: Uuid::from_u128(9),
            },
            migration_id: Uuid::from_u128(10),
            destination_id: Uuid::from_u128(11),
            expected_freeze_revision: 1,
            manifest_digest: "11".repeat(32),
            issuer_id: Uuid::from_u128(12),
            workspace_root_fingerprint: "22".repeat(16),
            binding_digest: "33".repeat(32),
            delegation_digest: "44".repeat(32),
            membership_digest: "55".repeat(32),
            display_name: "Ada".to_owned(),
            declared_role: "reviewer".to_owned(),
            transport: MemberTransportWire::Other,
            reason: MemberBindingRejectionReasonWire::PrincipalTransportMismatch,
            reserved_at_ms: 2,
            requested_at_ms: 1,
            observed_at_ms: 3,
            rejected_at_ms: 4,
            request_digest: "77".repeat(32),
            receipt_digest: "66".repeat(32),
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
    fn binding_rejection_readback_requires_explicit_nullable_and_closed_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = MemberBindingRejectionQueryResponse {
            target: target(),
            request_id: Uuid::from_u128(6),
            rejection: Some(rejection()),
        };
        let value = serde_json::to_value(&response)?;
        assert_eq!(value["rejection"]["reason"], "principal_transport_mismatch");
        let mut missing = value.clone();
        let _removed = missing
            .as_object_mut()
            .ok_or("response object")?
            .remove("rejection");
        assert!(serde_json::from_value::<MemberBindingRejectionQueryResponse>(missing).is_err());
        let mut future = value;
        future["rejection"]["reason"] = "future_reason".into();
        assert!(serde_json::from_value::<MemberBindingRejectionQueryResponse>(future).is_err());
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
