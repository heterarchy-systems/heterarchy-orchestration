//! Scoped Agent provision operation query/cancellation contracts.

use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::AgentProvisionUncertaintyRetirement;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::{
    deserialize_optional_non_null, leadership::WorkspaceQueryScope,
    workspace_project::ProviderProjectReferenceWire,
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvisionOperationQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub operation_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvisionOperationCancelRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub operation_id: Uuid,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvisionOperationUncertaintyRetirementRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub retirement_id: Uuid,
    pub operation_id: Uuid,
    pub expected_revision: u64,
    pub recovery_version: u8,
}

/// Immutable evidence that recovery stopped while the provider effect truth
/// remains unknown.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvisionOperationUncertaintyRetirementWire {
    pub retirement_id: Uuid,
    pub operation_id: Uuid,
    pub workspace_id: Uuid,
    pub requester_session_id: Uuid,
    pub expected_unknown_revision: u64,
    pub retired_operation_revision: u64,
    pub recovery_version: u8,
    pub retired_at_ms: i64,
    pub actor_principal: ProvisionOperationActorPrincipalWire,
    pub intent_digest: String,
}

/// Authenticated actor identity copied from the request authentication
/// context into immutable retirement evidence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ProvisionOperationActorPrincipalWire {
    TrustedStdio,
    DirectToken { token_id: Uuid },
    OAuthGrant { grant_id: Uuid },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvisionOperationWire {
    pub operation_id: Uuid,
    pub request_id: Uuid,
    pub slot_ordinal: u8,
    pub effect_kind: String,
    pub provider_family: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub runtime_session_id: Option<Uuid>,
    pub state: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub provider_conversation_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub reported_failure_code: Option<String>,
    pub prepared_at_ms: i64,
    pub updated_at_ms: i64,
    pub revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub uncertainty_retirement: Option<ProvisionOperationUncertaintyRetirementWire>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub project: Option<ProviderProjectReferenceWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvisionOperationQueryResponse {
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub operation: Option<ProvisionOperationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvisionOperationCancelResponse {
    pub scope: WorkspaceQueryScope,
    pub operation: ProvisionOperationWire,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvisionOperationUncertaintyRetirementResponse {
    pub scope: WorkspaceQueryScope,
    pub operation: ProvisionOperationWire,
    pub retirement: ProvisionOperationUncertaintyRetirementWire,
    pub changed: bool,
}

impl From<&AgentProvisionUncertaintyRetirement> for ProvisionOperationUncertaintyRetirementWire {
    fn from(value: &AgentProvisionUncertaintyRetirement) -> Self {
        Self {
            retirement_id: value.retirement_id(),
            operation_id: value.operation_id(),
            workspace_id: value.workspace_id().into_uuid(),
            requester_session_id: value.requester_session_id().into_uuid(),
            expected_unknown_revision: value.expected_unknown_revision(),
            retired_operation_revision: value.retired_revision(),
            recovery_version: value.recovery_version(),
            retired_at_ms: value.retired_at().get(),
            actor_principal: principal_wire(value.actor_principal()),
            intent_digest: hex(&value.intent_digest()),
        }
    }
}

fn principal_wire(value: AuthenticatedPrincipal) -> ProvisionOperationActorPrincipalWire {
    match value {
        AuthenticatedPrincipal::TrustedStdio => ProvisionOperationActorPrincipalWire::TrustedStdio,
        AuthenticatedPrincipal::DirectToken(value) => {
            ProvisionOperationActorPrincipalWire::DirectToken {
                token_id: value.into_uuid(),
            }
        }
        AuthenticatedPrincipal::OAuthGrant(value) => {
            ProvisionOperationActorPrincipalWire::OAuthGrant {
                grant_id: value.into_uuid(),
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
