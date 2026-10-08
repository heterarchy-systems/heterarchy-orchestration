//! Authenticated operator-worker commands. Clocks and principal are server supplied.

use heterarchy_orchestration_operations::{
    AgentProvisionEffectKind, AgentProvisionOperation, AgentProvisionProviderFamily,
    AgentProvisionState, ProviderMessageOutbox, ProviderMessageState, ProviderMessageTarget,
};
use heterarchy_orchestration_sessions::AgentProfileProviderFamily;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::{node_chat::ChatProviderStateWire, provisioning_operations::ProvisionOperationWire};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderDispatchRequest {}

/// Current selection only; it cannot identify an uncertain dispatch request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionSelectionObservation {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub operation: Option<ProvisionOperationWire>,
    pub retry_authorized: bool,
    pub request_equivalence_proven: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageSelectionQuery {
    pub after_message_id: Option<Uuid>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageSelectionObservation {
    pub messages: Vec<ProviderMessageOutboxWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_message_id: Option<Uuid>,
    pub retry_authorized: bool,
    pub request_equivalence_proven: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderIncomingReadbackResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub message: Option<ProviderIncomingMessageResponse>,
    pub retry_authorized: bool,
    pub request_equivalence_proven: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderOperationFence {
    pub operation_id: Uuid,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionInspection {
    pub operation_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionConfirmation {
    pub operation_id: Uuid,
    pub expected_revision: u64,
    pub provider_conversation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionFailure {
    pub operation_id: Uuid,
    pub expected_revision: u64,
    pub failure_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageFence {
    pub message_id: Uuid,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageInspection {
    pub message_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderDeliveryConfirmation {
    pub message_id: Uuid,
    pub expected_revision: u64,
    pub provider_conversation_id: String,
    pub provider_turn_id: String,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderIncomingRole {
    Assistant,
    User,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderIncomingMessage {
    pub reply_to_message_id: Uuid,
    pub provider_conversation_id: String,
    pub provider_turn_id: String,
    pub role: ProviderIncomingRole,
    pub body: String,
}

/// Closed action the provider adapter may execute or inspect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderWorkerActionWire {
    Execute,
    Inspect,
}

/// Exact selected provider provision target accompanying one frozen dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionTargetWire {
    pub profile_id: Uuid,
    pub runtime_session_id: Uuid,
    pub effect_kind: String,
    pub provider_family: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub provider_conversation_id: Option<String>,
}

/// One durable provision operation and its exact one-shot worker fence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionDispatchWire {
    pub workspace_id: Uuid,
    pub operation: ProvisionOperationWire,
    pub operation_revision: u64,
    pub action: ProviderWorkerActionWire,
    pub target: ProviderProvisionTargetWire,
}

/// No released provision action is represented by `dispatch: null`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionDispatchResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dispatch: Option<ProviderProvisionDispatchWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub reconciled: Option<ProviderProvisionMutationResponse>,
}

/// Exact operator readback. It never calls the dispatcher or admits Execute.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionQueryResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub operation: Option<ProvisionOperationWire>,
}

/// Durable provision mutation response scoped to the operation's Workspace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProvisionMutationResponse {
    pub workspace_id: Uuid,
    pub operation: ProvisionOperationWire,
}

/// Provider family supported by the current frozen target contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderWorkerFamilyWire {
    #[serde(rename = "chatgpt")]
    ChatGpt,
}

/// Full immutable provider route captured before external I/O.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageTargetWire {
    pub workspace_id: Uuid,
    pub profile_id: Uuid,
    pub participation_id: Uuid,
    pub provider_family: ProviderWorkerFamilyWire,
    pub runtime_session_id: Uuid,
    pub provider_conversation_id: String,
}

/// Exact provider delivery receipt recorded by the durable outbox.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderDeliveryReceiptWire {
    pub provider_turn_id: String,
    pub body: String,
}

/// Durable message outbox state with exact target and revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageOutboxWire {
    pub message_id: Uuid,
    pub target: ProviderMessageTargetWire,
    pub state: ChatProviderStateWire,
    pub queued_at_ms: i64,
    pub last_dispatch_at_ms: i64,
    pub updated_at_ms: i64,
    pub revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<ProviderDeliveryReceiptWire>,
}

/// Frozen provider action, or an action-none durable reconciliation result.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageDispatchResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub message: Option<ProviderMessageOutboxWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub action: Option<ProviderWorkerActionWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub body: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expires_at_ms: Option<i64>,
}

/// Exact message inspection is fenced to the supplied identity and can only
/// return Inspect or action-none state.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageInspectionResponse {
    pub message: ProviderMessageOutboxWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub action: Option<ProviderWorkerActionWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub body: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expires_at_ms: Option<i64>,
}

/// Durable provider-message mutation readback with the exact frozen message
/// body and expiry observed for the operation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderMessageMutationResponse {
    pub message: ProviderMessageOutboxWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub body: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expires_at_ms: Option<i64>,
}

/// Idempotent incoming provider turn readback tied to its exact reply route.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderIncomingMessageResponse {
    pub created: bool,
    pub message_id: Uuid,
    pub workspace_id: Uuid,
    pub target: ProviderMessageTargetWire,
    pub provider_turn_id: String,
    pub role: ProviderIncomingRole,
    pub body: String,
    pub sequence: u64,
    pub submitted_at_ms: i64,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

impl From<&AgentProvisionOperation> for ProvisionOperationWire {
    fn from(operation: &AgentProvisionOperation) -> Self {
        Self {
            operation_id: operation.operation_id(),
            request_id: operation.request_id(),
            slot_ordinal: operation.slot_ordinal(),
            effect_kind: match operation.effect_kind() {
                AgentProvisionEffectKind::WakeExistingRuntime => "wake_existing_runtime",
                AgentProvisionEffectKind::WakeNewRuntime => "wake_new_runtime",
                AgentProvisionEffectKind::ProvisionNew => "provision_new",
                AgentProvisionEffectKind::ReuseReadyRuntime => "reuse_ready_runtime",
            }
            .to_owned(),
            provider_family: match operation.provider_family() {
                AgentProvisionProviderFamily::ChatGpt => "chatgpt",
            }
            .to_owned(),
            profile_id: operation
                .profile_id()
                .map(heterarchy_orchestration_sessions::AgentProfileId::into_uuid),
            runtime_session_id: operation
                .runtime_session_id()
                .map(heterarchy_orchestration_sessions::AgentProfileRuntimeSessionId::into_uuid),
            state: match operation.state() {
                AgentProvisionState::Prepared => "prepared",
                AgentProvisionState::EffectPending => "effect_pending",
                AgentProvisionState::EffectUnknown => "effect_unknown",
                AgentProvisionState::Observed => "observed",
                AgentProvisionState::Confirmed => "confirmed",
                AgentProvisionState::Failed => "failed",
                AgentProvisionState::Cancelled => "cancelled",
            }
            .to_owned(),
            provider_conversation_id: operation.provider_conversation_id().map(str::to_owned),
            reported_failure_code: operation
                .reported_failure_code()
                .map(|code| code.as_str().to_owned()),
            prepared_at_ms: operation.prepared_at().get(),
            updated_at_ms: operation.updated_at().get(),
            revision: operation.revision(),
        }
    }
}

impl From<&ProviderMessageTarget> for ProviderMessageTargetWire {
    fn from(target: &ProviderMessageTarget) -> Self {
        Self {
            workspace_id: target.workspace_id().into_uuid(),
            profile_id: target.profile_id().into_uuid(),
            participation_id: target.participation_id().into_uuid(),
            provider_family: match target.provider_family() {
                AgentProfileProviderFamily::ChatGpt => ProviderWorkerFamilyWire::ChatGpt,
            },
            runtime_session_id: target.runtime_session_id().into_uuid(),
            provider_conversation_id: target.provider_conversation_id().to_owned(),
        }
    }
}

impl From<&ProviderMessageOutbox> for ProviderMessageOutboxWire {
    fn from(outbox: &ProviderMessageOutbox) -> Self {
        Self {
            message_id: outbox.message_id(),
            target: ProviderMessageTargetWire::from(outbox.target()),
            state: match outbox.state() {
                ProviderMessageState::Queued => ChatProviderStateWire::Queued,
                ProviderMessageState::EffectPending => ChatProviderStateWire::EffectPending,
                ProviderMessageState::Delivered => ChatProviderStateWire::Delivered,
                ProviderMessageState::Failed => ChatProviderStateWire::Failed,
                ProviderMessageState::Unknown => ChatProviderStateWire::Unknown,
            },
            queued_at_ms: outbox.queued_at().get(),
            last_dispatch_at_ms: outbox.last_dispatch_at().get(),
            updated_at_ms: outbox.updated_at().get(),
            revision: outbox.revision(),
            receipt: outbox.receipt().map(|receipt| ProviderDeliveryReceiptWire {
                provider_turn_id: receipt.provider_turn_id().to_owned(),
                body: receipt.body().to_owned(),
            }),
        }
    }
}
