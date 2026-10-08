//! Authenticated HTTP contracts for Profile deletion and provider receipts.

use super::AgentProfileResponse;
use heterarchy_orchestration_operations::{
    AgentProfileProviderDeletionAction, AgentProfileProviderDeletionDeferredReason,
    AgentProfileProviderDeletionState,
};
use heterarchy_orchestration_runtime::profile_deletion::{
    AgentProfileDeletionDeferredReason, AgentProfileDeletionProgressSummary,
    AgentProfileDeletionResumeSummary, AgentProfileDeletionSnapshot, AgentProfileDeletionStage,
    AgentProfileDeletionStepResult, AgentProfileProviderDeletionDispatchSummary,
    AgentProfileProviderDeletionFence, AgentProfileProviderDeletionReceiptInput,
    AgentProfileProviderDeletionReceiptSummary,
};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

/// New Profile deletion request. Caller authority comes from the bearer token.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileDeletionRequest {
    pub request_id: String,
    pub expected_profile_revision: i64,
}

/// Current Profile and read-only durable deletion projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileDeletionResponse {
    pub profile: AgentProfileResponse,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub deletion: Option<AgentProfileDeletionProgressResponse>,
}

impl From<AgentProfileDeletionSnapshot> for AgentProfileDeletionResponse {
    fn from(value: AgentProfileDeletionSnapshot) -> Self {
        Self {
            profile: value.profile.into(),
            deletion: value.deletion.map(Into::into),
        }
    }
}

/// Bounded durable progress projection for one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileDeletionProgressResponse {
    pub request_id: Uuid,
    pub expected_profile_revision: i64,
    pub requested_at_ms: i64,
    pub stage: AgentProfileDeletionStageWire,
    pub pending_effects: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub linked_conversation_count: Option<u64>,
    pub deleted_conversation_count: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub deferred_reason: Option<AgentProfileDeletionDeferredReasonWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub completed_at_ms: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub revision: Option<u64>,
}

impl From<AgentProfileDeletionProgressSummary> for AgentProfileDeletionProgressResponse {
    fn from(value: AgentProfileDeletionProgressSummary) -> Self {
        Self {
            request_id: value.request_id,
            expected_profile_revision: value.expected_profile_revision,
            requested_at_ms: value.requested_at_ms,
            stage: value.stage.into(),
            pending_effects: value.pending_effects,
            linked_conversation_count: value.linked_conversation_count,
            deleted_conversation_count: value.deleted_conversation_count,
            deferred_reason: value.deferred_reason.map(Into::into),
            completed_at_ms: value.completed_at_ms,
            revision: value.revision,
        }
    }
}

/// Outcome of one bounded explicit resume request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileDeletionResumeResponse {
    pub step: AgentProfileDeletionStepWire,
    pub snapshot: AgentProfileDeletionResponse,
}

impl From<AgentProfileDeletionResumeSummary> for AgentProfileDeletionResumeResponse {
    fn from(value: AgentProfileDeletionResumeSummary) -> Self {
        Self {
            step: value.step.into(),
            snapshot: value.snapshot.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentProfileDeletionStageWire {
    ClosingAdmission,
    WaitingForEffects,
    CheckingHistory,
    ProviderDeletionPending,
    DeletingConversations,
    WaitingForConfirmation,
    RetryScheduled,
    AttentionRequired,
    Finishing,
    Completed,
}

impl From<AgentProfileDeletionStage> for AgentProfileDeletionStageWire {
    fn from(value: AgentProfileDeletionStage) -> Self {
        match value {
            AgentProfileDeletionStage::ClosingAdmission => Self::ClosingAdmission,
            AgentProfileDeletionStage::WaitingForEffects => Self::WaitingForEffects,
            AgentProfileDeletionStage::CheckingHistory => Self::CheckingHistory,
            AgentProfileDeletionStage::ProviderDeletionPending => Self::ProviderDeletionPending,
            AgentProfileDeletionStage::DeletingConversations => Self::DeletingConversations,
            AgentProfileDeletionStage::WaitingForConfirmation => Self::WaitingForConfirmation,
            AgentProfileDeletionStage::RetryScheduled => Self::RetryScheduled,
            AgentProfileDeletionStage::AttentionRequired => Self::AttentionRequired,
            AgentProfileDeletionStage::Finishing => Self::Finishing,
            AgentProfileDeletionStage::Completed => Self::Completed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentProfileDeletionDeferredReasonWire {
    HistoryMismatch,
    CatalogUnavailable,
    ManifestMismatch,
    ProviderBusy,
    AuthenticationRequired,
    SurfaceUnavailable,
    ActionUnavailable,
}

impl From<AgentProfileDeletionDeferredReason> for AgentProfileDeletionDeferredReasonWire {
    fn from(value: AgentProfileDeletionDeferredReason) -> Self {
        match value {
            AgentProfileDeletionDeferredReason::HistoryMismatch => Self::HistoryMismatch,
            AgentProfileDeletionDeferredReason::CatalogUnavailable => Self::CatalogUnavailable,
            AgentProfileDeletionDeferredReason::ManifestMismatch => Self::ManifestMismatch,
            AgentProfileDeletionDeferredReason::ProviderBusy => Self::ProviderBusy,
            AgentProfileDeletionDeferredReason::AuthenticationRequired => {
                Self::AuthenticationRequired
            }
            AgentProfileDeletionDeferredReason::SurfaceUnavailable => Self::SurfaceUnavailable,
            AgentProfileDeletionDeferredReason::ActionUnavailable => Self::ActionUnavailable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentProfileDeletionStepWire {
    Progressed,
    Waiting,
    Deferred,
    Complete,
}

impl From<AgentProfileDeletionStepResult> for AgentProfileDeletionStepWire {
    fn from(value: AgentProfileDeletionStepResult) -> Self {
        match value {
            AgentProfileDeletionStepResult::Progressed => Self::Progressed,
            AgentProfileDeletionStepResult::Waiting => Self::Waiting,
            AgentProfileDeletionStepResult::Deferred => Self::Deferred,
            AgentProfileDeletionStepResult::Complete => Self::Complete,
        }
    }
}

/// Action included in every dispatch/receipt fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentProfileProviderDeletionActionWire {
    Execute,
    Inspect,
}

impl From<AgentProfileProviderDeletionActionWire> for AgentProfileProviderDeletionAction {
    fn from(value: AgentProfileProviderDeletionActionWire) -> Self {
        match value {
            AgentProfileProviderDeletionActionWire::Execute => Self::Execute,
            AgentProfileProviderDeletionActionWire::Inspect => Self::Inspect,
        }
    }
}

impl From<AgentProfileProviderDeletionAction> for AgentProfileProviderDeletionActionWire {
    fn from(value: AgentProfileProviderDeletionAction) -> Self {
        match value {
            AgentProfileProviderDeletionAction::Execute => Self::Execute,
            AgentProfileProviderDeletionAction::Inspect => Self::Inspect,
        }
    }
}

/// Exact fence returned by dispatch and required by later provider callbacks.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionFenceWire {
    pub operation_id: Uuid,
    pub request_id: Uuid,
    pub profile_id: Uuid,
    pub runtime_session_id: Uuid,
    pub provider_conversation_id: String,
    pub expected_revision: u64,
    pub attempt_number: u64,
    pub action: AgentProfileProviderDeletionActionWire,
}

impl From<AgentProfileProviderDeletionFence> for AgentProfileProviderDeletionFenceWire {
    fn from(value: AgentProfileProviderDeletionFence) -> Self {
        Self {
            operation_id: value.operation_id(),
            request_id: value.request_id(),
            profile_id: value.profile_id().into_uuid(),
            runtime_session_id: value.runtime_session_id().into_uuid(),
            provider_conversation_id: value.provider_conversation_id().to_owned(),
            expected_revision: value.expected_revision(),
            attempt_number: value.attempt_number(),
            action: value.action().into(),
        }
    }
}

impl AgentProfileProviderDeletionFenceWire {
    /// Decode the exact canonical provider operation fence.
    ///
    /// # Errors
    /// Rejects invalid identities, revisions, attempts and target framing.
    pub fn into_runtime(
        self,
    ) -> Result<AgentProfileProviderDeletionFence, heterarchy_orchestration_runtime::RuntimeError>
    {
        // Runtime constructor validates the same bounds; route code uses
        // this fallible conversion to preserve its typed error.
        AgentProfileProviderDeletionFence::new(
            self.operation_id,
            self.request_id,
            self.profile_id,
            self.runtime_session_id,
            self.provider_conversation_id,
            self.expected_revision,
            self.attempt_number,
            self.action.into(),
        )
    }
}

/// Closed receipt reason; the handler maps it to the Operations reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentProfileProviderDeletionDeferredReasonWire {
    ProviderBusy,
    AuthenticationRequired,
    SurfaceUnavailable,
    ActionUnavailable,
}

impl From<AgentProfileProviderDeletionDeferredReasonWire>
    for AgentProfileProviderDeletionDeferredReason
{
    fn from(value: AgentProfileProviderDeletionDeferredReasonWire) -> Self {
        match value {
            AgentProfileProviderDeletionDeferredReasonWire::ProviderBusy => Self::ProviderBusy,
            AgentProfileProviderDeletionDeferredReasonWire::AuthenticationRequired => {
                Self::AuthenticationRequired
            }
            AgentProfileProviderDeletionDeferredReasonWire::SurfaceUnavailable => {
                Self::SurfaceUnavailable
            }
            AgentProfileProviderDeletionDeferredReasonWire::ActionUnavailable => {
                Self::ActionUnavailable
            }
        }
    }
}

/// Closed provider callback outcome. A positive deletion requires all evidence
/// identities, and there is no local-page-absence outcome.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum AgentProfileProviderDeletionReceiptWire {
    NotStartedBeforeClick {
        reason: AgentProfileProviderDeletionDeferredReasonWire,
        #[serde(default)]
        browser_session_id: Option<String>,
        #[serde(default)]
        tab_id: Option<String>,
        #[serde(default)]
        document_id: Option<String>,
    },
    PositiveNoEffectReadback {
        reason: AgentProfileProviderDeletionDeferredReasonWire,
        browser_session_id: Option<String>,
        tab_id: Option<String>,
        document_id: Option<String>,
    },
    ProviderDeleteConfirmed {
        browser_session_id: String,
        tab_id: String,
        document_id: String,
    },
}

impl From<AgentProfileProviderDeletionReceiptWire> for AgentProfileProviderDeletionReceiptInput {
    fn from(value: AgentProfileProviderDeletionReceiptWire) -> Self {
        match value {
            AgentProfileProviderDeletionReceiptWire::NotStartedBeforeClick {
                reason,
                browser_session_id,
                tab_id,
                document_id,
            } => Self::NotStartedBeforeClick {
                reason: reason.into(),
                browser_session_id,
                tab_id,
                document_id,
            },
            AgentProfileProviderDeletionReceiptWire::PositiveNoEffectReadback {
                reason,
                browser_session_id,
                tab_id,
                document_id,
            } => Self::PositiveNoEffectReadback {
                reason: reason.into(),
                browser_session_id,
                tab_id,
                document_id,
            },
            AgentProfileProviderDeletionReceiptWire::ProviderDeleteConfirmed {
                browser_session_id,
                tab_id,
                document_id,
            } => Self::ProviderDeleteConfirmed {
                browser_session_id,
                tab_id,
                document_id,
            },
        }
    }
}

/// Body for unknown-outcome reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionUnknownRequest {
    pub fence: AgentProfileProviderDeletionFenceWire,
}

/// Body for durable provider receipt submission.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionReceiptRequest {
    pub fence: AgentProfileProviderDeletionFenceWire,
    pub receipt: AgentProfileProviderDeletionReceiptWire,
}

/// One provider action selected by the globally single-in-flight journal.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionDispatchResponse {
    pub fence: AgentProfileProviderDeletionFenceWire,
    pub state: AgentProfileProviderDeletionStateWire,
    pub provider: String,
}

impl From<AgentProfileProviderDeletionDispatchSummary>
    for AgentProfileProviderDeletionDispatchResponse
{
    fn from(value: AgentProfileProviderDeletionDispatchSummary) -> Self {
        Self {
            fence: value.fence.into(),
            state: value.state.into(),
            provider: "chatgpt".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionNextResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dispatch: Option<AgentProfileProviderDeletionDispatchResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionAckResponse {
    pub fence: AgentProfileProviderDeletionFenceWire,
    pub state: AgentProfileProviderDeletionStateWire,
    pub observed_at_ms: i64,
}

/// State observation with no provider action or permission-bearing fence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionOperationWire {
    pub operation_id: Uuid,
    pub request_id: Uuid,
    pub profile_id: Uuid,
    pub runtime_session_id: Uuid,
    pub provider_conversation_id: String,
    pub state: AgentProfileProviderDeletionStateWire,
    pub revision: u64,
    pub attempt_number: u64,
    pub prepared_at_ms: i64,
    pub updated_at_ms: i64,
}

impl From<&heterarchy_orchestration_operations::AgentProfileProviderDeletionOperation>
    for AgentProfileProviderDeletionOperationWire
{
    fn from(
        value: &heterarchy_orchestration_operations::AgentProfileProviderDeletionOperation,
    ) -> Self {
        Self {
            operation_id: value.operation_id(),
            request_id: value.request_id(),
            profile_id: value.profile_id().into_uuid(),
            runtime_session_id: value.runtime_session_id().into_uuid(),
            provider_conversation_id: value.provider_conversation_id().to_owned(),
            state: value.state().into(),
            revision: value.revision(),
            attempt_number: value.attempt_number(),
            prepared_at_ms: value.prepared_at().get(),
            updated_at_ms: value.updated_at().get(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionReceiptObservation {
    pub fence: AgentProfileProviderDeletionFenceWire,
    pub receipt: AgentProfileProviderDeletionReceiptWire,
    pub observed_at_ms: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub retry_after_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionReadbackResponse {
    pub operation: AgentProfileProviderDeletionOperationWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<AgentProfileProviderDeletionReceiptObservation>,
    pub retry_authorized: bool,
    pub request_equivalence_proven: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProfileProviderDeletionSelectionResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub operation: Option<AgentProfileProviderDeletionOperationWire>,
    pub retry_authorized: bool,
    pub request_equivalence_proven: bool,
}

impl TryFrom<&heterarchy_orchestration_operations::AgentProfileProviderDeletionReceipt>
    for AgentProfileProviderDeletionReceiptObservation
{
    type Error = heterarchy_orchestration_runtime::RuntimeError;

    fn try_from(
        value: &heterarchy_orchestration_operations::AgentProfileProviderDeletionReceipt,
    ) -> Result<Self, Self::Error> {
        use heterarchy_orchestration_operations::AgentProfileProviderDeletionReceiptOutcome as Outcome;
        let dispatch = value.dispatch();
        let fence = AgentProfileProviderDeletionFenceWire {
            operation_id: dispatch.operation_id(),
            request_id: dispatch.request_id(),
            profile_id: dispatch.profile_id().into_uuid(),
            runtime_session_id: dispatch.runtime_session_id().into_uuid(),
            provider_conversation_id: dispatch.provider_conversation_id().to_owned(),
            expected_revision: dispatch.revision(),
            attempt_number: dispatch.attempt_number(),
            action: dispatch.action().into(),
        };
        let (receipt, retry_after_ms) = match value.outcome() {
            Outcome::NotStartedBeforeClick {
                reason,
                retry_after,
            } => (
                AgentProfileProviderDeletionReceiptWire::NotStartedBeforeClick {
                    reason: (*reason).into(),
                    browser_session_id: value.browser_session_id().map(str::to_owned),
                    tab_id: value.tab_id().map(str::to_owned),
                    document_id: value.document_id().map(str::to_owned),
                },
                Some(retry_after.get()),
            ),
            Outcome::PositiveNoEffectReadback {
                reason,
                retry_after,
            } => (
                AgentProfileProviderDeletionReceiptWire::PositiveNoEffectReadback {
                    reason: (*reason).into(),
                    browser_session_id: value.browser_session_id().map(str::to_owned),
                    tab_id: value.tab_id().map(str::to_owned),
                    document_id: value.document_id().map(str::to_owned),
                },
                Some(retry_after.get()),
            ),
            Outcome::ProviderDeleteConfirmed => (
                AgentProfileProviderDeletionReceiptWire::ProviderDeleteConfirmed {
                    browser_session_id: value
                        .browser_session_id()
                        .ok_or(Self::Error::Conflict)?
                        .to_owned(),
                    tab_id: value.tab_id().ok_or(Self::Error::Conflict)?.to_owned(),
                    document_id: value.document_id().ok_or(Self::Error::Conflict)?.to_owned(),
                },
                None,
            ),
        };
        Ok(Self {
            fence,
            receipt,
            retry_after_ms,
            observed_at_ms: value.observed_at().get(),
        })
    }
}

impl From<AgentProfileProviderDeletionDeferredReason>
    for AgentProfileProviderDeletionDeferredReasonWire
{
    fn from(value: AgentProfileProviderDeletionDeferredReason) -> Self {
        match value {
            AgentProfileProviderDeletionDeferredReason::ProviderBusy => Self::ProviderBusy,
            AgentProfileProviderDeletionDeferredReason::AuthenticationRequired => {
                Self::AuthenticationRequired
            }
            AgentProfileProviderDeletionDeferredReason::SurfaceUnavailable => {
                Self::SurfaceUnavailable
            }
            AgentProfileProviderDeletionDeferredReason::ActionUnavailable => {
                Self::ActionUnavailable
            }
        }
    }
}

impl From<AgentProfileProviderDeletionReceiptSummary> for AgentProfileProviderDeletionAckResponse {
    fn from(value: AgentProfileProviderDeletionReceiptSummary) -> Self {
        Self {
            fence: value.fence.into(),
            state: value.state.into(),
            observed_at_ms: value.observed_at_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentProfileProviderDeletionStateWire {
    Prepared,
    EffectPending,
    EffectUnknown,
    Deferred,
    Deleted,
}

impl From<AgentProfileProviderDeletionState> for AgentProfileProviderDeletionStateWire {
    fn from(value: AgentProfileProviderDeletionState) -> Self {
        match value {
            AgentProfileProviderDeletionState::Prepared => Self::Prepared,
            AgentProfileProviderDeletionState::EffectPending => Self::EffectPending,
            AgentProfileProviderDeletionState::EffectUnknown => Self::EffectUnknown,
            AgentProfileProviderDeletionState::Deferred => Self::Deferred,
            AgentProfileProviderDeletionState::Deleted => Self::Deleted,
        }
    }
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
