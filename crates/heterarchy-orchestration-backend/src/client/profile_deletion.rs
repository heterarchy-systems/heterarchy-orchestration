//! Official bounded client for Profile deletion and provider receipts.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::profile_deletion::{
    AgentProfileDeletionProgressResponse, AgentProfileDeletionResponse,
};
use crate::wire::profile_deletion::{
    AgentProfileDeletionRequest, AgentProfileDeletionResumeResponse,
    AgentProfileProviderDeletionAckResponse, AgentProfileProviderDeletionActionWire,
    AgentProfileProviderDeletionDispatchResponse, AgentProfileProviderDeletionNextResponse,
    AgentProfileProviderDeletionOperationWire, AgentProfileProviderDeletionReadbackResponse,
    AgentProfileProviderDeletionReceiptObservation, AgentProfileProviderDeletionReceiptRequest,
    AgentProfileProviderDeletionReceiptWire, AgentProfileProviderDeletionSelectionResponse,
    AgentProfileProviderDeletionStateWire, AgentProfileProviderDeletionUnknownRequest,
};

use super::{
    ClientError, OrchestrationHttpClient, ReadbackTarget, canonical_identity, require_identity,
    valid_profile,
};

const MAX_PROVIDER_ID_BYTES: usize = 512;
const MIN_PROVIDER_DELETION_RETRY_MS: i64 = 30_000;

impl OrchestrationHttpClient {
    /// Begin or replay one exact durable Profile deletion intent.
    ///
    /// # Errors
    /// Returns invalid intent, typed rejection, bounded transport failure, or
    /// an uncertain result carrying the immutable request identity.
    pub async fn begin_profile_deletion(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
        request: &AgentProfileDeletionRequest,
    ) -> Result<AgentProfileDeletionResponse, ClientError> {
        require_identity(profile_id)?;
        let request_id = canonical_identity(&request.request_id)?;
        if request.expected_profile_revision <= 0 {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::ProfileDeletionRequest { request_id };
        let response: AgentProfileDeletionResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/agent-profiles/{profile_id}/deletion"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_deletion_for_request(
            &response,
            profile_id,
            request_id,
            request.expected_profile_revision,
        ) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the current Profile and its durable deletion projection.
    ///
    /// # Errors
    /// Returns invalid identity, typed service rejection, bounded transport
    /// failure, or a response with mismatched identity or invalid progress.
    pub async fn profile_deletion_profile(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
    ) -> Result<AgentProfileDeletionResponse, ClientError> {
        require_identity(profile_id)?;
        let response: AgentProfileDeletionResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/agent-profiles/{profile_id}/deletion"),
                None,
                None,
            )
            .await?;
        if response.profile.profile_id != profile_id || !valid_deletion_response(&response) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read one exact durable Profile deletion request.
    ///
    /// # Errors
    /// Returns invalid identity, typed service rejection, bounded transport
    /// failure, or a response with mismatched request identity or progress.
    pub async fn profile_deletion_request(
        &self,
        authorization: &HeaderValue,
        request_id: Uuid,
    ) -> Result<AgentProfileDeletionResponse, ClientError> {
        require_identity(request_id)?;
        let response: AgentProfileDeletionResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/agent-profile-deletions/{request_id}"),
                None,
                None,
            )
            .await?;
        if !valid_deletion_response(&response)
            || response
                .deletion
                .as_ref()
                .is_none_or(|deletion| deletion.request_id != request_id)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Advance one exact deletion request by one bounded recovery step.
    ///
    /// # Errors
    /// Returns invalid identity, typed rejection, bounded transport failure,
    /// or an uncertain result carrying the request identity for readback.
    pub async fn resume_profile_deletion(
        &self,
        authorization: &HeaderValue,
        request_id: Uuid,
    ) -> Result<AgentProfileDeletionResumeResponse, ClientError> {
        require_identity(request_id)?;
        let readback = ReadbackTarget::ProfileDeletionRequest { request_id };
        let response: AgentProfileDeletionResumeResponse = self
            .call::<_, ()>(
                authorization,
                Method::POST,
                format!("/internal/v1/agent-profile-deletions/{request_id}/resume"),
                None,
                Some(readback.clone()),
            )
            .await?;
        if !valid_resume_response(&response, request_id)
            || response
                .snapshot
                .deletion
                .as_ref()
                .is_none_or(|deletion| deletion.request_id != request_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Request the next provider-deletion action from the durable journal.
    ///
    /// # Errors
    /// Returns typed rejection, bounded transport failure, or an uncertain
    /// selection result that must be read back before another request.
    pub async fn next_profile_deletion_provider_operation(
        &self,
        authorization: &HeaderValue,
    ) -> Result<AgentProfileProviderDeletionNextResponse, ClientError> {
        let readback = ReadbackTarget::ProfileDeletionSelection;
        let response: AgentProfileProviderDeletionNextResponse = self
            .call::<_, ()>(
                authorization,
                Method::POST,
                "/internal/v1/agent-profile-provider-deletions/next".to_owned(),
                None,
                Some(readback.clone()),
            )
            .await?;
        if !valid_provider_deletion_next(&response) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Observe the currently in-flight provider deletion without releasing work.
    ///
    /// This observation grants no retry permission and proves no equivalence
    /// with an earlier provider request.
    ///
    /// # Errors
    /// Returns typed rejection, bounded transport failure, or a malformed
    /// selection response.
    pub async fn profile_deletion_provider_selection(
        &self,
        authorization: &HeaderValue,
    ) -> Result<AgentProfileProviderDeletionSelectionResponse, ClientError> {
        let response: AgentProfileProviderDeletionSelectionResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                "/internal/v1/agent-profile-provider-deletions/in-flight".to_owned(),
                None,
                None,
            )
            .await?;
        if response.retry_authorized
            || response.request_equivalence_proven
            || response.operation.as_ref().is_some_and(|operation| {
                !valid_provider_operation(operation)
                    || !matches!(
                        operation.state,
                        AgentProfileProviderDeletionStateWire::EffectPending
                            | AgentProfileProviderDeletionStateWire::EffectUnknown
                    )
            })
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read one exact provider action fence and its optional durable receipt.
    ///
    /// Readback only observes the requested frozen target. It does not prove
    /// request equivalence or authorize a retry.
    ///
    /// # Errors
    /// Returns invalid fence, typed rejection, bounded transport failure, or a
    /// response violating exact target/receipt identity.
    pub async fn profile_deletion_provider_readback(
        &self,
        authorization: &HeaderValue,
        request: &AgentProfileProviderDeletionUnknownRequest,
    ) -> Result<AgentProfileProviderDeletionReadbackResponse, ClientError> {
        validate_fence(&request.fence)?;
        let response: AgentProfileProviderDeletionReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/agent-profile-provider-deletions/{}/query",
                    request.fence.operation_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.retry_authorized
            || response.request_equivalence_proven
            || !valid_provider_operation(&response.operation)
            || !operation_matches_fence(&response.operation, &request.fence)
            || response.receipt.as_ref().is_some_and(|receipt| {
                !valid_provider_receipt_observation(receipt, &request.fence, &response.operation)
            })
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Record that the exact Execute action's effect outcome became unknown.
    ///
    /// # Errors
    /// Returns invalid fence/action, typed rejection, bounded transport failure,
    /// or an uncertain result carrying the exact provider fence for readback.
    pub async fn mark_profile_deletion_provider_unknown(
        &self,
        authorization: &HeaderValue,
        request: &AgentProfileProviderDeletionUnknownRequest,
    ) -> Result<AgentProfileProviderDeletionAckResponse, ClientError> {
        validate_fence(&request.fence)?;
        if request.fence.action != AgentProfileProviderDeletionActionWire::Execute {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::ProfileDeletionProvider(Box::new(request.fence.clone()));
        let response: AgentProfileProviderDeletionAckResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/agent-profile-provider-deletions/{}/unknown",
                    request.fence.operation_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.fence != request.fence
            || response.state != AgentProfileProviderDeletionStateWire::EffectUnknown
            || response.observed_at_ms < 0
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Record or replay one exact provider receipt.
    ///
    /// # Errors
    /// Returns invalid fence/evidence/action, typed rejection, bounded transport
    /// failure, or an uncertain result carrying the exact provider fence.
    pub async fn record_profile_deletion_provider_receipt(
        &self,
        authorization: &HeaderValue,
        request: &AgentProfileProviderDeletionReceiptRequest,
    ) -> Result<AgentProfileProviderDeletionAckResponse, ClientError> {
        validate_fence(&request.fence)?;
        validate_receipt(&request.receipt, request.fence.action)?;
        let readback = ReadbackTarget::ProfileDeletionProvider(Box::new(request.fence.clone()));
        let response: AgentProfileProviderDeletionAckResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/agent-profile-provider-deletions/{}/receipt",
                    request.fence.operation_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        let expected_state = match &request.receipt {
            AgentProfileProviderDeletionReceiptWire::NotStartedBeforeClick { .. }
            | AgentProfileProviderDeletionReceiptWire::PositiveNoEffectReadback { .. } => {
                AgentProfileProviderDeletionStateWire::Deferred
            }
            AgentProfileProviderDeletionReceiptWire::ProviderDeleteConfirmed { .. } => {
                AgentProfileProviderDeletionStateWire::Deleted
            }
        };
        if response.fence != request.fence
            || response.state != expected_state
            || response.observed_at_ms < 0
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }
}

fn valid_deletion_response(response: &AgentProfileDeletionResponse) -> bool {
    valid_profile(&response.profile)
        && response.deletion.as_ref().is_none_or(|deletion| {
            valid_deletion_progress(deletion)
                && deletion.expected_profile_revision <= response.profile.revision
                && response.profile.lifecycle_state != "active"
                && (deletion.stage
                    != crate::wire::profile_deletion::AgentProfileDeletionStageWire::Completed
                    || response.profile.lifecycle_state == "retired")
        })
}

fn valid_resume_response(response: &AgentProfileDeletionResumeResponse, request_id: Uuid) -> bool {
    let Some(deletion) = response.snapshot.deletion.as_ref() else {
        return false;
    };
    valid_deletion_response(&response.snapshot)
        && deletion.request_id == request_id
        && match response.step {
            crate::wire::profile_deletion::AgentProfileDeletionStepWire::Complete => {
                deletion.stage
                    == crate::wire::profile_deletion::AgentProfileDeletionStageWire::Completed
            }
            crate::wire::profile_deletion::AgentProfileDeletionStepWire::Deferred => {
                deletion.deferred_reason.is_some()
            }
            crate::wire::profile_deletion::AgentProfileDeletionStepWire::Progressed
            | crate::wire::profile_deletion::AgentProfileDeletionStepWire::Waiting => true,
        }
}

fn valid_deletion_for_request(
    response: &AgentProfileDeletionResponse,
    profile_id: Uuid,
    request_id: Uuid,
    expected_profile_revision: i64,
) -> bool {
    response.profile.profile_id == profile_id
        && valid_deletion_response(response)
        && response.deletion.as_ref().is_some_and(|deletion| {
            deletion.request_id == request_id
                && deletion.expected_profile_revision == expected_profile_revision
        })
}

fn valid_deletion_progress(value: &AgentProfileDeletionProgressResponse) -> bool {
    use crate::wire::profile_deletion::AgentProfileDeletionStageWire as Stage;

    let revision_is_required = matches!(
        value.stage,
        Stage::ProviderDeletionPending
            | Stage::DeletingConversations
            | Stage::WaitingForConfirmation
            | Stage::RetryScheduled
            | Stage::AttentionRequired
            | Stage::Finishing
            | Stage::Completed
    );
    let linked_count_is_required = matches!(
        value.stage,
        Stage::ProviderDeletionPending
            | Stage::DeletingConversations
            | Stage::WaitingForConfirmation
            | Stage::RetryScheduled
            | Stage::Finishing
            | Stage::Completed
    );
    !value.request_id.is_nil()
        && value.expected_profile_revision > 0
        && value.requested_at_ms >= 0
        && value.revision.is_none_or(|revision| revision > 0)
        && (!revision_is_required || value.revision.is_some())
        && (!linked_count_is_required || value.linked_conversation_count.is_some())
        && value
            .completed_at_ms
            .is_none_or(|timestamp| timestamp >= value.requested_at_ms)
        && value
            .linked_conversation_count
            .is_none_or(|linked| value.deleted_conversation_count <= linked)
        && (!matches!(
            value.stage,
            Stage::AttentionRequired | Stage::RetryScheduled
        ) || value.deferred_reason.is_some())
        && if value.stage == Stage::Completed {
            value.completed_at_ms.is_some()
                && value.revision.is_some()
                && value.pending_effects == 0
                && value.deferred_reason.is_none()
                && value
                    .linked_conversation_count
                    .is_some_and(|linked| value.deleted_conversation_count == linked)
        } else {
            value.completed_at_ms.is_none()
        }
}

fn valid_provider_deletion_next(response: &AgentProfileProviderDeletionNextResponse) -> bool {
    response
        .dispatch
        .as_ref()
        .is_none_or(valid_provider_dispatch)
}

fn valid_provider_dispatch(value: &AgentProfileProviderDeletionDispatchResponse) -> bool {
    value.provider == "chatgpt"
        && validate_fence(&value.fence).is_ok()
        && matches!(
            (value.fence.action, value.state),
            (
                AgentProfileProviderDeletionActionWire::Execute,
                AgentProfileProviderDeletionStateWire::EffectPending,
            ) | (
                AgentProfileProviderDeletionActionWire::Inspect,
                AgentProfileProviderDeletionStateWire::EffectPending
                    | AgentProfileProviderDeletionStateWire::EffectUnknown,
            )
        )
}

fn valid_provider_operation(value: &AgentProfileProviderDeletionOperationWire) -> bool {
    !value.operation_id.is_nil()
        && !value.request_id.is_nil()
        && !value.profile_id.is_nil()
        && !value.runtime_session_id.is_nil()
        && !value.provider_conversation_id.is_empty()
        && value.provider_conversation_id.len() <= MAX_PROVIDER_ID_BYTES
        && value.revision > 0
        && value.attempt_number > 0
        && value.prepared_at_ms >= 0
        && value.updated_at_ms >= value.prepared_at_ms
}

fn operation_matches_fence(
    operation: &AgentProfileProviderDeletionOperationWire,
    fence: &crate::wire::profile_deletion::AgentProfileProviderDeletionFenceWire,
) -> bool {
    operation.operation_id == fence.operation_id
        && operation.request_id == fence.request_id
        && operation.profile_id == fence.profile_id
        && operation.runtime_session_id == fence.runtime_session_id
        && operation.provider_conversation_id == fence.provider_conversation_id
        && operation.revision >= fence.expected_revision
        && operation.attempt_number >= fence.attempt_number
}

fn valid_provider_receipt_observation(
    value: &AgentProfileProviderDeletionReceiptObservation,
    requested_fence: &crate::wire::profile_deletion::AgentProfileProviderDeletionFenceWire,
    operation: &AgentProfileProviderDeletionOperationWire,
) -> bool {
    if value.fence != *requested_fence
        || value.observed_at_ms < 0
        || value.observed_at_ms > operation.updated_at_ms
    {
        return false;
    }
    let valid_retry_after = || {
        value
            .observed_at_ms
            .checked_add(MIN_PROVIDER_DELETION_RETRY_MS)
            .is_some_and(|minimum| {
                value
                    .retry_after_ms
                    .is_some_and(|retry_after| retry_after >= minimum)
            })
    };
    let valid_identity = |value: &str| !value.is_empty() && value.len() <= MAX_PROVIDER_ID_BYTES;
    let valid_optional_identity =
        |value: &Option<String>| value.as_deref().is_none_or(valid_identity);
    match &value.receipt {
        AgentProfileProviderDeletionReceiptWire::NotStartedBeforeClick {
            browser_session_id,
            tab_id,
            document_id,
            ..
        } => {
            valid_optional_identity(browser_session_id)
                && valid_optional_identity(tab_id)
                && valid_optional_identity(document_id)
                && (document_id.is_none() || tab_id.is_some())
                && (tab_id.is_none() || browser_session_id.is_some())
                && value.fence.action == AgentProfileProviderDeletionActionWire::Execute
                && valid_retry_after()
                && (operation.attempt_number > value.fence.attempt_number
                    || operation.state == AgentProfileProviderDeletionStateWire::Deferred)
        }
        AgentProfileProviderDeletionReceiptWire::PositiveNoEffectReadback {
            browser_session_id,
            tab_id,
            document_id,
            ..
        } => {
            browser_session_id.as_deref().is_some_and(valid_identity)
                && tab_id.as_deref().is_some_and(valid_identity)
                && document_id.as_deref().is_some_and(valid_identity)
                && value.fence.action == AgentProfileProviderDeletionActionWire::Inspect
                && valid_retry_after()
                && (operation.attempt_number > value.fence.attempt_number
                    || operation.state == AgentProfileProviderDeletionStateWire::Deferred)
        }
        AgentProfileProviderDeletionReceiptWire::ProviderDeleteConfirmed {
            browser_session_id,
            tab_id,
            document_id,
        } => {
            valid_identity(browser_session_id)
                && valid_identity(tab_id)
                && valid_identity(document_id)
                && operation.state == AgentProfileProviderDeletionStateWire::Deleted
                && value.retry_after_ms.is_none()
        }
    }
}

fn validate_fence(
    fence: &crate::wire::profile_deletion::AgentProfileProviderDeletionFenceWire,
) -> Result<(), ClientError> {
    if fence.operation_id.is_nil()
        || fence.request_id.is_nil()
        || fence.profile_id.is_nil()
        || fence.runtime_session_id.is_nil()
        || fence.provider_conversation_id.is_empty()
        || fence.provider_conversation_id.len() > MAX_PROVIDER_ID_BYTES
        || fence.expected_revision == 0
        || fence.attempt_number == 0
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_receipt(
    receipt: &AgentProfileProviderDeletionReceiptWire,
    action: AgentProfileProviderDeletionActionWire,
) -> Result<(), ClientError> {
    let valid_identity = |value: &str| !value.is_empty() && value.len() <= MAX_PROVIDER_ID_BYTES;
    let valid_optional_identity =
        |value: &Option<String>| value.as_deref().is_none_or(valid_identity);
    match receipt {
        AgentProfileProviderDeletionReceiptWire::NotStartedBeforeClick {
            browser_session_id,
            tab_id,
            document_id,
            ..
        } => {
            if action != AgentProfileProviderDeletionActionWire::Execute
                || !valid_optional_identity(browser_session_id)
                || !valid_optional_identity(tab_id)
                || !valid_optional_identity(document_id)
                || (document_id.is_some() && tab_id.is_none())
                || (tab_id.is_some() && browser_session_id.is_none())
            {
                return Err(ClientError::InvalidRequest);
            }
        }
        AgentProfileProviderDeletionReceiptWire::PositiveNoEffectReadback {
            browser_session_id,
            tab_id,
            document_id,
            ..
        } => {
            if action != AgentProfileProviderDeletionActionWire::Inspect
                || !browser_session_id.as_deref().is_some_and(valid_identity)
                || !tab_id.as_deref().is_some_and(valid_identity)
                || !document_id.as_deref().is_some_and(valid_identity)
            {
                return Err(ClientError::InvalidRequest);
            }
        }
        AgentProfileProviderDeletionReceiptWire::ProviderDeleteConfirmed {
            browser_session_id,
            tab_id,
            document_id,
        } => {
            if !valid_identity(browser_session_id)
                || !valid_identity(tab_id)
                || !valid_identity(document_id)
            {
                return Err(ClientError::InvalidRequest);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
