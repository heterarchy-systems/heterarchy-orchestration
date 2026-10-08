//! Authenticated client for the operator provider-worker contract.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use heterarchy_orchestration_operations::{
    AgentProvisionFailureCode, MAX_MESSAGE_BODY_BYTES, MAX_PROVIDER_ID_BYTES,
    MAX_PROVIDER_MESSAGE_JSON_BYTES,
};

use crate::wire::node_chat::ChatProviderStateWire;
use crate::wire::provider_worker::{
    ProviderDeliveryConfirmation, ProviderDispatchRequest, ProviderIncomingMessage,
    ProviderIncomingMessageResponse, ProviderIncomingReadbackResponse,
    ProviderMessageDispatchResponse, ProviderMessageFence, ProviderMessageInspection,
    ProviderMessageInspectionResponse, ProviderMessageMutationResponse, ProviderMessageOutboxWire,
    ProviderMessageSelectionObservation, ProviderMessageSelectionQuery, ProviderMessageTargetWire,
    ProviderOperationFence, ProviderProvisionConfirmation, ProviderProvisionDispatchResponse,
    ProviderProvisionDispatchWire, ProviderProvisionFailure, ProviderProvisionQueryResponse,
    ProviderProvisionSelectionObservation, ProviderProvisionTargetWire, ProviderWorkerActionWire,
};

use super::provisioning_operations::valid_operation;
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const PROVIDER_WORKER_PATH: &str = "/internal/v1/provider-worker";
const MAX_PROVIDER_WORKER_REQUEST_BYTES: usize = 48 * 1024;
const MAX_PROVIDER_MESSAGE_PAGE_SIZE: u32 = 32;

#[derive(Debug, Clone, Copy)]
struct ProvisionMutationIntent<'a> {
    operation_id: Uuid,
    expected_revision: u64,
    outcome: ProvisionMutationOutcome<'a>,
}

#[derive(Debug, Clone, Copy)]
enum ProvisionMutationOutcome<'a> {
    Unknown,
    Failed(Option<&'a str>),
    Confirmed(&'a str),
}

impl OrchestrationHttpClient {
    /// Select one provider provision action. A lost reply carries only a
    /// non-attributing current-selection readback target; this call never retries.
    ///
    /// # Errors
    /// Returns invalid client input, bounded transport/protocol failures, a typed
    /// rejection, or an unknown outcome carrying the global selection target.
    pub async fn provider_provision_dispatch(
        &self,
        authorization: &HeaderValue,
    ) -> Result<ProviderProvisionDispatchResponse, ClientError> {
        let request = ProviderDispatchRequest {};
        validate_request_size(&request)?;
        let readback = ReadbackTarget::ProviderProvisionDispatch;
        let response: ProviderProvisionDispatchResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/provision/dispatch"),
                Some(&request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_provision_dispatch_response(&response) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one exact provider provision operation without dispatching work.
    ///
    /// # Errors
    /// Returns an invalid operation ID, typed service rejection, bounded
    /// transport failure, or protocol error for a mismatched operation echo.
    pub async fn provider_provision_query(
        &self,
        authorization: &HeaderValue,
        request: &crate::wire::provider_worker::ProviderProvisionInspection,
    ) -> Result<ProviderProvisionQueryResponse, ClientError> {
        require_identity(request.operation_id)?;
        validate_request_size(request)?;
        let response: ProviderProvisionQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/provision/query"),
                Some(request),
                None,
            )
            .await?;
        if response.operation.as_ref().is_some_and(|operation| {
            !valid_operation(operation) || operation.operation_id != request.operation_id
        }) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe the current provision selection. It proves neither attribution
    /// to a lost dispatch nor permission to retry it.
    ///
    /// # Errors
    /// Returns a typed service rejection, bounded transport failure, or protocol
    /// error if the observation carries either forbidden authority flag.
    pub async fn provider_provision_in_flight(
        &self,
        authorization: &HeaderValue,
    ) -> Result<ProviderProvisionSelectionObservation, ClientError> {
        let response: ProviderProvisionSelectionObservation = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("{PROVIDER_WORKER_PATH}/provision/in-flight"),
                None,
                None,
            )
            .await?;
        if !valid_provision_selection_observation(&response) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Mark an exact provision fence unknown after the provider outcome is uncertain.
    ///
    /// # Errors
    /// Returns an invalid fence, typed service rejection, or an uncertain outcome
    /// carrying the exact operation identity.
    pub async fn provider_provision_mark_unknown(
        &self,
        authorization: &HeaderValue,
        request: &ProviderOperationFence,
    ) -> Result<crate::wire::provider_worker::ProviderProvisionMutationResponse, ClientError> {
        validate_operation_fence(request)?;
        self.provision_mutation(
            authorization,
            "/provision/unknown",
            request,
            ProvisionMutationIntent {
                operation_id: request.operation_id,
                expected_revision: request.expected_revision,
                outcome: ProvisionMutationOutcome::Unknown,
            },
        )
        .await
    }

    /// Record a definite provider provision failure at its exact revision fence.
    ///
    /// # Errors
    /// Returns an invalid fence/failure code, typed service rejection, or an
    /// uncertain outcome carrying the exact operation identity.
    pub async fn provider_provision_fail(
        &self,
        authorization: &HeaderValue,
        request: &ProviderProvisionFailure,
    ) -> Result<crate::wire::provider_worker::ProviderProvisionMutationResponse, ClientError> {
        if request.operation_id.is_nil()
            || request.expected_revision == 0
            || request
                .failure_code
                .as_deref()
                .is_some_and(|code| AgentProvisionFailureCode::from_wire_name(code).is_err())
        {
            return Err(ClientError::InvalidRequest);
        }
        validate_request_size(request)?;
        self.provision_mutation(
            authorization,
            "/provision/fail",
            request,
            ProvisionMutationIntent {
                operation_id: request.operation_id,
                expected_revision: request.expected_revision,
                outcome: ProvisionMutationOutcome::Failed(request.failure_code.as_deref()),
            },
        )
        .await
    }

    /// Confirm the provider conversation bound to an exact provision operation.
    ///
    /// # Errors
    /// Returns invalid input, typed service rejection, or an uncertain outcome
    /// carrying the exact operation identity.
    pub async fn provider_provision_confirm(
        &self,
        authorization: &HeaderValue,
        request: &ProviderProvisionConfirmation,
    ) -> Result<crate::wire::provider_worker::ProviderProvisionMutationResponse, ClientError> {
        if request.operation_id.is_nil()
            || request.expected_revision == 0
            || !valid_provider_id(&request.provider_conversation_id)
        {
            return Err(ClientError::InvalidRequest);
        }
        validate_request_size(request)?;
        self.provision_mutation(
            authorization,
            "/provision/confirm",
            request,
            ProvisionMutationIntent {
                operation_id: request.operation_id,
                expected_revision: request.expected_revision,
                outcome: ProvisionMutationOutcome::Confirmed(&request.provider_conversation_id),
            },
        )
        .await
    }

    async fn provision_mutation<B: serde::Serialize>(
        &self,
        authorization: &HeaderValue,
        suffix: &str,
        request: &B,
        intent: ProvisionMutationIntent<'_>,
    ) -> Result<crate::wire::provider_worker::ProviderProvisionMutationResponse, ClientError> {
        let readback = ReadbackTarget::ProviderProvisionOperation {
            operation_id: intent.operation_id,
        };
        let response = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}{suffix}"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_provision_mutation(&response, intent) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Select one provider message action. A lost reply is never treated as an
    /// empty queue or permission to issue another dispatch.
    ///
    /// # Errors
    /// Returns invalid client input, bounded transport/protocol failures, a typed
    /// rejection, or an unknown outcome carrying the global selection target.
    pub async fn provider_message_dispatch(
        &self,
        authorization: &HeaderValue,
    ) -> Result<ProviderMessageDispatchResponse, ClientError> {
        let request = ProviderDispatchRequest {};
        validate_request_size(&request)?;
        let readback = ReadbackTarget::ProviderMessageDispatch;
        let response: ProviderMessageDispatchResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/messages/dispatch"),
                Some(&request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_message_dispatch_response(&response) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Observe a bounded UUID-keyset page of pending or unknown provider messages.
    ///
    /// # Errors
    /// Returns invalid page bounds/cursor, typed service rejection, bounded
    /// transport failure, or protocol error for a malformed page or authority flag.
    pub async fn provider_messages_in_flight(
        &self,
        authorization: &HeaderValue,
        request: &ProviderMessageSelectionQuery,
    ) -> Result<ProviderMessageSelectionObservation, ClientError> {
        validate_message_selection_query(request)?;
        validate_request_size(request)?;
        let response: ProviderMessageSelectionObservation = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/messages/in-flight"),
                Some(request),
                None,
            )
            .await?;
        if !valid_message_selection_observation(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read one exact message without emitting an Inspect action.
    ///
    /// # Errors
    /// Returns an invalid message ID, typed service rejection, bounded transport
    /// failure, or protocol error for a non-exact/non-pure readback.
    pub async fn provider_message_query(
        &self,
        authorization: &HeaderValue,
        request: &ProviderMessageInspection,
    ) -> Result<ProviderMessageInspectionResponse, ClientError> {
        require_identity(request.message_id)?;
        validate_request_size(request)?;
        let response: ProviderMessageInspectionResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/messages/query"),
                Some(request),
                None,
            )
            .await?;
        if response.message.message_id != request.message_id
            || response.action.is_some()
            || !valid_message_readback(&response)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Request one exact provider inspection action. This is an operator
    /// mutation and can return only Inspect or an action-free reconciliation.
    ///
    /// # Errors
    /// Returns an invalid message ID, typed service rejection, or an uncertain
    /// outcome carrying the exact message identity.
    pub async fn provider_message_inspect(
        &self,
        authorization: &HeaderValue,
        request: &ProviderMessageInspection,
    ) -> Result<ProviderMessageInspectionResponse, ClientError> {
        require_identity(request.message_id)?;
        validate_request_size(request)?;
        let readback = ReadbackTarget::ProviderMessageOutbox {
            message_id: request.message_id,
        };
        let response: ProviderMessageInspectionResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/messages/inspect"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.message.message_id != request.message_id
            || response
                .action
                .is_some_and(|action| action != ProviderWorkerActionWire::Inspect)
            || !valid_message_inspection_response(&response)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Mark one exact message revision unknown after an uncertain provider result.
    ///
    /// # Errors
    /// Returns an invalid fence, typed service rejection, or an uncertain outcome
    /// carrying the exact message identity.
    pub async fn provider_message_mark_unknown(
        &self,
        authorization: &HeaderValue,
        request: &ProviderMessageFence,
    ) -> Result<ProviderMessageMutationResponse, ClientError> {
        validate_message_fence(request)?;
        self.message_mutation(authorization, "/messages/unknown", request, |response| {
            response.message.state == ChatProviderStateWire::Unknown
        })
        .await
    }

    /// Record a definite failure for one exact message revision.
    ///
    /// # Errors
    /// Returns an invalid fence, typed service rejection, or an uncertain outcome
    /// carrying the exact message identity.
    pub async fn provider_message_fail(
        &self,
        authorization: &HeaderValue,
        request: &ProviderMessageFence,
    ) -> Result<ProviderMessageMutationResponse, ClientError> {
        validate_message_fence(request)?;
        self.message_mutation(authorization, "/messages/fail", request, |response| {
            response.message.state == ChatProviderStateWire::Failed
        })
        .await
    }

    /// Confirm delivery against the exact provider conversation, turn, body and
    /// current outbox revision.
    ///
    /// # Errors
    /// Returns invalid delivery evidence, typed service rejection, or an uncertain
    /// outcome carrying the exact message identity.
    pub async fn provider_message_delivered(
        &self,
        authorization: &HeaderValue,
        request: &ProviderDeliveryConfirmation,
    ) -> Result<ProviderMessageMutationResponse, ClientError> {
        if request.message_id.is_nil()
            || request.expected_revision == 0
            || !valid_provider_id(&request.provider_conversation_id)
            || !valid_provider_id(&request.provider_turn_id)
            || !valid_message_body(&request.body)
        {
            return Err(ClientError::InvalidRequest);
        }
        validate_request_size(request)?;
        let readback = ReadbackTarget::ProviderMessageOutbox {
            message_id: request.message_id,
        };
        let response: ProviderMessageMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/messages/delivered"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_message_mutation_response(&response, request.message_id)
            || response.message.revision < request.expected_revision
            || response.message.state != ChatProviderStateWire::Delivered
            || response.message.target.provider_conversation_id != request.provider_conversation_id
            || !response.message.receipt.as_ref().is_some_and(|receipt| {
                receipt.provider_turn_id == request.provider_turn_id && receipt.body == request.body
            })
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    async fn message_mutation<F>(
        &self,
        authorization: &HeaderValue,
        suffix: &str,
        request: &ProviderMessageFence,
        expected_state: F,
    ) -> Result<ProviderMessageMutationResponse, ClientError>
    where
        F: FnOnce(&ProviderMessageMutationResponse) -> bool,
    {
        validate_request_size(request)?;
        let readback = ReadbackTarget::ProviderMessageOutbox {
            message_id: request.message_id,
        };
        let response: ProviderMessageMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}{suffix}"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_message_mutation_response(&response, request.message_id)
            || response.message.revision < request.expected_revision
            || !expected_state(&response)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Record an incoming provider turn under its original reply route. A lost
    /// reply retains the whole turn intent for pure readback and is never retried.
    ///
    /// # Errors
    /// Returns invalid turn input, typed service rejection, or an uncertain
    /// outcome carrying the complete original turn intent.
    pub async fn provider_incoming_message(
        &self,
        authorization: &HeaderValue,
        request: &ProviderIncomingMessage,
    ) -> Result<ProviderIncomingMessageResponse, ClientError> {
        validate_incoming_request(request)?;
        validate_request_size(request)?;
        let readback = ReadbackTarget::ProviderIncomingTurn(Box::new(request.clone()));
        let response: ProviderIncomingMessageResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/messages/incoming"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_incoming_message_response(&response, request, false) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the exact original incoming turn without calling the writer.
    /// Absence is only an observation and never proves nonadmission.
    ///
    /// # Errors
    /// Returns invalid turn input, typed service rejection, bounded transport
    /// failure, or protocol error for changed turn content/authority flags.
    pub async fn provider_incoming_readback(
        &self,
        authorization: &HeaderValue,
        request: &ProviderIncomingMessage,
    ) -> Result<ProviderIncomingReadbackResponse, ClientError> {
        validate_incoming_request(request)?;
        validate_request_size(request)?;
        let response: ProviderIncomingReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!("{PROVIDER_WORKER_PATH}/messages/incoming/query"),
                Some(request),
                None,
            )
            .await?;
        if response.retry_authorized
            || response.request_equivalence_proven
            || response
                .message
                .as_ref()
                .is_some_and(|message| !valid_incoming_message_response(message, request, true))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_operation_fence(request: &ProviderOperationFence) -> Result<(), ClientError> {
    if request.operation_id.is_nil() || request.expected_revision == 0 {
        return Err(ClientError::InvalidRequest);
    }
    validate_request_size(request)
}

fn validate_message_fence(request: &ProviderMessageFence) -> Result<(), ClientError> {
    if request.message_id.is_nil() || request.expected_revision == 0 {
        return Err(ClientError::InvalidRequest);
    }
    validate_request_size(request)
}

fn validate_message_selection_query(
    request: &ProviderMessageSelectionQuery,
) -> Result<(), ClientError> {
    if request.limit == 0
        || request.limit > MAX_PROVIDER_MESSAGE_PAGE_SIZE
        || request.after_message_id.is_some_and(|id| id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_incoming_request(request: &ProviderIncomingMessage) -> Result<(), ClientError> {
    if request.reply_to_message_id.is_nil()
        || !valid_provider_id(&request.provider_conversation_id)
        || !valid_provider_id(&request.provider_turn_id)
        || !valid_message_body(&request.body)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_request_size<B: serde::Serialize>(request: &B) -> Result<(), ClientError> {
    if super::bounded_json(request)?.len() > MAX_PROVIDER_WORKER_REQUEST_BYTES {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_provision_dispatch_response(response: &ProviderProvisionDispatchResponse) -> bool {
    match (&response.dispatch, &response.reconciled) {
        (Some(dispatch), None) => valid_provision_dispatch(dispatch),
        (None, Some(reconciled)) => valid_reconciled_provision_mutation(reconciled),
        (None, None) => true,
        (Some(_), Some(_)) => false,
    }
}

fn valid_provision_dispatch(dispatch: &ProviderProvisionDispatchWire) -> bool {
    let operation = &dispatch.operation;
    !dispatch.workspace_id.is_nil()
        && valid_operation(operation)
        && dispatch.operation_revision == operation.revision
        && matches!(
            operation.state.as_str(),
            "effect_pending" | "effect_unknown"
        )
        && match dispatch.action {
            ProviderWorkerActionWire::Execute => operation.state == "effect_pending",
            ProviderWorkerActionWire::Inspect => true,
        }
        && operation.profile_id == Some(dispatch.target.profile_id)
        && operation.runtime_session_id == Some(dispatch.target.runtime_session_id)
        && operation.effect_kind == dispatch.target.effect_kind
        && operation.provider_family == dispatch.target.provider_family
        && valid_provider_provision_target(&dispatch.target)
        && operation
            .provider_conversation_id
            .as_deref()
            .is_none_or(|conversation| {
                dispatch.target.provider_conversation_id.as_deref() == Some(conversation)
            })
}

fn valid_provider_provision_target(target: &ProviderProvisionTargetWire) -> bool {
    !target.profile_id.is_nil()
        && !target.runtime_session_id.is_nil()
        && matches!(
            target.effect_kind.as_str(),
            "wake_existing_runtime" | "wake_new_runtime" | "provision_new" | "reuse_ready_runtime"
        )
        && target.provider_family == "chatgpt"
        && target
            .provider_conversation_id
            .as_deref()
            .is_none_or(valid_provider_id)
}

fn valid_provision_selection_observation(response: &ProviderProvisionSelectionObservation) -> bool {
    !response.retry_authorized
        && !response.request_equivalence_proven
        && response.operation.as_ref().is_none_or(|operation| {
            valid_operation(operation)
                && matches!(
                    operation.state.as_str(),
                    "effect_pending" | "effect_unknown"
                )
        })
}

fn valid_reconciled_provision_mutation(
    response: &crate::wire::provider_worker::ProviderProvisionMutationResponse,
) -> bool {
    !response.workspace_id.is_nil()
        && valid_operation(&response.operation)
        && matches!(
            response.operation.state.as_str(),
            "effect_unknown" | "failed"
        )
}

fn valid_provision_mutation(
    response: &crate::wire::provider_worker::ProviderProvisionMutationResponse,
    intent: ProvisionMutationIntent<'_>,
) -> bool {
    if response.workspace_id.is_nil()
        || !valid_operation(&response.operation)
        || response.operation.operation_id != intent.operation_id
        || response.operation.revision < intent.expected_revision
    {
        return false;
    }
    match intent.outcome {
        ProvisionMutationOutcome::Unknown => response.operation.state == "effect_unknown",
        ProvisionMutationOutcome::Failed(failure_code) => {
            response.operation.state == "failed"
                && response.operation.reported_failure_code.as_deref() == failure_code
        }
        ProvisionMutationOutcome::Confirmed(conversation_id) => {
            response.operation.state == "confirmed"
                && response.operation.provider_conversation_id.as_deref() == Some(conversation_id)
        }
    }
}

fn valid_message_selection_observation(
    response: &ProviderMessageSelectionObservation,
    request: &ProviderMessageSelectionQuery,
) -> bool {
    if response.retry_authorized
        || response.request_equivalence_proven
        || response.messages.len() > request.limit as usize
    {
        return false;
    }
    let mut previous = request.after_message_id;
    for message in &response.messages {
        if !valid_provider_message_outbox(message)
            || !matches!(
                message.state,
                ChatProviderStateWire::EffectPending | ChatProviderStateWire::Unknown
            )
            || previous.is_some_and(|id| message.message_id.as_bytes() <= id.as_bytes())
        {
            return false;
        }
        previous = Some(message.message_id);
    }
    response.next_message_id.is_none_or(|next| {
        response.messages.len() == request.limit as usize
            && response
                .messages
                .last()
                .is_some_and(|message| message.message_id == next)
    })
}

fn valid_message_dispatch_response(response: &ProviderMessageDispatchResponse) -> bool {
    let Some(message) = response.message.as_ref() else {
        return response.action.is_none()
            && response.body.is_none()
            && response.expires_at_ms.is_none();
    };
    if !valid_provider_message_outbox(message)
        || response
            .body
            .as_deref()
            .is_some_and(|body| !valid_provider_payload(body))
        || response.expires_at_ms.is_some_and(|value| value < 0)
    {
        return false;
    }
    match response.action {
        Some(ProviderWorkerActionWire::Execute) => {
            message.state == ChatProviderStateWire::EffectPending
                && response.body.as_deref().is_some_and(valid_provider_payload)
        }
        Some(ProviderWorkerActionWire::Inspect) => {
            matches!(
                message.state,
                ChatProviderStateWire::EffectPending | ChatProviderStateWire::Unknown
            ) && response.body.as_deref().is_some_and(valid_provider_payload)
        }
        None => true,
    }
}

fn valid_message_inspection_response(response: &ProviderMessageInspectionResponse) -> bool {
    valid_provider_message_outbox(&response.message)
        && response.action.is_none_or(|action| {
            action == ProviderWorkerActionWire::Inspect
                && matches!(
                    response.message.state,
                    ChatProviderStateWire::EffectPending | ChatProviderStateWire::Unknown
                )
                && response.body.as_deref().is_some_and(valid_provider_payload)
        })
        && response.body.as_deref().is_none_or(valid_provider_payload)
        && response.expires_at_ms.is_none_or(|value| value >= 0)
}

fn valid_message_readback(response: &ProviderMessageInspectionResponse) -> bool {
    response.action.is_none()
        && valid_provider_message_outbox(&response.message)
        && response.body.as_deref().is_some_and(valid_provider_payload)
        && response.expires_at_ms.is_none_or(|value| value >= 0)
}

fn valid_message_mutation_response(
    response: &ProviderMessageMutationResponse,
    message_id: Uuid,
) -> bool {
    response.message.message_id == message_id
        && valid_provider_message_outbox(&response.message)
        && response.body.as_deref().is_some_and(valid_provider_payload)
        && response.expires_at_ms.is_none_or(|value| value >= 0)
}

fn valid_provider_message_outbox(message: &ProviderMessageOutboxWire) -> bool {
    let target = &message.target;
    let valid_receipt = match (message.state, message.receipt.as_ref()) {
        (ChatProviderStateWire::Delivered, Some(receipt)) => {
            valid_provider_id(&receipt.provider_turn_id) && valid_message_body(&receipt.body)
        }
        (ChatProviderStateWire::Delivered, None) | (_, Some(_)) => false,
        (_, None) => true,
    };
    !message.message_id.is_nil()
        && !target.workspace_id.is_nil()
        && !target.profile_id.is_nil()
        && !target.participation_id.is_nil()
        && !target.runtime_session_id.is_nil()
        && valid_provider_id(&target.provider_conversation_id)
        && message.queued_at_ms >= 0
        && message.last_dispatch_at_ms >= 0
        && message.updated_at_ms >= message.queued_at_ms
        && (message.last_dispatch_at_ms == 0 || message.last_dispatch_at_ms >= message.queued_at_ms)
        && message.revision > 0
        && valid_receipt
}

fn valid_incoming_message_response(
    response: &ProviderIncomingMessageResponse,
    request: &ProviderIncomingMessage,
    readback: bool,
) -> bool {
    !response.message_id.is_nil()
        && !response.workspace_id.is_nil()
        && response.workspace_id == response.target.workspace_id
        && valid_provider_message_target(&response.target)
        && response.target.provider_conversation_id == request.provider_conversation_id
        && response.provider_turn_id == request.provider_turn_id
        && response.role == request.role
        && response.body == request.body
        && response.sequence > 0
        && response.submitted_at_ms >= 0
        && (!readback || !response.created)
}

fn valid_provider_message_target(target: &ProviderMessageTargetWire) -> bool {
    !target.workspace_id.is_nil()
        && !target.profile_id.is_nil()
        && !target.participation_id.is_nil()
        && !target.runtime_session_id.is_nil()
        && valid_provider_id(&target.provider_conversation_id)
}

fn valid_provider_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_PROVIDER_ID_BYTES
}

fn valid_message_body(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_MESSAGE_BODY_BYTES
}

fn valid_provider_payload(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_PROVIDER_MESSAGE_JSON_BYTES
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyper::header::HeaderValue;
    use serde_json::Value;
    use tokio::io::AsyncReadExt;
    use uuid::Uuid;

    use crate::client::tests::{fixture, reply};
    use crate::wire::node_chat::ChatProviderStateWire;
    use crate::wire::provider_worker::{
        ProviderIncomingMessage, ProviderIncomingMessageResponse, ProviderIncomingReadbackResponse,
        ProviderMessageInspection, ProviderMessageInspectionResponse, ProviderMessageOutboxWire,
        ProviderMessageSelectionObservation, ProviderMessageSelectionQuery,
        ProviderMessageTargetWire, ProviderProvisionDispatchResponse,
        ProviderProvisionQueryResponse, ProviderProvisionSelectionObservation,
    };
    use crate::wire::provisioning_operations::ProvisionOperationWire;

    use super::*;

    const CREDENTIAL: &str = "Bearer provider-worker-fixture-secret";

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn operation(operation_id: Uuid, state: &str) -> ProvisionOperationWire {
        ProvisionOperationWire {
            operation_id,
            request_id: id(2),
            slot_ordinal: 0,
            effect_kind: "wake_new_runtime".to_owned(),
            provider_family: "chatgpt".to_owned(),
            profile_id: Some(id(3)),
            runtime_session_id: Some(id(4)),
            state: state.to_owned(),
            provider_conversation_id: matches!(state, "observed" | "confirmed")
                .then(|| "conversation-1".to_owned()),
            reported_failure_code: None,
            prepared_at_ms: 1,
            updated_at_ms: 2,
            revision: 7,
        }
    }

    fn message(message_id: Uuid, state: ChatProviderStateWire) -> ProviderMessageOutboxWire {
        ProviderMessageOutboxWire {
            message_id,
            target: ProviderMessageTargetWire {
                workspace_id: id(1),
                profile_id: id(3),
                participation_id: id(5),
                provider_family: crate::wire::provider_worker::ProviderWorkerFamilyWire::ChatGpt,
                runtime_session_id: id(4),
                provider_conversation_id: "conversation-1".to_owned(),
            },
            state,
            queued_at_ms: 10,
            last_dispatch_at_ms: 11,
            updated_at_ms: 12,
            revision: 3,
            receipt: None,
        }
    }

    fn incoming_request() -> ProviderIncomingMessage {
        ProviderIncomingMessage {
            reply_to_message_id: id(20),
            provider_conversation_id: "conversation-1".to_owned(),
            provider_turn_id: "turn-1".to_owned(),
            role: crate::wire::provider_worker::ProviderIncomingRole::User,
            body: "Question from provider".to_owned(),
        }
    }

    fn incoming_response(
        request: &ProviderIncomingMessage,
        created: bool,
    ) -> ProviderIncomingMessageResponse {
        ProviderIncomingMessageResponse {
            created,
            message_id: id(21),
            workspace_id: id(1),
            target: message(id(20), ChatProviderStateWire::Delivered).target,
            provider_turn_id: request.provider_turn_id.clone(),
            role: request.role,
            body: request.body.clone(),
            sequence: 9,
            submitted_at_ms: 13,
        }
    }

    fn json_body(request: &[u8]) -> Result<Value, Box<dyn std::error::Error>> {
        let text = std::str::from_utf8(request)?;
        let body = text
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .ok_or("HTTP request headers missing")?;
        Ok(serde_json::from_str(body)?)
    }

    async fn captured_request(
        server: tokio::task::JoinHandle<Result<Vec<u8>, std::io::Error>>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        Ok(String::from_utf8(server.await??)?)
    }

    #[tokio::test]
    async fn provider_provision_query_uses_exact_id_and_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let authorization = HeaderValue::from_static(CREDENTIAL);
        let operation_id = id(40);
        let expected = ProviderProvisionQueryResponse {
            operation: Some(operation(operation_id, "effect_pending")),
        };
        let (client, server) =
            fixture(Some(reply("200 OK", &serde_json::to_value(&expected)?))).await?;
        assert_eq!(
            client
                .provider_provision_query(
                    &authorization,
                    &crate::wire::provider_worker::ProviderProvisionInspection { operation_id },
                )
                .await?,
            expected
        );
        let request = captured_request(server).await?;
        assert!(request.starts_with("POST /internal/v1/provider-worker/provision/query HTTP/1.1"));
        assert!(request.contains(CREDENTIAL));
        assert_eq!(
            json_body(request.as_bytes())?["operationId"],
            operation_id.to_string()
        );
        Ok(())
    }

    #[tokio::test]
    async fn provision_in_flight_readback_carries_no_retry_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let authorization = HeaderValue::from_static(CREDENTIAL);
        let expected = ProviderProvisionSelectionObservation {
            operation: None,
            retry_authorized: false,
            request_equivalence_proven: false,
        };
        let (client, server) =
            fixture(Some(reply("200 OK", &serde_json::to_value(&expected)?))).await?;
        assert_eq!(
            client.provider_provision_in_flight(&authorization).await?,
            expected
        );
        let request = captured_request(server).await?;
        assert!(
            request.starts_with("GET /internal/v1/provider-worker/provision/in-flight HTTP/1.1")
        );
        assert!(request.contains(CREDENTIAL));
        Ok(())
    }

    #[tokio::test]
    async fn provider_message_page_echoes_cursor_and_non_authority_flags()
    -> Result<(), Box<dyn std::error::Error>> {
        let authorization = HeaderValue::from_static(CREDENTIAL);
        let query = ProviderMessageSelectionQuery {
            after_message_id: Some(id(41)),
            limit: 3,
        };
        let expected = ProviderMessageSelectionObservation {
            messages: Vec::new(),
            next_message_id: None,
            retry_authorized: false,
            request_equivalence_proven: false,
        };
        let (client, server) =
            fixture(Some(reply("200 OK", &serde_json::to_value(&expected)?))).await?;
        assert_eq!(
            client
                .provider_messages_in_flight(&authorization, &query)
                .await?,
            expected
        );
        let request = captured_request(server).await?;
        assert!(
            request.starts_with("POST /internal/v1/provider-worker/messages/in-flight HTTP/1.1")
        );
        assert_eq!(
            json_body(request.as_bytes())?["afterMessageId"],
            id(41).to_string()
        );
        Ok(())
    }

    #[tokio::test]
    async fn provider_message_query_is_pure_and_exact() -> Result<(), Box<dyn std::error::Error>> {
        let authorization = HeaderValue::from_static(CREDENTIAL);
        let message_id = id(42);
        let expected = ProviderMessageInspectionResponse {
            message: message(message_id, ChatProviderStateWire::EffectPending),
            action: None,
            body: Some("provider request payload".to_owned()),
            expires_at_ms: Some(100),
        };
        let (client, server) =
            fixture(Some(reply("200 OK", &serde_json::to_value(&expected)?))).await?;
        assert_eq!(
            client
                .provider_message_query(&authorization, &ProviderMessageInspection { message_id })
                .await?,
            expected
        );
        let request = captured_request(server).await?;
        assert!(request.starts_with("POST /internal/v1/provider-worker/messages/query HTTP/1.1"));
        assert_eq!(
            json_body(request.as_bytes())?["messageId"],
            message_id.to_string()
        );
        Ok(())
    }

    #[tokio::test]
    async fn incoming_readback_preserves_the_original_turn_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        let authorization = HeaderValue::from_static(CREDENTIAL);
        let incoming = incoming_request();
        let expected = ProviderIncomingReadbackResponse {
            message: None,
            retry_authorized: false,
            request_equivalence_proven: false,
        };
        let (client, server) =
            fixture(Some(reply("200 OK", &serde_json::to_value(&expected)?))).await?;
        assert_eq!(
            client
                .provider_incoming_readback(&authorization, &incoming)
                .await?,
            expected
        );
        let request = captured_request(server).await?;
        assert!(
            request
                .starts_with("POST /internal/v1/provider-worker/messages/incoming/query HTTP/1.1")
        );
        assert_eq!(
            json_body(request.as_bytes())?["providerTurnId"],
            incoming.provider_turn_id
        );
        Ok(())
    }

    #[tokio::test]
    async fn mismatched_mutation_echo_keeps_exact_readback_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let authorization = HeaderValue::from_static(CREDENTIAL);
        let requested = id(50);
        let response = ProviderMessageMutationResponse {
            message: message(id(51), ChatProviderStateWire::Unknown),
            body: Some("provider request payload".to_owned()),
            expires_at_ms: Some(100),
        };
        let (client, server) =
            fixture(Some(reply("200 OK", &serde_json::to_value(&response)?))).await?;
        assert_eq!(
            client
                .provider_message_mark_unknown(
                    &authorization,
                    &ProviderMessageFence {
                        message_id: requested,
                        expected_revision: 3,
                    },
                )
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::ProviderMessageOutbox {
                    message_id: requested
                }
            })
        );
        let request = String::from_utf8(server.await??)?;
        assert!(request.starts_with("POST /internal/v1/provider-worker/messages/unknown HTTP/1.1"));
        assert_eq!(json_body(request.as_bytes())?["expectedRevision"], 3);
        Ok(())
    }

    #[tokio::test]
    async fn incoming_turn_readback_rejects_retry_flags_or_changed_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let authorization = HeaderValue::from_static(CREDENTIAL);
        let request = incoming_request();
        for response in [
            ProviderIncomingReadbackResponse {
                message: None,
                retry_authorized: true,
                request_equivalence_proven: false,
            },
            ProviderIncomingReadbackResponse {
                message: Some({
                    let mut message = incoming_response(&request, false);
                    message.body.push('!');
                    message
                }),
                retry_authorized: false,
                request_equivalence_proven: false,
            },
        ] {
            let (client, server) =
                fixture(Some(reply("200 OK", &serde_json::to_value(&response)?))).await?;
            assert_eq!(
                client
                    .provider_incoming_readback(&authorization, &request)
                    .await
                    .err(),
                Some(ClientError::Protocol)
            );
            let _request = server.await??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn lost_global_dispatch_is_unknown_and_never_automatically_retried()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(1))?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let request = read_request(&mut socket).await?;
            drop(socket);
            let second = tokio::time::timeout(Duration::from_millis(100), listener.accept()).await;
            Ok::<_, std::io::Error>((request, second.is_ok()))
        });
        assert_eq!(
            client
                .provider_message_dispatch(&HeaderValue::from_static(CREDENTIAL))
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::ProviderMessageDispatch
            })
        );
        let (request, retried) = server.await??;
        let request = String::from_utf8(request)?;
        assert!(
            request.starts_with("POST /internal/v1/provider-worker/messages/dispatch HTTP/1.1")
        );
        assert!(request.contains(CREDENTIAL));
        assert!(!retried);
        Ok(())
    }

    #[test]
    fn validators_reject_noncanonical_selection_and_provider_outbox_states() {
        let query = ProviderMessageSelectionQuery {
            after_message_id: None,
            limit: 1,
        };
        let observation = ProviderMessageSelectionObservation {
            messages: vec![message(id(60), ChatProviderStateWire::EffectPending)],
            next_message_id: Some(id(60)),
            retry_authorized: false,
            request_equivalence_proven: false,
        };
        assert!(valid_message_selection_observation(&observation, &query));

        let mut authorized = observation.clone();
        authorized.retry_authorized = true;
        assert!(!valid_message_selection_observation(&authorized, &query));

        let mut wrong_cursor = observation;
        wrong_cursor.next_message_id = Some(id(61));
        assert!(!valid_message_selection_observation(&wrong_cursor, &query));

        let partial_page_query = ProviderMessageSelectionQuery {
            after_message_id: None,
            limit: 2,
        };
        let partial_page_cursor = ProviderMessageSelectionObservation {
            messages: vec![message(id(60), ChatProviderStateWire::EffectPending)],
            next_message_id: Some(id(60)),
            retry_authorized: false,
            request_equivalence_proven: false,
        };
        assert!(!valid_message_selection_observation(
            &partial_page_cursor,
            &partial_page_query
        ));

        let mut delivered_without_receipt = message(id(62), ChatProviderStateWire::Delivered);
        delivered_without_receipt.receipt = None;
        assert!(!valid_provider_message_outbox(&delivered_without_receipt));

        let mut pending_with_receipt = message(id(63), ChatProviderStateWire::EffectPending);
        pending_with_receipt.receipt =
            Some(crate::wire::provider_worker::ProviderDeliveryReceiptWire {
                provider_turn_id: "turn".to_owned(),
                body: "reply".to_owned(),
            });
        assert!(!valid_provider_message_outbox(&pending_with_receipt));

        let mut inspected_after_update = message(id(64), ChatProviderStateWire::EffectPending);
        inspected_after_update.last_dispatch_at_ms = 13;
        inspected_after_update.updated_at_ms = 12;
        assert!(valid_provider_message_outbox(&inspected_after_update));
        assert!(valid_message_body(" \n "));

        let bad_provision = ProviderProvisionDispatchResponse {
            dispatch: None,
            reconciled: Some(
                crate::wire::provider_worker::ProviderProvisionMutationResponse {
                    workspace_id: id(1),
                    operation: operation(id(65), "prepared"),
                },
            ),
        };
        assert!(!valid_provision_dispatch_response(&bad_provision));
    }

    async fn read_request(socket: &mut tokio::net::TcpStream) -> Result<Vec<u8>, std::io::Error> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(header_end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or_default();
                if bytes.len() >= header_end + 4 + content_length {
                    return Ok(bytes);
                }
            }
        }
        Ok(bytes)
    }
}
