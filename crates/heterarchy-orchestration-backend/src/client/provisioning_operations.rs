//! Official client for scoped Agent provision operation inspection/cancellation.

use hyper::Method;
use hyper::header::HeaderValue;

use crate::wire::WorkspaceQueryScope;
use crate::wire::provisioning_operations::{
    ProvisionOperationCancelRequest, ProvisionOperationCancelResponse,
    ProvisionOperationQueryRequest, ProvisionOperationQueryResponse, ProvisionOperationWire,
};

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_SLOT_ORDINAL: u8 = 3;
const MAX_PROVIDER_CONVERSATION_ID_BYTES: usize = 512;

impl OrchestrationHttpClient {
    /// Read one exact source-fenced Agent provision operation.
    ///
    /// # Errors
    /// Returns invalid selectors, typed service rejection, bounded transport
    /// failures, or a malformed operation projection.
    pub async fn provision_operation(
        &self,
        authorization: &HeaderValue,
        request: &ProvisionOperationQueryRequest,
    ) -> Result<ProvisionOperationQueryResponse, ClientError> {
        validate_query_request(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/provision-operations/query",
            request.scope.workspace_id
        );
        let response: ProvisionOperationQueryResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if response.scope != request.scope
            || response.operation.as_ref().is_some_and(|operation| {
                !valid_operation(operation) || operation.operation_id != request.operation_id
            })
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Cancel one prepared operation. Lost or malformed replies carry the
    /// operation identity for observation; this method never retries.
    ///
    /// # Errors
    /// Returns invalid selectors, typed rejection, or an uncertain outcome
    /// carrying the exact operation readback identity.
    pub async fn cancel_provision_operation(
        &self,
        authorization: &HeaderValue,
        request: &ProvisionOperationCancelRequest,
    ) -> Result<ProvisionOperationCancelResponse, ClientError> {
        validate_cancel_request(request)?;
        let readback = ReadbackTarget::ProvisionOperation {
            workspace_id: request.scope.workspace_id,
            operation_id: request.operation_id,
        };
        let path = format!(
            "/internal/v1/workspaces/{}/provision-operations/cancel",
            request.scope.workspace_id
        );
        let response: ProvisionOperationCancelResponse = self
            .call(
                authorization,
                Method::POST,
                path,
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_cancel_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }
}

fn validate_query_request(request: &ProvisionOperationQueryRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.operation_id)?;
    if request.session_id.is_some_and(|id| id.is_nil()) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_cancel_request(request: &ProvisionOperationCancelRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.operation_id)?;
    if request.expected_revision == 0 {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_scope(scope: &WorkspaceQueryScope) -> Result<(), ClientError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_cancel_response(
    response: &ProvisionOperationCancelResponse,
    request: &ProvisionOperationCancelRequest,
) -> bool {
    response.scope == request.scope
        && valid_operation(&response.operation)
        && response.operation.operation_id == request.operation_id
        && if response.changed {
            request
                .expected_revision
                .checked_add(1)
                .is_some_and(|revision| response.operation.revision == revision)
                && response.operation.state == "cancelled"
        } else {
            response.operation.state == "cancelled"
        }
}

pub(super) fn valid_operation(operation: &ProvisionOperationWire) -> bool {
    !operation.operation_id.is_nil()
        && !operation.request_id.is_nil()
        && operation.slot_ordinal <= MAX_SLOT_ORDINAL
        && valid_effect_kind(&operation.effect_kind)
        && operation.provider_family == "chatgpt"
        && operation.profile_id.is_none_or(|id| !id.is_nil())
        && operation.runtime_session_id.is_none_or(|id| !id.is_nil())
        && valid_state(&operation.state)
        && valid_conversation(operation)
        && valid_failure(operation)
        && operation.prepared_at_ms >= 0
        && operation.updated_at_ms >= operation.prepared_at_ms
        && operation.revision > 0
}

fn valid_effect_kind(value: &str) -> bool {
    matches!(
        value,
        "wake_existing_runtime" | "wake_new_runtime" | "provision_new" | "reuse_ready_runtime"
    )
}

fn valid_state(value: &str) -> bool {
    matches!(
        value,
        "prepared"
            | "effect_pending"
            | "effect_unknown"
            | "observed"
            | "confirmed"
            | "failed"
            | "cancelled"
    )
}

pub(super) fn valid_conversation(operation: &ProvisionOperationWire) -> bool {
    operation.provider_conversation_id.is_some()
        == matches!(operation.state.as_str(), "observed" | "confirmed")
        && operation
            .provider_conversation_id
            .as_deref()
            .is_none_or(|value| {
                !value.is_empty() && value.len() <= MAX_PROVIDER_CONVERSATION_ID_BYTES
            })
}

pub(super) fn valid_failure(operation: &ProvisionOperationWire) -> bool {
    operation
        .reported_failure_code
        .as_deref()
        .is_none_or(|value| operation.state == "failed" && valid_failure_code(value))
}

fn valid_failure_code(value: &str) -> bool {
    matches!(
        value,
        "tab_load_timeout"
            | "document_observation_failed"
            | "document_identity_changed"
            | "document_identity_unavailable"
            | "script_injection_failed"
            | "composer_unavailable"
            | "composer_form_unavailable"
            | "composer_busy"
            | "composer_not_empty"
            | "composer_focus_failed"
            | "composer_selection_failed"
            | "composer_insert_failed"
            | "composer_body_mismatch"
            | "send_button_unavailable"
            | "composer_ready_timeout"
            | "unknown_adapter_failure"
            | "surface_open_failed"
            | "journal_write_failed"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 1,
            workspace_revision: 2,
            locator: "workspace".to_owned(),
        }
    }

    #[test]
    fn operation_query_requires_explicit_nullable_observations()
    -> Result<(), Box<dyn std::error::Error>> {
        let baseline = serde_json::to_value(ProvisionOperationQueryResponse {
            scope: scope(),
            operation: Some(operation("prepared", 1)),
        })?;
        assert!(
            serde_json::from_value::<ProvisionOperationQueryResponse>(baseline.clone()).is_ok()
        );
        for field in [
            "profileId",
            "runtimeSessionId",
            "providerConversationId",
            "reportedFailureCode",
        ] {
            let mut reply = baseline.clone();
            let _removed = reply["operation"]
                .as_object_mut()
                .ok_or("operation object")?
                .remove(field);
            assert!(serde_json::from_value::<ProvisionOperationQueryResponse>(reply).is_err());
        }
        let mut missing = baseline;
        let _removed = missing
            .as_object_mut()
            .ok_or("query object")?
            .remove("operation");
        assert!(serde_json::from_value::<ProvisionOperationQueryResponse>(missing).is_err());
        Ok(())
    }

    fn operation(state: &str, revision: u64) -> ProvisionOperationWire {
        ProvisionOperationWire {
            operation_id: Uuid::from_u128(3),
            request_id: Uuid::from_u128(4),
            slot_ordinal: 0,
            effect_kind: "provision_new".to_owned(),
            provider_family: "chatgpt".to_owned(),
            profile_id: None,
            runtime_session_id: None,
            state: state.to_owned(),
            provider_conversation_id: None,
            reported_failure_code: None,
            prepared_at_ms: 10,
            updated_at_ms: 10,
            revision,
        }
    }

    #[test]
    fn query_rejects_malformed_operation_projection() {
        let request = ProvisionOperationQueryRequest {
            scope: scope(),
            session_id: Some(Uuid::from_u128(5)),
            operation_id: Uuid::from_u128(3),
        };
        let response = ProvisionOperationQueryResponse {
            scope: request.scope.clone(),
            operation: Some(operation("prepared", 1)),
        };
        assert!(response.operation.as_ref().is_some_and(valid_operation));
        let mut malformed = response;
        if let Some(operation) = malformed.operation.as_mut() {
            operation.state = "running".to_owned();
        }
        assert!(!malformed.operation.as_ref().is_some_and(valid_operation));
    }

    #[test]
    fn cancel_changed_and_noop_revisions_have_distinct_validation() {
        let request = ProvisionOperationCancelRequest {
            scope: scope(),
            session_id: Uuid::from_u128(5),
            operation_id: Uuid::from_u128(3),
            expected_revision: 1,
        };
        let changed = ProvisionOperationCancelResponse {
            scope: request.scope.clone(),
            operation: operation("cancelled", 2),
            changed: true,
        };
        assert!(valid_cancel_response(&changed, &request));
        let noop = ProvisionOperationCancelResponse {
            scope: request.scope.clone(),
            operation: operation("cancelled", 99),
            changed: false,
        };
        assert!(valid_cancel_response(&noop, &request));
    }

    #[tokio::test]
    async fn malformed_cancel_exchange_returns_exact_operation_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(
            listener.local_addr()?,
            std::time::Duration::from_secs(1),
        )?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await?;
            let body = b"{}";
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await?;
            socket.write_all(body).await?;
            Ok::<_, std::io::Error>(())
        });
        let request = ProvisionOperationCancelRequest {
            scope: scope(),
            session_id: Uuid::from_u128(5),
            operation_id: Uuid::from_u128(3),
            expected_revision: 1,
        };
        assert_eq!(
            client
                .cancel_provision_operation(&HeaderValue::from_static("Bearer fixture"), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::ProvisionOperation {
                    workspace_id: request.scope.workspace_id,
                    operation_id: request.operation_id,
                }
            })
        );
        server.await??;
        Ok(())
    }

    #[test]
    fn zero_cancel_revision_is_rejected_before_network() {
        let request = ProvisionOperationCancelRequest {
            scope: scope(),
            session_id: Uuid::from_u128(5),
            operation_id: Uuid::from_u128(3),
            expected_revision: 0,
        };
        assert_eq!(
            validate_cancel_request(&request),
            Err(ClientError::InvalidRequest)
        );
    }
}
