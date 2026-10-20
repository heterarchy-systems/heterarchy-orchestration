//! Official client for scoped Agent provision operation inspection/cancellation.

use hyper::Method;
use hyper::header::HeaderValue;

use crate::wire::WorkspaceQueryScope;
use crate::wire::provisioning_operations::{
    ProvisionOperationActorPrincipalWire, ProvisionOperationCancelRequest,
    ProvisionOperationCancelResponse, ProvisionOperationQueryRequest,
    ProvisionOperationQueryResponse, ProvisionOperationUncertaintyRetirementRequest,
    ProvisionOperationUncertaintyRetirementResponse, ProvisionOperationWire,
};
use crate::wire::workspace_project::valid_provider_project_reference;

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

    /// Retire recovery for one unobserved provider effect. Lost or malformed
    /// replies carry the exact operation readback identity; this method never
    /// retries the mutation.
    ///
    /// # Errors
    /// Returns invalid selectors, typed rejection, or an uncertain outcome
    /// carrying the exact operation identity.
    pub async fn retire_unobserved_provision_operation(
        &self,
        authorization: &HeaderValue,
        request: &ProvisionOperationUncertaintyRetirementRequest,
    ) -> Result<ProvisionOperationUncertaintyRetirementResponse, ClientError> {
        validate_retirement_request(request)?;
        let readback = ReadbackTarget::ProvisionOperation {
            workspace_id: request.scope.workspace_id,
            operation_id: request.operation_id,
        };
        let path = format!(
            "/internal/v1/workspaces/{}/provision-operations/retire-unobserved",
            request.scope.workspace_id
        );
        let response: ProvisionOperationUncertaintyRetirementResponse = self
            .call(
                authorization,
                Method::POST,
                path,
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_retirement_response(&response, request) {
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

fn validate_retirement_request(
    request: &ProvisionOperationUncertaintyRetirementRequest,
) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.retirement_id)?;
    require_identity(request.operation_id)?;
    if request.expected_revision == 0 || request.recovery_version != 1 {
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

fn valid_retirement_response(
    response: &ProvisionOperationUncertaintyRetirementResponse,
    request: &ProvisionOperationUncertaintyRetirementRequest,
) -> bool {
    response.scope == request.scope
        && valid_operation(&response.operation)
        && response.operation.operation_id == request.operation_id
        && response.operation.uncertainty_retirement.as_ref() == Some(&response.retirement)
        && response.retirement.retirement_id == request.retirement_id
        && response.retirement.operation_id == request.operation_id
        && response.retirement.workspace_id == request.scope.workspace_id
        && response.retirement.requester_session_id == request.session_id
        && response.retirement.expected_unknown_revision == request.expected_revision
        && response.retirement.recovery_version == request.recovery_version
        && request
            .expected_revision
            .checked_add(1)
            .is_some_and(|revision| {
                response.retirement.retired_operation_revision == revision
                    && response.operation.revision == revision
            })
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
        && valid_uncertainty_retirement(operation)
        && operation
            .project
            .as_ref()
            .is_none_or(valid_provider_project_reference)
}

fn valid_uncertainty_retirement(operation: &ProvisionOperationWire) -> bool {
    operation
        .uncertainty_retirement
        .as_ref()
        .is_none_or(|retirement| {
            operation.effect_kind == "provision_new"
                && operation.state == "effect_unknown"
                && retirement.operation_id == operation.operation_id
                && !retirement.retirement_id.is_nil()
                && !retirement.workspace_id.is_nil()
                && !retirement.requester_session_id.is_nil()
                && retirement.expected_unknown_revision > 0
                && retirement
                    .expected_unknown_revision
                    .checked_add(1)
                    .is_some_and(|revision| {
                        retirement.retired_operation_revision == revision
                            && operation.revision == revision
                    })
                && retirement.recovery_version == 1
                && retirement.retired_at_ms >= 0
                && operation.updated_at_ms == retirement.retired_at_ms
                && valid_principal(&retirement.actor_principal)
                && valid_digest(&retirement.intent_digest)
        })
}

fn valid_principal(principal: &ProvisionOperationActorPrincipalWire) -> bool {
    match principal {
        ProvisionOperationActorPrincipalWire::TrustedStdio => true,
        ProvisionOperationActorPrincipalWire::DirectToken { token_id } => !token_id.is_nil(),
        ProvisionOperationActorPrincipalWire::OAuthGrant { grant_id } => !grant_id.is_nil(),
    }
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
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
    use crate::wire::provisioning_operations::ProvisionOperationUncertaintyRetirementWire;
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
            "uncertaintyRetirement",
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
            uncertainty_retirement: None,
            project: None,
        }
    }

    fn retirement(
        request: &ProvisionOperationUncertaintyRetirementRequest,
    ) -> ProvisionOperationUncertaintyRetirementWire {
        ProvisionOperationUncertaintyRetirementWire {
            retirement_id: request.retirement_id,
            operation_id: request.operation_id,
            workspace_id: request.scope.workspace_id,
            requester_session_id: request.session_id,
            expected_unknown_revision: request.expected_revision,
            retired_operation_revision: request.expected_revision + 1,
            recovery_version: request.recovery_version,
            retired_at_ms: 20,
            actor_principal: ProvisionOperationActorPrincipalWire::DirectToken {
                token_id: Uuid::from_u128(6),
            },
            intent_digest: "ab".repeat(32),
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

    #[tokio::test]
    async fn malformed_retirement_exchange_returns_exact_operation_readback()
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
        let request = ProvisionOperationUncertaintyRetirementRequest {
            scope: scope(),
            session_id: Uuid::from_u128(5),
            retirement_id: Uuid::from_u128(7),
            operation_id: Uuid::from_u128(3),
            expected_revision: 4,
            recovery_version: 1,
        };
        assert_eq!(
            client
                .retire_unobserved_provision_operation(
                    &HeaderValue::from_static("Bearer fixture"),
                    &request,
                )
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
    fn retirement_response_requires_exact_receipt_and_nullable_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = ProvisionOperationUncertaintyRetirementRequest {
            scope: scope(),
            session_id: Uuid::from_u128(5),
            retirement_id: Uuid::from_u128(7),
            operation_id: Uuid::from_u128(3),
            expected_revision: 4,
            recovery_version: 1,
        };
        let receipt = retirement(&request);
        let mut retired = operation("effect_unknown", 5);
        retired.updated_at_ms = receipt.retired_at_ms;
        retired.uncertainty_retirement = Some(receipt.clone());
        let response = ProvisionOperationUncertaintyRetirementResponse {
            scope: request.scope.clone(),
            operation: retired,
            retirement: receipt,
            changed: true,
        };
        assert!(valid_retirement_response(&response, &request));
        let encoded = serde_json::to_value(&response)?;
        assert_eq!(
            encoded["retirement"]["actorPrincipal"]["kind"],
            "direct_token"
        );
        assert_eq!(
            encoded["retirement"]["actorPrincipal"]["tokenId"],
            Uuid::from_u128(6).to_string()
        );
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
