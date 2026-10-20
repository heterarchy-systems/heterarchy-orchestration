//! Official no-retry client for Workspace `ChatGPT` Project bindings.

use heterarchy_orchestration_operations::WorkspaceProjectBindingRevision;
use hyper::Method;
use hyper::header::HeaderValue;
use std::str::FromStr;

use crate::wire::workspace_project::{
    WorkspaceProjectBindingReceiptWire, WorkspaceProjectBindingWire,
    WorkspaceProjectConfigureRequest, WorkspaceProjectConfigureResponse,
    WorkspaceProjectQueryRequest, WorkspaceProjectQueryResponse, WorkspaceProjectReadbackRequest,
    WorkspaceProjectReadbackResponse, valid_provider_project_id,
};

use super::membership::validate_scope;
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

impl OrchestrationHttpClient {
    /// Read one Workspace's current provider Project binding.
    ///
    /// # Errors
    /// Rejects malformed scopes, typed service failures, and foreign or
    /// malformed binding projections.
    pub async fn workspace_project_binding(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceProjectQueryRequest,
    ) -> Result<WorkspaceProjectQueryResponse, ClientError> {
        validate_scope(&request.scope)?;
        let response: WorkspaceProjectQueryResponse = self
            .call(
                authorization,
                Method::POST,
                path(request.scope.workspace_id, "query"),
                Some(request),
                None,
            )
            .await?;
        if response.workspace_id != request.scope.workspace_id
            || response
                .binding
                .as_ref()
                .is_some_and(|binding| !valid_binding(binding, request.scope.workspace_id))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Configure the exact Workspace Project binding once; uncertain replies
    /// carry the immutable receipt selector for a separate readback.
    ///
    /// # Errors
    /// Rejects malformed identities/revisions, typed service failures, or an
    /// uncertain response with the exact readback request. It never retries.
    pub async fn configure_workspace_project(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceProjectConfigureRequest,
    ) -> Result<WorkspaceProjectConfigureResponse, ClientError> {
        validate_configure_request(request)?;
        let readback = WorkspaceProjectReadbackRequest {
            request_id: request.request_id,
            scope: request.scope.clone(),
        };
        let response: WorkspaceProjectConfigureResponse = self
            .call(
                authorization,
                Method::POST,
                path(request.scope.workspace_id, "configure"),
                Some(request),
                Some(ReadbackTarget::WorkspaceProjectBinding(Box::new(readback))),
            )
            .await?;
        if !valid_configure_response(&response, request) {
            return Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::WorkspaceProjectBinding(Box::new(
                    WorkspaceProjectReadbackRequest {
                        request_id: request.request_id,
                        scope: request.scope.clone(),
                    },
                )),
            });
        }
        Ok(response)
    }

    /// Read the exact immutable configure receipt without granting retry.
    ///
    /// # Errors
    /// Rejects malformed selectors, typed service failures, and foreign
    /// receipt projections.
    pub async fn workspace_project_binding_readback(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceProjectReadbackRequest,
    ) -> Result<WorkspaceProjectReadbackResponse, ClientError> {
        validate_readback_request(request)?;
        let response: WorkspaceProjectReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                path(request.scope.workspace_id, "readback"),
                Some(request),
                None,
            )
            .await?;
        if response.retry_authorized
            || response.receipt.as_ref().is_some_and(|receipt| {
                !valid_receipt(receipt)
                    || receipt.request_id != request.request_id
                    || receipt.scope != request.scope
            })
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn path(workspace_id: uuid::Uuid, endpoint: &str) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/chatgpt-project/{endpoint}")
}

fn validate_configure_request(
    request: &WorkspaceProjectConfigureRequest,
) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.request_id)?;
    if !valid_provider_project_id(&request.project_id)
        || request
            .expected_revision
            .as_deref()
            .is_some_and(|value| WorkspaceProjectBindingRevision::from_str(value).is_err())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_readback_request(request: &WorkspaceProjectReadbackRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.request_id)
}

fn valid_binding(binding: &WorkspaceProjectBindingWire, workspace_id: uuid::Uuid) -> bool {
    binding.workspace_id == workspace_id
        && valid_provider_project_id(&binding.project_id)
        && WorkspaceProjectBindingRevision::from_str(&binding.revision).is_ok()
        && binding.configured_at_ms >= 0
}

fn valid_receipt(receipt: &WorkspaceProjectBindingReceiptWire) -> bool {
    receipt.request_id != uuid::Uuid::nil()
        && valid_scope_wire(&receipt.scope)
        && valid_provider_project_id(&receipt.project_id)
        && receipt
            .expected_revision
            .as_deref()
            .is_none_or(|value| WorkspaceProjectBindingRevision::from_str(value).is_ok())
        && receipt.project_id == receipt.binding.project_id
        && valid_binding(&receipt.binding, receipt.scope.workspace_id)
}

fn valid_configure_response(
    response: &WorkspaceProjectConfigureResponse,
    request: &WorkspaceProjectConfigureRequest,
) -> bool {
    let receipt = &response.receipt;
    if !valid_receipt(receipt)
        || receipt.request_id != request.request_id
        || receipt.scope != request.scope
        || receipt.expected_revision != request.expected_revision
        || receipt.project_id != request.project_id
        || !valid_binding(&response.current_binding, request.scope.workspace_id)
    {
        return false;
    }
    let expected_binding_revision = request
        .expected_revision
        .as_deref()
        .map(WorkspaceProjectBindingRevision::from_str)
        .transpose();
    let Ok(expected_binding_revision) = expected_binding_revision else {
        return false;
    };
    let receipt_revision =
        match WorkspaceProjectBindingRevision::from_str(&receipt.binding.revision) {
            Ok(value) => value.as_i64(),
            Err(_) => return false,
        };
    let expected_receipt_revision = expected_binding_revision
        .map_or(0, WorkspaceProjectBindingRevision::as_i64)
        .checked_add(1);
    let current_revision =
        match WorkspaceProjectBindingRevision::from_str(&response.current_binding.revision) {
            Ok(value) => value.as_i64(),
            Err(_) => return false,
        };
    expected_receipt_revision == Some(receipt_revision) && current_revision >= receipt_revision
}

fn valid_scope_wire(scope: &crate::wire::WorkspaceQueryScope) -> bool {
    validate_scope(scope).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::workspace_project::ProviderProjectReferenceWire;
    use uuid::Uuid;

    fn scope() -> crate::wire::WorkspaceQueryScope {
        crate::wire::WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "loc-2@3/project".to_owned(),
        }
    }

    fn binding(revision: &str) -> WorkspaceProjectBindingWire {
        WorkspaceProjectBindingWire {
            workspace_id: Uuid::from_u128(1),
            project_id: "g-p-6ad231f7ad7c8191bfc1d5ff782c171f".to_owned(),
            revision: revision.to_owned(),
            configured_at_ms: 10,
        }
    }

    #[test]
    fn configure_readback_accepts_exact_receipt_and_later_current_binding() {
        let request = WorkspaceProjectConfigureRequest {
            request_id: Uuid::from_u128(5),
            scope: scope(),
            expected_revision: Some("1".to_owned()),
            project_id: "g-p-7ad231f7ad7c8191bfc1d5ff782c171f".to_owned(),
        };
        let receipt = WorkspaceProjectBindingReceiptWire {
            request_id: request.request_id,
            scope: request.scope.clone(),
            expected_revision: request.expected_revision.clone(),
            project_id: request.project_id.clone(),
            binding: WorkspaceProjectBindingWire {
                project_id: request.project_id.clone(),
                revision: "2".to_owned(),
                ..binding("2")
            },
        };
        let response = WorkspaceProjectConfigureResponse {
            receipt,
            current_binding: binding("3"),
            replayed: true,
            work_authorized: false,
        };
        assert!(valid_configure_response(&response, &request));

        let wrong_route = ProviderProjectReferenceWire {
            project_id: request.project_id,
            binding_revision: "02".to_owned(),
        };
        assert!(!crate::wire::workspace_project::valid_provider_project_reference(&wrong_route));
    }

    #[test]
    fn configure_response_rejects_forged_revision_and_receipt() {
        let request = WorkspaceProjectConfigureRequest {
            request_id: Uuid::from_u128(5),
            scope: scope(),
            expected_revision: None,
            project_id: "g-p-6ad231f7ad7c8191bfc1d5ff782c171f".to_owned(),
        };
        let receipt = WorkspaceProjectBindingReceiptWire {
            request_id: request.request_id,
            scope: request.scope.clone(),
            expected_revision: None,
            project_id: request.project_id.clone(),
            binding: binding("1"),
        };
        let mut response = WorkspaceProjectConfigureResponse {
            receipt,
            current_binding: binding("1"),
            replayed: false,
            work_authorized: true,
        };
        assert!(valid_configure_response(&response, &request));
        response.receipt.binding.revision = "01".to_owned();
        assert!(!valid_configure_response(&response, &request));
    }
}
