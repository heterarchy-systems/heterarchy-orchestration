//! Bounded client calls for TF consent and reservation lifecycle.

use hyper::Method;
use hyper::header::HeaderValue;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::temporary_dispatch_lifecycle as wire;

impl OrchestrationHttpClient {
    /// Submit one exact lifecycle mutation. A lost or malformed reply produces
    /// an exact readback target and is never retried automatically.
    ///
    /// # Errors
    /// Returns invalid input, bounded transport error, typed service rejection,
    /// unknown mutation outcome, or response-correlation failure.
    pub async fn temporary_dispatch_lifecycle_mutate(
        &self,
        authorization: &HeaderValue,
        request: &wire::TemporaryDispatchLifecycleMutationRequest,
    ) -> Result<wire::TemporaryDispatchLifecycleMutationResponse, ClientError> {
        if !wire::valid_mutation_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback =
            ReadbackTarget::TemporaryDispatchLifecycleMutation(Box::new(request.clone()));
        let response: wire::TemporaryDispatchLifecycleMutationResponse = self
            .call(
                authorization,
                Method::POST,
                mutation_path(request.actor.scope.workspace_id),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_mutation_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Resolve one exact mutation using a currently authenticated actor Session.
    /// The original request remains unchanged after a Session rebind.
    ///
    /// # Errors
    /// Returns invalid selector, bounded transport error, typed rejection, or
    /// a foreign/malformed readback.
    pub async fn temporary_dispatch_lifecycle_readback(
        &self,
        authorization: &HeaderValue,
        request: &wire::TemporaryDispatchLifecycleReadbackRequest,
    ) -> Result<wire::TemporaryDispatchLifecycleReadbackResponse, ClientError> {
        if !wire::valid_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TemporaryDispatchLifecycleReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                readback_path(request.current_actor.scope.workspace_id),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_readback_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn mutation_path(workspace_id: uuid::Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/temporary-dispatch/lifecycle")
}

fn readback_path(workspace_id: uuid::Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/temporary-dispatch/lifecycle/readback")
}
