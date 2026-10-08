//! One exact read; no automatic retry, actor construction or permission grant.

use super::super::{ClientError, OrchestrationHttpClient};
use crate::wire::forge_reads::{CoordinatorGetRequest, CoordinatorGetResponse};
use hyper::{Method, header::HeaderValue};

impl OrchestrationHttpClient {
    /// Read the current Forge planning binding, preserving explicit absence.
    ///
    /// # Errors
    /// Returns invalid selectors, typed rejection, bounded transport failure,
    /// or mismatched/invalid Source observation. This read grants no authority.
    pub async fn coordinator_get(
        &self,
        authorization: &HeaderValue,
        request: &CoordinatorGetRequest,
    ) -> Result<CoordinatorGetResponse, ClientError> {
        crate::app_support::validate_workspace_reference(&request.scope, request.session_id)
            .map_err(|_| ClientError::InvalidRequest)?;
        if request.activity_id.is_nil() {
            return Err(ClientError::InvalidRequest);
        }
        let response: CoordinatorGetResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/coordinator-get",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.scope != request.scope
            || response.activity_id != request.activity_id
            || response.coordinator.as_ref().is_some_and(|binding| {
                binding.workspace_id != request.scope.workspace_id
                    || binding.activity_id != request.activity_id
                    || binding.agent_id.is_nil()
                    || binding.revision <= 0
                    || binding.updated_at_ms < 0
            })
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests;
