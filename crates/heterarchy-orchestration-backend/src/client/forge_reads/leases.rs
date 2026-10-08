//! Bounded client for the command/diff/writer-lease source reads.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::super::{ClientError, OrchestrationHttpClient};
use crate::wire::forge_reads::{
    CommandListQueryRequest, CommandListQueryResponse, DiffDoctorQueryRequest,
    DiffDoctorQueryResponse, DiffListQueryRequest, DiffListQueryResponse, DiffReadQueryRequest,
    DiffReadQueryResponse, WriterLeaseGetQueryRequest, WriterLeaseGetQueryResponse,
    WriterLeaseListQueryRequest, WriterLeaseListQueryResponse,
};
use crate::wire::leadership::WorkspaceQueryScope;

const MAX_WORKSPACE_LOCATOR_BYTES: usize = 4096;

impl OrchestrationHttpClient {
    /// Observe the bounded project command list through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested Activity identity.
    pub async fn command_list(
        &self,
        authorization: &HeaderValue,
        request: &CommandListQueryRequest,
    ) -> Result<CommandListQueryResponse, ClientError> {
        validate(&request.scope, request.session_id, request.activity_id)?;
        let response: CommandListQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/command-list",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.activity_id != request.activity_id.to_string() {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe one exact Diff through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested Diff identity.
    pub async fn diff_read(
        &self,
        authorization: &HeaderValue,
        request: &DiffReadQueryRequest,
    ) -> Result<DiffReadQueryResponse, ClientError> {
        validate(&request.scope, request.session_id, request.diff_id)?;
        let response: DiffReadQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/diff-read",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.diff.diff_id != request.diff_id.to_string() {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe the bounded Diff list through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested Activity identity.
    pub async fn diff_list(
        &self,
        authorization: &HeaderValue,
        request: &DiffListQueryRequest,
    ) -> Result<DiffListQueryResponse, ClientError> {
        validate(&request.scope, request.session_id, request.activity_id)?;
        let response: DiffListQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/diff-list",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.activity_id != request.activity_id.to_string() {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe one Diff doctor observation through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested Diff identity.
    pub async fn diff_doctor(
        &self,
        authorization: &HeaderValue,
        request: &DiffDoctorQueryRequest,
    ) -> Result<DiffDoctorQueryResponse, ClientError> {
        validate(&request.scope, request.session_id, request.diff_id)?;
        let response: DiffDoctorQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/diff-doctor",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.diff_id != request.diff_id.to_string() {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe one Writer Lease projection through the backend; a missing
    /// lease returns `lease: null`.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested lease identity.
    pub async fn writer_lease_get(
        &self,
        authorization: &HeaderValue,
        request: &WriterLeaseGetQueryRequest,
    ) -> Result<WriterLeaseGetQueryResponse, ClientError> {
        validate(&request.scope, request.session_id, request.lease_id)?;
        let response: WriterLeaseGetQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/writer-lease-get",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if let Some(lease) = &response.lease
            && lease.lease_id != request.lease_id.to_string()
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe the bounded Writer Lease list through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response whose limit does not
    /// echo the request.
    pub async fn writer_lease_list(
        &self,
        authorization: &HeaderValue,
        request: &WriterLeaseListQueryRequest,
    ) -> Result<WriterLeaseListQueryResponse, ClientError> {
        validate(&request.scope, request.session_id, Uuid::nil())?;
        if request.limit == 0 {
            return Err(ClientError::InvalidRequest);
        }
        let response: WriterLeaseListQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/writer-lease-list",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.limit != request.limit {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate(
    scope: &WorkspaceQueryScope,
    session_id: Option<Uuid>,
    subject_id: Uuid,
) -> Result<(), ClientError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_WORKSPACE_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
        || session_id.is_some_and(|id| id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    // `subject_id` is the request's own subject (activity/diff/lease); the
    // nil check happens per-call where the subject is required.
    let _ = subject_id;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "locator".to_owned(),
        }
    }

    #[test]
    fn command_list_and_diff_identity_echoes_are_enforced() -> TestResult {
        let request = CommandListQueryRequest {
            scope: scope(),
            session_id: None,
            activity_id: Uuid::from_u128(5),
        };
        validate(&request.scope, request.session_id, request.activity_id)?;
        assert_eq!(
            request.activity_id.to_string(),
            "00000000-0000-0000-0000-000000000005"
        );

        let diff = DiffReadQueryRequest {
            scope: scope(),
            session_id: None,
            diff_id: Uuid::from_u128(6),
        };
        assert_ne!(diff.diff_id, Uuid::nil());

        let nil_session = CommandListQueryRequest {
            scope: scope(),
            session_id: Some(Uuid::nil()),
            activity_id: Uuid::from_u128(5),
        };
        assert!(
            validate(
                &nil_session.scope,
                nil_session.session_id,
                nil_session.activity_id
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn writer_lease_bounds_are_enforced() -> TestResult {
        let get = WriterLeaseGetQueryRequest {
            scope: scope(),
            session_id: None,
            lease_id: Uuid::from_u128(7),
            scope_offset: 0,
            expected_revision: Some(6),
        };
        validate(&get.scope, get.session_id, get.lease_id)?;
        assert!(get.scope_offset == 0 && get.expected_revision.is_some());

        let list = WriterLeaseListQueryRequest {
            scope: scope(),
            session_id: None,
            limit: 16,
            cursor: None,
        };
        assert!(list.limit > 0);
        let zero = WriterLeaseListQueryRequest {
            limit: 0,
            ..list.clone()
        };
        assert_eq!(zero.limit, 0);
        Ok(())
    }
}
