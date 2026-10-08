//! Bounded client for the confined file source reads.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::super::{ClientError, OrchestrationHttpClient};
use crate::wire::forge_reads::{
    FileEntryWire, FileListQueryRequest, FileListQueryResponse, FileReadSliceQueryRequest,
    FileReadSliceQueryResponse, TextSearchMatchWire, TextSearchQueryRequest,
    TextSearchQueryResponse,
};

const MAX_WORKSPACE_LOCATOR_BYTES: usize = 4096;
const MAX_PATH_TOKEN_BYTES: usize = 5464;
const MAX_QUERY_BYTES: usize = 4096;
const MAX_FILE_LIST_RESULTS: u32 = 512;
const MAX_TEXT_SEARCH_RESULTS: u32 = 256;
const MAX_FILE_OUTPUT_BYTES: u32 = 65_536;

impl OrchestrationHttpClient {
    /// Observe one bounded directory listing through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested scope and Activity identity or exceeds its bounds.
    pub async fn file_list(
        &self,
        authorization: &HeaderValue,
        request: &FileListQueryRequest,
    ) -> Result<FileListQueryResponse, ClientError> {
        validate_common(
            request.scope.workspace_id,
            request.session_id,
            request.activity_id,
        )?;
        validate_scope(&request.scope)?;
        validate_bound(request.max_results, MAX_FILE_LIST_RESULTS)?;
        validate_bound(request.max_output_bytes, MAX_FILE_OUTPUT_BYTES)?;
        validate_optional_token(request.directory.as_deref())?;
        let response: FileListQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/file-list",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !echoes_request(
            &response.scope,
            &response.activity_id,
            request.scope.workspace_id,
            request.scope.workspace_revision,
            &request.scope.locator,
            request.activity_id,
        ) || response.entries.len() > usize::try_from(request.max_results).unwrap_or(usize::MAX)
            || !response.entries.iter().all(valid_entry)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe one bounded recursive literal search through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested scope and Activity identity or exceeds its bounds.
    pub async fn text_search(
        &self,
        authorization: &HeaderValue,
        request: &TextSearchQueryRequest,
    ) -> Result<TextSearchQueryResponse, ClientError> {
        validate_common(
            request.scope.workspace_id,
            request.session_id,
            request.activity_id,
        )?;
        validate_scope(&request.scope)?;
        validate_bound(request.max_results, MAX_TEXT_SEARCH_RESULTS)?;
        validate_bound(request.max_output_bytes, MAX_FILE_OUTPUT_BYTES)?;
        validate_optional_token(request.root.as_deref())?;
        if request.query.is_empty() || request.query.len() > MAX_QUERY_BYTES {
            return Err(ClientError::InvalidRequest);
        }
        let response: TextSearchQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/text-search",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !echoes_request(
            &response.scope,
            &response.activity_id,
            request.scope.workspace_id,
            request.scope.workspace_revision,
            &request.scope.locator,
            request.activity_id,
        ) || response.matches.len() > usize::try_from(request.max_results).unwrap_or(usize::MAX)
            || !response.matches.iter().all(valid_match)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe one bounded source line slice through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested scope, Activity identity, path, and line range.
    pub async fn file_read_slice(
        &self,
        authorization: &HeaderValue,
        request: &FileReadSliceQueryRequest,
    ) -> Result<FileReadSliceQueryResponse, ClientError> {
        validate_common(
            request.scope.workspace_id,
            request.session_id,
            request.activity_id,
        )?;
        validate_scope(&request.scope)?;
        validate_token(request.path.as_str())?;
        if request.start_line == 0 || request.end_line < request.start_line {
            return Err(ClientError::InvalidRequest);
        }
        let response: FileReadSliceQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/file-read-slice",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !echoes_request(
            &response.scope,
            &response.activity_id,
            request.scope.workspace_id,
            request.scope.workspace_revision,
            &request.scope.locator,
            request.activity_id,
        ) || response.path != request.path
            || response.start_line != request.start_line
            || response.end_line != request.end_line
            || response.end_line < response.start_line
            || response.total_lines < response.end_line
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_common(
    workspace_id: Uuid,
    session_id: Option<Uuid>,
    activity_id: Uuid,
) -> Result<(), ClientError> {
    if workspace_id.is_nil() || activity_id.is_nil() || session_id.is_some_and(|id| id.is_nil()) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_scope(scope: &crate::wire::leadership::WorkspaceQueryScope) -> Result<(), ClientError> {
    if scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_WORKSPACE_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_bound(value: u32, maximum: u32) -> Result<(), ClientError> {
    if value == 0 || value > maximum {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_token(token: &str) -> Result<(), ClientError> {
    if token.is_empty() || token.len() > MAX_PATH_TOKEN_BYTES {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_optional_token(token: Option<&str>) -> Result<(), ClientError> {
    token.map(validate_token).transpose().map(|_| ())
}

fn echoes_request(
    scope: &crate::wire::forge_reads::SourceScopeWire,
    activity_id: &str,
    workspace_id: Uuid,
    workspace_revision: i64,
    locator: &str,
    request_activity_id: Uuid,
) -> bool {
    scope.workspace_id == workspace_id
        && scope.workspace_revision == workspace_revision
        && scope.locator == locator
        && Uuid::parse_str(activity_id).is_ok_and(|parsed| parsed == request_activity_id)
}

fn valid_entry(entry: &FileEntryWire) -> bool {
    !entry.path.is_empty()
        && !entry.display_path.is_empty()
        && entry.size_bytes.is_none_or(|size| size > 0)
}

fn valid_match(match_row: &TextSearchMatchWire) -> bool {
    !match_row.path.is_empty() && !match_row.display_path.is_empty() && match_row.line > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::forge_reads::{FileEntryKindWire, SourceScopeWire};
    use crate::wire::leadership::WorkspaceQueryScope;

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
    fn file_request_bounds_and_echo_contract_are_enforced() -> TestResult {
        let request = FileListQueryRequest {
            scope: scope(),
            session_id: None,
            activity_id: Uuid::from_u128(5),
            directory: None,
            max_results: 64,
            max_output_bytes: 4096,
        };
        validate_common(
            request.scope.workspace_id,
            request.session_id,
            request.activity_id,
        )?;
        validate_scope(&request.scope)?;
        validate_bound(request.max_results, MAX_FILE_LIST_RESULTS)?;
        validate_bound(request.max_output_bytes, MAX_FILE_OUTPUT_BYTES)?;

        let echoed = SourceScopeWire {
            workspace_id: request.scope.workspace_id,
            workspace_revision: request.scope.workspace_revision,
            locator: request.scope.locator.clone(),
        };
        assert!(echoes_request(
            &echoed,
            &request.activity_id.to_string(),
            request.scope.workspace_id,
            request.scope.workspace_revision,
            &request.scope.locator,
            request.activity_id
        ));

        let foreign = SourceScopeWire {
            workspace_id: Uuid::from_u128(9),
            workspace_revision: request.scope.workspace_revision,
            locator: request.scope.locator.clone(),
        };
        assert!(!echoes_request(
            &foreign,
            &request.activity_id.to_string(),
            request.scope.workspace_id,
            request.scope.workspace_revision,
            &request.scope.locator,
            request.activity_id
        ));

        assert!(validate_bound(0, MAX_FILE_LIST_RESULTS).is_err());
        assert!(validate_bound(MAX_FILE_LIST_RESULTS + 1, MAX_FILE_LIST_RESULTS).is_err());
        Ok(())
    }

    #[test]
    fn slice_and_match_contracts_are_enforced() {
        let entry = FileEntryWire {
            path: "cGF0aA".to_owned(),
            display_path: "path".to_owned(),
            display_path_truncated: false,
            kind: FileEntryKindWire::RegularFile,
            size_bytes: Some(12),
        };
        assert!(valid_entry(&entry));

        let match_row = TextSearchMatchWire {
            path: "cGF0aA".to_owned(),
            display_path: "path".to_owned(),
            display_path_truncated: false,
            line: 3,
            preview: "let x".to_owned(),
        };
        assert!(valid_match(&match_row));

        assert!(validate_token("cGF0aA").is_ok());
        assert!(validate_token("").is_err());
        let request = FileReadSliceQueryRequest {
            scope: scope(),
            session_id: None,
            activity_id: Uuid::from_u128(5),
            path: "cGF0aA".to_owned(),
            start_line: 2,
            end_line: 5,
        };
        assert!(request.end_line >= request.start_line);
    }
}
