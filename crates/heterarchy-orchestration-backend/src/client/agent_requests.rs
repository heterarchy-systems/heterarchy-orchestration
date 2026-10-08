//! Bounded client for source-scoped `AgentRequest` reads.

use std::collections::HashSet;

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{ClientError, OrchestrationHttpClient};
use crate::wire::WorkspaceQueryScope;
use crate::wire::agent_requests::{
    AgentRequestListQueryRequest, AgentRequestListQueryResponse, AgentRequestQueryRequest,
    AgentRequestQueryResponse, AgentRequestWire, MAX_AGENT_REQUEST_LIST_LIMIT,
};

const MAX_LOCATOR_BYTES: usize = 4096;
const MAX_OBJECTIVE_BYTES: usize = 4096;
const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_REASON_BYTES: usize = 2048;

impl OrchestrationHttpClient {
    /// Read one `AgentRequest` in an exact source-scoped `Workspace`.
    ///
    /// # Errors
    /// Returns invalid request input, a typed service rejection, a bounded
    /// transport failure, or a protocol error for mismatched response identity.
    pub async fn agent_request(
        &self,
        authorization: &HeaderValue,
        request: &AgentRequestQueryRequest,
    ) -> Result<AgentRequestQueryResponse, ClientError> {
        validate_scope(&request.scope, request.session_id)?;
        require_identity(request.request_id)?;
        let response: AgentRequestQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/agent-requests/query",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_agent_request_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read one bounded, newest-first `AgentRequest` page in a source scope.
    ///
    /// # Errors
    /// Returns invalid request input, a typed service rejection, a bounded
    /// transport failure, or a protocol error for a malformed page.
    pub async fn agent_requests(
        &self,
        authorization: &HeaderValue,
        request: &AgentRequestListQueryRequest,
    ) -> Result<AgentRequestListQueryResponse, ClientError> {
        validate_scope(&request.scope, request.session_id)?;
        if request.limit == 0 || request.limit > MAX_AGENT_REQUEST_LIST_LIMIT {
            return Err(ClientError::InvalidRequest);
        }
        let response: AgentRequestListQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/agent-requests/list/query",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_agent_request_list_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_scope(
    scope: &WorkspaceQueryScope,
    session_id: Option<Uuid>,
) -> Result<(), ClientError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
        || session_id.is_some_and(|id| id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn require_identity(value: Uuid) -> Result<(), ClientError> {
    if value.is_nil() {
        Err(ClientError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn valid_agent_request_response(
    response: &AgentRequestQueryResponse,
    request: &AgentRequestQueryRequest,
) -> bool {
    response.scope == request.scope
        && response.request.as_ref().is_none_or(|value| {
            value.request_id == request.request_id
                && valid_agent_request(value, request.scope.workspace_id)
        })
}

fn valid_agent_request_list_response(
    response: &AgentRequestListQueryResponse,
    request: &AgentRequestListQueryRequest,
) -> bool {
    let Ok(limit) = usize::try_from(request.limit) else {
        return false;
    };
    if response.scope != request.scope
        || response.items.len() > limit
        || response.truncated && response.items.len() != limit
    {
        return false;
    }

    let mut request_ids = HashSet::with_capacity(response.items.len());
    for item in &response.items {
        if !valid_agent_request(item, request.scope.workspace_id)
            || !request_ids.insert(item.request_id)
        {
            return false;
        }
    }
    response.items.windows(2).all(|pair| {
        pair[0].created_at_ms > pair[1].created_at_ms
            || (pair[0].created_at_ms == pair[1].created_at_ms
                && pair[0].request_id < pair[1].request_id)
    })
}

fn valid_agent_request(value: &AgentRequestWire, workspace_id: Uuid) -> bool {
    value.request_id != Uuid::nil()
        && value.workspace_id == workspace_id.to_string()
        && canonical_non_nil_uuid(&value.activity_id)
        && !value.objective.trim().is_empty()
        && value.objective.len() <= MAX_OBJECTIVE_BYTES
        && !value.capability.trim().is_empty()
        && value.capability.len() <= MAX_CAPABILITY_BYTES
        && (1..=4).contains(&value.requested_count)
        && !value.reason.trim().is_empty()
        && value.reason.len() <= MAX_REASON_BYTES
        && value.created_at_ms >= 0
        && match value.state.as_str() {
            "resolution_pending" => value.revision == 1,
            "resolution_reserved" => value.revision == 2,
            "fulfilled" => value.revision == 3,
            _ => false,
        }
}

fn canonical_non_nil_uuid(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|parsed| !parsed.is_nil() && parsed.to_string() == value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::agent_requests::{AgentRequestListQueryResponse, AgentRequestQueryResponse};

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn request(request_id: Uuid, created_at_ms: i64) -> AgentRequestWire {
        AgentRequestWire {
            request_id,
            workspace_id: scope().workspace_id.to_string(),
            activity_id: Uuid::from_u128(5).to_string(),
            objective: "verify an independent capability".to_owned(),
            capability: "verification".to_owned(),
            requested_count: 2,
            reason: "additional evidence is useful".to_owned(),
            state: "resolution_pending".to_owned(),
            created_at_ms,
            revision: 1,
        }
    }

    #[test]
    fn detail_query_rejects_wrong_scope_request_id_and_unknown_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = AgentRequestQueryRequest {
            scope: scope(),
            session_id: Some(Uuid::from_u128(6)),
            request_id: Uuid::from_u128(7),
        };
        let response = AgentRequestQueryResponse {
            scope: query.scope.clone(),
            request: Some(request(query.request_id, 10)),
        };
        assert!(valid_agent_request_response(&response, &query));

        let mut foreign = response.clone();
        foreign
            .request
            .as_mut()
            .ok_or("detail missing")?
            .workspace_id = Uuid::from_u128(99).to_string();
        assert!(!valid_agent_request_response(&foreign, &query));

        let mut wrong_id = response.clone();
        wrong_id
            .request
            .as_mut()
            .ok_or("detail missing")?
            .request_id = Uuid::from_u128(8);
        assert!(!valid_agent_request_response(&wrong_id, &query));

        let mut unknown_status = response;
        unknown_status
            .request
            .as_mut()
            .ok_or("detail missing")?
            .state = "cancelled".to_owned();
        assert!(!valid_agent_request_response(&unknown_status, &query));
        assert!(require_identity(Uuid::nil()).is_err());
        let mut invalid_scope = scope();
        invalid_scope.workspace_revision = 0;
        assert!(validate_scope(&invalid_scope, None).is_err());
        Ok(())
    }

    #[test]
    fn list_query_rejects_out_of_order_duplicate_foreign_and_over_limit_items() {
        let query = AgentRequestListQueryRequest {
            scope: scope(),
            session_id: None,
            limit: 2,
        };
        let response = AgentRequestListQueryResponse {
            scope: query.scope.clone(),
            items: vec![
                request(Uuid::from_u128(9), 20),
                request(Uuid::from_u128(10), 10),
            ],
            truncated: false,
        };
        assert!(valid_agent_request_list_response(&response, &query));

        let mut full_truncated_page = response.clone();
        full_truncated_page.truncated = true;
        assert!(valid_agent_request_list_response(
            &full_truncated_page,
            &query
        ));

        let mut partial_truncated_page = response.clone();
        let _ = partial_truncated_page.items.pop();
        partial_truncated_page.truncated = true;
        assert!(!valid_agent_request_list_response(
            &partial_truncated_page,
            &query
        ));

        let mut out_of_order = response.clone();
        out_of_order.items.swap(0, 1);
        assert!(!valid_agent_request_list_response(&out_of_order, &query));

        let mut duplicate = response.clone();
        duplicate.items[1].request_id = duplicate.items[0].request_id;
        assert!(!valid_agent_request_list_response(&duplicate, &query));

        let mut foreign = response.clone();
        foreign.items[0].workspace_id = Uuid::from_u128(99).to_string();
        assert!(!valid_agent_request_list_response(&foreign, &query));

        let mut over_limit = response;
        over_limit.items.push(request(Uuid::from_u128(11), 0));
        assert!(!valid_agent_request_list_response(&over_limit, &query));
    }

    #[test]
    fn query_wire_requires_nullable_detail_field_and_closes_revision_state_pairs()
    -> Result<(), Box<dyn std::error::Error>> {
        let scope = scope();
        let response = AgentRequestQueryResponse {
            scope,
            request: None,
        };
        let mut encoded = serde_json::to_value(response)?;
        encoded
            .as_object_mut()
            .ok_or("response object missing")?
            .remove("request");
        assert!(serde_json::from_value::<AgentRequestQueryResponse>(encoded).is_err());

        let mut bad_revision = request(Uuid::from_u128(12), 10);
        bad_revision.state = "fulfilled".to_owned();
        assert!(!valid_agent_request(&bad_revision, Uuid::from_u128(1)));
        let mut bad_time = request(Uuid::from_u128(13), 10);
        bad_time.created_at_ms = -1;
        assert!(!valid_agent_request(&bad_time, Uuid::from_u128(1)));
        Ok(())
    }
}
