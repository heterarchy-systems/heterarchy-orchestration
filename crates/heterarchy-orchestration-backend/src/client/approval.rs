//! Official client for bounded approval policy and request projections.

use heterarchy_orchestration_operations::{MAX_REQUEST_TTL_MS, MIN_REQUEST_TTL_MS};
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::approval::{
    ApprovalAdmissionWire, ApprovalAutoScopeWire, ApprovalDecideRequest, ApprovalDecideResponse,
    ApprovalListQueryRequest, ApprovalPolicyQueryRequest, ApprovalPolicySetRequest,
    ApprovalPolicyWire, ApprovalRequestGetRequest, ApprovalRequestGetResponse, ApprovalRequestWire,
    ApprovalSubmitRequest, ApprovalSubmitResponse, ListQueryResponse, PolicyQueryResponse,
    SetResponse,
};

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_APPROVAL_SCOPES: usize = 16;
const MAX_APPROVAL_LIST_LIMIT: u32 = 64;
const MAX_DIGEST_HEX_BYTES: usize = 64;

impl OrchestrationHttpClient {
    /// Read one Workspace approval policy, preserving an absent policy as
    /// `policy: None` in the successful response.
    ///
    /// # Errors
    /// Returns bounded request/response protocol failures, typed service
    /// rejection, or bounded transport failures.
    pub async fn approval_policy(
        &self,
        authorization: &HeaderValue,
        request: &ApprovalPolicyQueryRequest,
    ) -> Result<Option<ApprovalPolicyWire>, ClientError> {
        validate_policy_query(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/approval-policy/query",
            request.scope.workspace_id
        );
        let response: PolicyQueryResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if !valid_policy_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response.policy)
    }

    /// Read a bounded approval-request page for one Workspace.
    ///
    /// # Errors
    /// Returns bounded request/response protocol failures, typed service
    /// rejection, or bounded transport failures.
    pub async fn approval_requests(
        &self,
        authorization: &HeaderValue,
        request: &ApprovalListQueryRequest,
    ) -> Result<Vec<ApprovalRequestWire>, ClientError> {
        validate_list_query(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/approval-requests/query",
            request.scope.workspace_id
        );
        let response: ListQueryResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if !valid_list_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response.requests)
    }

    /// Compare-and-set one Workspace approval policy.
    ///
    /// The mutation is never retried. If its response is uncertain, the
    /// returned error identifies the Workspace and the only revision that can
    /// prove this exact replacement.
    ///
    /// # Errors
    /// Returns bounded request failures, typed service rejection, or an
    /// uncertain outcome carrying a canonical policy readback identity.
    pub async fn set_approval_policy(
        &self,
        authorization: &HeaderValue,
        request: &ApprovalPolicySetRequest,
    ) -> Result<ApprovalPolicyWire, ClientError> {
        validate_policy_set(request)?;
        let expected_revision = request
            .expected_revision
            .unwrap_or_default()
            .checked_add(1)
            .ok_or(ClientError::InvalidRequest)?;
        let readback = ReadbackTarget::WorkspacePolicy {
            workspace_id: request.scope.workspace_id,
            expected_revision,
        };
        let path = format!(
            "/internal/v1/workspaces/{}/approval-policy/set",
            request.scope.workspace_id
        );
        let response: SetResponse = self
            .call(
                authorization,
                Method::POST,
                path,
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_set_response(&response, request, expected_revision) {
            return Err(ClientError::UnknownOutcome {
                readback: readback.clone(),
            });
        }
        Ok(response.policy)
    }

    /// Submit one caller-scoped approval request. The stable request ID is the
    /// durable replay identity; a later replay may return a terminal decision
    /// and a higher request revision.
    ///
    /// # Errors
    /// Returns bounded request failures, typed pre-effect rejection, or an
    /// uncertain outcome carrying the exact request readback identity.
    pub async fn submit_approval(
        &self,
        authorization: &HeaderValue,
        request: &ApprovalSubmitRequest,
    ) -> Result<ApprovalSubmitResponse, ClientError> {
        validate_submit_request(request)?;
        let readback = ReadbackTarget::ApprovalRequest {
            workspace_id: request.scope.workspace_id,
            request_id: request.request_id,
        };
        let path = format!(
            "/internal/v1/workspaces/{}/approval-requests/submit",
            request.scope.workspace_id
        );
        let response: ApprovalSubmitResponse = self
            .call(
                authorization,
                Method::POST,
                path,
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_submit_response(&response, request) {
            return Err(ClientError::UnknownOutcome {
                readback: readback.clone(),
            });
        }
        Ok(response)
    }

    /// Decide one approval request for the caller's validated session.
    /// Decisions have no replay nonce; an uncertain exchange carries an exact
    /// request observation identity and must be resolved by readback.
    ///
    /// # Errors
    /// Returns bounded request failures, typed pre-effect rejection, or an
    /// uncertain outcome carrying the exact request readback identity.
    pub async fn decide_approval(
        &self,
        authorization: &HeaderValue,
        request: &ApprovalDecideRequest,
    ) -> Result<ApprovalDecideResponse, ClientError> {
        validate_decide_request(request)?;
        let readback = ReadbackTarget::ApprovalRequest {
            workspace_id: request.scope.workspace_id,
            request_id: request.request_id,
        };
        let path = format!(
            "/internal/v1/workspaces/{}/approval-requests/decision",
            request.scope.workspace_id
        );
        let response: ApprovalDecideResponse = self
            .call(
                authorization,
                Method::POST,
                path,
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_decide_response(&response, request) {
            return Err(ClientError::UnknownOutcome {
                readback: readback.clone(),
            });
        }
        Ok(response)
    }

    /// Read one exact approval request and its historical admission metadata.
    /// An absent request must carry no admission metadata.
    ///
    /// # Errors
    /// Returns bounded request/response protocol failures, typed service
    /// rejection, or bounded transport failures.
    pub async fn approval_request(
        &self,
        authorization: &HeaderValue,
        request: &ApprovalRequestGetRequest,
    ) -> Result<ApprovalRequestGetResponse, ClientError> {
        validate_request_get(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/approval-requests/get",
            request.scope.workspace_id
        );
        let response: ApprovalRequestGetResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if !valid_request_get_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_policy_query(request: &ApprovalPolicyQueryRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    if request.session_id.is_some_and(|id| id.is_nil()) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_list_query(request: &ApprovalListQueryRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    if request.session_id.is_some_and(|id| id.is_nil())
        || !(1..=MAX_APPROVAL_LIST_LIMIT).contains(&request.limit)
        || request
            .state
            .as_deref()
            .is_some_and(|state| !valid_state_filter(state))
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_policy_set(request: &ApprovalPolicySetRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    if request
        .expected_revision
        .is_some_and(|revision| revision == 0)
    {
        return Err(ClientError::InvalidRequest);
    }
    validate_auto_scopes(&request.auto_scopes)
}

fn validate_submit_request(request: &ApprovalSubmitRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.request_id)?;
    if !valid_action(&request.action)
        || !valid_lower_hex_digest(&request.request_digest)
        || !(MIN_REQUEST_TTL_MS..=MAX_REQUEST_TTL_MS).contains(&request.ttl_ms)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_decide_request(request: &ApprovalDecideRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.request_id)?;
    if !matches!(request.decision.as_str(), "approve" | "reject" | "cancel") {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_request_get(request: &ApprovalRequestGetRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.request_id)?;
    if request.session_id.is_some_and(|id| id.is_nil()) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_scope(scope: &crate::wire::WorkspaceQueryScope) -> Result<(), ClientError> {
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

fn validate_auto_scopes(scopes: &[ApprovalAutoScopeWire]) -> Result<(), ClientError> {
    if scopes.len() > MAX_APPROVAL_SCOPES
        || scopes.iter().any(|scope| {
            scope.scope_id.is_nil()
                || scope.action.is_empty()
                || scope.action.chars().any(char::is_control)
                || !valid_action(&scope.action)
                || scope.expires_at_ms < 0
                || scope
                    .max_uses
                    .is_some_and(|uses| !(1..=1_000).contains(&uses))
        })
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_policy_query_response(
    response: &PolicyQueryResponse,
    request: &ApprovalPolicyQueryRequest,
) -> bool {
    response.scope == request.scope
        && response
            .policy
            .as_ref()
            .is_none_or(|policy| valid_policy(policy, request.scope.workspace_id, None))
}

fn valid_list_query_response(
    response: &ListQueryResponse,
    request: &ApprovalListQueryRequest,
) -> bool {
    let Ok(limit) = usize::try_from(request.limit) else {
        return false;
    };
    response.scope == request.scope
        && response.requests.len() <= limit
        && response.requests.iter().all(|item| {
            valid_request(item, request.scope.workspace_id)
                && request
                    .state
                    .as_ref()
                    .is_none_or(|state| item.state == *state)
        })
}

fn valid_set_response(
    response: &SetResponse,
    request: &ApprovalPolicySetRequest,
    expected_revision: u64,
) -> bool {
    response.scope == request.scope
        && valid_policy(
            &response.policy,
            request.scope.workspace_id,
            Some(expected_revision),
        )
        && response.policy.enabled == request.enabled
        && response.policy.auto_scopes == request.auto_scopes
}

fn valid_submit_response(
    response: &ApprovalSubmitResponse,
    request: &ApprovalSubmitRequest,
) -> bool {
    response.scope == request.scope
        && valid_submitted_request(&response.request, request, response.replay)
        && if response.replay {
            valid_admission_outcome(&response.request, response.admission.as_ref())
        } else {
            valid_initial_submit_response(&response.request, response.admission.as_ref())
        }
}

fn valid_initial_submit_response(
    request: &ApprovalRequestWire,
    admission: Option<&ApprovalAdmissionWire>,
) -> bool {
    request.revision == 1
        && match request.state.as_str() {
            "pending" => admission.is_none() && valid_submit_state(request),
            "approved" => {
                request.auto_scope_id.is_some()
                    && valid_submit_state(request)
                    && valid_admission_outcome(request, admission)
            }
            _ => false,
        }
}

fn valid_decide_response(
    response: &ApprovalDecideResponse,
    request: &ApprovalDecideRequest,
) -> bool {
    response.scope == request.scope
        && valid_request(&response.request, request.scope.workspace_id)
        && response.request.request_id == request.request_id
        && valid_decision_state(&response.request, request)
        && valid_admission_outcome(&response.request, response.admission.as_ref())
}

fn valid_request_get_response(
    response: &ApprovalRequestGetResponse,
    request: &ApprovalRequestGetRequest,
) -> bool {
    if response.scope != request.scope {
        return false;
    }
    match response.request.as_ref() {
        None => response.admission.is_none(),
        Some(value) => {
            valid_request(value, request.scope.workspace_id)
                && value.request_id == request.request_id
                && valid_admission_outcome(value, response.admission.as_ref())
        }
    }
}

fn valid_submitted_request(
    value: &ApprovalRequestWire,
    request: &ApprovalSubmitRequest,
    replay: bool,
) -> bool {
    valid_request_for_session(value, request.scope.workspace_id, Some(request.session_id))
        && value.request_id == request.request_id
        && value.action == request.action
        && value.request_digest == request.request_digest
        && value.requested_permissions == request.requested_permissions
        && (replay
            || value.expires_at_ms.checked_sub(value.requested_at_ms) == Some(request.ttl_ms))
}

fn valid_request_for_session(
    value: &ApprovalRequestWire,
    workspace_id: Uuid,
    session_id: Option<Uuid>,
) -> bool {
    valid_request(value, workspace_id)
        && session_id.is_none_or(|session_id| value.session_id == session_id)
}

fn valid_decision_state(value: &ApprovalRequestWire, request: &ApprovalDecideRequest) -> bool {
    let committed_decision = value.revision >= 2 && value.decided_at_ms.is_some();
    match request.decision.as_str() {
        "approve" => {
            committed_decision
                && value.state == "approved"
                && value.auto_scope_id.is_none()
                && value.decided_by == Some(request.session_id)
        }
        "reject" => {
            committed_decision
                && value.state == "rejected"
                && value.auto_scope_id.is_none()
                && value.decided_by == Some(request.session_id)
        }
        "cancel" => committed_decision && value.state == "cancelled" && value.decided_by.is_none(),
        _ => false,
    }
}

fn valid_admission_outcome(
    request: &ApprovalRequestWire,
    admission: Option<&ApprovalAdmissionWire>,
) -> bool {
    let requires_admission = matches!(request.state.as_str(), "approved" | "auto_approved");
    match (requires_admission, admission) {
        (true, Some(admission)) => valid_admission(admission, request),
        (false, None) => true,
        _ => false,
    }
}

fn valid_admission(admission: &ApprovalAdmissionWire, request: &ApprovalRequestWire) -> bool {
    !admission.admission_id.is_nil()
        && admission.request_id == request.request_id
        && admission.workspace_id == request.workspace_id
        && canonical_uuid_string(&admission.issuer)
        && canonical_uuid_string(&admission.backend_id)
        && request.decided_at_ms == Some(admission.issued_at_ms)
        && admission.issued_at_ms >= request.requested_at_ms
        && admission.expires_at_ms > admission.issued_at_ms
}

fn canonical_uuid_string(value: &str) -> bool {
    Uuid::parse_str(value)
        .ok()
        .filter(|id| !id.is_nil())
        .is_some_and(|id| id.to_string() == value)
}

fn valid_policy(
    policy: &ApprovalPolicyWire,
    workspace_id: Uuid,
    expected_revision: Option<u64>,
) -> bool {
    policy.workspace_id == workspace_id
        && !policy.workspace_id.is_nil()
        && policy.revision > 0
        && expected_revision.is_none_or(|revision| policy.revision == revision)
        && policy.created_at_ms >= 0
        && policy.updated_at_ms >= policy.created_at_ms
        && validate_auto_scopes(&policy.auto_scopes).is_ok()
}

fn valid_request(value: &ApprovalRequestWire, workspace_id: Uuid) -> bool {
    value.request_id != Uuid::nil()
        && value.revision > 0
        && value.workspace_id == workspace_id
        && value.session_id != Uuid::nil()
        && valid_action(&value.action)
        && valid_state(&value.state)
        && value.policy_revision.is_none_or(|revision| revision > 0)
        && value.auto_scope_id.is_none_or(|id| !id.is_nil())
        && value.requested_at_ms >= 0
        && value.expires_at_ms > value.requested_at_ms
        && value
            .decided_at_ms
            .is_none_or(|decided| decided >= value.requested_at_ms)
        && value.decided_by.is_none_or(|id| !id.is_nil())
        && (value.state == "pending" || value.decided_at_ms.is_some())
        && (value.state != "approved" || value.decided_by.is_some())
        && value
            .auto_scope_id
            .is_none_or(|_| value.state == "approved")
        && valid_lower_hex_digest(&value.request_digest)
}

fn valid_action(value: &str) -> bool {
    matches!(
        value,
        "direct_structured"
            | "discovered_project_command"
            | "shell_escape"
            | "analyzed_shell"
            | "generated_project_command"
    )
}

fn valid_state(value: &str) -> bool {
    matches!(
        value,
        "pending"
            | "approved"
            | "rejected"
            | "auto_approved"
            | "denied"
            | "cancelled"
            | "expired"
            | "superseded"
    )
}

fn valid_state_filter(value: &str) -> bool {
    !value.is_empty() && valid_state(value)
}

fn valid_submit_state(value: &ApprovalRequestWire) -> bool {
    match value.state.as_str() {
        "pending" => {
            value.decided_at_ms.is_none()
                && value.decided_by.is_none()
                && value.auto_scope_id.is_none()
        }
        "approved" => match value.auto_scope_id {
            Some(scope_id) => {
                value.decided_at_ms == Some(value.requested_at_ms)
                    && value.decided_by == Some(scope_id)
            }
            None => value.decided_at_ms.is_some() && value.decided_by.is_some(),
        },
        "rejected" | "denied" => {
            value.decided_at_ms.is_some()
                && value.decided_by.is_some()
                && value.auto_scope_id.is_none()
        }
        "auto_approved" => value.auto_scope_id.is_some_and(|scope_id| {
            value.decided_at_ms == Some(value.requested_at_ms) && value.decided_by == Some(scope_id)
        }),
        "cancelled" | "expired" => {
            value.decided_at_ms.is_some()
                && value.decided_by.is_none()
                && value.auto_scope_id.is_none()
        }
        "superseded" => true,
        _ => valid_state(&value.state),
    }
}

fn valid_lower_hex_digest(value: &str) -> bool {
    value.len() == MAX_DIGEST_HEX_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::wire::WorkspaceQueryScope;
    use crate::wire::approval::{
        ApprovalAdmissionWire, ApprovalAutoScopeWire, ApprovalDecideRequest,
        ApprovalDecideResponse, ApprovalListQueryRequest, ApprovalPermissionWire,
        ApprovalPolicySetRequest, ApprovalRequestGetRequest, ApprovalRequestGetResponse,
        ApprovalRequestWire, ApprovalSubmitRequest, ApprovalSubmitResponse, ListQueryResponse,
    };

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 1,
            workspace_revision: 1,
            locator: "workspace".to_owned(),
        }
    }

    fn auto_scope() -> ApprovalAutoScopeWire {
        ApprovalAutoScopeWire {
            scope_id: Uuid::from_u128(3),
            action: "direct_structured".to_owned(),
            ceiling: ApprovalPermissionWire {
                workspace_read: true,
                workspace_write: false,
                network: false,
            },
            max_uses: Some(1),
            expires_at_ms: 10,
        }
    }

    fn policy_set_request() -> ApprovalPolicySetRequest {
        ApprovalPolicySetRequest {
            scope: scope(),
            session_id: Uuid::from_u128(4),
            enabled: true,
            auto_scopes: vec![auto_scope()],
            expected_revision: None,
        }
    }

    fn submit_request() -> ApprovalSubmitRequest {
        ApprovalSubmitRequest {
            scope: scope(),
            session_id: Uuid::from_u128(4),
            request_id: Uuid::from_u128(5),
            action: "direct_structured".to_owned(),
            request_digest: "ab".repeat(32),
            requested_permissions: ApprovalPermissionWire {
                workspace_read: true,
                workspace_write: false,
                network: false,
            },
            ttl_ms: 1_000,
        }
    }

    fn decision_request(decision: &str) -> ApprovalDecideRequest {
        ApprovalDecideRequest {
            scope: scope(),
            session_id: Uuid::from_u128(4),
            request_id: Uuid::from_u128(5),
            decision: decision.to_owned(),
        }
    }

    fn request_wire(state: &str, session_id: Uuid) -> ApprovalRequestWire {
        let (decided_at_ms, decided_by, auto_scope_id) = match state {
            "approved" | "rejected" => (Some(20), Some(session_id), None),
            "auto_approved" => (Some(10), Some(Uuid::from_u128(6)), Some(Uuid::from_u128(3))),
            "cancelled" | "expired" => (Some(20), None, None),
            _ => (None, None, None),
        };
        ApprovalRequestWire {
            request_id: Uuid::from_u128(5),
            revision: 4,
            workspace_id: scope().workspace_id,
            action: "direct_structured".to_owned(),
            session_id,
            state: state.to_owned(),
            requested_permissions: ApprovalPermissionWire {
                workspace_read: true,
                workspace_write: false,
                network: false,
            },
            policy_revision: None,
            auto_scope_id,
            requested_at_ms: 10,
            expires_at_ms: 1_010,
            decided_at_ms,
            decided_by,
            request_digest: "ab".repeat(32),
        }
    }

    fn admission_wire(request: &ApprovalRequestWire) -> ApprovalAdmissionWire {
        ApprovalAdmissionWire {
            admission_id: Uuid::from_u128(7),
            request_id: request.request_id,
            workspace_id: request.workspace_id,
            issuer: Uuid::from_u128(8).to_string(),
            backend_id: Uuid::from_u128(9).to_string(),
            issued_at_ms: request.decided_at_ms.unwrap_or(request.requested_at_ms),
            expires_at_ms: request.decided_at_ms.unwrap_or(request.requested_at_ms) + 80,
        }
    }

    #[test]
    fn policy_set_request_keeps_scope_and_policy_bounds() {
        let mut request = policy_set_request();
        assert!(validate_policy_set(&request).is_ok());

        request.scope.locator = "x".repeat(MAX_LOCATOR_BYTES + 1);
        assert_eq!(
            validate_policy_set(&request),
            Err(ClientError::InvalidRequest)
        );

        request.scope = scope();
        request.auto_scopes = (0..=MAX_APPROVAL_SCOPES)
            .map(|index| {
                let mut value = auto_scope();
                value.scope_id = Uuid::from_u128(index as u128 + 10);
                value
            })
            .collect();
        assert_eq!(
            validate_policy_set(&request),
            Err(ClientError::InvalidRequest)
        );
    }

    #[test]
    fn digest_validation_requires_lower_case_hex() {
        assert!(valid_lower_hex_digest(&"a".repeat(MAX_DIGEST_HEX_BYTES)));
        assert!(!valid_lower_hex_digest(&"A".repeat(MAX_DIGEST_HEX_BYTES)));
        assert!(!valid_lower_hex_digest(
            &"a".repeat(MAX_DIGEST_HEX_BYTES - 1)
        ));
    }

    #[test]
    fn submit_replay_accepts_terminal_request_and_historical_admission() {
        let request = submit_request();
        let projected = request_wire("approved", request.session_id);
        let response = ApprovalSubmitResponse {
            scope: request.scope.clone(),
            request: projected.clone(),
            admission: Some(admission_wire(&projected)),
            replay: true,
        };
        assert!(valid_submit_response(&response, &request));
        let mut changed_ttl = request.clone();
        changed_ttl.ttl_ms = 20_000;
        assert!(valid_submitted_request(
            &response.request,
            &changed_ttl,
            true
        ));
        assert!(!valid_submitted_request(
            &response.request,
            &changed_ttl,
            false
        ));
        let mut fresh_terminal = response.clone();
        fresh_terminal.replay = false;
        assert!(!valid_submit_response(&fresh_terminal, &request));

        let mut auto = request_wire("approved", request.session_id);
        auto.revision = 1;
        auto.auto_scope_id = Some(Uuid::from_u128(3));
        auto.decided_at_ms = Some(auto.requested_at_ms);
        auto.decided_by = auto.auto_scope_id;
        let response = ApprovalSubmitResponse {
            scope: request.scope.clone(),
            request: auto.clone(),
            admission: Some(admission_wire(&auto)),
            replay: false,
        };
        assert!(valid_submit_response(&response, &request));

        let projected = request_wire("rejected", request.session_id);
        let response = ApprovalSubmitResponse {
            scope: request.scope.clone(),
            request: projected,
            admission: None,
            replay: true,
        };
        assert!(valid_submit_response(&response, &request));
    }

    #[test]
    fn decision_response_requires_session_attribution_and_admission_binding() {
        let request = decision_request("approve");
        let mut projected = request_wire("approved", Uuid::from_u128(5));
        projected.decided_by = Some(request.session_id);
        let response = ApprovalDecideResponse {
            scope: request.scope.clone(),
            request: projected.clone(),
            admission: Some(admission_wire(&projected)),
        };
        assert!(valid_decide_response(&response, &request));

        let mut response = response;
        response.request.decided_by = Some(Uuid::from_u128(10));
        assert!(!valid_decide_response(&response, &request));
        response.request.decided_by = Some(request.session_id);
        response.admission = None;
        assert!(!valid_decide_response(&response, &request));

        let cancel = decision_request("cancel");
        let cancelled = request_wire("cancelled", Uuid::from_u128(6));
        assert!(valid_decide_response(
            &ApprovalDecideResponse {
                scope: cancel.scope.clone(),
                request: cancelled,
                admission: None,
            },
            &cancel
        ));
    }

    #[test]
    fn request_get_rejects_dangling_admission_and_accepts_absence() {
        let request = ApprovalRequestGetRequest {
            scope: scope(),
            session_id: None,
            request_id: Uuid::from_u128(5),
        };
        assert!(valid_request_get_response(
            &ApprovalRequestGetResponse {
                scope: request.scope.clone(),
                request: None,
                admission: None,
            },
            &request
        ));
        assert!(!valid_request_get_response(
            &ApprovalRequestGetResponse {
                scope: request.scope.clone(),
                request: None,
                admission: Some(admission_wire(&request_wire(
                    "approved",
                    Uuid::from_u128(4),
                ))),
            },
            &request
        ));

        let mut malformed = request_wire("rejected", Uuid::from_u128(4));
        malformed.decided_at_ms = None;
        assert!(!valid_request_get_response(
            &ApprovalRequestGetResponse {
                scope: request.scope.clone(),
                request: Some(malformed),
                admission: None,
            },
            &request
        ));

        let projected = request_wire("approved", Uuid::from_u128(4));
        assert!(valid_request_get_response(
            &ApprovalRequestGetResponse {
                scope: request.scope,
                request: Some(projected.clone()),
                admission: Some(admission_wire(&projected)),
            },
            &ApprovalRequestGetRequest {
                scope: scope(),
                session_id: Some(Uuid::from_u128(6)),
                request_id: Uuid::from_u128(5),
            }
        ));
    }

    #[test]
    fn scope_unavailable_is_known_rejection_but_other_mutation_server_errors_are_unknown() {
        let readback = ReadbackTarget::WorkspacePolicy {
            workspace_id: scope().workspace_id,
            expected_revision: 1,
        };
        let failure = br#"{"code":"WORKSPACE_SCOPE_UNAVAILABLE","retryable":true}"#;
        assert_eq!(
            super::super::service_error(
                hyper::StatusCode::SERVICE_UNAVAILABLE,
                failure,
                Some(readback.clone())
            ),
            ClientError::Rejected {
                status: 503,
                code: super::super::ServiceCode::ScopeUnavailable
            }
        );
        assert_eq!(
            super::super::service_error(
                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"code":"ORCHESTRATION_UNAVAILABLE","retryable":true}"#,
                Some(readback.clone())
            ),
            ClientError::UnknownOutcome {
                readback: readback.clone()
            }
        );
    }

    #[test]
    fn list_response_requires_scope_and_filter_but_not_selected_session() {
        let request = ApprovalListQueryRequest {
            scope: scope(),
            session_id: Some(Uuid::from_u128(4)),
            state: Some("pending".to_owned()),
            limit: 1,
        };
        let mut response = ListQueryResponse {
            scope: request.scope.clone(),
            requests: vec![ApprovalRequestWire {
                request_id: Uuid::from_u128(5),
                revision: 1,
                workspace_id: request.scope.workspace_id,
                action: "direct_structured".to_owned(),
                session_id: Uuid::from_u128(4),
                state: "pending".to_owned(),
                requested_permissions: ApprovalPermissionWire {
                    workspace_read: true,
                    workspace_write: false,
                    network: false,
                },
                policy_revision: None,
                auto_scope_id: None,
                requested_at_ms: 1,
                expires_at_ms: 2,
                decided_at_ms: None,
                decided_by: None,
                request_digest: "a".repeat(MAX_DIGEST_HEX_BYTES),
            }],
        };
        assert!(valid_list_query_response(&response, &request));

        response.scope.workspace_revision += 1;
        assert!(!valid_list_query_response(&response, &request));
        response.scope = request.scope.clone();
        response.requests[0].session_id = Uuid::from_u128(6);
        assert!(valid_list_query_response(&response, &request));
        response.requests[0].state = "rejected".to_owned();
        assert!(!valid_list_query_response(&response, &request));
    }

    #[tokio::test]
    async fn uncertain_policy_set_keeps_workspace_result_revision_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let client =
            OrchestrationHttpClient::new("127.0.0.1:1".parse()?, Duration::from_millis(50))?;
        let request = policy_set_request();
        assert_eq!(
            client
                .set_approval_policy(&HeaderValue::from_static("Bearer fixture"), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::WorkspacePolicy {
                    workspace_id: request.scope.workspace_id,
                    expected_revision: 1,
                }
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn uncertain_submit_keeps_exact_request_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let client =
            OrchestrationHttpClient::new("127.0.0.1:1".parse()?, Duration::from_millis(50))?;
        let request = submit_request();
        assert_eq!(
            client
                .submit_approval(&HeaderValue::from_static("Bearer fixture"), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::ApprovalRequest {
                    workspace_id: request.scope.workspace_id,
                    request_id: request.request_id,
                }
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_decision_response_is_unknown_with_request_observation()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(1))?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await?;
            let body = b"{}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(response.as_bytes()).await?;
            socket.write_all(body).await?;
            Ok::<_, std::io::Error>(())
        });
        let request = decision_request("approve");
        assert_eq!(
            client
                .decide_approval(&HeaderValue::from_static("Bearer fixture"), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::ApprovalRequest {
                    workspace_id: request.scope.workspace_id,
                    request_id: request.request_id,
                }
            })
        );
        server.await??;
        Ok(())
    }
}
