//! Exact Coordinator handoff mutation and durable readback client operations.

use hyper::Method;
use hyper::header::HeaderValue;

use crate::wire::departure::{DepartureKindWire, DepartureReceiptWire};
use crate::wire::handoff::{
    HandoffPlanWire, HandoffQueryResponse, HandoffRequest, HandoffResponse,
};

use super::membership::validate_scope;
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

impl OrchestrationHttpClient {
    /// Transfer one exact Activity Coordinator binding through the caller's credential.
    ///
    /// # Errors
    /// Returns `UnknownOutcome` with the full handoff intent after an unverified reply.
    pub async fn member_handoff(
        &self,
        authorization: &HeaderValue,
        request: &HandoffRequest,
    ) -> Result<HandoffResponse, ClientError> {
        validate_handoff_request(request)?;
        let readback = handoff_readback(request);
        let response: HandoffResponse = self
            .call(
                authorization,
                Method::POST,
                handoff_path(request, "handoff"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.request != *request || !valid_handoff_receipt(&response.receipt, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one exact historical handoff receipt, including explicit absence.
    ///
    /// # Errors
    /// Returns `Protocol` for malformed or foreign readback; a query never manufactures an
    /// unknown mutation outcome.
    pub async fn member_handoff_query(
        &self,
        authorization: &HeaderValue,
        request: &HandoffRequest,
    ) -> Result<HandoffQueryResponse, ClientError> {
        validate_handoff_request(request)?;
        let response: HandoffQueryResponse = self
            .call(
                authorization,
                Method::POST,
                handoff_path(request, "handoff/query"),
                Some(request),
                None,
            )
            .await?;
        if response.request != *request
            || response
                .receipt
                .as_ref()
                .is_some_and(|receipt| !valid_handoff_receipt(receipt, request))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn handoff_path(request: &HandoffRequest, suffix: &str) -> String {
    format!(
        "/internal/v1/workspaces/{}/members/{suffix}",
        request.source.scope.workspace_id
    )
}

fn handoff_readback(request: &HandoffRequest) -> ReadbackTarget {
    ReadbackTarget::MemberHandoff(Box::new(request.clone()))
}

fn validate_handoff_request(request: &HandoffRequest) -> Result<(), ClientError> {
    validate_scope(&request.source.scope)?;
    require_identity(request.source.activity_id)?;
    require_identity(request.source.agent_id)?;
    require_identity(request.source.session_id)?;
    require_identity(request.target_agent_id)?;
    require_identity(request.target_session_id)?;
    require_identity(request.request_id)?;
    if request.target_agent_id == request.source.agent_id
        || request.target_session_id == request.source.session_id
        || !valid_revision_transition(request.source.expected_agent_revision)
        || !valid_revision_transition(request.expected_target_agent_revision)
        || !valid_revision_transition(request.expected_coordinator_revision)
        || !valid_handoff_plan(&request.expected_plan)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_revision_transition(expected: i64) -> bool {
    expected > 0 && expected.checked_add(1).is_some()
}

fn valid_handoff_plan(plan: &HandoffPlanWire) -> bool {
    let valid_goal = plan
        .goal
        .is_none_or(|goal| !goal.goal_id.is_nil() && goal.expected_revision > 0);
    let valid_dag_revision = plan.dag_revision.is_none_or(|revision| revision > 0);
    let valid_head_revision = plan.dag_head_revision.is_none_or(|revision| revision > 0);
    let valid_node = plan
        .node
        .is_none_or(|node| !node.node_id.is_nil() && node.expected_revision > 0);
    let valid_shape = !(plan.goal.is_none()
        && (plan.dag_revision.is_some()
            || plan.dag_head_revision.is_some()
            || plan.node.is_some()))
        && !(plan.dag_revision.is_none()
            && (plan.dag_head_revision.is_some() || plan.node.is_some()))
        && (plan.dag_revision.is_none() || plan.dag_head_revision.is_some());
    valid_goal && valid_dag_revision && valid_head_revision && valid_node && valid_shape
}

fn valid_handoff_receipt(receipt: &DepartureReceiptWire, request: &HandoffRequest) -> bool {
    let Some(result_agent_revision) = request.source.expected_agent_revision.checked_add(1) else {
        return false;
    };
    let Some(result_coordinator_revision) = request.expected_coordinator_revision.checked_add(1)
    else {
        return false;
    };
    let Some(target_participant_id) = receipt.target_participant_id else {
        return false;
    };
    let Some(started_event_id) = receipt.handoff_started_event_id else {
        return false;
    };
    let Some(completed_event_id) = receipt.handoff_completed_event_id else {
        return false;
    };

    receipt.request_id == request.request_id
        && receipt.workspace_id == request.source.scope.workspace_id
        && !receipt.participant_id.is_nil()
        && !target_participant_id.is_nil()
        && target_participant_id != receipt.participant_id
        && receipt.activity_id == request.source.activity_id
        && receipt.agent_id == request.source.agent_id
        && receipt.session_id == request.source.session_id
        && receipt.kind == DepartureKindWire::Handoff
        && receipt.expected_agent_revision == request.source.expected_agent_revision
        && receipt.result_agent_revision == result_agent_revision
        && receipt.target_agent_id == Some(request.target_agent_id)
        && receipt.target_session_id == Some(request.target_session_id)
        && receipt.result_coordinator_revision == Some(result_coordinator_revision)
        && receipt.departed_event_id.is_none()
        && !started_event_id.is_nil()
        && !completed_event_id.is_nil()
        && started_event_id != completed_event_id
        && receipt.membership_revision > 0
        && receipt.source_reconciled_at_ms >= 0
        && receipt.committed_at_ms >= receipt.source_reconciled_at_ms
        && valid_metadata(&receipt.display_name)
        && valid_metadata(&receipt.declared_role)
}

fn valid_metadata(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::WorkspaceQueryScope;
    use crate::wire::departure::{DepartureKindWire, DepartureTarget};
    use crate::wire::handoff::{HandoffGoalWire, HandoffNodeWire};
    use crate::wire::membership::MemberTransportWire;
    use uuid::Uuid;

    #[tokio::test]
    async fn wrong_handoff_http_echo_keeps_full_unknown_selector_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let mut wrong = request.clone();
        wrong.expected_target_agent_revision += 1;
        let body = serde_json::to_string(&HandoffResponse {
            request: wrong.clone(),
            receipt: receipt(&request),
            replayed: false,
        })?;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let (client, server) = super::super::tests::fixture(Some(response)).await?;
        let authorization = HeaderValue::from_static("Bearer handoff-test");
        assert_eq!(
            client.member_handoff(&authorization, &request).await.err(),
            Some(ClientError::UnknownOutcome {
                readback: handoff_readback(&request)
            })
        );
        let captured = server.await??;
        assert!(String::from_utf8(captured)?.starts_with("POST /internal/v1/workspaces/"));
        let body = serde_json::to_string(&HandoffQueryResponse {
            request: wrong,
            receipt: None,
        })?;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let (client, server) = super::super::tests::fixture(Some(response)).await?;
        assert_eq!(
            client
                .member_handoff_query(&authorization, &request)
                .await
                .err(),
            Some(ClientError::Protocol)
        );
        let _captured = server.await??;
        Ok(())
    }

    fn request() -> HandoffRequest {
        HandoffRequest {
            source: DepartureTarget {
                scope: WorkspaceQueryScope {
                    workspace_id: Uuid::from_u128(1),
                    location_id: Uuid::from_u128(2),
                    location_revision: 3,
                    workspace_revision: 4,
                    locator: "workspace".to_owned(),
                },
                activity_id: Uuid::from_u128(5),
                agent_id: Uuid::from_u128(6),
                session_id: Uuid::from_u128(7),
                expected_agent_revision: 8,
            },
            target_agent_id: Uuid::from_u128(9),
            target_session_id: Uuid::from_u128(10),
            expected_target_agent_revision: 11,
            expected_coordinator_revision: 12,
            expected_plan: HandoffPlanWire {
                goal: Some(HandoffGoalWire {
                    goal_id: Uuid::from_u128(13),
                    expected_revision: 14,
                }),
                dag_revision: Some(15),
                dag_head_revision: Some(16),
                node: Some(HandoffNodeWire {
                    node_id: Uuid::from_u128(17),
                    expected_revision: 18,
                }),
            },
            request_id: Uuid::from_u128(19),
        }
    }

    fn receipt(request: &HandoffRequest) -> DepartureReceiptWire {
        DepartureReceiptWire {
            request_id: request.request_id,
            workspace_id: request.source.scope.workspace_id,
            participant_id: Uuid::from_u128(20),
            activity_id: request.source.activity_id,
            agent_id: request.source.agent_id,
            session_id: request.source.session_id,
            kind: DepartureKindWire::Handoff,
            expected_agent_revision: request.source.expected_agent_revision,
            result_agent_revision: request.source.expected_agent_revision + 1,
            target_participant_id: Some(Uuid::from_u128(21)),
            target_agent_id: Some(request.target_agent_id),
            target_session_id: Some(request.target_session_id),
            result_coordinator_revision: Some(request.expected_coordinator_revision + 1),
            departed_event_id: None,
            handoff_started_event_id: Some(Uuid::from_u128(22)),
            handoff_completed_event_id: Some(Uuid::from_u128(23)),
            membership_revision: 2,
            presence_cleared: true,
            source_reconciled_at_ms: 24,
            committed_at_ms: 25,
            display_name: "Ada".to_owned(),
            declared_role: "reviewer".to_owned(),
            transport: MemberTransportWire::Mcp,
        }
    }

    #[test]
    fn handoff_request_validates_exact_distinct_bindings_and_closed_plan() {
        let value = request();
        assert!(validate_handoff_request(&value).is_ok());
        assert_eq!(
            handoff_path(&value, "handoff"),
            "/internal/v1/workspaces/00000000-0000-0000-0000-000000000001/members/handoff"
        );
        assert_eq!(
            handoff_path(&value, "handoff/query"),
            "/internal/v1/workspaces/00000000-0000-0000-0000-000000000001/members/handoff/query"
        );

        let mut invalid = value.clone();
        invalid.target_agent_id = invalid.source.agent_id;
        assert_eq!(
            validate_handoff_request(&invalid),
            Err(ClientError::InvalidRequest)
        );

        let mut invalid = value.clone();
        invalid.expected_plan.goal = None;
        assert_eq!(
            validate_handoff_request(&invalid),
            Err(ClientError::InvalidRequest)
        );

        let mut invalid = value.clone();
        invalid.expected_target_agent_revision = 0;
        assert_eq!(
            validate_handoff_request(&invalid),
            Err(ClientError::InvalidRequest)
        );

        let mut invalid = value.clone();
        invalid.expected_coordinator_revision = i64::MAX;
        assert_eq!(
            validate_handoff_request(&invalid),
            Err(ClientError::InvalidRequest)
        );

        let mut invalid = value;
        invalid.request_id = Uuid::nil();
        assert_eq!(
            validate_handoff_request(&invalid),
            Err(ClientError::InvalidRequest)
        );
    }

    #[test]
    fn readback_target_and_handoff_receipt_bind_the_complete_request() {
        let request = request();
        let target = handoff_readback(&request);
        assert_eq!(
            target,
            ReadbackTarget::MemberHandoff(Box::new(request.clone()))
        );
        let valid = receipt(&request);
        assert!(valid_handoff_receipt(&valid, &request));

        let mut invalid = valid.clone();
        invalid.result_coordinator_revision = Some(14);
        assert!(!valid_handoff_receipt(&invalid, &request));
        let mut invalid = valid.clone();
        invalid.target_agent_id = Some(Uuid::from_u128(99));
        assert!(!valid_handoff_receipt(&invalid, &request));
        let mut invalid = valid.clone();
        invalid.departed_event_id = Some(Uuid::from_u128(98));
        assert!(!valid_handoff_receipt(&invalid, &request));
        let mut invalid = valid.clone();
        invalid.committed_at_ms = invalid.source_reconciled_at_ms - 1;
        assert!(!valid_handoff_receipt(&invalid, &request));
        let mut invalid = valid;
        invalid.display_name = "Ada\n".to_owned();
        assert!(!valid_handoff_receipt(&invalid, &request));
    }
}
