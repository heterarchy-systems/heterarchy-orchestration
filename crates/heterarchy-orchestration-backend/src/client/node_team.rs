//! Caller-scoped Node-Team query and compare-and-swap through the official client.

use hyper::Method;
use hyper::header::HeaderValue;

use crate::wire::node_team::{
    NodeTeamBindingWire, NodeTeamQueryRequest, NodeTeamQueryResponse,
    NodeTeamReservationDispositionWire, NodeTeamReservationScopeRelationWire, NodeTeamSetRequest,
    NodeTeamSetResponse,
};

use super::membership::validate_scope;
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_NODE_TEAM_RESERVATIONS: usize = 32;

impl OrchestrationHttpClient {
    /// Read one exact Node-Team snapshot and bounded assignment-reservation page.
    ///
    /// This is a query and never creates mutation-unknown semantics.
    ///
    /// # Errors
    /// Rejects invalid selectors, malformed or foreign projections, typed service
    /// rejections, and bounded transport failures.
    pub async fn node_team_read(
        &self,
        authorization: &HeaderValue,
        request: &NodeTeamQueryRequest,
    ) -> Result<NodeTeamQueryResponse, ClientError> {
        validate_request(request)?;
        let response: NodeTeamQueryResponse = self
            .call(
                authorization,
                Method::POST,
                node_team_path(request),
                Some(request),
                None,
            )
            .await?;
        if !valid_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Set one exact Node-Team binding with the caller's current leadership fence.
    ///
    /// There is no replay nonce or automatic retry. An uncertain response carries
    /// the exact caller and Node query needed for readback; that observation does
    /// not establish mutation attribution.
    ///
    /// # Errors
    /// Rejects invalid selectors or fences, typed service rejections, and bounded
    /// transport failures. An uncertain exchange or malformed success returns an
    /// unknown outcome with the exact Node-Team readback target.
    pub async fn node_team_set(
        &self,
        authorization: &HeaderValue,
        request: &NodeTeamSetRequest,
    ) -> Result<NodeTeamSetResponse, ClientError> {
        validate_set_request(request)?;
        let readback = ReadbackTarget::NodeTeam(Box::new(node_team_query_request(request)));
        let response: NodeTeamSetResponse = self
            .call(
                authorization,
                Method::POST,
                node_team_set_path(request),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_set_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }
}

fn node_team_path(request: &NodeTeamQueryRequest) -> String {
    format!(
        "/internal/v1/workspaces/{}/node-teams/query",
        request.scope.workspace_id
    )
}

fn node_team_set_path(request: &NodeTeamSetRequest) -> String {
    format!(
        "/internal/v1/workspaces/{}/node-teams/set",
        request.scope.workspace_id
    )
}

fn node_team_query_request(request: &NodeTeamSetRequest) -> NodeTeamQueryRequest {
    NodeTeamQueryRequest {
        scope: request.scope.clone(),
        session_id: request.session_id,
        sender_activity_id: request.sender_activity_id,
        node_scope: request.node_scope,
        after_assignment_id: None,
    }
}

fn validate_request(request: &NodeTeamQueryRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.session_id)?;
    require_identity(request.sender_activity_id)?;
    require_identity(request.node_scope.activity_id)?;
    require_identity(request.node_scope.goal_id)?;
    require_identity(request.node_scope.node_id)?;
    if request.node_scope.goal_created_at_ms < 0
        || request.node_scope.dag_revision <= 0
        || request.node_scope.dag_base_goal_revision <= 0
        || request.after_assignment_id.is_some_and(|id| id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_set_request(request: &NodeTeamSetRequest) -> Result<(), ClientError> {
    validate_request(&node_team_query_request(request))?;
    if request
        .expected_revision
        .is_some_and(|revision| revision == 0 || revision >= i64::MAX.unsigned_abs())
        || request.team_id.is_some_and(|team_id| team_id.is_nil())
        || request.sender_leadership_revision <= 0
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_response(response: &NodeTeamQueryResponse, request: &NodeTeamQueryRequest) -> bool {
    response.request == *request && valid_snapshot(&response.snapshot, request)
}

fn valid_set_response(response: &NodeTeamSetResponse, request: &NodeTeamSetRequest) -> bool {
    let query = node_team_query_request(request);
    response.request == *request
        && valid_binding(&response.binding, &query, false)
        && response.binding.team_id == request.team_id
        && response.binding.bound_by == "orchestrator"
        && request
            .expected_revision
            .map_or(Some(1), |revision| revision.checked_add(1))
            == Some(response.binding.revision)
}

fn valid_snapshot(
    snapshot: &crate::wire::node_team::NodeTeamSnapshotWire,
    request: &NodeTeamQueryRequest,
) -> bool {
    if snapshot.workspace_id != request.scope.workspace_id
        || snapshot.node_scope != request.node_scope
        || snapshot.after_assignment_id != request.after_assignment_id
        || snapshot.next_assignment_id.is_some_and(|id| id.is_nil())
        || snapshot.reservations.len() > MAX_NODE_TEAM_RESERVATIONS
        || snapshot
            .binding
            .as_ref()
            .is_some_and(|binding| !valid_binding(binding, request, false))
        || snapshot
            .legacy_binding
            .as_ref()
            .is_some_and(|binding| !valid_binding(binding, request, true))
        || snapshot
            .reservations
            .iter()
            .any(|reservation| !valid_reservation(reservation, request))
        || !snapshot
            .reservations
            .windows(2)
            .all(|pair| pair[0].assignment_id.as_bytes() < pair[1].assignment_id.as_bytes())
        || !request.after_assignment_id.is_none_or(|after| {
            snapshot
                .reservations
                .iter()
                .all(|reservation| reservation.assignment_id.as_bytes() > after.as_bytes())
        })
    {
        return false;
    }

    match snapshot.next_assignment_id {
        None => true,
        Some(next) => {
            snapshot.reservations.len() == MAX_NODE_TEAM_RESERVATIONS
                && snapshot
                    .reservations
                    .last()
                    .is_some_and(|reservation| reservation.assignment_id == next)
                && request
                    .after_assignment_id
                    .is_none_or(|after| next.as_bytes() > after.as_bytes())
        }
    }
}

fn valid_binding(
    binding: &NodeTeamBindingWire,
    request: &NodeTeamQueryRequest,
    legacy: bool,
) -> bool {
    if binding.goal_id != request.node_scope.goal_id
        || binding.dag_revision != request.node_scope.dag_revision
        || binding.node_id != request.node_scope.node_id
        || binding.team_id.is_some_and(|id| id.is_nil())
        || !matches!(binding.bound_by.as_str(), "operator" | "orchestrator")
        || binding.bound_at_ms < 0
        || binding.revision == 0
        || binding
            .legacy_retirement
            .as_ref()
            .is_some_and(|retirement| {
                retirement.request_id.is_nil()
                    || retirement.retired_at_ms < 0
                    || retirement.binding_revision == 0
            })
    {
        return false;
    }

    if legacy {
        binding.activity_id.is_none()
            && binding.goal_created_at_ms.is_none()
            && binding.dag_base_goal_revision.is_none()
    } else {
        binding.legacy_retirement.is_none()
            && binding.activity_id == Some(request.node_scope.activity_id)
            && binding.goal_created_at_ms == Some(request.node_scope.goal_created_at_ms)
            && binding.dag_base_goal_revision == Some(request.node_scope.dag_base_goal_revision)
    }
}

fn valid_reservation(
    reservation: &crate::wire::node_team::NodeTeamReservationWire,
    request: &NodeTeamQueryRequest,
) -> bool {
    if reservation.activity_id.is_nil() || reservation.assignment_id.is_nil() {
        return false;
    }
    match reservation.scope_relation {
        NodeTeamReservationScopeRelationWire::Exact => {
            reservation.disposition != NodeTeamReservationDispositionWire::LegacyUnknown
                && reservation.activity_id == request.node_scope.activity_id
        }
        NodeTeamReservationScopeRelationWire::HistoricalCoordinates => {
            reservation.disposition != NodeTeamReservationDispositionWire::LegacyUnknown
        }
        NodeTeamReservationScopeRelationWire::LegacyUnknown => {
            reservation.disposition == NodeTeamReservationDispositionWire::LegacyUnknown
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use super::*;
    use crate::wire::WorkspaceQueryScope;
    use crate::wire::node_team::{
        NodeTeamLegacyRetirementWire, NodeTeamQueryRequest, NodeTeamReservationWire,
        NodeTeamRetirementAuthorityWire, NodeTeamScopeWire,
    };
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    const CREDENTIAL: &str = "Bearer node-team-fixture-secret";

    fn request(after_assignment_id: Option<Uuid>) -> NodeTeamQueryRequest {
        NodeTeamQueryRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            session_id: Uuid::from_u128(5),
            sender_activity_id: Uuid::from_u128(6),
            node_scope: NodeTeamScopeWire {
                activity_id: Uuid::from_u128(12),
                goal_id: Uuid::from_u128(7),
                goal_created_at_ms: 8,
                dag_revision: 9,
                dag_base_goal_revision: 10,
                node_id: Uuid::from_u128(11),
            },
            after_assignment_id,
        }
    }

    fn set_request(expected_revision: Option<u64>, team_id: Option<Uuid>) -> NodeTeamSetRequest {
        let query = request(None);
        NodeTeamSetRequest {
            scope: query.scope,
            session_id: query.session_id,
            sender_activity_id: query.sender_activity_id,
            node_scope: query.node_scope,
            expected_revision,
            team_id,
            sender_leadership_revision: 7,
        }
    }

    fn binding(request: &NodeTeamQueryRequest, legacy: bool) -> NodeTeamBindingWire {
        NodeTeamBindingWire {
            activity_id: (!legacy).then_some(request.node_scope.activity_id),
            goal_created_at_ms: (!legacy).then_some(request.node_scope.goal_created_at_ms),
            dag_base_goal_revision: (!legacy).then_some(request.node_scope.dag_base_goal_revision),
            goal_id: request.node_scope.goal_id,
            dag_revision: request.node_scope.dag_revision,
            node_id: request.node_scope.node_id,
            team_id: Some(Uuid::from_u128(20)),
            bound_by: "orchestrator".to_owned(),
            bound_at_ms: 13,
            revision: 2,
            legacy_retirement: legacy.then_some(NodeTeamLegacyRetirementWire {
                request_id: Uuid::from_u128(21),
                retired_at_ms: 14,
                binding_revision: 2,
                authority: NodeTeamRetirementAuthorityWire::WorkspaceLeader,
            }),
        }
    }

    fn reservation(
        request: &NodeTeamQueryRequest,
        assignment_id: Uuid,
        relation: NodeTeamReservationScopeRelationWire,
    ) -> NodeTeamReservationWire {
        NodeTeamReservationWire {
            activity_id: if relation == NodeTeamReservationScopeRelationWire::Exact {
                request.node_scope.activity_id
            } else {
                Uuid::from_u128(30)
            },
            assignment_id,
            disposition: match relation {
                NodeTeamReservationScopeRelationWire::LegacyUnknown => {
                    NodeTeamReservationDispositionWire::LegacyUnknown
                }
                _ => NodeTeamReservationDispositionWire::Pending,
            },
            scope_relation: relation,
        }
    }

    fn snapshot(
        request: &NodeTeamQueryRequest,
        reservations: Vec<NodeTeamReservationWire>,
        next_assignment_id: Option<Uuid>,
    ) -> crate::wire::node_team::NodeTeamSnapshotWire {
        crate::wire::node_team::NodeTeamSnapshotWire {
            workspace_id: request.scope.workspace_id,
            node_scope: request.node_scope,
            after_assignment_id: request.after_assignment_id,
            binding: Some(binding(request, false)),
            legacy_binding: Some(binding(request, true)),
            reservations,
            next_assignment_id,
        }
    }

    fn set_response(request: &NodeTeamSetRequest) -> NodeTeamSetResponse {
        let query = node_team_query_request(request);
        let mut binding = binding(&query, false);
        binding.team_id = request.team_id;
        binding.bound_by = "orchestrator".to_owned();
        binding.revision = request.expected_revision.map_or(1, |revision| revision + 1);
        NodeTeamSetResponse {
            request: request.clone(),
            binding,
        }
    }

    #[test]
    fn request_checks_identity_generation_and_cursor_without_conflating_activities() {
        let request = request(Some(Uuid::from_u128(40)));
        assert!(validate_request(&request).is_ok());
        assert_eq!(
            node_team_path(&request),
            "/internal/v1/workspaces/00000000-0000-0000-0000-000000000001/node-teams/query"
        );

        let mut invalid = request.clone();
        invalid.session_id = Uuid::nil();
        assert_eq!(validate_request(&invalid), Err(ClientError::InvalidRequest));

        let mut invalid = request.clone();
        invalid.sender_activity_id = Uuid::nil();
        assert_eq!(validate_request(&invalid), Err(ClientError::InvalidRequest));

        let mut invalid = request.clone();
        invalid.node_scope.dag_base_goal_revision = 0;
        assert_eq!(validate_request(&invalid), Err(ClientError::InvalidRequest));

        let mut invalid = request;
        invalid.after_assignment_id = Some(Uuid::nil());
        assert_eq!(validate_request(&invalid), Err(ClientError::InvalidRequest));
    }

    #[test]
    fn exact_legacy_bindings_and_reservation_generation_page_are_checked() {
        let request = request(Some(Uuid::from_u128(39)));
        let snapshot = snapshot(
            &request,
            vec![
                reservation(
                    &request,
                    Uuid::from_u128(40),
                    NodeTeamReservationScopeRelationWire::Exact,
                ),
                reservation(
                    &request,
                    Uuid::from_u128(41),
                    NodeTeamReservationScopeRelationWire::HistoricalCoordinates,
                ),
                reservation(
                    &request,
                    Uuid::from_u128(42),
                    NodeTeamReservationScopeRelationWire::LegacyUnknown,
                ),
            ],
            None,
        );
        assert!(valid_snapshot(&snapshot, &request));

        let response = NodeTeamQueryResponse {
            request: request.clone(),
            snapshot,
        };
        assert!(valid_response(&response, &request));

        let mut wrong_workspace = response.clone();
        wrong_workspace.snapshot.workspace_id = Uuid::from_u128(99);
        assert!(!valid_response(&wrong_workspace, &request));

        let mut wrong_scope_echo = response.clone();
        wrong_scope_echo.request.node_scope.goal_created_at_ms += 1;
        assert!(!valid_response(&wrong_scope_echo, &request));

        let mut wrong_cursor_echo = response.clone();
        wrong_cursor_echo.snapshot.after_assignment_id = None;
        assert!(!valid_response(&wrong_cursor_echo, &request));

        let mut exact_with_retirement = response.clone();
        let mut invalid_binding = binding(&request, false);
        invalid_binding.legacy_retirement =
            Some(crate::wire::node_team::NodeTeamLegacyRetirementWire {
                request_id: Uuid::from_u128(90),
                retired_at_ms: 12,
                binding_revision: 1,
                authority: crate::wire::node_team::NodeTeamRetirementAuthorityWire::LocalOperator,
            });
        exact_with_retirement.snapshot.binding = Some(invalid_binding);
        assert!(!valid_response(&exact_with_retirement, &request));

        let mut exact_has_legacy_nulls = response.clone();
        let mut invalid_binding = binding(&request, false);
        invalid_binding.activity_id = None;
        exact_has_legacy_nulls.snapshot.binding = Some(invalid_binding);
        assert!(!valid_response(&exact_has_legacy_nulls, &request));

        let mut legacy_has_exact_generation = response;
        let mut invalid_binding = binding(&request, true);
        invalid_binding.goal_created_at_ms = Some(request.node_scope.goal_created_at_ms);
        legacy_has_exact_generation.snapshot.legacy_binding = Some(invalid_binding);
        assert!(!valid_response(&legacy_has_exact_generation, &request));
    }

    #[test]
    fn reservation_cursor_order_bounds_and_relation_disposition_are_enforced() {
        let request = request(Some(Uuid::from_u128(39)));
        let base = snapshot(
            &request,
            vec![
                reservation(
                    &request,
                    Uuid::from_u128(40),
                    NodeTeamReservationScopeRelationWire::Exact,
                ),
                reservation(
                    &request,
                    Uuid::from_u128(41),
                    NodeTeamReservationScopeRelationWire::HistoricalCoordinates,
                ),
            ],
            None,
        );
        assert!(valid_snapshot(&base, &request));

        let mut unsorted = base.clone();
        unsorted.reservations.swap(0, 1);
        assert!(!valid_snapshot(&unsorted, &request));

        let mut before_cursor = base.clone();
        before_cursor.reservations[0].assignment_id = Uuid::from_u128(38);
        assert!(!valid_snapshot(&before_cursor, &request));

        let mut wrong_next = base.clone();
        wrong_next.next_assignment_id = Some(Uuid::from_u128(41));
        assert!(!valid_snapshot(&wrong_next, &request));

        let mut mismatched_relation = base.clone();
        mismatched_relation.reservations[0].activity_id = Uuid::from_u128(30);
        assert!(!valid_snapshot(&mismatched_relation, &request));

        let mut legacy_wrong_disposition = base;
        legacy_wrong_disposition.reservations[1].disposition =
            NodeTeamReservationDispositionWire::Active;
        legacy_wrong_disposition.reservations[1].scope_relation =
            NodeTeamReservationScopeRelationWire::LegacyUnknown;
        assert!(!valid_snapshot(&legacy_wrong_disposition, &request));

        let mut scoped_legacy_unknown = snapshot(
            &request,
            vec![reservation(
                &request,
                Uuid::from_u128(40),
                NodeTeamReservationScopeRelationWire::Exact,
            )],
            None,
        );
        scoped_legacy_unknown.reservations[0].disposition =
            NodeTeamReservationDispositionWire::LegacyUnknown;
        assert!(!valid_snapshot(&scoped_legacy_unknown, &request));
        scoped_legacy_unknown.reservations[0].scope_relation =
            NodeTeamReservationScopeRelationWire::HistoricalCoordinates;
        assert!(!valid_snapshot(&scoped_legacy_unknown, &request));

        let mut full_page_request = request.clone();
        full_page_request.after_assignment_id = None;
        let mut full_page = Vec::new();
        for offset in 1..=32 {
            full_page.push(reservation(
                &full_page_request,
                Uuid::from_u128(100 + offset),
                NodeTeamReservationScopeRelationWire::Exact,
            ));
        }
        let last = full_page.last().map(|row| row.assignment_id);
        let page = snapshot(&full_page_request, full_page, last);
        assert!(valid_snapshot(&page, &full_page_request));
    }

    #[test]
    fn set_request_checks_generation_cas_team_and_current_leadership_fence() {
        let valid = set_request(Some(6), Some(Uuid::from_u128(20)));
        assert!(validate_set_request(&valid).is_ok());
        assert_eq!(
            node_team_set_path(&valid),
            "/internal/v1/workspaces/00000000-0000-0000-0000-000000000001/node-teams/set"
        );
        let mut max_next_revision = valid.clone();
        max_next_revision.expected_revision = Some(i64::MAX.unsigned_abs() - 1);
        assert!(validate_set_request(&max_next_revision).is_ok());
        let max_next_response = set_response(&max_next_revision);
        assert_eq!(max_next_response.binding.revision, i64::MAX.unsigned_abs());
        assert!(valid_set_response(&max_next_response, &max_next_revision));

        let mut invalid = valid.clone();
        invalid.expected_revision = Some(0);
        assert_eq!(
            validate_set_request(&invalid),
            Err(ClientError::InvalidRequest)
        );
        let mut invalid = valid.clone();
        invalid.expected_revision = Some(i64::MAX.unsigned_abs());
        assert_eq!(
            validate_set_request(&invalid),
            Err(ClientError::InvalidRequest)
        );
        let mut invalid = valid.clone();
        invalid.team_id = Some(Uuid::nil());
        assert_eq!(
            validate_set_request(&invalid),
            Err(ClientError::InvalidRequest)
        );
        let mut invalid = valid;
        invalid.sender_leadership_revision = 0;
        assert_eq!(
            validate_set_request(&invalid),
            Err(ClientError::InvalidRequest)
        );
    }

    #[test]
    fn set_response_requires_exact_cas_echo_and_requested_binding_revision() {
        let request = set_request(Some(6), Some(Uuid::from_u128(20)));
        let response = set_response(&request);
        assert!(valid_set_response(&response, &request));

        let mut wrong_cas = response.clone();
        wrong_cas.request.expected_revision = Some(5);
        assert!(!valid_set_response(&wrong_cas, &request));

        let mut wrong_team = response.clone();
        wrong_team.binding.team_id = None;
        assert!(!valid_set_response(&wrong_team, &request));

        let mut wrong_authority = response.clone();
        wrong_authority.binding.bound_by = "operator".to_owned();
        assert!(!valid_set_response(&wrong_authority, &request));

        let mut wrong_revision = response.clone();
        wrong_revision.binding.revision += 1;
        assert!(!valid_set_response(&wrong_revision, &request));

        let mut retired = response;
        retired.binding.legacy_retirement = Some(NodeTeamLegacyRetirementWire {
            request_id: Uuid::from_u128(90),
            retired_at_ms: 14,
            binding_revision: 7,
            authority: NodeTeamRetirementAuthorityWire::WorkspaceLeader,
        });
        assert!(!valid_set_response(&retired, &request));

        let clear_request = set_request(None, None);
        let clear_response = set_response(&clear_request);
        assert_eq!(clear_response.binding.revision, 1);
        assert!(valid_set_response(&clear_response, &clear_request));
    }

    #[tokio::test]
    async fn malformed_set_success_preserves_exact_node_team_readback_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = set_request(Some(6), Some(Uuid::from_u128(20)));
        let valid = serde_json::to_value(set_response(&request))?;
        let mut wrong_echo = valid.clone();
        wrong_echo["request"]["expectedRevision"] = serde_json::json!(5);
        let mut wrong_binding = valid;
        wrong_binding["binding"]["revision"] = serde_json::json!(9);

        for body in [wrong_echo, wrong_binding] {
            let (client, server) = fixture(body).await?;
            assert_eq!(
                client
                    .node_team_set(&HeaderValue::from_static(CREDENTIAL), &request)
                    .await
                    .err(),
                Some(ClientError::UnknownOutcome {
                    readback: ReadbackTarget::NodeTeam(Box::new(node_team_query_request(&request)))
                })
            );
            let captured = String::from_utf8(server.await??)?;
            assert!(captured.starts_with("POST /internal/v1/workspaces/"));
            assert!(captured.contains("/node-teams/set HTTP/1.1"));
        }
        Ok(())
    }

    async fn fixture(
        response: serde_json::Value,
    ) -> Result<
        (
            OrchestrationHttpClient,
            tokio::task::JoinHandle<Result<Vec<u8>, io::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(1))?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let count = socket.read(&mut buffer).await?;
                if count == 0 {
                    return Err(io::Error::other("client closed before completing request"));
                }
                request.extend_from_slice(&buffer[..count]);
                if request.len() > super::super::MAX_REQUEST_BYTES + super::super::MAX_HEADER_BYTES
                {
                    return Err(io::Error::other("fixture request exceeded bound"));
                }
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or_default();
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }

            let body = response.to_string();
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(reply.as_bytes()).await?;
            socket.shutdown().await?;
            drop(socket);
            match tokio::time::timeout(Duration::from_millis(100), listener.accept()).await {
                Ok(Ok((mut socket, _))) => {
                    let _ = socket.shutdown().await;
                    return Err(io::Error::other("client retried a Node-Team mutation"));
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {}
            }
            Ok(request)
        });
        Ok((client, server))
    }
}
