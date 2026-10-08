//! Bounded HTTP client for source-scoped Leadership lifecycle operations.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::leadership::{
    LeadershipQueryRequest, LeadershipViewResponse, LeadershipWire, ReviewHistoryWire,
};
use crate::wire::leadership_lifecycle::{
    LeadershipAdvanceRequest, LeadershipAdvanceResponse, LeadershipConfirmRequest,
    LeadershipConfirmResponse, LeadershipConfirmationQueryRequest,
    LeadershipConfirmationQueryResponse, LeadershipConfirmationSnapshotWire,
    LeadershipIntentStateWire, LeadershipLifecycleTarget, LeadershipReceiptWire,
    LeadershipReconcileRequest, LeadershipReconcileResponse,
};

use super::teams::valid_scope;
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

impl OrchestrationHttpClient {
    /// Advance one exact Workspace, Activity, or Team leadership scope.
    ///
    /// An uncertain exchange carries the exact scope for readback. The client
    /// never retries the advance automatically.
    ///
    /// # Errors
    /// Rejects invalid scope selectors, returns typed service rejection,
    /// bounded transport failure, or an unknown outcome with exact readback identity.
    pub async fn advance_leadership(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipAdvanceRequest,
    ) -> Result<LeadershipAdvanceResponse, ClientError> {
        validate_target(&request.target)?;
        let readback = leadership_scope_readback(&request.target);
        let response: LeadershipAdvanceResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/leadership/advance",
                    request.target.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_advance_response(&response, &request.target) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Confirm one exact elected leader through the existing Broker fence saga.
    ///
    /// An uncertain exchange carries the exact scope/request identity for
    /// readback. The client never retries confirmation automatically.
    ///
    /// # Errors
    /// Rejects invalid scope, election/request identities, or revision; returns
    /// typed service rejection, bounded transport failure, or an unknown
    /// outcome with exact readback identity.
    pub async fn confirm_leadership(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipConfirmRequest,
    ) -> Result<LeadershipConfirmResponse, ClientError> {
        validate_confirm_request(request)?;
        let readback = leadership_confirmation_readback(&request.target, request.request_id);
        let response: LeadershipConfirmResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/leadership/confirm",
                    request.target.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_confirm_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Reconcile one exact durable leadership recovery request.
    ///
    /// An uncertain exchange carries the exact scope/request identity for
    /// readback. The client never retries recovery automatically.
    ///
    /// # Errors
    /// Rejects invalid scope/request identity or revision; returns typed service
    /// rejection, bounded transport failure, or an unknown outcome with exact
    /// readback identity.
    pub async fn reconcile_leadership(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipReconcileRequest,
    ) -> Result<LeadershipReconcileResponse, ClientError> {
        validate_reconcile_request(request)?;
        let readback = leadership_confirmation_readback(&request.target, request.request_id);
        let response: LeadershipReconcileResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/leadership/reconcile",
                    request.target.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_reconcile_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one exact durable confirmation intent without changing its state.
    ///
    /// An absent intent is an explicit readback observation; it grants no
    /// permission to retry the original confirmation.
    ///
    /// # Errors
    /// Rejects invalid scope/request identity, typed service rejection, bounded
    /// transport failure, or an inconsistent exact readback.
    pub async fn leadership_confirmation(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipConfirmationQueryRequest,
    ) -> Result<LeadershipConfirmationQueryResponse, ClientError> {
        validate_confirmation_query_request(request)?;
        let response: LeadershipConfirmationQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/leadership/confirmation/query",
                    request.target.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_confirmation_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_target(target: &LeadershipLifecycleTarget) -> Result<(), ClientError> {
    if !valid_scope(&target.scope)
        || target.session_id.is_nil()
        || target.activity_id.is_some_and(|id| id.is_nil())
        || target.team_id.is_some_and(|id| id.is_nil())
        || (target.activity_id.is_some() && target.team_id.is_some())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_confirm_request(request: &LeadershipConfirmRequest) -> Result<(), ClientError> {
    validate_target(&request.target)?;
    require_identity(request.election_id)?;
    require_identity(request.request_id)?;
    require_revision(request.expected_leadership_revision)
}

fn validate_reconcile_request(request: &LeadershipReconcileRequest) -> Result<(), ClientError> {
    validate_target(&request.target)?;
    require_identity(request.request_id)?;
    require_revision(request.expected_leadership_revision)
}

fn validate_confirmation_query_request(
    request: &LeadershipConfirmationQueryRequest,
) -> Result<(), ClientError> {
    validate_target(&request.target)?;
    require_identity(request.request_id)
}

fn require_revision(revision: i64) -> Result<(), ClientError> {
    if revision < 0 {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn leadership_scope_readback(target: &LeadershipLifecycleTarget) -> ReadbackTarget {
    ReadbackTarget::LeadershipScope {
        workspace_id: target.scope.workspace_id,
        activity_id: target.activity_id,
        team_id: target.team_id,
    }
}

fn leadership_confirmation_readback(
    target: &LeadershipLifecycleTarget,
    request_id: Uuid,
) -> ReadbackTarget {
    ReadbackTarget::LeadershipConfirmation {
        workspace_id: target.scope.workspace_id,
        activity_id: target.activity_id,
        team_id: target.team_id,
        request_id,
    }
}

fn valid_advance_response(
    response: &LeadershipAdvanceResponse,
    target: &LeadershipLifecycleTarget,
) -> bool {
    response.target == *target
        && (!response.advanced || response.leadership.is_some())
        && response
            .leadership
            .as_ref()
            .is_none_or(|leadership| valid_leadership_view(leadership, target))
}

fn valid_leadership_view(leadership: &LeadershipWire, target: &LeadershipLifecycleTarget) -> bool {
    let request = LeadershipQueryRequest {
        scope: target.scope.clone(),
        session_id: Some(target.session_id),
        activity_id: target.activity_id,
        team_id: target.team_id,
        review_after: None,
        limit: 1,
    };
    let response = LeadershipViewResponse {
        leadership: Some(leadership.clone()),
        review_history: ReviewHistoryWire {
            entries: Vec::new(),
            next_cursor: None,
        },
    };
    super::leadership::valid_response(&response, &request)
}

fn valid_confirm_response(
    response: &LeadershipConfirmResponse,
    request: &LeadershipConfirmRequest,
) -> bool {
    response.target == request.target
        && response.request_id == request.request_id
        && response.election_id == request.election_id
        && matches!(
            response.intent_state,
            LeadershipIntentStateWire::Committed
                | LeadershipIntentStateWire::Superseded
                | LeadershipIntentStateWire::Rejected
        )
        && valid_receipt(&response.record, &request.target)
}

fn valid_reconcile_response(
    response: &LeadershipReconcileResponse,
    request: &LeadershipReconcileRequest,
) -> bool {
    response.target == request.target
        && response.request_id == request.request_id
        && valid_receipt(&response.record, &request.target)
}

fn valid_confirmation_query_response(
    response: &LeadershipConfirmationQueryResponse,
    request: &LeadershipConfirmationQueryRequest,
) -> bool {
    response.target == request.target
        && response.request_id == request.request_id
        && response
            .confirmation
            .as_ref()
            .is_none_or(|snapshot| valid_confirmation_snapshot(snapshot, &request.target))
}

fn valid_receipt(record: &LeadershipReceiptWire, target: &LeadershipLifecycleTarget) -> bool {
    record.workspace_id == target.scope.workspace_id
        && record.activity_id == target.activity_id
        && record.team_id == target.team_id
        && record
            .leader_participant_id
            .is_none_or(|participant_id| !participant_id.is_nil())
        && record.leadership_revision > 0
        && record
            .stabilization_until_ms
            .is_none_or(|deadline| deadline >= 0)
        && record.updated_at_ms >= 0
}

fn valid_confirmation_snapshot(
    snapshot: &LeadershipConfirmationSnapshotWire,
    target: &LeadershipLifecycleTarget,
) -> bool {
    if snapshot.election_id.is_nil()
        || snapshot.elected_participant_id.is_nil()
        || snapshot.expected_local_revision <= 0
        || snapshot.requested_fence_revision <= 0
        || snapshot.requested_fence_revision != snapshot.expected_local_revision
        || snapshot
            .remote_recorded_revision
            .is_some_and(|revision| revision <= 0)
        || snapshot
            .remote_recorded_revision
            .is_some_and(|revision| revision != snapshot.requested_fence_revision)
        || snapshot.confirmed_at_ms.is_some_and(|time| time < 0)
        || !valid_receipt(&snapshot.record, target)
    {
        return false;
    }
    match snapshot.intent_state {
        LeadershipIntentStateWire::Pending => {
            snapshot.remote_recorded_revision.is_none() && snapshot.confirmed_at_ms.is_none()
        }
        LeadershipIntentStateWire::RemoteRecorded => {
            snapshot.remote_recorded_revision == Some(snapshot.requested_fence_revision)
                && snapshot.confirmed_at_ms.is_none()
        }
        LeadershipIntentStateWire::Committed => {
            snapshot.remote_recorded_revision == Some(snapshot.requested_fence_revision)
                && snapshot.confirmed_at_ms.is_some()
        }
        LeadershipIntentStateWire::Superseded | LeadershipIntentStateWire::Rejected => {
            snapshot.confirmed_at_ms.is_none()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use super::super::{MAX_HEADER_BYTES, MAX_REQUEST_BYTES};
    use super::*;
    use crate::wire::leadership::WorkspaceQueryScope;
    use crate::wire::leadership_lifecycle::{
        LeadershipAdvanceRequest, LeadershipConfirmRequest, LeadershipConfirmationQueryRequest,
        LeadershipReceiptWire, LeadershipReconcileRequest, LeadershipStateWire,
    };
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const CREDENTIAL: &str = "Bearer leadership-lifecycle-fixture-secret";

    fn target() -> LeadershipLifecycleTarget {
        LeadershipLifecycleTarget {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Some(Uuid::from_u128(5)),
            team_id: None,
            session_id: Uuid::from_u128(6),
        }
    }

    fn confirm_request() -> LeadershipConfirmRequest {
        LeadershipConfirmRequest {
            target: target(),
            election_id: Uuid::from_u128(7),
            request_id: Uuid::from_u128(8),
            expected_leadership_revision: 2,
        }
    }

    fn reconcile_request() -> LeadershipReconcileRequest {
        LeadershipReconcileRequest {
            target: target(),
            request_id: Uuid::from_u128(9),
            expected_leadership_revision: 2,
        }
    }

    fn receipt() -> LeadershipReceiptWire {
        LeadershipReceiptWire {
            workspace_id: Uuid::from_u128(1),
            activity_id: Some(Uuid::from_u128(5)),
            team_id: None,
            state: LeadershipStateWire::Active,
            leader_participant_id: Some(Uuid::from_u128(10)),
            leadership_revision: 3,
            stabilization_until_ms: Some(20),
            updated_at_ms: 10,
        }
    }

    #[test]
    fn lifecycle_requests_require_exclusive_source_scope_and_exact_revision() {
        let valid_target = target();
        assert!(validate_target(&valid_target).is_ok());
        let mut invalid = valid_target.clone();
        invalid.team_id = Some(Uuid::from_u128(11));
        assert!(validate_target(&invalid).is_err());
        let mut invalid = valid_target;
        invalid.session_id = Uuid::nil();
        assert!(validate_target(&invalid).is_err());

        let mut confirm = confirm_request();
        assert!(validate_confirm_request(&confirm).is_ok());
        confirm.expected_leadership_revision = 0;
        assert!(validate_confirm_request(&confirm).is_ok());
        confirm.expected_leadership_revision = -1;
        assert!(validate_confirm_request(&confirm).is_err());
        let mut query = LeadershipConfirmationQueryRequest {
            target: target(),
            request_id: Uuid::nil(),
        };
        assert!(validate_confirmation_query_request(&query).is_err());
        query.request_id = Uuid::from_u128(8);
        assert!(validate_confirmation_query_request(&query).is_ok());
        assert!(validate_reconcile_request(&reconcile_request()).is_ok());
    }

    #[tokio::test]
    async fn advance_wrong_target_success_keeps_scope_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = LeadershipAdvanceRequest { target: target() };
        let mut body = serde_json::json!({
            "target": request.target.clone(),
            "advanced": false,
            "leadership": null
        });
        body["target"]["scope"]["workspaceId"] = serde_json::json!(Uuid::from_u128(99));
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .advance_leadership(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::LeadershipScope {
                    workspace_id: request.target.scope.workspace_id,
                    activity_id: request.target.activity_id,
                    team_id: request.target.team_id,
                }
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/leadership/advance HTTP/1.1"));
        Ok(())
    }

    #[tokio::test]
    async fn confirm_wrong_request_id_success_keeps_confirmation_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = confirm_request();
        let body = serde_json::json!({
            "target": request.target.clone(),
            "requestId": Uuid::from_u128(99),
            "electionId": request.election_id,
            "intentState": "committed",
            "record": serde_json::to_value(receipt())?
        });
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .confirm_leadership(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::LeadershipConfirmation {
                    workspace_id: request.target.scope.workspace_id,
                    activity_id: request.target.activity_id,
                    team_id: request.target.team_id,
                    request_id: request.request_id,
                }
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/leadership/confirm HTTP/1.1"));
        Ok(())
    }

    #[tokio::test]
    async fn pending_confirmation_success_requires_exact_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = confirm_request();
        let body = serde_json::json!({
            "target": request.target.clone(), "requestId": request.request_id,
            "electionId": request.election_id, "intentState": "pending",
            "record": serde_json::to_value(receipt())?
        });
        let (client, server) = fixture(body).await?;
        assert_eq!(
            client
                .confirm_leadership(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: leadership_confirmation_readback(&request.target, request.request_id)
            })
        );
        let _captured = server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_unknown_resolution_success_keeps_confirmation_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = reconcile_request();
        let body = serde_json::json!({
            "target": request.target.clone(),
            "requestId": request.request_id,
            "resolution": "unknown_resolution",
            "record": serde_json::to_value(receipt())?
        });
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .reconcile_leadership(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::LeadershipConfirmation {
                    workspace_id: request.target.scope.workspace_id,
                    activity_id: request.target.activity_id,
                    team_id: request.target.team_id,
                    request_id: request.request_id,
                }
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/leadership/reconcile HTTP/1.1"));
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
                if request.len() > MAX_REQUEST_BYTES + MAX_HEADER_BYTES {
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
                    return Err(io::Error::other("client retried an uncertain mutation"));
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {}
            }
            Ok(request)
        });
        Ok((client, server))
    }
}
