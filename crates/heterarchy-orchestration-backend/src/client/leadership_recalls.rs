//! Bounded client for source-scoped leadership recall start/replay.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::leadership::WorkspaceQueryScope;
use crate::wire::leadership_recalls::{
    LeadershipRecallStartRequest, LeadershipRecallStartResponse, LeadershipRecallTargetWire,
};

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_REASON_BYTES: usize = 4_096;
const MAX_ELECTORATE_SIZE: i64 = 64;

impl OrchestrationHttpClient {
    /// Start or exactly replay one source-scoped leadership recall.
    ///
    /// The request ID is the durable replay key. An uncertain response carries
    /// the exact Workspace/request readback target; the client never retries
    /// the mutation automatically.
    ///
    /// # Errors
    /// Rejects invalid scope, actor, target, revision, or reason; returns typed
    /// service rejection, bounded transport failure, or exact readback identity.
    pub async fn start_leadership_recall(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipRecallStartRequest,
    ) -> Result<LeadershipRecallStartResponse, ClientError> {
        validate_request(request)?;
        let readback = ReadbackTarget::LeadershipReview {
            workspace_id: request.scope.workspace_id,
            request_id: request.request_id,
        };
        let response: LeadershipRecallStartResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/leadership/recalls/start",
                    request.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }
}

fn validate_request(request: &LeadershipRecallStartRequest) -> Result<(), ClientError> {
    require_scope(&request.scope)?;
    require_identity(request.activity_id)?;
    require_identity(request.session_id)?;
    require_identity(request.request_id)?;
    if request.expected_leadership_revision < 1
        || !valid_target(request.target, request.team_id)
        || request.reason.trim().is_empty()
        || request.reason.len() > MAX_REASON_BYTES
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn require_scope(scope: &WorkspaceQueryScope) -> Result<(), ClientError> {
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

fn valid_target(target: LeadershipRecallTargetWire, team_id: Option<Uuid>) -> bool {
    match target {
        LeadershipRecallTargetWire::Team => team_id.is_some_and(|id| !id.is_nil()),
        LeadershipRecallTargetWire::Workspace | LeadershipRecallTargetWire::Activity => {
            team_id.is_none()
        }
    }
}

fn valid_response(
    response: &LeadershipRecallStartResponse,
    request: &LeadershipRecallStartRequest,
) -> bool {
    response.scope == request.scope
        && response.activity_id == request.activity_id
        && response.session_id == request.session_id
        && response.target == request.target
        && response.team_id == request.team_id
        && response.receipt.request_id == request.request_id
        && !response.receipt.request_id.is_nil()
        && !response.receipt.election_id.is_nil()
        && response.receipt.leadership_revision > 0
        && (1..=MAX_ELECTORATE_SIZE).contains(&response.receipt.electorate_size)
        && response.receipt.defense_deadline_ms >= 0
        && response.receipt.discussion_deadline_ms >= response.receipt.defense_deadline_ms
        && response.receipt.ballot_deadline_ms >= response.receipt.discussion_deadline_ms
        && response.receipt.candidate_is_caller
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    use crate::wire::leadership::WorkspaceQueryScope;
    use crate::wire::leadership_recalls::{
        LeadershipRecallReceiptWire, LeadershipRecallStartRequest, LeadershipRecallStartResponse,
        LeadershipRecallTargetWire,
    };
    use crate::wire::leadership_reviews::LeadershipReviewPhaseWire;

    use super::*;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn request() -> LeadershipRecallStartRequest {
        LeadershipRecallStartRequest {
            scope: scope(),
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
            target: LeadershipRecallTargetWire::Team,
            team_id: Some(Uuid::from_u128(7)),
            expected_leadership_revision: 8,
            request_id: Uuid::from_u128(9),
            reason: "Replace an inactive Team leader".to_owned(),
        }
    }

    fn response(
        request: &LeadershipRecallStartRequest,
        phase: LeadershipReviewPhaseWire,
        replay: bool,
    ) -> LeadershipRecallStartResponse {
        LeadershipRecallStartResponse {
            scope: request.scope.clone(),
            activity_id: request.activity_id,
            session_id: request.session_id,
            target: request.target,
            team_id: request.team_id,
            receipt: LeadershipRecallReceiptWire {
                request_id: request.request_id,
                election_id: Uuid::from_u128(10),
                leadership_revision: 9,
                electorate_size: 3,
                phase,
                defense_deadline_ms: 100,
                discussion_deadline_ms: 200,
                ballot_deadline_ms: 300,
                replay,
                candidate_is_caller: true,
            },
        }
    }

    #[test]
    fn request_requires_exact_scope_target_and_bounded_reason() {
        let valid = request();
        assert!(validate_request(&valid).is_ok());

        let mut invalid = valid.clone();
        invalid.team_id = None;
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.team_id = Some(Uuid::nil());
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.target = LeadershipRecallTargetWire::Workspace;
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.target = LeadershipRecallTargetWire::Activity;
        invalid.team_id = None;
        assert!(validate_request(&invalid).is_ok());
        let mut invalid = valid.clone();
        invalid.expected_leadership_revision = 0;
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.request_id = Uuid::nil();
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.session_id = Uuid::nil();
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.reason = " \n\t".to_owned();
        assert!(validate_request(&invalid).is_err());
        invalid.reason = "x".repeat(MAX_REASON_BYTES + 1);
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.scope.workspace_revision = 0;
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.scope.workspace_id = Uuid::nil();
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.scope.locator = "bad\nlocator".to_owned();
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid;
        invalid.activity_id = Uuid::nil();
        assert!(validate_request(&invalid).is_err());
    }

    #[test]
    fn receipt_echoes_exact_request_and_allows_current_replay_phase() {
        let request = request();
        for phase in [
            LeadershipReviewPhaseWire::Defense,
            LeadershipReviewPhaseWire::Discussion,
            LeadershipReviewPhaseWire::Ballot,
            LeadershipReviewPhaseWire::AwaitingDecision,
            LeadershipReviewPhaseWire::Resolved,
        ] {
            assert!(valid_response(&response(&request, phase, true), &request));
        }
        let mut wrong_activity = response(&request, LeadershipReviewPhaseWire::Ballot, false);
        wrong_activity.activity_id = Uuid::from_u128(90);
        assert!(!valid_response(&wrong_activity, &request));
        let mut wrong_team = response(&request, LeadershipReviewPhaseWire::Ballot, false);
        wrong_team.team_id = Some(Uuid::from_u128(91));
        assert!(!valid_response(&wrong_team, &request));
        let mut false_candidate = response(&request, LeadershipReviewPhaseWire::Ballot, false);
        false_candidate.receipt.candidate_is_caller = false;
        assert!(!valid_response(&false_candidate, &request));
        let mut invalid_revision = response(&request, LeadershipReviewPhaseWire::Ballot, false);
        invalid_revision.receipt.leadership_revision = 0;
        assert!(!valid_response(&invalid_revision, &request));
        let mut empty_electorate = response(&request, LeadershipReviewPhaseWire::Ballot, false);
        empty_electorate.receipt.electorate_size = 0;
        assert!(!valid_response(&empty_electorate, &request));
        let mut unordered_deadlines = response(&request, LeadershipReviewPhaseWire::Ballot, false);
        unordered_deadlines.receipt.ballot_deadline_ms = 199;
        assert!(!valid_response(&unordered_deadlines, &request));
    }

    #[test]
    fn response_requires_explicit_nullable_team_selector() -> Result<(), Box<dyn std::error::Error>>
    {
        let query = request();
        let response = response(&query, LeadershipReviewPhaseWire::Discussion, true);
        let mut wire = serde_json::to_value(&response)?;
        let object = wire.as_object_mut().ok_or("response object")?;
        let _omitted = object.remove("teamId");
        assert!(serde_json::from_value::<LeadershipRecallStartResponse>(wire).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn recall_start_uses_exact_route_and_dynamic_replay_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = request();
        let body =
            serde_json::to_vec(&response(&query, LeadershipReviewPhaseWire::Resolved, true))?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(2))?;
        let expected_path = format!(
            "POST /internal/v1/workspaces/{}/leadership/recalls/start HTTP/1.1",
            query.scope.workspace_id
        );
        let request_id = query.request_id;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let request = read_request(&mut socket).await?;
            assert!(request.starts_with(&expected_path));
            assert!(request.contains(&request_id.to_string()));
            assert!(request.contains("\"target\":\"team\""));
            write_response(&mut socket, &body).await?;
            Ok::<_, std::io::Error>(())
        });
        let result = client
            .start_leadership_recall(&HeaderValue::from_static("Bearer fixture"), &query)
            .await?;
        assert!(result.receipt.replay);
        assert_eq!(result.receipt.phase, LeadershipReviewPhaseWire::Resolved);
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn malformed_and_lost_recall_replies_keep_exact_review_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(2))?;
        let workspace_id = request.scope.workspace_id;
        let request_id = request.request_id;
        let expected_path = format!(
            "POST /internal/v1/workspaces/{workspace_id}/leadership/recalls/start HTTP/1.1"
        );
        let server = tokio::spawn(async move {
            let (mut malformed_socket, _) = listener.accept().await?;
            let malformed_request = read_request(&mut malformed_socket).await?;
            assert!(malformed_request.starts_with(&expected_path));
            assert!(malformed_request.contains(&request_id.to_string()));
            write_response(&mut malformed_socket, b"{}").await?;
            let (mut lost_socket, _) = listener.accept().await?;
            let lost_request = read_request(&mut lost_socket).await?;
            assert!(lost_request.starts_with(&expected_path));
            assert!(lost_request.contains(&request_id.to_string()));
            drop(lost_socket);
            Ok::<_, std::io::Error>(())
        });

        for _attempt in 0..2 {
            assert!(matches!(
                client
                    .start_leadership_recall(
                        &HeaderValue::from_static("Bearer fixture"),
                        &request,
                    )
                    .await,
                Err(ClientError::UnknownOutcome {
                    readback: ReadbackTarget::LeadershipReview {
                        workspace_id: actual_workspace,
                        request_id: actual_request,
                    }
                }) if actual_workspace == workspace_id && actual_request == request_id
            ));
        }
        server.await??;
        Ok(())
    }

    async fn read_request(socket: &mut tokio::net::TcpStream) -> Result<String, std::io::Error> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 2_048];
        loop {
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
            let Some(header_end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::trim)
                        .and_then(|value| value.parse::<usize>().ok())
                })
                .ok_or_else(|| std::io::Error::other("HTTP Content-Length missing"))?;
            if bytes.len() >= header_end + 4 + content_length {
                return Ok(String::from_utf8_lossy(&bytes).into_owned());
            }
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn write_response(
        socket: &mut tokio::net::TcpStream,
        body: &[u8],
    ) -> Result<(), std::io::Error> {
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(headers.as_bytes()).await?;
        socket.write_all(body).await?;
        Ok(())
    }
}
