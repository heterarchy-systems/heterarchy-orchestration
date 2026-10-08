//! Official client for scoped leadership ballot cast/query operations.

use hyper::Method;
use hyper::header::HeaderValue;

use crate::wire::WorkspaceQueryScope;
use crate::wire::leadership_ballots::{
    LeadershipBallotCastRequest, LeadershipBallotCastResponse, LeadershipBallotQueryRequest,
    LeadershipBallotQueryResponse,
};

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_LOCATOR_BYTES: usize = 4_096;

impl OrchestrationHttpClient {
    /// Cast one source-fenced leadership ballot.
    ///
    /// # Errors
    /// Returns invalid scope/identity input, typed service rejection, or an
    /// uncertain outcome carrying the exact ballot observation identity.
    pub async fn cast_leadership_ballot(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipBallotCastRequest,
    ) -> Result<LeadershipBallotCastResponse, ClientError> {
        validate_cast_request(request)?;
        let readback = ReadbackTarget::LeadershipBallot {
            workspace_id: request.scope.workspace_id,
            activity_id: request.activity_id,
            session_id: request.session_id,
            election_id: request.election_id,
        };
        let path = format!(
            "/internal/v1/workspaces/{}/leadership/ballots/cast",
            request.scope.workspace_id
        );
        let response: LeadershipBallotCastResponse = self
            .call(
                authorization,
                Method::POST,
                path,
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_cast_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one immutable ballot observation for the selected election.
    /// Session ownership authorizes the read but does not alter ballot identity.
    ///
    /// # Errors
    /// Returns invalid scope/identity input, typed service rejection, bounded
    /// transport failures, or a malformed ballot projection.
    pub async fn leadership_ballot(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipBallotQueryRequest,
    ) -> Result<LeadershipBallotQueryResponse, ClientError> {
        validate_query_request(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/leadership/ballots/query",
            request.scope.workspace_id
        );
        let response: LeadershipBallotQueryResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if !valid_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_cast_request(request: &LeadershipBallotCastRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.activity_id)?;
    require_identity(request.session_id)?;
    require_identity(request.election_id)
}

fn validate_query_request(request: &LeadershipBallotQueryRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.activity_id)?;
    require_identity(request.session_id)?;
    require_identity(request.election_id)
}

fn validate_scope(scope: &WorkspaceQueryScope) -> Result<(), ClientError> {
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

fn valid_cast_response(
    response: &LeadershipBallotCastResponse,
    request: &LeadershipBallotCastRequest,
) -> bool {
    response.scope == request.scope
        && response.activity_id == request.activity_id
        && response.session_id == request.session_id
        && response.receipt.election_id == request.election_id
        && !response.receipt.voter_participant_id.is_nil()
        && response
            .receipt
            .voter_participation_id
            .is_none_or(|id| !id.is_nil())
        && response.receipt.choice == request.choice
        && response.receipt.ballot_revision == 1
}

fn valid_query_response(
    response: &LeadershipBallotQueryResponse,
    request: &LeadershipBallotQueryRequest,
) -> bool {
    response.scope == request.scope
        && response.activity_id == request.activity_id
        && response.session_id == request.session_id
        && response.ballot.as_ref().is_none_or(|ballot| {
            ballot.election_id == request.election_id
                && !ballot.voter_participant_id.is_nil()
                && ballot.voter_participation_id.is_none_or(|id| !id.is_nil())
                && !ballot.cast_by_participant_id.is_nil()
                && ballot.ballot_revision == 1
                && ballot.cast_at_ms >= 0
        })
}

#[cfg(test)]
mod tests {
    use crate::wire::leadership_ballots::LeadershipBallotChoice;
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    use super::*;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 1,
            workspace_revision: 2,
            locator: "workspace".to_owned(),
        }
    }

    #[test]
    fn ballot_query_distinguishes_explicit_absence_from_an_incomplete_reply()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut reply = serde_json::json!({
            "scope": scope(), "activityId": Uuid::from_u128(3),
            "sessionId": Uuid::from_u128(4), "ballot": null,
        });
        assert!(serde_json::from_value::<LeadershipBallotQueryResponse>(reply.clone()).is_ok());
        let _removed = reply
            .as_object_mut()
            .ok_or("reply object")?
            .remove("ballot");
        assert!(serde_json::from_value::<LeadershipBallotQueryResponse>(reply).is_err());
        let mut receipt = serde_json::json!({
            "electionId": Uuid::from_u128(5), "voterParticipantId": Uuid::from_u128(6),
            "voterParticipationId": null, "choice": "YES", "ballotRevision": 1, "replay": false,
        });
        assert!(
            serde_json::from_value::<crate::wire::leadership_ballots::LeadershipBallotReceiptWire>(
                receipt.clone()
            )
            .is_ok()
        );
        let _removed = receipt
            .as_object_mut()
            .ok_or("receipt object")?
            .remove("voterParticipationId");
        assert!(
            serde_json::from_value::<crate::wire::leadership_ballots::LeadershipBallotReceiptWire>(
                receipt
            )
            .is_err()
        );
        Ok(())
    }

    fn cast_request() -> LeadershipBallotCastRequest {
        LeadershipBallotCastRequest {
            scope: scope(),
            activity_id: Uuid::from_u128(3),
            session_id: Uuid::from_u128(4),
            election_id: Uuid::from_u128(5),
            choice: LeadershipBallotChoice::Yes,
        }
    }

    #[test]
    fn ballot_response_validation_keeps_exact_scope_choice_and_revision() {
        let request = cast_request();
        let response = LeadershipBallotCastResponse {
            scope: request.scope.clone(),
            activity_id: request.activity_id,
            session_id: request.session_id,
            receipt: crate::wire::leadership_ballots::LeadershipBallotReceiptWire {
                election_id: request.election_id,
                voter_participant_id: Uuid::from_u128(6),
                voter_participation_id: Some(Uuid::from_u128(7)),
                choice: LeadershipBallotChoice::Yes,
                ballot_revision: 1,
                replay: false,
            },
        };
        assert!(valid_cast_response(&response, &request));
        let mut malformed = response;
        malformed.receipt.ballot_revision = 2;
        assert!(!valid_cast_response(&malformed, &request));
    }

    #[test]
    fn query_rejects_foreign_or_invalid_ballot_observation() {
        let request = LeadershipBallotQueryRequest {
            scope: scope(),
            activity_id: Uuid::from_u128(3),
            session_id: Uuid::from_u128(4),
            election_id: Uuid::from_u128(5),
        };
        let response = LeadershipBallotQueryResponse {
            scope: request.scope.clone(),
            activity_id: request.activity_id,
            session_id: request.session_id,
            ballot: None,
        };
        assert!(valid_query_response(&response, &request));
        let mut malformed = response;
        malformed.scope.workspace_revision += 1;
        assert!(!valid_query_response(&malformed, &request));
    }

    #[tokio::test]
    async fn malformed_cast_exchange_keeps_exact_ballot_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(
            listener.local_addr()?,
            std::time::Duration::from_secs(1),
        )?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await?;
            let body = b"{}";
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await?;
            socket.write_all(body).await?;
            Ok::<_, std::io::Error>(())
        });
        let request = cast_request();
        assert!(matches!(
            client
                .cast_leadership_ballot(&HeaderValue::from_static("Bearer fixture"), &request)
                .await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::LeadershipBallot {
                    workspace_id,
                    activity_id,
                    session_id,
                    election_id,
                }
            }) if workspace_id == request.scope.workspace_id
                && activity_id == request.activity_id
                && session_id == request.session_id
                && election_id == request.election_id
        ));
        server.await??;
        Ok(())
    }
}
