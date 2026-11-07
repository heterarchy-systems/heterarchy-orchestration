//! Official client methods for permanent-transfer Source dispatch and recovery.

use hyper::Method;
use hyper::header::HeaderValue;

use super::super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::team_expansion::TeamExpansionActorWire;
use crate::wire::team_permanent_transfer as wire;

impl OrchestrationHttpClient {
    /// Commit and send only a newly created Source dispatch permit. An
    /// uncertain response retains the exact mutation request for query-only
    /// recovery; this method never retries automatically.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, unknown outcome with the exact
    /// dispatch identity, or a malformed response classified as unknown.
    pub async fn dispatch_team_permanent_transfer_source(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferSourceDispatchRequest,
    ) -> Result<wire::TeamPermanentTransferSourceDispatchResponse, ClientError> {
        if !wire::valid_team_permanent_transfer_source_dispatch_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback =
            ReadbackTarget::TeamPermanentTransferSourceDispatch(Box::new(request.clone()));
        let response: wire::TeamPermanentTransferSourceDispatchResponse = self
            .call_with_response_limit(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/source/dispatch",
                    request.actor.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
                wire::MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES,
            )
            .await?;
        if !wire::valid_team_permanent_transfer_source_dispatch_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the local immutable dispatch and optional Source fact. This route
    /// does not contact Source and remains independent of the original actor.
    ///
    /// # Errors
    /// Returns malformed scope, a bounded transport failure, or a malformed
    /// local readback.
    pub async fn read_team_permanent_transfer_source(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferSourceReadbackRequest,
    ) -> Result<wire::TeamPermanentTransferSourceReadbackResponse, ClientError> {
        if !wire::valid_team_permanent_transfer_source_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamPermanentTransferSourceReadbackResponse = self
            .call_with_response_limit(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/source/readback",
                    request.workspace_id
                ),
                Some(request),
                None,
                wire::MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES,
            )
            .await?;
        if !wire::valid_team_permanent_transfer_source_readback_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Explicitly reconcile one stored dispatch by asking the Backend's
    /// source owner to perform the Source `Get` operation and persist the
    /// returned fact. This never issues Source `Prepare` or `AbortUnstarted`.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, an uncertain local receipt
    /// outcome with exact readback identity, or malformed mutation response.
    pub async fn reconcile_team_permanent_transfer_source(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferSourceReconcileRequest,
    ) -> Result<wire::TeamPermanentTransferSourceReconcileResponse, ClientError> {
        if !wire::valid_team_permanent_transfer_source_reconcile_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback =
            ReadbackTarget::TeamPermanentTransferSourceReadback(Box::new(request.clone()));
        let response: wire::TeamPermanentTransferSourceReconcileResponse = self
            .call_with_response_limit(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/source/reconcile",
                    request.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
                wire::MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES,
            )
            .await?;
        if !wire::valid_team_permanent_transfer_source_reconcile_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Resolve a possibly lost dispatch result by reading the local journal
    /// first. If its exact dispatch exists but the Source fact is absent, an
    /// explicit option may request reconciliation through Source `Get`. This
    /// helper never calls the dispatch endpoint or a Source mutation.
    ///
    /// # Errors
    /// Returns invalid original/current actor context, a readback failure, a
    /// corrupt mismatch with the original mutation, or reconciliation failure.
    pub async fn recover_team_permanent_transfer_source(
        &self,
        authorization: &HeaderValue,
        original: &wire::TeamPermanentTransferSourceDispatchRequest,
        current_actor: &TeamExpansionActorWire,
        reconcile_missing_source_fact: bool,
    ) -> Result<wire::TeamPermanentTransferSourceReadbackResponse, ClientError> {
        if !wire::valid_team_permanent_transfer_source_dispatch_request(original) {
            return Err(ClientError::InvalidRequest);
        }
        let request = wire::TeamPermanentTransferSourceReadbackRequest {
            workspace_id: original.actor.scope.workspace_id,
            preparation_id: original.command.preparation_id,
            current_actor: current_actor.clone(),
        };
        if !wire::valid_team_permanent_transfer_source_readback_request(&request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = self
            .read_team_permanent_transfer_source(authorization, &request)
            .await?;
        let Some(dispatch) = readback.dispatch.as_ref() else {
            // There is no local permit, so Source has not been contacted.
            return Ok(readback);
        };
        if !wire::valid_team_permanent_transfer_source_dispatch_recovery(original, dispatch) {
            return Err(ClientError::Protocol);
        }
        if readback.source.is_some() || !reconcile_missing_source_fact {
            return Ok(readback);
        }

        let reconciled = self
            .reconcile_team_permanent_transfer_source(authorization, &request)
            .await?;
        if reconciled.dispatch.as_ref().is_none_or(|dispatch| {
            !wire::valid_team_permanent_transfer_source_dispatch_recovery(original, dispatch)
        }) {
            return Err(ClientError::Protocol);
        }
        Ok(reconciled)
    }
}

#[cfg(test)]
mod tests {
    use std::{io, net::SocketAddr, time::Duration};

    use hyper::header::HeaderValue;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use uuid::Uuid;

    use super::*;
    use crate::wire::{
        WorkspaceQueryScope,
        team_expansion::TeamExpansionActorWire,
        team_permanent_transfer::{
            TeamPermanentTransferSourceReadbackRequest,
            TeamPermanentTransferSourceReadbackResponse, test_dispatch_fixture_parts,
        },
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn actor(workspace_revision: i64) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: id(100),
                location_id: id(101),
                location_revision: 3,
                workspace_revision,
                locator: "project://unit-test".to_owned(),
            },
            activity_id: id(102),
            session_id: id(103),
        }
    }

    fn readback_request(
        current_actor: TeamExpansionActorWire,
    ) -> TeamPermanentTransferSourceReadbackRequest {
        TeamPermanentTransferSourceReadbackRequest {
            workspace_id: current_actor.scope.workspace_id,
            preparation_id: id(105),
            current_actor,
        }
    }

    async fn start_server(
        responses: Vec<Vec<u8>>,
    ) -> TestResult<(SocketAddr, tokio::task::JoinHandle<io::Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let authority = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let mut request_lines = Vec::with_capacity(responses.len());
            for response_body in responses {
                let (mut stream, _) = listener.accept().await?;
                let request = read_request(&mut stream).await?;
                request_lines.push(
                    request
                        .lines()
                        .next()
                        .ok_or_else(|| io::Error::other("HTTP request line missing"))?
                        .to_owned(),
                );
                let headers = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    response_body.len()
                );
                stream.write_all(headers.as_bytes()).await?;
                stream.write_all(&response_body).await?;
            }
            Ok(request_lines)
        });
        Ok((authority, task))
    }

    async fn read_request(stream: &mut TcpStream) -> io::Result<String> {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 4096];
        let mut body_end = None;
        loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "HTTP request ended early",
                ));
            }
            bytes.extend_from_slice(&chunk[..read]);
            if body_end.is_none()
                && let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n")
            {
                let header_text = String::from_utf8_lossy(&bytes[..header_end]);
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                body_end = Some(header_end + 4 + content_length);
            }
            if body_end.is_some_and(|end| bytes.len() >= end) {
                return Ok(String::from_utf8_lossy(&bytes).into_owned());
            }
        }
    }

    fn make_client(authority: SocketAddr) -> TestResult<OrchestrationHttpClient> {
        Ok(OrchestrationHttpClient::new(
            authority,
            Duration::from_secs(1),
        )?)
    }

    #[tokio::test]
    async fn malformed_dispatch_reply_is_unknown_and_never_retried() -> TestResult {
        let (request, _dispatch) = test_dispatch_fixture_parts()?;
        let (authority, server) = start_server(vec![b"{\"wrong\":true}".to_vec()]).await?;
        let client = make_client(authority)?;
        let authorization = HeaderValue::from_static("Bearer test-credential");
        let Err(error) = client
            .dispatch_team_permanent_transfer_source(&authorization, &request)
            .await
        else {
            return Err("malformed mutation reply was accepted".into());
        };
        assert!(matches!(
            error,
            ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamPermanentTransferSourceDispatch(original)
            } if original.as_ref() == &request
        ));
        assert_eq!(
            server.await??,
            vec![
                "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000064/teams/permanent-transfers/source/dispatch HTTP/1.1"
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn readback_then_optional_reconcile_never_reposts_dispatch() -> TestResult {
        let (original, dispatch) = test_dispatch_fixture_parts()?;
        let current_actor = actor(11);
        let request = readback_request(current_actor);
        let readback = TeamPermanentTransferSourceReadbackResponse {
            request: request.clone(),
            dispatch: Some(dispatch.clone()),
            source: None,
        };
        let reconcile = TeamPermanentTransferSourceReadbackResponse {
            request: request.clone(),
            dispatch: Some(dispatch),
            source: None,
        };
        let bodies = vec![
            serde_json::to_vec(&readback)?,
            serde_json::to_vec(&reconcile)?,
        ];
        let (authority, server) = start_server(bodies).await?;
        let client = make_client(authority)?;
        let authorization = HeaderValue::from_static("Bearer test-credential");
        let recovered = client
            .recover_team_permanent_transfer_source(&authorization, &original, &actor(11), true)
            .await?;
        assert!(recovered.dispatch.is_some());
        assert!(recovered.source.is_none());
        assert_eq!(
            server.await??,
            vec![
                "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000064/teams/permanent-transfers/source/readback HTTP/1.1",
                "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000064/teams/permanent-transfers/source/reconcile HTTP/1.1",
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_local_dispatch_stops_recovery_before_source_reconcile() -> TestResult {
        let (original, _) = test_dispatch_fixture_parts()?;
        let request = readback_request(actor(12));
        let no_attempt = TeamPermanentTransferSourceReadbackResponse {
            request,
            dispatch: None,
            source: None,
        };
        let (authority, server) = start_server(vec![serde_json::to_vec(&no_attempt)?]).await?;
        let client = make_client(authority)?;
        let authorization = HeaderValue::from_static("Bearer test-credential");
        let recovered = client
            .recover_team_permanent_transfer_source(&authorization, &original, &actor(12), true)
            .await?;
        assert!(recovered.dispatch.is_none());
        assert!(recovered.source.is_none());
        assert_eq!(
            server.await??,
            vec![
                "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000064/teams/permanent-transfers/source/readback HTTP/1.1"
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_and_reconcile_failures_keep_distinct_recovery_classification() -> TestResult {
        let request = readback_request(actor(10));
        let (authority, server) = start_server(vec![b"{}".to_vec()]).await?;
        let client = make_client(authority)?;
        let authorization = HeaderValue::from_static("Bearer test-credential");
        assert!(matches!(
            client
                .read_team_permanent_transfer_source(&authorization, &request)
                .await,
            Err(ClientError::Protocol)
        ));
        assert_eq!(
            server.await??,
            vec![
                "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000064/teams/permanent-transfers/source/readback HTTP/1.1"
            ],
        );

        let (authority, server) = start_server(vec![b"{}".to_vec()]).await?;
        let client = make_client(authority)?;
        let Err(error) = client
            .reconcile_team_permanent_transfer_source(&authorization, &request)
            .await
        else {
            return Err("malformed reconcile reply was accepted".into());
        };
        assert!(matches!(
            error,
            ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamPermanentTransferSourceReadback(original)
            } if original.as_ref() == &request
        ));
        assert_eq!(
            server.await??,
            vec![
                "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000064/teams/permanent-transfers/source/reconcile HTTP/1.1"
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn oversized_dispatch_response_is_bounded_and_unknown() -> TestResult {
        let (request, _) = test_dispatch_fixture_parts()?;
        let oversized = vec![b'x'; wire::MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES + 1];
        let (authority, server) = start_server(vec![oversized]).await?;
        let client = make_client(authority)?;
        let authorization = HeaderValue::from_static("Bearer test-credential");
        let Err(error) = client
            .dispatch_team_permanent_transfer_source(&authorization, &request)
            .await
        else {
            return Err("oversized mutation response was accepted".into());
        };
        assert!(matches!(
            error,
            ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamPermanentTransferSourceDispatch(original)
            } if original.as_ref() == &request
        ));
        assert_eq!(
            server.await??,
            vec![
                "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000064/teams/permanent-transfers/source/dispatch HTTP/1.1"
            ],
        );
        Ok(())
    }
}
