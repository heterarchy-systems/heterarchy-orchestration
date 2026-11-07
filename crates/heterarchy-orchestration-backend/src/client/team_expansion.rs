//! Typed client calls for Team capacity and expansion mutations.

use hyper::Method;
use hyper::header::HeaderValue;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::team_expansion as wire;

impl OrchestrationHttpClient {
    /// Read the current Team-specific cap. A missing snapshot is an explicit
    /// legacy-unconfigured state, not an inferred default.
    ///
    /// # Errors
    /// Rejects a malformed query, bounded transport failure, service refusal,
    /// or malformed response.
    pub async fn team_capacity(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamCapacityQueryRequest,
    ) -> Result<wire::TeamCapacityQueryResponse, ClientError> {
        if !wire::capacity_query_request_is_valid(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamCapacityQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/capacity/query",
                    request.actor.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::capacity_query_response_is_valid(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Submit one exact capacity, request, decision, or slot-assignment intent.
    /// The original intent is retained for actor-bound recovery; no mutation
    /// is automatically retried after an uncertain response.
    ///
    /// # Errors
    /// Returns invalid request, typed service refusal, unknown outcome requiring
    /// exact readback, or malformed response.
    pub async fn mutate_team_expansion(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamExpansionMutationRequest,
    ) -> Result<wire::TeamExpansionMutationResponse, ClientError> {
        if !wire::valid_mutation_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::TeamExpansionMutation(Box::new(request.clone()));
        let path = match &request.action {
            wire::TeamExpansionMutationActionWire::SetCapacity { .. } => format!(
                "/internal/v1/workspaces/{}/teams/capacity/set",
                request.actor.scope.workspace_id
            ),
            _ => format!(
                "/internal/v1/workspaces/{}/teams/expansion",
                request.actor.scope.workspace_id
            ),
        };
        let response: wire::TeamExpansionMutationResponse = self
            .call(
                authorization,
                Method::POST,
                path,
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_mutation_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        // An explicit ApprovedPendingAnchor receipt is a known durable state,
        // not a transport-unknown outcome. The current Orchestrator can recover
        // its exact decision_id with FinalizeAnchor; no new approval is implied.
        Ok(response)
    }

    /// Read the exact original expansion intent using the current authenticated
    /// requester, target or Orchestrator Session permitted by the owner.
    ///
    /// # Errors
    /// Returns invalid selector, bounded transport failure, service refusal,
    /// or malformed readback. Callers must not substitute a new request ID.
    pub async fn team_expansion_readback(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamExpansionReadbackRequest,
    ) -> Result<wire::TeamExpansionReadbackResponse, ClientError> {
        if !wire::valid_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamExpansionReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/expansion/readback",
                    request.current_actor.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_readback_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    use super::*;
    use crate::{client::ReadbackTarget, wire::leadership::WorkspaceQueryScope};

    const CREDENTIAL: &str = "Bearer team-expansion-client-fixture-secret";

    fn request() -> wire::TeamExpansionMutationRequest {
        wire::TeamExpansionMutationRequest {
            actor: wire::TeamExpansionActorWire {
                scope: WorkspaceQueryScope {
                    workspace_id: Uuid::from_u128(0xE001),
                    location_id: Uuid::from_u128(0xE002),
                    location_revision: 3,
                    workspace_revision: 4,
                    locator: "workspace".to_owned(),
                },
                activity_id: Uuid::from_u128(0xE003),
                session_id: Uuid::from_u128(0xE004),
            },
            action: wire::TeamExpansionMutationActionWire::Request {
                command: wire::TeamExpansionRequestAction {
                    team_id: Uuid::from_u128(0xE005),
                    request_id: Uuid::from_u128(0xE006),
                    expected_team_revision: 1,
                    expected_capacity_revision: 1,
                    expected_leadership_revision: 2,
                    requested_count: 1,
                    reason: "reviewed workload expansion".to_owned(),
                },
            },
        }
    }

    fn actor(session_id: Uuid) -> wire::TeamExpansionActorWire {
        wire::TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(0xE101),
                location_id: Uuid::from_u128(0xE102),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(0xE103),
            session_id,
        }
    }

    fn finalize_request(
        actor: wire::TeamExpansionActorWire,
        expected_leadership_revision: i64,
    ) -> wire::TeamExpansionMutationRequest {
        wire::TeamExpansionMutationRequest {
            actor,
            action: wire::TeamExpansionMutationActionWire::FinalizeAnchor {
                command: wire::TeamExpansionFinalizeAnchorAction {
                    request_id: Uuid::from_u128(0xE104),
                    decision_id: Uuid::from_u128(0xE105),
                    expected_leadership_revision,
                },
            },
        }
    }

    fn anchored_decision(
        workspace_id: Uuid,
        finalizer_session_id: Uuid,
        finalizer_leadership_revision: i64,
        replayed: bool,
    ) -> wire::TeamExpansionDecisionReceiptWire {
        wire::TeamExpansionDecisionReceiptWire {
            decision_id: Uuid::from_u128(0xE105),
            request_id: Uuid::from_u128(0xE104),
            workspace_id,
            team_id: Uuid::from_u128(0xE106),
            state: wire::TeamExpansionDecisionStateWire::Approved,
            approved_count: 1,
            cooldown_started_at_ms: Some(100),
            cooldown_until_ms: Some(100 + 60 * 60 * 1_000),
            finalized_by: Some(wire::TeamExpansionActorReceiptWire {
                activity_id: Uuid::from_u128(0xE103),
                agent_id: Uuid::from_u128(0xE107),
                session_id: finalizer_session_id,
                participation_id: Uuid::from_u128(0xE108),
                profile_id: Uuid::from_u128(0xE109),
                participant_id: Uuid::from_u128(0xE10A),
                display_name: "orchestrator".to_owned(),
            }),
            finalized_by_leadership_revision: Some(finalizer_leadership_revision),
            expected_team_revision: 1,
            capacity_revision: 2,
            leadership_revision: 4,
            actor: wire::TeamExpansionActorReceiptWire {
                activity_id: Uuid::from_u128(0xE103),
                agent_id: Uuid::from_u128(0xE10B),
                session_id: Uuid::from_u128(0xE10C),
                participation_id: Uuid::from_u128(0xE10D),
                profile_id: Uuid::from_u128(0xE10E),
                participant_id: Uuid::from_u128(0xE10F),
                display_name: "decision orchestrator".to_owned(),
            },
            recorded_at_ms: 100,
            replayed,
        }
    }

    #[tokio::test]
    async fn malformed_success_preserves_exact_expansion_intent_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let (client, server) = fixture(serde_json::json!({})).await?;

        let result = client
            .mutate_team_expansion(&HeaderValue::from_static(CREDENTIAL), &request)
            .await;
        assert!(matches!(
            result,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamExpansionMutation(original)
            }) if *original == request
        ));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/expansion HTTP/1.1"));
        let wire_request = serde_json::to_string(&request)?;
        assert!(captured.ends_with(&wire_request));
        let request_id = match &request.action {
            wire::TeamExpansionMutationActionWire::Request { command } => command.request_id,
            _ => return Err("fixture must use a Team expansion request".into()),
        };
        assert!(captured.contains(&request_id.to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn replaced_orchestrator_replay_and_exact_readback_preserve_original_finalizer()
    -> Result<(), Box<dyn std::error::Error>> {
        let current_orchestrator = actor(Uuid::from_u128(0xE110));
        let original_finalizer = actor(Uuid::from_u128(0xE111));
        let mutation_request = finalize_request(current_orchestrator.clone(), 6);
        let replayed_receipt = anchored_decision(
            current_orchestrator.scope.workspace_id,
            original_finalizer.session_id,
            3,
            true,
        );
        let mutation_response = wire::TeamExpansionMutationResponse {
            request: mutation_request.clone(),
            result: wire::TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(replayed_receipt.clone()),
            },
        };
        let (client, server) = fixture(serde_json::to_value(mutation_response)?).await?;
        let direct_replay = client
            .mutate_team_expansion(&HeaderValue::from_static(CREDENTIAL), &mutation_request)
            .await?;
        let wire::TeamExpansionMutationResultWire::Decision { receipt } = direct_replay.result
        else {
            return Err("finalize replay must return a decision receipt".into());
        };
        assert!(receipt.replayed);
        assert!(
            receipt
                .finalized_by
                .as_ref()
                .is_some_and(|actor| { actor.session_id == original_finalizer.session_id })
        );
        let direct_receipt = *receipt;
        let _captured = server.await??;

        let readback_request = wire::TeamExpansionReadbackRequest {
            intent: finalize_request(original_finalizer.clone(), 3),
            current_actor: current_orchestrator.clone(),
        };
        let mut durable_receipt = replayed_receipt;
        durable_receipt.replayed = false;
        let readback_response = wire::TeamExpansionReadbackResponse {
            request: readback_request.clone(),
            result: Some(wire::TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(durable_receipt.clone()),
            }),
        };
        let (readback_client, readback_server) =
            fixture(serde_json::to_value(readback_response)?).await?;
        let readback = readback_client
            .team_expansion_readback(&HeaderValue::from_static(CREDENTIAL), &readback_request)
            .await?;
        let Some(wire::TeamExpansionMutationResultWire::Decision { receipt }) = readback.result
        else {
            return Err("authorized readback must return the durable decision".into());
        };
        assert_eq!(*receipt, durable_receipt);
        let mut direct_as_readback = direct_receipt.clone();
        direct_as_readback.replayed = false;
        assert_eq!(direct_as_readback, durable_receipt);
        let _captured = readback_server.await??;

        let mut substituted = readback_response_for(&readback_request, durable_receipt);
        if let Some(wire::TeamExpansionMutationResultWire::Decision { receipt }) =
            substituted.result.as_mut()
            && let Some(finalizer) = receipt.finalized_by.as_mut()
        {
            finalizer.session_id = Uuid::from_u128(0xE112);
        }
        let (bad_client, bad_server) = fixture(serde_json::to_value(substituted)?).await?;
        assert!(matches!(
            bad_client
                .team_expansion_readback(&HeaderValue::from_static(CREDENTIAL), &readback_request,)
                .await,
            Err(ClientError::Protocol)
        ));
        let _captured = bad_server.await??;
        Ok(())
    }

    fn readback_response_for(
        request: &wire::TeamExpansionReadbackRequest,
        receipt: wire::TeamExpansionDecisionReceiptWire,
    ) -> wire::TeamExpansionReadbackResponse {
        wire::TeamExpansionReadbackResponse {
            request: request.clone(),
            result: Some(wire::TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(receipt),
            }),
        }
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
            Ok(request)
        });
        Ok((client, server))
    }
}
