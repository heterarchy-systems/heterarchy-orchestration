//! Typed V2 Team-expansion approval calls with exact plan readback.

use hyper::Method;
use hyper::header::HeaderValue;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::team_expansion_v2 as wire;

impl OrchestrationHttpClient {
    /// Commit one exact V2 decision with a complete ordered Workspace target
    /// plan. The client retains the original request after an uncertain result
    /// and never retries the mutation automatically.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, unknown outcome carrying the
    /// exact V2 readback target, or malformed response.
    pub async fn mutate_team_expansion_decision_v2(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamExpansionDecisionV2Request,
    ) -> Result<wire::TeamExpansionDecisionV2Response, ClientError> {
        if !wire::valid_decision_v2_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::TeamExpansionDecisionV2(Box::new(request.clone()));
        let response: wire::TeamExpansionDecisionV2Response = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v2/workspaces/{}/teams/expansion/decide",
                    request.actor.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_decision_v2_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read an exact V2 decision and its frozen slot plan after an unknown
    /// mutation outcome. This is a pure query and cannot retry the approval.
    ///
    /// # Errors
    /// Returns invalid request, typed authorization/service rejection, bounded
    /// transport failure, or a malformed source readback.
    pub async fn team_expansion_decision_v2_readback(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamExpansionDecisionV2ReadbackRequest,
    ) -> Result<wire::TeamExpansionDecisionV2ReadbackResponse, ClientError> {
        if !wire::valid_decision_v2_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamExpansionDecisionV2ReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v2/workspaces/{}/teams/expansion/readback",
                    request.current_actor.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_decision_v2_readback_response(&response, request) {
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
    use crate::wire::{
        WorkspaceQueryScope,
        team_expansion::{TeamExpansionActorReceiptWire, TeamExpansionDecisionStateWire},
        team_expansion_v2::{
            TeamExpansionApprovalSlotV2Wire, TeamExpansionApprovalTargetV2Wire,
            TeamExpansionDecisionV2ReadbackRequest, TeamExpansionDecisionV2ReadbackResponse,
            TeamExpansionDecisionV2ReceiptWire, TeamExpansionDecisionV2Request,
        },
    };

    const CREDENTIAL: &str = "Bearer team-expansion-v2-client-fixture-secret";

    fn actor(session_id: Uuid) -> crate::wire::team_expansion::TeamExpansionActorWire {
        crate::wire::team_expansion::TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(0xE201),
                location_id: Uuid::from_u128(0xE202),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(0xE203),
            session_id,
        }
    }

    fn request() -> TeamExpansionDecisionV2Request {
        TeamExpansionDecisionV2Request {
            actor: actor(Uuid::from_u128(0xE204)),
            command: crate::wire::team_expansion::TeamExpansionDecisionAction {
                team_id: Uuid::from_u128(0xE205),
                request_id: Uuid::from_u128(0xE206),
                decision_id: Uuid::from_u128(0xE207),
                expected_team_revision: 2,
                expected_capacity_revision: 1,
                expected_leadership_revision: 3,
                expected_requester_leadership_revision: 1,
                decision: crate::wire::team_expansion::TeamExpansionDecisionWire::Approve {
                    approved_count: 1,
                },
            },
            slot_plan: vec![TeamExpansionApprovalSlotV2Wire {
                ordinal: 0,
                target: TeamExpansionApprovalTargetV2Wire {
                    profile_id: Uuid::from_u128(0xE208),
                    participation_id: Uuid::from_u128(0xE209),
                },
            }],
        }
    }

    fn receipt(request: &TeamExpansionDecisionV2Request) -> TeamExpansionDecisionV2ReceiptWire {
        TeamExpansionDecisionV2ReceiptWire {
            decision: crate::wire::team_expansion::TeamExpansionDecisionReceiptWire {
                decision_id: request.command.decision_id,
                request_id: request.command.request_id,
                workspace_id: request.actor.scope.workspace_id,
                team_id: request.command.team_id,
                state: TeamExpansionDecisionStateWire::ApprovedPendingAnchor,
                approved_count: 1,
                cooldown_started_at_ms: None,
                cooldown_until_ms: None,
                finalized_by: None,
                finalized_by_leadership_revision: None,
                expected_team_revision: request.command.expected_team_revision,
                capacity_revision: request.command.expected_capacity_revision,
                leadership_revision: request.command.expected_leadership_revision,
                actor: actor_receipt(&request.actor),
                recorded_at_ms: 100,
                replayed: false,
            },
            slot_plan: request.slot_plan.clone(),
        }
    }

    fn actor_receipt(
        actor: &crate::wire::team_expansion::TeamExpansionActorWire,
    ) -> TeamExpansionActorReceiptWire {
        TeamExpansionActorReceiptWire {
            activity_id: actor.activity_id,
            agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
                actor.scope.workspace_id,
                actor.activity_id,
                actor.session_id,
            )
            .into_uuid(),
            session_id: actor.session_id,
            participation_id: Uuid::from_u128(0xE20B),
            profile_id: Uuid::from_u128(0xE20C),
            participant_id: Uuid::from_u128(0xE20D),
            display_name: "orchestrator".to_owned(),
        }
    }

    #[tokio::test]
    async fn uncertain_v2_approval_keeps_full_target_plan_and_never_retries()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let (client, server) = fixture(serde_json::json!({})).await?;
        assert!(matches!(
            client
                .mutate_team_expansion_decision_v2(
                    &HeaderValue::from_static(CREDENTIAL),
                    &request,
                )
                .await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamExpansionDecisionV2(original)
            }) if *original == request
        ));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/internal/v2/workspaces/"));
        assert!(captured.contains("/teams/expansion/decide HTTP/1.1"));
        assert!(captured.contains(&request.slot_plan[0].target.profile_id.to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn v2_readback_preserves_the_original_plan_and_rejects_substitution()
    -> Result<(), Box<dyn std::error::Error>> {
        let intent = request();
        let current_actor = actor(Uuid::from_u128(0xE210));
        let request = TeamExpansionDecisionV2ReadbackRequest {
            intent: intent.clone(),
            current_actor,
        };
        let response = TeamExpansionDecisionV2ReadbackResponse {
            request: request.clone(),
            result: Some(receipt(&intent)),
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let recovered = client
            .team_expansion_decision_v2_readback(&HeaderValue::from_static(CREDENTIAL), &request)
            .await?;
        assert_eq!(recovered.request, request);
        assert_eq!(recovered.result, Some(receipt(&intent)));
        let _captured = server.await??;

        let mut substituted = receipt(&intent);
        substituted.slot_plan[0].target.profile_id = Uuid::from_u128(0xE211);
        let malformed = TeamExpansionDecisionV2ReadbackResponse {
            request: request.clone(),
            result: Some(substituted),
        };
        let (bad_client, bad_server) = fixture(serde_json::to_value(malformed)?).await?;
        assert!(matches!(
            bad_client
                .team_expansion_decision_v2_readback(
                    &HeaderValue::from_static(CREDENTIAL),
                    &request,
                )
                .await,
            Err(ClientError::Protocol)
        ));
        let _captured = bad_server.await??;
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
            Ok(request)
        });
        Ok((client, server))
    }
}
