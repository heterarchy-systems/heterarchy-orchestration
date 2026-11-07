//! Typed V3 Team-expansion approval calls with exact source Home readback.

use hyper::Method;
use hyper::header::HeaderValue;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::team_expansion_v3 as wire;

impl OrchestrationHttpClient {
    /// Submit one exact V3 approval plan. Uncertain outcomes retain the full
    /// original plan and are recovered only through V3 exact readback.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, unknown outcome with the exact
    /// V3 intent, or malformed response.
    pub async fn mutate_team_expansion_decision_v3(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamExpansionDecisionV3Request,
    ) -> Result<wire::TeamExpansionDecisionV3Response, ClientError> {
        if !wire::valid_decision_v3_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::TeamExpansionDecisionV3(Box::new(request.clone()));
        let response: wire::TeamExpansionDecisionV3Response = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v3/workspaces/{}/teams/expansion/decide",
                    request.actor.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_decision_v3_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the exact V3 decision and original target/source Home plan after
    /// an uncertain mutation. This query never retries or substitutes V2.
    ///
    /// # Errors
    /// Returns invalid request, typed authorization/service rejection, bounded
    /// transport failure, or malformed source readback.
    pub async fn team_expansion_decision_v3_readback(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamExpansionDecisionV3ReadbackRequest,
    ) -> Result<wire::TeamExpansionDecisionV3ReadbackResponse, ClientError> {
        if !wire::valid_decision_v3_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamExpansionDecisionV3ReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v3/workspaces/{}/teams/expansion/readback",
                    request.current_actor.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_decision_v3_readback_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read the authorized immutable V3 source by Workspace and decision ID.
    /// The current reader does not need the original issuer's target plan, and
    /// an absent/unauthorized/V2/legacy decision remains an explicit `None`.
    /// Operations validates the historical source plan; this client can only
    /// validate the returned plan's structure because the query has no intent.
    ///
    /// # Errors
    /// Returns invalid request, typed authorization/service rejection, bounded
    /// transport failure, or malformed source projection.
    pub async fn team_expansion_decision_v3_projection(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamExpansionDecisionV3ProjectionRequest,
    ) -> Result<wire::TeamExpansionDecisionV3ProjectionResponse, ClientError> {
        if !wire::valid_decision_v3_projection_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamExpansionDecisionV3ProjectionResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v3/workspaces/{}/teams/expansion/projection",
                    request.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_decision_v3_projection_response(&response, request) {
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
        team_expansion::{
            TeamExpansionActorReceiptWire, TeamExpansionActorWire, TeamExpansionDecisionAction,
            TeamExpansionDecisionStateWire, TeamExpansionDecisionWire,
        },
        team_expansion_v3::{
            TeamExpansionApprovalSlotV3Wire, TeamExpansionApprovalTargetV3Wire,
            TeamExpansionDecisionV3ProjectionRequest, TeamExpansionDecisionV3ProjectionResponse,
            TeamExpansionDecisionV3ProjectionWire, TeamExpansionDecisionV3ReadbackRequest,
            TeamExpansionDecisionV3ReadbackResponse, TeamExpansionDecisionV3ReceiptWire,
            TeamExpansionDecisionV3Request,
        },
    };

    const CREDENTIAL: &str = "Bearer team-expansion-v3-client-fixture-secret";

    fn actor(session_id: Uuid) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(0xE301),
                location_id: Uuid::from_u128(0xE302),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(0xE303),
            session_id,
        }
    }

    fn request() -> TeamExpansionDecisionV3Request {
        TeamExpansionDecisionV3Request {
            actor: actor(Uuid::from_u128(0xE304)),
            command: TeamExpansionDecisionAction {
                team_id: Uuid::from_u128(0xE305),
                request_id: Uuid::from_u128(0xE306),
                decision_id: Uuid::from_u128(0xE307),
                expected_team_revision: 2,
                expected_capacity_revision: 1,
                expected_leadership_revision: 3,
                expected_requester_leadership_revision: 1,
                decision: TeamExpansionDecisionWire::Approve { approved_count: 1 },
            },
            slot_plan: vec![TeamExpansionApprovalSlotV3Wire {
                ordinal: 0,
                target: TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
                    profile_id: Uuid::from_u128(0xE308),
                    participation_id: Uuid::from_u128(0xE309),
                    source_team_id: Uuid::from_u128(0xE30A),
                    source_membership_revision: 11,
                },
            }],
        }
    }

    fn actor_receipt(actor: &TeamExpansionActorWire) -> TeamExpansionActorReceiptWire {
        TeamExpansionActorReceiptWire {
            activity_id: actor.activity_id,
            agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
                actor.scope.workspace_id,
                actor.activity_id,
                actor.session_id,
            )
            .into_uuid(),
            session_id: actor.session_id,
            participation_id: Uuid::from_u128(0xE30B),
            profile_id: Uuid::from_u128(0xE30C),
            participant_id: Uuid::from_u128(0xE30D),
            display_name: "orchestrator".to_owned(),
        }
    }

    fn receipt(request: &TeamExpansionDecisionV3Request) -> TeamExpansionDecisionV3ReceiptWire {
        TeamExpansionDecisionV3ReceiptWire {
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

    fn projection(
        request: &TeamExpansionDecisionV3Request,
    ) -> TeamExpansionDecisionV3ProjectionWire {
        let decision = receipt(request).decision;
        TeamExpansionDecisionV3ProjectionWire {
            original_issuer: decision.actor.clone(),
            decision,
            slot_plan: request.slot_plan.clone(),
        }
    }

    #[tokio::test]
    async fn uncertain_v3_approval_keeps_full_transfer_plan_and_uses_v3_route()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let (client, server) = fixture(serde_json::json!({})).await?;
        assert!(matches!(
            client
                .mutate_team_expansion_decision_v3(
                    &HeaderValue::from_static(CREDENTIAL),
                    &request,
                )
                .await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamExpansionDecisionV3(original)
            }) if *original == request
        ));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/internal/v3/workspaces/"));
        assert!(captured.contains("/teams/expansion/decide HTTP/1.1"));
        let TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
            source_membership_revision,
            ..
        } = request.slot_plan[0].target
        else {
            return Err(io::Error::other("fixture must cover a source Home transfer").into());
        };
        assert!(captured.contains(&source_membership_revision.to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn v3_readback_preserves_source_plan_and_rejects_substitution()
    -> Result<(), Box<dyn std::error::Error>> {
        let intent = request();
        let request = TeamExpansionDecisionV3ReadbackRequest {
            intent: intent.clone(),
            current_actor: actor(Uuid::from_u128(0xE310)),
        };
        let response = TeamExpansionDecisionV3ReadbackResponse {
            request: request.clone(),
            result: Some(receipt(&intent)),
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let recovered = client
            .team_expansion_decision_v3_readback(&HeaderValue::from_static(CREDENTIAL), &request)
            .await?;
        assert_eq!(recovered.request, request);
        assert_eq!(recovered.result, Some(receipt(&intent)));
        let _captured = server.await??;

        let mut substituted = receipt(&intent);
        let TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
            source_membership_revision,
            ..
        } = &mut substituted.slot_plan[0].target
        else {
            return Err(io::Error::other("fixture must cover a source Home transfer").into());
        };
        *source_membership_revision += 1;
        let malformed = TeamExpansionDecisionV3ReadbackResponse {
            request: request.clone(),
            result: Some(substituted),
        };
        let (bad_client, bad_server) = fixture(serde_json::to_value(malformed)?).await?;
        assert!(matches!(
            bad_client
                .team_expansion_decision_v3_readback(
                    &HeaderValue::from_static(CREDENTIAL),
                    &request,
                )
                .await,
            Err(ClientError::Protocol)
        ));
        let _captured = bad_server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn current_o_projection_needs_no_original_plan_and_uses_only_v3_route()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = request();
        let request = TeamExpansionDecisionV3ProjectionRequest {
            workspace_id: original.actor.scope.workspace_id,
            decision_id: original.command.decision_id,
            current_actor: actor(Uuid::from_u128(0xE311)),
        };
        let response = TeamExpansionDecisionV3ProjectionResponse {
            request: request.clone(),
            result: Some(projection(&original)),
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let recovered = client
            .team_expansion_decision_v3_projection(&HeaderValue::from_static(CREDENTIAL), &request)
            .await?;
        assert_eq!(recovered.request, request);
        assert_eq!(recovered.result, Some(projection(&original)));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/internal/v3/workspaces/"));
        assert!(captured.contains("/teams/expansion/projection HTTP/1.1"));
        assert!(!captured.contains("slotPlan"));

        let mut substituted = projection(&original);
        let TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
            source_membership_revision,
            ..
        } = &mut substituted.slot_plan[0].target
        else {
            return Err(io::Error::other("fixture must cover a source Home transfer").into());
        };
        *source_membership_revision = 0;
        let malformed = TeamExpansionDecisionV3ProjectionResponse {
            request: request.clone(),
            result: Some(substituted),
        };
        let (bad_client, bad_server) = fixture(serde_json::to_value(malformed)?).await?;
        assert!(matches!(
            bad_client
                .team_expansion_decision_v3_projection(
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
