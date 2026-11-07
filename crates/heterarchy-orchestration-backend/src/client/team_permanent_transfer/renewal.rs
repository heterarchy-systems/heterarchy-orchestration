//! Renewal submission and evidence-backed generation recovery.

use hyper::Method;
use hyper::header::HeaderValue;

use super::super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::team_permanent_transfer as wire;

impl OrchestrationHttpClient {
    /// Renew one exact stale-Leader proposal. An uncertain response retains
    /// the complete proposal, predecessor and expected generation.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, unknown outcome with the exact
    /// renewal request, or malformed response.
    pub async fn renew_team_permanent_transfer(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferRenewalRequest,
    ) -> Result<wire::TeamPermanentTransferRenewalResponse, ClientError> {
        if !wire::valid_renewal_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::TeamPermanentTransferRenewal(Box::new(request.clone()));
        let response: wire::TeamPermanentTransferRenewalResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/renew",
                    request.proposal.actor.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_renewal_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one historical generation by exact Workspace and transfer IDs.
    /// The current reader is independent of the immutable proposal actor.
    ///
    /// # Errors
    /// Returns invalid request, typed authorization/service rejection, bounded
    /// transport failure, or malformed/cross-correlated generation history.
    pub async fn read_team_permanent_transfer_generation(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferReadbackRequest,
    ) -> Result<wire::TeamPermanentTransferGenerationReadbackResponse, ClientError> {
        if !wire::valid_generation_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamPermanentTransferGenerationReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/generation/readback",
                    request.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_generation_readback_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Resolve an uncertain renewal from one authorized child-generation read
    /// that proves its exact retained proposal, predecessor and number.
    /// `current_actor` is the present readback authority; the proposal actor is
    /// checked independently against the immutable receipt.
    ///
    /// # Errors
    /// Returns invalid request, authorization/service rejection, bounded
    /// transport failure, or malformed generation readback. `None` means the
    /// available durable history does not prove this exact renewal.
    pub async fn recover_team_permanent_transfer_renewal(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferRenewalRequest,
        current_actor: &crate::wire::team_expansion::TeamExpansionActorWire,
    ) -> Result<Option<wire::TeamPermanentTransferGenerationSnapshotWire>, ClientError> {
        if !wire::valid_renewal_request(request)
            || current_actor.scope.workspace_id != request.proposal.actor.scope.workspace_id
        {
            return Err(ClientError::InvalidRequest);
        }

        let readback_request = wire::TeamPermanentTransferReadbackRequest {
            workspace_id: request.proposal.actor.scope.workspace_id,
            transfer_id: request.proposal.command.transfer_id,
            current_actor: current_actor.clone(),
        };
        let Some(renewed) = self
            .read_team_permanent_transfer_generation(authorization, &readback_request)
            .await?
            .result
        else {
            return Ok(None);
        };
        if !wire::valid_exact_renewal_recovery(request, &renewed) {
            return Ok(None);
        }
        Ok(Some(renewed))
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
        team_expansion::{TeamExpansionActorReceiptWire, TeamExpansionActorWire},
        team_permanent_transfer::{
            TeamPermanentTransferGenerationReadbackResponse,
            TeamPermanentTransferGenerationSnapshotWire, TeamPermanentTransferGenerationStateWire,
            TeamPermanentTransferGenerationWire, TeamPermanentTransferIntentActionWire,
            TeamPermanentTransferIntentReceiptWire, TeamPermanentTransferIntentRequest,
            TeamPermanentTransferRenewalRequest, TeamPermanentTransferRenewalResponse,
        },
    };

    const CREDENTIAL: &str = "Bearer transfer-renewal-fixture-secret";

    fn actor(activity: u128, session: u128) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(activity),
            session_id: Uuid::from_u128(session),
        }
    }

    fn request() -> TeamPermanentTransferRenewalRequest {
        TeamPermanentTransferRenewalRequest {
            proposal: TeamPermanentTransferIntentRequest {
                actor: actor(10, 11),
                command: TeamPermanentTransferIntentActionWire {
                    transfer_id: Uuid::from_u128(20),
                    source_team_id: Uuid::from_u128(30),
                    destination_team_id: Uuid::from_u128(31),
                    target_participation_id: Uuid::from_u128(32),
                    destination_request_id: Uuid::from_u128(33),
                    destination_decision_id: Uuid::from_u128(34),
                    destination_ordinal: 1,
                    expected_destination_team_revision: 2,
                    expected_destination_capacity_revision: 3,
                    expected_workspace_leadership_revision: 4,
                    expected_source_membership_revision: 5,
                    expected_source_leadership_revision: 7,
                },
            },
            predecessor_transfer_id: Uuid::from_u128(21),
            expected_generation: 3,
        }
    }

    fn intent_receipt(
        request: &TeamPermanentTransferRenewalRequest,
        transfer_id: Uuid,
        leader_participation: u128,
        leader_profile: u128,
        leadership_revision: i64,
        recorded_at_ms: i64,
    ) -> TeamPermanentTransferIntentReceiptWire {
        let actor = &request.proposal.actor;
        let command = &request.proposal.command;
        TeamPermanentTransferIntentReceiptWire {
            transfer_id,
            workspace_id: actor.scope.workspace_id,
            source_team_id: command.source_team_id,
            destination_team_id: command.destination_team_id,
            target_participation_id: command.target_participation_id,
            target_profile_id: Uuid::from_u128(40),
            source_membership_revision: command.expected_source_membership_revision,
            source_leadership_revision: leadership_revision,
            source_leader_participation_id: Uuid::from_u128(leader_participation),
            source_leader_profile_id: Uuid::from_u128(leader_profile),
            destination_team_revision: command.expected_destination_team_revision,
            destination_capacity_revision: command.expected_destination_capacity_revision,
            destination_workspace_leadership_revision: command
                .expected_workspace_leadership_revision,
            destination_request_id: command.destination_request_id,
            destination_decision_id: command.destination_decision_id,
            destination_ordinal: command.destination_ordinal,
            workspace_actor: TeamExpansionActorReceiptWire {
                activity_id: actor.activity_id,
                agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
                    actor.scope.workspace_id,
                    actor.activity_id,
                    actor.session_id,
                )
                .into_uuid(),
                session_id: actor.session_id,
                participation_id: Uuid::from_u128(41),
                profile_id: Uuid::from_u128(42),
                participant_id: Uuid::from_u128(43),
                display_name: "original Orchestrator".to_owned(),
            },
            recorded_at_ms,
            replayed: false,
        }
    }

    fn snapshot(
        intent: TeamPermanentTransferIntentReceiptWire,
        number: u64,
        predecessor_transfer_id: Option<Uuid>,
        state: TeamPermanentTransferGenerationStateWire,
        head_transfer_id: Uuid,
        head_generation: u64,
    ) -> TeamPermanentTransferGenerationSnapshotWire {
        TeamPermanentTransferGenerationSnapshotWire {
            generation: TeamPermanentTransferGenerationWire {
                number,
                predecessor_transfer_id,
                state,
                recorded_at_ms: intent.recorded_at_ms,
                head_transfer_id,
                head_generation,
            },
            intent,
        }
    }

    fn superseded_response(
        request: &TeamPermanentTransferRenewalRequest,
        replayed: bool,
    ) -> TeamPermanentTransferRenewalResponse {
        let generation_number = request.expected_generation + 1;
        let mut intent = intent_receipt(
            request,
            request.proposal.command.transfer_id,
            45,
            46,
            request.proposal.command.expected_source_leadership_revision,
            101,
        );
        intent.replayed = replayed;
        TeamPermanentTransferRenewalResponse {
            request: request.clone(),
            receipt: snapshot(
                intent,
                generation_number,
                Some(request.predecessor_transfer_id),
                TeamPermanentTransferGenerationStateWire::Superseded,
                Uuid::from_u128(98),
                generation_number + 1,
            ),
        }
    }

    fn generation_readback_response(
        request: &TeamPermanentTransferRenewalRequest,
        reader: TeamExpansionActorWire,
        state: Option<TeamPermanentTransferGenerationStateWire>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let generation_number = request.expected_generation + 1;
        let result = state.map(|state| {
            let (head_transfer_id, head_generation) = match state {
                TeamPermanentTransferGenerationStateWire::IntentOnly
                | TeamPermanentTransferGenerationStateWire::Preparing => {
                    (request.proposal.command.transfer_id, generation_number)
                }
                TeamPermanentTransferGenerationStateWire::Superseded => {
                    (Uuid::from_u128(98), generation_number + 1)
                }
            };
            snapshot(
                intent_receipt(
                    request,
                    request.proposal.command.transfer_id,
                    45,
                    46,
                    7,
                    101,
                ),
                generation_number,
                Some(request.predecessor_transfer_id),
                state,
                head_transfer_id,
                head_generation,
            )
        });
        Ok(serde_json::to_value(
            TeamPermanentTransferGenerationReadbackResponse {
                request:
                    crate::wire::team_permanent_transfer::TeamPermanentTransferReadbackRequest {
                        workspace_id: request.proposal.actor.scope.workspace_id,
                        transfer_id: request.proposal.command.transfer_id,
                        current_actor: reader,
                    },
                result,
            },
        )?)
    }

    #[tokio::test]
    async fn unknown_renewal_outcome_retains_exact_request_and_never_retries()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let (client, server) = fixture(vec![serde_json::json!({})]).await?;
        assert!(matches!(
            client
                .renew_team_permanent_transfer(&HeaderValue::from_static(CREDENTIAL), &request)
                .await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamPermanentTransferRenewal(original)
            }) if *original == request
        ));
        let captured = String::from_utf8(server.await??)?;
        assert_eq!(captured.matches("POST /internal/v1/").count(), 1);
        assert!(captured.contains("/teams/permanent-transfers/renew HTTP/1.1"));
        Ok(())
    }

    #[tokio::test]
    async fn superseded_history_is_accepted_only_for_exact_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let replay = superseded_response(&request, true);
        assert!(wire::valid_renewal_response(&replay, &request));
        let (client, server) = fixture(vec![serde_json::to_value(&replay)?]).await?;
        let response = client
            .renew_team_permanent_transfer(&HeaderValue::from_static(CREDENTIAL), &request)
            .await?;
        assert_eq!(
            response.receipt.generation.state,
            TeamPermanentTransferGenerationStateWire::Superseded
        );
        assert_eq!(
            response.receipt.generation.head_transfer_id,
            Uuid::from_u128(98)
        );
        assert_eq!(response.receipt.generation.head_generation, 5);
        let captured = String::from_utf8(server.await??)?;
        assert_eq!(captured.matches("POST /internal/v1/").count(), 1);

        let fresh_superseded = superseded_response(&request, false);
        assert!(!wire::valid_renewal_response(&fresh_superseded, &request));
        let (client, server) = fixture(vec![serde_json::to_value(fresh_superseded)?]).await?;
        assert!(matches!(
            client
                .renew_team_permanent_transfer(&HeaderValue::from_static(CREDENTIAL), &request)
                .await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamPermanentTransferRenewal(original)
            }) if *original == request
        ));
        let captured = String::from_utf8(server.await??)?;
        assert_eq!(captured.matches("POST /internal/v1/").count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn recovery_uses_one_child_generation_read_and_keeps_proposer_separate_from_reader()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let current_reader = actor(50, 51);
        assert_ne!(request.proposal.actor.session_id, current_reader.session_id);

        let active = generation_readback_response(
            &request,
            current_reader.clone(),
            Some(TeamPermanentTransferGenerationStateWire::IntentOnly),
        )?;
        let (client, server) = fixture(vec![active]).await?;
        let recovered = client
            .recover_team_permanent_transfer_renewal(
                &HeaderValue::from_static(CREDENTIAL),
                &request,
                &current_reader,
            )
            .await?;
        assert_eq!(
            recovered
                .as_ref()
                .map(|snapshot| snapshot.intent.transfer_id),
            Some(request.proposal.command.transfer_id)
        );
        let captured = String::from_utf8(server.await??)?;
        assert_eq!(captured.matches("POST /internal/v1/").count(), 1);
        assert!(captured.contains("/teams/permanent-transfers/generation/readback HTTP/1.1"));
        assert!(!captured.contains("renew HTTP/1.1"));

        let superseded = generation_readback_response(
            &request,
            current_reader.clone(),
            Some(TeamPermanentTransferGenerationStateWire::Superseded),
        )?;
        let (client, server) = fixture(vec![superseded]).await?;
        let recovered = client
            .recover_team_permanent_transfer_renewal(
                &HeaderValue::from_static(CREDENTIAL),
                &request,
                &current_reader,
            )
            .await?
            .ok_or("superseded generation still proves the original commit")?;
        assert_eq!(
            recovered.generation.state,
            TeamPermanentTransferGenerationStateWire::Superseded
        );
        assert_eq!(recovered.generation.head_transfer_id, Uuid::from_u128(98));
        assert_eq!(recovered.generation.head_generation, 5);
        let captured = String::from_utf8(server.await??)?;
        assert_eq!(captured.matches("POST /internal/v1/").count(), 1);

        let absent = generation_readback_response(&request, current_reader.clone(), None)?;
        let (client, server) = fixture(vec![absent]).await?;
        assert!(
            client
                .recover_team_permanent_transfer_renewal(
                    &HeaderValue::from_static(CREDENTIAL),
                    &request,
                    &current_reader,
                )
                .await?
                .is_none()
        );
        let captured = String::from_utf8(server.await??)?;
        assert_eq!(captured.matches("POST /internal/v1/").count(), 1);
        Ok(())
    }

    async fn fixture(
        responses: Vec<serde_json::Value>,
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
            let mut captured = Vec::new();
            for response in responses {
                let (mut socket, _) = listener.accept().await?;
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let count = socket.read(&mut buffer).await?;
                    if count == 0 {
                        return Err(io::Error::other("client closed before request completed"));
                    }
                    request.extend_from_slice(&buffer[..count]);
                    if request.len()
                        > super::super::super::MAX_REQUEST_BYTES
                            + super::super::super::MAX_HEADER_BYTES
                    {
                        return Err(io::Error::other("fixture request exceeded bound"));
                    }
                    let Some(header_end) =
                        request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                    else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .ok_or_else(|| io::Error::other("request omitted content length"))?;
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
                captured.extend_from_slice(&request);
                captured.extend_from_slice(b"\n---\n");
                let body = serde_json::to_vec(&response)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(head.as_bytes()).await?;
                socket.write_all(&body).await?;
                socket.shutdown().await?;
            }
            Ok(captured)
        });
        Ok((client, server))
    }
}
