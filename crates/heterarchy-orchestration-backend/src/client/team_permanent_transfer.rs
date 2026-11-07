//! Typed client for permanent Team transfer intent and consent statements.

mod renewal;
mod source;

use hyper::Method;
use hyper::header::HeaderValue;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::team_permanent_transfer as wire;

impl OrchestrationHttpClient {
    /// Commit Operations target identity capture and its admission barrier.
    /// An uncertain result retains this complete request for readback; no retry
    /// is triggered automatically.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, unknown outcome with the exact
    /// preparation intent, or malformed response.
    pub async fn prepare_team_permanent_transfer(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferPreparationRequest,
    ) -> Result<wire::TeamPermanentTransferPreparationResponse, ClientError> {
        if !wire::valid_preparation_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::TeamPermanentTransferPreparation(Box::new(request.clone()));
        let response: wire::TeamPermanentTransferPreparationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/prepare",
                    request.actor.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_preparation_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Recover one exact PREPARING receipt with one independently authorized
    /// read. Missing or mismatching evidence is not permission to resend.
    ///
    /// # Errors
    /// Returns invalid request, authorized readback failure, bounded transport
    /// failure or a malformed readback.
    pub async fn recover_team_permanent_transfer_preparation(
        &self,
        authorization: &HeaderValue,
        original: &wire::TeamPermanentTransferPreparationRequest,
        current_actor: &crate::wire::team_expansion::TeamExpansionActorWire,
    ) -> Result<Option<wire::TeamPermanentTransferPreparationReceiptWire>, ClientError> {
        let request = wire::TeamPermanentTransferPreparationReadbackRequest {
            workspace_id: original.actor.scope.workspace_id,
            preparation_id: original.command.preparation_id,
            current_actor: current_actor.clone(),
        };
        if !wire::valid_preparation_request(original)
            || !wire::valid_preparation_readback_request(&request)
        {
            return Err(ClientError::InvalidRequest);
        }
        let Some(receipt) = self
            .read_team_permanent_transfer_preparation(authorization, &request)
            .await?
            .result
        else {
            return Ok(None);
        };
        Ok(wire::valid_exact_preparation_recovery(original, &receipt).then_some(receipt))
    }

    /// Read one authorized original preparation record.
    ///
    /// # Errors
    /// Returns malformed selectors, transport failures, or mismatched replies.
    pub async fn read_team_permanent_transfer_preparation(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferPreparationReadbackRequest,
    ) -> Result<wire::TeamPermanentTransferPreparationReadbackResponse, ClientError> {
        if !wire::valid_preparation_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamPermanentTransferPreparationReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/preparation/readback",
                    request.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_preparation_readback_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Submit one exact Orchestrator-authored transfer proposal. An uncertain
    /// outcome retains the entire request and never retries automatically.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, unknown outcome with the exact
    /// proposal, or malformed response.
    pub async fn propose_team_permanent_transfer(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferIntentRequest,
    ) -> Result<wire::TeamPermanentTransferIntentResponse, ClientError> {
        if !wire::valid_intent_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::TeamPermanentTransferIntent(Box::new(request.clone()));
        let response: wire::TeamPermanentTransferIntentResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/propose",
                    request.actor.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_intent_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Record one exact current source Home Leader consent or target Profile
    /// acceptance. This is a point-in-time statement, not transfer cutover.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, unknown outcome retaining the
    /// exact consent request, or malformed response.
    pub async fn record_team_permanent_transfer_consent(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferConsentRequest,
    ) -> Result<wire::TeamPermanentTransferConsentResponse, ClientError> {
        if !wire::valid_consent_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::TeamPermanentTransferConsent(Box::new(request.clone()));
        let response: wire::TeamPermanentTransferConsentResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/consent",
                    request.actor.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_consent_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Query one authorized transfer proposal and its two point-in-time
    /// consents. The reader is independent of the original proposer.
    ///
    /// # Errors
    /// Returns invalid request, typed authorization/service rejection, bounded
    /// transport failure, or malformed exact-ID readback.
    pub async fn read_team_permanent_transfer_snapshot(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferReadbackRequest,
    ) -> Result<wire::TeamPermanentTransferReadbackResponse, ClientError> {
        if !wire::valid_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamPermanentTransferReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/readback",
                    request.workspace_id
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

    /// Read one exact transfer consent by its original transfer/receipt IDs.
    /// The authenticated reader may be a rebound original consentor or current
    /// Workspace Orchestrator; only that receipt is returned.
    ///
    /// # Errors
    /// Returns invalid request, typed authorization/service rejection, bounded
    /// transport failure, or malformed exact-consent readback.
    pub async fn read_team_permanent_transfer_consent(
        &self,
        authorization: &HeaderValue,
        request: &wire::TeamPermanentTransferConsentReadbackRequest,
    ) -> Result<wire::TeamPermanentTransferConsentReadbackResponse, ClientError> {
        if !wire::valid_consent_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TeamPermanentTransferConsentReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/permanent-transfers/consent/readback",
                    request.intent.actor.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_consent_readback_response(&response, request) {
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
        team_expansion::TeamExpansionActorWire,
        team_permanent_transfer::{
            TeamPermanentTransferBindingOriginWire, TeamPermanentTransferBindingWire,
            TeamPermanentTransferCapturedPrincipalWire, TeamPermanentTransferConsentActionWire,
            TeamPermanentTransferConsentKindWire, TeamPermanentTransferConsentReadbackRequest,
            TeamPermanentTransferConsentReadbackResponse, TeamPermanentTransferConsentReceiptWire,
            TeamPermanentTransferConsentRequest, TeamPermanentTransferIntentActionWire,
            TeamPermanentTransferIntentReceiptWire, TeamPermanentTransferIntentRequest,
            TeamPermanentTransferPreparationActionWire,
            TeamPermanentTransferPreparationReadbackRequest,
            TeamPermanentTransferPreparationReadbackResponse,
            TeamPermanentTransferPreparationReceiptWire, TeamPermanentTransferPreparationRequest,
            TeamPermanentTransferPreparationResponse, TeamPermanentTransferPreparationStateWire,
            TeamPermanentTransferReadbackRequest, TeamPermanentTransferReadbackResponse,
            TeamPermanentTransferSnapshotWire,
        },
    };

    const CREDENTIAL: &str = "Bearer transfer-client-fixture-secret";

    fn actor(session_id: Uuid) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(0xE401),
                location_id: Uuid::from_u128(0xE402),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(0xE403),
            session_id,
        }
    }

    fn actor_receipt(
        actor: &TeamExpansionActorWire,
        participation_id: Uuid,
        profile_id: Uuid,
    ) -> crate::wire::team_expansion::TeamExpansionActorReceiptWire {
        crate::wire::team_expansion::TeamExpansionActorReceiptWire {
            activity_id: actor.activity_id,
            agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
                actor.scope.workspace_id,
                actor.activity_id,
                actor.session_id,
            )
            .into_uuid(),
            session_id: actor.session_id,
            participation_id,
            profile_id,
            participant_id: Uuid::from_u128(0xE410),
            display_name: "transfer actor".to_owned(),
        }
    }

    fn intent_request() -> TeamPermanentTransferIntentRequest {
        TeamPermanentTransferIntentRequest {
            actor: actor(Uuid::from_u128(0xE404)),
            command: TeamPermanentTransferIntentActionWire {
                transfer_id: Uuid::from_u128(0xE405),
                source_team_id: Uuid::from_u128(0xE406),
                destination_team_id: Uuid::from_u128(0xE407),
                target_participation_id: Uuid::from_u128(0xE408),
                destination_request_id: Uuid::from_u128(0xE409),
                destination_decision_id: Uuid::from_u128(0xE40A),
                destination_ordinal: 0,
                expected_destination_team_revision: 2,
                expected_destination_capacity_revision: 3,
                expected_workspace_leadership_revision: 4,
                expected_source_membership_revision: 5,
                expected_source_leadership_revision: 6,
            },
        }
    }

    fn preparation_request() -> TeamPermanentTransferPreparationRequest {
        TeamPermanentTransferPreparationRequest {
            actor: actor(Uuid::from_u128(0xE404)),
            command: TeamPermanentTransferPreparationActionWire {
                preparation_id: Uuid::from_u128(0xE430),
                transfer_id: Uuid::from_u128(0xE405),
                expected_generation: 2,
                expected_workspace_leadership_revision: 4,
                source_leader_consent_id: Uuid::from_u128(0xE431),
                target_acceptance_id: Uuid::from_u128(0xE432),
            },
        }
    }

    fn preparation_receipt(
        request: &TeamPermanentTransferPreparationRequest,
    ) -> TeamPermanentTransferPreparationReceiptWire {
        TeamPermanentTransferPreparationReceiptWire {
            preparation_id: request.command.preparation_id,
            workspace_id: request.actor.scope.workspace_id,
            transfer_id: request.command.transfer_id,
            source_team_id: Uuid::from_u128(0xE406),
            destination_team_id: Uuid::from_u128(0xE407),
            generation: request.command.expected_generation,
            workspace_leadership_revision: request.command.expected_workspace_leadership_revision,
            source_leader_consent_id: request.command.source_leader_consent_id,
            target_acceptance_id: request.command.target_acceptance_id,
            target_participation_id: Uuid::from_u128(0xE408),
            target_profile_id: Uuid::from_u128(0xE411),
            target_participant_ids: vec![Uuid::from_u128(0xE415)],
            identity_version: 1,
            participation_revision: 2,
            source_membership_revision: 3,
            intent_digest: "ab".repeat(32),
            payload_digest: "cd".repeat(32),
            sessions: vec![Uuid::from_u128(0xE420), Uuid::from_u128(0xE421)],
            bindings: vec![TeamPermanentTransferBindingWire {
                participant_id: Uuid::from_u128(0xE415),
                activity_id: Uuid::from_u128(0xE416),
                agent_id: Uuid::from_u128(0xE417),
                session_id: Uuid::from_u128(0xE420),
                source_request_id: Uuid::from_u128(0xE418),
                origin: TeamPermanentTransferBindingOriginWire::Registration,
                principal: TeamPermanentTransferCapturedPrincipalWire::DirectToken {
                    token_id: Uuid::from_u128(0xE419),
                },
            }],
            actor: actor_receipt(
                &request.actor,
                Uuid::from_u128(0xE422),
                Uuid::from_u128(0xE423),
            ),
            recorded_at_ms: 100,
            session_count: 2,
            binding_count: 1,
            state: TeamPermanentTransferPreparationStateWire::Preparing,
            replayed: false,
        }
    }

    fn intent_receipt(
        request: &TeamPermanentTransferIntentRequest,
    ) -> TeamPermanentTransferIntentReceiptWire {
        TeamPermanentTransferIntentReceiptWire {
            transfer_id: request.command.transfer_id,
            workspace_id: request.actor.scope.workspace_id,
            source_team_id: request.command.source_team_id,
            destination_team_id: request.command.destination_team_id,
            target_participation_id: request.command.target_participation_id,
            target_profile_id: Uuid::from_u128(0xE40B),
            source_membership_revision: request.command.expected_source_membership_revision,
            source_leadership_revision: request.command.expected_source_leadership_revision,
            source_leader_participation_id: Uuid::from_u128(0xE40C),
            source_leader_profile_id: Uuid::from_u128(0xE40D),
            destination_team_revision: request.command.expected_destination_team_revision,
            destination_capacity_revision: request.command.expected_destination_capacity_revision,
            destination_workspace_leadership_revision: request
                .command
                .expected_workspace_leadership_revision,
            destination_request_id: request.command.destination_request_id,
            destination_decision_id: request.command.destination_decision_id,
            destination_ordinal: request.command.destination_ordinal,
            workspace_actor: actor_receipt(
                &request.actor,
                Uuid::from_u128(0xE40E),
                Uuid::from_u128(0xE40F),
            ),
            recorded_at_ms: 100,
            replayed: false,
        }
    }

    fn consent_request() -> TeamPermanentTransferConsentRequest {
        TeamPermanentTransferConsentRequest {
            actor: actor(Uuid::from_u128(0xE411)),
            command: TeamPermanentTransferConsentActionWire {
                receipt_id: Uuid::from_u128(0xE412),
                transfer_id: Uuid::from_u128(0xE405),
                kind: TeamPermanentTransferConsentKindWire::TargetAgentAcceptance,
            },
        }
    }

    fn consent_receipt(
        request: &TeamPermanentTransferConsentRequest,
    ) -> TeamPermanentTransferConsentReceiptWire {
        TeamPermanentTransferConsentReceiptWire {
            receipt_id: request.command.receipt_id,
            transfer_id: request.command.transfer_id,
            kind: request.command.kind,
            authority_revision: 5,
            membership_revision: 5,
            actor: actor_receipt(
                &request.actor,
                Uuid::from_u128(0xE408),
                Uuid::from_u128(0xE40B),
            ),
            recorded_at_ms: 101,
            replayed: false,
        }
    }

    fn snapshot(request: &TeamPermanentTransferIntentRequest) -> TeamPermanentTransferSnapshotWire {
        let intent = intent_receipt(request);
        let source_actor = TeamExpansionActorWire {
            scope: request.actor.scope.clone(),
            activity_id: Uuid::from_u128(0xE413),
            session_id: Uuid::from_u128(0xE414),
        };
        let target_actor = actor(Uuid::from_u128(0xE415));
        TeamPermanentTransferSnapshotWire {
            source_home_leader_consent: Some(TeamPermanentTransferConsentReceiptWire {
                receipt_id: Uuid::from_u128(0xE416),
                transfer_id: intent.transfer_id,
                kind: TeamPermanentTransferConsentKindWire::SourceHomeLeader,
                authority_revision: intent.source_leadership_revision,
                membership_revision: intent.source_membership_revision,
                actor: actor_receipt(
                    &source_actor,
                    intent.source_leader_participation_id,
                    intent.source_leader_profile_id,
                ),
                recorded_at_ms: 101,
                replayed: false,
            }),
            target_agent_acceptance: Some(TeamPermanentTransferConsentReceiptWire {
                receipt_id: Uuid::from_u128(0xE417),
                transfer_id: intent.transfer_id,
                kind: TeamPermanentTransferConsentKindWire::TargetAgentAcceptance,
                authority_revision: 5,
                membership_revision: intent.source_membership_revision,
                actor: actor_receipt(
                    &target_actor,
                    intent.target_participation_id,
                    intent.target_profile_id,
                ),
                recorded_at_ms: 102,
                replayed: false,
            }),
            intent,
        }
    }

    #[tokio::test]
    async fn uncertain_proposal_and_consent_keep_exact_requests()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = intent_request();
        let (client, server) = fixture(serde_json::json!({})).await?;
        assert!(matches!(
            client
                .propose_team_permanent_transfer(
                    &HeaderValue::from_static(CREDENTIAL),
                    &request,
                )
                .await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamPermanentTransferIntent(original)
            }) if *original == request
        ));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/permanent-transfers/propose HTTP/1.1"));

        let request = consent_request();
        let (client, server) = fixture(serde_json::json!({})).await?;
        assert!(matches!(
            client
                .record_team_permanent_transfer_consent(
                    &HeaderValue::from_static(CREDENTIAL),
                    &request,
                )
                .await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamPermanentTransferConsent(original)
            }) if *original == request
        ));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/permanent-transfers/consent HTTP/1.1"));
        Ok(())
    }

    #[tokio::test]
    async fn current_orchestrator_reads_exact_transfer_snapshot_by_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = intent_request();
        let request = TeamPermanentTransferReadbackRequest {
            workspace_id: original.actor.scope.workspace_id,
            transfer_id: original.command.transfer_id,
            current_actor: actor(Uuid::from_u128(0xE418)),
        };
        let response = TeamPermanentTransferReadbackResponse {
            request: request.clone(),
            result: Some(snapshot(&original)),
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let recovered = client
            .read_team_permanent_transfer_snapshot(&HeaderValue::from_static(CREDENTIAL), &request)
            .await?;
        assert_eq!(recovered.request, request);
        assert_eq!(recovered.result, Some(snapshot(&original)));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/permanent-transfers/readback HTTP/1.1"));
        assert!(!captured.contains("destinationDecisionId"));
        Ok(())
    }

    #[tokio::test]
    async fn rebound_reader_reads_only_exact_consent_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        let intent = consent_request();
        let request = TeamPermanentTransferConsentReadbackRequest {
            intent: intent.clone(),
            current_actor: actor(Uuid::from_u128(0xE419)),
        };
        let response = TeamPermanentTransferConsentReadbackResponse {
            request: request.clone(),
            result: Some(consent_receipt(&intent)),
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let recovered = client
            .read_team_permanent_transfer_consent(&HeaderValue::from_static(CREDENTIAL), &request)
            .await?;
        assert_eq!(recovered.request, request);
        assert_eq!(recovered.result, Some(consent_receipt(&intent)));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/permanent-transfers/consent/readback HTTP/1.1"));
        Ok(())
    }

    #[tokio::test]
    async fn uncertain_preparation_retains_exact_consent_and_generation_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = preparation_request();
        let mut forged = preparation_receipt(&original);
        forged.source_leader_consent_id = Uuid::from_u128(0xE499);
        let response = TeamPermanentTransferPreparationResponse {
            request: original.clone(),
            receipt: forged,
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let result = client
            .prepare_team_permanent_transfer(&HeaderValue::from_static(CREDENTIAL), &original)
            .await;
        let Err(error) = result else {
            return Err(io::Error::other("correlation mismatch was accepted").into());
        };
        assert_eq!(
            error,
            ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamPermanentTransferPreparation(Box::new(
                    original.clone()
                ))
            }
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/permanent-transfers/prepare HTTP/1.1"));
        for id in [
            original.command.preparation_id,
            original.command.transfer_id,
            original.command.source_leader_consent_id,
            original.command.target_acceptance_id,
        ] {
            assert!(captured.contains(&id.to_string()));
        }
        Ok(())
    }

    #[tokio::test]
    async fn recovery_reads_once_under_current_actor_and_matches_original_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = preparation_request();
        let current_reader = actor(Uuid::from_u128(0xE450));
        let readback = TeamPermanentTransferPreparationReadbackRequest {
            workspace_id: original.actor.scope.workspace_id,
            preparation_id: original.command.preparation_id,
            current_actor: current_reader.clone(),
        };
        let response = TeamPermanentTransferPreparationReadbackResponse {
            request: readback,
            result: Some(preparation_receipt(&original)),
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let recovered = client
            .recover_team_permanent_transfer_preparation(
                &HeaderValue::from_static(CREDENTIAL),
                &original,
                &current_reader,
            )
            .await?;
        assert_eq!(recovered, Some(preparation_receipt(&original)));
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/permanent-transfers/preparation/readback HTTP/1.1"));
        let (headers, body) = captured
            .split_once("\r\n\r\n")
            .ok_or_else(|| io::Error::other("readback body separator missing"))?;
        assert!(
            headers
                .starts_with("POST /internal/v1/workspaces/00000000-0000-0000-0000-00000000e401/")
        );
        let readback: TeamPermanentTransferPreparationReadbackRequest = serde_json::from_str(body)?;
        assert_eq!(readback.preparation_id, original.command.preparation_id);
        assert_eq!(readback.current_actor, current_reader);
        assert!(!body.contains("sourceLeaderConsentId"));
        assert!(!body.contains("targetAcceptanceId"));
        Ok(())
    }

    #[tokio::test]
    async fn recovery_never_treats_a_different_consent_pair_as_preparation_proof()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = preparation_request();
        let current_reader = actor(Uuid::from_u128(0xE450));
        let readback = TeamPermanentTransferPreparationReadbackRequest {
            workspace_id: original.actor.scope.workspace_id,
            preparation_id: original.command.preparation_id,
            current_actor: current_reader.clone(),
        };
        let mut wrong = preparation_receipt(&original);
        wrong.source_leader_consent_id = Uuid::from_u128(0xE499);
        let response = TeamPermanentTransferPreparationReadbackResponse {
            request: readback,
            result: Some(wrong),
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let recovered = client
            .recover_team_permanent_transfer_preparation(
                &HeaderValue::from_static(CREDENTIAL),
                &original,
                &current_reader,
            )
            .await?;
        assert!(recovered.is_none());
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/permanent-transfers/preparation/readback HTTP/1.1"));
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
                        .ok_or_else(|| {
                            io::Error::other("fixture request missing content length")
                        })?;
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
