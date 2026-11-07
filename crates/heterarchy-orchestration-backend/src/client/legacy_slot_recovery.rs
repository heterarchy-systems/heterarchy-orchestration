//! Typed client for one exact pre-hold Team expansion slot recovery.

use hyper::Method;
use hyper::header::HeaderValue;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::legacy_slot_recovery as wire;

impl OrchestrationHttpClient {
    /// Recover one exact legacy approval slot with a distinct recovery ID.
    /// The original decision ID and ordinal remain unchanged; uncertain
    /// responses carry the full request for explicit owner readback.
    ///
    /// # Errors
    /// Returns invalid request, typed rejection, unknown outcome, or malformed
    /// recovery receipt.
    pub async fn recover_legacy_team_expansion_slot(
        &self,
        authorization: &HeaderValue,
        request: &wire::LegacySlotRecoveryRequest,
    ) -> Result<wire::LegacySlotRecoveryResponse, ClientError> {
        if !wire::valid_legacy_slot_recovery_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::LegacyTeamExpansionSlotRecovery(Box::new(
            wire::LegacySlotRecoveryReadbackRequest {
                intent: request.intent,
                current_actor: request.actor.clone(),
            },
        ));
        let response: wire::LegacySlotRecoveryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v2/workspaces/{}/teams/expansion/legacy-slot-recovery",
                    request.actor.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !wire::valid_legacy_slot_recovery_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the exact recovery intent. This query never creates a hold or
    /// retries the reservation.
    ///
    /// # Errors
    /// Returns invalid request, authorization/service rejection, bounded
    /// transport failure, or a malformed recovery receipt.
    pub async fn legacy_team_expansion_slot_recovery_readback(
        &self,
        authorization: &HeaderValue,
        request: &wire::LegacySlotRecoveryReadbackRequest,
    ) -> Result<wire::LegacySlotRecoveryReadbackResponse, ClientError> {
        if !wire::valid_legacy_slot_recovery_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::LegacySlotRecoveryReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v2/workspaces/{}/teams/expansion/legacy-slot-recovery/readback",
                    request.current_actor.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !wire::valid_legacy_slot_recovery_readback_response(&response, request) {
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
        legacy_slot_recovery::{
            LegacySlotRecoveryIntentWire, LegacySlotRecoveryReadbackRequest,
            LegacySlotRecoveryReadbackResponse, LegacySlotRecoveryReceiptWire,
            LegacySlotRecoveryRequest,
        },
        team_expansion::{TeamExpansionActorReceiptWire, TeamExpansionActorWire},
    };

    const CREDENTIAL: &str = "Bearer legacy-slot-recovery-client-fixture-secret";

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

    fn intent() -> LegacySlotRecoveryIntentWire {
        LegacySlotRecoveryIntentWire {
            workspace_id: Uuid::from_u128(0xE301),
            recovery_request_id: Uuid::from_u128(0xE304),
            approval_id: Uuid::from_u128(0xE305),
            ordinal: 0,
            team_id: Uuid::from_u128(0xE306),
            expected_team_revision: 3,
            expected_capacity_revision: 2,
            target_profile_id: Uuid::from_u128(0xE307),
            target_participation_id: Uuid::from_u128(0xE308),
        }
    }

    fn request() -> LegacySlotRecoveryRequest {
        LegacySlotRecoveryRequest {
            actor: actor(Uuid::from_u128(0xE309)),
            intent: intent(),
            expected_leadership_revision: 5,
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
            participation_id: Uuid::from_u128(0xE30A),
            profile_id: Uuid::from_u128(0xE30B),
            participant_id: Uuid::from_u128(0xE30C),
            display_name: "orchestrator".to_owned(),
        }
    }

    fn receipt(request: &LegacySlotRecoveryRequest) -> LegacySlotRecoveryReceiptWire {
        LegacySlotRecoveryReceiptWire {
            intent: request.intent,
            approval_request_id: Uuid::from_u128(0xE30D),
            leadership_revision: request.expected_leadership_revision,
            actor: actor_receipt(&request.actor),
            recorded_at_ms: 100,
            replayed: false,
        }
    }

    #[tokio::test]
    async fn o2_replay_unknown_readback_retains_original_intent_and_current_reader()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = request();
        let retry = LegacySlotRecoveryRequest {
            actor: actor(Uuid::from_u128(0xE30E)),
            intent: original.intent,
            expected_leadership_revision: 6,
        };
        let (client, mutation_server) = fixture(serde_json::json!({})).await?;
        let readback = match client
            .recover_legacy_team_expansion_slot(&HeaderValue::from_static(CREDENTIAL), &retry)
            .await
        {
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::LegacyTeamExpansionSlotRecovery(readback),
            }) => *readback,
            _ => return Err("uncertain recovery must retain its exact readback target".into()),
        };
        assert_eq!(readback.intent, original.intent);
        assert_eq!(readback.current_actor, retry.actor);
        let captured = String::from_utf8(mutation_server.await??)?;
        assert!(captured.contains("/internal/v2/workspaces/"));
        assert!(captured.contains("/legacy-slot-recovery HTTP/1.1"));
        assert!(captured.contains(&retry.intent.recovery_request_id.to_string()));
        assert!(captured.contains(&original.intent.approval_id.to_string()));

        let mut source_receipt = receipt(&original);
        source_receipt.replayed = false;
        let response = LegacySlotRecoveryReadbackResponse {
            request: readback.clone(),
            result: Some(source_receipt.clone()),
        };
        let (readback_client, readback_server) = fixture(serde_json::to_value(response)?).await?;
        let recovered = readback_client
            .legacy_team_expansion_slot_recovery_readback(
                &HeaderValue::from_static(CREDENTIAL),
                &readback,
            )
            .await?;
        assert_eq!(recovered.result, Some(source_receipt));
        let _captured = readback_server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn recovery_readback_keeps_original_slot_and_uses_current_reader()
    -> Result<(), Box<dyn std::error::Error>> {
        let mutation = request();
        let readback = LegacySlotRecoveryReadbackRequest {
            intent: mutation.intent,
            current_actor: actor(Uuid::from_u128(0xE30E)),
        };
        let mut recovered = receipt(&mutation);
        recovered.replayed = true;
        let response = LegacySlotRecoveryReadbackResponse {
            request: readback.clone(),
            result: Some(recovered.clone()),
        };
        let (client, server) = fixture(serde_json::to_value(response)?).await?;
        let result = client
            .legacy_team_expansion_slot_recovery_readback(
                &HeaderValue::from_static(CREDENTIAL),
                &readback,
            )
            .await?;
        assert_eq!(result.result, Some(recovered.clone()));
        let _captured = server.await??;

        let mut changed = readback.clone();
        changed.intent.workspace_id = Uuid::from_u128(0xE30F);
        let stale = LegacySlotRecoveryReadbackResponse {
            request: readback,
            result: Some(recovered),
        };
        assert!(!wire::valid_legacy_slot_recovery_readback_response(
            &stale, &changed
        ));
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
