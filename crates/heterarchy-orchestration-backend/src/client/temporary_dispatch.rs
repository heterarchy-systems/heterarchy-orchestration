//! Bounded client calls for temporary-dispatch admission and exact readback.

use hyper::Method;
use hyper::header::HeaderValue;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::temporary_dispatch as wire;

impl OrchestrationHttpClient {
    /// Submit one exact new admission request. Uncertain replies return the
    /// original request as a readback target and are never retried here.
    ///
    /// # Errors
    /// Returns invalid input, bounded transport errors, a typed service
    /// rejection, an unknown mutation outcome, or response-correlation failure.
    pub async fn admit_temporary_dispatch(
        &self,
        authorization: &HeaderValue,
        request: &wire::TemporaryDispatchAdmissionRequest,
    ) -> Result<wire::TemporaryDispatchAdmissionResponse, ClientError> {
        if !wire::valid_admission_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::TemporaryDispatch(Box::new(request.clone()));
        let response: wire::TemporaryDispatchAdmissionResponse = self
            .call(
                authorization,
                Method::POST,
                admission_path(request),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_admission_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one exact original admission receipt as a separately specified
    /// current actor. An absent receipt is only a snapshot observation; it does
    /// not prove that an in-flight admission cannot later commit.
    ///
    /// # Errors
    /// Returns invalid input, bounded query transport, typed service rejection,
    /// or response-correlation failure.
    pub async fn temporary_dispatch_readback(
        &self,
        authorization: &HeaderValue,
        request: &wire::TemporaryDispatchReadbackRequest,
    ) -> Result<Option<wire::TemporaryDispatchReceiptWire>, ClientError> {
        if !wire::valid_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response: wire::TemporaryDispatchReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                readback_path(request),
                Some(request),
                None,
            )
            .await?;
        if !valid_readback_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response.receipt)
    }
}

fn admission_path(request: &wire::TemporaryDispatchAdmissionRequest) -> String {
    format!(
        "/internal/v1/workspaces/{}/teams/{}/temporary-dispatch/admit",
        request.scope.workspace_id, request.requesting_team_id
    )
}

fn readback_path(request: &wire::TemporaryDispatchReadbackRequest) -> String {
    format!(
        "/internal/v1/workspaces/{}/teams/{}/temporary-dispatch/readback",
        request.current_scope.workspace_id, request.intent.requesting_team_id
    )
}

fn valid_admission_response(
    response: &wire::TemporaryDispatchAdmissionResponse,
    request: &wire::TemporaryDispatchAdmissionRequest,
) -> bool {
    if response.request != *request {
        return false;
    }
    match &response.outcome {
        wire::TemporaryDispatchAdmissionOutcomeWire::Admitted { receipt } => {
            wire::receipt_matches_intent(receipt.as_ref(), request)
        }
        wire::TemporaryDispatchAdmissionOutcomeWire::QuotaRejected {
            request_id,
            workspace_id,
            requesting_team_id,
            retry_at_ms,
        } => {
            *request_id == request.request_id
                && *workspace_id == request.scope.workspace_id
                && *requesting_team_id == request.requesting_team_id
                && *retry_at_ms >= 0
        }
    }
}

fn valid_readback_response(
    response: &wire::TemporaryDispatchReadbackResponse,
    request: &wire::TemporaryDispatchReadbackRequest,
) -> bool {
    response.request == *request
        && response
            .receipt
            .as_ref()
            .is_none_or(|receipt| wire::receipt_matches_intent(receipt, &request.intent))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io, time::Duration};
    use tokio::io::AsyncReadExt;
    use uuid::Uuid;

    fn request() -> wire::TemporaryDispatchAdmissionRequest {
        wire::TemporaryDispatchAdmissionRequest {
            scope: crate::wire::WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            requesting_team_id: Uuid::from_u128(5),
            request_id: Uuid::from_u128(6),
            activity_id: Uuid::from_u128(7),
            session_id: Uuid::from_u128(8),
            expected_leadership_revision: 9,
            target_participant_id: Uuid::from_u128(10),
            requested_scope: "bounded work".to_owned(),
            requested_end_at_ms: 100,
        }
    }

    fn receipt(
        request: &wire::TemporaryDispatchAdmissionRequest,
    ) -> wire::TemporaryDispatchReceiptWire {
        wire::TemporaryDispatchReceiptWire {
            request_id: request.request_id,
            workspace_id: request.scope.workspace_id,
            requesting_team_id: request.requesting_team_id,
            leadership_revision: request.expected_leadership_revision,
            actor: wire::TemporaryDispatchActorReceiptWire {
                activity_id: request.activity_id,
                agent_id: Uuid::from_u128(11),
                session_id: request.session_id,
                participation_id: Uuid::from_u128(12),
                profile_id: Uuid::from_u128(13),
                participant_id: Uuid::from_u128(14),
                display_name: "Leader".to_owned(),
            },
            target_home_team_id: Uuid::from_u128(15),
            target_participant_id: request.target_participant_id,
            requested_scope: request.requested_scope.clone(),
            requested_end_at_ms: request.requested_end_at_ms,
            admitted_at_ms: 50,
            lock_until_ms: Some(3_600_050),
            replayed: false,
        }
    }

    #[test]
    fn admission_response_correlates_admitted_and_quota_rejected_results() {
        let request = request();
        let admitted = wire::TemporaryDispatchAdmissionResponse {
            request: request.clone(),
            outcome: wire::TemporaryDispatchAdmissionOutcomeWire::Admitted {
                receipt: Box::new(receipt(&request)),
            },
        };
        assert!(valid_admission_response(&admitted, &request));
        let encoded = serde_json::to_value(&admitted);
        assert!(encoded.is_ok());
        let Some(encoded) = encoded.ok() else {
            return;
        };
        assert_eq!(
            encoded["outcome"]["receipt"]["requestId"],
            serde_json::json!(request.request_id)
        );

        let mut malformed_lock = admitted.clone();
        if let wire::TemporaryDispatchAdmissionOutcomeWire::Admitted { receipt } =
            &mut malformed_lock.outcome
        {
            receipt.lock_until_ms = Some(3_600_051);
        }
        assert!(!valid_admission_response(&malformed_lock, &request));

        let quota_rejected = wire::TemporaryDispatchAdmissionResponse {
            request: request.clone(),
            outcome: wire::TemporaryDispatchAdmissionOutcomeWire::QuotaRejected {
                request_id: request.request_id,
                workspace_id: request.scope.workspace_id,
                requesting_team_id: request.requesting_team_id,
                retry_at_ms: 3_600_000,
            },
        };
        assert!(valid_admission_response(&quota_rejected, &request));

        let mut mismatched = admitted;
        mismatched.request.requested_scope.push('!');
        assert!(!valid_admission_response(&mismatched, &request));
    }

    #[test]
    fn rebind_readback_keeps_original_session_and_checks_receipt_correlation() {
        let mut intent = request();
        intent.scope.location_revision = 1;
        intent.scope.workspace_revision = 1;
        intent.scope.locator = "original-stale-scope".to_owned();
        let readback = wire::TemporaryDispatchReadbackRequest {
            intent: intent.clone(),
            current_scope: crate::wire::WorkspaceQueryScope {
                workspace_id: intent.scope.workspace_id,
                location_id: Uuid::from_u128(18),
                location_revision: 21,
                workspace_revision: 22,
                locator: "rebound-current-scope".to_owned(),
            },
            current_actor: wire::TemporaryDispatchCurrentActorWire {
                activity_id: Uuid::from_u128(16),
                session_id: Uuid::from_u128(17),
            },
        };
        assert!(wire::valid_readback_request(&readback));
        let response = wire::TemporaryDispatchReadbackResponse {
            request: readback.clone(),
            receipt: Some(receipt(&intent)),
        };
        assert!(valid_readback_response(&response, &readback));

        let mut substituted_original = response;
        if let Some(receipt) = substituted_original.receipt.as_mut() {
            receipt.actor.session_id = Uuid::from_u128(18);
        }
        assert!(!valid_readback_response(&substituted_original, &readback));

        let mut foreign_current_scope = readback.clone();
        foreign_current_scope.current_scope.workspace_id = Uuid::from_u128(19);
        assert!(!wire::valid_readback_request(&foreign_current_scope));
    }

    #[test]
    fn unknown_admission_outcome_retains_the_exact_original_readback_intent() {
        let intent = request();
        let target = ReadbackTarget::TemporaryDispatch(Box::new(intent.clone()));
        assert_eq!(target, ReadbackTarget::TemporaryDispatch(Box::new(intent)));
    }

    #[test]
    fn maximum_escaped_echo_and_receipt_fit_client_response_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut intent = request();
        intent.scope.locator = "\\\"".repeat(4_096 / 2);
        intent.requested_scope = "\u{1}".repeat(wire::MAX_TEMPORARY_DISPATCH_SCOPE_BYTES);
        assert!(wire::valid_admission_request(&intent));

        let admission = wire::TemporaryDispatchAdmissionResponse {
            request: intent.clone(),
            outcome: wire::TemporaryDispatchAdmissionOutcomeWire::Admitted {
                receipt: Box::new(receipt(&intent)),
            },
        };
        assert!(valid_admission_response(&admission, &intent));
        let encoded_admission = serde_json::to_vec(&admission)?;
        assert!(encoded_admission.len() <= super::super::MAX_RESPONSE_BYTES);

        let readback_request = wire::TemporaryDispatchReadbackRequest {
            intent: intent.clone(),
            current_scope: crate::wire::WorkspaceQueryScope {
                workspace_id: intent.scope.workspace_id,
                location_id: Uuid::from_u128(18),
                location_revision: 19,
                workspace_revision: 20,
                locator: "\\\"".repeat(4_096 / 2),
            },
            current_actor: wire::TemporaryDispatchCurrentActorWire {
                activity_id: Uuid::from_u128(16),
                session_id: Uuid::from_u128(17),
            },
        };
        let readback = wire::TemporaryDispatchReadbackResponse {
            request: readback_request.clone(),
            receipt: Some(receipt(&intent)),
        };
        assert!(valid_readback_response(&readback, &readback_request));
        let encoded_readback = serde_json::to_vec(&readback)?;
        assert!(encoded_readback.len() <= super::super::MAX_RESPONSE_BYTES);
        Ok(())
    }

    #[tokio::test]
    async fn lost_reply_returns_exact_readback_target_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(1))?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let captured = read_request(&mut socket).await?;
            drop(socket);
            match tokio::time::timeout(Duration::from_millis(50), listener.accept()).await {
                Err(_) => Ok(captured),
                Ok(Ok(_)) => Err(io::Error::other("client retried an uncertain mutation")),
                Ok(Err(error)) => Err(error),
            }
        });
        let request = request();
        let result = client
            .admit_temporary_dispatch(
                &HeaderValue::from_static("Bearer temporary-dispatch-fixture-secret"),
                &request,
            )
            .await;
        assert_eq!(
            result.err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TemporaryDispatch(Box::new(request.clone()))
            })
        );
        let captured = server.await??;
        let captured = String::from_utf8(captured)?;
        assert!(captured.contains("POST /internal/v1/workspaces/"));
        assert!(captured.contains("temporary-dispatch/admit HTTP/1.1"));
        assert!(captured.contains(&request.request_id.to_string()));
        Ok(())
    }

    async fn read_request(socket: &mut tokio::net::TcpStream) -> Result<Vec<u8>, io::Error> {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                return Err(io::Error::other("client closed before request completed"));
            }
            request.extend_from_slice(&buffer[..count]);
            if request.len() > super::super::MAX_REQUEST_BYTES + super::super::MAX_HEADER_BYTES {
                return Err(io::Error::other("fixture request exceeded bound"));
            }
            if let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .ok_or_else(|| io::Error::other("fixture content length missing"))?;
                if request.len() >= header_end + 4 + content_length {
                    return Ok(request);
                }
            }
        }
    }
}
