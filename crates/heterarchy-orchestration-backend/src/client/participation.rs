//! Bounded HTTP client for source-scoped logical presence and inbox operations.

use std::collections::HashSet;

use hyper::Method;
use hyper::header::HeaderValue;

use crate::wire::leadership::WorkspaceQueryScope;
use crate::wire::participation::{
    InboxAckRequest, InboxAckResponse, InboxEntryWire, InboxListRequest, InboxListResponse,
    InboxParticipantWire, InboxQueryRequest, InboxQueryResponse, ParticipantTarget,
    PresenceQueryRequest, PresenceQueryResponse, PresenceTouchRequest, PresenceTouchResponse,
    PresenceWire,
};

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
const MAX_INBOX_PAGE: u32 = 64;

impl OrchestrationHttpClient {
    /// Compare-and-set one exact participant's logical presence.
    ///
    /// An uncertain exchange carries the exact participant and expected
    /// revision for observation. The client never retries the mutation.
    ///
    /// # Errors
    /// Rejects invalid target/revision input, returns typed service rejection,
    /// bounded transport failure, or an unknown outcome with exact readback identity.
    pub async fn touch_presence(
        &self,
        authorization: &HeaderValue,
        request: &PresenceTouchRequest,
    ) -> Result<PresenceTouchResponse, ClientError> {
        validate_presence_touch_request(request)?;
        let readback = ReadbackTarget::Presence {
            workspace_id: request.target.scope.workspace_id,
            activity_id: request.target.activity_id,
            agent_id: request.target.agent_id,
            session_id: request.target.session_id,
            expected_revision: request.expected_presence_revision,
        };
        let response: PresenceTouchResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/presence/touch",
                    request.target.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_presence_touch_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the current logical presence for one exact participant target.
    ///
    /// Absence is an explicit observation. It does not imply that the
    /// participant never had a presence row.
    ///
    /// # Errors
    /// Rejects invalid target input, typed service rejection, bounded transport
    /// failure, or an inconsistent source projection.
    pub async fn presence(
        &self,
        authorization: &HeaderValue,
        request: &PresenceQueryRequest,
    ) -> Result<PresenceQueryResponse, ClientError> {
        validate_target(&request.target)?;
        let response: PresenceQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/presence/query",
                    request.target.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_presence_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read a bounded page of exact logical inbox entries for one participant.
    ///
    /// # Errors
    /// Rejects invalid target/page input, typed service rejection, bounded
    /// transport failure, or a malformed, duplicate, or foreign event entry.
    pub async fn list_inbox(
        &self,
        authorization: &HeaderValue,
        request: &InboxListRequest,
    ) -> Result<InboxListResponse, ClientError> {
        validate_inbox_list_request(request)?;
        let response: InboxListResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/inbox/list",
                    request.target.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_inbox_list_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Acknowledge one exact inbox receipt with compare-and-set semantics.
    ///
    /// An uncertain exchange carries the exact participant/event receipt and
    /// expected revision for observation. The client never retries the ACK.
    ///
    /// # Errors
    /// Rejects invalid target/event/revision input, returns typed service
    /// rejection, bounded transport failure, or an unknown outcome with exact
    /// readback identity.
    pub async fn acknowledge_inbox(
        &self,
        authorization: &HeaderValue,
        request: &InboxAckRequest,
    ) -> Result<InboxAckResponse, ClientError> {
        validate_inbox_ack_request(request)?;
        let readback = ReadbackTarget::InboxReceipt {
            workspace_id: request.target.scope.workspace_id,
            activity_id: request.target.activity_id,
            agent_id: request.target.agent_id,
            session_id: request.target.session_id,
            event_id: request.event_id,
            expected_revision: request.expected_receipt_revision,
        };
        let response: InboxAckResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/inbox/ack",
                    request.target.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_inbox_ack_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one exact event receipt for the selected participant.
    ///
    /// Absence is an explicit observation and has no mutation or retry meaning.
    ///
    /// # Errors
    /// Rejects invalid target/event input, typed service rejection, bounded
    /// transport failure, or an inconsistent exact receipt projection.
    pub async fn inbox_entry(
        &self,
        authorization: &HeaderValue,
        request: &InboxQueryRequest,
    ) -> Result<InboxQueryResponse, ClientError> {
        validate_inbox_query_request(request)?;
        let response: InboxQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/inbox/query",
                    request.target.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_inbox_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_presence_touch_request(request: &PresenceTouchRequest) -> Result<(), ClientError> {
    validate_target(&request.target)?;
    require_next_revision(request.expected_presence_revision)
}

fn validate_inbox_list_request(request: &InboxListRequest) -> Result<(), ClientError> {
    validate_target(&request.target)?;
    if !(1..=MAX_INBOX_PAGE).contains(&request.limit) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_inbox_ack_request(request: &InboxAckRequest) -> Result<(), ClientError> {
    validate_target(&request.target)?;
    require_identity(request.event_id)?;
    require_next_revision(request.expected_receipt_revision)
}

fn validate_inbox_query_request(request: &InboxQueryRequest) -> Result<(), ClientError> {
    validate_target(&request.target)?;
    require_identity(request.event_id)
}

fn validate_target(target: &ParticipantTarget) -> Result<(), ClientError> {
    validate_scope(&target.scope)?;
    require_identity(target.activity_id)?;
    require_identity(target.agent_id)?;
    require_identity(target.session_id)
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

fn require_next_revision(revision: i64) -> Result<(), ClientError> {
    if revision <= 0 || revision.checked_add(1).is_none() {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_presence_touch_response(
    response: &PresenceTouchResponse,
    request: &PresenceTouchRequest,
) -> bool {
    response.target == request.target
        && valid_presence(&response.presence, &request.target)
        && request
            .expected_presence_revision
            .checked_add(1)
            .is_some_and(|next| response.presence.presence_revision == next)
}

fn valid_presence_query_response(
    response: &PresenceQueryResponse,
    request: &PresenceQueryRequest,
) -> bool {
    response.target == request.target
        && response
            .presence
            .as_ref()
            .is_none_or(|presence| valid_presence(presence, &request.target))
}

fn valid_presence(presence: &PresenceWire, target: &ParticipantTarget) -> bool {
    presence.workspace_id == target.scope.workspace_id
        && !presence.participant_id.is_nil()
        && presence.agent_id == target.agent_id
        && presence.session_id == target.session_id
        && valid_display_name(&presence.display_name)
        && presence.membership_revision > 0
        && presence.presence_revision > 0
        && presence.last_authenticated_at_ms >= 0
}

fn valid_inbox_list_response(response: &InboxListResponse, request: &InboxListRequest) -> bool {
    let Ok(entry_count) = u32::try_from(response.entries.len()) else {
        return false;
    };
    if response.target != request.target
        || entry_count > request.limit
        || entry_count > MAX_INBOX_PAGE
    {
        return false;
    }

    let mut event_ids = HashSet::with_capacity(response.entries.len());
    response
        .entries
        .iter()
        .all(|entry| valid_inbox_entry(entry, &request.target) && event_ids.insert(entry.event_id))
}

fn valid_inbox_ack_response(response: &InboxAckResponse, request: &InboxAckRequest) -> bool {
    response.target == request.target
        && valid_inbox_entry(&response.entry, &request.target)
        && response.entry.event_id == request.event_id
        && request
            .expected_receipt_revision
            .checked_add(1)
            .is_some_and(|next| response.entry.receipt_revision == next)
        && response.entry.acknowledged_at_ms.is_some()
}

fn valid_inbox_query_response(response: &InboxQueryResponse, request: &InboxQueryRequest) -> bool {
    response.target == request.target
        && response.entry.as_ref().is_none_or(|entry| {
            valid_inbox_entry(entry, &request.target) && entry.event_id == request.event_id
        })
}

fn valid_inbox_entry(entry: &InboxEntryWire, target: &ParticipantTarget) -> bool {
    entry.workspace_id == target.scope.workspace_id
        && entry.activity_id == target.activity_id
        && !entry.event_id.is_nil()
        && valid_recipient(&entry.recipient, target)
        && valid_participant(&entry.actor)
        && entry.membership_revision > 0
        && entry.observed_at_ms >= 0
        && entry.receipt_revision > 0
        && entry.acknowledged_at_ms.is_none_or(|time| time >= 0)
}

fn valid_recipient(recipient: &InboxParticipantWire, target: &ParticipantTarget) -> bool {
    valid_participant(recipient)
        && recipient.agent_id == target.agent_id
        && recipient.session_id == target.session_id
}

fn valid_participant(participant: &InboxParticipantWire) -> bool {
    !participant.participant_id.is_nil()
        && !participant.agent_id.is_nil()
        && !participant.session_id.is_nil()
        && valid_display_name(&participant.display_name)
}

fn valid_display_name(display_name: &str) -> bool {
    !display_name.trim().is_empty() && display_name.len() <= MAX_DISPLAY_NAME_BYTES
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};
    use uuid::Uuid;

    use super::super::{MAX_HEADER_BYTES, MAX_REQUEST_BYTES};
    use super::*;
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn target() -> ParticipantTarget {
        ParticipantTarget {
            scope: scope(),
            activity_id: Uuid::from_u128(3),
            agent_id: Uuid::from_u128(4),
            session_id: Uuid::from_u128(5),
        }
    }

    fn participant(seed: u128, name: &str) -> InboxParticipantWire {
        InboxParticipantWire {
            participant_id: Uuid::from_u128(seed),
            agent_id: Uuid::from_u128(seed + 1),
            session_id: Uuid::from_u128(seed + 2),
            display_name: name.to_owned(),
        }
    }

    fn entry(target: &ParticipantTarget, event_id: Uuid) -> InboxEntryWire {
        InboxEntryWire {
            workspace_id: target.scope.workspace_id,
            activity_id: target.activity_id,
            event_id,
            recipient: InboxParticipantWire {
                participant_id: Uuid::from_u128(6),
                agent_id: target.agent_id,
                session_id: target.session_id,
                display_name: "recipient".to_owned(),
            },
            actor: participant(7, "actor"),
            membership_revision: 1,
            observed_at_ms: 10,
            receipt_revision: 1,
            acknowledged_at_ms: None,
        }
    }

    #[test]
    fn presence_projections_enforce_exact_identity_bounds_and_revision() {
        let target = target();
        let valid_presence = PresenceWire {
            workspace_id: target.scope.workspace_id,
            participant_id: Uuid::from_u128(6),
            agent_id: target.agent_id,
            session_id: target.session_id,
            display_name: "participant".to_owned(),
            membership_revision: 1,
            presence_revision: 2,
            last_authenticated_at_ms: 10,
        };
        let touch = PresenceTouchRequest {
            target: target.clone(),
            expected_presence_revision: 1,
        };
        assert!(valid_presence_touch_response(
            &PresenceTouchResponse {
                target: target.clone(),
                presence: valid_presence.clone(),
            },
            &touch,
        ));
        let query = PresenceQueryRequest {
            target: target.clone(),
        };
        assert!(valid_presence_query_response(
            &PresenceQueryResponse {
                target: target.clone(),
                presence: Some(valid_presence.clone()),
            },
            &query,
        ));
        let mut foreign_presence = valid_presence.clone();
        foreign_presence.workspace_id = Uuid::from_u128(99);
        assert!(!valid_presence_query_response(
            &PresenceQueryResponse {
                target: target.clone(),
                presence: Some(foreign_presence),
            },
            &query,
        ));
        let mut overlong_name = valid_presence.clone();
        overlong_name.display_name = "n".repeat(MAX_DISPLAY_NAME_BYTES + 1);
        assert!(!valid_presence_query_response(
            &PresenceQueryResponse {
                target: target.clone(),
                presence: Some(overlong_name),
            },
            &query,
        ));
        let mut wrong_revision = valid_presence.clone();
        wrong_revision.presence_revision = 3;
        assert!(!valid_presence_touch_response(
            &PresenceTouchResponse {
                target: target.clone(),
                presence: wrong_revision,
            },
            &touch,
        ));
    }

    #[test]
    fn inbox_receipts_enforce_exact_identity_bounds_and_revision() {
        let target = target();
        let list_request = InboxListRequest {
            target: target.clone(),
            limit: 2,
        };
        let first = entry(&target, Uuid::from_u128(20));
        assert!(valid_inbox_list_response(
            &InboxListResponse {
                target: target.clone(),
                entries: vec![first.clone()],
            },
            &list_request,
        ));
        assert!(!valid_inbox_list_response(
            &InboxListResponse {
                target: target.clone(),
                entries: vec![first.clone(), first],
            },
            &list_request,
        ));
        let mut foreign_recipient = entry(&target, Uuid::from_u128(22));
        foreign_recipient.recipient.agent_id = Uuid::from_u128(23);
        assert!(!valid_inbox_list_response(
            &InboxListResponse {
                target: target.clone(),
                entries: vec![foreign_recipient],
            },
            &list_request,
        ));

        let ack_request = InboxAckRequest {
            target: target.clone(),
            event_id: Uuid::from_u128(21),
            expected_receipt_revision: 1,
        };
        let mut acknowledged = entry(&target, ack_request.event_id);
        acknowledged.receipt_revision = 2;
        acknowledged.acknowledged_at_ms = Some(11);
        assert!(valid_inbox_ack_response(
            &InboxAckResponse {
                target: target.clone(),
                entry: acknowledged,
            },
            &ack_request,
        ));

        let query_request = InboxQueryRequest {
            target: target.clone(),
            event_id: Uuid::from_u128(24),
        };
        assert!(!valid_inbox_query_response(
            &InboxQueryResponse {
                target: target.clone(),
                entry: Some(entry(&target, Uuid::from_u128(25))),
            },
            &query_request,
        ));
    }

    #[test]
    fn request_validation_enforces_canonical_scope_bounds_and_revision_successors() {
        let target = target();
        assert!(validate_target(&target).is_ok());

        let mut invalid_target = target.clone();
        invalid_target.scope.locator = " ".repeat(MAX_LOCATOR_BYTES + 1);
        assert!(validate_target(&invalid_target).is_err());
        let mut invalid_target = target.clone();
        invalid_target.agent_id = Uuid::nil();
        assert!(validate_target(&invalid_target).is_err());

        assert!(require_next_revision(1).is_ok());
        assert!(require_next_revision(0).is_err());
        assert!(require_next_revision(i64::MAX).is_err());
        assert!(validate_inbox_list_request(&InboxListRequest { target, limit: 65 }).is_err());
    }

    #[tokio::test]
    async fn malformed_success_receipts_keep_exact_readback_and_are_not_retried()
    -> Result<(), Box<dyn std::error::Error>> {
        const CREDENTIAL: &str = "Bearer participation-fixture-secret";

        let target = target();
        let touch = PresenceTouchRequest {
            target: target.clone(),
            expected_presence_revision: 10,
        };
        let presence_reply = serde_json::json!({
            "target": touch.target.clone(),
            "presence": {
                "workspaceId": target.scope.workspace_id,
                "participantId": Uuid::from_u128(6),
                "agentId": target.agent_id,
                "sessionId": target.session_id,
                "displayName": "participant",
                "membershipRevision": 1,
                "presenceRevision": 12,
                "lastAuthenticatedAtMs": 10
            }
        });
        let (client, server) = malformed_reply_fixture(presence_reply).await?;
        assert_eq!(
            client
                .touch_presence(&HeaderValue::from_static(CREDENTIAL), &touch)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::Presence {
                    workspace_id: target.scope.workspace_id,
                    activity_id: target.activity_id,
                    agent_id: target.agent_id,
                    session_id: target.session_id,
                    expected_revision: 10,
                }
            })
        );
        let request = String::from_utf8(server.await??)?;
        assert!(request.starts_with("POST /internal/v1/workspaces/"));
        assert!(request.contains("/presence/touch HTTP/1.1"));
        assert!(request.contains("\"expectedPresenceRevision\":10"));

        let ack = InboxAckRequest {
            target: target.clone(),
            event_id: Uuid::from_u128(20),
            expected_receipt_revision: 5,
        };
        let inbox_reply = serde_json::json!({
            "target": ack.target.clone(),
            "entry": {
                "workspaceId": target.scope.workspace_id,
                "activityId": target.activity_id,
                "eventId": ack.event_id,
                "recipient": {
                    "participantId": Uuid::from_u128(6),
                    "agentId": target.agent_id,
                    "sessionId": target.session_id,
                    "displayName": "recipient"
                },
                "actor": {
                    "participantId": Uuid::from_u128(7),
                    "agentId": Uuid::from_u128(8),
                    "sessionId": Uuid::from_u128(9),
                    "displayName": "actor"
                },
                "membershipRevision": 1,
                "observedAtMs": 10,
                "receiptRevision": 7,
                "acknowledgedAtMs": 11
            }
        });
        let (client, server) = malformed_reply_fixture(inbox_reply).await?;
        assert_eq!(
            client
                .acknowledge_inbox(&HeaderValue::from_static(CREDENTIAL), &ack)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::InboxReceipt {
                    workspace_id: target.scope.workspace_id,
                    activity_id: target.activity_id,
                    agent_id: target.agent_id,
                    session_id: target.session_id,
                    event_id: ack.event_id,
                    expected_revision: 5,
                }
            })
        );
        let request = String::from_utf8(server.await??)?;
        assert!(request.starts_with("POST /internal/v1/workspaces/"));
        assert!(request.contains("/inbox/ack HTTP/1.1"));
        assert!(request.contains("\"expectedReceiptRevision\":5"));
        Ok(())
    }

    async fn malformed_reply_fixture(
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
                if request.len() > MAX_REQUEST_BYTES + MAX_HEADER_BYTES {
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
            drop(socket);

            match tokio::time::timeout(Duration::from_millis(100), listener.accept()).await {
                Ok(Ok((mut socket, _))) => {
                    let _ = socket.shutdown().await;
                    return Err(io::Error::other("client retried an uncertain mutation"));
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {}
            }
            Ok(request)
        });
        Ok((client, server))
    }
}
