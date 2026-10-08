//! Bounded client for source-scoped leadership statement append and readback.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::leadership::WorkspaceQueryScope;
use crate::wire::leadership_reviews::{
    LeadershipReviewStatementKindWire, LeadershipReviewStatementWire,
};
use crate::wire::leadership_statements::{
    LeadershipStatementAppendRequest, LeadershipStatementAppendResponse,
    LeadershipStatementQueryRequest, LeadershipStatementQueryResponse,
};

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
const MAX_STATEMENT_BODY_BYTES: usize = 16_384;

impl OrchestrationHttpClient {
    /// Append or replay one exact statement using the caller's original credential.
    ///
    /// An uncertain exchange carries the exact Workspace/message readback
    /// identity. The receipt phase is the current review phase at this
    /// append/replay, not immutable statement metadata. The client never
    /// retries the append automatically.
    ///
    /// # Errors
    /// Rejects invalid scope, identities, or body; returns typed service
    /// rejection, bounded transport failure, or exact durable readback identity.
    pub async fn append_leadership_statement(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipStatementAppendRequest,
    ) -> Result<LeadershipStatementAppendResponse, ClientError> {
        validate_append_request(request)?;
        let readback = ReadbackTarget::LeadershipStatement {
            workspace_id: request.scope.workspace_id,
            message_id: request.message_id,
        };
        let response: LeadershipStatementAppendResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/leadership/statements/append",
                    request.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_append_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one exact statement using an optional caller-owned Session selector.
    ///
    /// Session ownership authorizes the read but does not filter the durable
    /// statement. A missing readback is represented explicitly as `None`.
    ///
    /// # Errors
    /// Rejects invalid scope/identity input, typed service rejection, bounded
    /// transport failure, or an inconsistent durable statement readback.
    pub async fn leadership_statement(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipStatementQueryRequest,
    ) -> Result<LeadershipStatementQueryResponse, ClientError> {
        validate_query_request(request)?;
        let response: LeadershipStatementQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/leadership/statements/query",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_append_request(request: &LeadershipStatementAppendRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.activity_id)?;
    require_identity(request.session_id)?;
    require_identity(request.election_id)?;
    require_identity(request.request_id)?;
    require_identity(request.message_id)?;
    validate_body(&request.body)
}

fn validate_query_request(request: &LeadershipStatementQueryRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    if request.session_id.is_some_and(|id| id.is_nil()) {
        return Err(ClientError::InvalidRequest);
    }
    require_identity(request.message_id)
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

fn validate_body(body: &str) -> Result<(), ClientError> {
    if body.trim().is_empty() || body.len() > MAX_STATEMENT_BODY_BYTES {
        Err(ClientError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn valid_append_response(
    response: &LeadershipStatementAppendResponse,
    request: &LeadershipStatementAppendRequest,
) -> bool {
    response.scope == request.scope
        && response.receipt.message_id == request.message_id
        && response.receipt.election_id == request.election_id
        && !response.receipt.message_id.is_nil()
        && !response.receipt.election_id.is_nil()
        && response.receipt.sequence > 0
        && i64::try_from(response.receipt.sequence).is_ok()
}

fn valid_query_response(
    response: &LeadershipStatementQueryResponse,
    request: &LeadershipStatementQueryRequest,
) -> bool {
    response.scope == request.scope
        && response.record.as_ref().is_none_or(|record| {
            record.workspace_id == request.scope.workspace_id
                && !record.request_id.is_nil()
                && valid_statement(&record.statement, request.message_id)
        })
}

fn valid_statement(statement: &LeadershipReviewStatementWire, message_id: Uuid) -> bool {
    statement.statement_id == message_id
        && !statement.statement_id.is_nil()
        && !statement.election_id.is_nil()
        && valid_identity(&statement.author)
        && validate_body(&statement.body).is_ok()
        && statement.sequence > 0
        && i64::try_from(statement.sequence).is_ok()
        && statement.submitted_at_ms >= 0
        && (!statement.incumbent_final
            || statement.kind == LeadershipReviewStatementKindWire::Defense)
}

fn valid_identity(
    identity: &crate::wire::leadership_reviews::LeadershipReviewIdentityWire,
) -> bool {
    identity.participant_id.is_none_or(|id| !id.is_nil())
        && identity.participation_id.is_none_or(|id| !id.is_nil())
        && identity.profile_id.is_none_or(|id| !id.is_nil())
        && identity.session_id.is_none_or(|id| !id.is_nil())
        && !identity.display_name.trim().is_empty()
        && identity.display_name.len() <= MAX_DISPLAY_NAME_BYTES
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    use crate::wire::leadership::WorkspaceQueryScope;
    use crate::wire::leadership_reviews::{
        LeadershipReviewIdentityWire, LeadershipReviewPhaseWire, LeadershipReviewStatementKindWire,
        LeadershipReviewStatementWire,
    };
    use crate::wire::leadership_statements::{
        LeadershipStatementAppendRequest, LeadershipStatementAppendResponse,
        LeadershipStatementQueryRequest, LeadershipStatementQueryResponse,
        LeadershipStatementReadbackWire, LeadershipStatementReceiptWire,
    };

    use super::*;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn append_request() -> LeadershipStatementAppendRequest {
        LeadershipStatementAppendRequest {
            scope: scope(),
            activity_id: Uuid::from_u128(3),
            session_id: Uuid::from_u128(4),
            election_id: Uuid::from_u128(5),
            request_id: Uuid::from_u128(6),
            message_id: Uuid::from_u128(7),
            kind: LeadershipReviewStatementKindWire::Discussion,
            body: "A bounded review statement".to_owned(),
        }
    }

    fn query_request() -> LeadershipStatementQueryRequest {
        LeadershipStatementQueryRequest {
            scope: scope(),
            session_id: Some(Uuid::from_u128(4)),
            message_id: Uuid::from_u128(7),
        }
    }

    fn identity() -> LeadershipReviewIdentityWire {
        LeadershipReviewIdentityWire {
            participant_id: Some(Uuid::from_u128(8)),
            participation_id: Some(Uuid::from_u128(9)),
            profile_id: Some(Uuid::from_u128(10)),
            session_id: Some(Uuid::from_u128(11)),
            display_name: "Review author".to_owned(),
        }
    }

    fn statement(message_id: Uuid) -> LeadershipReviewStatementWire {
        LeadershipReviewStatementWire {
            statement_id: message_id,
            election_id: Uuid::from_u128(5),
            kind: LeadershipReviewStatementKindWire::Discussion,
            author: identity(),
            body: "A bounded review statement".to_owned(),
            sequence: 2,
            submitted_at_ms: 12,
            incumbent_final: false,
        }
    }

    fn receipt(
        request: &LeadershipStatementAppendRequest,
        phase: LeadershipReviewPhaseWire,
        replay: bool,
    ) -> LeadershipStatementAppendResponse {
        LeadershipStatementAppendResponse {
            scope: request.scope.clone(),
            receipt: LeadershipStatementReceiptWire {
                message_id: request.message_id,
                election_id: request.election_id,
                sequence: 2,
                phase,
                replay,
            },
        }
    }

    fn readback(request: &LeadershipStatementQueryRequest) -> LeadershipStatementQueryResponse {
        LeadershipStatementQueryResponse {
            scope: request.scope.clone(),
            record: Some(LeadershipStatementReadbackWire {
                workspace_id: request.scope.workspace_id,
                request_id: Uuid::from_u128(6),
                statement: statement(request.message_id),
            }),
        }
    }

    #[test]
    fn append_request_requires_exact_non_nil_source_and_bounded_body() {
        let valid = append_request();
        assert!(validate_append_request(&valid).is_ok());

        let mut invalid = valid.clone();
        invalid.message_id = Uuid::nil();
        assert!(validate_append_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.session_id = Uuid::nil();
        assert!(validate_append_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.scope.workspace_revision = 0;
        assert!(validate_append_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.body = " \n\t".to_owned();
        assert!(validate_append_request(&invalid).is_err());
        invalid.body = "x".repeat(MAX_STATEMENT_BODY_BYTES + 1);
        assert!(validate_append_request(&invalid).is_err());
    }

    #[test]
    fn query_request_requires_non_nil_exact_source_and_message() {
        let valid = query_request();
        assert!(validate_query_request(&valid).is_ok());

        let mut invalid = valid.clone();
        invalid.message_id = Uuid::nil();
        assert!(validate_query_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.session_id = Some(Uuid::nil());
        assert!(validate_query_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.scope.locator = "bad\nlocator".to_owned();
        assert!(validate_query_request(&invalid).is_err());
    }

    #[test]
    fn append_receipt_echoes_exact_statement_without_freezing_current_phase() {
        let request = append_request();
        for phase in [
            LeadershipReviewPhaseWire::Defense,
            LeadershipReviewPhaseWire::Discussion,
            LeadershipReviewPhaseWire::Ballot,
            LeadershipReviewPhaseWire::AwaitingDecision,
            LeadershipReviewPhaseWire::Resolved,
        ] {
            assert!(valid_append_response(
                &receipt(&request, phase, true),
                &request
            ));
        }
        let mut wrong_message = receipt(&request, LeadershipReviewPhaseWire::Resolved, true);
        wrong_message.receipt.message_id = Uuid::from_u128(99);
        assert!(!valid_append_response(&wrong_message, &request));
        let mut wrong_election = receipt(&request, LeadershipReviewPhaseWire::Resolved, false);
        wrong_election.receipt.election_id = Uuid::from_u128(99);
        assert!(!valid_append_response(&wrong_election, &request));
        let mut zero_sequence = receipt(&request, LeadershipReviewPhaseWire::Resolved, false);
        zero_sequence.receipt.sequence = 0;
        assert!(!valid_append_response(&zero_sequence, &request));
    }

    #[test]
    fn query_preserves_explicit_absence_and_rejects_foreign_or_malformed_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = query_request();
        let valid = readback(&request);
        assert!(valid_query_response(&valid, &request));

        let mut absent = valid.clone();
        absent.record = None;
        assert!(valid_query_response(&absent, &request));

        let mut foreign = valid.clone();
        foreign
            .record
            .as_mut()
            .ok_or("record missing")?
            .workspace_id = Uuid::from_u128(99);
        assert!(!valid_query_response(&foreign, &request));

        let mut wrong_message = valid.clone();
        wrong_message
            .record
            .as_mut()
            .ok_or("record missing")?
            .statement
            .statement_id = Uuid::from_u128(99);
        assert!(!valid_query_response(&wrong_message, &request));

        let mut malformed_body = valid.clone();
        malformed_body
            .record
            .as_mut()
            .ok_or("record missing")?
            .statement
            .body = "x".repeat(MAX_STATEMENT_BODY_BYTES + 1);
        assert!(!valid_query_response(&malformed_body, &request));

        let mut wire = serde_json::to_value(&absent)?;
        let _omitted = wire
            .as_object_mut()
            .ok_or("response object")?
            .remove("record");
        assert!(serde_json::from_value::<LeadershipStatementQueryResponse>(wire).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn append_and_query_keep_exact_message_bindings_and_current_phase()
    -> Result<(), Box<dyn std::error::Error>> {
        let append = append_request();
        let query = query_request();
        let append_body =
            serde_json::to_vec(&receipt(&append, LeadershipReviewPhaseWire::Resolved, true))?;
        let query_body = serde_json::to_vec(&readback(&query))?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(2))?;
        let workspace_id = append.scope.workspace_id;
        let message_id = append.message_id;
        let server = tokio::spawn(async move {
            for (path, body) in [
                (
                    format!(
                        "POST /internal/v1/workspaces/{workspace_id}/leadership/statements/append HTTP/1.1"
                    ),
                    append_body,
                ),
                (
                    format!(
                        "POST /internal/v1/workspaces/{workspace_id}/leadership/statements/query HTTP/1.1"
                    ),
                    query_body,
                ),
            ] {
                let (mut socket, _) = listener.accept().await?;
                let request_text = read_request(&mut socket).await?;
                assert!(request_text.starts_with(&path));
                assert!(request_text.contains(&message_id.to_string()));
                write_response(&mut socket, &body).await?;
            }
            Ok::<_, std::io::Error>(())
        });

        let append_result = client
            .append_leadership_statement(&HeaderValue::from_static("Bearer fixture"), &append)
            .await?;
        assert!(append_result.receipt.replay);
        assert_eq!(
            append_result.receipt.phase,
            LeadershipReviewPhaseWire::Resolved
        );
        let query_result = client
            .leadership_statement(&HeaderValue::from_static("Bearer fixture"), &query)
            .await?;
        assert!(valid_query_response(&query_result, &query));
        assert_eq!(
            query_result
                .record
                .as_ref()
                .ok_or("statement readback missing")?
                .statement
                .statement_id,
            message_id
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn malformed_and_lost_append_exchanges_keep_exact_message_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut first_request = append_request();
        first_request.message_id = Uuid::from_u128(70);
        let mut second_request = append_request();
        second_request.message_id = Uuid::from_u128(71);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(2))?;
        let workspace_id = first_request.scope.workspace_id;
        let first_message = first_request.message_id;
        let second_message = second_request.message_id;
        let server = tokio::spawn(async move {
            let (mut malformed_socket, _) = listener.accept().await?;
            let request_text = read_request(&mut malformed_socket).await?;
            assert!(request_text.starts_with(&format!(
                "POST /internal/v1/workspaces/{workspace_id}/leadership/statements/append HTTP/1.1"
            )));
            assert!(request_text.contains(&first_message.to_string()));
            write_response(&mut malformed_socket, b"{}").await?;

            let (mut lost_socket, _) = listener.accept().await?;
            let request_text = read_request(&mut lost_socket).await?;
            assert!(request_text.contains(&second_message.to_string()));
            drop(lost_socket);
            Ok::<_, std::io::Error>(())
        });

        for (request, message_id) in [
            (first_request, first_message),
            (second_request, second_message),
        ] {
            assert!(matches!(
                client
                    .append_leadership_statement(
                        &HeaderValue::from_static("Bearer fixture"),
                        &request,
                    )
                    .await,
                Err(ClientError::UnknownOutcome {
                    readback: ReadbackTarget::LeadershipStatement {
                        workspace_id: actual_workspace,
                        message_id: actual_message,
                    }
                }) if actual_workspace == workspace_id && actual_message == message_id
            ));
        }
        server.await??;
        Ok(())
    }

    async fn read_request(socket: &mut tokio::net::TcpStream) -> Result<String, std::io::Error> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 2_048];
        loop {
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
            let Some(header_end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::trim)
                        .and_then(|value| value.parse::<usize>().ok())
                })
                .ok_or_else(|| std::io::Error::other("HTTP Content-Length is missing"))?;
            if bytes.len() >= header_end + 4 + content_length {
                return Ok(String::from_utf8_lossy(&bytes).into_owned());
            }
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn write_response(
        socket: &mut tokio::net::TcpStream,
        body: &[u8],
    ) -> Result<(), std::io::Error> {
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(headers.as_bytes()).await?;
        socket.write_all(body).await?;
        Ok(())
    }
}
