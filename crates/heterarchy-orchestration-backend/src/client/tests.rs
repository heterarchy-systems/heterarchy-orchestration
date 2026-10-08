use std::fmt::Write as _;
use std::time::Duration;

use hyper::header::HeaderValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

const CREDENTIAL: &str = "Bearer client-fixture-secret";

#[tokio::test]
async fn close_requires_exact_transition_revision_and_a_committed_http_status()
-> Result<(), Box<dyn std::error::Error>> {
    let session_id = Uuid::now_v7();
    for (status, revision) in [("200 OK", 7), ("202 Accepted", 2)] {
        let body = serde_json::json!({"sessionId":session_id,"requestId":Uuid::now_v7(),"state":"closed","revision":revision,"createdAtMs":1,"updatedAtMs":2});
        let (client, server) = fixture(Some(reply(status, &body))).await?;
        assert_eq!(
            client
                .close_session(
                    &HeaderValue::from_static(CREDENTIAL),
                    session_id,
                    &AgentSessionCloseRequest {
                        expected_revision: 1
                    }
                )
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::Session(session_id)
            })
        );
        let _captured = server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn deadline_closes_inline_connection_and_releases_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_millis(50))?;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        // A pending response cannot outlive the client deadline. EOF proves
        // that no detached Hyper connection driver retained this socket.
        let count = socket.read_to_end(&mut request).await?;
        Ok::<_, std::io::Error>(count)
    });
    assert_eq!(
        client
            .session(&HeaderValue::from_static(CREDENTIAL), Uuid::now_v7())
            .await
            .err(),
        Some(ClientError::Unavailable)
    );
    assert_eq!(client.slots.available_permits(), CLIENT_CONCURRENCY);
    assert!(tokio::time::timeout(Duration::from_secs(1), peer).await??? > 0);
    Ok(())
}

#[tokio::test]
async fn oversized_and_overfull_headers_cannot_expand_response_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let large_body = "x".repeat(MAX_RESPONSE_BYTES + 1);
    let oversized_body = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{large_body}",
        large_body.len()
    );
    let mut headers = String::new();
    for index in 0..MAX_HEADERS {
        write!(headers, "x-field-{index}: data\r\n")?;
    }
    let excessive_headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n{headers}Content-Length: 2\r\nConnection: close\r\n\r\n{{}}"
    );
    let oversized_header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nx-large: {}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}",
        "x".repeat(MAX_HEADER_BYTES)
    );
    for response in [oversized_body, excessive_headers, oversized_header] {
        let (client, server) = fixture(Some(response)).await?;
        let error = client
            .session(&HeaderValue::from_static(CREDENTIAL), Uuid::now_v7())
            .await
            .err();
        assert!(matches!(
            error,
            Some(ClientError::Protocol | ClientError::Unavailable)
        ));
        match server.await? {
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ) => {}
            Err(error) => return Err(error.into()),
        }
        assert_eq!(client.slots.available_permits(), CLIENT_CONCURRENCY);
    }
    Ok(())
}

#[tokio::test]
async fn oversized_serialization_is_rejected_before_network_and_releases_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let client = OrchestrationHttpClient::new("127.0.0.1:1".parse()?, Duration::from_secs(1))?;
    let request = AgentProfileCreateRequest {
        request_id: Uuid::now_v7().to_string(),
        stable_name: "bounded-profile".to_owned(),
        display_name: "x".repeat(MAX_REQUEST_BYTES + 1),
        preferred_model: None,
        preferred_reasoning: None,
        base_capabilities: Vec::new(),
    };
    assert_eq!(
        client
            .create_profile(&HeaderValue::from_static(CREDENTIAL), &request)
            .await
            .err(),
        Some(ClientError::InvalidRequest)
    );
    assert_eq!(client.slots.available_permits(), CLIENT_CONCURRENCY);
    Ok(())
}

#[tokio::test]
async fn profile_read_rejects_malformed_snapshot_time_and_origin()
-> Result<(), Box<dyn std::error::Error>> {
    let profile_id = Uuid::from_u128(1);
    let base = serde_json::json!({
        "profileId":profile_id,"requestId":Uuid::from_u128(2),"stableName":"stable",
        "displayName":"Display","providerFamily":"chatgpt","preferredModel":null,
        "preferredReasoning":null,"baseCapabilities":[],"lifecycleState":"active",
        "creationOrigin":"user_manual","createdAtMs":10,"updatedAtMs":10,"lastUsedAtMs":null,"revision":1,
    });
    for (field, invalid) in [
        ("updatedAtMs", serde_json::json!(9)),
        ("creationOrigin", serde_json::json!("unknown_origin")),
    ] {
        let mut payload = base.clone();
        payload[field] = invalid;
        let body = serde_json::to_string(&payload)?;
        let (client, server) = fixture(Some(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len(),
        ))).await?;
        assert_eq!(
            client
                .profile(&HeaderValue::from_static(CREDENTIAL), profile_id)
                .await
                .err(),
            Some(ClientError::Protocol)
        );
        let _request = server.await??;
    }
    Ok(())
}

pub(super) async fn fixture(
    response: Option<String>,
) -> Result<
    (
        OrchestrationHttpClient,
        tokio::task::JoinHandle<Result<Vec<u8>, std::io::Error>>,
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
                break;
            }
            request.extend_from_slice(&buffer[..count]);
            if request.len() > MAX_REQUEST_BYTES + MAX_HEADER_BYTES {
                return Err(std::io::Error::other("fixture request exceeded bound"));
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
        if let Some(response) = response {
            socket.write_all(response.as_bytes()).await?;
        }
        socket.shutdown().await?;
        Ok(request)
    });
    Ok((client, server))
}

pub(super) fn reply(status: &str, body: &serde_json::Value) -> String {
    let body = body.to_string();
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[tokio::test]
async fn lost_mutation_response_retains_exact_nonce_and_original_credential()
-> Result<(), Box<dyn std::error::Error>> {
    let (client, server) = fixture(None).await?;
    let nonce = Uuid::now_v7();
    let result = client
        .open_session(
            &HeaderValue::from_static(CREDENTIAL),
            &AgentSessionOpenRequest {
                request_id: nonce.to_string(),
            },
        )
        .await;
    assert_eq!(
        result.err(),
        Some(ClientError::UnknownOutcome {
            readback: ReadbackTarget::SessionRequest(nonce)
        })
    );
    let captured = server.await??;
    let request = String::from_utf8(captured)?;
    assert!(request.contains(CREDENTIAL));
    assert!(request.contains(&nonce.to_string()));
    assert!(!request.contains("principal"));
    assert!(!format!("{client:?}").contains(CREDENTIAL));
    Ok(())
}

#[tokio::test]
async fn mismatched_success_identity_cannot_be_used_as_canonical_readback()
-> Result<(), Box<dyn std::error::Error>> {
    let session_id = Uuid::now_v7();
    let wrong = Uuid::now_v7();
    let response = reply(
        "200 OK",
        &serde_json::json!({"sessionId":wrong,"requestId":Uuid::now_v7(),"state":"open","revision":1,"createdAtMs":1,"updatedAtMs":1}),
    );
    let (client, server) = fixture(Some(response)).await?;
    assert_eq!(
        client
            .session(&HeaderValue::from_static(CREDENTIAL), session_id)
            .await
            .err(),
        Some(ClientError::Protocol)
    );
    let _captured = server.await??;
    Ok(())
}

#[tokio::test]
async fn session_open_readback_requires_exact_nonce_correlation()
-> Result<(), Box<dyn std::error::Error>> {
    let request_id = Uuid::now_v7();
    let session_id = Uuid::now_v7();
    let response = reply(
        "200 OK",
        &serde_json::json!({
            "requestId": request_id,
            "session": {"sessionId":session_id,"requestId":request_id,"state":"closed","revision":2,"createdAtMs":1,"updatedAtMs":2}
        }),
    );
    let (client, server) = fixture(Some(response)).await?;
    let observed = client
        .session_open_readback(&HeaderValue::from_static(CREDENTIAL), request_id)
        .await?
        .ok_or("session readback missing")?;
    assert_eq!(observed.session_id, session_id);
    assert_eq!(observed.state, "closed");
    let captured = String::from_utf8(server.await??)?;
    assert!(captured.contains(&format!(
        "GET /internal/v1/session-open-requests/{request_id}"
    )));

    let wrong = Uuid::now_v7();
    let (client, server) = fixture(Some(reply(
        "200 OK",
        &serde_json::json!({"requestId":request_id,"session":{"sessionId":session_id,"requestId":wrong,"state":"open","revision":1,"createdAtMs":1,"updatedAtMs":1}}),
    )))
    .await?;
    assert_eq!(
        client
            .session_open_readback(&HeaderValue::from_static(CREDENTIAL), request_id)
            .await
            .err(),
        Some(ClientError::Protocol)
    );
    let _captured = server.await??;
    assert_eq!(
        client
            .session_open_readback(&HeaderValue::from_static(CREDENTIAL), Uuid::nil())
            .await
            .err(),
        Some(ClientError::InvalidRequest)
    );
    Ok(())
}

#[tokio::test]
async fn failed_mutation_never_inherits_peer_retry_advice_or_error_text()
-> Result<(), Box<dyn std::error::Error>> {
    let session_id = Uuid::now_v7();
    let response = reply(
        "503 Service Unavailable",
        &serde_json::json!({"code":"ORCHESTRATION_UNAVAILABLE","message":CREDENTIAL,"retryable":true}),
    );
    let (client, server) = fixture(Some(response)).await?;
    let error = client
        .close_session(
            &HeaderValue::from_static(CREDENTIAL),
            session_id,
            &AgentSessionCloseRequest {
                expected_revision: 1,
            },
        )
        .await
        .err();
    assert_eq!(
        error,
        Some(ClientError::UnknownOutcome {
            readback: ReadbackTarget::Session(session_id)
        })
    );
    assert!(!format!("{error:?}").contains(CREDENTIAL));
    let _captured = server.await??;
    Ok(())
}

#[tokio::test]
async fn bounded_overload_and_unconfirmed_commit_have_distinct_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let id = Uuid::now_v7();
    for (status, code, expected) in [
        (
            "503 Service Unavailable",
            "ORCHESTRATION_CAPACITY",
            ClientError::Capacity,
        ),
        (
            "409 Conflict",
            "COMMITTED_BUT_UNCONFIRMED",
            ClientError::ReadbackRequired {
                readback: ReadbackTarget::Session(id),
            },
        ),
        (
            "403 Forbidden",
            "FORBIDDEN",
            ClientError::Rejected {
                status: 403,
                code: ServiceCode::Forbidden,
            },
        ),
    ] {
        let (client, server) = fixture(Some(reply(
            status,
            &serde_json::json!({"code":code,"message":CREDENTIAL,"retryable":false}),
        )))
        .await?;
        assert_eq!(
            client
                .close_session(
                    &HeaderValue::from_static(CREDENTIAL),
                    id,
                    &AgentSessionCloseRequest {
                        expected_revision: 1
                    }
                )
                .await
                .err(),
            Some(expected)
        );
        let _captured = server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn held_exchanges_cannot_expand_client_concurrency() -> Result<(), Box<dyn std::error::Error>>
{
    let client = OrchestrationHttpClient::new("127.0.0.1:1".parse()?, Duration::from_secs(1))?;
    let mut permits = Vec::new();
    for _ in 0..CLIENT_CONCURRENCY {
        permits.push(client.slots.clone().try_acquire_owned()?);
    }
    assert_eq!(
        client
            .session(&HeaderValue::from_static(CREDENTIAL), Uuid::now_v7())
            .await
            .err(),
        Some(ClientError::Capacity)
    );
    drop(permits);
    assert_eq!(client.slots.available_permits(), CLIENT_CONCURRENCY);
    Ok(())
}
