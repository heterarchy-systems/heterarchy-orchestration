use std::{error::Error, io, net::SocketAddr, time::Duration};

use forge_backend::ops::CommandOp;
use forge_backend::{ForgeHttpClientConfigError, ForgeHttpClientError, ForgeHttpServiceCode};
use hyper::StatusCode;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{ForgeCommandHttpClient, MAX_HEADER_BYTES, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES};

const SERVICE_SECRET: &str = "fixture-forge-service-secret";

#[tokio::test]
async fn departure_revision_rejection_retains_the_known_domain_class() -> Result<(), Box<dyn Error>>
{
    for (op, retryable, readback, action, expected) in [
        (
            CommandOp::AgentDepartureReconcile,
            false,
            Value::Null,
            "refresh_work",
            ForgeHttpServiceCode::StaleRevision,
        ),
        (
            CommandOp::WorkspaceResolve,
            false,
            Value::Null,
            "refresh_work",
            ForgeHttpServiceCode::ForgeUnavailable,
        ),
        (
            CommandOp::AgentDepartureReconcile,
            true,
            Value::Null,
            "refresh_work",
            ForgeHttpServiceCode::ForgeUnavailable,
        ),
        (
            CommandOp::AgentDepartureReconcile,
            false,
            json!({"kind":"agent_departure_reconciliation"}),
            "refresh_work",
            ForgeHttpServiceCode::ForgeUnavailable,
        ),
        (
            CommandOp::AgentDepartureReconcile,
            false,
            Value::Null,
            "read_back",
            ForgeHttpServiceCode::ForgeUnavailable,
        ),
    ] {
        let (authority, server) = one_exchange(move |request| {
            http_response("503 Service Unavailable", "", json!({
                "forge_api":forge_backend::FORGE_API_VERSION, "request_id":request["request_id"], "ok":false,
                "error":{"code":"FORGE_UNAVAILABLE","message":"redacted rejection","category":"revision_conflict",
                    "retryable":retryable,"recovery_action":action,"recovery_hint":"read exact state","readback":readback},
            }).to_string().as_bytes())
        }).await?;
        let client =
            ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;
        let result = client.command(op, json!({})).await;
        assert!(
            matches!(result, Err(ForgeHttpClientError::Service {status:503,code}) if code==expected)
        );
        let _request = server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn command_uses_typed_op_endpoint_and_redacts_the_bearer() -> Result<(), Box<dyn Error>> {
    let (authority, server) = one_exchange(|request| {
        success_response(&request["request_id"], forge_backend::FORGE_API_VERSION)
    })
    .await?;
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;

    let result = client
        .command(CommandOp::WorkspaceResolve, json!({"workspace_id": "w-1"}))
        .await?;
    assert_eq!(result, json!({"resolved": true}));

    let request = server.await??;
    let request_line = std::str::from_utf8(&request)?
        .split("\r\n")
        .next()
        .ok_or("request line missing")?;
    assert_eq!(
        request_line,
        "POST /api/v1/commands/workspace.resolve HTTP/1.1"
    );
    let header_end = header_end(&request).ok_or("request header terminator missing")?;
    let headers = std::str::from_utf8(&request[..header_end])?;
    assert!(headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("authorization")
                && value.trim() == format!("Bearer {SERVICE_SECRET}")
        })
    }));
    let request_body = request_value(&request)?;
    assert_eq!(request_body["op"], "workspace.resolve");
    assert_eq!(request_body["payload"], json!({"workspace_id": "w-1"}));
    assert!(!format!("{client:?}").contains(SERVICE_SECRET));
    Ok(())
}

#[tokio::test]
async fn durable_command_preserves_envelope_and_payload_request_identity()
-> Result<(), Box<dyn Error>> {
    let durable_request_id = Uuid::from_u128(0xB701);
    for (op, payload) in [
        (
            CommandOp::AgentBindingRejectionGet,
            json!({"request_id": durable_request_id}),
        ),
        (
            CommandOp::AgentBindingReject,
            json!({
                "reservation": {"request_id": durable_request_id},
                "activation": {"request_id": durable_request_id},
            }),
        ),
    ] {
        let (authority, server) = one_exchange(|request| {
            success_response(&request["request_id"], forge_backend::FORGE_API_VERSION)
        })
        .await?;
        let client =
            ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;
        let result = client
            .command_with_request_id(durable_request_id, op, payload)
            .await?;
        assert_eq!(result, json!({"resolved": true}));
        let request = server.await??;
        let body = request_value(&request)?;
        assert_eq!(body["request_id"], durable_request_id.to_string());
        match op {
            CommandOp::AgentBindingRejectionGet => {
                assert_eq!(
                    body["payload"]["request_id"],
                    durable_request_id.to_string()
                );
            }
            CommandOp::AgentBindingReject => {
                assert_eq!(
                    body["payload"]["reservation"]["request_id"],
                    durable_request_id.to_string()
                );
                assert_eq!(
                    body["payload"]["activation"]["request_id"],
                    durable_request_id.to_string()
                );
            }
            _ => return Err("unexpected durable command fixture".into()),
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = ForgeCommandHttpClient::new(
        listener.local_addr()?,
        SERVICE_SECRET,
        Duration::from_secs(1),
    )?;
    drop(listener);
    assert_eq!(
        client
            .command_with_request_id(
                Uuid::nil(),
                CommandOp::AgentBindingRejectionGet,
                json!({"request_id": Uuid::nil()}),
            )
            .await,
        Err(ForgeHttpClientError::InvalidRequest)
    );
    Ok(())
}

#[tokio::test]
async fn command_rejects_wrong_request_nonce_and_protocol_version() -> Result<(), Box<dyn Error>> {
    let wrong_nonce = json!(Uuid::now_v7());
    let (authority, nonce_server) =
        one_exchange(move |_| success_response(&wrong_nonce, forge_backend::FORGE_API_VERSION))
            .await?;
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;
    assert_eq!(
        client.command(CommandOp::ActivityGet, json!({})).await,
        Err(ForgeHttpClientError::Protocol)
    );
    let _nonce_request = nonce_server.await??;

    let (authority, version_server) = one_exchange(|request| {
        success_response(&request["request_id"], forge_backend::FORGE_API_VERSION + 1)
    })
    .await?;
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;
    assert_eq!(
        client.command(CommandOp::ActivityGet, json!({})).await,
        Err(ForgeHttpClientError::Protocol)
    );
    let _version_request = version_server.await??;
    Ok(())
}

#[tokio::test]
async fn canonical_rich_forge_failure_envelope_maps_to_a_redacted_service_error()
-> Result<(), Box<dyn Error>> {
    let (authority, server) = one_exchange(|request| {
        let body = json!({
            "forge_api": forge_backend::FORGE_API_VERSION,
            "request_id": request["request_id"],
            "ok": false,
            "error": {
                "code": "WORKSPACE_CONFINEMENT_DENIED",
                "message": "the Workspace effect scope overlaps another scope",
                "category": "path_confinement",
                "retryable": false,
                "recovery_action": "correct_input",
                "recovery_hint": "",
                "readback": null
            }
        });
        http_response("409 Conflict", "", body.to_string().as_bytes())
    })
    .await?;
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;

    let result = client.command(CommandOp::WorkspaceResolve, json!({})).await;
    assert_eq!(
        result,
        Err(ForgeHttpClientError::Service {
            status: StatusCode::CONFLICT.as_u16(),
            code: ForgeHttpServiceCode::WorkspaceConfinementDenied,
        })
    );
    assert!(!format!("{result:?}").contains("overlaps another scope"));
    let _request = server.await??;
    Ok(())
}

#[tokio::test]
async fn request_and_response_bodies_keep_their_existing_bounds() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let authority = listener.local_addr()?;
    drop(listener);
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;
    let oversized_payload = "x".repeat(MAX_REQUEST_BYTES);
    assert_eq!(
        client
            .command(
                CommandOp::WorkspaceResolve,
                json!({"oversized_payload": oversized_payload})
            )
            .await,
        Err(ForgeHttpClientError::RequestTooLarge)
    );

    let body = vec![b' '; MAX_RESPONSE_BYTES + 1];
    let (authority, server) = one_exchange(move |_| http_response("200 OK", "", &body)).await?;
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(2))?;
    assert_eq!(
        client.command(CommandOp::WorkspaceResolve, json!({})).await,
        Err(ForgeHttpClientError::ResponseTooLarge)
    );
    let _request = server.await??;
    Ok(())
}

#[tokio::test]
async fn response_header_count_and_byte_limits_reject_the_exchange() -> Result<(), Box<dyn Error>> {
    let mut too_many_headers = String::new();
    for index in 0..14 {
        use std::fmt::Write as _;
        writeln!(&mut too_many_headers, "x-fixture-{index}: value\r")?;
    }
    let (authority, field_server) = one_exchange(move |request| {
        let body = success_reply(request).to_string();
        http_response("200 OK", &too_many_headers, body.as_bytes())
    })
    .await?;
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;
    let result = client.command(CommandOp::ActivityGet, json!({})).await;
    assert_header_protocol_error(&result);
    let _field_request = field_server.await??;

    let oversized_header = format!("x-fixture: {}\r\n", "x".repeat(MAX_HEADER_BYTES));
    let (authority, byte_server) = one_exchange(move |request| {
        let body = success_reply(request).to_string();
        http_response("200 OK", &oversized_header, body.as_bytes())
    })
    .await?;
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;
    let result = client.command(CommandOp::ActivityGet, json!({})).await;
    assert_header_protocol_error(&result);
    let _byte_request = byte_server.await??;
    Ok(())
}

#[tokio::test]
async fn immediate_capacity_rejects_a_fifth_exchange_then_recovers() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let authority = listener.local_addr()?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut held = Vec::with_capacity(4);
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().await?;
            let request = read_request(&mut stream).await?;
            held.push((stream, request_value(&request)?));
        }
        let _ = ready_tx.send(());
        release_rx
            .await
            .map_err(|_| io::Error::other("test did not release held exchanges"))?;
        for (mut stream, request) in held {
            let response =
                success_response(&request["request_id"], forge_backend::FORGE_API_VERSION);
            stream.write_all(&response).await?;
        }
        let (mut stream, _) = listener.accept().await?;
        let request = read_request(&mut stream).await?;
        let request_body = request_value(&request)?;
        let response = success_response(
            &request_body["request_id"],
            forge_backend::FORGE_API_VERSION,
        );
        stream.write_all(&response).await?;
        Ok::<_, io::Error>(request)
    });
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(5))?;
    let calls = (0..4)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.command(CommandOp::ActivityGet, json!({})).await })
        })
        .collect::<Vec<_>>();
    ready_rx.await?;
    assert_eq!(
        client.command(CommandOp::ActivityGet, json!({})).await,
        Err(ForgeHttpClientError::Capacity)
    );
    release_tx
        .send(())
        .map_err(|()| io::Error::other("test server stopped before releasing exchanges"))?;
    for call in calls {
        assert_eq!(call.await??, json!({"resolved": true}));
    }
    assert_eq!(
        client.command(CommandOp::ActivityGet, json!({})).await?,
        json!({"resolved": true})
    );
    let recovered_request = server.await??;
    assert_eq!(request_value(&recovered_request)?["op"], "activity.get");
    Ok(())
}

#[tokio::test]
async fn timed_out_exchange_closes_inline_connection_and_releases_capacity()
-> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let authority = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stalled, _) = listener.accept().await?;
        let _stalled_request = read_request(&mut stalled).await?;
        let mut byte = [0_u8; 1];
        if stalled.read(&mut byte).await? != 0 {
            return Err(io::Error::other(
                "timed-out exchange kept its connection open",
            ));
        }
        drop(stalled);

        let (mut recovered, _) = listener.accept().await?;
        let request = read_request(&mut recovered).await?;
        let request_body = request_value(&request)?;
        let response = success_response(
            &request_body["request_id"],
            forge_backend::FORGE_API_VERSION,
        );
        recovered.write_all(&response).await?;
        Ok::<_, io::Error>(request)
    });
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;

    assert_eq!(
        client.command(CommandOp::ActivityGet, json!({})).await,
        Err(ForgeHttpClientError::Timeout)
    );
    assert_eq!(
        client.command(CommandOp::ActivityGet, json!({})).await?,
        json!({"resolved": true})
    );
    let recovered_request = server.await??;
    assert_eq!(request_value(&recovered_request)?["op"], "activity.get");
    Ok(())
}

#[test]
fn constructor_rejects_untrusted_authority_and_invalid_bearers() {
    let non_loopback = SocketAddr::from(([192, 0, 2, 1], 53000));
    assert!(matches!(
        ForgeCommandHttpClient::new(non_loopback, SERVICE_SECRET, Duration::from_secs(1)),
        Err(ForgeHttpClientConfigError::NonLoopbackAuthority)
    ));
    let missing_port = SocketAddr::from(([127, 0, 0, 1], 0));
    assert!(matches!(
        ForgeCommandHttpClient::new(missing_port, SERVICE_SECRET, Duration::from_secs(1)),
        Err(ForgeHttpClientConfigError::MissingAuthorityPort)
    ));
    let authority = SocketAddr::from(([127, 0, 0, 1], 53000));
    assert!(matches!(
        ForgeCommandHttpClient::new(authority, "", Duration::from_secs(1)),
        Err(ForgeHttpClientConfigError::EmptyServiceBearer)
    ));
    assert!(matches!(
        ForgeCommandHttpClient::new(authority, "invalid\nbearer", Duration::from_secs(1)),
        Err(ForgeHttpClientConfigError::InvalidServiceBearer)
    ));
}

fn assert_header_protocol_error(result: &Result<Value, ForgeHttpClientError>) {
    assert!(matches!(
        result,
        Err(ForgeHttpClientError::Protocol | ForgeHttpClientError::Transport)
    ));
}

async fn one_exchange(
    response: impl FnOnce(&Value) -> Vec<u8> + Send + 'static,
) -> io::Result<(SocketAddr, JoinHandle<io::Result<Vec<u8>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let authority = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_request(&mut stream).await?;
        let request_body = request_value(&request)?;
        stream.write_all(&response(&request_body)).await?;
        Ok(request)
    });
    Ok((authority, server))
}

#[tokio::test]
async fn unreachable_loopback_authority_returns_a_typed_transport_error()
-> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let authority = listener.local_addr()?;
    drop(listener);
    let client = ForgeCommandHttpClient::new(authority, SERVICE_SECRET, Duration::from_secs(1))?;
    assert_eq!(
        client.command(CommandOp::ActivityGet, json!({})).await,
        Err(ForgeHttpClientError::Transport)
    );
    Ok(())
}

async fn read_request(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 4096];
    let mut expected_end = None;
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        if expected_end.is_none()
            && let Some(end) = header_end(&request)
        {
            let headers = std::str::from_utf8(&request[..end])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let content_length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing content length")
                })?
                .parse::<usize>()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            expected_end = Some(
                end.checked_add(4)
                    .and_then(|header| header.checked_add(content_length))
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "request length overflow")
                    })?,
            );
        }
        if let Some(end) = expected_end
            && request.len() >= end
        {
            request.truncate(end);
            return Ok(request);
        }
        if request.len() > MAX_REQUEST_BYTES + MAX_HEADER_BYTES + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fixture request too large",
            ));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "request ended early",
    ))
}

fn request_value(request: &[u8]) -> io::Result<Value> {
    let end = header_end(request)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "request headers incomplete"))?;
    serde_json::from_slice(&request[end + 4..])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn success_reply(request: &Value) -> Value {
    json!({
        "forge_api": forge_backend::FORGE_API_VERSION,
        "request_id": request["request_id"],
        "ok": true,
        "result": {"resolved": true}
    })
}

fn success_response(request_id: &Value, forge_api: u32) -> Vec<u8> {
    let body = json!({
        "forge_api": forge_api,
        "request_id": request_id,
        "ok": true,
        "result": {"resolved": true}
    });
    let body = body.to_string();
    http_response("200 OK", "", body.as_bytes())
}

fn http_response(status: &str, extra_headers: &str, body: &[u8]) -> Vec<u8> {
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n",
        body.len()
    );
    let mut response = headers.into_bytes();
    response.extend_from_slice(body);
    response
}
