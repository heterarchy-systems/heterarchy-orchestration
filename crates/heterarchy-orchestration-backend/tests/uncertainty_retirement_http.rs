//! Exact HTTP readback for legacy Agent provision operation history.

use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};

use forge_domain::{AuthenticatedPrincipal, DirectTokenId, TimestampMillis};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenService, McpTokenWorker,
};
use heterarchy_orchestration_backend::{
    client::OrchestrationHttpClient,
    serve_with_runtime,
    wire::{WorkspaceQueryScope, provisioning_operations::*},
};
use heterarchy_orchestration_runtime::{OrchestrationRuntime, access::OperatorPolicy};
use hyper::{StatusCode, header::HeaderValue};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const CREDENTIAL: &str = "Bearer uncertainty-retirement-fixture";
const TOKEN_KEY: [u8; 32] = [0x71; 32];
const DEADLINE: Duration = Duration::from_secs(5);

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn scope() -> WorkspaceQueryScope {
    WorkspaceQueryScope {
        workspace_id: id(1),
        location_id: id(2),
        location_revision: 1,
        workspace_revision: 1,
        locator: "workspace".to_owned(),
    }
}

fn request() -> ProvisionOperationQueryRequest {
    ProvisionOperationQueryRequest {
        scope: scope(),
        session_id: None,
        operation_id: id(5),
    }
}

fn operation() -> ProvisionOperationWire {
    ProvisionOperationWire {
        operation_id: id(5),
        request_id: id(7),
        slot_ordinal: 0,
        effect_kind: "provision_new".to_owned(),
        provider_family: "chatgpt".to_owned(),
        profile_id: Some(id(8)),
        runtime_session_id: Some(id(9)),
        state: "effect_unknown".to_owned(),
        provider_conversation_id: None,
        reported_failure_code: None,
        prepared_at_ms: 10,
        updated_at_ms: 20,
        revision: 4,
        uncertainty_retirement: Some(ProvisionOperationUncertaintyRetirementWire {
            retirement_id: id(4),
            operation_id: id(5),
            workspace_id: id(1),
            requester_session_id: id(3),
            expected_unknown_revision: 3,
            retired_operation_revision: 4,
            recovery_version: 1,
            retired_at_ms: 20,
            actor_principal: ProvisionOperationActorPrincipalWire::DirectToken { token_id: id(6) },
            intent_digest: "ab".repeat(32),
        }),
    }
}

async fn reply<T: serde::Serialize>(socket: &mut TcpStream, response: &T) -> TestResult {
    let body = serde_json::to_vec(response)?;
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    socket.write_all(&body).await?;
    Ok(())
}

async fn post_status(
    address: SocketAddr,
    path: &str,
    authorization: Option<&str>,
    body: &[u8],
) -> TestResult<u16> {
    let mut socket = TcpStream::connect(address).await?;
    let authorization = authorization
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    let headers = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{authorization}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    socket.write_all(body).await?;
    let mut response = Vec::new();
    socket.read_to_end(&mut response).await?;
    let status = std::str::from_utf8(&response)?
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("HTTP status line")?
        .parse()?;
    Ok(status)
}

#[tokio::test]
async fn scoped_query_preserves_exact_legacy_unknown_receipt() -> TestResult {
    let request = request();
    let response = ProvisionOperationQueryResponse {
        scope: request.scope.clone(),
        operation: Some(operation()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request_bytes = vec![0_u8; 16 * 1024];
        let count = socket.read(&mut request_bytes).await?;
        let request_text = String::from_utf8(request_bytes[..count].to_vec())?;
        assert!(request_text.contains(CREDENTIAL));
        assert!(request_text.starts_with(
            "POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000001/provision-operations/query HTTP/1.1"
        ));
        reply(&mut socket, &response).await?;
        TestResult::Ok(())
    };

    let client = OrchestrationHttpClient::new(address, DEADLINE)?;
    let credential = HeaderValue::from_static(CREDENTIAL);
    let query = client.provision_operation(&credential, &request);
    let (transport_result, readback) =
        tokio::time::timeout(DEADLINE, async { tokio::join!(server, query) }).await?;
    transport_result?;
    let readback = readback?;
    assert_eq!(readback.scope, request.scope);
    assert_eq!(readback.operation, Some(operation()));
    Ok(())
}

#[tokio::test]
async fn operation_history_requires_auth_and_exact_workspace_path() -> TestResult {
    let root = std::env::temp_dir().join(format!("operation-history-http-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root)?;
    let catalog = McpTokenCatalog::open(root.join("operator-tokens.sqlite3"))?;
    let (token_client, token_worker) =
        McpTokenWorker::start(McpTokenService::new(TOKEN_KEY, catalog))?;
    let issued = token_client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("operation history fixture".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let bearer = format!("Bearer {}", issued.reveal().as_str());
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
        issued.metadata().id().into_uuid(),
    ));
    let runtime = OrchestrationRuntime::open(&root)?
        .with_operator_policy(OperatorPolicy::new(vec![principal])?);
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = listener.local_addr()?;
    drop(listener);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(serve_with_runtime(
        Arc::new(runtime),
        address,
        token_client,
        None,
        cancellation.clone(),
    ));

    let request = request();
    let body = serde_json::to_vec(&request)?;
    let valid_path = format!(
        "/internal/v1/workspaces/{}/provision-operations/query",
        request.scope.workspace_id
    );
    let unauthenticated = tokio::time::timeout(DEADLINE, async {
        loop {
            match post_status(address, &valid_path, None, &body).await {
                Ok(status) => break status,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await?;
    assert_eq!(unauthenticated, StatusCode::UNAUTHORIZED.as_u16());

    let wrong_path = format!(
        "/internal/v1/workspaces/{}/provision-operations/query",
        id(999)
    );
    assert_eq!(
        post_status(address, &wrong_path, Some(&bearer), &body).await?,
        StatusCode::BAD_REQUEST.as_u16()
    );

    cancellation.cancel();
    tokio::time::timeout(DEADLINE, server).await???;
    drop(token_worker);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn query_requires_explicit_nullable_retirement_projection() -> TestResult {
    let request = request();
    let response = ProvisionOperationQueryResponse {
        scope: request.scope.clone(),
        operation: Some(operation()),
    };
    let mut value = serde_json::to_value(response)?;
    assert_eq!(
        value["operation"]["uncertaintyRetirement"]["retirementId"],
        id(4).to_string()
    );
    value["operation"]
        .as_object_mut()
        .ok_or("operation object")?
        .remove("uncertaintyRetirement");
    assert!(serde_json::from_value::<ProvisionOperationQueryResponse>(value).is_err());
    Ok(())
}
