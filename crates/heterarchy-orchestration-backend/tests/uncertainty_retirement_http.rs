//! Official HTTP-client contract for durable provider uncertainty retirement.
//!
//! Runtime and catalog tests own state-machine coverage. This test freezes the
//! external JSON shape, authenticated route, exact replay, and readback query
//! used after a lost retirement response.

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use forge_domain::{AuthenticatedPrincipal, DirectTokenId, TimestampMillis};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
    McpTokenWorker,
};
use heterarchy_orchestration_backend::client::{
    ClientError, OrchestrationHttpClient, ReadbackTarget,
};
use heterarchy_orchestration_backend::serve_with_runtime;
use heterarchy_orchestration_backend::wire::WorkspaceQueryScope;
use heterarchy_orchestration_backend::wire::provider_worker::{
    ProviderProvisionDispatchResponse, ProviderProvisionDispatchWire,
    ProviderProvisionSelectionObservation, ProviderProvisionTargetWire, ProviderWorkerActionWire,
};
use heterarchy_orchestration_backend::wire::provisioning_operations::{
    ProvisionOperationActorPrincipalWire, ProvisionOperationQueryRequest,
    ProvisionOperationQueryResponse, ProvisionOperationUncertaintyRetirementRequest,
    ProvisionOperationUncertaintyRetirementResponse, ProvisionOperationUncertaintyRetirementWire,
    ProvisionOperationWire,
};
use heterarchy_orchestration_runtime::OrchestrationRuntime;
use heterarchy_orchestration_runtime::access::OperatorPolicy;
use hyper::header::HeaderValue;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CREDENTIAL: &str = "Bearer uncertainty-retirement-fixture";
const TOKEN_KEY: [u8; 32] = [0x71; 32];

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

fn request() -> ProvisionOperationUncertaintyRetirementRequest {
    ProvisionOperationUncertaintyRetirementRequest {
        scope: scope(),
        session_id: id(3),
        retirement_id: id(4),
        operation_id: id(5),
        expected_revision: 3,
        recovery_version: 1,
    }
}

fn retirement(
    request: &ProvisionOperationUncertaintyRetirementRequest,
) -> ProvisionOperationUncertaintyRetirementWire {
    ProvisionOperationUncertaintyRetirementWire {
        retirement_id: request.retirement_id,
        operation_id: request.operation_id,
        workspace_id: request.scope.workspace_id,
        requester_session_id: request.session_id,
        expected_unknown_revision: request.expected_revision,
        retired_operation_revision: request.expected_revision + 1,
        recovery_version: request.recovery_version,
        retired_at_ms: 20,
        actor_principal: ProvisionOperationActorPrincipalWire::DirectToken { token_id: id(6) },
        intent_digest: "ab".repeat(32),
    }
}

fn operation(
    request: &ProvisionOperationUncertaintyRetirementRequest,
    receipt: Option<ProvisionOperationUncertaintyRetirementWire>,
) -> ProvisionOperationWire {
    let retired = receipt.is_some();
    ProvisionOperationWire {
        operation_id: request.operation_id,
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
        updated_at_ms: if retired { 20 } else { 15 },
        revision: if retired {
            request.expected_revision + 1
        } else {
            request.expected_revision
        },
        uncertainty_retirement: receipt,
        project: None,
    }
}

async fn reply<T: Serialize>(socket: &mut tokio::net::TcpStream, response: &T) -> TestResult {
    let body = serde_json::to_vec(response)?;
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    socket.write_all(&body).await?;
    Ok(())
}

fn token_runtime(
    root: &std::path::Path,
) -> TestResult<(McpTokenRuntime, String, AuthenticatedPrincipal)> {
    let catalog = McpTokenCatalog::open(root.join("operator-tokens.sqlite3"))?;
    let (client, worker) = McpTokenWorker::start(McpTokenService::new(TOKEN_KEY, catalog))?;
    let issued = client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("uncertainty retirement route fixture".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let bearer = format!("Bearer {}", issued.reveal().as_str());
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
        issued.metadata().id().into_uuid(),
    ));
    Ok((
        McpTokenRuntime::from_parts(client, worker),
        bearer,
        principal,
    ))
}

async fn post_status(
    address: SocketAddr,
    path: &str,
    authorization: Option<&str>,
    body: &[u8],
) -> TestResult<u16> {
    let mut socket = tokio::net::TcpStream::connect(address).await?;
    let authorization = authorization
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    let headers = format!(
        "POST {path} HTTP/1.1\r\nHost: {address}\r\n{authorization}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
async fn retirement_replay_and_query_preserve_exact_authenticated_receipt() -> TestResult {
    let request = request();
    let receipt = retirement(&request);
    let retired = operation(&request, Some(receipt.clone()));
    let created = ProvisionOperationUncertaintyRetirementResponse {
        scope: request.scope.clone(),
        operation: retired.clone(),
        retirement: receipt.clone(),
        changed: true,
    };
    let replay = ProvisionOperationUncertaintyRetirementResponse {
        changed: false,
        ..created.clone()
    };
    let query = ProvisionOperationQueryResponse {
        scope: request.scope.clone(),
        operation: Some(retired),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let expected_request = request.clone();
    let server = tokio::spawn(async move {
        for (index, response) in [
            serde_json::to_value(&created)?,
            serde_json::to_value(&replay)?,
            serde_json::to_value(&query)?,
        ]
        .into_iter()
        .enumerate()
        {
            let (mut socket, _) = listener.accept().await?;
            let mut request_bytes = vec![0_u8; 16 * 1024];
            let read = socket.read(&mut request_bytes).await?;
            let request_text = String::from_utf8(request_bytes[..read].to_vec())?;
            assert!(request_text.contains(CREDENTIAL));
            let expected_path = if index < 2 {
                format!(
                    "POST /internal/v1/workspaces/{}/provision-operations/retire-unobserved HTTP/1.1",
                    expected_request.scope.workspace_id
                )
            } else {
                format!(
                    "POST /internal/v1/workspaces/{}/provision-operations/query HTTP/1.1",
                    expected_request.scope.workspace_id
                )
            };
            assert!(request_text.starts_with(&expected_path));
            reply(&mut socket, &response).await?;
        }
        TestResult::Ok(())
    });

    let client = OrchestrationHttpClient::new(address, Duration::from_secs(2))?;
    let authorization = HeaderValue::from_static(CREDENTIAL);
    assert!(
        client
            .retire_unobserved_provision_operation(&authorization, &request)
            .await?
            .changed
    );
    let replayed = client
        .retire_unobserved_provision_operation(&authorization, &request)
        .await?;
    assert!(!replayed.changed);
    assert_eq!(replayed.retirement, receipt);
    let readback = client
        .provision_operation(
            &authorization,
            &ProvisionOperationQueryRequest {
                scope: request.scope,
                session_id: Some(request.session_id),
                operation_id: request.operation_id,
            },
        )
        .await?;
    assert_eq!(
        readback
            .operation
            .and_then(|operation| operation.uncertainty_retirement),
        Some(receipt)
    );
    server.await??;
    Ok(())
}

#[tokio::test]
async fn query_requires_explicit_nullable_retirement_projection() -> TestResult {
    let request = request();
    let response = ProvisionOperationQueryResponse {
        scope: request.scope.clone(),
        operation: Some(operation(&request, None)),
    };
    let mut value = serde_json::to_value(response)?;
    assert!(value["operation"]["uncertaintyRetirement"].is_null());
    value["operation"]
        .as_object_mut()
        .ok_or("operation object")?
        .remove("uncertaintyRetirement");
    assert!(serde_json::from_value::<ProvisionOperationQueryResponse>(value).is_err());
    Ok(())
}

#[tokio::test]
async fn retired_unknown_is_rejected_from_dispatch_and_in_flight_source_wrappers() -> TestResult {
    let request = request();
    let receipt = retirement(&request);
    let operation = operation(&request, Some(receipt));
    let dispatch = ProviderProvisionDispatchResponse {
        dispatch: Some(ProviderProvisionDispatchWire {
            workspace_id: request.scope.workspace_id,
            operation: operation.clone(),
            operation_revision: operation.revision,
            action: ProviderWorkerActionWire::Inspect,
            target: ProviderProvisionTargetWire {
                profile_id: operation.profile_id.ok_or("profile")?,
                runtime_session_id: operation.runtime_session_id.ok_or("runtime")?,
                effect_kind: operation.effect_kind.clone(),
                provider_family: operation.provider_family.clone(),
                provider_conversation_id: None,
                project: None,
            },
        }),
        reconciled: None,
    };
    let in_flight = ProviderProvisionSelectionObservation {
        operation: Some(operation),
        retry_authorized: false,
        request_equivalence_proven: false,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        for response in [
            serde_json::to_value(dispatch)?,
            serde_json::to_value(in_flight)?,
        ] {
            let (mut socket, _) = listener.accept().await?;
            let mut request_bytes = vec![0_u8; 16 * 1024];
            let _read = socket.read(&mut request_bytes).await?;
            reply(&mut socket, &response).await?;
        }
        TestResult::Ok(())
    });
    let client = OrchestrationHttpClient::new(address, Duration::from_secs(2))?;
    let authorization = HeaderValue::from_static(CREDENTIAL);
    assert_eq!(
        client
            .provider_provision_dispatch(&authorization)
            .await
            .err(),
        Some(ClientError::UnknownOutcome {
            readback: ReadbackTarget::ProviderProvisionDispatch,
        })
    );
    assert_eq!(
        client
            .provider_provision_in_flight(&authorization)
            .await
            .err(),
        Some(ClientError::Protocol)
    );
    server.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_axum_route_rejects_missing_auth_and_malformed_scope_before_effects() -> TestResult {
    let root = std::env::temp_dir().join(format!("retirement-http-route-{}", Uuid::now_v7()));
    let data_root = root.join("orchestration");
    std::fs::create_dir_all(&data_root)?;
    let (tokens, bearer, principal) = token_runtime(&root)?;
    let runtime = OrchestrationRuntime::open(&data_root)?
        .with_operator_policy(OperatorPolicy::new(vec![principal])?);
    let reserved = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let address = reserved.local_addr()?;
    drop(reserved);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(serve_with_runtime(
        Arc::new(runtime),
        address,
        tokens.client(),
        None,
        cancellation.clone(),
    ));
    let request = request();
    let body = serde_json::to_vec(&request)?;
    let valid_path = format!(
        "/internal/v1/workspaces/{}/provision-operations/retire-unobserved",
        request.scope.workspace_id
    );
    let unauthenticated = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match post_status(address, &valid_path, None, &body).await {
                Ok(status) => break status,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await?;
    assert_eq!(unauthenticated, 401);

    let wrong_path = format!(
        "/internal/v1/workspaces/{}/provision-operations/retire-unobserved",
        id(999)
    );
    assert_eq!(
        post_status(address, &wrong_path, Some(&bearer), &body).await?,
        400
    );
    let mut unsupported = request;
    unsupported.recovery_version = 2;
    assert_eq!(
        post_status(
            address,
            &valid_path,
            Some(&bearer),
            &serde_json::to_vec(&unsupported)?,
        )
        .await?,
        400
    );

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), server).await???;
    drop(tokens);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
