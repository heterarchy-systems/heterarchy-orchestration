//! Provider retirement stays closed at the composed HTTP boundary while
//! provider-neutral Profiles, Teams, and exact stored `AgentRequest` recovery remain.

use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};

use forge_domain::{AuthenticatedPrincipal, DirectTokenId, TimestampMillis};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenService, McpTokenWorker,
};
use heterarchy_orchestration_backend::{
    client::OrchestrationHttpClient,
    serve_with_runtime,
    wire::{
        WorkspaceQueryScope,
        agent_request_admission::{
            AdmissionEffectScopeWire, AdmissionIndependenceWire, AgentRequestAdmissionRequest,
        },
    },
};
use heterarchy_orchestration_operations::{AgentRequestEffectScope, AgentRequestIndependence};
use heterarchy_orchestration_runtime::{
    AgentProfileCreateCommand, OrchestrationRuntime, access::OperatorPolicy,
    provisioning::AgentRequestSubmit,
};
use hyper::{StatusCode, header::HeaderValue};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn retired_routes_are_absent_and_generic_history_remains() -> TestResult {
    let root = std::env::temp_dir().join(format!("provider-retirement-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root)?;
    let (token_client, token_worker, bearer, principal) = token_authority(&root)?;

    let runtime = OrchestrationRuntime::open(&root)?
        .with_operator_policy(OperatorPolicy::new(vec![principal])?);
    let profile_request_id = Uuid::now_v7();
    let profile_command = || AgentProfileCreateCommand {
        request_id: profile_request_id,
        stable_name: "provider-retirement-profile".to_owned(),
        display_name: "Provider neutral profile".to_owned(),
        preferred_model: None,
        preferred_reasoning: None,
        base_capabilities: vec!["verification".to_owned()],
        created_at_ms: 1,
    };
    let created_profile = runtime.create_agent_profile(profile_command())?;
    let replayed_profile = runtime.create_agent_profile(profile_command())?;
    assert!(created_profile.created);
    assert!(!replayed_profile.created);
    assert_eq!(
        created_profile.profile.profile_id,
        replayed_profile.profile.profile_id
    );
    assert_eq!(created_profile.profile.provider_family, "unbound");
    let profile_id = created_profile.profile.profile_id;
    let session_id = runtime
        .open_session(Uuid::now_v7(), principal, 1)?
        .session
        .session_id;
    let request = admission_request(session_id);
    runtime.submit_agent_request(AgentRequestSubmit {
        workspace_id: request.scope.workspace_id,
        activity_id: request.activity_id,
        requester_agent_id: request.coordinator_agent_id,
        requester_session_id: request.coordinator_session_id,
        coordinator_revision: u64::try_from(request.coordinator_revision)?,
        request_id: request.request_id,
        objective: request.objective.clone(),
        capability: request.capability.clone(),
        count: request.count,
        effect_scope: AgentRequestEffectScope::ReadOnly,
        independence: AgentRequestIndependence::IndependentFromCurrentWriter,
        reason: request.reason.clone(),
        created_at_ms: 2,
    })?;

    let runtime = Arc::new(runtime);
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    drop(listener);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(serve_with_runtime(
        runtime,
        address,
        token_client,
        None,
        cancellation.clone(),
    ));
    let client = OrchestrationHttpClient::new(address, DEADLINE)?;

    let result: TestResult = async {
        wait_until_ready(address, &bearer).await?;

        let profile_readback = client.profile(&bearer, profile_id).await?;
        assert_eq!(profile_readback.provider_family, "unbound");

        let recovered = client
            .recover_agent_request_admission(&bearer, &request)
            .await?;
        assert_eq!(recovered.request, request);
        let snapshot = recovered.snapshot.ok_or("stored AgentRequest was not recovered")?;
        assert_eq!(snapshot.receipt.request_id, request.request_id);
        assert_eq!(snapshot.receipt.state, heterarchy_orchestration_backend::wire::agent_request_admission::AdmissionRequestStateWire::ResolutionPending);
        assert!(snapshot.resolution.is_none());
        assert!(snapshot.operations.is_empty());

        let (team_status, team_body) = raw_request(
            address,
            "GET",
            &format!("/internal/v1/workspaces/{}/teams?limit=10", Uuid::from_u128(0x741)),
            &bearer,
            "",
        )
        .await?;
        // This fixture has no verified Source import, so the existing Team
        // query must fail at its authority boundary rather than claim a page.
        assert_eq!(team_status, StatusCode::CONFLICT);
        let team_failure: serde_json::Value = serde_json::from_slice(&team_body)?;
        assert_eq!(team_failure["code"], "ORCHESTRATION_CONFLICT", "{team_failure}");
        assert_eq!(team_failure["retryable"], false, "{team_failure}");

        assert_provider_route_contracts(
            address,
            &bearer,
            request.request_id,
            request.scope.workspace_id,
        )
        .await?;
        Ok(())
    }
    .await;

    cancellation.cancel();
    server.await??;
    drop(token_worker);
    std::fs::remove_dir_all(root)?;
    result
}

async fn assert_provider_route_contracts(
    address: SocketAddr,
    bearer: &HeaderValue,
    request_id: Uuid,
    workspace_id: Uuid,
) -> TestResult {
    let operation_id = Uuid::now_v7();
    let retired = [
        (
            "POST",
            "/internal/v1/provider-worker/provision/dispatch".to_owned(),
        ),
        (
            "POST",
            "/internal/v1/provider-worker/messages/dispatch".to_owned(),
        ),
        (
            "POST",
            format!("/internal/v1/agent-requests/{request_id}/resolution"),
        ),
        (
            "POST",
            format!("/internal/v1/agent-requests/{request_id}/provision-targets"),
        ),
        (
            "POST",
            format!("/internal/v1/agent-requests/{request_id}/reconcile"),
        ),
        (
            "POST",
            format!("/internal/v1/workspaces/{workspace_id}/agent-requests/admit"),
        ),
        (
            "POST",
            format!("/internal/v1/provision-operations/{operation_id}/join"),
        ),
        (
            "POST",
            format!("/internal/v1/provision-operations/{operation_id}/activate"),
        ),
        (
            "POST",
            format!("/internal/v1/provision-operations/{operation_id}/participation/query"),
        ),
        (
            "POST",
            format!("/internal/v1/workspaces/{workspace_id}/provision-operations/cancel"),
        ),
        (
            "POST",
            format!(
                "/internal/v1/workspaces/{workspace_id}/provision-operations/retire-unobserved"
            ),
        ),
        (
            "POST",
            format!("/internal/v1/workspaces/{workspace_id}/workspace-chat/send"),
        ),
        (
            "POST",
            format!("/internal/v1/workspaces/{workspace_id}/node-chat/send"),
        ),
    ];
    for (method, path) in retired {
        let (status, _) = raw_request(address, method, &path, bearer, "{}").await?;
        // Resolution history retains GET on this path; only POST is retired.
        let expected = if path == format!("/internal/v1/agent-requests/{request_id}/resolution") {
            StatusCode::METHOD_NOT_ALLOWED
        } else {
            StatusCode::NOT_FOUND
        };
        assert_eq!(status, expected, "{method} {path}");
    }

    let (history_status, _) = raw_request(
        address,
        "POST",
        &format!("/internal/v1/workspaces/{workspace_id}/provision-operations/query"),
        bearer,
        "{}",
    )
    .await?;
    assert_eq!(history_status, StatusCode::UNPROCESSABLE_ENTITY);
    for path in [
        format!("/internal/v1/workspaces/{workspace_id}/workspace-chat/query"),
        format!("/internal/v1/workspaces/{workspace_id}/workspace-chat/recover"),
        format!("/internal/v1/workspaces/{workspace_id}/node-chat/query"),
        format!("/internal/v1/workspaces/{workspace_id}/node-chat/recover"),
        format!("/internal/v1/workspaces/{workspace_id}/agent-requests/admit/recover"),
    ] {
        let (status, _) = raw_request(address, "POST", &path, bearer, "{}").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "POST {path}");
    }
    Ok(())
}

fn token_authority(
    root: &std::path::Path,
) -> TestResult<(
    heterarchy_orchestration_auth::McpTokenClient,
    McpTokenWorker<McpTokenCatalog>,
    HeaderValue,
    AuthenticatedPrincipal,
)> {
    let catalog = McpTokenCatalog::open(root.join("operator-tokens.sqlite3"))?;
    let (client, worker) = McpTokenWorker::start(McpTokenService::new([0x74; 32], catalog))?;
    let issued = client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("provider retirement HTTP fixture".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let mut bearer = HeaderValue::from_str(&format!("Bearer {}", issued.reveal().as_str()))?;
    bearer.set_sensitive(true);
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
        issued.metadata().id().into_uuid(),
    ));
    Ok((client, worker, bearer, principal))
}

fn admission_request(session_id: Uuid) -> AgentRequestAdmissionRequest {
    AgentRequestAdmissionRequest {
        scope: WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(0x741),
            location_id: Uuid::from_u128(0x742),
            location_revision: 1,
            workspace_revision: 1,
            locator: "provider-retirement-fixture".to_owned(),
        },
        activity_id: Uuid::from_u128(0x743),
        coordinator_session_id: session_id,
        coordinator_agent_id: Uuid::from_u128(0x744),
        coordinator_revision: 1,
        request_id: Uuid::now_v7(),
        objective: "Read back an intent without reserving a provider".to_owned(),
        capability: "verification".to_owned(),
        count: 1,
        effect_scope: AdmissionEffectScopeWire::ReadOnly,
        independence: AdmissionIndependenceWire::IndependentFromCurrentWriter,
        reason: "Preserve exact durable history".to_owned(),
    }
}

async fn wait_until_ready(address: SocketAddr, bearer: &HeaderValue) -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        loop {
            match raw_request(address, "GET", "/health", bearer, "").await {
                Ok((StatusCode::OK, _)) => return Ok::<(), Box<dyn Error>>(()),
                Ok(_) | Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await??;
    Ok(())
}

async fn raw_request(
    address: SocketAddr,
    method: &str,
    path: &str,
    bearer: &HeaderValue,
    body: &str,
) -> TestResult<(StatusCode, Vec<u8>)> {
    let mut stream = TcpStream::connect(address).await?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        bearer.to_str()?,
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("HTTP response has no header terminator")?;
    let headers = std::str::from_utf8(&response[..split])?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("HTTP response has no status")?
        .parse::<u16>()?;
    Ok((
        StatusCode::from_u16(status)?,
        response[split + 4..].to_vec(),
    ))
}
