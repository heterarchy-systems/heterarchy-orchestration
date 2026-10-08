//! Real socket worker readback/permission/restart journey with isolated auth.

use crate::{
    client::{ClientError, OrchestrationHttpClient, ServiceCode},
    wire::provider_worker::{ProviderMessageSelectionQuery, ProviderProvisionInspection},
};
use forge_domain::{AuthenticatedPrincipal, DirectTokenId, TimestampMillis};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
    McpTokenWorker,
};
use heterarchy_orchestration_operations::{AgentRequestEffectScope, AgentRequestIndependence};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, access::OperatorPolicy, provisioning::AgentRequestSubmit,
};
use hyper::header::HeaderValue;
use std::{error::Error, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Fixture {
    root: PathBuf,
    authority: McpTokenRuntime,
    operator: AuthenticatedPrincipal,
    operator_header: HeaderValue,
    foreign_header: HeaderValue,
    operation_id: Uuid,
}

impl Fixture {
    fn open() -> TestResult<(Self, Arc<OrchestrationRuntime>)> {
        let root = std::env::temp_dir().join(format!("worker-http-readback-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let catalog = McpTokenCatalog::open(root.join("tokens.sqlite3"))?;
        let (client, worker) = McpTokenWorker::start(McpTokenService::new([0x63; 32], catalog))?;
        let token = client.issue(McpTokenIssueRequest::new(
            McpTokenLabel::new("operator".to_owned())?,
            TimestampMillis::new(1)?,
        ))?;
        let operator = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
            token.metadata().id().into_uuid(),
        ));
        let operator_header =
            HeaderValue::from_str(&format!("Bearer {}", token.reveal().as_str()))?;
        let foreign = client.issue(McpTokenIssueRequest::new(
            McpTokenLabel::new("ordinary".to_owned())?,
            TimestampMillis::new(1)?,
        ))?;
        let foreign_header =
            HeaderValue::from_str(&format!("Bearer {}", foreign.reveal().as_str()))?;
        let authority = McpTokenRuntime::from_parts(client, worker);
        let runtime = OrchestrationRuntime::open(&root)?
            .with_operator_policy(OperatorPolicy::new(vec![operator])?);
        let request_id = Uuid::now_v7();
        runtime.submit_agent_request(AgentRequestSubmit {
            workspace_id: Uuid::now_v7(),
            activity_id: Uuid::now_v7(),
            requester_agent_id: Uuid::now_v7(),
            requester_session_id: Uuid::now_v7(),
            coordinator_revision: 1,
            request_id,
            objective: "fixture provider readback".to_owned(),
            capability: "chat".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::SourceWrite,
            independence: AgentRequestIndependence::None,
            reason: "read-only client proof".to_owned(),
            created_at_ms: 1,
        })?;
        runtime.reserve_agent_request_resolution(request_id, 2, 3)?;
        runtime.prepare_agent_request_effect_targets(request_id, 6, 7)?;
        let released = runtime
            .provider_worker_next_provision(operator, 10)?
            .ok_or("fixture Execute missing")?;
        let operation_id = released.operation().operation_id();
        Ok((
            Self {
                root,
                authority,
                operator,
                operator_header,
                foreign_header,
                operation_id,
            },
            Arc::new(runtime),
        ))
    }
}

async fn start(
    fixture: &Fixture,
    runtime: Arc<OrchestrationRuntime>,
) -> TestResult<(
    OrchestrationHttpClient,
    CancellationToken,
    tokio::task::JoinHandle<Result<(), crate::BackendError>>,
)> {
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address: SocketAddr = reservation.local_addr()?;
    drop(reservation);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(crate::serve_with_runtime(
        runtime,
        address,
        fixture.authority.client(),
        None,
        cancellation.clone(),
    ));
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok((
                OrchestrationHttpClient::new(address, Duration::from_secs(2))?,
                cancellation,
                server,
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cancellation.cancel();
    let _ = server.await;
    Err("worker HTTP listener never became ready".into())
}

async fn assert_readback(fixture: &Fixture, client: &OrchestrationHttpClient) -> TestResult {
    let query = ProviderProvisionInspection {
        operation_id: fixture.operation_id,
    };
    let first = client
        .provider_provision_query(&fixture.operator_header, &query)
        .await?
        .operation
        .ok_or("exact operation readback absent")?;
    assert_eq!(first.operation_id, fixture.operation_id);
    assert_eq!(first.state, "effect_pending");
    let selection = client
        .provider_provision_in_flight(&fixture.operator_header)
        .await?;
    assert!(!selection.retry_authorized && !selection.request_equivalence_proven);
    assert_eq!(selection.operation, Some(first.clone()));
    let again = client
        .provider_provision_query(&fixture.operator_header, &query)
        .await?;
    assert_eq!(again.operation, Some(first));
    let messages = client
        .provider_messages_in_flight(
            &fixture.operator_header,
            &ProviderMessageSelectionQuery {
                after_message_id: None,
                limit: 32,
            },
        )
        .await?;
    assert!(messages.messages.is_empty());
    assert!(!messages.retry_authorized && !messages.request_equivalence_proven);
    assert!(matches!(
        client
            .provider_provision_query(&fixture.foreign_header, &query)
            .await,
        Err(ClientError::Rejected {
            status: 403,
            code: ServiceCode::Forbidden
        })
    ));
    Ok(())
}

#[tokio::test]
async fn official_worker_readback_is_pure_authorized_and_survives_restart() -> TestResult {
    let (fixture, runtime) = Fixture::open()?;
    let (client, cancellation, server) = start(&fixture, Arc::clone(&runtime)).await?;
    assert_readback(&fixture, &client).await?;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(3), server).await???;
    drop(client);
    drop(runtime);
    let reopened = Arc::new(
        OrchestrationRuntime::open(&fixture.root)?
            .with_operator_policy(OperatorPolicy::new(vec![fixture.operator])?),
    );
    let (client, cancellation, server) = start(&fixture, reopened).await?;
    assert_readback(&fixture, &client).await?;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(3), server).await???;
    drop(client);
    let Fixture {
        root, authority, ..
    } = fixture;
    drop(authority);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
