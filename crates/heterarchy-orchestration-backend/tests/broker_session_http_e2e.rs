//! Real Broker-cluster-backed HTTP Session E2E for the Orchestration backend.
//!
//! Real three-node `agentbrokerd` cluster (shared helper), real
//! `OrchestrationRuntime::open_with_broker_cluster`, real `serve_with_runtime`
//! and the official `OrchestrationHttpClient` with a real bearer token. No mock
//! Broker, no forged epoch, no direct SQLite writes.
//!
//! ```text
//! AGENT_BROKERD_BIN=/Volumes/256.SSD/RustBuild/broker/debug/agentbrokerd \
//!   .codex/rust-ssd orchestration cargo test --offline \
//!   -p heterarchy-orchestration-backend --test broker_session_http_e2e -- --ignored
//! ```
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::Path;
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
use heterarchy_orchestration_backend::wire::{AgentSessionCloseRequest, AgentSessionOpenRequest};
use heterarchy_orchestration_operations::{
    BrokerClusterConfig, BrokerOwnerAuthority, BrokerOwnerAuthorityError,
};
use heterarchy_orchestration_runtime::OrchestrationRuntime;
use heterarchy_orchestration_runtime::access::OperatorPolicy;
use hyper::StatusCode;
use hyper::header::HeaderValue;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[path = "../../heterarchy-orchestration-runtime/tests/support/broker_cluster.rs"]
mod broker_cluster;

use broker_cluster::{Cluster, TestResult};

const DEADLINE: Duration = Duration::from_secs(8);
const TOKEN_KEY: [u8; 32] = [0x67; 32];

fn token_runtime(root: &Path) -> Result<McpTokenRuntime, Box<dyn std::error::Error>> {
    let catalog = McpTokenCatalog::open(root.join("operator-tokens.sqlite3"))?;
    let (client, worker) = McpTokenWorker::start(McpTokenService::new(TOKEN_KEY, catalog))?;
    Ok(McpTokenRuntime::from_parts(client, worker))
}

/// Issue the single operator bearer used across backend incarnations.
fn issue_bearer(
    root: &Path,
) -> Result<(HeaderValue, AuthenticatedPrincipal), Box<dyn std::error::Error>> {
    let catalog = McpTokenCatalog::open(root.join("operator-tokens.sqlite3"))?;
    let (client, worker) = McpTokenWorker::start(McpTokenService::new(TOKEN_KEY, catalog))?;
    let issued = client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("broker session e2e operator".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let mut bearer = HeaderValue::from_str(&format!("Bearer {}", issued.reveal().as_str()))?;
    bearer.set_sensitive(true);
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
        issued.metadata().id().into_uuid(),
    ));
    drop(McpTokenRuntime::from_parts(client, worker));
    Ok((bearer, principal))
}

struct Backend {
    address: SocketAddr,
    client: OrchestrationHttpClient,
    cancellation: CancellationToken,
    server: JoinHandle<Result<(), heterarchy_orchestration_backend::BackendError>>,
    token: McpTokenRuntime,
}

impl Backend {
    async fn start(
        data_root: &Path,
        cluster: BrokerClusterConfig,
        principal: AuthenticatedPrincipal,
        bearer: &HeaderValue,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = OrchestrationRuntime::open_with_broker_cluster(data_root, cluster)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?);
        let token = token_runtime(data_root.parent().ok_or("data root has no parent")?)?;
        let reserved = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let address: SocketAddr = reserved.local_addr()?;
        drop(reserved);
        let cancellation = CancellationToken::new();
        let server = tokio::spawn(serve_with_runtime(
            Arc::new(runtime),
            address,
            token.client(),
            None,
            cancellation.clone(),
        ));
        let client = OrchestrationHttpClient::new(address, DEADLINE)?;
        tokio::time::timeout(DEADLINE, async {
            loop {
                match client.session_open_readback(bearer, Uuid::now_v7()).await {
                    Ok(None) => return Ok::<(), ClientError>(()),
                    Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                    Ok(Some(_)) => return Err(ClientError::Protocol),
                    Err(error) => return Err(error),
                }
            }
        })
        .await??;
        Ok(Self {
            address,
            client,
            cancellation,
            server,
            token,
        })
    }

    /// Stop only the backend this test spawned and release the data root.
    async fn stop(self) -> TestResult {
        self.cancellation.cancel();
        tokio::time::timeout(DEADLINE, self.server).await???;
        drop(self.token);
        Ok(())
    }
}

fn open_request(request_id: Uuid) -> AgentSessionOpenRequest {
    AgentSessionOpenRequest {
        request_id: request_id.to_string(),
    }
}

/// A mutation attempted without Broker quorum must neither succeed nor leave a
/// durable record; an uncertain outcome must name the exact readback target.
fn assert_blocked_open<T: std::fmt::Debug>(
    outcome: Result<T, ClientError>,
    request_id: Uuid,
) -> TestResult {
    eprintln!("fail-closed outcome without quorum: {outcome:?}");
    match outcome {
        Err(ClientError::UnknownOutcome {
            readback: ReadbackTarget::SessionRequest(id),
        }) if id == request_id => Ok(()),
        Err(ClientError::Rejected { status, .. })
            if status == StatusCode::SERVICE_UNAVAILABLE.as_u16() =>
        {
            Ok(())
        }
        other => Err(format!("mutation was not fail-closed: {other:?}").into()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN pointing at a built agentbrokerd"]
// One sequential scenario over a single real cluster; phases share live state.
#[allow(clippy::too_many_lines)]
async fn http_sessions_over_real_three_node_cluster() -> TestResult {
    let mut cluster = Cluster::start().await?;
    let config = cluster.orchestration_config()?;
    let root = cluster.root.join("http");
    let data_root = root.join("orchestration");
    std::fs::create_dir_all(&data_root)?;
    let (bearer, principal) = issue_bearer(&root)?;

    // --- A. normal path ---------------------------------------------------
    let backend = Backend::start(&data_root, config, principal, &bearer).await?;
    let client = &backend.client;
    let first_request = Uuid::now_v7();
    let opened = client
        .open_session(&bearer, &open_request(first_request))
        .await?;
    assert!(opened.created);
    assert_eq!(opened.session.request_id, first_request);
    assert_eq!(opened.session.state, "open");
    let session = opened.session.clone();

    let by_request = client
        .session_open_readback(&bearer, first_request)
        .await?
        .ok_or("durable readback by request id was empty")?;
    assert_eq!(by_request.session_id, session.session_id);
    assert_eq!(by_request.revision, session.revision);
    assert_eq!(
        client.session(&bearer, session.session_id).await?.state,
        "open"
    );

    let replay = client
        .open_session(&bearer, &open_request(first_request))
        .await?;
    assert!(!replay.created, "same request id must not create again");
    assert_eq!(replay.session.session_id, session.session_id);

    // --- B. quorum loss: mutations fail closed, nothing durable ------------
    let survivors: Vec<u64> = cluster.children.keys().copied().collect();
    cluster.stop_node(survivors[0])?;
    cluster.stop_node(survivors[1])?;
    let outage_request = Uuid::now_v7();
    assert_blocked_open(
        client
            .open_session(&bearer, &open_request(outage_request))
            .await,
        outage_request,
    )?;
    assert!(
        client
            .session_open_readback(&bearer, outage_request)
            .await?
            .is_none(),
        "no durable Session may exist for a Broker-unconfirmed request"
    );
    let blocked_close = client
        .close_session(
            &bearer,
            session.session_id,
            &AgentSessionCloseRequest {
                expected_revision: session.revision,
            },
        )
        .await;
    assert!(
        blocked_close.is_err(),
        "close must not succeed without Broker quorum: {blocked_close:?}"
    );
    assert_eq!(
        client.session(&bearer, session.session_id).await?.state,
        "open",
        "failed close must leave the Session open"
    );

    // --- C. recovery: read durable state first, then safe resume -----------
    cluster.start_node(survivors[0])?;
    cluster.start_node(survivors[1])?;
    cluster.wait_for_raft_listener(survivors[0]).await?;
    cluster.wait_for_raft_listener(survivors[1]).await?;
    cluster.wait_for_leader().await?;
    let recovered_readback = client
        .session_open_readback(&bearer, outage_request)
        .await?;
    assert!(recovered_readback.is_none());
    // Readback proved the original request never committed, so one resume with
    // the *same* request id is safe and idempotent.
    let resumed = client
        .open_session(&bearer, &open_request(outage_request))
        .await?;
    assert!(resumed.created);
    assert_eq!(resumed.session.request_id, outage_request);
    let resumed_again = client
        .open_session(&bearer, &open_request(outage_request))
        .await?;
    assert!(!resumed_again.created);
    assert_eq!(resumed_again.session.session_id, resumed.session.session_id);

    let closed = client
        .close_session(
            &bearer,
            session.session_id,
            &AgentSessionCloseRequest {
                expected_revision: session.revision,
            },
        )
        .await?;
    assert_eq!(closed.state, "closed");
    assert_eq!(closed.revision, session.revision + 1);
    let closed_readback = client.session(&bearer, session.session_id).await?;
    assert_eq!(closed_readback.state, "closed");
    assert_eq!(closed_readback.revision, closed.revision);

    // --- D. restart on the same data root ---------------------------------
    let stale_state = root.join("previous-incarnation-owner.json");
    std::fs::copy(data_root.join("broker-command-session.json"), &stale_state)?;
    backend.stop().await?;
    let restarted = Backend::start(&data_root, config, principal, &bearer).await?;
    let client = &restarted.client;
    let survived = client.session(&bearer, session.session_id).await?;
    assert_eq!(survived.session_id, closed_readback.session_id);
    assert_eq!(survived.state, "closed");
    assert_eq!(survived.revision, closed_readback.revision);
    assert_eq!(survived.updated_at_ms, closed_readback.updated_at_ms);
    let resumed_survives = client
        .session_open_readback(&bearer, outage_request)
        .await?
        .ok_or("open Session lost across restart")?;
    assert_eq!(resumed_survives.session_id, resumed.session.session_id);
    assert_eq!(resumed_survives.state, "open");
    // New process passes Broker owner validation for a fresh mutation.
    let post_restart = Uuid::now_v7();
    assert!(
        client
            .open_session(&bearer, &open_request(post_restart))
            .await?
            .created
    );

    // --- E. previous incarnation is fenced --------------------------------
    // The durable state of the previous process incarnation (epoch N) cannot
    // acquire or mutate once the restarted process owns epoch N+1.
    let runtime_destination = destination_id(&data_root)?;
    let mut zombie = BrokerOwnerAuthority::new_with_cluster(
        &stale_state,
        runtime_destination,
        Uuid::now_v7(),
        config,
    )?;
    assert!(matches!(
        zombie.ensure_current_owner().await,
        Err(BrokerOwnerAuthorityError::Stale | BrokerOwnerAuthorityError::Rejected)
    ));
    // The real owner is unaffected and still admits mutations.
    assert!(
        client
            .open_session(&bearer, &open_request(Uuid::now_v7()))
            .await?
            .created
    );
    restarted.stop().await?;
    Ok(())
}

fn destination_id(data_root: &Path) -> Result<Uuid, Box<dyn std::error::Error>> {
    // Read-only open: no owner lock, no mutation.
    let runtime = OrchestrationRuntime::open_existing_operations_read_only(data_root)?;
    Ok(runtime.destination_id())
}

/// Client-side deadlines that expire while the server may still commit create
/// genuinely ambiguous outcomes. Durable readback by the original request id
/// then decides; the same id is never turned into a second Session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN pointing at a built agentbrokerd"]
async fn ambiguous_client_deadline_never_duplicates_a_session() -> TestResult {
    let cluster = Cluster::start().await?;
    let config = cluster.orchestration_config()?;
    let root = cluster.root.join("ambiguity");
    let data_root = root.join("orchestration");
    std::fs::create_dir_all(&data_root)?;
    let (bearer, principal) = issue_bearer(&root)?;
    let backend = Backend::start(&data_root, config, principal, &bearer).await?;
    let impatient = OrchestrationHttpClient::new(backend.address, Duration::from_millis(1))?;
    for _ in 0..8 {
        let request_id = Uuid::now_v7();
        let attempt = impatient
            .open_session(&bearer, &open_request(request_id))
            .await;
        eprintln!("impatient attempt outcome: {attempt:?}");
        let committed = backend
            .client
            .session_open_readback(&bearer, request_id)
            .await?;
        // Resolve by the original id only after readback; never a new id.
        let resolved = backend
            .client
            .open_session(&bearer, &open_request(request_id))
            .await?;
        assert_eq!(resolved.session.request_id, request_id);
        if let Some(existing) = committed {
            assert!(!resolved.created, "committed request was applied twice");
            assert_eq!(resolved.session.session_id, existing.session_id);
        }
        let replay = backend
            .client
            .open_session(&bearer, &open_request(request_id))
            .await?;
        assert!(!replay.created);
        assert_eq!(replay.session.session_id, resolved.session.session_id);
    }
    backend.stop().await
}
