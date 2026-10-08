//! Real loopback Forge-to-Orchestration bootstrap over isolated source stores.
//! This proves import transport and startup, not provider or Team cooperation.

use std::path::{Path, PathBuf};
use std::time::Duration;

use forge_backend::http::{AppState, BackendClients};
use forge_backend::{BackendConfig as ForgeConfig, ForgeHttpClient};
use forge_control::{
    AgentOperationsFreezeReadRequest, AgentOperationsFreezeReleaseRequest,
    AgentOperationsFreezeRequest, ForgeApprovalCoordinator, ForgeControl, ForgeControlWorker,
    ForgeExecutionCoordinator, ResolvedWorkspaceScope,
};
use forge_domain::{
    AuthenticatedPrincipal, DirectTokenId, EstablishWorkspace, ForgeBackendId, LocationId,
    LocationRevision, OrchestrationIssuerId, TimestampMillis, WorkspaceId, WorkspaceRoot,
};
use forge_persistence::{AgentOperationsFreezeState, GlobalRegistry, WorkspaceStore};
use forge_workspace::{LocationService, WorkspaceStateLayout};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
    McpTokenWorker,
};
use heterarchy_orchestration_backend::client::{ClientError, OrchestrationHttpClient};
use heterarchy_orchestration_backend::forge_commands::ForgeCommandHttpClient;
use heterarchy_orchestration_backend::forge_source::ForgeSourceHttpClient;
use heterarchy_orchestration_backend::wire::WorkspaceQueryScope;
use heterarchy_orchestration_backend::wire::operations_import::{
    ImportPhaseWire, ImportPrepareRequest, ImportProgressWire, ImportStepRequest,
};
use heterarchy_orchestration_backend::{
    AdmissionIssuerBootstrap, BackendConfig, CoordinatorAuthority, TokenAuthorityBootstrap, serve,
};
use heterarchy_orchestration_runtime::OrchestrationRuntime;
use heterarchy_orchestration_runtime::access::OperatorPolicy;
use hyper::StatusCode;
use hyper::header::HeaderValue;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const FORGE_SECRET: &str = "isolated-bootstrap-http-fixture";
const DEADLINE: Duration = Duration::from_secs(5);

struct ForgeHost {
    state: AppState,
    worker: ForgeControlWorker,
    leases: ForgeControlWorker,
    execution: ForgeExecutionCoordinator,
    approval: ForgeApprovalCoordinator,
    scope: ResolvedWorkspaceScope,
    client: ForgeHttpClient,
    address: std::net::SocketAddr,
}

fn establish_source(root: &Path) -> TestResult<ResolvedWorkspaceScope> {
    let location_root = root.join("source");
    let workspace_root = location_root.join("workspace");
    std::fs::create_dir_all(&workspace_root)?;
    let registry_path = root.join("registry.sqlite3");
    let mut locations = LocationService::new(GlobalRegistry::open(&registry_path)?);
    let location = locations.register_selected_path(
        LocationId::from_uuid(Uuid::now_v7()),
        "bootstrap HTTP fixture",
        &location_root,
        TimestampMillis::new(1)?,
    )?;
    let layout = WorkspaceStateLayout::initialize(&workspace_root)?;
    let command = EstablishWorkspace::new(
        WorkspaceId::from_uuid(Uuid::now_v7()),
        location.id(),
        WorkspaceRoot::from_canonical(layout.workspace_root().to_path_buf())?,
        layout.root_fingerprint()?,
        TimestampMillis::new(2)?,
    );
    drop(WorkspaceStore::open(&layout, &command)?);
    drop(locations);
    let control = ForgeControl::new(GlobalRegistry::open_existing(&registry_path)?);
    let locator = control
        .workspace_discover(location.id(), LocationRevision::INITIAL)?
        .into_iter()
        .find(|candidate| candidate.display_name() == "workspace")
        .ok_or("fixture Workspace was not discovered")?
        .locator()
        .clone();
    Ok(control.resolve_workspace(location.id(), LocationRevision::INITIAL, &locator)?)
}

impl ForgeHost {
    async fn start(
        root: &Path,
        backend: ForgeBackendId,
        issuer: OrchestrationIssuerId,
        destination: Uuid,
    ) -> TestResult<Self> {
        let scope = establish_source(root)?;
        let registry_path = root.join("registry.sqlite3");
        let (control, worker) = ForgeControlWorker::start(ForgeControl::new(
            GlobalRegistry::open_existing(&registry_path)?,
        ))?;
        let execution = ForgeExecutionCoordinator::start(control.clone(), 2)?;
        let approval = ForgeApprovalCoordinator::start(control.clone(), execution.client())?;
        let (lease_client, leases) = ForgeControlWorker::start_activity_lease(ForgeControl::new(
            GlobalRegistry::open_existing(&registry_path)?,
        ))?;
        let product_cache = root.join("product-cache");
        std::fs::create_dir(&product_cache)?;
        let execution_client = execution.client();
        let approval_client = approval.client();
        let state = forge_backend::http::assemble(
            ForgeConfig::new(
                std::net::Ipv4Addr::LOCALHOST.into(),
                0,
                FORGE_SECRET,
                backend,
                issuer,
            )?,
            BackendClients {
                control: &control,
                execution: &execution_client,
                approval: &approval_client,
                activity_leases: &lease_client,
                runtime_instance_id: Uuid::now_v7(),
                product_cache_root: &product_cache,
                destination_id: destination,
            },
        )?;
        let address = forge_backend::http::serve(state.clone()).await?;
        let client = ForgeHttpClient::new(address, FORGE_SECRET, backend, destination, DEADLINE)?;
        Ok(Self {
            state,
            worker,
            leases,
            execution,
            approval,
            scope,
            client,
            address,
        })
    }

    async fn shutdown(mut self) -> TestResult {
        let report = self.state.shutdown(DEADLINE).await;
        let approval = self.approval.shutdown();
        let execution = self.execution.shutdown();
        let leases = self.leases.shutdown();
        let worker = self.worker.shutdown();
        assert!(report.is_successful());
        let _ = approval?;
        let _ = execution?;
        leases?;
        worker?;
        Ok(())
    }
}

fn token_authority(
    root: &Path,
) -> TestResult<(McpTokenRuntime, HeaderValue, AuthenticatedPrincipal)> {
    let catalog = McpTokenCatalog::open(root.join("operator-tokens.sqlite3"))?;
    let (client, worker) = McpTokenWorker::start(McpTokenService::new([0x67; 32], catalog))?;
    let issued = client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("isolated bootstrap operator".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let mut bearer = HeaderValue::from_str(&format!("Bearer {}", issued.reveal().as_str()))?;
    bearer.set_sensitive(true);
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
        issued.metadata().id().into_uuid(),
    ));
    Ok((
        McpTokenRuntime::from_parts(client, worker),
        bearer,
        principal,
    ))
}

fn scope_wire(scope: &ResolvedWorkspaceScope) -> WorkspaceQueryScope {
    WorkspaceQueryScope {
        workspace_id: scope.workspace_id().into_uuid(),
        workspace_revision: scope.workspace_revision().get(),
        location_id: scope.locator().location_id().into_uuid(),
        location_revision: scope.locator().expected_revision().get(),
        locator: scope.locator().encode(),
    }
}

fn step_request(
    prepare: &ImportPrepareRequest,
    progress: &ImportProgressWire,
) -> ImportStepRequest {
    ImportStepRequest {
        scope: prepare.scope.clone(),
        destination_id: prepare.destination_id,
        expected_pages: progress.page_count,
        expected_verified_pages: progress.verified_pages,
    }
}

async fn bootstrap_flow(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    prepare: &ImportPrepareRequest,
) -> TestResult {
    let mut progress = client.import_prepare(bearer, prepare).await?;
    assert_eq!(progress.phase, ImportPhaseWire::Importing);
    assert_eq!(progress.page_count, 0);
    let first_request = step_request(prepare, &progress);
    progress = client
        .import_next_page(bearer, prepare.migration_id, &first_request)
        .await?;
    assert!(
        matches!(client.import_next_page(bearer, prepare.migration_id, &first_request).await,
        Err(ClientError::Rejected { status, .. }) if status == StatusCode::CONFLICT.as_u16())
    );
    assert_eq!(
        client.import_progress(bearer, prepare.migration_id).await?,
        Some(progress.clone())
    );
    // Fresh source has no historical Agent operations. All five table families
    // still cross the real source HTTP and independent destination verification.
    for _ in 1..5 {
        progress = client
            .import_next_page(
                bearer,
                prepare.migration_id,
                &step_request(prepare, &progress),
            )
            .await?;
    }
    assert_eq!(progress.phase, ImportPhaseWire::Imported);
    assert_eq!(progress.page_count, 5);
    assert_eq!(progress.record_count, 0);
    assert_eq!(client.import_prepare(bearer, prepare).await?, progress);
    for _ in 0..5 {
        progress = client
            .import_verify_next_page(
                bearer,
                prepare.migration_id,
                &step_request(prepare, &progress),
            )
            .await?;
    }
    assert_eq!(progress.phase, ImportPhaseWire::Verified);
    assert_eq!(progress.import_digest, progress.verify_digest);
    assert_eq!(progress.verified_pages, progress.page_count);
    assert_eq!(client.import_prepare(bearer, prepare).await?, progress);
    let aborted = client
        .import_abort(
            bearer,
            prepare.migration_id,
            &step_request(prepare, &progress),
        )
        .await?;
    assert_eq!(aborted.phase, ImportPhaseWire::Aborted);
    assert_eq!(client.import_prepare(bearer, prepare).await?, aborted);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_http_bootstrap_verifies_then_aborts_without_native_source_fallback() -> TestResult {
    let root = std::env::temp_dir().join(format!("cooperation-bootstrap-http-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let data_root: PathBuf = root.join("orchestration");
    std::fs::create_dir(&data_root)?;
    let runtime = OrchestrationRuntime::open(&data_root)?;
    let destination = runtime.destination_id();
    drop(runtime);
    let backend = ForgeBackendId::from_uuid(Uuid::now_v7());
    let issuer = OrchestrationIssuerId::from_uuid(Uuid::now_v7());
    let source = ForgeHost::start(&root, backend, issuer, destination).await?;
    let migration_id = Uuid::now_v7();
    let freeze = source
        .client
        .agent_operations_freeze(&AgentOperationsFreezeRequest {
            scope: source.scope.clone(),
            migration_id,
            destination_id: destination,
            observed_at: TimestampMillis::new(10)?,
        })
        .await?;
    let prepare = ImportPrepareRequest {
        scope: scope_wire(&source.scope),
        migration_id,
        destination_id: destination,
    };
    let authority = CoordinatorAuthority::from_http(
        ForgeSourceHttpClient::new(ForgeCommandHttpClient::new(
            source.address,
            FORGE_SECRET,
            DEADLINE,
        )?),
        vec![source.scope.clone()],
    )?
    .with_effect_facts(source.client.clone());
    let (token, bearer, principal) = token_authority(&root)?;
    let reserved = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = reserved.local_addr()?;
    drop(reserved);
    let config = BackendConfig::new(address, data_root.clone())?
        .with_token_authority(TokenAuthorityBootstrap::Runtime(token))
        .with_operator_policy(OperatorPolicy::new(vec![principal])?)
        .with_coordinator_authority(authority)
        .with_admission_issuer(AdmissionIssuerBootstrap::new(issuer, backend, 300_000)?);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(serve(config, cancellation.clone()));
    let client = OrchestrationHttpClient::new(address, DEADLINE)?;
    let result: TestResult = async {
        tokio::time::timeout(DEADLINE, async {
            loop {
                match client.import_progress(&bearer, migration_id).await {
                    Ok(None) => return Ok::<(), ClientError>(()),
                    Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                    Ok(Some(_)) => return Err(ClientError::Protocol),
                    Err(error) => return Err(error),
                }
            }
        })
        .await??;
        bootstrap_flow(&client, &bearer, &prepare).await?;
        let released = source
            .client
            .agent_operations_freeze_release(&AgentOperationsFreezeReleaseRequest {
                scope: source.scope.clone(),
                migration_id,
                destination_id: destination,
                expected_revision: freeze.revision(),
                observed_at: TimestampMillis::new(11)?,
            })
            .await?;
        assert_eq!(released.state(), AgentOperationsFreezeState::Released);
        assert_eq!(
            source
                .client
                .agent_operations_freeze_read(&AgentOperationsFreezeReadRequest {
                    scope: source.scope.clone(),
                    migration_id,
                    destination_id: destination,
                })
                .await?,
            Some(released)
        );
        Ok(())
    }
    .await;
    cancellation.cancel();
    let stopped = tokio::time::timeout(DEADLINE, server).await;
    let source_stopped = source.shutdown().await;
    stopped???;
    source_stopped?;
    let catalog = OrchestrationRuntime::open_existing_operations_read_only(&data_root)?;
    let configured = catalog
        .admission_issuer_config()?
        .ok_or("issuer not initialized")?;
    assert_eq!(configured.issuer(), issuer);
    assert_eq!(configured.backend_id(), backend);
    drop(catalog);
    std::fs::remove_dir_all(root)?;
    result
}
