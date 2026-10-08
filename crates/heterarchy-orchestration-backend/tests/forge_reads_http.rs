//! Real-HTTP fixture for the Forge source read surface.
//!
//! One process runs the production `serve_with_runtime` backend (bearer
//! auth, operator grant, configured Forge HTTP source authority) against a
//! real loopback Forge command stub speaking the exact wire envelope; the
//! official `OrchestrationHttpClient` drives every converted read. The
//! fixture proves parity of the typed projections, bearer/membership
//! denial, stale-scope rejection, and fail-closed Source loss.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use forge_control::WorkspaceLocator;
use forge_domain::{LocationId, LocationRevision};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenService, McpTokenWorker,
};
use heterarchy_orchestration_backend::client::{ClientError, OrchestrationHttpClient, ServiceCode};
use heterarchy_orchestration_backend::{
    forge_source_config, serve_with_runtime, wire::WorkspaceQueryScope,
};
use heterarchy_orchestration_runtime::{OrchestrationRuntime, access::OperatorPolicy};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const OPERATOR_SECRET: [u8; 32] = [0xA7; 32];
const FORGE_BEARER_SECRET: &str = "forge-reads-fixture-secret";
const WORKSPACE_ID: Uuid = Uuid::from_u128(0xA801);
const LOCATION_ID: Uuid = Uuid::from_u128(0xA802);
const ACTIVITY_ID: Uuid = Uuid::from_u128(0xA803);
const GOAL_ID: Uuid = Uuid::from_u128(0xA804);
const DIFF_ID: Uuid = Uuid::from_u128(0xA807);
const CHANGE_SET_ID: Uuid = Uuid::from_u128(0xA808);
const LEASE_ID: Uuid = Uuid::from_u128(0xA809);
const AGENT_ID: Uuid = Uuid::from_u128(0xA80A);
const LOCATION_REVISION: i64 = 5;
const WORKSPACE_REVISION: i64 = 8;
const RELATIVE_ROOT: &str = "demo-ws";

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The exact trusted scope the fixture's source authority is configured
/// with.
fn fixture_scope() -> Value {
    json!({
        "workspace_id": WORKSPACE_ID,
        "workspace_revision": WORKSPACE_REVISION,
        "locator": {
            "location_id": LOCATION_ID,
            "expected_revision": LOCATION_REVISION,
            "relative_root": RELATIVE_ROOT,
        },
    })
}

fn request_scope() -> Result<WorkspaceQueryScope, Box<dyn std::error::Error>> {
    Ok(WorkspaceQueryScope {
        workspace_id: WORKSPACE_ID,
        location_id: LOCATION_ID,
        location_revision: LOCATION_REVISION,
        workspace_revision: WORKSPACE_REVISION,
        locator: root_locator()?.encode(),
    })
}

/// The Location root locator ("."), which is what `workspace.list`
/// entries must sit under.
fn location_root_locator() -> Result<WorkspaceLocator, Box<dyn std::error::Error>> {
    Ok(WorkspaceLocator::new(
        LocationId::from_uuid(LOCATION_ID),
        LocationRevision::new(LOCATION_REVISION)?,
        std::path::Path::new("."),
    )?)
}

fn root_locator() -> Result<WorkspaceLocator, Box<dyn std::error::Error>> {
    Ok(WorkspaceLocator::new(
        LocationId::from_uuid(LOCATION_ID),
        LocationRevision::new(LOCATION_REVISION)?,
        std::path::Path::new(RELATIVE_ROOT),
    )?)
}

fn child_locator() -> Result<WorkspaceLocator, Box<dyn std::error::Error>> {
    Ok(WorkspaceLocator::new(
        LocationId::from_uuid(LOCATION_ID),
        LocationRevision::new(LOCATION_REVISION)?,
        std::path::Path::new("alpha"),
    )?)
}

#[derive(Default)]
struct StubState {
    captured: Mutex<Vec<(String, Value)>>,
    /// Per-op canned results; `workspace.resolve` can be swapped at runtime
    /// to prove the stale path.
    results: Mutex<BTreeMap<&'static str, Value>>,
}

impl StubState {
    fn set(&self, op: &'static str, result: Value) {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(op, result);
    }

    fn captured(&self) -> Vec<(String, Value)> {
        self.captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Accept loop for the stub Forge command server: one request per
/// connection, envelope-verified replies built from the canned result for
/// the requested op.
async fn serve_stub(listener: tokio::net::TcpListener, state: std::sync::Arc<StubState>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_stub_connection(&mut socket, &state).await {
                eprintln!("DEBUG stub connection ended: {error}");
            }
        });
    }
}

async fn handle_stub_connection(
    socket: &mut tokio::net::TcpStream,
    state: &StubState,
) -> Result<(), std::io::Error> {
    {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        let (header_end, body_start, body_length) = loop {
            let Ok(count) = socket.read(&mut chunk).await else {
                return Ok(());
            };
            if count == 0 {
                return Ok(());
            }
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.len() > 256 * 1024 {
                return Ok(());
            }
            if let Some(position) = find_header_end(&bytes) {
                let header_text = String::from_utf8_lossy(&bytes[..position]);
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                let body_start = position + 4;
                if bytes.len() >= body_start + content_length {
                    break (position, body_start, content_length);
                }
            }
        };
        let Ok(body) =
            serde_json::from_slice::<Value>(&bytes[body_start..body_start + body_length])
        else {
            return Ok(());
        };
        let request_line = String::from_utf8_lossy(&bytes[..header_end]);
        let Some(path) = request_line
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .map(str::to_owned)
        else {
            return Ok(());
        };
        state
            .captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((path.clone(), body.clone()));
        let op = path.rsplit('/').next().unwrap_or_default().to_owned();
        let result = state
            .results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(op.as_str())
            .cloned();
        let (status, response) = match result {
            Some(result) => (
                200,
                json!({
                    "forge_api": body["forge_api"],
                    "request_id": body["request_id"],
                    "ok": true,
                    "result": result,
                }),
            ),
            None => (
                503,
                json!({
                    "forge_api": body["forge_api"],
                    "request_id": body["request_id"],
                    "ok": false,
                    "error": {
                        "code": "SOURCE_UNAVAILABLE",
                        "message": "stub op is not configured",
                        "category": "scope",
                        "retryable": true,
                        "recovery_action": "retry_later",
                        "recovery_hint": "",
                        "readback": null,
                    },
                }),
            ),
        };
        let reason = match status {
            200 => "OK",
            _ => "Service Unavailable",
        };
        let response_body = response.to_string();
        let response_text = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
            response_body.len(),
        );
        let _ = socket.write_all(response_text.as_bytes()).await;
        let _ = socket.shutdown().await;
        Ok(())
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

struct Fixture {
    client: OrchestrationHttpClient,
    /// Keeps the auth workers alive for the whole fixture lifetime; their
    /// concrete types are intentionally erased.
    #[expect(dead_code)]
    keepalive: Vec<Box<dyn std::any::Any + Send + Sync>>,
    stub: std::sync::Arc<StubState>,
    stub_task: tokio::task::JoinHandle<()>,
    operator_bearer: hyper::header::HeaderValue,
    foreign_bearer: hyper::header::HeaderValue,
    server: tokio::task::JoinHandle<Result<(), heterarchy_orchestration_backend::BackendError>>,
    cancellation: CancellationToken,
    _dir: tempfile_guard::TempDir,
}

mod tempfile_guard {
    use std::path::PathBuf;

    pub(super) struct TempDir(pub PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    pub(super) fn create(label: &str) -> std::io::Result<TempDir> {
        let path = std::env::temp_dir().join(format!(
            "{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(TempDir(path))
    }
}

struct AuthFixture {
    token_client: heterarchy_orchestration_auth::McpTokenClient,
    operator_bearer: hyper::header::HeaderValue,
    foreign_bearer: hyper::header::HeaderValue,
    operator_principal: forge_domain::AuthenticatedPrincipal,
    workers: Vec<Box<dyn std::any::Any + Send + Sync>>,
}

fn start_auth(auth_root: &std::path::Path) -> Result<AuthFixture, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(auth_root)?;
    let token_catalog = McpTokenCatalog::open(auth_root.join("tokens.sqlite3"))?;
    let (token_client, token_worker) =
        McpTokenWorker::start(McpTokenService::new(OPERATOR_SECRET, token_catalog))?;
    let operator_token = token_client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("forge reads operator".to_owned())?,
        forge_domain::TimestampMillis::new(1)?,
    ))?;
    let operator_bearer = hyper::header::HeaderValue::from_str(&format!(
        "Bearer {}",
        operator_token.reveal().as_str()
    ))?;
    let operator_principal = forge_domain::AuthenticatedPrincipal::DirectToken(
        forge_domain::DirectTokenId::from_uuid(operator_token.metadata().id().into_uuid()),
    );
    let foreign_token = token_client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("forge reads foreign".to_owned())?,
        forge_domain::TimestampMillis::new(2)?,
    ))?;
    let foreign_bearer = hyper::header::HeaderValue::from_str(&format!(
        "Bearer {}",
        foreign_token.reveal().as_str()
    ))?;
    Ok(AuthFixture {
        token_client,
        operator_bearer,
        foreign_bearer,
        operator_principal,
        workers: vec![Box::new(token_worker)],
    })
}

async fn start_fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    let dir = tempfile_guard::create("forge-reads-http")?;
    let root = &dir.0;

    let auth = start_auth(&root.join("auth"))?;
    let AuthFixture {
        token_client,
        operator_bearer,
        foreign_bearer,
        operator_principal,
        workers,
    } = auth;
    let runtime_root = root.join("runtime");
    std::fs::create_dir_all(&runtime_root)?;
    let runtime = OrchestrationRuntime::open(runtime_root.as_path())?
        .with_operator_policy(OperatorPolicy::new(vec![operator_principal])?);

    let location_root = location_root_locator()?.encode();
    let child = child_locator()?.encode();
    let stub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let forge_address = stub_listener.local_addr()?;
    let stub = std::sync::Arc::new(StubState::default());
    seed_stub_results(&stub, &location_root, &child);

    let stub_task = tokio::spawn(serve_stub(stub_listener, stub.clone()));

    let authority = write_source_config(root, forge_address)?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(serve_with_runtime(
        std::sync::Arc::new(runtime),
        address,
        token_client.clone(),
        Some(authority),
        cancellation.clone(),
    ));
    // Wait for the backend to accept connections.
    let client = OrchestrationHttpClient::new(address, Duration::from_secs(5))?;
    for _ in 0..100 {
        if let Ok(Ok(_)) = tokio::time::timeout(
            Duration::from_millis(200),
            tokio::net::TcpStream::connect(address),
        )
        .await
        {
            break;
        }
    }

    Ok(Fixture {
        client,
        keepalive: workers,
        stub,
        stub_task,
        operator_bearer,
        foreign_bearer,
        server,
        cancellation,
        _dir: dir,
    })
}

fn goal_row() -> Value {
    json!({
        "goal_id": GOAL_ID,
        "workspace_id": WORKSPACE_ID,
        "revision": 3,
        "status": {"kind": "active", "reason": null},
        "objective": "Ship the slice",
        "success_criteria": ["tests pass"],
        "constraints": [],
        "out_of_scope": [],
        "verification_requirements": [
            {"kind": "execution_succeeded", "description": "ci green"}
        ],
        "stop_conditions": [],
        "created_at_ms": 10,
        "updated_at_ms": 20,
    })
}

fn dag_row() -> Value {
    json!({
        "goal_id": GOAL_ID,
        "revision_id": Uuid::from_u128(0xA805),
        "base_goal_revision": 2,
        "dag_revision": 4,
        "parent_revision": 3,
        "created_at_ms": 30,
        "head_revision": 6,
        "head_updated_at_ms": 40,
        "nodes": [
            {"node_id": Uuid::from_u128(0xA806), "ordinal": 0, "title": "Implement",
             "objective": "Write it", "acceptance": ["done"], "lifecycle": "active",
             "status": "ready", "waiting_on": [], "revision": 2, "evidence_count": 1,
             "created_at_ms": 31, "updated_at_ms": 32}
        ],
        "edges": [],
    })
}

/// File-family readbacks echo the request's own scope and activity.
fn file_list_result(request: &Value) -> Value {
    let scope = request["payload"]["scope"].clone();
    let activity = request["payload"]["activity"]["activity_id"].clone();
    json!({
        "scope": scope,
        "activity_id": activity,
        "directory_base64": null,
        "directory_display_path": null,
        "directory_display_path_truncated": false,
        "entries": [
            {"path_base64": "bGliLnJz", "display_path": "lib.rs",
             "display_path_truncated": false, "kind": "regular_file", "size_bytes": 12}
        ],
        "scanned_entries": 3,
        "truncated": false,
    })
}

/// Seed the stub's canned per-op results with the exact readback shapes the
/// codecs and clients accept.
fn seed_stub_results(stub: &StubState, location_root: &str, child: &str) {
    seed_stub_stage4(stub);

    stub.set(
        "location.list",
        json!({"entries": [
            {"location_id": LOCATION_ID, "display_name": "Demo Location", "revision": LOCATION_REVISION, "state": "enabled"}
        ]}),
    );
    stub.set(
        "workspace.list",
        json!({"entries": [
            {"locator": location_root, "display_name": "Demo Workspace",
             "workspace_id": WORKSPACE_ID, "workspace_revision": WORKSPACE_REVISION}
        ]}),
    );
    stub.set(
        "workspace.discover",
        json!({"entries": [
            {"locator": child, "kind": "direct_child", "display_name": "alpha",
             "signals": {"present": ["cargo_workspace"]}, "disposition": "candidate",
             "reason": "cargo_workspace"}
        ]}),
    );
    stub.set(
        "workspace.resolve",
        json!({"workspace_id": WORKSPACE_ID, "workspace_revision": WORKSPACE_REVISION}),
    );
    stub.set(
        "activity.get",
        json!({
            "scope": fixture_scope(),
            "activity_id": ACTIVITY_ID,
            "found": true,
            "activity": {
                "activity_id": ACTIVITY_ID,
                "workspace_id": WORKSPACE_ID,
                "state": "open",
                "revision": 2,
                "created_at_ms": 1,
                "updated_at_ms": 2,
                "lease_owner_id": null,
                "lease_until_ms": null,
                "lease_revision": 1,
            },
            "retry_authorized": false,
            "request_equivalence_proven": false,
        }),
    );
}

/// Stage-4 (`command.list`/`diff.read`/writer-lease) canned results.
fn seed_stub_stage4(stub: &StubState) {
    stub.set(
        "command.list",
        json!({
            "activity_id": ACTIVITY_ID,
            "catalog_fingerprint": "00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff",
            "commands": [
                {"id": "cmd-verify", "class": "verify"},
                {"id": "cmd-pkg", "class": "generated_package"}
            ],
        }),
    );
    stub.set(
        "diff.read",
        json!({
            "scope": fixture_scope(),
            "diff": {
                "diff_id": DIFF_ID,
                "workspace_id": WORKSPACE_ID,
                "activity_id": ACTIVITY_ID,
                "change_set_id": CHANGE_SET_ID,
                "origin": "tree_change_set",
                "origin_mutation_id": null,
                "entries": [
                    {"subject": "file", "primary_path_base64": "bGliLnJz",
                     "secondary_path_base64": null, "change_kind": "modified",
                     "additions": 4, "deletions": 1}
                ],
                "state": "ready",
                "render_kind": "text",
                "compressed_hash": null,
                "uncompressed_hash": "blake3-256:abcd",
                "compressed_size_bytes": null,
                "uncompressed_size_bytes": 120,
                "renderer_version": 3,
                "redaction_policy_version": 1,
                "created_at_ms": 10,
                "updated_at_ms": 20,
                "revision": 2,
            },
            "health": "available",
            "content": "diff text",
        }),
    );
    stub.set(
        "writer_lease.get",
        json!({
            "writer_lease_id": LEASE_ID,
            "owner_activity_id": ACTIVITY_ID,
            "agent_id": AGENT_ID,
            "owner_session_id": null,
            "revision": 6,
            "lease_until_ms": 200,
            "observed_at_ms": 100,
            "state": "active",
            "status": "expired_recovery_required",
            "total_scope_count": 3,
            "scope_offset": 2,
            "scopes": [{"kind": "exact_path", "path_base64": "bGliLnJz"}],
        }),
    );
    stub.set(
        "writer_lease.list",
        json!({
            "leases": [
                {"writer_lease_id": LEASE_ID, "owner_activity_id": ACTIVITY_ID,
                 "agent_id": AGENT_ID, "owner_session_id": null, "revision": 6,
                 "lease_until_ms": 200, "observed_at_ms": 100, "state": "active",
                 "status": "active", "total_scope_count": 1, "scope_offset": 0,
                 "scopes": [{"kind": "directory_subtree", "path_base64": "YWFh"}]}
            ],
            "next_cursor": null,
        }),
    );
}

/// Write the host-trusted source configuration beside the fixture root and
/// load the production authority from it.
fn write_source_config(
    root: &std::path::Path,
    forge_address: std::net::SocketAddr,
) -> Result<heterarchy_orchestration_backend::CoordinatorAuthority, Box<dyn std::error::Error>> {
    let bearer_file = root.join("forge-http.bearer");
    std::fs::write(&bearer_file, format!("{FORGE_BEARER_SECRET}\n"))?;
    std::fs::write(
        root.join("forge-source.json"),
        serde_json::to_vec(&json!({
            "address": forge_address,
            "bearer_file": bearer_file,
            "scopes": [fixture_scope()],
        }))?,
    )?;
    forge_source_config::load(&root.join("forge-source.json"))
        .map_err(|error| format!("source config load failed: {error}").into())
}

#[tokio::test]
async fn session_open_nonce_readback_is_principal_bound_and_terminal_safe() -> TestResult {
    let fixture = start_fixture().await?;
    let request_id = Uuid::now_v7();
    let opened = fixture
        .client
        .open_session(
            &fixture.operator_bearer,
            &heterarchy_orchestration_backend::wire::AgentSessionOpenRequest {
                request_id: request_id.to_string(),
            },
        )
        .await?;
    let observed = fixture
        .client
        .session_open_readback(&fixture.operator_bearer, request_id)
        .await?
        .ok_or("opened Session nonce was not readable")?;
    assert_eq!(observed.session_id, opened.session.session_id);
    assert_eq!(observed.request_id, request_id);
    assert!(matches!(
        fixture
            .client
            .session_open_readback(&fixture.foreign_bearer, request_id)
            .await,
        Err(ClientError::Rejected {
            status: 403,
            code: ServiceCode::Forbidden
        })
    ));
    assert!(
        fixture
            .client
            .session_open_readback(&fixture.operator_bearer, Uuid::now_v7())
            .await?
            .is_none()
    );
    let closed = fixture
        .client
        .close_session(
            &fixture.operator_bearer,
            observed.session_id,
            &heterarchy_orchestration_backend::wire::AgentSessionCloseRequest {
                expected_revision: observed.revision,
            },
        )
        .await?;
    assert_eq!(closed.state, "closed");
    assert_eq!(
        fixture
            .client
            .session_open_readback(&fixture.operator_bearer, request_id)
            .await?
            .ok_or("terminal Session nonce was not readable")?
            .state,
        "closed"
    );

    fixture.stub_task.abort();
    fixture.cancellation.cancel();
    let _ = fixture.server.await;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn forge_reads_parity_denial_and_source_loss_over_real_http() -> TestResult {
    let _ = tracing_subscriber::fmt().try_init();
    let fixture = start_fixture().await?;
    let bearer = fixture.operator_bearer.clone();
    let scope = request_scope()?;

    // Complete planning binding; this remains an observation, never an actor.
    let binding_request =
        heterarchy_orchestration_backend::wire::forge_reads::CoordinatorGetRequest {
            scope: scope.clone(),
            session_id: None,
            activity_id: ACTIVITY_ID,
        };
    fixture.stub.set("activity.coordinator_get", json!({"found":true,"coordinator":{
        "workspace_id":WORKSPACE_ID,"activity_id":ACTIVITY_ID,"agent_id":AGENT_ID,"revision":7,"updated_at_ms":11
    }}));
    let binding = fixture
        .client
        .coordinator_get(&bearer, &binding_request)
        .await?;
    assert_eq!(
        binding.coordinator.ok_or("binding missing")?.updated_at_ms,
        11
    );
    fixture
        .stub
        .set("activity.coordinator_get", json!({"found":false}));
    assert!(
        fixture
            .client
            .coordinator_get(&bearer, &binding_request)
            .await?
            .coordinator
            .is_none()
    );
    let foreign_binding = fixture
        .client
        .coordinator_get(&fixture.foreign_bearer, &binding_request)
        .await;
    assert!(matches!(
        foreign_binding,
        Err(ClientError::Rejected {
            status: 403,
            code: ServiceCode::Forbidden
        })
    ));

    // --- Parity: global location list ---
    let locations = fixture
        .client
        .location_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::LocationListRequest {},
        )
        .await?;
    assert_eq!(locations.entries.len(), 1);
    assert_eq!(locations.entries[0].location_id, LOCATION_ID);
    assert_eq!(locations.entries[0].display_name, "Demo Location");
    assert_eq!(locations.entries[0].revision, LOCATION_REVISION);

    // --- Parity: workspace catalog list under the exact root locator ---
    let workspaces = fixture
        .client
        .workspace_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WorkspaceListRequest {
                location_id: LOCATION_ID,
                expected_revision: LOCATION_REVISION,
            },
        )
        .await?;
    assert_eq!(workspaces.entries.len(), 1);
    assert_eq!(
        workspaces.entries[0].locator,
        location_root_locator()?.encode()
    );
    assert_eq!(workspaces.entries[0].workspace_id, WORKSPACE_ID);
    assert_eq!(workspaces.entries[0].workspace_revision, WORKSPACE_REVISION);

    // --- Parity: discovery candidates ---
    let discovered = fixture
        .client
        .workspace_discover(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WorkspaceDiscoverRequest {
                location_id: LOCATION_ID,
                expected_revision: LOCATION_REVISION,
            },
        )
        .await?;
    assert_eq!(discovered.entries.len(), 1);
    assert_eq!(discovered.entries[0].locator, child_locator()?.encode());
    assert_eq!(
        discovered.entries[0].disposition,
        heterarchy_orchestration_backend::wire::forge_reads::DiscoveryDispositionWire::Candidate
    );

    // --- Parity: fresh workspace resolution echoes the caller identity ---
    let resolved = fixture
        .client
        .workspace_resolve(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WorkspaceResolveRequest {
                scope: scope.clone(),
                session_id: None,
            },
        )
        .await?;
    assert_eq!(resolved.workspace_id, WORKSPACE_ID);
    assert_eq!(resolved.workspace_revision, WORKSPACE_REVISION);
    assert_eq!(resolved.locator, scope.locator);

    // --- Parity: confined file family ---
    let listing = fixture
        .client
        .file_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::FileListQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: ACTIVITY_ID,
                directory: None,
                max_results: 64,
                max_output_bytes: 4096,
            },
        )
        .await;
    // The static stub has no file.list result configured yet; the call must
    // fail closed as Source unavailability rather than fabricate output.
    assert!(
        matches!(
            &listing,
            Err(ClientError::Rejected {
                status: 503,
                code: ServiceCode::ScopeUnavailable
            })
        ),
        "unexpected file.list outcome: {listing:?}"
    );

    // Configure a scope-echoing file.list result from the captured request
    // shape (the stub stores static results; this one embeds the exact echo).
    let captured = fixture.stub.captured();
    let file_request = captured
        .iter()
        .find(|(path, _)| path.ends_with("file.list"))
        .map(|(_, body)| body.clone())
        .ok_or("file.list request was not captured")?;
    fixture
        .stub
        .set("file.list", file_list_result(&file_request));
    let listing = fixture
        .client
        .file_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::FileListQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: ACTIVITY_ID,
                directory: None,
                max_results: 64,
                max_output_bytes: 4096,
            },
        )
        .await?;
    assert_eq!(listing.entries.len(), 1);
    assert_eq!(listing.entries[0].display_path, "lib.rs");
    assert_eq!(listing.activity_id, ACTIVITY_ID.to_string());
    assert_eq!(listing.scope.workspace_id, WORKSPACE_ID);
    assert_eq!(listing.scope.locator, scope.locator);
    assert_eq!(listing.scanned_entries, 3);

    // --- Parity: goal family ---
    fixture
        .stub
        .set("goal.list", json!({"goals": [goal_row()]}));
    fixture.stub.set("goal.read", {
        let mut object = goal_row()
            .as_object()
            .ok_or("goal row is not an object")?
            .clone();
        object.insert("found".to_owned(), json!(true));
        Value::Object(object)
    });
    fixture.stub.set("dag.read_current", {
        let mut object = dag_row()
            .as_object()
            .ok_or("dag row is not an object")?
            .clone();
        object.insert("found".to_owned(), json!(true));
        Value::Object(object)
    });

    let goals = fixture
        .client
        .goal_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::GoalListQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: ACTIVITY_ID,
                limit: 32,
            },
        )
        .await?;
    assert_eq!(goals.goals.len(), 1);
    assert_eq!(goals.goals[0].goal_id, GOAL_ID.to_string());
    assert_eq!(goals.goals[0].revision, 3);

    let goal = fixture
        .client
        .goal_get(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::GoalGetQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: ACTIVITY_ID,
                goal_id: GOAL_ID,
            },
        )
        .await?;
    let goal = goal.goal.as_ref().ok_or("expected the goal row")?;
    assert_eq!(goal.objective, "Ship the slice");
    assert_eq!(goal.workspace_id, WORKSPACE_ID.to_string());

    // Authenticated Source observations still must match the exact request.
    let mut foreign_goal = goal_row();
    foreign_goal["workspace_id"] = json!(Uuid::from_u128(99));
    fixture
        .stub
        .set("goal.list", json!({"goals":[foreign_goal.clone()]}));
    assert!(matches!(
        fixture
            .client
            .goal_list(
                &bearer,
                &heterarchy_orchestration_backend::wire::forge_reads::GoalListQueryRequest {
                    scope: scope.clone(),
                    session_id: None,
                    activity_id: ACTIVITY_ID,
                    limit: 32
                }
            )
            .await,
        Err(ClientError::Rejected {
            status: 503,
            code: ServiceCode::Unknown
        })
    ));
    foreign_goal["found"] = json!(true);
    fixture.stub.set("goal.read", foreign_goal);
    assert!(matches!(
        fixture
            .client
            .goal_get(
                &bearer,
                &heterarchy_orchestration_backend::wire::forge_reads::GoalGetQueryRequest {
                    scope: scope.clone(),
                    session_id: None,
                    activity_id: ACTIVITY_ID,
                    goal_id: GOAL_ID
                }
            )
            .await,
        Err(ClientError::Rejected {
            status: 503,
            code: ServiceCode::Unknown
        })
    ));
    let mut malformed_goal = goal_row();
    malformed_goal["goal_id"] = json!("not-a-goal-id");
    fixture
        .stub
        .set("goal.list", json!({"goals":[malformed_goal]}));
    assert!(matches!(
        fixture
            .client
            .goal_list(
                &bearer,
                &heterarchy_orchestration_backend::wire::forge_reads::GoalListQueryRequest {
                    scope: scope.clone(),
                    session_id: None,
                    activity_id: ACTIVITY_ID,
                    limit: 32
                }
            )
            .await,
        Err(ClientError::Rejected {
            status: 503,
            code: ServiceCode::Unknown
        })
    ));

    let dag = fixture
        .client
        .dag_get_current(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::DagCurrentQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: ACTIVITY_ID,
                goal_id: GOAL_ID,
            },
        )
        .await?;
    let dag = dag.dag.as_ref().ok_or("expected the dag row")?;
    assert_eq!(dag.goal_id, GOAL_ID.to_string());
    assert_eq!(dag.dag_revision, 4);
    assert_eq!(dag.nodes.len(), 1);
    assert_eq!(
        dag.nodes[0].status,
        heterarchy_orchestration_backend::wire::forge_reads::DagStatusWire::Ready
    );

    // Invalid Source DAG identities/generations are never usable observations.
    let mut malformed_dag = dag_row();
    malformed_dag["found"] = json!(true);
    malformed_dag["revision_id"] = json!("not-a-revision-id");
    let mut foreign_dag = dag_row();
    foreign_dag["found"] = json!(true);
    foreign_dag["goal_id"] = json!(Uuid::from_u128(99));
    let mut impossible_dag = dag_row();
    impossible_dag["found"] = json!(true);
    impossible_dag["parent_revision"] = Value::Null;
    let mut unknown_dependency = dag_row();
    unknown_dependency["found"] = json!(true);
    unknown_dependency["nodes"][0]["waiting_on"] = json!([Uuid::from_u128(99)]);
    let mut unknown_edge = dag_row();
    unknown_edge["found"] = json!(true);
    unknown_edge["edges"] =
        json!([{"upstream":Uuid::from_u128(0xA806),"downstream":Uuid::from_u128(99)}]);
    for invalid in [
        malformed_dag,
        foreign_dag,
        impossible_dag,
        unknown_dependency,
        unknown_edge,
    ] {
        fixture.stub.set("dag.read_current", invalid);
        assert!(matches!(
            fixture
                .client
                .dag_get_current(
                    &bearer,
                    &heterarchy_orchestration_backend::wire::forge_reads::DagCurrentQueryRequest {
                        scope: scope.clone(),
                        session_id: None,
                        activity_id: ACTIVITY_ID,
                        goal_id: GOAL_ID
                    }
                )
                .await,
            Err(ClientError::Rejected {
                status: 503,
                code: ServiceCode::Unknown
            })
        ));
    }

    // --- Parity: command list ---
    let commands = fixture
        .client
        .command_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::CommandListQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: ACTIVITY_ID,
            },
        )
        .await?;
    assert_eq!(commands.activity_id, ACTIVITY_ID.to_string());
    assert!(commands.catalog_fingerprint.contains(':'));
    assert_eq!(commands.commands.len(), 2);

    // --- Parity: diff read ---
    let diff = fixture
        .client
        .diff_read(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::DiffReadQueryRequest {
                scope: scope.clone(),
                session_id: None,
                diff_id: DIFF_ID,
            },
        )
        .await?;
    assert_eq!(diff.diff.diff_id, DIFF_ID.to_string());
    assert_eq!(
        diff.health,
        heterarchy_orchestration_backend::wire::forge_reads::DiffArtifactHealthWire::Available
    );
    assert_eq!(diff.content.as_deref(), Some("diff text"));

    // --- Parity: writer lease get + list ---
    let lease = fixture
        .client
        .writer_lease_get(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WriterLeaseGetQueryRequest {
                scope: scope.clone(),
                session_id: None,
                lease_id: LEASE_ID,
                scope_offset: 2,
                expected_revision: None,
            },
        )
        .await?;
    let lease = lease.lease.as_ref().ok_or("expected the lease row")?;
    assert_eq!(lease.lease_id, LEASE_ID.to_string());
    assert_eq!(
        lease.status,
        heterarchy_orchestration_backend::wire::forge_reads::WriterLeaseStatusWire::ExpiredRecoveryRequired
    );
    assert_eq!(
        lease.blocking_reason.as_deref(),
        Some("expired_writer_lease")
    );
    let leases = fixture
        .client
        .writer_lease_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WriterLeaseListQueryRequest {
                scope: scope.clone(),
                session_id: None,
                limit: 16,
                cursor: None,
            },
        )
        .await?;
    assert!(!leases.truncated);
    assert_eq!(leases.leases.len(), 1);
    assert_eq!(leases.limit, 16);

    // --- Request capture parity: exact Forge command paths and payloads ---
    let captured = fixture.stub.captured();
    let find = |suffix: &str| {
        captured
            .iter()
            .find(|(path, _)| path.ends_with(suffix))
            .map(|(_, body)| body.clone())
            .ok_or_else(|| format!("{suffix} not captured"))
    };
    let resolve = find("workspace.resolve")?;
    assert_eq!(resolve["op"], "workspace.resolve");
    assert_eq!(
        resolve["payload"],
        json!({
            "location_id": LOCATION_ID,
            "expected_revision": LOCATION_REVISION,
            "relative_root": RELATIVE_ROOT,
        })
    );
    let goal_read = find("goal.read")?;
    assert_eq!(goal_read["payload"]["goal_id"], json!(GOAL_ID));
    assert_eq!(goal_read["payload"]["activity_id"], json!(ACTIVITY_ID));
    let dag_read = find("dag.read_current")?;
    assert_eq!(dag_read["payload"]["goal_id"], json!(GOAL_ID));
    let command_list = find("command.list")?;
    assert_eq!(
        command_list["payload"]["activity"]["activity_id"],
        json!(ACTIVITY_ID)
    );
    let diff_read_req = find("diff.read")?;
    assert_eq!(diff_read_req["payload"]["diff_id"], json!(DIFF_ID));
    let lease_get = find("writer_lease.get")?;
    assert_eq!(lease_get["payload"]["writer_lease_id"], json!(LEASE_ID));
    assert_eq!(lease_get["payload"]["scope_offset"], json!(2));

    // --- Denial: bearer required ---
    let denied = fixture
        .client
        .location_list(
            &hyper::header::HeaderValue::from_static("Bearer not-a-token"),
            &heterarchy_orchestration_backend::wire::forge_reads::LocationListRequest {},
        )
        .await;
    assert!(
        matches!(&denied, Err(ClientError::Rejected { .. })),
        "unexpected unauthenticated outcome: {denied:?}"
    );

    // --- Denial: a non-operator principal without membership is forbidden
    // on membership-fenced reads (the location-scoped catalog lists keep
    // authenticated-caller parity with the native surface) ---
    let forbidden = fixture
        .client
        .workspace_resolve(
            &fixture.foreign_bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WorkspaceResolveRequest {
                scope: scope.clone(),
                session_id: None,
            },
        )
        .await;
    assert!(
        matches!(
            &forbidden,
            Err(ClientError::Rejected {
                status: 403,
                code: ServiceCode::Forbidden
            })
        ),
        "unexpected foreign-caller outcome: {forbidden:?}"
    );
    let forbidden_lease = fixture
        .client
        .writer_lease_get(
            &fixture.foreign_bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WriterLeaseGetQueryRequest {
                scope: scope.clone(),
                session_id: None,
                lease_id: LEASE_ID,
                scope_offset: 0,
                expected_revision: None,
            },
        )
        .await;
    assert!(
        matches!(
            &forbidden_lease,
            Err(ClientError::Rejected {
                status: 403,
                code: ServiceCode::Forbidden
            })
        ),
        "unexpected foreign lease outcome: {forbidden_lease:?}"
    );
    let forbidden_goal = fixture
        .client
        .goal_list(
            &fixture.foreign_bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::GoalListQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: ACTIVITY_ID,
                limit: 8,
            },
        )
        .await;
    assert!(
        matches!(
            &forbidden_goal,
            Err(ClientError::Rejected {
                status: 403,
                code: ServiceCode::Forbidden
            })
        ),
        "unexpected foreign goal outcome: {forbidden_goal:?}"
    );

    // --- Stale: a fresh resolve that disagrees with the caller reference ---
    fixture.stub.set(
        "workspace.resolve",
        json!({"workspace_id": WORKSPACE_ID, "workspace_revision": WORKSPACE_REVISION + 1}),
    );
    let stale = fixture
        .client
        .workspace_resolve(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WorkspaceResolveRequest {
                scope: scope.clone(),
                session_id: None,
            },
        )
        .await;
    assert!(
        matches!(
            &stale,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict
            })
        ),
        "unexpected stale outcome: {stale:?}"
    );
    fixture.stub.set(
        "workspace.resolve",
        json!({"workspace_id": WORKSPACE_ID, "workspace_revision": WORKSPACE_REVISION}),
    );

    fixture.stub.set(
        "workspace.resolve",
        json!({"workspace_id":WORKSPACE_ID,"workspace_revision":WORKSPACE_REVISION-1}),
    );
    assert!(matches!(
        fixture
            .client
            .coordinator_get(&bearer, &binding_request)
            .await,
        Err(ClientError::Rejected {
            status: 409,
            code: ServiceCode::Conflict
        })
    ));
    fixture.stub.set(
        "workspace.resolve",
        json!({"workspace_id":WORKSPACE_ID,"workspace_revision":WORKSPACE_REVISION}),
    );

    // --- Source loss: fail closed on every family, no native fallback ---
    fixture.stub_task.abort();
    assert!(matches!(
        fixture
            .client
            .coordinator_get(&bearer, &binding_request)
            .await,
        Err(ClientError::Rejected {
            status: 503,
            code: ServiceCode::ScopeUnavailable
        })
    ));
    let source_lost = fixture
        .client
        .workspace_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::WorkspaceListRequest {
                location_id: LOCATION_ID,
                expected_revision: LOCATION_REVISION,
            },
        )
        .await;
    assert!(
        matches!(
            &source_lost,
            Err(ClientError::Rejected {
                status: 503,
                code: ServiceCode::ScopeUnavailable
            })
        ),
        "unexpected source-loss outcome: {source_lost:?}"
    );
    let global_lost = fixture
        .client
        .location_list(
            &bearer,
            &heterarchy_orchestration_backend::wire::forge_reads::LocationListRequest {},
        )
        .await;
    assert!(
        matches!(
            &global_lost,
            Err(ClientError::Rejected {
                status: 503,
                code: ServiceCode::ScopeUnavailable
            })
        ),
        "unexpected global source-loss outcome: {global_lost:?}"
    );

    fixture.cancellation.cancel();
    let _ = fixture.server.await;
    Ok(())
}
