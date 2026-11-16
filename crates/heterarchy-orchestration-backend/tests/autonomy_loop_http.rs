//! End-to-end HTTP integration tests for the V1 Autonomous Organization Execution Loop.
//!
//! Exercises `ExecutionPreference` profile configuration, fail-closed `Workspace` scope
//! registration and embedded `SessionAuthority` supervision, independent reviewer
//! assignment with diff-digest-bound verdicts and pre-completion gating, and secret-free
//! `AssignmentExecutionSnapshot` preparation over real loopback HTTP listeners.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use forge_control::WorkspaceLocator;
use forge_domain::{
    AuthenticatedPrincipal, DirectTokenId, LocationId, LocationRevision, TimestampMillis,
};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
    McpTokenWorker,
};
use heterarchy_orchestration_backend::{
    BackendConfig, BackendError, CoordinatorAuthority, TokenAuthorityBootstrap,
    forge_commands::ForgeCommandHttpClient,
    forge_source::ForgeSourceHttpClient,
    serve, serve_with_runtime,
    session_process::EmbeddedSessionAuthorityConfig,
    wire::{
        AgentProfileCreateRequest, AgentProfileCreateResponse, AgentProfileResponse,
        AgentSessionOpenRequest, AgentSessionOpenResponse, ExecutionPreferenceWire,
        ProfileMutationResponse, ProfileUpdateRequest, WorkspaceQueryScope,
        autonomy_loop::{
            AssignReviewerRequest, ExecutionAuthorWire, ExecutionPermissionsWire,
            ExecutionSnapshotOutcomeResponse, ExecutionTimeoutsWire, McpGrantWire,
            PrepareExecutionSnapshotRequest, PrincipalIdentityWire, RecordReviewVerdictRequest,
            RegisteredWorkspaceScopeResponse, ReviewerCandidateWire, ReviewerOperationResponse,
            ScopeRegisterRequest, ScopeUnregisterRequest, VerifyNodeCompletionGateRequest,
            WorkCorrelationWire,
        },
        reviewer_qualification_rule::{
            ActualRuntimeFactWire, EvidenceOwnerNamespaceWire, EvidenceSourceOwnerWire,
            QualificationEvidenceRequirementWire, ReviewerQualificationRuleChangeWire,
            ReviewerQualificationRuleMutationRequest, ReviewerQualificationRuleMutationResponse,
            ReviewerQualificationRulePolicyWire, ReviewerQualificationRuleQueryRequest,
            ReviewerQualificationRuleQueryResponse, RuntimeFactPredicateWire,
        },
    },
};
use heterarchy_orchestration_operations::BrokerClusterConfig;
use heterarchy_orchestration_runtime::{OrchestrationRuntime, access::OperatorPolicy};
use hyper::StatusCode;
use rusqlite::{Connection, params};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const READY_DEADLINE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const FORGE_TIMEOUT: Duration = Duration::from_secs(2);
const FORGE_SERVICE_SECRET: &str = "autonomy-loop-forge-service-secret";
const TOKEN_PEPPER: [u8; 32] = [0x5A; 32];

struct TempDir(PathBuf);

impl TempDir {
    fn create(label: &str) -> TestResult<Self> {
        let raw =
            std::env::temp_dir().join(format!("{label}-{}-{}", std::process::id(), Uuid::now_v7()));
        fs::create_dir_all(&raw)?;
        fs::set_permissions(&raw, fs::Permissions::from_mode(0o700))?;
        Ok(Self(raw.canonicalize()?))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

type SessionOwners = Mutex<BTreeMap<String, (u64, String)>>;

struct FakeSingleNodeBroker {
    broker_address: SocketAddr,
    operations_address: SocketAddr,
    _ops_guard: StdTcpListener,
    accept_task: JoinHandle<()>,
}

impl FakeSingleNodeBroker {
    async fn start() -> TestResult<Self> {
        let broker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let broker_address = broker_listener.local_addr()?;
        let ops_guard = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let operations_address = ops_guard.local_addr()?;
        let owners: Arc<SessionOwners> = Arc::new(Mutex::new(BTreeMap::new()));
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = broker_listener.accept().await else {
                    return;
                };
                let state = Arc::clone(&owners);
                tokio::spawn(async move {
                    let _ = handle_broker_connection(socket, &state).await;
                });
            }
        });
        Ok(Self {
            broker_address,
            operations_address,
            _ops_guard: ops_guard,
            accept_task,
        })
    }

    fn cluster_config(&self) -> TestResult<BrokerClusterConfig> {
        Ok(BrokerClusterConfig::from_single_node(
            self.broker_address,
            self.operations_address,
        )?)
    }
}

impl Drop for FakeSingleNodeBroker {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn handle_broker_connection(
    socket: TcpStream,
    owners: &SessionOwners,
) -> Result<(), std::io::Error> {
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let count = reader.read_line(&mut line).await?;
    if count == 0 {
        return Ok(());
    }
    let Ok(envelope) = serde_json::from_str::<Value>(&line) else {
        return Ok(());
    };
    let Some(reply) = evaluate_broker_frame(&envelope, owners) else {
        return Ok(());
    };
    let mut encoded = reply.to_string();
    encoded.push('\n');
    write_half.write_all(encoded.as_bytes()).await?;
    write_half.flush().await?;
    write_half.shutdown().await
}

fn evaluate_broker_frame(envelope: &Value, owners: &SessionOwners) -> Option<Value> {
    let request_id = envelope.get("request_id")?.as_str()?;
    let operation = envelope.get("operation")?.as_str()?;
    match operation {
        "acquire_command_session_owner" => {
            let session_id = envelope.get("command_session_id")?.as_str()?;
            let expected = envelope.get("expected_owner_epoch")?.as_u64()?;
            let instance_id = envelope.get("owner_instance_id")?.as_str()?;
            let mut guard = match owners.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let next_epoch = match guard.get(session_id).cloned() {
                None => {
                    guard.insert(session_id.to_owned(), (expected, instance_id.to_owned()));
                    Some(expected)
                }
                Some((cur_epoch, cur_instance))
                    if cur_instance == instance_id && cur_epoch == expected =>
                {
                    Some(cur_epoch)
                }
                Some((cur_epoch, _)) if cur_epoch == expected => {
                    let advanced = cur_epoch.saturating_add(1);
                    guard.insert(session_id.to_owned(), (advanced, instance_id.to_owned()));
                    Some(advanced)
                }
                Some(_) => None,
            };
            drop(guard);
            Some(match next_epoch {
                Some(epoch) => json!({
                    "version": 3,
                    "request_id": request_id,
                    "ok": true,
                    "result": { "owner_epoch": epoch },
                }),
                None => json!({
                    "version": 3,
                    "request_id": request_id,
                    "ok": false,
                    "error": {
                        "code": "STALE_FENCE",
                        "message": "expected owner epoch mismatch",
                        "disposition": "rejected",
                    },
                }),
            })
        }
        "authority.record" => {
            let revision = envelope.get("revision")?.as_u64()?;
            Some(json!({
                "version": 3,
                "request_id": request_id,
                "ok": true,
                "result": { "recorded_revision": revision },
            }))
        }
        _ => None,
    }
}

struct FakeForgeHttpStub {
    address: SocketAddr,
    accept_task: JoinHandle<()>,
}

impl FakeForgeHttpStub {
    async fn start(workspace_id: Uuid, workspace_revision: i64) -> TestResult<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let _ =
                        handle_forge_stub_connection(&mut socket, workspace_id, workspace_revision)
                            .await;
                });
            }
        });
        Ok(Self {
            address,
            accept_task,
        })
    }
}

impl Drop for FakeForgeHttpStub {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn handle_forge_stub_connection(
    socket: &mut TcpStream,
    workspace_id: Uuid,
    workspace_revision: i64,
) -> Result<(), std::io::Error> {
    let Some((path, body)) = read_forge_http_request(socket).await? else {
        return Ok(());
    };
    let (status, reason, payload) = if path.rsplit('/').next() == Some("workspace.resolve") {
        (
            200_u16,
            "OK",
            json!({
                "forge_api": body["forge_api"],
                "request_id": body["request_id"],
                "ok": true,
                "result": {
                    "workspace_id": workspace_id,
                    "workspace_revision": workspace_revision,
                },
            }),
        )
    } else {
        (
            503_u16,
            "Service Unavailable",
            json!({
                "forge_api": body["forge_api"],
                "request_id": body["request_id"],
                "ok": false,
                "error": {
                    "code": "SOURCE_UNAVAILABLE",
                    "message": "unsupported stub command",
                    "category": "scope",
                    "retryable": true,
                    "recovery_action": "retry_later",
                    "recovery_hint": "",
                    "readback": null,
                },
            }),
        )
    };
    let encoded = payload.to_string();
    let len = encoded.len();
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{encoded}"
    );
    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await
}

async fn read_forge_http_request(
    socket: &mut TcpStream,
) -> Result<Option<(String, Value)>, std::io::Error> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    let (header_end, body_start, content_len) = loop {
        let count = socket.read(&mut chunk).await?;
        if count == 0 {
            return Ok(None);
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > 128 * 1024 {
            return Ok(None);
        }
        if let Some(pos) = find_double_crlf(&bytes) {
            let headers = String::from_utf8_lossy(&bytes[..pos]);
            let length = parse_content_length(&headers);
            let start = pos + 4;
            if bytes.len() >= start + length {
                break (pos, start, length);
            }
        }
    };
    let header_text = String::from_utf8_lossy(&bytes[..header_end]);
    let Some(path) = header_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .map(str::to_owned)
    else {
        return Ok(None);
    };
    let Ok(body) = serde_json::from_slice::<Value>(&bytes[body_start..body_start + content_len])
    else {
        return Ok(None);
    };
    Ok(Some((path, body)))
}

fn find_double_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(headers: &str) -> usize {
    for line in headers.lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
            && let Ok(length) = value.trim().parse::<usize>()
        {
            return length;
        }
    }
    0
}

struct IssuedOperatorAuth {
    token_runtime: McpTokenRuntime,
    bearer: String,
    principal: AuthenticatedPrincipal,
}

fn start_operator_auth(auth_dir: &Path) -> TestResult<IssuedOperatorAuth> {
    fs::create_dir_all(auth_dir)?;
    let catalog = McpTokenCatalog::open(auth_dir.join("tokens.sqlite3"))?;
    let (client, worker) = McpTokenWorker::start(McpTokenService::new(TOKEN_PEPPER, catalog))?;
    let issued = client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("autonomy-loop-operator".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let secret = issued.reveal().as_str();
    let bearer = format!("Bearer {secret}");
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
        issued.metadata().id().into_uuid(),
    ));
    Ok(IssuedOperatorAuth {
        token_runtime: McpTokenRuntime::from_parts(client, worker),
        bearer,
        principal,
    })
}

fn reserve_loopback_addr() -> TestResult<SocketAddr> {
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr)
}

struct RunningBackend {
    _dir: TempDir,
    _broker: FakeSingleNodeBroker,
    _forge_stub: Option<FakeForgeHttpStub>,
    token_runtime: McpTokenRuntime,
    address: SocketAddr,
    bearer: String,
    cancellation: CancellationToken,
    server: JoinHandle<Result<(), BackendError>>,
}

fn build_http_coordinator_authority(stub: &FakeForgeHttpStub) -> TestResult<CoordinatorAuthority> {
    let cmd_client =
        ForgeCommandHttpClient::new(stub.address, FORGE_SERVICE_SECRET, FORGE_TIMEOUT)?;
    Ok(CoordinatorAuthority::from_http(
        ForgeSourceHttpClient::new(cmd_client),
        Vec::new(),
    )?)
}

impl RunningBackend {
    async fn start_basic(label: &str) -> TestResult<Self> {
        Self::start_internal(label, None, None).await
    }

    async fn start_with_empty_http_scopes(
        label: &str,
        workspace_id: Uuid,
        workspace_revision: i64,
        pre_serve_seed: Option<&(dyn Fn(&Path) -> TestResult + Sync)>,
    ) -> TestResult<Self> {
        let forge_stub = FakeForgeHttpStub::start(workspace_id, workspace_revision).await?;
        Self::start_internal(label, Some(forge_stub), pre_serve_seed).await
    }

    async fn start_internal(
        label: &str,
        forge_stub: Option<FakeForgeHttpStub>,
        pre_serve_seed: Option<&(dyn Fn(&Path) -> TestResult + Sync)>,
    ) -> TestResult<Self> {
        let dir = TempDir::create(label)?;
        let broker = FakeSingleNodeBroker::start().await?;
        let auth = start_operator_auth(&dir.path().join("auth"))?;
        let runtime_root = dir.path().join("runtime");
        fs::create_dir_all(&runtime_root)?;
        let runtime = OrchestrationRuntime::open_with_broker_cluster(
            &runtime_root,
            broker.cluster_config()?,
        )?
        .with_operator_policy(OperatorPolicy::new(vec![auth.principal])?);
        if let Some(seed_fn) = pre_serve_seed {
            seed_fn(&runtime_root)?;
        }
        let coordinator_authority = forge_stub
            .as_ref()
            .map(build_http_coordinator_authority)
            .transpose()?;
        let address = reserve_loopback_addr()?;
        let cancellation = CancellationToken::new();
        let server = tokio::spawn(serve_with_runtime(
            Arc::new(runtime),
            address,
            auth.token_runtime.client(),
            coordinator_authority,
            cancellation.clone(),
        ));
        wait_for_ready(address).await?;
        Ok(Self {
            _dir: dir,
            _broker: broker,
            _forge_stub: forge_stub,
            token_runtime: auth.token_runtime,
            address,
            bearer: auth.bearer,
            cancellation,
            server,
        })
    }

    async fn shutdown(self) -> TestResult {
        self.cancellation.cancel();
        self.server.await??;
        drop(self.token_runtime);
        Ok(())
    }
}

async fn wait_for_ready(address: SocketAddr) -> TestResult {
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        if let Ok((status, body)) = send_http_raw(address, "GET", "/ready", None, b"").await
            && status == StatusCode::OK
        {
            let payload: Value = serde_json::from_slice(&body)?;
            if payload.get("status").and_then(Value::as_str) == Some("ready") {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for /ready on {address}").into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn send_http_raw(
    address: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: &[u8],
) -> TestResult<(StatusCode, Vec<u8>)> {
    let mut socket = TcpStream::connect(address).await?;
    let auth_header =
        bearer.map_or_else(String::new, |token| format!("Authorization: {token}\r\n"));
    let body_len = body.len();
    let content_headers = if body.is_empty() {
        String::new()
    } else {
        format!("Content-Type: application/json\r\nContent-Length: {body_len}\r\n")
    };
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{auth_header}{content_headers}Connection: close\r\n\r\n"
    );
    socket.write_all(head.as_bytes()).await?;
    if !body.is_empty() {
        socket.write_all(body).await?;
    }
    socket.flush().await?;
    let mut raw = Vec::new();
    socket.read_to_end(&mut raw).await?;
    let split_at = find_double_crlf(&raw).ok_or("missing HTTP response header delimiter")?;
    let header_str = std::str::from_utf8(&raw[..split_at])?;
    let status_code: u16 = header_str
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("missing HTTP status code")?
        .parse()?;
    let status = StatusCode::from_u16(status_code)?;
    let raw_body = &raw[split_at + 4..];
    let is_chunked = header_str.lines().any(|line| {
        line.split_once(':').is_some_and(|(k, v)| {
            k.eq_ignore_ascii_case("transfer-encoding") && v.trim().eq_ignore_ascii_case("chunked")
        })
    });
    let decoded = if is_chunked {
        decode_chunked_body(raw_body)?
    } else {
        raw_body.to_vec()
    };
    Ok((status, decoded))
}

fn decode_chunked_body(mut rest: &[u8]) -> TestResult<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("malformed chunk header")?;
        let size_hex = std::str::from_utf8(&rest[..line_end])?.trim();
        let size = usize::from_str_radix(size_hex, 16)?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            break;
        }
        if rest.len() < size + 2 {
            return Err("truncated chunk payload".into());
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
    Ok(out)
}

async fn request_json<Req, Resp>(
    backend: &RunningBackend,
    method: &str,
    path: &str,
    request: &Req,
) -> TestResult<(StatusCode, Resp)>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let payload = serde_json::to_vec(request)?;
    let (status, body) = send_http_raw(
        backend.address,
        method,
        path,
        Some(&backend.bearer),
        &payload,
    )
    .await?;
    let parsed = serde_json::from_slice::<Resp>(&body).map_err(|err| {
        let preview = String::from_utf8_lossy(&body);
        format!("failed decoding {method} {path} (status {status}): {err}; body={preview}")
    })?;
    Ok((status, parsed))
}

async fn get_json<Resp: DeserializeOwned>(
    backend: &RunningBackend,
    path: &str,
) -> TestResult<(StatusCode, Resp)> {
    let (status, body) =
        send_http_raw(backend.address, "GET", path, Some(&backend.bearer), b"").await?;
    let parsed = serde_json::from_slice::<Resp>(&body)?;
    Ok((status, parsed))
}

fn build_workspace_scope(
    workspace_id: Uuid,
    location_id: Uuid,
    location_revision: i64,
    workspace_revision: i64,
) -> TestResult<WorkspaceQueryScope> {
    let locator = WorkspaceLocator::new(
        LocationId::from_uuid(location_id),
        LocationRevision::new(location_revision)?,
        Path::new("."),
    )?
    .encode();
    Ok(WorkspaceQueryScope {
        workspace_id,
        location_id,
        location_revision,
        workspace_revision,
        locator,
    })
}

async fn open_operator_session(backend: &RunningBackend) -> TestResult<Uuid> {
    let (status, opened): (_, AgentSessionOpenResponse) = request_json(
        backend,
        "POST",
        "/internal/v1/sessions",
        &AgentSessionOpenRequest {
            request_id: Uuid::now_v7().to_string(),
        },
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert!(opened.created);
    Ok(opened.session.session_id)
}

#[tokio::test]
async fn slice_1_agent_profile_execution_preference_create_get_and_update_over_http() -> TestResult
{
    let backend = RunningBackend::start_basic("autonomy-slice1-http").await?;
    let digest_v1 = "a1".repeat(32);
    let create_req = AgentProfileCreateRequest {
        request_id: Uuid::now_v7().to_string(),
        stable_name: "astra-orchestrator".to_owned(),
        display_name: "Astra Orchestrator".to_owned(),
        preferred_model: Some("gpt-6-astra".to_owned()),
        preferred_reasoning: Some("high".to_owned()),
        base_capabilities: vec!["orchestration".to_owned()],
        execution_preference: Some(ExecutionPreferenceWire {
            cli_engine: "codex".to_owned(),
            preferred_model: "gpt-6-astra".to_owned(),
            preferred_reasoning: Some("high".to_owned()),
            capabilities: vec!["rust".to_owned(), "orchestration".to_owned()],
            role_prompt_digest_hex: Some(digest_v1.clone()),
        }),
    };

    let (create_status, created): (_, AgentProfileCreateResponse) =
        request_json(&backend, "POST", "/internal/v1/agent-profiles", &create_req).await?;
    assert_eq!(create_status, StatusCode::CREATED);
    assert!(created.created);
    assert_eq!(created.profile.revision, 1);
    let expected_v1 = ExecutionPreferenceWire {
        cli_engine: "codex".to_owned(),
        preferred_model: "gpt-6-astra".to_owned(),
        preferred_reasoning: Some("high".to_owned()),
        capabilities: vec!["orchestration".to_owned(), "rust".to_owned()],
        role_prompt_digest_hex: Some(digest_v1),
    };
    assert_eq!(
        created.profile.execution_preference,
        Some(expected_v1.clone())
    );

    let profile_id = created.profile.profile_id;
    let get_path = format!("/internal/v1/agent-profiles/{profile_id}");
    let (get_status, fetched): (_, AgentProfileResponse) = get_json(&backend, &get_path).await?;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(fetched.execution_preference, Some(expected_v1));

    let digest_v2 = "b2".repeat(32);
    let expected_v2 = ExecutionPreferenceWire {
        cli_engine: "agy".to_owned(),
        preferred_model: "gemini-4-carbon-high".to_owned(),
        preferred_reasoning: Some("high".to_owned()),
        capabilities: vec!["orchestration".to_owned(), "review".to_owned()],
        role_prompt_digest_hex: Some(digest_v2),
    };
    let update_req = ProfileUpdateRequest {
        request_id: Uuid::now_v7().to_string(),
        expected_revision: 1,
        display_name: "Carbon Verifier".to_owned(),
        preferred_model: Some("gemini-4-carbon-high".to_owned()),
        preferred_reasoning: Some("high".to_owned()),
        base_capabilities: vec!["orchestration".to_owned(), "review".to_owned()],
        execution_preference: Some(expected_v2.clone()),
    };
    let update_path = format!("/internal/v1/agent-profiles/{profile_id}/configuration");
    let (update_status, mutated): (_, ProfileMutationResponse) =
        request_json(&backend, "PUT", &update_path, &update_req).await?;
    assert_eq!(update_status, StatusCode::OK);
    assert!(!mutated.replayed);
    assert_eq!(mutated.profile.revision, 2);
    assert_eq!(
        mutated.profile.execution_preference,
        Some(expected_v2.clone())
    );

    let (verify_status, verified): (_, AgentProfileResponse) =
        get_json(&backend, &get_path).await?;
    assert_eq!(verify_status, StatusCode::OK);
    assert_eq!(verified.revision, 2);
    assert_eq!(verified.execution_preference, Some(expected_v2));

    backend.shutdown().await
}

#[tokio::test]
async fn slice_2_workspace_scope_registry_fails_closed_and_tracks_monotonic_generations()
-> TestResult {
    let workspace_id = Uuid::from_u128(0xC201);
    let location_id = Uuid::from_u128(0xC202);
    let workspace_revision = 3_i64;
    let backend = RunningBackend::start_with_empty_http_scopes(
        "autonomy-slice2-scopes",
        workspace_id,
        workspace_revision,
        None,
    )
    .await?;
    let scope = build_workspace_scope(workspace_id, location_id, 1, workspace_revision)?;
    let session_id = open_operator_session(&backend).await?;
    let scoped_query = ReviewerQualificationRuleQueryRequest {
        scope: scope.clone(),
        session_id,
        rule_id: Uuid::from_u128(0xC299),
    };
    let query_path =
        format!("/internal/v1/workspaces/{workspace_id}/reviewer-qualification-rules/query");

    let (empty_status, empty_scopes): (_, Vec<RegisteredWorkspaceScopeResponse>) =
        get_json(&backend, "/internal/v1/workspaces/scopes").await?;
    assert_eq!(empty_status, StatusCode::OK);
    assert!(empty_scopes.is_empty());

    let (denied_status, denied_err): (_, Value) =
        request_json(&backend, "POST", &query_path, &scoped_query).await?;
    assert_eq!(denied_status, StatusCode::FORBIDDEN);
    assert_eq!(denied_err["code"], "FORBIDDEN");

    let (reg1_status, reg1): (_, RegisteredWorkspaceScopeResponse) = request_json(
        &backend,
        "POST",
        "/internal/v1/workspaces/scopes",
        &ScopeRegisterRequest {
            request_id: Uuid::now_v7().to_string(),
            expected_revision: 0,
            scope: scope.clone(),
        },
    )
    .await?;
    assert_eq!(reg1_status, StatusCode::OK);
    assert!(reg1.active);
    assert_eq!((reg1.revision, reg1.generation), (1, 1));

    let (list1_status, list1): (_, Vec<RegisteredWorkspaceScopeResponse>) =
        get_json(&backend, "/internal/v1/workspaces/scopes").await?;
    assert_eq!(list1_status, StatusCode::OK);
    assert_eq!(list1.len(), 1);
    assert_eq!((list1[0].revision, list1[0].generation), (1, 1));

    let (ok_status, ok_resp): (_, ReviewerQualificationRuleQueryResponse) =
        request_json(&backend, "POST", &query_path, &scoped_query).await?;
    assert_eq!(ok_status, StatusCode::OK);
    assert!(ok_resp.current.is_none());

    assert_unregister_and_reregister(&backend, &scope, &query_path, &scoped_query).await?;
    backend.shutdown().await
}

async fn assert_unregister_and_reregister(
    backend: &RunningBackend,
    scope: &WorkspaceQueryScope,
    query_path: &str,
    scoped_query: &ReviewerQualificationRuleQueryRequest,
) -> TestResult {
    let workspace_id = scope.workspace_id;
    let unregister_path = format!("/internal/v1/workspaces/{workspace_id}/scopes/unregister");
    let (unreg_status, unreg): (_, RegisteredWorkspaceScopeResponse) = request_json(
        backend,
        "POST",
        &unregister_path,
        &ScopeUnregisterRequest {
            request_id: Uuid::now_v7().to_string(),
            expected_revision: 1,
        },
    )
    .await?;
    assert_eq!(unreg_status, StatusCode::OK);
    assert!(!unreg.active);
    assert_eq!((unreg.revision, unreg.generation), (2, 2));

    let (denied_after_unreg, _): (_, Value) =
        request_json(backend, "POST", query_path, scoped_query).await?;
    assert_eq!(denied_after_unreg, StatusCode::FORBIDDEN);

    let (reg2_status, reg2): (_, RegisteredWorkspaceScopeResponse) = request_json(
        backend,
        "POST",
        "/internal/v1/workspaces/scopes",
        &ScopeRegisterRequest {
            request_id: Uuid::now_v7().to_string(),
            expected_revision: 2,
            scope: scope.clone(),
        },
    )
    .await?;
    assert_eq!(reg2_status, StatusCode::OK);
    assert!(reg2.active);
    assert_eq!((reg2.revision, reg2.generation), (3, 3));

    let (list2_status, list2): (_, Vec<RegisteredWorkspaceScopeResponse>) =
        get_json(backend, "/internal/v1/workspaces/scopes").await?;
    assert_eq!(list2_status, StatusCode::OK);
    assert_eq!(list2.len(), 1);
    assert_eq!((list2[0].revision, list2[0].generation), (3, 3));
    Ok(())
}

#[tokio::test]
async fn slice_2_embedded_session_authority_initializes_catalog_serves_ready_and_drains()
-> TestResult {
    let dir = TempDir::create("autonomy-slice2-embedded-session")?;
    let data_root = dir.path().join("data");
    fs::create_dir_all(&data_root)?;
    fs::set_permissions(&data_root, fs::Permissions::from_mode(0o700))?;
    let secret_file = dir.path().join("session-authority.secret");
    fs::write(&secret_file, "embedded-session-authority-secret-v1\n")?;
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600))?;

    let broker = FakeSingleNodeBroker::start().await?;
    let auth = start_operator_auth(&dir.path().join("auth"))?;
    let listen_addr = reserve_loopback_addr()?;
    let session_catalog_path = data_root.join("agent-sessions.sqlite3");
    assert!(!session_catalog_path.exists());

    let embedded_cfg = EmbeddedSessionAuthorityConfig::new(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        session_catalog_path.clone(),
        secret_file,
    )?;
    let config = BackendConfig::new(listen_addr, data_root)?
        .with_token_authority(TokenAuthorityBootstrap::Runtime(auth.token_runtime))
        .with_operator_policy(OperatorPolicy::new(vec![auth.principal])?)
        .with_broker_cluster(broker.cluster_config()?)
        .with_embedded_session_authority(embedded_cfg);

    let cancellation = CancellationToken::new();
    let serve_handle = tokio::spawn(serve(config, cancellation.clone()));
    wait_for_ready(listen_addr).await?;
    assert!(session_catalog_path.is_file());

    let (ready_status, ready_body) = send_http_raw(listen_addr, "GET", "/ready", None, b"").await?;
    assert_eq!(ready_status, StatusCode::OK);
    let ready_json: Value = serde_json::from_slice(&ready_body)?;
    assert_eq!(ready_json["service"], "heterarchy-orchestration-backend");
    assert_eq!(ready_json["status"], "ready");

    cancellation.cancel();
    serve_handle.await??;
    Ok(())
}

struct ReviewerHttpFixture {
    workspace_id: Uuid,
    correlation: WorkCorrelationWire,
    leader: ExecutionAuthorWire,
    executor: ExecutionAuthorWire,
    self_executor_candidate: ReviewerCandidateWire,
    self_leader_candidate: ReviewerCandidateWire,
    uninterested_candidate: ReviewerCandidateWire,
    qualified_candidate: ReviewerCandidateWire,
}

impl ReviewerHttpFixture {
    fn build() -> Self {
        let workspace_id = Uuid::from_u128(0xD301);
        let executor_profile = Uuid::from_u128(0xD311).to_string();
        let leader_profile = Uuid::from_u128(0xD312).to_string();
        Self {
            workspace_id,
            correlation: WorkCorrelationWire {
                goal_id: Uuid::from_u128(0xD302).to_string(),
                dag_revision: 1,
                node_id: Uuid::from_u128(0xD303).to_string(),
                assignment_id: Uuid::from_u128(0xD304).to_string(),
                attempt: 1,
            },
            executor: ExecutionAuthorWire {
                profile_id: executor_profile.clone(),
                participant_id: Uuid::from_u128(0xD321).to_string(),
                principal: PrincipalIdentityWire {
                    kind: "direct_token".to_owned(),
                    id: Uuid::from_u128(0xD331).to_string(),
                },
            },
            leader: ExecutionAuthorWire {
                profile_id: leader_profile.clone(),
                participant_id: Uuid::from_u128(0xD322).to_string(),
                principal: PrincipalIdentityWire {
                    kind: "direct_token".to_owned(),
                    id: Uuid::from_u128(0xD332).to_string(),
                },
            },
            self_executor_candidate: candidate_wire(
                &executor_profile,
                0xD341,
                0xD351,
                0xD361,
                0xD371,
                0xD381,
            ),
            self_leader_candidate: candidate_wire(
                &leader_profile,
                0xD342,
                0xD352,
                0xD362,
                0xD372,
                0xD382,
            ),
            uninterested_candidate: candidate_wire(
                &Uuid::from_u128(0xD313).to_string(),
                0xD343,
                0xD353,
                0xD363,
                0xD373,
                0xD383,
            ),
            qualified_candidate: candidate_wire(
                &Uuid::from_u128(0xD314).to_string(),
                0xD344,
                0xD354,
                0xD364,
                0xD374,
                0xD384,
            ),
        }
    }
}

fn candidate_wire(
    profile_id: &str,
    participant_tag: u128,
    participation_tag: u128,
    token_tag: u128,
    agent_tag: u128,
    session_tag: u128,
) -> ReviewerCandidateWire {
    ReviewerCandidateWire {
        profile_id: profile_id.to_owned(),
        participant_id: Uuid::from_u128(participant_tag).to_string(),
        participation_id: Uuid::from_u128(participation_tag).to_string(),
        principal: PrincipalIdentityWire {
            kind: "direct_token".to_owned(),
            id: Uuid::from_u128(token_tag).to_string(),
        },
        agent_id: Uuid::from_u128(agent_tag).to_string(),
        session_id: Uuid::from_u128(session_tag).to_string(),
    }
}

fn seed_interested_reviewer_sqlite(
    runtime_root: &Path,
    workspace_id: Uuid,
    candidate: &ReviewerCandidateWire,
) -> TestResult {
    let profile_uuid = Uuid::parse_str(&candidate.profile_id)?;
    let participation_uuid = Uuid::parse_str(&candidate.participation_id)?;
    let participant_uuid = Uuid::parse_str(&candidate.participant_id)?;
    let agent_uuid = Uuid::parse_str(&candidate.agent_id)?;
    let session_uuid = Uuid::parse_str(&candidate.session_id)?;
    let receipt_req_id = Uuid::from_u128(0xD391);
    let activity_uuid = Uuid::from_u128(0xD392);

    let conn = Connection::open(runtime_root.join("agent-operations.sqlite3"))?;
    conn.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) SELECT ?1,?2,destination_id,1,2,NULL,5,0,?3,5,?3 FROM ops_catalog_identity WHERE singleton=1",
        params![
            Uuid::from_u128(0xD390).as_bytes(),
            workspace_id.as_bytes(),
            [0_u8; 32].as_slice(),
        ],
    )?;
    conn.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
        params![
            workspace_id.as_bytes(),
            profile_uuid.as_bytes(),
            participation_uuid.as_bytes(),
        ],
    )?;
    conn.execute(
        "INSERT INTO ops_reviewer_participation_receipts(workspace_id,participation_id,profile_id,participant_id,request_id,revision,expected_revision,statement,withdraws_request_id,withdraws_revision,activity_id,agent_id,session_id,display_name,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,1,0,'INTERESTED',NULL,NULL,?6,?7,?8,'Independent Reviewer',10)",
        params![
            workspace_id.as_bytes(),
            participation_uuid.as_bytes(),
            profile_uuid.as_bytes(),
            participant_uuid.as_bytes(),
            receipt_req_id.as_bytes(),
            activity_uuid.as_bytes(),
            agent_uuid.as_bytes(),
            session_uuid.as_bytes(),
        ],
    )?;
    conn.execute(
        "INSERT INTO ops_reviewer_participation_heads(workspace_id,participation_id,current_revision,request_id) VALUES(?1,?2,1,?3)",
        params![
            workspace_id.as_bytes(),
            participation_uuid.as_bytes(),
            receipt_req_id.as_bytes(),
        ],
    )?;
    Ok(())
}

async fn publish_qualification_rule_over_http(
    backend: &RunningBackend,
    workspace_id: Uuid,
    domain: &str,
) -> TestResult {
    let scope = build_workspace_scope(workspace_id, Uuid::from_u128(0xD309), 1, 1)?;
    let (scope_status, _): (_, RegisteredWorkspaceScopeResponse) = request_json(
        backend,
        "POST",
        "/internal/v1/workspaces/scopes",
        &ScopeRegisterRequest {
            request_id: Uuid::now_v7().to_string(),
            expected_revision: 0,
            scope: scope.clone(),
        },
    )
    .await?;
    assert_eq!(scope_status, StatusCode::OK);

    let session_id = open_operator_session(backend).await?;
    let publish_path =
        format!("/internal/v1/workspaces/{workspace_id}/reviewer-qualification-rules/publish");
    let publish_req = ReviewerQualificationRuleMutationRequest {
        scope,
        session_id,
        rule_id: Uuid::from_u128(0xD395),
        request_id: Uuid::now_v7(),
        expected_current_generation: None,
        change: ReviewerQualificationRuleChangeWire::Publish {
            policy: ReviewerQualificationRulePolicyWire {
                domain: domain.to_owned(),
                permitted_scope: "workspace".to_owned(),
                evidence_requirements: vec![
                    QualificationEvidenceRequirementWire::ActualRuntimeProvenance {
                        source_owner: EvidenceSourceOwnerWire {
                            namespace: EvidenceOwnerNamespaceWire::RuntimeAdapter,
                            id: Uuid::from_u128(0xD396),
                        },
                        fact_predicates: vec![
                            RuntimeFactPredicateWire::Text {
                                fact: ActualRuntimeFactWire::Model,
                                allowed_values: vec!["gpt-6.1-sol".to_owned()],
                            },
                            RuntimeFactPredicateWire::Text {
                                fact: ActualRuntimeFactWire::ToolVersions,
                                allowed_values: vec!["rustc-1.98.0".to_owned()],
                            },
                            RuntimeFactPredicateWire::Text {
                                fact: ActualRuntimeFactWire::EnvironmentFingerprint,
                                allowed_values: vec!["darwin-arm64".to_owned()],
                            },
                        ],
                    },
                ],
            },
        },
    };
    let (pub_status, pub_resp): (_, ReviewerQualificationRuleMutationResponse) =
        request_json(backend, "POST", &publish_path, &publish_req).await?;
    assert_eq!(pub_status, StatusCode::OK);
    assert_eq!(pub_resp.generation.generation, 1);
    Ok(())
}

#[tokio::test]
async fn slice_3_independent_reviewer_assignment_verdict_and_diff_digest_gate_over_http()
-> TestResult {
    let fixture = ReviewerHttpFixture::build();
    let seed = |root: &Path| {
        seed_interested_reviewer_sqlite(root, fixture.workspace_id, &fixture.qualified_candidate)
    };
    let backend = RunningBackend::start_with_empty_http_scopes(
        "autonomy-slice3-reviewer",
        fixture.workspace_id,
        1,
        Some(&seed),
    )
    .await?;
    let domain = "rust-autonomy";
    publish_qualification_rule_over_http(&backend, fixture.workspace_id, domain).await?;

    assert_reviewer_assignment_rejections(&backend, &fixture, domain).await?;
    let accepted_digest = "c4".repeat(32);
    admit_qualified_reviewer_and_check_pre_gate(&backend, &fixture, domain, &accepted_digest)
        .await?;
    assert_reviewer_verdict_and_completion_gate(&backend, &fixture, &accepted_digest).await?;

    backend.shutdown().await
}

async fn assert_reviewer_assignment_rejections(
    backend: &RunningBackend,
    fixture: &ReviewerHttpFixture,
    domain: &str,
) -> TestResult {
    let assign_path = format!(
        "/internal/v1/workspaces/{}/reviewer-assignments",
        fixture.workspace_id
    );
    for (candidate, expected_code) in [
        (&fixture.self_executor_candidate, "SELF_REVIEW_PROFILE"),
        (
            &fixture.self_leader_candidate,
            "SELF_REVIEW_ASSIGNING_LEADER",
        ),
        (&fixture.uninterested_candidate, "NO_QUALIFIED_REVIEWER"),
    ] {
        let (status, resp): (_, ReviewerOperationResponse) = request_json(
            backend,
            "POST",
            &assign_path,
            &AssignReviewerRequest {
                request_id: Uuid::now_v7().to_string(),
                correlation: fixture.correlation.clone(),
                required_domain: domain.to_owned(),
                executor: fixture.executor.clone(),
                assigning_leader: fixture.leader.clone(),
                candidates: vec![candidate.clone()],
            },
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(!resp.admitted);
        assert_eq!(resp.rejection_code.as_deref(), Some(expected_code));
        assert!(resp.assignment.is_none());
    }
    Ok(())
}

async fn admit_qualified_reviewer_and_check_pre_gate(
    backend: &RunningBackend,
    fixture: &ReviewerHttpFixture,
    domain: &str,
    accepted_digest: &str,
) -> TestResult {
    let ws = fixture.workspace_id;
    let assign_path = format!("/internal/v1/workspaces/{ws}/reviewer-assignments");
    let gate_path = format!("/internal/v1/workspaces/{ws}/node-completion-gate");

    let (assign_status, assigned): (_, ReviewerOperationResponse) = request_json(
        backend,
        "POST",
        &assign_path,
        &AssignReviewerRequest {
            request_id: Uuid::now_v7().to_string(),
            correlation: fixture.correlation.clone(),
            required_domain: domain.to_owned(),
            executor: fixture.executor.clone(),
            assigning_leader: fixture.leader.clone(),
            candidates: vec![fixture.qualified_candidate.clone()],
        },
    )
    .await?;
    assert_eq!(assign_status, StatusCode::OK);
    assert!(assigned.admitted);
    let assignment = assigned.assignment.ok_or("missing admitted assignment")?;
    assert_eq!(assignment.revision, 1);
    assert_eq!(assignment.rule_generation, Some(1));

    let (pre_gate_status, pre_gate): (_, ReviewerOperationResponse) = request_json(
        backend,
        "POST",
        &gate_path,
        &VerifyNodeCompletionGateRequest {
            correlation: fixture.correlation.clone(),
            current_handoff_diff_digest_hex: accepted_digest.to_owned(),
        },
    )
    .await?;
    assert_eq!(pre_gate_status, StatusCode::CONFLICT);
    assert_eq!(
        pre_gate.rejection_code.as_deref(),
        Some("REVIEW_NOT_ACCEPTED")
    );
    Ok(())
}

async fn assert_reviewer_verdict_and_completion_gate(
    backend: &RunningBackend,
    fixture: &ReviewerHttpFixture,
    accepted_digest: &str,
) -> TestResult {
    let ws = fixture.workspace_id;
    let verdict_path = format!("/internal/v1/workspaces/{ws}/reviewer-verdicts");
    let gate_path = format!("/internal/v1/workspaces/{ws}/node-completion-gate");

    let (mismatch_status, mismatch_resp): (_, ReviewerOperationResponse) = request_json(
        backend,
        "POST",
        &verdict_path,
        &RecordReviewVerdictRequest {
            request_id: Uuid::now_v7().to_string(),
            correlation: fixture.correlation.clone(),
            expected_revision: 1,
            reviewer_agent_id: Uuid::now_v7().to_string(),
            reviewer_session_id: fixture.qualified_candidate.session_id.clone(),
            reviewer: None,
            verdict: "accepted".to_owned(),
            handoff_diff_digest_hex: accepted_digest.to_owned(),
        },
    )
    .await?;
    assert_eq!(mismatch_status, StatusCode::CONFLICT);
    assert!(!mismatch_resp.admitted);
    assert_eq!(
        mismatch_resp.rejection_code.as_deref(),
        Some("REVIEWER_IDENTITY_MISMATCH")
    );

    let (verdict_status, verdict_resp): (_, ReviewerOperationResponse) = request_json(
        backend,
        "POST",
        &verdict_path,
        &RecordReviewVerdictRequest {
            request_id: Uuid::now_v7().to_string(),
            correlation: fixture.correlation.clone(),
            expected_revision: 1,
            reviewer_agent_id: fixture.qualified_candidate.agent_id.clone(),
            reviewer_session_id: fixture.qualified_candidate.session_id.clone(),
            reviewer: None,
            verdict: "accepted".to_owned(),
            handoff_diff_digest_hex: accepted_digest.to_owned(),
        },
    )
    .await?;
    assert_eq!(verdict_status, StatusCode::OK);
    assert!(verdict_resp.admitted);

    let (pass_status, pass_gate): (_, ReviewerOperationResponse) = request_json(
        backend,
        "POST",
        &gate_path,
        &VerifyNodeCompletionGateRequest {
            correlation: fixture.correlation.clone(),
            current_handoff_diff_digest_hex: accepted_digest.to_owned(),
        },
    )
    .await?;
    assert_eq!(pass_status, StatusCode::OK);
    assert!(pass_gate.admitted);

    let mutated_digest = "d5".repeat(32);
    let (drift_status, drift_gate): (_, ReviewerOperationResponse) = request_json(
        backend,
        "POST",
        &gate_path,
        &VerifyNodeCompletionGateRequest {
            correlation: fixture.correlation.clone(),
            current_handoff_diff_digest_hex: mutated_digest,
        },
    )
    .await?;
    assert_eq!(drift_status, StatusCode::CONFLICT);
    assert!(!drift_gate.admitted);
    assert_eq!(
        drift_gate.rejection_code.as_deref(),
        Some("VERDICT_DIFF_DIGEST_MISMATCH")
    );
    Ok(())
}

fn sample_snapshot_request(
    profile_id: Uuid,
    profile_revision: i64,
    prompt_digest_hex: &str,
    attempt: u32,
) -> PrepareExecutionSnapshotRequest {
    PrepareExecutionSnapshotRequest {
        request_id: Uuid::now_v7().to_string(),
        snapshot_id: Uuid::now_v7().to_string(),
        goal_id: Uuid::from_u128(0xE502).to_string(),
        dag_revision: 1,
        node_id: Uuid::from_u128(0xE503).to_string(),
        assignment_id: Uuid::from_u128(0xE504).to_string(),
        attempt,
        activity_id: Uuid::from_u128(0xE505).to_string(),
        profile_id: profile_id.to_string(),
        profile_revision,
        resolved_engine: "codex".to_owned(),
        resolved_model: "gpt-6-astra".to_owned(),
        resolved_reasoning: Some("high".to_owned()),
        role_prompt_digest_hex: Some(prompt_digest_hex.to_owned()),
        worktree_path: "/var/tmp/forge-worktrees/autonomy-node-1".to_owned(),
        lease_id: Uuid::from_u128(0xE506).to_string(),
        lease_revision: 1,
        lease_expires_at_ms: 90_000,
        mcp_grant: Some(McpGrantWire {
            token_id: Uuid::from_u128(0xE507).to_string(),
            issued_at_ms: 100,
            expires_at_ms: 90_000,
        }),
        permissions: ExecutionPermissionsWire {
            allow_workspace_write: true,
            allow_local_shell: true,
            allow_network: false,
        },
        timeouts: ExecutionTimeoutsWire {
            startup_timeout_ms: 5_000,
            execution_timeout_ms: 60_000,
            termination_grace_ms: 5_000,
        },
    }
}

#[tokio::test]
async fn slice_5_execution_snapshot_rejects_secret_fields_ssd_worktree_mismatches_and_commits_prepared()
-> TestResult {
    let backend = RunningBackend::start_basic("autonomy-slice5-snapshots").await?;
    let prompt_digest = "e6".repeat(32);
    let (_, created): (_, AgentProfileCreateResponse) = request_json(
        &backend,
        "POST",
        "/internal/v1/agent-profiles",
        &AgentProfileCreateRequest {
            request_id: Uuid::now_v7().to_string(),
            stable_name: "snapshot-worker".to_owned(),
            display_name: "Snapshot Worker".to_owned(),
            preferred_model: Some("gpt-6-astra".to_owned()),
            preferred_reasoning: Some("high".to_owned()),
            base_capabilities: vec!["rust".to_owned()],
            execution_preference: Some(ExecutionPreferenceWire {
                cli_engine: "codex".to_owned(),
                preferred_model: "gpt-6-astra".to_owned(),
                preferred_reasoning: Some("high".to_owned()),
                capabilities: vec!["rust".to_owned()],
                role_prompt_digest_hex: Some(prompt_digest.clone()),
            }),
        },
    )
    .await?;
    let profile_id = created.profile.profile_id;
    let profile_rev = created.profile.revision;
    let workspace_id = Uuid::from_u128(0xE501);
    let path = format!("/internal/v1/workspaces/{workspace_id}/execution-snapshots");

    assert_secret_grant_and_validation_rejections(
        &backend,
        &path,
        profile_id,
        profile_rev,
        &prompt_digest,
    )
    .await?;

    let valid_req = sample_snapshot_request(profile_id, profile_rev, &prompt_digest, 4);
    let expected_snapshot = Uuid::parse_str(&valid_req.snapshot_id)?;
    let (commit_status, committed): (_, ExecutionSnapshotOutcomeResponse) =
        request_json(&backend, "POST", &path, &valid_req).await?;
    assert_eq!(commit_status, StatusCode::OK);
    assert!(committed.committed);
    assert!(committed.rejection_code.is_none());
    let record = committed
        .record
        .ok_or("missing committed LaunchLedgerRecordWire")?;
    assert_eq!(record.snapshot_id, expected_snapshot);
    assert_eq!(record.workspace_id, workspace_id);
    assert_eq!(record.attempt, 4);
    assert_eq!(record.revision, 1);
    assert_eq!(record.state.status, "PREPARED");
    assert!(record.state.ownership.is_none());

    backend.shutdown().await
}

async fn assert_secret_grant_and_validation_rejections(
    backend: &RunningBackend,
    path: &str,
    profile_id: Uuid,
    profile_rev: i64,
    prompt_digest: &str,
) -> TestResult {
    let mut raw_secret_body = serde_json::to_value(sample_snapshot_request(
        profile_id,
        profile_rev,
        prompt_digest,
        1,
    ))?;
    raw_secret_body["mcpGrant"]["bearerSecret"] = Value::String("raw-secret-token".to_owned());
    let encoded_secret_body = serde_json::to_vec(&raw_secret_body)?;
    let (secret_status, _) = send_http_raw(
        backend.address,
        "POST",
        path,
        Some(&backend.bearer),
        &encoded_secret_body,
    )
    .await?;
    assert!(
        matches!(
            secret_status,
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
        ),
        "expected 400 or 422 for secret field in mcpGrant, got {secret_status}"
    );

    let mut ssd_req = sample_snapshot_request(profile_id, profile_rev, prompt_digest, 1);
    "/Volumes/256.SSD/RustBuild/orchestration/worktree".clone_into(&mut ssd_req.worktree_path);
    let mut stale_rev_req = sample_snapshot_request(profile_id, profile_rev + 1, prompt_digest, 2);
    stale_rev_req.request_id = Uuid::now_v7().to_string();
    let mut engine_mismatch_req =
        sample_snapshot_request(profile_id, profile_rev, prompt_digest, 3);
    "agy".clone_into(&mut engine_mismatch_req.resolved_engine);

    for (req, expected_code) in [
        (&ssd_req, "FORBIDDEN_SSD_BUILD_WORKTREE"),
        (&stale_rev_req, "PROFILE_REVISION_MISMATCH"),
        (&engine_mismatch_req, "ENGINE_MISMATCH"),
    ] {
        let (status, outcome): (_, ExecutionSnapshotOutcomeResponse) =
            request_json(backend, "POST", path, req).await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(!outcome.committed);
        assert_eq!(outcome.rejection_code.as_deref(), Some(expected_code));
        assert!(outcome.record.is_none());
    }
    Ok(())
}
