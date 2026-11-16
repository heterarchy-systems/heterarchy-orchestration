//! End-to-end loopback HTTP acceptance tests for Native Swarm coordination (Slice 5).
//!
//! Covers:
//! 1. `Standalone` Swarm full lifecycle (`Coordinator` + 2 `Share` authors + `#channel`
//!    isolation + `Inherit` adversarial `Verifier` + `converged_diff_digest` quorum gate
//!    -> `Settled`), plus bearer auth, operator policy, `deny_unknown_fields`, cross-workspace
//!    isolation, and `/Volumes/256.SSD/RustBuild` worktree rejection.
//! 2. `DagNodeHybrid` micro-swarm lifecycle (halting at `CandidateReady` and failing closed
//!    with `409` on every direct `Settled` bypass attempt).
//! 3. Sybil helper lineage self-review rejection (worker spawning `Verifier` -> `403`,
//!    non-`Inherit` `Verifier` -> `409`, child helper or lineage-overlapping `Verifier`
//!    reviewing parent/ancestor work -> `409`, plus `spawn_depth <= 4` and cascade kill).
//! 4. Two-phase watermark crash retention + lossless `SuspendedQuota` model switch +
//!    single-flight `32`-msg / `64 KiB` doorbell coalescing + deterministic pure-Rust
//!    Stall & Livelock breaker over HTTP.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use forge_domain::{AuthenticatedPrincipal, DirectTokenId, TimestampMillis};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
    McpTokenWorker,
};
use heterarchy_orchestration_backend::{
    BackendError, SwarmHostOptions, serve_with_runtime, serve_with_runtime_and_swarm_options,
    wire::{
        AgentProfileCreateRequest, AgentProfileCreateResponse, ExecutionPreferenceWire,
        autonomy_loop::{ExecutionAuthorWire, PrincipalIdentityWire},
        swarm::{
            AddSwarmMemberRequest, ClaimSwarmWakeupRequest, CompleteSwarmTurnRequest,
            ConvergeSwarmRequest, ConvergeSwarmResponse, CreateSwarmChannelRequest,
            CreateSwarmRunRequest, CreateSwarmRunResponse, CreateSwarmVerificationPanelRequest,
            DagNodeHybridTargetWire, FailSwarmTurnRequest, KillSwarmMemberRequest,
            KillSwarmMemberResponse, PostSwarmMessageRequest, PostSwarmMessageResponse,
            RecordSwarmVerdictRequest, SettleSwarmRequest, SubscribeSwarmChannelRequest,
            SubscribeSwarmChannelResponse, SwarmBoardPageResponse, SwarmChannelWire,
            SwarmErrorResponse, SwarmMemberWire, SwarmModelSwitchResponse,
            SwarmRunInspectionResponse, SwarmRunListResponse, SwarmRunModeWire,
            SwarmStallCheckRequest, SwarmStallCheckResponse, SwarmVerdictWire,
            SwarmWakeupBatchWire, SwitchSwarmMemberModelRequest,
        },
    },
};
use heterarchy_orchestration_operations::{
    BrokerClusterConfig, MAX_SNAPSHOT_COALESCED_BYTES, MAX_SNAPSHOT_COALESCED_MESSAGES,
};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime,
    access::OperatorPolicy,
    execution_supervisor::{CommandOverride, LocalProcessRunnerConfig},
};
use hyper::StatusCode;
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
const TOKEN_PEPPER: [u8; 32] = [0x6B; 32];

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
    if reader.read_line(&mut line).await? == 0 {
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

struct IssuedTokens {
    token_runtime: McpTokenRuntime,
    operator_bearer: String,
    non_operator_bearer: String,
    operator_principal: AuthenticatedPrincipal,
}

fn start_tokens(auth_dir: &Path) -> TestResult<IssuedTokens> {
    fs::create_dir_all(auth_dir)?;
    let catalog = McpTokenCatalog::open(auth_dir.join("tokens.sqlite3"))?;
    let (client, worker) = McpTokenWorker::start(McpTokenService::new(TOKEN_PEPPER, catalog))?;
    let operator_issued = client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("swarm-operator".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let non_operator_issued = client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("swarm-non-operator".to_owned())?,
        TimestampMillis::new(2)?,
    ))?;
    let operator_bearer = format!("Bearer {}", operator_issued.reveal().as_str());
    let non_operator_bearer = format!("Bearer {}", non_operator_issued.reveal().as_str());
    let operator_principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
        operator_issued.metadata().id().into_uuid(),
    ));
    Ok(IssuedTokens {
        token_runtime: McpTokenRuntime::from_parts(client, worker),
        operator_bearer,
        non_operator_bearer,
        operator_principal,
    })
}

fn reserve_loopback_addr() -> TestResult<SocketAddr> {
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr)
}

struct RunningSwarmBackend {
    _dir: TempDir,
    _broker: FakeSingleNodeBroker,
    runtime: Arc<OrchestrationRuntime>,
    runtime_root: PathBuf,
    token_runtime: McpTokenRuntime,
    address: SocketAddr,
    bearer: String,
    non_operator_bearer: String,
    cancellation: CancellationToken,
    server: JoinHandle<Result<(), BackendError>>,
}

impl RunningSwarmBackend {
    async fn start(label: &str) -> TestResult<Self> {
        let dir = TempDir::create(label)?;
        let broker = FakeSingleNodeBroker::start().await?;
        let tokens = start_tokens(&dir.path().join("auth"))?;
        let runtime_root = dir.path().join("runtime");
        fs::create_dir_all(&runtime_root)?;
        let runtime = Arc::new(
            OrchestrationRuntime::open_with_broker_cluster(
                &runtime_root,
                broker.cluster_config()?,
            )?
            .with_operator_policy(OperatorPolicy::new(vec![tokens.operator_principal])?),
        );
        let address = reserve_loopback_addr()?;
        let cancellation = CancellationToken::new();
        let server = tokio::spawn(serve_with_runtime(
            Arc::clone(&runtime),
            address,
            tokens.token_runtime.client(),
            None,
            cancellation.clone(),
        ));
        wait_for_ready(address).await?;
        Ok(Self {
            _dir: dir,
            _broker: broker,
            runtime,
            runtime_root,
            token_runtime: tokens.token_runtime,
            address,
            bearer: tokens.operator_bearer,
            non_operator_bearer: tokens.non_operator_bearer,
            cancellation,
            server,
        })
    }

    async fn start_with_worker_shell(
        label: &str,
        non_ssd_dir: &Path,
        shell_script: &str,
    ) -> TestResult<(Self, PathBuf)> {
        let dir = TempDir::create(label)?;
        let broker = FakeSingleNodeBroker::start().await?;
        let tokens = start_tokens(&dir.path().join("auth"))?;
        let raw_reveal = tokens
            .operator_bearer
            .strip_prefix("Bearer ")
            .ok_or("expected Bearer prefix")?;
        let worker_bearer_path = non_ssd_dir.join("worker.token");
        fs::write(&worker_bearer_path, format!("{raw_reveal}\n"))?;
        fs::set_permissions(&worker_bearer_path, fs::Permissions::from_mode(0o600))?;
        let worker_bearer_path = worker_bearer_path.canonicalize()?;

        let runtime_root = dir.path().join("runtime");
        fs::create_dir_all(&runtime_root)?;
        let runtime = Arc::new(
            OrchestrationRuntime::open_with_broker_cluster(
                &runtime_root,
                broker.cluster_config()?,
            )?
            .with_operator_policy(OperatorPolicy::new(vec![tokens.operator_principal])?),
        );
        let runner_cfg = LocalProcessRunnerConfig::new()
            .with_command_override(CommandOverride::new("/bin/sh", ["-c", shell_script]));
        let swarm_options = SwarmHostOptions::default()
            .with_worker_bearer_file(worker_bearer_path.clone())
            .with_process_runner_config(runner_cfg);

        let address = reserve_loopback_addr()?;
        let cancellation = CancellationToken::new();
        let server = tokio::spawn(serve_with_runtime_and_swarm_options(
            Arc::clone(&runtime),
            address,
            tokens.token_runtime.client(),
            None,
            swarm_options,
            cancellation.clone(),
        ));
        wait_for_ready(address).await?;
        Ok((
            Self {
                _dir: dir,
                _broker: broker,
                runtime,
                runtime_root,
                token_runtime: tokens.token_runtime,
                address,
                bearer: tokens.operator_bearer,
                non_operator_bearer: tokens.non_operator_bearer,
                cancellation,
                server,
            },
            worker_bearer_path,
        ))
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

fn find_double_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
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

async fn request_json<Req, Resp>(
    backend: &RunningSwarmBackend,
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
    backend: &RunningSwarmBackend,
    path: &str,
) -> TestResult<(StatusCode, Resp)> {
    let (status, body) =
        send_http_raw(backend.address, "GET", path, Some(&backend.bearer), b"").await?;
    let parsed = serde_json::from_slice::<Resp>(&body).map_err(|err| {
        let preview = String::from_utf8_lossy(&body);
        format!("failed decoding GET {path} (status {status}): {err}; body={preview}")
    })?;
    Ok((status, parsed))
}

fn oid_hex(fill: char) -> String {
    std::iter::repeat_n(fill, 40).collect()
}

fn digest_hex(byte_hex: &str) -> String {
    byte_hex.repeat(32)
}

fn author_identity_wire(seed: u128) -> ExecutionAuthorWire {
    ExecutionAuthorWire {
        profile_id: Uuid::from_u128(0xA000 + seed).to_string(),
        participant_id: Uuid::from_u128(0xB000 + seed).to_string(),
        principal: PrincipalIdentityWire {
            kind: "direct_token".to_owned(),
            id: Uuid::from_u128(0xC000 + seed).to_string(),
        },
    }
}

fn standalone_run_request(request_id: Uuid, title: &str) -> CreateSwarmRunRequest {
    CreateSwarmRunRequest {
        request_id: request_id.to_string(),
        title: title.to_owned(),
        mode: SwarmRunModeWire {
            kind: "STANDALONE".to_owned(),
            hybrid_target: None,
        },
        max_members: 16,
        max_spawn_depth: 4,
        max_total_turns: 256,
        required_verifier_quorum: 1,
        allow_worker_all_mention: false,
        baseline_commit_oid: oid_hex('1'),
        baseline_tree_blake3_hex: digest_hex("a1"),
        scope: None,
    }
}

async fn add_member_over_http(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    swarm_run_id: Uuid,
    req: &AddSwarmMemberRequest,
) -> TestResult<SwarmMemberWire> {
    let path = format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members");
    let (status, member): (_, SwarmMemberWire) = request_json(backend, "POST", &path, req).await?;
    assert_eq!(status, StatusCode::CREATED);
    Ok(member)
}

fn find_member(
    inspection: &SwarmRunInspectionResponse,
    member_id: Uuid,
) -> Result<&SwarmMemberWire, &'static str> {
    inspection
        .members
        .iter()
        .find(|m| m.member_id == member_id)
        .ok_or("member missing from inspection response")
}

// ============================================================================
// E2E Scenario 1: Standalone Swarm Full Lifecycle + Auth / Boundary Guards
// ============================================================================

#[tokio::test]
async fn e2e_1_standalone_swarm_full_lifecycle_channel_isolation_and_quorum_settlement()
-> TestResult {
    let backend = RunningSwarmBackend::start("swarm-e2e1-standalone").await?;
    let workspace_id = Uuid::from_u128(0xE101);

    assert_auth_and_unknown_field_guards(&backend, workspace_id).await?;

    let create_req_id = Uuid::now_v7();
    let create_path = format!("/v1/workspaces/{workspace_id}/swarms");
    let create_req = standalone_run_request(create_req_id, "Standalone E2E Lifecycle");
    let (create_status, created): (_, CreateSwarmRunResponse) =
        request_json(&backend, "POST", &create_path, &create_req).await?;
    assert_eq!(create_status, StatusCode::CREATED);
    assert_eq!(created.run.state, "ACTIVE");
    assert!(!created.run.replayed);
    assert_eq!(created.main_channel.name, "main");
    let run_id = created.run.swarm_run_id;
    let main_channel_id = created.main_channel.channel_id;

    let (replay_status, replayed): (_, CreateSwarmRunResponse) =
        request_json(&backend, "POST", &create_path, &create_req).await?;
    assert_eq!(replay_status, StatusCode::OK);
    assert!(replayed.run.replayed);
    assert_eq!(replayed.run.swarm_run_id, run_id);

    let (list_status, listed): (_, SwarmRunListResponse) = get_json(&backend, &create_path).await?;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(listed.runs.len(), 1);

    let foreign_ws = Uuid::from_u128(0xE199);
    let (foreign_status, _): (_, Value) = get_json(
        &backend,
        &format!("/v1/workspaces/{foreign_ws}/swarms/{run_id}"),
    )
    .await?;
    assert_eq!(foreign_status, StatusCode::NOT_FOUND);

    let fixture = setup_standalone_roster_and_channels(&backend, workspace_id, run_id).await?;

    exercise_channel_isolation_and_author_turns(
        &backend,
        workspace_id,
        run_id,
        main_channel_id,
        fixture.backend_ch.channel_id,
        &fixture.coord,
        &fixture.first_author,
        &fixture.second_author,
    )
    .await?;

    exercise_verdict_quorum_and_standalone_settlement(
        &backend,
        workspace_id,
        run_id,
        fixture.verifier.member_id,
    )
    .await?;

    backend.shutdown().await
}

async fn assert_auth_and_unknown_field_guards(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
) -> TestResult {
    let path = format!("/v1/workspaces/{workspace_id}/swarms");
    let body = serde_json::to_vec(&standalone_run_request(Uuid::now_v7(), "Auth Check"))?;

    let (unauth_status, _) = send_http_raw(backend.address, "POST", &path, None, &body).await?;
    assert_eq!(unauth_status, StatusCode::UNAUTHORIZED);

    let (forbid_status, forbid_raw) = send_http_raw(
        backend.address,
        "POST",
        &path,
        Some(&backend.non_operator_bearer),
        &body,
    )
    .await?;
    assert_eq!(forbid_status, StatusCode::FORBIDDEN);
    let forbid_err: SwarmErrorResponse = serde_json::from_slice(&forbid_raw)?;
    assert_eq!(forbid_err.code, "FORBIDDEN");

    let mut unknown_json =
        serde_json::to_value(standalone_run_request(Uuid::now_v7(), "Unknown Field"))?;
    unknown_json["rawBearerSecret"] = Value::String("leaked-token".to_owned());
    let (unknown_status, _) = send_http_raw(
        backend.address,
        "POST",
        &path,
        Some(&backend.bearer),
        &serde_json::to_vec(&unknown_json)?,
    )
    .await?;
    assert!(matches!(
        unknown_status,
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
    ));
    Ok(())
}

struct StandaloneFixture {
    coord: SwarmMemberWire,
    first_author: SwarmMemberWire,
    second_author: SwarmMemberWire,
    verifier: SwarmMemberWire,
    backend_ch: SwarmChannelWire,
}

async fn assert_ssd_worktree_rejected_over_http(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
) -> TestResult {
    let ssd_bad_req = AddSwarmMemberRequest {
        member_id: Uuid::now_v7().to_string(),
        roster_name: "ssd-violator".to_owned(),
        role_kind: "WORKER".to_owned(),
        parent_member_id: None,
        workspace_mode: "SHARE".to_owned(),
        worktree_path: Some("/Volumes/256.SSD/RustBuild/orchestration/bad-wt".to_owned()),
        identity: author_identity_wire(90),
        initial_channels: Vec::new(),
    };
    let members_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members");
    let (ssd_status, ssd_err): (_, SwarmErrorResponse) =
        request_json(backend, "POST", &members_path, &ssd_bad_req).await?;
    assert_eq!(ssd_status, StatusCode::CONFLICT);
    assert_eq!(ssd_err.code, "FORBIDDEN_SSD_BUILD_WORKTREE");
    Ok(())
}

async fn setup_standalone_roster_and_channels(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
) -> TestResult<StandaloneFixture> {
    assert_ssd_worktree_rejected_over_http(backend, workspace_id, run_id).await?;
    let coord = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE110).to_string(),
            roster_name: "coordinator".to_owned(),
            role_kind: "COORDINATOR".to_owned(),
            parent_member_id: None,
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: author_identity_wire(10),
            initial_channels: Vec::new(),
        },
    )
    .await?;

    let channels_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/channels");
    let (ch_status, backend_ch): (_, SwarmChannelWire) = request_json(
        backend,
        "POST",
        &channels_path,
        &CreateSwarmChannelRequest {
            channel_id: Uuid::from_u128(0xE120).to_string(),
            name: "backend".to_owned(),
            purpose: Some("Backend author slice".to_owned()),
            created_by_member_id: Some(coord.member_id.to_string()),
        },
    )
    .await?;
    assert_eq!(ch_status, StatusCode::CREATED);

    let first_author = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE111).to_string(),
            roster_name: "author-a".to_owned(),
            role_kind: "WORKER".to_owned(),
            parent_member_id: Some(coord.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e1-author-a".to_owned()),
            identity: author_identity_wire(11),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    subscribe_member_to_channel(
        backend,
        workspace_id,
        run_id,
        backend_ch.channel_id,
        first_author.member_id,
    )
    .await?;

    let second_author = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE112).to_string(),
            roster_name: "author-b".to_owned(),
            role_kind: "WORKER".to_owned(),
            parent_member_id: Some(coord.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e1-author-b".to_owned()),
            identity: author_identity_wire(12),
            initial_channels: Vec::new(),
        },
    )
    .await?;

    let verifier = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE113).to_string(),
            roster_name: "adversarial-verifier".to_owned(),
            role_kind: "VERIFIER".to_owned(),
            parent_member_id: Some(coord.member_id.to_string()),
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: author_identity_wire(99),
            initial_channels: Vec::new(),
        },
    )
    .await?;

    Ok(StandaloneFixture {
        coord,
        first_author,
        second_author,
        verifier,
        backend_ch,
    })
}

async fn subscribe_member_to_channel(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    channel_id: Uuid,
    member_id: Uuid,
) -> TestResult {
    let sub_path = format!(
        "/v1/workspaces/{workspace_id}/swarms/{run_id}/channels/{channel_id}/subscriptions"
    );
    let (sub_status, sub_resp): (_, SubscribeSwarmChannelResponse) = request_json(
        backend,
        "POST",
        &sub_path,
        &SubscribeSwarmChannelRequest {
            member_id: member_id.to_string(),
        },
    )
    .await?;
    assert_eq!(sub_status, StatusCode::OK);
    assert!(sub_resp.newly_subscribed);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn exercise_channel_isolation_and_author_turns(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    main_channel_id: Uuid,
    backend_channel_id: Uuid,
    coord: &SwarmMemberWire,
    author_a: &SwarmMemberWire,
    author_b: &SwarmMemberWire,
) -> TestResult {
    let msg_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/messages");
    let inspect_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}");

    let (all_forbid_status, all_forbid_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &msg_path,
        &PostSwarmMessageRequest {
            message_id: Uuid::now_v7().to_string(),
            channel_id: Some(main_channel_id.to_string()),
            author_member_id: Some(author_a.member_id.to_string()),
            body: "@all unauthorized worker broadcast".to_owned(),
        },
    )
    .await?;
    assert_eq!(all_forbid_status, StatusCode::FORBIDDEN);
    assert_eq!(all_forbid_err.code, "WORKER_ALL_MENTION_FORBIDDEN");

    let (ch_msg_status, ch_msg): (_, PostSwarmMessageResponse) = request_json(
        backend,
        "POST",
        &msg_path,
        &PostSwarmMessageRequest {
            message_id: Uuid::now_v7().to_string(),
            channel_id: Some(backend_channel_id.to_string()),
            author_member_id: Some(coord.member_id.to_string()),
            body: "@#backend implement backend HTTP routes".to_owned(),
        },
    )
    .await?;
    assert_eq!(ch_msg_status, StatusCode::CREATED);
    assert_eq!(ch_msg.woken_member_ids, vec![author_a.member_id]);
    let seq1 = ch_msg.message.sequence;

    let (_, snap_after_ch): (_, SwarmRunInspectionResponse) =
        get_json(backend, &inspect_path).await?;
    assert_eq!(
        find_member(&snap_after_ch, author_a.member_id)?.member_state,
        "WAKE_QUEUED"
    );
    assert_eq!(
        find_member(&snap_after_ch, author_b.member_id)?.member_state,
        "WAITING"
    );

    let (direct_b_status, direct_b): (_, PostSwarmMessageResponse) = request_json(
        backend,
        "POST",
        &msg_path,
        &PostSwarmMessageRequest {
            message_id: Uuid::now_v7().to_string(),
            channel_id: None,
            author_member_id: Some(coord.member_id.to_string()),
            body: "@author-b implement wire contracts".to_owned(),
        },
    )
    .await?;
    assert_eq!(direct_b_status, StatusCode::CREATED);
    assert!(direct_b.message.sequence > seq1);

    let (_, backend_only_page): (_, SwarmBoardPageResponse) = get_json(
        backend,
        &format!("{msg_path}?sinceSequence=0&channelId={backend_channel_id}"),
    )
    .await?;
    assert_eq!(backend_only_page.messages.len(), 1);
    assert_eq!(backend_only_page.messages[0].sequence, seq1);

    let (_, incremental_page): (_, SwarmBoardPageResponse) =
        get_json(backend, &format!("{msg_path}?sinceSequence={seq1}")).await?;
    assert_eq!(incremental_page.messages.len(), 1);
    assert_eq!(
        incremental_page.messages[0].sequence,
        direct_b.message.sequence
    );

    complete_share_turn(
        backend,
        workspace_id,
        run_id,
        author_a.member_id,
        &oid_hex('2'),
        &digest_hex("b2"),
    )
    .await?;
    complete_share_turn(
        backend,
        workspace_id,
        run_id,
        author_b.member_id,
        &oid_hex('3'),
        &digest_hex("c3"),
    )
    .await?;
    Ok(())
}

async fn complete_share_turn(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    member_id: Uuid,
    commit_oid: &str,
    tree_hex: &str,
) -> TestResult {
    let claim_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{member_id}/turns/claim");
    let snap_id = Uuid::now_v7();
    let (claim_status, batch): (_, SwarmWakeupBatchWire) = request_json(
        backend,
        "POST",
        &claim_path,
        &ClaimSwarmWakeupRequest {
            snapshot_id: snap_id.to_string(),
        },
    )
    .await?;
    assert_eq!(claim_status, StatusCode::OK);
    assert_eq!(batch.member.member_state, "WORKING");
    assert_eq!(batch.member.active_snapshot_id, Some(snap_id));
    assert!(batch.inflight_target_seq > batch.committed_seen_seq);

    let complete_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{member_id}/turns/complete");
    let (done_status, done_member): (_, SwarmMemberWire) = request_json(
        backend,
        "POST",
        &complete_path,
        &CompleteSwarmTurnRequest {
            expected_snapshot_id: snap_id.to_string(),
            output_commit_oid: Some(commit_oid.to_owned()),
            output_tree_blake3_hex: Some(tree_hex.to_owned()),
        },
    )
    .await?;
    assert_eq!(done_status, StatusCode::OK);
    assert_eq!(done_member.member_state, "WAITING");
    assert_eq!(done_member.committed_seen_seq, batch.inflight_target_seq);
    assert_eq!(
        done_member.last_output_commit_oid.as_deref(),
        Some(commit_oid)
    );
    Ok(())
}

async fn exercise_verdict_quorum_and_standalone_settlement(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    verifier_id: Uuid,
) -> TestResult {
    let converge_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/converge");
    let verdicts_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/verdicts");
    let inspect_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}");
    let converged_diff = digest_hex("d4");
    let converged_tree = digest_hex("e5");
    let converged_oid = oid_hex('4');

    let (no_verdict_status, no_verdict_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &converge_path,
        &ConvergeSwarmRequest {
            request_id: Uuid::now_v7().to_string(),
            converged_commit_oid: converged_oid.clone(),
            converged_tree_blake3_hex: converged_tree.clone(),
            converged_diff_digest_hex: converged_diff.clone(),
            target_state: None,
            stage_candidate_only: false,
        },
    )
    .await?;
    assert_eq!(no_verdict_status, StatusCode::CONFLICT);
    assert_eq!(no_verdict_err.code, "REVIEW_NOT_ACCEPTED");

    let (_, still_active): (_, SwarmRunInspectionResponse) =
        get_json(backend, &inspect_path).await?;
    assert_eq!(still_active.run.state, "ACTIVE");

    let stale_diff = digest_hex("99");
    let (stale_v_status, _): (_, SwarmVerdictWire) = request_json(
        backend,
        "POST",
        &verdicts_path,
        &RecordSwarmVerdictRequest {
            request_id: Uuid::now_v7().to_string(),
            panel_id: None,
            verifier_member_id: verifier_id.to_string(),
            outcome: "ACCEPTED".to_owned(),
            reviewed_diff_digest_hex: stale_diff,
            rationale: "Reviewed older candidate".to_owned(),
        },
    )
    .await?;
    assert_eq!(stale_v_status, StatusCode::CREATED);

    let (mismatch_status, mismatch_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &converge_path,
        &ConvergeSwarmRequest {
            request_id: Uuid::now_v7().to_string(),
            converged_commit_oid: converged_oid.clone(),
            converged_tree_blake3_hex: converged_tree.clone(),
            converged_diff_digest_hex: converged_diff.clone(),
            target_state: None,
            stage_candidate_only: false,
        },
    )
    .await?;
    assert_eq!(mismatch_status, StatusCode::CONFLICT);
    assert_eq!(mismatch_err.code, "VERDICT_DIFF_DIGEST_MISMATCH");

    let matching_verdict_req = RecordSwarmVerdictRequest {
        request_id: Uuid::now_v7().to_string(),
        panel_id: None,
        verifier_member_id: verifier_id.to_string(),
        outcome: "ACCEPTED".to_owned(),
        reviewed_diff_digest_hex: converged_diff.clone(),
        rationale: "3-way merge diff verified clean".to_owned(),
    };
    let (v_status, verdict): (_, SwarmVerdictWire) =
        request_json(backend, "POST", &verdicts_path, &matching_verdict_req).await?;
    assert_eq!(v_status, StatusCode::CREATED);
    assert!(!verdict.replayed);

    let (v_replay_status, v_replay): (_, SwarmVerdictWire) =
        request_json(backend, "POST", &verdicts_path, &matching_verdict_req).await?;
    assert_eq!(v_replay_status, StatusCode::OK);
    assert!(v_replay.replayed);

    let settle_req = ConvergeSwarmRequest {
        request_id: Uuid::now_v7().to_string(),
        converged_commit_oid: converged_oid,
        converged_tree_blake3_hex: converged_tree,
        converged_diff_digest_hex: converged_diff.clone(),
        target_state: None,
        stage_candidate_only: false,
    };
    let (conv_status, converged): (_, ConvergeSwarmResponse) =
        request_json(backend, "POST", &converge_path, &settle_req).await?;
    assert_eq!(conv_status, StatusCode::OK);
    assert!(converged.settled);
    assert_eq!(converged.run.state, "SETTLED");
    let gate = converged.gate.ok_or("expected gate proof on settlement")?;
    assert_eq!(gate.converged_diff_digest_hex, converged_diff);
    assert_eq!(gate.accepted_verdicts.len(), 1);

    let (conv_replay_status, conv_replay): (_, ConvergeSwarmResponse) =
        request_json(backend, "POST", &converge_path, &settle_req).await?;
    assert_eq!(conv_replay_status, StatusCode::OK);
    assert!(conv_replay.run.replayed);
    assert_eq!(conv_replay.run.state, "SETTLED");
    Ok(())
}

// ============================================================================
// E2E Scenario 2: DagNodeHybrid Halts at CandidateReady & Rejects Settled Bypass
// ============================================================================

#[tokio::test]
async fn e2e_2_dag_node_hybrid_halts_at_candidate_ready_and_rejects_settled_bypass() -> TestResult {
    let backend = RunningSwarmBackend::start("swarm-e2e2-hybrid").await?;
    let workspace_id = Uuid::from_u128(0xE201);
    let (run_id, verifier_id) = setup_hybrid_run_and_triad(&backend, workspace_id).await?;

    let diff_hex = digest_hex("66");
    let tree_hex = digest_hex("55");
    let commit_oid = oid_hex('b');
    let verdicts_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/verdicts");
    let (v_status, _): (_, SwarmVerdictWire) = request_json(
        &backend,
        "POST",
        &verdicts_path,
        &RecordSwarmVerdictRequest {
            request_id: Uuid::now_v7().to_string(),
            panel_id: None,
            verifier_member_id: verifier_id.to_string(),
            outcome: "ACCEPTED".to_owned(),
            reviewed_diff_digest_hex: diff_hex.clone(),
            rationale: "Hybrid node candidate approved".to_owned(),
        },
    )
    .await?;
    assert_eq!(v_status, StatusCode::CREATED);

    assert_hybrid_candidate_ready_and_settle_rejections(
        &backend,
        workspace_id,
        run_id,
        &commit_oid,
        &tree_hex,
        &diff_hex,
    )
    .await?;

    backend.shutdown().await
}

async fn setup_hybrid_run_and_triad(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
) -> TestResult<(Uuid, Uuid)> {
    let create_path = format!("/v1/workspaces/{workspace_id}/swarms");
    let hybrid_req = CreateSwarmRunRequest {
        request_id: Uuid::now_v7().to_string(),
        title: "DagNodeHybrid Micro-Swarm".to_owned(),
        mode: SwarmRunModeWire {
            kind: "DAG_NODE_HYBRID".to_owned(),
            hybrid_target: Some(DagNodeHybridTargetWire {
                goal_id: Uuid::from_u128(0xE210).to_string(),
                dag_revision: 1,
                node_id: Uuid::from_u128(0xE211).to_string(),
                declared_write_manifest_blake3_hex: digest_hex("44"),
            }),
        },
        max_members: 8,
        max_spawn_depth: 2,
        max_total_turns: 64,
        required_verifier_quorum: 1,
        allow_worker_all_mention: false,
        baseline_commit_oid: oid_hex('a'),
        baseline_tree_blake3_hex: digest_hex("11"),
        scope: None,
    };
    let (create_status, created): (_, CreateSwarmRunResponse) =
        request_json(backend, "POST", &create_path, &hybrid_req).await?;
    assert_eq!(create_status, StatusCode::CREATED);
    assert_eq!(created.run.mode.kind, "DAG_NODE_HYBRID");
    let run_id = created.run.swarm_run_id;

    let coord = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE220).to_string(),
            roster_name: "coordinator".to_owned(),
            role_kind: "COORDINATOR".to_owned(),
            parent_member_id: None,
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: author_identity_wire(20),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    let _worker = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE221).to_string(),
            roster_name: "hybrid-worker".to_owned(),
            role_kind: "WORKER".to_owned(),
            parent_member_id: Some(coord.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e2-hybrid-worker".to_owned()),
            identity: author_identity_wire(21),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    let verifier = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE222).to_string(),
            roster_name: "hybrid-verifier".to_owned(),
            role_kind: "VERIFIER".to_owned(),
            parent_member_id: Some(coord.member_id.to_string()),
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: author_identity_wire(22),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    Ok((run_id, verifier.member_id))
}

async fn assert_hybrid_candidate_ready_and_settle_rejections(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    commit_oid: &str,
    tree_hex: &str,
    diff_hex: &str,
) -> TestResult {
    let converge_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/converge");
    let inspect_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}");
    let settle_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/settle");

    let (bypass1_status, bypass1_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &converge_path,
        &ConvergeSwarmRequest {
            request_id: Uuid::now_v7().to_string(),
            converged_commit_oid: commit_oid.to_owned(),
            converged_tree_blake3_hex: tree_hex.to_owned(),
            converged_diff_digest_hex: diff_hex.to_owned(),
            target_state: Some("SETTLED".to_owned()),
            stage_candidate_only: false,
        },
    )
    .await?;
    assert_eq!(bypass1_status, StatusCode::CONFLICT);
    assert_eq!(bypass1_err.code, "DAG_NODE_HYBRID_CANNOT_SETTLE_DIRECTLY");

    let (_, pre_snap): (_, SwarmRunInspectionResponse) = get_json(backend, &inspect_path).await?;
    assert_eq!(pre_snap.run.state, "ACTIVE");

    let (conv_status, conv_resp): (_, ConvergeSwarmResponse) = request_json(
        backend,
        "POST",
        &converge_path,
        &ConvergeSwarmRequest {
            request_id: Uuid::now_v7().to_string(),
            converged_commit_oid: commit_oid.to_owned(),
            converged_tree_blake3_hex: tree_hex.to_owned(),
            converged_diff_digest_hex: diff_hex.to_owned(),
            target_state: None,
            stage_candidate_only: false,
        },
    )
    .await?;
    assert_eq!(conv_status, StatusCode::OK);
    assert!(!conv_resp.settled);
    assert_eq!(conv_resp.run.state, "CANDIDATE_READY");
    assert!(conv_resp.gate.is_some());

    let (bypass2_status, bypass2_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &settle_path,
        &SettleSwarmRequest {
            request_id: Uuid::now_v7().to_string(),
        },
    )
    .await?;
    assert_eq!(bypass2_status, StatusCode::CONFLICT);
    assert_eq!(bypass2_err.code, "DAG_NODE_HYBRID_CANNOT_SETTLE_DIRECTLY");

    let (_, post_snap): (_, SwarmRunInspectionResponse) = get_json(backend, &inspect_path).await?;
    assert_eq!(post_snap.run.state, "CANDIDATE_READY");
    assert!(post_snap.run.settlement_request_id.is_none());
    Ok(())
}

// ============================================================================
// E2E Scenario 3: Sybil Helper Lineage Self-Review & Spawn Authority Rejection
// ============================================================================

#[tokio::test]
async fn e2e_3_sybil_helper_lineage_and_worker_verifier_spawn_fail_closed() -> TestResult {
    let backend = RunningSwarmBackend::start("swarm-e2e3-sybil").await?;
    let workspace_id = Uuid::from_u128(0xE301);
    let create_path = format!("/v1/workspaces/{workspace_id}/swarms");
    let (_, created): (_, CreateSwarmRunResponse) = request_json(
        &backend,
        "POST",
        &create_path,
        &standalone_run_request(Uuid::now_v7(), "Sybil Lineage Defense"),
    )
    .await?;
    let run_id = created.run.swarm_run_id;

    let coord_identity = author_identity_wire(30);
    let coord = add_member_over_http(
        &backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE310).to_string(),
            roster_name: "coordinator".to_owned(),
            role_kind: "COORDINATOR".to_owned(),
            parent_member_id: None,
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: coord_identity.clone(),
            initial_channels: Vec::new(),
        },
    )
    .await?;

    let worker = add_member_over_http(
        &backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE311).to_string(),
            roster_name: "parent-worker".to_owned(),
            role_kind: "WORKER".to_owned(),
            parent_member_id: Some(coord.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e3-parent-worker".to_owned()),
            identity: author_identity_wire(31),
            initial_channels: Vec::new(),
        },
    )
    .await?;

    let helper_identity = author_identity_wire(32);
    let child_helper = add_member_over_http(
        &backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE312).to_string(),
            roster_name: "child-helper".to_owned(),
            role_kind: "HELPER".to_owned(),
            parent_member_id: Some(worker.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e3-child-helper".to_owned()),
            identity: helper_identity.clone(),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    assert_eq!(child_helper.spawn_depth, 2);

    assert_worker_verifier_spawn_and_mode_rejections(
        &backend,
        workspace_id,
        run_id,
        coord.member_id,
        worker.member_id,
    )
    .await?;

    assert_lineage_self_review_rejections(
        &backend,
        workspace_id,
        run_id,
        coord.member_id,
        worker.member_id,
        child_helper.member_id,
        &helper_identity,
        &coord_identity,
    )
    .await?;

    assert_spawn_depth_bound_and_cascade_kill(&backend, workspace_id, run_id, &child_helper)
        .await?;

    backend.shutdown().await
}

async fn assert_worker_verifier_spawn_and_mode_rejections(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    worker_id: Uuid,
) -> TestResult {
    let members_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members");

    let (w_spawn_status, w_spawn_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &members_path,
        &AddSwarmMemberRequest {
            member_id: Uuid::now_v7().to_string(),
            roster_name: "worker-spawned-verifier".to_owned(),
            role_kind: "VERIFIER".to_owned(),
            parent_member_id: Some(worker_id.to_string()),
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: author_identity_wire(40),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    assert_eq!(w_spawn_status, StatusCode::FORBIDDEN);
    assert_eq!(w_spawn_err.code, "WORKER_VERIFIER_SPAWN_FORBIDDEN");

    let (share_v_status, share_v_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &members_path,
        &AddSwarmMemberRequest {
            member_id: Uuid::now_v7().to_string(),
            roster_name: "mutable-verifier".to_owned(),
            role_kind: "VERIFIER".to_owned(),
            parent_member_id: Some(coord_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e3-bad-verifier".to_owned()),
            identity: author_identity_wire(41),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    assert_eq!(share_v_status, StatusCode::CONFLICT);
    assert_eq!(share_v_err.code, "VERIFIER_MUST_INHERIT");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn assert_lineage_self_review_rejections(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    worker_id: Uuid,
    child_helper_id: Uuid,
    helper_identity: &ExecutionAuthorWire,
    coord_identity: &ExecutionAuthorWire,
) -> TestResult {
    let panels_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/panels");
    let verdicts_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/verdicts");

    let (helper_v_status, helper_v_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &verdicts_path,
        &RecordSwarmVerdictRequest {
            request_id: Uuid::now_v7().to_string(),
            panel_id: None,
            verifier_member_id: child_helper_id.to_string(),
            outcome: "ACCEPTED".to_owned(),
            reviewed_diff_digest_hex: digest_hex("77"),
            rationale: "Child helper reviewing parent work".to_owned(),
        },
    )
    .await?;
    assert_eq!(helper_v_status, StatusCode::CONFLICT);
    assert_eq!(helper_v_err.code, "SYBIL_LINEAGE_VIOLATION");

    let sybil_descendant_verifier = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE315).to_string(),
            roster_name: "sybil-profile-verifier".to_owned(),
            role_kind: "VERIFIER".to_owned(),
            parent_member_id: Some(coord_id.to_string()),
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: ExecutionAuthorWire {
                profile_id: helper_identity.profile_id.clone(),
                participant_id: Uuid::from_u128(0xBEEF_1111).to_string(),
                principal: PrincipalIdentityWire {
                    kind: "direct_token".to_owned(),
                    id: Uuid::from_u128(0xBEEF_2222).to_string(),
                },
            },
            initial_channels: Vec::new(),
        },
    )
    .await?;

    let (panel_sybil_status, panel_sybil_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &panels_path,
        &CreateSwarmVerificationPanelRequest {
            request_id: Uuid::now_v7().to_string(),
            author_member_ids: vec![worker_id.to_string()],
            verifier_member_ids: vec![sybil_descendant_verifier.member_id.to_string()],
            required_quorum: 1,
        },
    )
    .await?;
    assert_eq!(panel_sybil_status, StatusCode::CONFLICT);
    assert_eq!(panel_sybil_err.code, "SYBIL_LINEAGE_VIOLATION");

    let sybil_coord_principal_verifier = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE316).to_string(),
            roster_name: "sybil-coord-principal".to_owned(),
            role_kind: "VERIFIER".to_owned(),
            parent_member_id: Some(coord_id.to_string()),
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: ExecutionAuthorWire {
                profile_id: Uuid::from_u128(0xBEEF_3333).to_string(),
                participant_id: Uuid::from_u128(0xBEEF_4444).to_string(),
                principal: coord_identity.principal.clone(),
            },
            initial_channels: Vec::new(),
        },
    )
    .await?;

    let (coord_sybil_status, coord_sybil_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &verdicts_path,
        &RecordSwarmVerdictRequest {
            request_id: Uuid::now_v7().to_string(),
            panel_id: None,
            verifier_member_id: sybil_coord_principal_verifier.member_id.to_string(),
            outcome: "ACCEPTED".to_owned(),
            reviewed_diff_digest_hex: digest_hex("77"),
            rationale: "Shares principal with Coordinator".to_owned(),
        },
    )
    .await?;
    assert_eq!(coord_sybil_status, StatusCode::CONFLICT);
    assert_eq!(coord_sybil_err.code, "SYBIL_LINEAGE_VIOLATION");
    Ok(())
}

async fn assert_spawn_depth_bound_and_cascade_kill(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    depth2_helper: &SwarmMemberWire,
) -> TestResult {
    let d3 = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE323).to_string(),
            roster_name: "helper-depth-3".to_owned(),
            role_kind: "HELPER".to_owned(),
            parent_member_id: Some(depth2_helper.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e3-d3".to_owned()),
            identity: author_identity_wire(53),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    assert_eq!(d3.spawn_depth, 3);

    let d4 = add_member_over_http(
        backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE324).to_string(),
            roster_name: "helper-depth-4".to_owned(),
            role_kind: "HELPER".to_owned(),
            parent_member_id: Some(d3.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e3-d4".to_owned()),
            identity: author_identity_wire(54),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    assert_eq!(d4.spawn_depth, 4);

    let members_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members");
    let (d5_status, d5_err): (_, SwarmErrorResponse) = request_json(
        backend,
        "POST",
        &members_path,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE325).to_string(),
            roster_name: "helper-depth-5".to_owned(),
            role_kind: "HELPER".to_owned(),
            parent_member_id: Some(d4.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e3-d5".to_owned()),
            identity: author_identity_wire(55),
            initial_channels: Vec::new(),
        },
    )
    .await?;
    assert_eq!(d5_status, StatusCode::CONFLICT);
    assert_eq!(d5_err.code, "SPAWN_DEPTH_EXCEEDED");

    let kill_path = format!(
        "/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{}/kill",
        depth2_helper.member_id
    );
    let (kill_status, killed): (_, KillSwarmMemberResponse) =
        request_json(backend, "POST", &kill_path, &KillSwarmMemberRequest {}).await?;
    assert_eq!(kill_status, StatusCode::OK);
    assert_eq!(killed.killed_members.len(), 3);
    assert!(
        killed
            .killed_members
            .iter()
            .all(|m| m.member_state == "KILLED")
    );
    Ok(())
}

// ============================================================================
// E2E Scenario 4: Two-Phase Watermark Crash Retention, Coalescing, Quota
// Failover, and Deterministic Stall & Livelock Breaker over HTTP
// ============================================================================

#[tokio::test]
async fn e2e_4_watermark_crash_retention_coalescing_quota_failover_and_stall_livelock_monitors()
-> TestResult {
    let backend = RunningSwarmBackend::start("swarm-e2e4-resilience").await?;
    let workspace_id = Uuid::from_u128(0xE401);

    let (worker_profile_id, worker_profile_rev) =
        create_live_agent_profile(&backend, "swarm-resilience-worker").await?;

    let create_path = format!("/v1/workspaces/{workspace_id}/swarms");
    let (_, created): (_, CreateSwarmRunResponse) = request_json(
        &backend,
        "POST",
        &create_path,
        &standalone_run_request(Uuid::now_v7(), "Resilience, Coalescing & Monitors"),
    )
    .await?;
    let run_id = created.run.swarm_run_id;

    let coord = add_member_over_http(
        &backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE410).to_string(),
            roster_name: "coordinator".to_owned(),
            role_kind: "COORDINATOR".to_owned(),
            parent_member_id: None,
            workspace_mode: "INHERIT".to_owned(),
            worktree_path: None,
            identity: author_identity_wire(60),
            initial_channels: Vec::new(),
        },
    )
    .await?;

    let worker_a = add_member_over_http(
        &backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE411).to_string(),
            roster_name: "worker-alpha".to_owned(),
            role_kind: "WORKER".to_owned(),
            parent_member_id: Some(coord.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e4-alpha".to_owned()),
            identity: ExecutionAuthorWire {
                profile_id: worker_profile_id.to_string(),
                participant_id: Uuid::from_u128(0xB411).to_string(),
                principal: PrincipalIdentityWire {
                    kind: "direct_token".to_owned(),
                    id: Uuid::from_u128(0xC411).to_string(),
                },
            },
            initial_channels: Vec::new(),
        },
    )
    .await?;

    let worker_b = add_member_over_http(
        &backend,
        workspace_id,
        run_id,
        &AddSwarmMemberRequest {
            member_id: Uuid::from_u128(0xE412).to_string(),
            roster_name: "worker-beta".to_owned(),
            role_kind: "WORKER".to_owned(),
            parent_member_id: Some(coord.member_id.to_string()),
            workspace_mode: "SHARE".to_owned(),
            worktree_path: Some("/var/tmp/forge-swarm/e4-beta".to_owned()),
            identity: author_identity_wire(62),
            initial_channels: Vec::new(),
        },
    )
    .await?;

    verify_crash_retention_and_quota_model_switch_over_http(
        &backend,
        workspace_id,
        run_id,
        coord.member_id,
        worker_a.member_id,
        worker_profile_id,
        worker_profile_rev,
    )
    .await?;

    verify_single_flight_32_msg_and_64kib_coalescing_over_http(
        &backend,
        workspace_id,
        run_id,
        coord.member_id,
        worker_a.member_id,
    )
    .await?;

    verify_livelock_and_silent_stall_monitors_over_http(
        &backend,
        workspace_id,
        run_id,
        coord.member_id,
        worker_a.member_id,
        worker_b.member_id,
    )
    .await?;

    backend.shutdown().await
}

async fn create_live_agent_profile(
    backend: &RunningSwarmBackend,
    stable_name: &str,
) -> TestResult<(Uuid, i64)> {
    let (status, created): (_, AgentProfileCreateResponse) = request_json(
        backend,
        "POST",
        "/internal/v1/agent-profiles",
        &AgentProfileCreateRequest {
            request_id: Uuid::now_v7().to_string(),
            stable_name: stable_name.to_owned(),
            display_name: "Swarm Resilience Worker".to_owned(),
            preferred_model: Some("gpt-6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["rust".to_owned()],
            execution_preference: Some(ExecutionPreferenceWire {
                cli_engine: "codex".to_owned(),
                preferred_model: "gpt-6-luna".to_owned(),
                preferred_reasoning: Some("max".to_owned()),
                capabilities: vec!["rust".to_owned()],
                role_prompt_digest_hex: Some(digest_hex("7a")),
            }),
        },
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    Ok((created.profile.profile_id, created.profile.revision))
}

async fn post_board_mention(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    author_id: Uuid,
    body: String,
) -> TestResult<PostSwarmMessageResponse> {
    let path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/messages");
    let (status, resp): (_, PostSwarmMessageResponse) = request_json(
        backend,
        "POST",
        &path,
        &PostSwarmMessageRequest {
            message_id: Uuid::now_v7().to_string(),
            channel_id: None,
            author_member_id: Some(author_id.to_string()),
            body,
        },
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    Ok(resp)
}

async fn verify_crash_retention_and_quota_model_switch_over_http(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    worker_id: Uuid,
    profile_id: Uuid,
    profile_rev: i64,
) -> TestResult {
    verify_crash_preserves_zero_watermark(backend, workspace_id, run_id, coord_id, worker_id)
        .await?;
    verify_quota_model_switch_drains_all_messages(
        backend,
        workspace_id,
        run_id,
        worker_id,
        profile_id,
        profile_rev,
    )
    .await
}

async fn verify_crash_preserves_zero_watermark(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    worker_id: Uuid,
) -> TestResult {
    post_board_mention(
        backend,
        workspace_id,
        run_id,
        coord_id,
        "@worker-alpha task item 1".to_owned(),
    )
    .await?;
    let second = post_board_mention(
        backend,
        workspace_id,
        run_id,
        coord_id,
        "@worker-alpha task item 2".to_owned(),
    )
    .await?;
    assert_eq!(second.coalesced_doorbell_member_ids, vec![worker_id]);

    let claim_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{worker_id}/turns/claim");
    let fail_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{worker_id}/turns/fail");

    let snap1 = Uuid::now_v7();
    let (_, batch1): (_, SwarmWakeupBatchWire) = request_json(
        backend,
        "POST",
        &claim_path,
        &ClaimSwarmWakeupRequest {
            snapshot_id: snap1.to_string(),
        },
    )
    .await?;
    assert_eq!(batch1.committed_seen_seq, 0);
    assert_eq!(batch1.inflight_target_seq, 2);

    post_board_mention(
        backend,
        workspace_id,
        run_id,
        coord_id,
        "@worker-alpha task item 3 arrived while working".to_owned(),
    )
    .await?;

    let (fail_status, after_crash): (_, SwarmMemberWire) = request_json(
        backend,
        "POST",
        &fail_path,
        &FailSwarmTurnRequest {
            expected_snapshot_id: snap1.to_string(),
            disposition: "TRANSIENT_RETRY".to_owned(),
            consume_turn: false,
        },
    )
    .await?;
    assert_eq!(fail_status, StatusCode::OK);
    assert_eq!(after_crash.committed_seen_seq, 0);
    assert_eq!(after_crash.member_state, "WAKE_QUEUED");
    Ok(())
}

async fn verify_quota_model_switch_drains_all_messages(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    worker_id: Uuid,
    profile_id: Uuid,
    profile_rev: i64,
) -> TestResult {
    let claim_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{worker_id}/turns/claim");
    let fail_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{worker_id}/turns/fail");
    let complete_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{worker_id}/turns/complete");

    let snap2 = Uuid::now_v7();
    let (_, batch2): (_, SwarmWakeupBatchWire) = request_json(
        backend,
        "POST",
        &claim_path,
        &ClaimSwarmWakeupRequest {
            snapshot_id: snap2.to_string(),
        },
    )
    .await?;
    assert_eq!(batch2.messages.len(), 3);
    assert_eq!(batch2.inflight_target_seq, 3);

    let (_, suspended): (_, SwarmMemberWire) = request_json(
        backend,
        "POST",
        &fail_path,
        &FailSwarmTurnRequest {
            expected_snapshot_id: snap2.to_string(),
            disposition: "QUOTA_EXCEEDED".to_owned(),
            consume_turn: false,
        },
    )
    .await?;
    assert_eq!(suspended.member_state, "SUSPENDED_QUOTA");
    assert_eq!(suspended.committed_seen_seq, 0);

    let switch_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{worker_id}/model-switch");
    let (sw_status, switched): (_, SwarmModelSwitchResponse) = request_json(
        backend,
        "POST",
        &switch_path,
        &SwitchSwarmMemberModelRequest {
            new_profile_id: None,
            request_id: Some(Uuid::now_v7().to_string()),
            profile_id: Some(profile_id.to_string()),
            expected_profile_revision: Some(profile_rev),
            new_preference: Some(ExecutionPreferenceWire {
                cli_engine: "agy".to_owned(),
                preferred_model: "gemini-4-carbon-high".to_owned(),
                preferred_reasoning: Some("high".to_owned()),
                capabilities: vec!["rust".to_owned()],
                role_prompt_digest_hex: Some(digest_hex("8b")),
            }),
        },
    )
    .await?;
    assert_eq!(sw_status, StatusCode::OK);
    assert_eq!(switched.member.member_state, "WAKE_QUEUED");
    assert_eq!(switched.member.committed_seen_seq, 0);
    assert_eq!(
        switched.resolved_preference.preferred_model,
        "gemini-4-carbon-high"
    );

    let snap3 = Uuid::now_v7();
    let (_, batch3): (_, SwarmWakeupBatchWire) = request_json(
        backend,
        "POST",
        &claim_path,
        &ClaimSwarmWakeupRequest {
            snapshot_id: snap3.to_string(),
        },
    )
    .await?;
    assert_eq!(batch3.messages.len(), 3);

    let (_, completed): (_, SwarmMemberWire) = request_json(
        backend,
        "POST",
        &complete_path,
        &CompleteSwarmTurnRequest {
            expected_snapshot_id: snap3.to_string(),
            output_commit_oid: Some(oid_hex('c')),
            output_tree_blake3_hex: Some(digest_hex("33")),
        },
    )
    .await?;
    assert_eq!(completed.member_state, "WAITING");
    assert_eq!(completed.committed_seen_seq, 3);
    Ok(())
}

async fn verify_single_flight_32_msg_and_64kib_coalescing_over_http(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    worker_id: Uuid,
) -> TestResult {
    verify_32_message_coalescing_bound(backend, workspace_id, run_id, coord_id, worker_id).await?;
    verify_64kib_byte_coalescing_bound(backend, workspace_id, run_id, coord_id, worker_id).await
}

async fn claim_and_complete_idle_turn(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    worker_id: Uuid,
) -> TestResult<(SwarmWakeupBatchWire, SwarmMemberWire)> {
    let claim_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{worker_id}/turns/claim");
    let complete_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{worker_id}/turns/complete");
    let snap = Uuid::now_v7();
    let (_, batch): (_, SwarmWakeupBatchWire) = request_json(
        backend,
        "POST",
        &claim_path,
        &ClaimSwarmWakeupRequest {
            snapshot_id: snap.to_string(),
        },
    )
    .await?;
    let (_, member): (_, SwarmMemberWire) = request_json(
        backend,
        "POST",
        &complete_path,
        &CompleteSwarmTurnRequest {
            expected_snapshot_id: snap.to_string(),
            output_commit_oid: None,
            output_tree_blake3_hex: None,
        },
    )
    .await?;
    Ok((batch, member))
}

async fn verify_32_message_coalescing_bound(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    worker_id: Uuid,
) -> TestResult {
    for idx in 1..=36_u32 {
        post_board_mention(
            backend,
            workspace_id,
            run_id,
            coord_id,
            format!("@worker-alpha burst-msg-{idx}"),
        )
        .await?;
    }
    let (batch_a, after_a) =
        claim_and_complete_idle_turn(backend, workspace_id, run_id, worker_id).await?;
    assert_eq!(batch_a.messages.len(), MAX_SNAPSHOT_COALESCED_MESSAGES);
    assert_eq!(after_a.member_state, "WAKE_QUEUED");

    let (batch_b, after_b) =
        claim_and_complete_idle_turn(backend, workspace_id, run_id, worker_id).await?;
    assert_eq!(batch_b.messages.len(), 4);
    assert_eq!(after_b.member_state, "WAITING");
    Ok(())
}

async fn verify_64kib_byte_coalescing_bound(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    worker_id: Uuid,
) -> TestResult {
    let prefix = "@worker-alpha ";
    let mut big_msg = String::from(prefix);
    big_msg.extend(std::iter::repeat_n('z', 16 * 1024 - prefix.len()));
    assert_eq!(big_msg.len(), 16 * 1024);
    for _ in 0..5 {
        post_board_mention(backend, workspace_id, run_id, coord_id, big_msg.clone()).await?;
    }

    let (byte_batch_1, after_bytes_1) =
        claim_and_complete_idle_turn(backend, workspace_id, run_id, worker_id).await?;
    assert_eq!(byte_batch_1.messages.len(), 4);
    assert_eq!(byte_batch_1.total_body_bytes, MAX_SNAPSHOT_COALESCED_BYTES);
    assert_eq!(after_bytes_1.member_state, "WAKE_QUEUED");

    let (byte_batch_2, after_bytes_2) =
        claim_and_complete_idle_turn(backend, workspace_id, run_id, worker_id).await?;
    assert_eq!(byte_batch_2.messages.len(), 1);
    assert_eq!(after_bytes_2.member_state, "WAITING");
    Ok(())
}

async fn verify_livelock_and_silent_stall_monitors_over_http(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    alpha_id: Uuid,
    beta_id: Uuid,
) -> TestResult {
    post_board_mention(
        backend,
        workspace_id,
        run_id,
        beta_id,
        "@worker-alpha ping 1".to_owned(),
    )
    .await?;
    run_non_progressing_ping_pong_turn(
        backend,
        workspace_id,
        run_id,
        alpha_id,
        "@worker-beta pong 1",
    )
    .await?;
    run_non_progressing_ping_pong_turn(
        backend,
        workspace_id,
        run_id,
        beta_id,
        "@worker-alpha ping 2",
    )
    .await?;

    let stall_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/stall-check");
    let (ll_status, ll_resp): (_, SwarmStallCheckResponse) = request_json(
        backend,
        "POST",
        &stall_path,
        &SwarmStallCheckRequest {
            stall_grace_ms: Some(30_000),
            max_stall_nudges: Some(2),
            max_ping_pong_streak: Some(3),
            max_member_turns: Some(64),
            observed_at_ms: None,
        },
    )
    .await?;
    assert_eq!(ll_status, StatusCode::OK);
    assert_eq!(ll_resp.action, "LIVELOCK_BROKEN");
    assert_eq!(
        ll_resp.livelock_reason.as_deref(),
        Some("PING_PONG_STREAK_EXCEEDED")
    );
    assert_eq!(ll_resp.stopped_members.len(), 2);
    assert!(
        ll_resp
            .stopped_members
            .iter()
            .all(|m| m.member_state == "STOPPED")
    );

    verify_silent_stall_nudges_and_abort(backend, workspace_id, run_id, coord_id, &stall_path).await
}

async fn verify_silent_stall_nudges_and_abort(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    coord_id: Uuid,
    stall_path: &str,
) -> TestResult {
    let inspect_path = format!("/v1/workspaces/{workspace_id}/swarms/{run_id}");
    let (_, after_ll): (_, SwarmRunInspectionResponse) = get_json(backend, &inspect_path).await?;
    assert_eq!(
        find_member(&after_ll, coord_id)?.member_state,
        "WAKE_QUEUED"
    );

    let base_future_ms = after_ll.run.updated_at_ms + 100_000;
    for (offset_ms, expected_action) in [
        (0_i64, "STALL_NUDGED"),
        (10_000_i64, "STALL_NUDGED"),
        (20_000_i64, "STALL_ABORTED"),
    ] {
        let (_, resp): (_, SwarmStallCheckResponse) = request_json(
            backend,
            "POST",
            stall_path,
            &SwarmStallCheckRequest {
                stall_grace_ms: Some(5_000),
                max_stall_nudges: Some(2),
                max_ping_pong_streak: Some(10),
                max_member_turns: Some(64),
                observed_at_ms: Some(base_future_ms + offset_ms),
            },
        )
        .await?;
        assert_eq!(resp.action, expected_action);
        assert_eq!(resp.target_member_id, Some(coord_id));
        if expected_action == "STALL_ABORTED" {
            assert_eq!(resp.run_state.as_deref(), Some("ABORTED"));
        }
    }
    Ok(())
}

async fn run_non_progressing_ping_pong_turn(
    backend: &RunningSwarmBackend,
    workspace_id: Uuid,
    run_id: Uuid,
    member_id: Uuid,
    reply_body: &str,
) -> TestResult {
    let claim_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{member_id}/turns/claim");
    let complete_path =
        format!("/v1/workspaces/{workspace_id}/swarms/{run_id}/members/{member_id}/turns/complete");
    let snap = Uuid::now_v7();
    let (_, _batch): (_, SwarmWakeupBatchWire) = request_json(
        backend,
        "POST",
        &claim_path,
        &ClaimSwarmWakeupRequest {
            snapshot_id: snap.to_string(),
        },
    )
    .await?;
    post_board_mention(
        backend,
        workspace_id,
        run_id,
        member_id,
        reply_body.to_owned(),
    )
    .await?;
    let (_, _done): (_, SwarmMemberWire) = request_json(
        backend,
        "POST",
        &complete_path,
        &CompleteSwarmTurnRequest {
            expected_snapshot_id: snap.to_string(),
            output_commit_oid: None,
            output_tree_blake3_hex: None,
        },
    )
    .await?;
    Ok(())
}

// ============================================================================
// E2E Scenario 5: OrchestrationHttpClient + swarm_cli + autonomy_cli Live Loopback
// ============================================================================

mod client_and_cli_e2e {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use forge_control::WorkspaceLocator;
    use forge_domain::{
        ActivityId, AgentInstanceId, GoalId, LocationId, LocationRevision, WorkspaceId,
    };
    use heterarchy_orchestration_backend::{
        autonomy_cli::{self, AutonomyCliCommand},
        client::{ClientError, OrchestrationHttpClient, ServiceCode},
        profile_cli::{self, ProfileCliCommand},
        swarm_cli::{self, SwarmCliCommand},
        wire::{
            AgentProfileCreateRequest, AgentProfileCreateResponse, AgentProfileResponse,
            ExecutionPreferenceWire, ProfileMutationResponse, WorkspaceQueryScope,
            autonomy_loop::{
                AssignReviewerRequest, DagSettlementAuthorityCheckRequest,
                DagSettlementAuthorityResponse, DagSettlementGovernanceQuery,
                DagSettlementGovernanceResponse, ExecutionPermissionsWire,
                ExecutionSnapshotOutcomeResponse, ExecutionSnapshotQuery,
                ExecutionSnapshotQueryResponse, ExecutionTimeoutsWire, McpGrantWire,
                PrepareExecutionSnapshotRequest, RegisteredWorkspaceScopeResponse,
                ReviewerCandidateWire, VerifyNodeCompletionGateRequest, WorkCorrelationWire,
                WorkspaceScopeMutationRequest, WorkspaceScopeQueryResponse,
            },
            swarm::{
                AbortSwarmRunRequest, AddSwarmMemberRequest, ClaimSwarmWakeupRequest,
                CompleteSwarmTurnRequest, ConvergeSwarmRequest, CreateSwarmChannelRequest,
                CreateSwarmRunResponse, CreateSwarmVerificationPanelRequest,
                DispatchSwarmTurnRequest, DispatchSwarmTurnResponse, FailSwarmTurnRequest,
                KillSwarmMemberRequest, ListSwarmRunsQuery, PostSwarmMessageRequest,
                PostSwarmMessageResponse, ReadSwarmMessagesQuery, RecordSwarmVerdictRequest,
                SettleSwarmRequest, SubscribeSwarmChannelRequest, SwarmBoardPageResponse,
                SwarmMemberWire, SwarmRunInspectionResponse, SwarmRunListResponse, SwarmRunWire,
                SwarmStallCheckRequest, SwarmStallCheckResponse, SwitchSwarmMemberModelRequest,
            },
        },
    };
    use heterarchy_orchestration_operations::{DagSettledFact, EffectFactInput};
    use heterarchy_orchestration_runtime::AutonomySettlementOutboxFact;
    use hyper::header::HeaderValue;
    use rusqlite::{Connection, params};
    use uuid::Uuid;

    use super::{
        RunningSwarmBackend, TestResult, author_identity_wire, digest_hex, oid_hex,
        standalone_run_request,
    };

    fn cli_args(items: &[&str]) -> Vec<OsString> {
        items.iter().map(OsString::from).collect()
    }

    fn valid_locator(location_id: Uuid, location_revision: i64) -> TestResult<String> {
        Ok(WorkspaceLocator::new(
            LocationId::from_uuid(location_id),
            LocationRevision::new(location_revision)?,
            Path::new("."),
        )?
        .encode())
    }

    fn write_private_file(path: &Path, contents: &str) -> TestResult {
        fs::write(path, contents)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(())
    }

    #[tokio::test]
    async fn e2e_5_orchestration_http_client_and_cli_subcommands_roundtrip_over_loopback()
    -> TestResult {
        let backend = RunningSwarmBackend::start("swarm-e2e5-client-and-cli").await?;
        let client = OrchestrationHttpClient::new(backend.address, Duration::from_secs(5))?;
        let auth = HeaderValue::from_str(&backend.bearer)?;

        exercise_client_autonomy_surface(&client, &auth).await?;
        exercise_client_swarm_surface(&client, &auth).await?;
        exercise_autonomy_and_swarm_clis(&backend).await?;

        backend.shutdown().await
    }

    async fn exercise_client_autonomy_surface(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
    ) -> TestResult {
        let workspace_id = Uuid::from_u128(0xF501);
        let location_id = Uuid::from_u128(0xF502);
        assert_eq!(client.get_workspace_scope(auth, workspace_id).await?, None);

        let scope = WorkspaceQueryScope {
            workspace_id,
            location_id,
            location_revision: 1,
            workspace_revision: 2,
            locator: valid_locator(location_id, 1)?,
        };
        let registered = client
            .mutate_workspace_scope(
                auth,
                &WorkspaceScopeMutationRequest::Register {
                    request_id: Uuid::now_v7().to_string(),
                    expected_revision: 0,
                    scope: scope.clone(),
                },
            )
            .await?;
        assert!(registered.active);
        assert_eq!(registered.revision, 1);
        assert_eq!(
            client.get_workspace_scope(auth, workspace_id).await?,
            Some(RegisteredWorkspaceScopeResponse {
                replayed: false,
                ..registered
            })
        );
        assert_eq!(client.list_workspace_scopes(auth).await?.len(), 1);

        let unregistered = client
            .mutate_workspace_scope(
                auth,
                &WorkspaceScopeMutationRequest::Unregister {
                    workspace_id: workspace_id.to_string(),
                    request_id: Uuid::now_v7().to_string(),
                    expected_revision: 1,
                },
            )
            .await?;
        assert!(!unregistered.active);
        assert_eq!(client.get_workspace_scope(auth, workspace_id).await?, None);

        exercise_client_reviewer_and_snapshot_methods(client, auth, workspace_id).await
    }

    async fn exercise_client_reviewer_and_snapshot_methods(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        workspace_id: Uuid,
    ) -> TestResult {
        let executor = author_identity_wire(0x71);
        let correlation = WorkCorrelationWire {
            goal_id: Uuid::from_u128(0xF510).to_string(),
            dag_revision: 1,
            node_id: Uuid::from_u128(0xF511).to_string(),
            assignment_id: Uuid::from_u128(0xF512).to_string(),
            attempt: 1,
        };
        let self_assign = client
            .assign_reviewer(
                auth,
                workspace_id,
                &AssignReviewerRequest {
                    request_id: Uuid::now_v7().to_string(),
                    correlation: correlation.clone(),
                    required_domain: "rust".to_owned(),
                    executor: executor.clone(),
                    assigning_leader: author_identity_wire(0x72),
                    candidates: vec![ReviewerCandidateWire {
                        profile_id: executor.profile_id.clone(),
                        participant_id: Uuid::from_u128(0xF513).to_string(),
                        participation_id: Uuid::from_u128(0xF514).to_string(),
                        principal: executor.principal.clone(),
                        agent_id: Uuid::from_u128(0xF515).to_string(),
                        session_id: Uuid::from_u128(0xF516).to_string(),
                    }],
                },
            )
            .await?;
        assert!(!self_assign.admitted);

        let gate = client
            .verify_reviewer_gate(
                auth,
                workspace_id,
                &VerifyNodeCompletionGateRequest {
                    correlation: correlation.clone(),
                    current_handoff_diff_digest_hex: digest_hex("ab"),
                },
            )
            .await?;
        assert!(!gate.admitted);

        exercise_client_snapshot_lifecycle(client, auth, workspace_id, &correlation).await
    }

    async fn exercise_client_snapshot_lifecycle(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        workspace_id: Uuid,
        correlation: &WorkCorrelationWire,
    ) -> TestResult {
        let prompt_digest = digest_hex("cd");
        let created_profile = client
            .create_profile(
                auth,
                &AgentProfileCreateRequest {
                    request_id: Uuid::now_v7().to_string(),
                    stable_name: "client-snapshot-profile".to_owned(),
                    display_name: "Client Snapshot Profile".to_owned(),
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

        let mut snap_req = build_client_snapshot_req(
            created_profile.profile.profile_id,
            created_profile.profile.revision,
            &prompt_digest,
            correlation,
            "/Volumes/256.SSD/RustBuild/orchestration/forbidden",
        );
        let ssd_rejected = client
            .prepare_execution_snapshot(auth, workspace_id, &snap_req)
            .await?;
        assert!(!ssd_rejected.committed);
        assert_eq!(
            ssd_rejected.rejection_code.as_deref(),
            Some("FORBIDDEN_SSD_BUILD_WORKTREE")
        );

        snap_req.request_id = Uuid::now_v7().to_string();
        snap_req.snapshot_id = Uuid::now_v7().to_string();
        "/var/tmp/forge-swarm/client-e2e-snap".clone_into(&mut snap_req.worktree_path);
        let prepared = client
            .prepare_execution_snapshot(auth, workspace_id, &snap_req)
            .await?;
        assert!(prepared.committed);

        let fetched = client
            .get_execution_snapshot(
                auth,
                workspace_id,
                &ExecutionSnapshotQuery {
                    goal_id: correlation.goal_id.clone(),
                    dag_revision: correlation.dag_revision,
                    node_id: correlation.node_id.clone(),
                    assignment_id: correlation.assignment_id.clone(),
                    attempt: correlation.attempt,
                },
            )
            .await?
            .ok_or("expected prepared snapshot record")?;
        assert_eq!(fetched.state.status, "PREPARED");
        Ok(())
    }

    fn build_client_snapshot_req(
        profile_id: Uuid,
        profile_revision: i64,
        prompt_digest: &str,
        correlation: &WorkCorrelationWire,
        worktree_path: &str,
    ) -> PrepareExecutionSnapshotRequest {
        PrepareExecutionSnapshotRequest {
            request_id: Uuid::now_v7().to_string(),
            snapshot_id: Uuid::now_v7().to_string(),
            goal_id: correlation.goal_id.clone(),
            dag_revision: correlation.dag_revision,
            node_id: correlation.node_id.clone(),
            assignment_id: correlation.assignment_id.clone(),
            attempt: correlation.attempt,
            activity_id: Uuid::from_u128(0xF520).to_string(),
            profile_id: profile_id.to_string(),
            profile_revision,
            resolved_engine: "codex".to_owned(),
            resolved_model: "gpt-6-astra".to_owned(),
            resolved_reasoning: Some("high".to_owned()),
            role_prompt_digest_hex: Some(prompt_digest.to_owned()),
            worktree_path: worktree_path.to_owned(),
            lease_id: Uuid::from_u128(0xF521).to_string(),
            lease_revision: 1,
            lease_expires_at_ms: 90_000,
            mcp_grant: Some(McpGrantWire {
                token_id: Uuid::from_u128(0xF522).to_string(),
                issued_at_ms: 10,
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

    async fn exercise_client_swarm_surface(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
    ) -> TestResult {
        let ws = Uuid::from_u128(0xF601);
        let created = client
            .create_swarm_run(
                auth,
                ws,
                &standalone_run_request(Uuid::now_v7(), "Typed Client Swarm"),
            )
            .await?;
        let run_id = created.run.swarm_run_id;
        assert_eq!(
            client
                .list_swarm_runs(auth, ws, &ListSwarmRunsQuery { limit: Some(10) })
                .await?
                .runs
                .len(),
            1
        );

        let ssd_err = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "bad-ssd".to_owned(),
                    role_kind: "WORKER".to_owned(),
                    parent_member_id: None,
                    workspace_mode: "SHARE".to_owned(),
                    worktree_path: Some("/Volumes/256.SSD/RustBuild/bad".to_owned()),
                    identity: author_identity_wire(0x80),
                    initial_channels: Vec::new(),
                },
            )
            .await;
        assert_eq!(
            ssd_err,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict,
            })
        );

        let (coord, worker, verifier) = seed_client_swarm_roster(client, auth, ws, run_id).await?;
        exercise_client_turns_model_switch_and_settlement(
            client, auth, ws, run_id, &coord, &worker, &verifier,
        )
        .await?;
        exercise_client_stall_kill_and_abort(client, auth, ws).await
    }

    async fn create_fallback_worker_profile(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
    ) -> TestResult<Uuid> {
        Ok(client
            .create_profile(
                auth,
                &AgentProfileCreateRequest {
                    request_id: Uuid::now_v7().to_string(),
                    stable_name: "swarm-client-fallback".to_owned(),
                    display_name: "Swarm Client Fallback".to_owned(),
                    preferred_model: Some("gemini-4-carbon-high".to_owned()),
                    preferred_reasoning: Some("high".to_owned()),
                    base_capabilities: vec!["rust".to_owned()],
                    execution_preference: Some(ExecutionPreferenceWire {
                        cli_engine: "agy".to_owned(),
                        preferred_model: "gemini-4-carbon-high".to_owned(),
                        preferred_reasoning: Some("high".to_owned()),
                        capabilities: vec!["rust".to_owned()],
                        role_prompt_digest_hex: None,
                    }),
                },
            )
            .await?
            .profile
            .profile_id)
    }

    async fn seed_client_swarm_roster(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        run_id: Uuid,
    ) -> TestResult<(SwarmMemberWire, SwarmMemberWire, SwarmMemberWire)> {
        let fallback_profile = create_fallback_worker_profile(client, auth).await?;
        let coord = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "coordinator".to_owned(),
                    role_kind: "COORDINATOR".to_owned(),
                    parent_member_id: None,
                    workspace_mode: "INHERIT".to_owned(),
                    worktree_path: None,
                    identity: author_identity_wire(0x81),
                    initial_channels: Vec::new(),
                },
            )
            .await?;
        let channel = client
            .create_swarm_channel(
                auth,
                ws,
                run_id,
                &CreateSwarmChannelRequest {
                    channel_id: Uuid::now_v7().to_string(),
                    name: "slice".to_owned(),
                    purpose: Some("Client test channel".to_owned()),
                    created_by_member_id: Some(coord.member_id.to_string()),
                },
            )
            .await?;
        let mut worker_identity = author_identity_wire(0x82);
        worker_identity.profile_id = fallback_profile.to_string();
        let worker = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "worker-one".to_owned(),
                    role_kind: "WORKER".to_owned(),
                    parent_member_id: Some(coord.member_id.to_string()),
                    workspace_mode: "SHARE".to_owned(),
                    worktree_path: Some("/var/tmp/forge-swarm/client-worker-one".to_owned()),
                    identity: worker_identity,
                    initial_channels: Vec::new(),
                },
            )
            .await?;
        let sub = client
            .subscribe_swarm_channel(
                auth,
                ws,
                run_id,
                channel.channel_id,
                &SubscribeSwarmChannelRequest {
                    member_id: worker.member_id.to_string(),
                },
            )
            .await?;
        assert!(sub.newly_subscribed);
        let verifier = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "verifier-one".to_owned(),
                    role_kind: "VERIFIER".to_owned(),
                    parent_member_id: Some(coord.member_id.to_string()),
                    workspace_mode: "INHERIT".to_owned(),
                    worktree_path: None,
                    identity: author_identity_wire(0x83),
                    initial_channels: Vec::new(),
                },
            )
            .await?;
        Ok((coord, worker, verifier))
    }

    async fn exercise_client_turns_model_switch_and_settlement(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        run_id: Uuid,
        coord: &SwarmMemberWire,
        worker: &SwarmMemberWire,
        verifier: &SwarmMemberWire,
    ) -> TestResult {
        let posted = client
            .post_swarm_message(
                auth,
                ws,
                run_id,
                &PostSwarmMessageRequest {
                    message_id: Uuid::now_v7().to_string(),
                    channel_id: None,
                    author_member_id: Some(coord.member_id.to_string()),
                    body: "@worker-one implement client slice".to_owned(),
                },
            )
            .await?;
        assert_eq!(posted.woken_member_ids, vec![worker.member_id]);
        let board = client
            .read_swarm_messages(auth, ws, run_id, &ReadSwarmMessagesQuery::default())
            .await?;
        assert_eq!(board.messages.len(), 1);

        exercise_worker_quota_switch_and_complete(client, auth, ws, run_id, worker).await?;
        exercise_panel_convergence_and_settlement(client, auth, ws, run_id, worker, verifier).await
    }

    async fn exercise_worker_quota_switch_and_complete(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        run_id: Uuid,
        worker: &SwarmMemberWire,
    ) -> TestResult {
        let snap_1 = Uuid::now_v7();
        let batch_1 = client
            .claim_swarm_turn(
                auth,
                ws,
                run_id,
                worker.member_id,
                &ClaimSwarmWakeupRequest {
                    snapshot_id: snap_1.to_string(),
                },
            )
            .await?;
        assert_eq!(batch_1.messages.len(), 1);

        let suspended = client
            .fail_swarm_turn(
                auth,
                ws,
                run_id,
                worker.member_id,
                &FailSwarmTurnRequest {
                    expected_snapshot_id: snap_1.to_string(),
                    disposition: "QUOTA_EXCEEDED".to_owned(),
                    consume_turn: false,
                },
            )
            .await?;
        assert_eq!(suspended.member_state, "SUSPENDED_QUOTA");

        let switched = client
            .switch_swarm_member_model(
                auth,
                ws,
                run_id,
                worker.member_id,
                &SwitchSwarmMemberModelRequest {
                    new_profile_id: None,
                    request_id: Some(Uuid::now_v7().to_string()),
                    profile_id: Some(worker.identity.profile_id.to_string()),
                    expected_profile_revision: Some(1),
                    new_preference: Some(ExecutionPreferenceWire {
                        cli_engine: "codex".to_owned(),
                        preferred_model: "gpt-6-astra".to_owned(),
                        preferred_reasoning: Some("high".to_owned()),
                        capabilities: vec!["rust".to_owned()],
                        role_prompt_digest_hex: None,
                    }),
                },
            )
            .await?;
        assert_eq!(switched.member.member_state, "WAKE_QUEUED");

        let snap_2 = Uuid::now_v7();
        let _batch_2 = client
            .claim_swarm_turn(
                auth,
                ws,
                run_id,
                worker.member_id,
                &ClaimSwarmWakeupRequest {
                    snapshot_id: snap_2.to_string(),
                },
            )
            .await?;
        let completed = client
            .complete_swarm_turn(
                auth,
                ws,
                run_id,
                worker.member_id,
                &CompleteSwarmTurnRequest {
                    expected_snapshot_id: snap_2.to_string(),
                    output_commit_oid: Some(oid_hex('4')),
                    output_tree_blake3_hex: Some(digest_hex("b4")),
                },
            )
            .await?;
        assert_eq!(completed.member_state, "WAITING");
        Ok(())
    }

    async fn exercise_panel_convergence_and_settlement(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        run_id: Uuid,
        worker: &SwarmMemberWire,
        verifier: &SwarmMemberWire,
    ) -> TestResult {
        let panel = client
            .create_swarm_verification_panel(
                auth,
                ws,
                run_id,
                &CreateSwarmVerificationPanelRequest {
                    request_id: Uuid::now_v7().to_string(),
                    author_member_ids: vec![worker.member_id.to_string()],
                    verifier_member_ids: vec![verifier.member_id.to_string()],
                    required_quorum: 1,
                },
            )
            .await?;
        let diff_hex = digest_hex("d9");
        let staged = client
            .converge_swarm_run(
                auth,
                ws,
                run_id,
                &ConvergeSwarmRequest {
                    request_id: Uuid::now_v7().to_string(),
                    converged_commit_oid: oid_hex('4'),
                    converged_tree_blake3_hex: digest_hex("b4"),
                    converged_diff_digest_hex: diff_hex.clone(),
                    target_state: None,
                    stage_candidate_only: true,
                },
            )
            .await?;
        assert_eq!(staged.run.state, "CANDIDATE_READY");

        let verdict = client
            .record_swarm_verdict(
                auth,
                ws,
                run_id,
                &RecordSwarmVerdictRequest {
                    request_id: Uuid::now_v7().to_string(),
                    panel_id: Some(panel.panel_id.to_string()),
                    verifier_member_id: verifier.member_id.to_string(),
                    outcome: "ACCEPTED".to_owned(),
                    reviewed_diff_digest_hex: diff_hex,
                    rationale: "clean client lifecycle".to_owned(),
                },
            )
            .await?;
        assert_eq!(verdict.outcome, "ACCEPTED");

        let settled = client
            .settle_swarm_run(
                auth,
                ws,
                run_id,
                &SettleSwarmRequest {
                    request_id: Uuid::now_v7().to_string(),
                },
            )
            .await?;
        assert!(settled.settled);
        assert_eq!(settled.run.state, "SETTLED");
        let snapshot = client.get_swarm_run_snapshot(auth, ws, run_id).await?;
        assert_eq!(snapshot.run.state, "SETTLED");
        Ok(())
    }

    async fn exercise_client_stall_kill_and_abort(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
    ) -> TestResult {
        let second = client
            .create_swarm_run(
                auth,
                ws,
                &standalone_run_request(Uuid::now_v7(), "Second Run Abort Check"),
            )
            .await?;
        let run2 = second.run.swarm_run_id;
        let member = client
            .add_swarm_member(
                auth,
                ws,
                run2,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "temp-worker".to_owned(),
                    role_kind: "WORKER".to_owned(),
                    parent_member_id: None,
                    workspace_mode: "INHERIT".to_owned(),
                    worktree_path: None,
                    identity: author_identity_wire(0x89),
                    initial_channels: Vec::new(),
                },
            )
            .await?;
        let stall = client
            .evaluate_swarm_stall_check(auth, ws, run2, &SwarmStallCheckRequest::default())
            .await?;
        assert_eq!(stall.action, "NO_ACTION");

        let killed = client
            .kill_swarm_member(auth, ws, run2, member.member_id, &KillSwarmMemberRequest {})
            .await?;
        assert_eq!(killed.killed_members.len(), 1);

        let aborted = client
            .abort_swarm_run(auth, ws, run2, &AbortSwarmRunRequest {})
            .await?;
        assert_eq!(aborted.state, "ABORTED");
        Ok(())
    }

    async fn run_profile_cli_json<T: serde::de::DeserializeOwned>(argv: &[&str]) -> TestResult<T> {
        let ProfileCliCommand::Run(opts) = profile_cli::parse_with_env(&cli_args(argv), |_| None)?
        else {
            return Err("expected ProfileCliCommand::Run".into());
        };
        let raw = profile_cli::execute(&opts).await?;
        Ok(serde_json::from_str(&raw)?)
    }

    async fn run_autonomy_cli_json<T: serde::de::DeserializeOwned>(argv: &[&str]) -> TestResult<T> {
        let AutonomyCliCommand::Run(opts) =
            autonomy_cli::parse_with_env(&cli_args(argv), |_| None)?
        else {
            return Err("expected AutonomyCliCommand::Run".into());
        };
        let raw = autonomy_cli::execute(&opts).await?;
        Ok(serde_json::from_str(&raw)?)
    }

    async fn run_swarm_cli_json<T: serde::de::DeserializeOwned>(argv: &[&str]) -> TestResult<T> {
        let SwarmCliCommand::Run(opts) = swarm_cli::parse_with_env(&cli_args(argv), |_| None)?
        else {
            return Err("expected SwarmCliCommand::Run".into());
        };
        let raw = swarm_cli::execute(&opts).await?;
        Ok(serde_json::from_str(&raw)?)
    }

    async fn run_swarm_cli_with_env_json<T: serde::de::DeserializeOwned>(
        argv: &[&str],
        env_map: &BTreeMap<String, OsString>,
    ) -> TestResult<T> {
        let SwarmCliCommand::Run(opts) =
            swarm_cli::parse_with_env(&cli_args(argv), |key| env_map.get(key).cloned())?
        else {
            return Err("expected SwarmCliCommand::Run".into());
        };
        let raw = swarm_cli::execute(&opts).await?;
        Ok(serde_json::from_str(&raw)?)
    }

    async fn exercise_autonomy_and_swarm_clis(backend: &RunningSwarmBackend) -> TestResult {
        let cli_dir = super::TempDir::create("swarm-e2e5-cli")?;
        let dir = cli_dir.path();
        // Test raw token reveal format (matching `token issue --output`) without "Bearer " prefix.
        let raw_reveal = backend
            .bearer
            .strip_prefix("Bearer ")
            .ok_or("expected Bearer prefix")?;
        let bearer_path = dir.join("cli-operator.token");
        write_private_file(&bearer_path, &format!("{raw_reveal}\n"))?;
        let bearer_arg = bearer_path.to_str().ok_or("utf8 bearer path")?;
        let addr_arg = backend.address.to_string();

        exercise_autonomy_cli_commands(dir, &addr_arg, bearer_arg).await?;
        exercise_swarm_cli_commands(dir, &addr_arg, bearer_arg).await
    }

    async fn exercise_autonomy_cli_commands(
        dir: &Path,
        addr_arg: &str,
        bearer_arg: &str,
    ) -> TestResult {
        let ws = Uuid::from_u128(0xF701);
        let loc = Uuid::from_u128(0xF702);
        let locator = valid_locator(loc, 1)?;
        let ws_str = ws.to_string();
        let loc_str = loc.to_string();
        let reg_req_id = Uuid::now_v7().to_string();

        let reg_out: RegisteredWorkspaceScopeResponse = run_autonomy_cli_json(&[
            "autonomy",
            "scope-mutate",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--register",
            "--request-id",
            &reg_req_id,
            "--expected-revision",
            "0",
            "--workspace-id",
            &ws_str,
            "--location-id",
            &loc_str,
            "--location-revision",
            "1",
            "--workspace-revision",
            "1",
            "--locator",
            &locator,
        ])
        .await?;
        assert!(reg_out.active);

        let get_out: WorkspaceScopeQueryResponse = run_autonomy_cli_json(&[
            "autonomy",
            "scope-get",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            &ws_str,
        ])
        .await?;
        assert!(get_out.scope.is_some());

        let unreg_file = dir.join("scope-unreg.json");
        fs::write(
            &unreg_file,
            serde_json::to_vec_pretty(&WorkspaceScopeMutationRequest::Unregister {
                workspace_id: ws_str.clone(),
                request_id: Uuid::now_v7().to_string(),
                expected_revision: 1,
            })?,
        )?;
        let unreg_out: RegisteredWorkspaceScopeResponse = run_autonomy_cli_json(&[
            "autonomy",
            "scope-mutate",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--request",
            unreg_file.to_str().ok_or("utf8")?,
        ])
        .await?;
        assert!(!unreg_out.active);

        let goal_str = Uuid::from_u128(0xF710).to_string();
        let node_str = Uuid::from_u128(0xF711).to_string();
        let assign_str = Uuid::from_u128(0xF712).to_string();
        let snap_out: ExecutionSnapshotQueryResponse = run_autonomy_cli_json(&[
            "autonomy",
            "snapshot-get",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            &ws_str,
            "--goal-id",
            &goal_str,
            "--dag-revision",
            "1",
            "--node-id",
            &node_str,
            "--assignment-id",
            &assign_str,
            "--attempt",
            "1",
        ])
        .await?;
        assert!(snap_out.record.is_none());
        let _ = ExecutionSnapshotOutcomeResponse {
            committed: false,
            rejection_code: None,
            detail: None,
            record: None,
        };
        Ok(())
    }

    async fn exercise_swarm_cli_commands(
        dir: &Path,
        addr_arg: &str,
        bearer_arg: &str,
    ) -> TestResult {
        let ws_str = Uuid::from_u128(0xF801).to_string();
        let create_file = dir.join("swarm-create.json");
        fs::write(
            &create_file,
            serde_json::to_vec_pretty(&standalone_run_request(Uuid::now_v7(), "CLI Swarm E2E"))?,
        )?;
        let created: CreateSwarmRunResponse = run_swarm_cli_json(&[
            "swarm",
            "create",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            &ws_str,
            "--request",
            create_file.to_str().ok_or("utf8")?,
        ])
        .await?;
        let run_str = created.run.swarm_run_id.to_string();

        let member_file = dir.join("swarm-member.json");
        fs::write(
            &member_file,
            serde_json::to_vec_pretty(&AddSwarmMemberRequest {
                member_id: Uuid::now_v7().to_string(),
                roster_name: "cli-coord".to_owned(),
                role_kind: "COORDINATOR".to_owned(),
                parent_member_id: None,
                workspace_mode: "INHERIT".to_owned(),
                worktree_path: None,
                identity: author_identity_wire(0x95),
                initial_channels: Vec::new(),
            })?,
        )?;
        let coord: SwarmMemberWire = run_swarm_cli_json(&[
            "swarm",
            "member-add",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            &ws_str,
            "--swarm-run-id",
            &run_str,
            "--request",
            member_file.to_str().ok_or("utf8")?,
        ])
        .await?;

        verify_swarm_cli_board_status_stall_and_abort(
            addr_arg,
            bearer_arg,
            &ws_str,
            &run_str,
            &coord.member_id.to_string(),
        )
        .await
    }

    async fn verify_swarm_cli_board_status_stall_and_abort(
        addr_arg: &str,
        bearer_arg: &str,
        ws_str: &str,
        run_str: &str,
        coord_id_str: &str,
    ) -> TestResult {
        let msg_id_str = Uuid::now_v7().to_string();
        let posted: PostSwarmMessageResponse = run_swarm_cli_json(&[
            "swarm",
            "post",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            ws_str,
            "--swarm-run-id",
            run_str,
            "--message-id",
            &msg_id_str,
            "--author-member-id",
            coord_id_str,
            "--body",
            "CLI board check",
        ])
        .await?;
        assert_eq!(posted.message.sequence, 1);

        let page: SwarmBoardPageResponse = run_swarm_cli_json(&[
            "swarm",
            "read",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            ws_str,
            "--swarm-run-id",
            run_str,
            "--limit",
            "10",
        ])
        .await?;
        assert_eq!(page.messages.len(), 1);

        let listed: SwarmRunListResponse = run_swarm_cli_json(&[
            "swarm",
            "list",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            ws_str,
        ])
        .await?;
        assert_eq!(listed.runs.len(), 1);

        let snap: SwarmRunInspectionResponse = run_swarm_cli_json(&[
            "swarm",
            "status",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            ws_str,
            "--swarm-run-id",
            run_str,
        ])
        .await?;
        assert_eq!(snap.members.len(), 1);

        let stall: SwarmStallCheckResponse = run_swarm_cli_json(&[
            "swarm",
            "stall-check",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            ws_str,
            "--swarm-run-id",
            run_str,
        ])
        .await?;
        assert_eq!(stall.action, "NO_ACTION");

        let aborted: SwarmRunWire = run_swarm_cli_json(&[
            "swarm",
            "abort",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            ws_str,
            "--swarm-run-id",
            run_str,
        ])
        .await?;
        assert_eq!(aborted.state, "ABORTED");
        Ok(())
    }

    struct VarTmpDir(PathBuf);

    impl VarTmpDir {
        fn create(label: &str) -> TestResult<Self> {
            let raw = Path::new("/var/tmp").join(format!(
                "{label}-{}-{}",
                std::process::id(),
                Uuid::now_v7()
            ));
            fs::create_dir_all(&raw)?;
            fs::set_permissions(&raw, fs::Permissions::from_mode(0o700))?;
            Ok(Self(raw.canonicalize()?))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for VarTmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const DISPATCH_WORKER_SHELL_SCRIPT: &str = "test -n \"$HETERARCHY_WORKSPACE_ID\" \
        && test -n \"$HETERARCHY_SWARM_RUN_ID\" \
        && test -n \"$HETERARCHY_SWARM_MEMBER_ID\" \
        && test -n \"$HETERARCHY_ORCHESTRATION_BACKEND_ADDR\" \
        && test -r \"$HETERARCHY_ORCHESTRATION_BEARER_FILE\" \
        && printf '%s|%s|%s|%s|%s' \
           \"$HETERARCHY_WORKSPACE_ID\" \
           \"$HETERARCHY_SWARM_RUN_ID\" \
           \"$HETERARCHY_SWARM_MEMBER_ID\" \
           \"$HETERARCHY_ORCHESTRATION_BACKEND_ADDR\" \
           \"$HETERARCHY_ORCHESTRATION_BEARER_FILE\" > worker-env.txt";

    #[tokio::test]
    async fn e2e_6_profile_autonomy_settlement_and_swarm_dispatch_worker_env_cli_over_loopback()
    -> TestResult {
        let non_ssd = VarTmpDir::create("swarm-e2e6-non-ssd")?;
        let (backend, worker_bearer_path) = RunningSwarmBackend::start_with_worker_shell(
            "swarm-e2e6-dispatch",
            non_ssd.path(),
            DISPATCH_WORKER_SHELL_SCRIPT,
        )
        .await?;
        let client = OrchestrationHttpClient::new(backend.address, Duration::from_secs(10))?;
        let auth = HeaderValue::from_str(&backend.bearer)?;
        let addr_arg = backend.address.to_string();
        let bearer_arg = worker_bearer_path.to_str().ok_or("utf8 bearer path")?;

        let profile_id = exercise_profile_cli_lifecycle(&addr_arg, bearer_arg).await?;
        exercise_dag_settlement_cli_and_client(&backend, &client, &auth, &addr_arg, bearer_arg)
            .await?;
        exercise_swarm_dispatch_and_worker_env_cli(
            &client,
            &auth,
            &addr_arg,
            bearer_arg,
            non_ssd.path(),
            profile_id,
        )
        .await?;

        backend.shutdown().await
    }

    async fn exercise_profile_cli_lifecycle(addr_arg: &str, bearer_arg: &str) -> TestResult<Uuid> {
        let create_req_id = Uuid::now_v7().to_string();
        let created: AgentProfileCreateResponse = run_profile_cli_json(&[
            "profile",
            "create",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--request-id",
            &create_req_id,
            "--stable-name",
            "dispatch-worker-prof",
            "--display-name",
            "Dispatch Worker",
            "--cli-engine",
            "agy",
            "--preferred-model",
            "gemini-4-carbon-high",
            "--preferred-reasoning",
            "high",
            "--capability",
            "rust",
        ])
        .await?;
        assert!(created.created);
        let profile_id = created.profile.profile_id;
        let profile_id_str = profile_id.to_string();

        let fetched: AgentProfileResponse = run_profile_cli_json(&[
            "profile",
            "get",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--profile-id",
            &profile_id_str,
        ])
        .await?;
        assert_eq!(fetched.stable_name, "dispatch-worker-prof");

        let listed: Vec<AgentProfileResponse> = run_profile_cli_json(&[
            "profile",
            "list",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--limit",
            "10",
        ])
        .await?;
        assert!(listed.iter().any(|p| p.profile_id == profile_id));

        verify_profile_cli_update_and_receipt(addr_arg, bearer_arg, &profile_id_str).await?;
        Ok(profile_id)
    }

    async fn verify_profile_cli_update_and_receipt(
        addr_arg: &str,
        bearer_arg: &str,
        profile_id_str: &str,
    ) -> TestResult {
        let upd_req_id = Uuid::now_v7().to_string();
        let updated: ProfileMutationResponse = run_profile_cli_json(&[
            "profile",
            "update",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--profile-id",
            profile_id_str,
            "--request-id",
            &upd_req_id,
            "--expected-revision",
            "1",
            "--display-name",
            "Dispatch Worker v2",
            "--cli-engine",
            "codex",
            "--preferred-model",
            "gpt-6-astra",
            "--preferred-reasoning",
            "high",
            "--capability",
            "rust",
        ])
        .await?;
        assert!(!updated.replayed);
        assert_eq!(updated.profile.revision, 2);
        assert_eq!(
            updated
                .profile
                .execution_preference
                .as_ref()
                .map(|pref| pref.cli_engine.as_str()),
            Some("codex")
        );

        let receipt: ProfileMutationResponse = run_profile_cli_json(&[
            "profile",
            "mutation-get",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--request-id",
            &upd_req_id,
        ])
        .await?;
        assert!(!receipt.replayed);
        assert_eq!(receipt.request_id, Uuid::parse_str(&upd_req_id)?);
        assert_eq!(receipt.profile.revision, 2);
        Ok(())
    }

    async fn exercise_dag_settlement_cli_and_client(
        backend: &RunningSwarmBackend,
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        addr_arg: &str,
        bearer_arg: &str,
    ) -> TestResult {
        let ws = Uuid::from_u128(0xF901);
        let goal = Uuid::from_u128(0xF902);
        let leader = Uuid::from_u128(0xF903);
        let settlement_id = Uuid::from_u128(0xF904);
        let ws_str = ws.to_string();
        let goal_str = goal.to_string();

        let missing_auth = client
            .check_dag_settlement_authority(
                auth,
                ws,
                &DagSettlementAuthorityCheckRequest {
                    expected_leadership_revision: 3,
                    participant: None,
                },
            )
            .await;
        assert_eq!(
            missing_auth,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict,
            })
        );

        let missing_gov = client
            .get_dag_settlement_governance(
                auth,
                ws,
                &DagSettlementGovernanceQuery {
                    goal_id: goal_str.clone(),
                    dag_revision: 3,
                },
            )
            .await;
        assert_eq!(
            missing_gov,
            Err(ClientError::Rejected {
                status: 404,
                code: ServiceCode::NotFound,
            })
        );

        seed_dag_settlement_state(backend, ws, goal, leader, settlement_id)?;
        verify_dag_settlement_cli_roundtrip(
            addr_arg,
            bearer_arg,
            &ws_str,
            &goal_str,
            leader,
            settlement_id,
        )
        .await
    }

    fn seed_dag_settlement_state(
        backend: &RunningSwarmBackend,
        ws: Uuid,
        goal: Uuid,
        leader_participant: Uuid,
        settlement_id: Uuid,
    ) -> TestResult {
        let migration_id = Uuid::from_u128(0xF910);
        let conn = Connection::open(backend.runtime_root.join("agent-operations.sqlite3"))?;
        conn.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                migration_id.as_bytes(),
                ws.as_bytes(),
                backend.runtime.destination_id().as_bytes(),
                [0_u8; 32].as_slice(),
            ],
        )?;
        conn.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,3,1,NULL,1)",
            params![ws.as_bytes(), leader_participant.as_bytes()],
        )?;
        drop(conn);

        let fact =
            AutonomySettlementOutboxFact::CoreEffect(EffectFactInput::DagSettled(DagSettledFact {
                sequence: 1,
                migration_id,
                destination_id: backend.runtime.destination_id(),
                settlement_id,
                workspace_id: WorkspaceId::from_uuid(ws),
                activity_id: ActivityId::from_uuid(Uuid::from_u128(0xF911)),
                goal_id: GoalId::from_uuid(goal),
                goal_revision: 1,
                dag_revision: 3,
                settled_file_count: 2,
                settled_tree_digest: [0x5A; 32],
                coordinator_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xF912)),
                coordinator_session_id: None,
                settled_at_ms: 1_700,
            }));
        backend
            .runtime
            .autonomy_outbox_bridge()
            .apply_outbox_facts(migration_id, &[fact])?;
        Ok(())
    }

    async fn verify_dag_settlement_cli_roundtrip(
        addr_arg: &str,
        bearer_arg: &str,
        ws_str: &str,
        goal_str: &str,
        leader: Uuid,
        settlement_id: Uuid,
    ) -> TestResult {
        let auth_out: DagSettlementAuthorityResponse = run_autonomy_cli_json(&[
            "autonomy",
            "dag-settlement-authority",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            ws_str,
            "--expected-leadership-revision",
            "3",
        ])
        .await?;
        assert_eq!(auth_out.leader_participant_id, Some(leader));
        assert_eq!(auth_out.leadership_revision, 3);

        let gov_out: DagSettlementGovernanceResponse = run_autonomy_cli_json(&[
            "autonomy",
            "dag-settlement-governance",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            ws_str,
            "--goal-id",
            goal_str,
            "--dag-revision",
            "3",
        ])
        .await?;
        assert_eq!(gov_out.settlement_id, settlement_id);
        assert_eq!(gov_out.dag_revision, 3);
        assert_eq!(gov_out.settled_at_ms, 1_700);
        Ok(())
    }

    async fn exercise_swarm_dispatch_and_worker_env_cli(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        addr_arg: &str,
        bearer_arg: &str,
        non_ssd_dir: &Path,
        profile_id: Uuid,
    ) -> TestResult {
        let ws = Uuid::from_u128(0xFA01);
        let worktree = non_ssd_dir.join("worker-worktree");
        fs::create_dir_all(&worktree)?;
        let (run_id, coord, worker) =
            seed_dispatch_roster(client, auth, ws, &worktree, profile_id).await?;

        verify_dispatch_route_rejections(client, auth, ws, run_id, &coord, &worker, &worktree)
            .await?;

        let worker_env = run_live_cli_dispatch_and_capture_env(
            addr_arg,
            bearer_arg,
            ws,
            run_id,
            worker.member_id,
            &worktree,
        )
        .await?;
        verify_worker_env_only_swarm_cli(ws, run_id, worker.member_id, &worker_env).await
    }

    async fn seed_dispatch_roster(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        worktree: &Path,
        profile_id: Uuid,
    ) -> TestResult<(Uuid, SwarmMemberWire, SwarmMemberWire)> {
        let created = client
            .create_swarm_run(
                auth,
                ws,
                &standalone_run_request(Uuid::now_v7(), "Dispatch + Worker Env CLI"),
            )
            .await?;
        let run_id = created.run.swarm_run_id;
        let mut coord_id = author_identity_wire(0xA1);
        coord_id.profile_id = profile_id.to_string();
        let coord = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "dispatch-coord".to_owned(),
                    role_kind: "COORDINATOR".to_owned(),
                    parent_member_id: None,
                    workspace_mode: "INHERIT".to_owned(),
                    worktree_path: None,
                    identity: coord_id,
                    initial_channels: Vec::new(),
                },
            )
            .await?;
        let mut worker_id = author_identity_wire(0xA2);
        worker_id.profile_id = profile_id.to_string();
        let worker = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "dispatch-worker".to_owned(),
                    role_kind: "WORKER".to_owned(),
                    parent_member_id: Some(coord.member_id.to_string()),
                    workspace_mode: "SHARE".to_owned(),
                    worktree_path: Some(worktree.to_str().ok_or("utf8 worktree")?.to_owned()),
                    identity: worker_id,
                    initial_channels: Vec::new(),
                },
            )
            .await?;
        Ok((run_id, coord, worker))
    }

    async fn verify_dispatch_route_rejections(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        run_id: Uuid,
        coord: &SwarmMemberWire,
        worker: &SwarmMemberWire,
        worktree: &Path,
    ) -> TestResult {
        let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let base_req = build_dispatch_client_req(worktree, now_ms)?;

        // Worker is still WAITING -> 409 MEMBER_NOT_WAKE_QUEUED.
        let not_woken = client
            .dispatch_swarm_member_turn(auth, ws, run_id, worker.member_id, &base_req)
            .await;
        assert_eq!(
            not_woken,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict,
            })
        );

        // Wake both coordinator (INHERIT) and worker (SHARE).
        let wake = client
            .post_swarm_message(
                auth,
                ws,
                run_id,
                &PostSwarmMessageRequest {
                    message_id: Uuid::now_v7().to_string(),
                    channel_id: None,
                    author_member_id: None,
                    body: "@dispatch-coord @dispatch-worker execute slice".to_owned(),
                },
            )
            .await?;
        assert_eq!(wake.woken_member_ids.len(), 2);

        // INHERIT member with forbidden SSD default_worktree_path -> 409 Conflict.
        let mut ssd_req = build_dispatch_client_req(worktree, now_ms)?;
        "/Volumes/256.SSD/RustBuild/orchestration/bad"
            .clone_into(&mut ssd_req.default_worktree_path);
        let ssd_err = client
            .dispatch_swarm_member_turn(auth, ws, run_id, coord.member_id, &ssd_req)
            .await;
        assert_eq!(
            ssd_err,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict,
            })
        );
        Ok(())
    }

    fn build_dispatch_client_req(
        worktree: &Path,
        now_ms: i64,
    ) -> TestResult<DispatchSwarmTurnRequest> {
        Ok(DispatchSwarmTurnRequest {
            snapshot_request_id: Uuid::now_v7().to_string(),
            snapshot_id: Uuid::now_v7().to_string(),
            activity_id: Uuid::from_u128(0xFA10).to_string(),
            default_worktree_path: worktree.to_str().ok_or("utf8 worktree")?.to_owned(),
            lease_id: Uuid::from_u128(0xFA11).to_string(),
            lease_revision: 1,
            lease_expires_at_ms: now_ms + 300_000,
            mcp_grant: None,
            permissions: ExecutionPermissionsWire {
                allow_workspace_write: true,
                allow_local_shell: true,
                allow_network: false,
            },
            timeouts: ExecutionTimeoutsWire {
                startup_timeout_ms: 5_000,
                execution_timeout_ms: 15_000,
                termination_grace_ms: 2_000,
            },
            dispatched_at_ms: None,
            clean_exit_output_commit_oid: None,
            clean_exit_output_tree_blake3_hex: None,
            forge_session_id: None,
        })
    }

    async fn run_live_cli_dispatch_and_capture_env(
        addr_arg: &str,
        bearer_arg: &str,
        ws: Uuid,
        run_id: Uuid,
        worker_id: Uuid,
        worktree: &Path,
    ) -> TestResult<BTreeMap<String, OsString>> {
        let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let lease_exp_str = (now_ms + 300_000).to_string();
        let ws_str = ws.to_string();
        let run_str = run_id.to_string();
        let worker_str = worker_id.to_string();
        let snap_id_str = Uuid::now_v7().to_string();
        let act_str = Uuid::from_u128(0xFA20).to_string();
        let lease_str = Uuid::from_u128(0xFA21).to_string();
        let worktree_str = worktree.to_str().ok_or("utf8 worktree")?;

        let dispatched: DispatchSwarmTurnResponse = run_swarm_cli_json(&[
            "swarm",
            "dispatch",
            "--addr",
            addr_arg,
            "--bearer-file",
            bearer_arg,
            "--workspace-id",
            &ws_str,
            "--swarm-run-id",
            &run_str,
            "--member-id",
            &worker_str,
            "--snapshot-id",
            &snap_id_str,
            "--activity-id",
            &act_str,
            "--worktree-path",
            worktree_str,
            "--lease-id",
            &lease_str,
            "--lease-revision",
            "1",
            "--lease-expires-at-ms",
            &lease_exp_str,
            "--allow-workspace-write",
            "--allow-local-shell",
            "--startup-timeout-ms",
            "5000",
            "--execution-timeout-ms",
            "15000",
            "--termination-grace-ms",
            "2000",
        ])
        .await?;
        assert_eq!(dispatched.wakeup_batch.messages.len(), 1);
        assert_eq!(dispatched.committed_snapshot.resolved_engine, "codex");
        assert_eq!(dispatched.committed_snapshot.resolved_model, "gpt-6-astra");
        assert_eq!(dispatched.supervision.status, "EXITED");
        assert_eq!(dispatched.supervision.exit_code, Some(0));
        assert_eq!(dispatched.disposition.kind, "COMPLETED");
        assert_eq!(dispatched.disposition.member.member_state, "WAITING");
        assert!(dispatched.used_parent_spawner_slot);

        parse_captured_worker_env(&worktree.join("worker-env.txt"), addr_arg)
    }

    fn parse_captured_worker_env(
        env_file: &Path,
        expected_addr: &str,
    ) -> TestResult<BTreeMap<String, OsString>> {
        let raw = fs::read_to_string(env_file)?;
        let parts: Vec<&str> = raw.split('|').collect();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[3], expected_addr);

        let mut env_map = BTreeMap::new();
        env_map.insert(
            "HETERARCHY_WORKSPACE_ID".to_owned(),
            OsString::from(parts[0]),
        );
        env_map.insert(
            "HETERARCHY_SWARM_RUN_ID".to_owned(),
            OsString::from(parts[1]),
        );
        env_map.insert(
            "HETERARCHY_SWARM_MEMBER_ID".to_owned(),
            OsString::from(parts[2]),
        );
        env_map.insert(
            "HETERARCHY_ORCHESTRATION_BACKEND_ADDR".to_owned(),
            OsString::from(parts[3]),
        );
        env_map.insert(
            "HETERARCHY_ORCHESTRATION_BEARER_FILE".to_owned(),
            OsString::from(parts[4]),
        );
        Ok(env_map)
    }

    async fn verify_worker_env_only_swarm_cli(
        ws: Uuid,
        run_id: Uuid,
        worker_id: Uuid,
        worker_env: &BTreeMap<String, OsString>,
    ) -> TestResult {
        let posted: PostSwarmMessageResponse = run_swarm_cli_with_env_json(
            &["swarm", "post", "--channel", "main", "--body", "report"],
            worker_env,
        )
        .await?;
        assert_eq!(posted.message.sequence, 2);
        assert_eq!(posted.message.author_member_id, Some(worker_id));
        assert_eq!(posted.message.body, "report");

        let read_page: SwarmBoardPageResponse = run_swarm_cli_with_env_json(
            &["swarm", "read", "--channel", "main", "--after-seq", "0"],
            worker_env,
        )
        .await?;
        assert_eq!(read_page.messages.len(), 2);
        assert_eq!(read_page.messages[1].body, "report");

        let status: SwarmRunInspectionResponse =
            run_swarm_cli_with_env_json(&["swarm", "status"], worker_env).await?;
        assert_eq!(status.run.workspace_id, ws);
        assert_eq!(status.run.swarm_run_id, run_id);
        assert_eq!(status.members.len(), 2);
        Ok(())
    }
}

mod live_east_west_bridge_e2e {
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use forge_backend::config::BackendConfig;
    use forge_backend::http::{BackendClients, assemble, serve};
    use forge_control::{
        AgentBindingActivateRequest, AgentBindingReserveRequest, AgentOperationsDelegationRequest,
        CoordinatorClaimRequest, ForgeApprovalCoordinator, ForgeControl, ForgeControlClient,
        ForgeControlWorker, ForgeExecutionCoordinator, ResolvedWorkspaceScope,
    };
    use forge_domain::{
        ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentSessionId,
        AgentSessionOpenRequestId, AgentTransportKind, AuthenticatedPrincipal, EstablishWorkspace,
        ForgeBackendId, LocationId, LocationRevision, OpenAgentSession, OrchestrationIssuerId,
        RuntimeInstanceId, TimestampMillis, WorkspaceId, WorkspaceRoot,
    };
    use forge_persistence::{
        AgentBindingActivationRequest, AgentBindingReservationRequest,
        AgentOperationsFreezeRequest, GlobalRegistry, WorkspaceStore,
    };
    use forge_workspace::{LocationService, WorkspaceStateLayout};
    use heterarchy_orchestration_backend::{
        CoordinatorAuthority, SwarmHostOptions,
        client::{ClientError, OrchestrationHttpClient, ServiceCode},
        forge_source_config, serve_with_runtime_and_swarm_options,
        wire::{
            AgentProfileCreateRequest, AgentProfileCreateResponse, ExecutionPreferenceWire,
            autonomy_loop::{ExecutionPermissionsWire, ExecutionTimeoutsWire},
            swarm::{
                AddSwarmMemberRequest, DispatchSwarmTurnRequest, ForgeConvergeSwarmRequest,
                ForgeSettleSwarmRequest, PostSwarmMessageRequest, RecordSwarmVerdictRequest,
                SyncAutonomyOutboxRequest,
            },
        },
    };
    use heterarchy_orchestration_runtime::{
        OrchestrationRuntime,
        access::OperatorPolicy,
        execution_supervisor::{CommandOverride, LocalProcessRunnerConfig},
    };
    use hyper::StatusCode;
    use hyper::header::HeaderValue;
    use serde_json::{Value, json};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        FakeSingleNodeBroker, IssuedTokens, READY_DEADLINE, TempDir, TestResult,
        author_identity_wire, reserve_loopback_addr, send_http_raw, standalone_run_request,
        start_tokens, wait_for_ready,
    };

    const FORGE_BEARER_SECRET: &str = "live-e2e7-forge-bearer-secret";
    const WORKER_WRITE_SCRIPT: &str =
        "printf 'settled from live east-west swarm\\n' > e2e7_settled.txt";

    struct VarTmpScratch(PathBuf);

    impl VarTmpScratch {
        fn create(label: &str) -> TestResult<Self> {
            let raw = PathBuf::from(format!(
                "/var/tmp/heterarchy-{label}-{}-{}",
                std::process::id(),
                Uuid::now_v7()
            ));
            fs::create_dir_all(&raw)?;
            fs::set_permissions(&raw, fs::Permissions::from_mode(0o700))?;
            Ok(Self(raw.canonicalize()?))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for VarTmpScratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct SeededForgeState {
        root: PathBuf,
        workspace_root: PathBuf,
        registry_path: PathBuf,
        scope: ResolvedWorkspaceScope,
        activity_id: ActivityId,
        session_id: AgentSessionId,
        coordinator_agent_id: AgentInstanceId,
        migration_id: Uuid,
    }

    struct RunningForgeServer {
        state: forge_backend::AppState,
        address: SocketAddr,
        backend_id: Uuid,
        worker: ForgeControlWorker,
        lease_worker: ForgeControlWorker,
        execution: ForgeExecutionCoordinator,
        approval: ForgeApprovalCoordinator,
    }

    impl RunningForgeServer {
        async fn shutdown(self) -> TestResult {
            let Self {
                state,
                mut worker,
                mut lease_worker,
                mut execution,
                mut approval,
                ..
            } = self;
            let report = state.shutdown(Duration::from_secs(5)).await;
            assert!(report.is_successful(), "forge drain report: {report:?}");
            let _ = approval.shutdown();
            let _ = execution.shutdown();
            worker.shutdown()?;
            lease_worker.shutdown()?;
            Ok(())
        }
    }

    struct LiveSseCollector {
        kinds: Arc<Mutex<Vec<String>>>,
        notify: Arc<Notify>,
        reader_task: tokio::task::JoinHandle<()>,
    }

    impl Drop for LiveSseCollector {
        fn drop(&mut self) {
            self.reader_task.abort();
        }
    }

    fn now_ts() -> TestResult<TimestampMillis> {
        let ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        Ok(TimestampMillis::new(ms)?)
    }

    fn add_ms(base: TimestampMillis, delta: i64) -> TestResult<TimestampMillis> {
        Ok(TimestampMillis::new(
            base.get().checked_add(delta).ok_or("timestamp overflow")?,
        )?)
    }

    #[tokio::test]
    async fn e2e_7_live_forge_and_orchestration_east_west_swarm_bridge_and_sse_events() -> TestResult
    {
        let scratch = VarTmpScratch::create("e2e7-east-west")?;
        let orch_dir = TempDir::create("e2e7-orch")?;
        let broker = FakeSingleNodeBroker::start().await?;
        let tokens = start_tokens(&orch_dir.path().join("auth"))?;
        let runtime_root = orch_dir.path().join("runtime");
        fs::create_dir_all(&runtime_root)?;
        let runtime = Arc::new(
            OrchestrationRuntime::open_with_broker_cluster(
                &runtime_root,
                broker.cluster_config()?,
            )?
            .with_operator_policy(OperatorPolicy::new(vec![tokens.operator_principal])?),
        );

        let seeded = seed_forge_workspace_and_coordinator(
            &scratch.path().join("forge"),
            tokens.operator_principal,
            runtime.destination_id(),
        )?;
        let forge_server = start_live_forge_server(&seeded, runtime.destination_id()).await?;
        let authority = load_http_coordinator_authority(
            scratch.path(),
            forge_server.address,
            &seeded.scope,
            forge_server.backend_id,
            runtime.destination_id(),
        )?;

        let (orch_addr, orch_cancel, orch_task) = start_live_orchestration_server(
            scratch.path(),
            &tokens,
            Arc::clone(&runtime),
            authority,
        )
        .await?;
        let ws_uuid = seeded.scope.workspace_id().into_uuid();
        let sse = connect_workspace_sse(orch_addr, &tokens.operator_bearer, ws_uuid).await?;

        let flow_res = exercise_live_bridge_lifecycle(orch_addr, &tokens, &seeded).await;
        let sse_res = wait_for_expected_sse_kinds(&sse).await;

        orch_cancel.cancel();
        orch_task.await??;
        drop(tokens.token_runtime);
        forge_server.shutdown().await?;

        flow_res?;
        sse_res
    }

    fn seed_forge_workspace_and_coordinator(
        forge_root: &Path,
        principal: AuthenticatedPrincipal,
        destination_id: Uuid,
    ) -> TestResult<SeededForgeState> {
        fs::create_dir_all(forge_root)?;
        let now = now_ts()?;
        let registry_path = forge_root.join("registry.sqlite3");
        let (scope, workspace_root) = create_forge_location_and_workspace(&registry_path, now)?;
        fs::write(workspace_root.join("README.md"), "baseline\n")?;

        let activity_id = ActivityId::from_uuid(Uuid::now_v7());
        let session_id = AgentSessionId::from_uuid(Uuid::now_v7());
        let coordinator_agent_id = AgentInstanceId::from_uuid(Uuid::now_v7());
        let migration_id = Uuid::now_v7();

        let control = ForgeControl::new(GlobalRegistry::open(&registry_path)?);
        let _ = control.bootstrap_activity(
            &scope,
            activity_id,
            RuntimeInstanceId::from_uuid(Uuid::now_v7()),
            now,
            add_ms(now, 600_000)?,
        )?;
        let _ = control.agent_session_open(&OpenAgentSession::new(
            session_id,
            AgentSessionOpenRequestId::from_uuid(Uuid::now_v7()),
            principal,
            add_ms(now, 1)?,
        ))?;

        let layout = WorkspaceStateLayout::inspect_existing(&workspace_root)?
            .ok_or("missing forge workspace layout")?;
        let mut store = WorkspaceStore::open_existing(&layout)?;
        let freeze = store.freeze_agent_operations(&AgentOperationsFreezeRequest::new(
            migration_id,
            destination_id,
            add_ms(now, 5)?,
        )?)?;
        drop(store);

        let (client, mut worker) =
            ForgeControlWorker::start(ForgeControl::new(GlobalRegistry::open(&registry_path)?))?;
        bind_and_claim_coordinator(
            &client,
            &scope,
            (activity_id, coordinator_agent_id, session_id, principal),
            freeze,
            now,
        )?;
        worker.shutdown()?;

        Ok(SeededForgeState {
            root: forge_root.to_path_buf(),
            workspace_root,
            registry_path,
            scope,
            activity_id,
            session_id,
            coordinator_agent_id,
            migration_id,
        })
    }

    fn create_forge_location_and_workspace(
        registry_path: &Path,
        now: TimestampMillis,
    ) -> TestResult<(ResolvedWorkspaceScope, PathBuf)> {
        let location_root = registry_path
            .parent()
            .ok_or("registry parent missing")?
            .join("location");
        let workspace_root = location_root.join("workspace-0");
        fs::create_dir_all(&workspace_root)?;
        let location_id = LocationId::from_uuid(Uuid::now_v7());
        let workspace_id = WorkspaceId::from_uuid(Uuid::now_v7());

        let mut locations = LocationService::new(GlobalRegistry::open(registry_path)?);
        let location =
            locations.register_selected_path(location_id, "e2e7-loc", &location_root, now)?;
        let layout = WorkspaceStateLayout::initialize(&workspace_root)?;
        let establish = EstablishWorkspace::new(
            workspace_id,
            location.id(),
            WorkspaceRoot::from_canonical(layout.workspace_root().to_path_buf())?,
            layout.root_fingerprint()?,
            add_ms(now, 1)?,
        );
        drop(WorkspaceStore::open(&layout, &establish)?);
        drop(locations);

        let control = ForgeControl::new(GlobalRegistry::open(registry_path)?);
        let locator = control
            .workspace_discover(location_id, LocationRevision::INITIAL)?
            .into_iter()
            .find(|entry| entry.display_name() == "workspace-0")
            .ok_or("workspace-0 discovery missing")?
            .locator()
            .clone();
        let scope = control.resolve_workspace(location_id, LocationRevision::INITIAL, &locator)?;
        Ok((scope, workspace_root))
    }

    fn bind_and_claim_coordinator(
        client: &ForgeControlClient,
        scope: &ResolvedWorkspaceScope,
        ids: (
            ActivityId,
            AgentInstanceId,
            AgentSessionId,
            AuthenticatedPrincipal,
        ),
        freeze: forge_persistence::AgentOperationsFreeze,
        now: TimestampMillis,
    ) -> TestResult {
        let (activity_id, agent_id, session_id, principal) = ids;
        let req_id = Uuid::now_v7();
        let reservation = AgentBindingReservationRequest::new(
            req_id,
            activity_id,
            agent_id,
            session_id,
            principal,
            add_ms(now, 10)?,
        )?;
        let _ = client.reserve_agent_binding(AgentBindingReserveRequest {
            scope: scope.clone(),
            freeze: freeze.clone(),
            reservation,
            observed_at: add_ms(now, 11)?,
        })?;
        let delegation = client.delegate_agent_operations(AgentOperationsDelegationRequest {
            scope: scope.clone(),
            freeze,
            manifest_digest: [0xE7; 32],
            observed_at: add_ms(now, 12)?,
        })?;
        let activation = AgentBindingActivationRequest::new(
            req_id,
            [0xE8; 32],
            AgentDisplayName::new("e2e7-coordinator".to_owned())?,
            AgentDeclaredRole::new("coordinator".to_owned())?,
            AgentTransportKind::StreamableHttp,
        )?;
        let _ = client.activate_agent_binding(AgentBindingActivateRequest {
            scope: scope.clone(),
            delegation,
            activation,
            session_id,
            principal,
            observed_at: add_ms(now, 13)?,
        })?;
        let _ = client.activity_coordinator_claim(CoordinatorClaimRequest {
            session_id,
            scope: scope.clone(),
            activity_id,
            agent_id,
            principal,
            observed_at: add_ms(now, 14)?,
        })?;
        Ok(())
    }

    async fn start_live_forge_server(
        seeded: &SeededForgeState,
        destination_id: Uuid,
    ) -> TestResult<RunningForgeServer> {
        let control = ForgeControl::new(GlobalRegistry::open_existing(&seeded.registry_path)?);
        let (client, worker) = ForgeControlWorker::start(control)?;
        let (activity_leases, lease_worker) = ForgeControlWorker::start_activity_lease(
            ForgeControl::new(GlobalRegistry::open_existing(&seeded.registry_path)?),
        )?;
        let execution = ForgeExecutionCoordinator::start(client.clone(), 2)?;
        let approval = ForgeApprovalCoordinator::start(client.clone(), execution.client())?;
        let execution_client = execution.client();
        let approval_client = approval.client();
        let product_cache = seeded.root.join("product-cache");
        let backend_id = Uuid::now_v7();
        let state = assemble(
            BackendConfig::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                0,
                FORGE_BEARER_SECRET,
                ForgeBackendId::from_uuid(backend_id),
                OrchestrationIssuerId::from_uuid(Uuid::now_v7()),
            )?,
            BackendClients {
                control: &client,
                execution: &execution_client,
                approval: &approval_client,
                activity_leases: &activity_leases,
                runtime_instance_id: Uuid::now_v7(),
                product_cache_root: &product_cache,
                destination_id,
            },
        )?;
        let address = serve(state.clone()).await?;
        Ok(RunningForgeServer {
            state,
            address,
            backend_id,
            worker,
            lease_worker,
            execution,
            approval,
        })
    }

    fn load_http_coordinator_authority(
        scratch: &Path,
        forge_address: SocketAddr,
        scope: &ResolvedWorkspaceScope,
        backend_id: Uuid,
        destination_id: Uuid,
    ) -> TestResult<CoordinatorAuthority> {
        let bearer_path = scratch.join("forge-source.bearer");
        fs::write(&bearer_path, format!("{FORGE_BEARER_SECRET}\n"))?;
        fs::set_permissions(&bearer_path, fs::Permissions::from_mode(0o600))?;
        let config_path = scratch.join("forge-source.json");
        let doc = json!({
            "address": forge_address.to_string(),
            "bearer_file": bearer_path,
            "scopes": [{
                "workspace_id": scope.workspace_id().into_uuid(),
                "workspace_revision": scope.workspace_revision().get(),
                "locator": {
                    "location_id": scope.locator().location_id().into_uuid(),
                    "expected_revision": scope.locator().expected_revision().get(),
                    "relative_root": scope.locator().relative_root().to_string_lossy(),
                }
            }],
            "effect_facts": {
                "backend_id": backend_id,
                "destination_id": destination_id,
            }
        });
        fs::write(&config_path, serde_json::to_vec_pretty(&doc)?)?;
        Ok(forge_source_config::load(&config_path)?)
    }

    async fn start_live_orchestration_server(
        scratch: &Path,
        tokens: &IssuedTokens,
        runtime: Arc<OrchestrationRuntime>,
        authority: CoordinatorAuthority,
    ) -> TestResult<(
        SocketAddr,
        CancellationToken,
        tokio::task::JoinHandle<Result<(), heterarchy_orchestration_backend::BackendError>>,
    )> {
        let raw_reveal = tokens
            .operator_bearer
            .strip_prefix("Bearer ")
            .ok_or("expected Bearer prefix")?;
        let worker_bearer = scratch.join("orch-worker.token");
        fs::write(&worker_bearer, format!("{raw_reveal}\n"))?;
        fs::set_permissions(&worker_bearer, fs::Permissions::from_mode(0o600))?;
        let runner_cfg = LocalProcessRunnerConfig::new()
            .with_command_override(CommandOverride::new("/bin/sh", ["-c", WORKER_WRITE_SCRIPT]));
        let swarm_options = SwarmHostOptions::default()
            .with_worker_bearer_file(worker_bearer.canonicalize()?)
            .with_process_runner_config(runner_cfg);

        let address = reserve_loopback_addr()?;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(serve_with_runtime_and_swarm_options(
            runtime,
            address,
            tokens.token_runtime.client(),
            Some(authority),
            swarm_options,
            cancellation.clone(),
        ));
        wait_for_ready(address).await?;
        Ok((address, cancellation, task))
    }

    async fn connect_workspace_sse(
        address: SocketAddr,
        bearer: &str,
        workspace_id: Uuid,
    ) -> TestResult<LiveSseCollector> {
        let stream = TcpStream::connect(address).await?;
        let (read_half, mut write_half) = stream.into_split();
        let req = format!(
            "GET /v1/workspaces/{workspace_id}/events HTTP/1.1\r\nHost: {address}\r\nAuthorization: {bearer}\r\nAccept: text/event-stream\r\n\r\n"
        );
        write_half.write_all(req.as_bytes()).await?;
        write_half.flush().await?;

        let mut reader = BufReader::new(read_half);
        let mut status_line = String::new();
        reader.read_line(&mut status_line).await?;
        assert!(
            status_line.starts_with("HTTP/1.1 200"),
            "unexpected SSE status: {status_line}"
        );
        loop {
            let mut hdr = String::new();
            reader.read_line(&mut hdr).await?;
            if hdr == "\r\n" || hdr.is_empty() {
                break;
            }
        }

        let kinds = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(Notify::new());
        let kinds_clone = Arc::clone(&kinds);
        let notify_clone = Arc::clone(&notify);
        let reader_task = tokio::spawn(async move {
            let _keep_write = write_half;
            let mut line = String::new();
            loop {
                line.clear();
                let Ok(n) = reader.read_line(&mut line).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                if let Some(json_str) = line.trim_end().strip_prefix("data:")
                    && let Ok(val) = serde_json::from_str::<Value>(json_str.trim())
                    && let Some(kind) = val.get("kind").and_then(Value::as_str)
                    && let Ok(mut guard) = kinds_clone.lock()
                {
                    guard.push(kind.to_owned());
                    drop(guard);
                    notify_clone.notify_waiters();
                }
            }
        });
        Ok(LiveSseCollector {
            kinds,
            notify,
            reader_task,
        })
    }

    async fn exercise_live_bridge_lifecycle(
        orch_addr: SocketAddr,
        tokens: &IssuedTokens,
        seeded: &SeededForgeState,
    ) -> TestResult {
        let profile_id = create_active_worker_profile(orch_addr, &tokens.operator_bearer).await?;
        let client = OrchestrationHttpClient::new(orch_addr, Duration::from_secs(10))?;
        let auth = HeaderValue::from_str(&tokens.operator_bearer)?;
        let ws = seeded.scope.workspace_id().into_uuid();

        let (run_id, worker_id, verifier_id) =
            seed_swarm_roster(&client, &auth, ws, profile_id).await?;
        dispatch_and_verify_auto_capture(&client, &auth, ws, run_id, worker_id, seeded).await?;

        let converged = client
            .forge_converge_swarm_run(
                &auth,
                ws,
                run_id,
                &ForgeConvergeSwarmRequest {
                    request_id: Uuid::now_v7().to_string(),
                    session_id: seeded.session_id.into_uuid().to_string(),
                    candidate_member_ids: Vec::new(),
                    stage_candidate_only: true,
                },
            )
            .await?;
        assert_eq!(converged.run.state, "CANDIDATE_READY");
        assert_eq!(converged.changed_files, vec!["e2e7_settled.txt".to_owned()]);
        let diff_hex = converged
            .run
            .converged_diff_digest_hex
            .ok_or("missing converged_diff_digest_hex")?;

        verify_settle_gating_and_finalize(
            &client,
            &auth,
            ws,
            run_id,
            verifier_id,
            &diff_hex,
            seeded,
        )
        .await
    }

    async fn create_active_worker_profile(orch_addr: SocketAddr, bearer: &str) -> TestResult<Uuid> {
        let (status, created): (_, AgentProfileCreateResponse) = post_json_raw(
            orch_addr,
            bearer,
            "/internal/v1/agent-profiles",
            &AgentProfileCreateRequest {
                request_id: Uuid::now_v7().to_string(),
                stable_name: "e2e7-bridge-worker".to_owned(),
                display_name: "E2E7 Bridge Worker".to_owned(),
                preferred_model: Some("gpt-6-astra".to_owned()),
                preferred_reasoning: Some("high".to_owned()),
                base_capabilities: vec!["rust".to_owned()],
                execution_preference: Some(ExecutionPreferenceWire {
                    cli_engine: "codex".to_owned(),
                    preferred_model: "gpt-6-astra".to_owned(),
                    preferred_reasoning: Some("high".to_owned()),
                    capabilities: vec!["rust".to_owned()],
                    role_prompt_digest_hex: None,
                }),
            },
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        Ok(created.profile.profile_id)
    }

    async fn post_json_raw<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        addr: SocketAddr,
        bearer: &str,
        path: &str,
        body: &Req,
    ) -> TestResult<(StatusCode, Resp)> {
        let encoded = serde_json::to_vec(body)?;
        let (status, bytes) = send_http_raw(addr, "POST", path, Some(bearer), &encoded).await?;
        Ok((status, serde_json::from_slice(&bytes)?))
    }

    async fn seed_swarm_roster(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        profile_id: Uuid,
    ) -> TestResult<(Uuid, Uuid, Uuid)> {
        let created = client
            .create_swarm_run(
                auth,
                ws,
                &standalone_run_request(Uuid::now_v7(), "E2E 7 Live East-West Swarm"),
            )
            .await?;
        let run_id = created.run.swarm_run_id;

        let mut coord_ident = author_identity_wire(0xD1);
        coord_ident.profile_id = profile_id.to_string();
        let coord = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "e2e7-coord".to_owned(),
                    role_kind: "COORDINATOR".to_owned(),
                    parent_member_id: None,
                    workspace_mode: "INHERIT".to_owned(),
                    worktree_path: None,
                    identity: coord_ident,
                    initial_channels: Vec::new(),
                },
            )
            .await?;

        let mut worker_ident = author_identity_wire(0xD2);
        worker_ident.profile_id = profile_id.to_string();
        let worker = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "e2e7-worker".to_owned(),
                    role_kind: "WORKER".to_owned(),
                    parent_member_id: Some(coord.member_id.to_string()),
                    workspace_mode: "SHARE".to_owned(),
                    worktree_path: None,
                    identity: worker_ident,
                    initial_channels: Vec::new(),
                },
            )
            .await?;

        let verifier = client
            .add_swarm_member(
                auth,
                ws,
                run_id,
                &AddSwarmMemberRequest {
                    member_id: Uuid::now_v7().to_string(),
                    roster_name: "e2e7-verifier".to_owned(),
                    role_kind: "VERIFIER".to_owned(),
                    parent_member_id: None,
                    workspace_mode: "INHERIT".to_owned(),
                    worktree_path: None,
                    identity: author_identity_wire(0xD3),
                    initial_channels: Vec::new(),
                },
            )
            .await?;

        Ok((run_id, worker.member_id, verifier.member_id))
    }

    async fn dispatch_and_verify_auto_capture(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        run_id: Uuid,
        worker_id: Uuid,
        seeded: &SeededForgeState,
    ) -> TestResult {
        let wake = client
            .post_swarm_message(
                auth,
                ws,
                run_id,
                &PostSwarmMessageRequest {
                    message_id: Uuid::now_v7().to_string(),
                    channel_id: None,
                    author_member_id: None,
                    body: "@e2e7-worker produce settled artifact".to_owned(),
                },
            )
            .await?;
        assert_eq!(wake.woken_member_ids, vec![worker_id]);

        let now_ms = now_ts()?.get();
        let dispatched = client
            .dispatch_swarm_member_turn(
                auth,
                ws,
                run_id,
                worker_id,
                &DispatchSwarmTurnRequest {
                    snapshot_request_id: Uuid::now_v7().to_string(),
                    snapshot_id: Uuid::now_v7().to_string(),
                    activity_id: seeded.activity_id.into_uuid().to_string(),
                    default_worktree_path: String::new(),
                    lease_id: Uuid::now_v7().to_string(),
                    lease_revision: 1,
                    lease_expires_at_ms: now_ms + 300_000,
                    mcp_grant: None,
                    permissions: ExecutionPermissionsWire {
                        allow_workspace_write: true,
                        allow_local_shell: true,
                        allow_network: false,
                    },
                    timeouts: ExecutionTimeoutsWire {
                        startup_timeout_ms: 5_000,
                        execution_timeout_ms: 15_000,
                        termination_grace_ms: 2_000,
                    },
                    dispatched_at_ms: Some(now_ms),
                    clean_exit_output_commit_oid: None,
                    clean_exit_output_tree_blake3_hex: None,
                    forge_session_id: Some(seeded.session_id.into_uuid().to_string()),
                },
            )
            .await?;
        assert_eq!(dispatched.supervision.exit_code, Some(0));
        assert_eq!(dispatched.disposition.kind, "COMPLETED");
        assert!(
            dispatched
                .disposition
                .member
                .last_output_commit_oid
                .is_some()
        );
        assert!(
            dispatched
                .disposition
                .member
                .last_output_tree_blake3_hex
                .is_some()
        );
        assert!(!seeded.workspace_root.join("e2e7_settled.txt").exists());
        Ok(())
    }

    async fn verify_settle_gating_and_finalize(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        run_id: Uuid,
        verifier_id: Uuid,
        diff_hex: &str,
        seeded: &SeededForgeState,
    ) -> TestResult {
        let premature = client
            .forge_settle_swarm_run(
                auth,
                ws,
                run_id,
                &ForgeSettleSwarmRequest {
                    request_id: Uuid::now_v7().to_string(),
                    session_id: seeded.session_id.into_uuid().to_string(),
                    activity_id: Some(seeded.activity_id.into_uuid().to_string()),
                    coordinator_agent_id: seeded.coordinator_agent_id.into_uuid().to_string(),
                    outbox_migration_id: Some(seeded.migration_id.to_string()),
                    outbox_after_sequence: Some(0),
                    purge_ephemeral: true,
                },
            )
            .await;
        assert_eq!(
            premature,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict,
            })
        );
        assert!(!seeded.workspace_root.join("e2e7_settled.txt").exists());

        let verdict = client
            .record_swarm_verdict(
                auth,
                ws,
                run_id,
                &RecordSwarmVerdictRequest {
                    request_id: Uuid::now_v7().to_string(),
                    panel_id: None,
                    verifier_member_id: verifier_id.to_string(),
                    outcome: "ACCEPTED".to_owned(),
                    reviewed_diff_digest_hex: diff_hex.to_owned(),
                    rationale: "verified converged diff".to_owned(),
                },
            )
            .await?;
        assert_eq!(verdict.outcome, "ACCEPTED");

        let settled = client
            .forge_settle_swarm_run(
                auth,
                ws,
                run_id,
                &ForgeSettleSwarmRequest {
                    request_id: Uuid::now_v7().to_string(),
                    session_id: seeded.session_id.into_uuid().to_string(),
                    activity_id: Some(seeded.activity_id.into_uuid().to_string()),
                    coordinator_agent_id: seeded.coordinator_agent_id.into_uuid().to_string(),
                    outbox_migration_id: Some(seeded.migration_id.to_string()),
                    outbox_after_sequence: Some(0),
                    purge_ephemeral: true,
                },
            )
            .await?;
        assert_eq!(settled.run.state, "SETTLED");
        assert!(settled.settled_file_count >= 1);
        assert!(settled.purged_ephemeral);
        assert_eq!(settled.outbox_sequence, Some(1));
        let sync_receipt = settled.outbox_sync.ok_or("missing outbox_sync receipt")?;
        assert_eq!(sync_receipt.applied_count, 1);
        assert_eq!(sync_receipt.swarm_settled_count, 1);
        assert_eq!(sync_receipt.last_sequence, 1);

        let disk_content = fs::read_to_string(seeded.workspace_root.join("e2e7_settled.txt"))?;
        assert_eq!(disk_content, "settled from live east-west swarm\n");

        verify_replay_and_mismatched_settle(client, auth, ws, run_id, seeded).await
    }

    async fn verify_replay_and_mismatched_settle(
        client: &OrchestrationHttpClient,
        auth: &HeaderValue,
        ws: Uuid,
        run_id: Uuid,
        seeded: &SeededForgeState,
    ) -> TestResult {
        let replay_sync = client
            .sync_autonomy_outbox(
                auth,
                ws,
                &SyncAutonomyOutboxRequest {
                    migration_id: seeded.migration_id.to_string(),
                    after_sequence: 0,
                    limit: Some(16),
                },
            )
            .await?;
        assert_eq!(replay_sync.applied_count, 0);
        assert_eq!(replay_sync.dag_settled_count, 0);
        assert_eq!(replay_sync.swarm_settled_count, 0);
        assert_eq!(replay_sync.last_sequence, 1);

        let mismatched_settle = client
            .forge_settle_swarm_run(
                auth,
                ws,
                run_id,
                &ForgeSettleSwarmRequest {
                    request_id: Uuid::now_v7().to_string(),
                    session_id: seeded.session_id.into_uuid().to_string(),
                    activity_id: Some(seeded.activity_id.into_uuid().to_string()),
                    coordinator_agent_id: seeded.coordinator_agent_id.into_uuid().to_string(),
                    outbox_migration_id: None,
                    outbox_after_sequence: None,
                    purge_ephemeral: false,
                },
            )
            .await;
        assert_eq!(
            mismatched_settle,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict,
            })
        );
        Ok(())
    }

    async fn wait_for_expected_sse_kinds(sse: &LiveSseCollector) -> TestResult {
        let required = [
            "swarm_run_changed",
            "swarm_member_state_changed",
            "swarm_board_message_posted",
            "swarm_verdict_recorded",
            "settlement_projected",
        ];
        let deadline = Instant::now() + READY_DEADLINE;
        loop {
            let observed = sse.kinds.lock().map_err(|_| "sse kinds poisoned")?.clone();
            if required
                .iter()
                .all(|expected| observed.iter().any(|k| k == expected))
            {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for SSE kinds {required:?}, observed: {observed:?}"
                )
                .into());
            }
            let _ = tokio::time::timeout(deadline - now, sse.notify.notified()).await;
        }
    }
}
