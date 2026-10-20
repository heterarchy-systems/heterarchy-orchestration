use std::{
    error::Error,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use forge_control::ResolvedWorkspaceScope;
use forge_domain::{
    AuthenticatedPrincipal, DirectTokenId, ForgeBackendId, LocationId, LocationRevision,
    OrchestrationIssuerId, WorkspaceId, WorkspaceRevision,
};
use forge_workspace::WorkspaceLocator;
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
    McpTokenWorker,
};
use heterarchy_orchestration_backend::{
    CoordinatorAuthority,
    client::{ClientError, OrchestrationHttpClient, ReadbackTarget},
    forge_commands::ForgeCommandHttpClient,
    forge_source::ForgeSourceHttpClient,
    serve_with_runtime,
    wire::{
        AgentSessionOpenRequest,
        leadership::WorkspaceQueryScope,
        membership::{
            MemberActivateRequest, MemberConfirmRequest, MemberPhaseWire, MemberRegisterRequest,
            MemberRegistrationQueryRequest, MemberTarget, MemberTransportWire,
        },
        node_assignment_admission::{
            NodeAssignmentAdmissionRequest, NodeAssignmentSourceFencesWire,
            NodeAssignmentTargetWire,
        },
        node_chat::{
            ChatMessageRequest, ChatProviderStateWire, ChatQueryWire, NodeChatQueryRequest,
            NodeChatSendRequest,
        },
        node_team::NodeTeamScopeWire,
        workspace_chat::{
            WorkspaceChatMessageRequest, WorkspaceChatQueryRequest, WorkspaceChatQueryWire,
            WorkspaceChatSendRequest, WorkspaceChatTargetFenceWire,
        },
    },
};
use heterarchy_orchestration_operations::{
    AgentRequestEffectScope, AgentRequestIndependence, BrokerClusterConfig,
    ConfigureAdmissionIssuerCommand, NodeTeamBindingScope, native_session_participation_id,
};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime,
    access::OperatorPolicy,
    leadership::member_agent_id,
    node_team::{NodeTeamSet, NodeTeamSourceObservation},
    provider_chat::{ProviderIncomingCommand, ProviderIncomingRole},
    provisioning::AgentRequestSubmit,
    teams::{TeamCreateSpec, TeamRosterMemberSpec},
};
use hyper::header::HeaderValue;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[path = "../../heterarchy-orchestration-runtime/tests/support/broker_cluster.rs"]
mod broker_cluster;

const DEADLINE: Duration = Duration::from_secs(5);
const FAKE_FORGE_SECRET: &str = "fake-exact-forge-proof";
type BackendServer = (
    SocketAddr,
    CancellationToken,
    tokio::task::JoinHandle<Result<(), heterarchy_orchestration_backend::BackendError>>,
);

fn wall_now_ms() -> Result<i64, Box<dyn Error>> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

#[derive(Clone)]
struct FakeForgeState {
    workspace_id: Uuid,
    activity_id: Uuid,
    coordinator_agent_id: Uuid,
    coordinator_session_id: Uuid,
    principal_id: Uuid,
    goal_id: Uuid,
    node_id: Uuid,
    assignment_id: Uuid,
    target_agent_id: Uuid,
    target_session_id: Uuid,
    target_agent_revision: Arc<std::sync::atomic::AtomicI64>,
    target_agent_context_reads: Arc<AtomicUsize>,
    target_agent_revision_on_read: Arc<AtomicUsize>,
    target_agent_revision_after_read: Arc<std::sync::atomic::AtomicI64>,
    assignment_present: Arc<AtomicBool>,
    native_reservation: Arc<Mutex<Option<Value>>>,
    native_activation: Arc<Mutex<Option<Value>>>,
}

struct StoredNativeRequester {
    activity: Vec<u8>,
    agent: Vec<u8>,
    session: Vec<u8>,
    participation: Option<Vec<u8>>,
    profile: Option<Vec<u8>>,
    participant: Option<Vec<u8>>,
    key: String,
    display_name: String,
    offer_payload: Vec<u8>,
}

/// This is an explicit test-only Source HTTP fake. The ignored test launches
/// the pinned Broker artifact separately and never contacts a product Forge.
struct ExactFakeForge {
    address: SocketAddr,
    scope: ResolvedWorkspaceScope,
    state: FakeForgeState,
    cancellation: CancellationToken,
    server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl ExactFakeForge {
    async fn start(state: FakeForgeState) -> Result<Self, Box<dyn Error>> {
        let location_id = LocationId::from_uuid(Uuid::from_u128(0x501));
        let locator = WorkspaceLocator::new(location_id, LocationRevision::INITIAL, ".")?;
        let scope = ResolvedWorkspaceScope::from_observation(
            WorkspaceId::from_uuid(state.workspace_id),
            WorkspaceRevision::INITIAL,
            locator,
        );
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let app = Router::new()
            .route("/api/v1/commands/{op}", post(fake_forge_command))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        Ok(Self {
            address,
            scope,
            state,
            cancellation,
            server,
        })
    }

    fn authority(&self) -> Result<CoordinatorAuthority, Box<dyn Error>> {
        Ok(CoordinatorAuthority::from_http(
            ForgeSourceHttpClient::new(ForgeCommandHttpClient::new(
                self.address,
                FAKE_FORGE_SECRET,
                DEADLINE,
            )?),
            vec![self.scope.clone()],
        )?)
    }

    async fn shutdown(self) -> Result<(), Box<dyn Error>> {
        self.cancellation.cancel();
        self.server.await??;
        Ok(())
    }
}

async fn fake_forge_command(
    State(state): State<FakeForgeState>,
    AxumPath(op): AxumPath<String>,
    Json(envelope): Json<Value>,
) -> Json<Value> {
    let request_id = envelope.get("request_id").cloned().unwrap_or(Value::Null);
    let payload = envelope.get("payload").cloned().unwrap_or(Value::Null);
    let result = fake_forge_result(&state, &op, &payload);
    Json(json!({
        "forge_api": 1,
        "request_id": request_id,
        "ok": true,
        "result": result
    }))
}

fn fake_forge_result(state: &FakeForgeState, op: &str, payload: &Value) -> Value {
    match op {
        "workspace.resolve" => json!({
            "workspace_id": state.workspace_id,
            "workspace_revision": 1
        }),
        "activity.get" => fake_activity_readback(state, payload),
        "agent.operations.delegation_get" => fake_delegation_readback(state, payload),
        "agent.binding.reservation_get" => fake_reservation_readback(state, payload),
        "agent.binding.reserve" => fake_reserve(state, payload),
        "agent.binding.activation_get" => fake_activation_readback(state, payload),
        "agent.binding.activate" => fake_activate(state, payload),
        "activity.coordinator_actor_get" => json!({
            "found": true,
            "actor": {
                "workspace_id": state.workspace_id,
                "activity_id": state.activity_id,
                "agent_id": state.coordinator_agent_id,
                "expected_coordinator_revision": 1,
                "principal": {"kind": "direct_token", "token_id": state.principal_id},
                "session_id": state.coordinator_session_id
            }
        }),
        "agent.get_context" => fake_target_agent_context(state),
        "node.assignment_read" => {
            let assignment = state.assignment_present.load(Ordering::SeqCst).then(|| {
                json!({
                    "assignment_id": state.assignment_id,
                    "assignment_revision": 1,
                    "target_agent_id": state.target_agent_id,
                    "target_session_id": state.target_session_id,
                    "goal_revision": 1,
                    "dag_head_revision": 1,
                    "node_revision": 1,
                    "state": "running",
                    "cancellation_requested": false
                })
            });
            json!({
                "found": true,
                "workspace_id": state.workspace_id,
                "activity_id": state.activity_id,
                "goal_id": state.goal_id,
                "goal_revision": 1,
                "goal_created_at_ms": 20,
                "dag_base_goal_revision": 1,
                "dag_revision": 1,
                "head_revision": 1,
                "current_dag_revision": 1,
                "node_id": state.node_id,
                "node_revision": 1,
                "assignment": assignment
            })
        }
        _ => json!({"unsupported": op}),
    }
}

fn fake_target_agent_context(state: &FakeForgeState) -> Value {
    let context_read = state
        .target_agent_context_reads
        .fetch_add(1, Ordering::SeqCst)
        + 1;
    let revision = if state.target_agent_revision_on_read.load(Ordering::SeqCst) == context_read {
        state
            .target_agent_revision_after_read
            .load(Ordering::SeqCst)
    } else {
        state.target_agent_revision.load(Ordering::SeqCst)
    };
    json!({
        "found": true,
        "agent": {
            "agent_id": state.target_agent_id,
            "workspace_id": state.workspace_id,
            "activity_id": state.activity_id,
            "display_name": "exact test target",
            "attachment_state": "attached",
            "revision": revision
        },
        "activity_revision": 1,
        "activity_liveness": "live",
        "work_updates": [],
        "work_update_count": 0,
        "focus_event_count": 0
    })
}

async fn raw_admission_response(
    address: SocketAddr,
    bearer: &HeaderValue,
    request: &NodeAssignmentAdmissionRequest,
) -> Result<(u16, Option<String>), Box<dyn Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(address).await?;
    let body = serde_json::to_vec(request)?;
    let headers = format!(
        "POST /internal/v1/workspaces/{}/node-assignment-admissions/admit HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         Authorization: {}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        request.scope.workspace_id,
        address.port(),
        bearer.to_str()?,
        body.len(),
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(&body).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let response = String::from_utf8(response)?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or("HTTP response")?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("HTTP status")?
        .parse()?;
    let code = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| value.get("code").and_then(Value::as_str).map(str::to_owned));
    Ok((status, code))
}

fn fake_activity_readback(state: &FakeForgeState, payload: &Value) -> Value {
    let activity_id = payload.get("activity_id").cloned().unwrap_or(Value::Null);
    json!({
        "scope": payload.get("scope").cloned().unwrap_or(Value::Null),
        "activity_id": activity_id,
        "found": true,
        "activity": {
            "activity_id": activity_id,
            "workspace_id": state.workspace_id,
            "state": "open",
            "revision": 1,
            "created_at_ms": 1,
            "updated_at_ms": 1,
            "lease_owner_id": null,
            "lease_until_ms": null,
            "lease_revision": 1
        },
        "retry_authorized": false,
        "request_equivalence_proven": false
    })
}

fn fake_source_base(payload: &Value, found: bool) -> Value {
    json!({
        "scope": payload.get("scope").cloned().unwrap_or(Value::Null),
        "destination_id": payload.get("destination_id").cloned().unwrap_or(Value::Null),
        "found": found,
        "retry_authorized": false,
        "request_equivalence_proven": false
    })
}

fn fake_delegation_readback(state: &FakeForgeState, payload: &Value) -> Value {
    let reference = payload.get("reference").cloned().unwrap_or(Value::Null);
    let mut result = fake_source_base(payload, true);
    result["workspace_id"] = payload
        .get("scope")
        .and_then(|scope| scope.get("workspace_id"))
        .cloned()
        .unwrap_or(Value::Null);
    result["migration_id"] = reference
        .get("migration_id")
        .cloned()
        .unwrap_or(Value::Null);
    result["freeze_destination_id"] = payload
        .get("destination_id")
        .cloned()
        .unwrap_or(Value::Null);
    result["freeze_revision"] = reference
        .get("expected_freeze_revision")
        .cloned()
        .unwrap_or(Value::Null);
    result["freeze_state"] = json!("frozen");
    result["freeze_created_at_ms"] = json!(10);
    result["freeze_updated_at_ms"] = json!(11);
    result["manifest_digest"] = reference
        .get("manifest_digest")
        .cloned()
        .unwrap_or(Value::Null);
    result["delegated_at_ms"] = json!(12);
    // Keep the configured Workspace explicit so this fake cannot silently
    // represent a different Source root if the request shape changes.
    if result["workspace_id"] != json!(state.workspace_id) {
        return Value::Null;
    }
    result
}

fn fake_reservation_readback(state: &FakeForgeState, payload: &Value) -> Value {
    if let Ok(reservation) = state.native_reservation.lock()
        && let Some(reservation) = reservation.as_ref()
    {
        return reservation.clone();
    }
    fake_source_base(payload, false)
}

fn fake_reserve(state: &FakeForgeState, payload: &Value) -> Value {
    let reference = payload.get("reference").cloned().unwrap_or(Value::Null);
    let binding = payload.get("reservation").cloned().unwrap_or(Value::Null);
    let mut result = fake_source_base(payload, true);
    result["workspace_id"] = payload
        .get("scope")
        .and_then(|scope| scope.get("workspace_id"))
        .cloned()
        .unwrap_or(Value::Null);
    result["migration_id"] = reference
        .get("migration_id")
        .cloned()
        .unwrap_or(Value::Null);
    result["freeze_revision"] = reference
        .get("expected_freeze_revision")
        .cloned()
        .unwrap_or(Value::Null);
    for field in [
        "request_id",
        "activity_id",
        "agent_id",
        "session_id",
        "principal",
    ] {
        result[field] = binding.get(field).cloned().unwrap_or(Value::Null);
    }
    result["requested_at_ms"] = binding
        .get("requested_at_ms")
        .cloned()
        .unwrap_or(Value::Null);
    result["reserved_at_ms"] = payload
        .get("observed_at_ms")
        .cloned()
        .unwrap_or(Value::Null);
    if let Ok(mut reservation) = state.native_reservation.lock() {
        *reservation = Some(result.clone());
    }
    result
}

fn fake_activation_readback(state: &FakeForgeState, payload: &Value) -> Value {
    if let Ok(activation) = state.native_activation.lock()
        && let Some(activation) = activation.as_ref()
    {
        return activation.clone();
    }
    fake_source_base(payload, false)
}

fn fake_activate(state: &FakeForgeState, payload: &Value) -> Value {
    let Ok(reservation) = state.native_reservation.lock() else {
        return Value::Null;
    };
    let Some(reservation) = reservation.as_ref() else {
        return Value::Null;
    };
    let mut result = reservation.clone();
    result["membership_digest"] = payload
        .get("activation")
        .and_then(|activation| activation.get("membership_digest"))
        .cloned()
        .unwrap_or(Value::Null);
    result["display_name"] = payload
        .get("activation")
        .and_then(|activation| activation.get("display_name"))
        .cloned()
        .unwrap_or(Value::Null);
    result["declared_role"] = payload
        .get("activation")
        .and_then(|activation| activation.get("declared_role"))
        .cloned()
        .unwrap_or(Value::Null);
    result["transport"] = payload
        .get("activation")
        .and_then(|activation| activation.get("transport"))
        .cloned()
        .unwrap_or(Value::Null);
    result["activated_at_ms"] = payload
        .get("observed_at_ms")
        .cloned()
        .unwrap_or(Value::Null);
    if let Ok(mut activation) = state.native_activation.lock() {
        *activation = Some(result.clone());
    }
    result
}

fn token_authority(
    root: &Path,
) -> Result<(McpTokenRuntime, HeaderValue, AuthenticatedPrincipal), Box<dyn Error>> {
    let catalog = McpTokenCatalog::open(root.join("operator-tokens.sqlite3"))?;
    let (client, worker) = McpTokenWorker::start(McpTokenService::new([0x71; 32], catalog))?;
    let issued = client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("composed HTTP fixture".to_owned())?,
        forge_domain::TimestampMillis::new(1)?,
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

struct ComposedFixture {
    root: PathBuf,
    runtime: Arc<OrchestrationRuntime>,
    bearer: HeaderValue,
    token: McpTokenRuntime,
    principal: AuthenticatedPrincipal,
    scope: WorkspaceQueryScope,
    sender_session: Uuid,
    sender_activity: Uuid,
    sender_agent: Uuid,
    target_activity: Uuid,
    target_agent: Uuid,
    node_target_session: Uuid,
    node_target_agent: Uuid,
    target_profile: Uuid,
    target_runtime: Uuid,
    operation_id: Uuid,
}

struct FixtureIdentities {
    sender_session: Uuid,
    target_session: Uuid,
    node_target_session: Uuid,
    sender_activity: Uuid,
    target_activity: Uuid,
    sender_agent: Uuid,
    target_agent: Uuid,
    node_target_agent: Uuid,
}

struct FixtureRoutes {
    direct_operation: Uuid,
    direct_profile: Uuid,
    direct_runtime: Uuid,
    node_operation: Uuid,
    node_profile: Uuid,
}

fn seed_identities(
    runtime: &OrchestrationRuntime,
    principal: AuthenticatedPrincipal,
    workspace_id: Uuid,
) -> Result<FixtureIdentities, Box<dyn Error>> {
    let sender_session = runtime
        .open_session(Uuid::from_u128(0x601), principal, 2)?
        .session;
    let target_session = runtime
        .open_session(Uuid::from_u128(0x602), principal, 3)?
        .session;
    let node_target_session = runtime
        .open_session(Uuid::from_u128(0x60e), principal, 4)?
        .session;
    let sender_activity = Uuid::from_u128(0x603);
    Ok(FixtureIdentities {
        sender_session: sender_session.session_id,
        target_session: target_session.session_id,
        node_target_session: node_target_session.session_id,
        sender_activity,
        target_activity: Uuid::from_u128(0x604),
        sender_agent: member_agent_id(workspace_id, sender_activity, sender_session.session_id)
            .into_uuid(),
        target_agent: Uuid::from_u128(0x605),
        node_target_agent: member_agent_id(
            workspace_id,
            sender_activity,
            node_target_session.session_id,
        )
        .into_uuid(),
    })
}

fn submit_fixture_request(
    runtime: &OrchestrationRuntime,
    command: AgentRequestSubmit,
    claimed_at_ms: i64,
    reserved_at_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let request_id = command.request_id;
    runtime.submit_agent_request(command)?;
    runtime.reserve_agent_request_resolution(request_id, claimed_at_ms, reserved_at_ms)?;
    Ok(())
}

fn provision_routes(
    runtime: &OrchestrationRuntime,
    principal: AuthenticatedPrincipal,
    workspace_id: Uuid,
    identities: &FixtureIdentities,
) -> Result<FixtureRoutes, Box<dyn Error>> {
    let direct_request = Uuid::from_u128(0x606);
    submit_fixture_request(
        runtime,
        AgentRequestSubmit {
            workspace_id,
            activity_id: identities.target_activity,
            requester_agent_id: identities.sender_agent,
            requester_session_id: identities.sender_session,
            coordinator_revision: 1,
            request_id: direct_request,
            objective: "composed route".to_owned(),
            capability: "chat".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::ReadOnly,
            independence: AgentRequestIndependence::None,
            reason: "HTTP acceptance".to_owned(),
            created_at_ms: 4,
        },
        5,
        6,
    )?;
    let node_request = Uuid::from_u128(0x610);
    submit_fixture_request(
        runtime,
        AgentRequestSubmit {
            workspace_id,
            activity_id: identities.sender_activity,
            requester_agent_id: identities.sender_agent,
            requester_session_id: identities.sender_session,
            coordinator_revision: 1,
            request_id: node_request,
            objective: "node composed route".to_owned(),
            capability: "node-chat".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::ReadOnly,
            independence: AgentRequestIndependence::None,
            reason: "HTTP node acceptance".to_owned(),
            created_at_ms: 7,
        },
        8,
        9,
    )?;
    let direct_prepared = runtime.prepare_agent_request_effect_targets(direct_request, 10, 11)?;
    let node_prepared = runtime.prepare_agent_request_effect_targets(node_request, 12, 13)?;
    let direct_action = runtime
        .provider_worker_next_provision(principal, 14)?
        .ok_or("provider action")?;
    assert_eq!(
        direct_action.operation().operation_id(),
        direct_prepared[0].operation_id
    );
    let direct = runtime.provider_worker_confirm_provision(
        principal,
        direct_action.operation().operation_id(),
        direct_action.operation().revision(),
        "conversation-composed",
        15,
    )?;
    let direct_profile = direct.profile_id().ok_or("target profile")?;
    let direct_runtime = direct.runtime_session_id().ok_or("target runtime")?;
    runtime.bind_profile_session(
        direct.operation_id(),
        direct_profile.into_uuid(),
        identities.target_session,
        principal,
        16,
    )?;
    let node_action = runtime
        .provider_worker_next_provision(principal, 10_016)?
        .ok_or("node provider action")?;
    assert_eq!(
        node_action.operation().operation_id(),
        node_prepared[0].operation_id
    );
    let node = runtime.provider_worker_confirm_provision(
        principal,
        node_action.operation().operation_id(),
        node_action.operation().revision(),
        "conversation-node-composed",
        10_017,
    )?;
    let node_profile = node.profile_id().ok_or("node profile")?;
    runtime.bind_profile_session(
        node.operation_id(),
        node_profile.into_uuid(),
        identities.node_target_session,
        principal,
        10_018,
    )?;
    Ok(FixtureRoutes {
        direct_operation: direct.operation_id(),
        direct_profile: direct_profile.into_uuid(),
        direct_runtime: direct_runtime.into_uuid(),
        node_operation: node.operation_id(),
        node_profile: node_profile.into_uuid(),
    })
}

fn seed_operations_catalog(
    root: &Path,
    runtime: &OrchestrationRuntime,
    principal: AuthenticatedPrincipal,
    workspace_id: Uuid,
    identities: &FixtureIdentities,
    routes: &FixtureRoutes,
) -> Result<(), Box<dyn Error>> {
    let sender_participant = Uuid::from_u128(0x607);
    let target_participant = Uuid::from_u128(0x608);
    let node_target_participant = Uuid::from_u128(0x611);
    let sender_profile = Uuid::from_u128(0x609);
    let sender_participation = Uuid::from_u128(0x60a);
    let target_participation = Uuid::from_u128(0x60b);
    let node_target_participation = Uuid::from_u128(0x612);
    let principal_id = match principal {
        AuthenticatedPrincipal::DirectToken(id) => id.into_uuid(),
        _ => return Err("fixture principal kind".into()),
    };
    let connection = Connection::open(root.join("agent-operations.sqlite3"))?;
    connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![Uuid::from_u128(0x60c).as_bytes(),workspace_id.as_bytes(),runtime.destination_id().as_bytes(),[0xA7_u8;32].as_slice()],
    )?;
    for (profile, participation) in [
        (sender_profile, sender_participation),
        (routes.direct_profile, target_participation),
        (routes.node_profile, node_target_participation),
    ] {
        connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,12,1)",
            params![workspace_id.as_bytes(),profile.as_bytes(),participation.as_bytes()],
        )?;
    }
    for (participant, session, profile, participation, activity, agent, mapping) in [
        (
            sender_participant,
            identities.sender_session,
            sender_profile,
            sender_participation,
            identities.sender_activity,
            identities.sender_agent,
            Uuid::from_u128(0x60d),
        ),
        (
            target_participant,
            identities.target_session,
            routes.direct_profile,
            target_participation,
            identities.target_activity,
            identities.target_agent,
            routes.direct_operation,
        ),
        (
            node_target_participant,
            identities.node_target_session,
            routes.node_profile,
            node_target_participation,
            identities.sender_activity,
            identities.node_target_agent,
            routes.node_operation,
        ),
    ] {
        connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,1,?4,'member','researcher',2,1,12)",
            params![workspace_id.as_bytes(),participant.as_bytes(),session.as_bytes(),principal_id.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?1,?5,1,?6,1,12)",
            params![mapping.as_bytes(),workspace_id.as_bytes(),participation.as_bytes(),profile.as_bytes(),session.as_bytes(),participant.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![workspace_id.as_bytes(),participant.as_bytes(),activity.as_bytes(),session.as_bytes(),agent.as_bytes(),mapping.as_bytes()],
        )?;
    }
    connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,1,NULL,NULL,20)",
        params![workspace_id.as_bytes(), sender_participant.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,?2,?2,1,'CONFIRMED',NULL,NULL,?3,1,20)",
        params![workspace_id.as_bytes(), [0_u8; 16].as_slice(), sender_participant.as_bytes()],
    )?;
    Ok(())
}

impl ComposedFixture {
    fn seed(
        broker: BrokerClusterConfig,
        scope: WorkspaceQueryScope,
    ) -> Result<Self, Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("composed-http-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let (token, bearer, principal) = token_authority(&root)?;
        let runtime = OrchestrationRuntime::open_with_broker_cluster(&root, broker)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?);
        let identities = seed_identities(&runtime, principal, scope.workspace_id)?;
        let routes = provision_routes(&runtime, principal, scope.workspace_id, &identities)?;
        seed_operations_catalog(
            &root,
            &runtime,
            principal,
            scope.workspace_id,
            &identities,
            &routes,
        )?;
        Ok(Self {
            root,
            runtime: Arc::new(runtime),
            bearer,
            token,
            principal,
            scope,
            sender_session: identities.sender_session,
            sender_activity: identities.sender_activity,
            sender_agent: identities.sender_agent,
            target_activity: identities.target_activity,
            target_agent: identities.target_agent,
            node_target_session: identities.node_target_session,
            node_target_agent: identities.node_target_agent,
            target_profile: routes.direct_profile,
            target_runtime: routes.direct_runtime,
            operation_id: routes.direct_operation,
        })
    }
}

fn fake_scope() -> Result<WorkspaceQueryScope, Box<dyn Error>> {
    let location = LocationId::from_uuid(Uuid::from_u128(0x501));
    let locator = WorkspaceLocator::new(location, LocationRevision::INITIAL, ".")?;
    Ok(WorkspaceQueryScope {
        workspace_id: Uuid::from_u128(0x500),
        location_id: location.into_uuid(),
        location_revision: 1,
        workspace_revision: 1,
        locator: locator.encode(),
    })
}

fn start_backend(
    runtime: Arc<OrchestrationRuntime>,
    token: &McpTokenRuntime,
    authority: CoordinatorAuthority,
) -> Result<BackendServer, Box<dyn Error>> {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = listener.local_addr()?;
    drop(listener);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(serve_with_runtime(
        runtime,
        address,
        token.client(),
        Some(authority),
        cancellation.clone(),
    ));
    Ok((address, cancellation, server))
}

fn offer_request(
    fixture: &ComposedFixture,
    source: &FakeForgeState,
) -> Result<NodeAssignmentAdmissionRequest, Box<dyn Error>> {
    let principal_id = match fixture.principal {
        AuthenticatedPrincipal::DirectToken(id) => id.into_uuid(),
        _ => return Err("fixture principal kind".into()),
    };
    offer_request_as(
        fixture,
        source,
        source.activity_id,
        source.coordinator_agent_id,
        source.coordinator_session_id,
        source.target_agent_id,
        source.target_session_id,
        None,
        None,
        principal_id,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "Builds one exact Source offer for a bounded admission-boundary case."
)]
fn offer_request_as(
    fixture: &ComposedFixture,
    source: &FakeForgeState,
    coordinator_activity_id: Uuid,
    coordinator_agent_id: Uuid,
    coordinator_session_id: Uuid,
    target_agent_id: Uuid,
    target_session_id: Uuid,
    team_id: Option<Uuid>,
    expected_binding_revision: Option<u64>,
    offer_principal_id: Uuid,
) -> Result<NodeAssignmentAdmissionRequest, Box<dyn Error>> {
    offer_request_with_principals(
        fixture,
        source,
        coordinator_activity_id,
        coordinator_agent_id,
        coordinator_session_id,
        target_agent_id,
        target_session_id,
        team_id,
        expected_binding_revision,
        offer_principal_id,
        offer_principal_id,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "Builds one exact Source offer for a bounded admission-boundary case."
)]
fn offer_request_with_principals(
    fixture: &ComposedFixture,
    source: &FakeForgeState,
    coordinator_activity_id: Uuid,
    coordinator_agent_id: Uuid,
    coordinator_session_id: Uuid,
    target_agent_id: Uuid,
    target_session_id: Uuid,
    team_id: Option<Uuid>,
    expected_binding_revision: Option<u64>,
    coordinator_principal_id: Uuid,
    target_principal_id: Uuid,
) -> Result<NodeAssignmentAdmissionRequest, Box<dyn Error>> {
    let payload = serde_json::to_vec(&json!({
        "scope": {
            "workspace_id": fixture.scope.workspace_id,
            "workspace_revision": 1,
            "locator": {
                "location_id": fixture.scope.location_id,
                "expected_revision": 1,
                "relative_root": "."
            }
        },
        "activity_id": coordinator_activity_id,
        "assignment_id": source.assignment_id,
        "observed_at_ms": 30,
        "action": {
            "kind": "offer",
            "coordinator": {
                "session_id": coordinator_session_id,
                "agent_id": coordinator_agent_id,
                "expected_revision": 1,
                "principal": {"kind": "direct_token", "token_id": coordinator_principal_id}
            },
            "target": {
                "agent_id": target_agent_id,
                "session_id": target_session_id,
                "expected_revision": 1,
                "principal": {"kind": "direct_token", "token_id": target_principal_id}
            },
            "goal_id": source.goal_id,
            "expected_goal_revision": 1,
            "expected_goal_created_at_ms": 20,
            "dag_revision": 1,
            "expected_head_revision": 1,
            "node_id": source.node_id,
            "expected_node_revision": 1,
            "credit_budget": null
        }
    }))?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    let digest = digest
        .iter()
        .fold(String::with_capacity(64), |mut result, byte| {
            use std::fmt::Write as _;
            let _ = write!(result, "{byte:02x}");
            result
        });
    Ok(NodeAssignmentAdmissionRequest {
        scope: fixture.scope.clone(),
        activity_id: coordinator_activity_id,
        coordinator_session_id,
        coordinator_agent_id,
        coordinator_revision: 1,
        assignment_id: source.assignment_id,
        source_fences: NodeAssignmentSourceFencesWire {
            goal_id: source.goal_id,
            goal_created_at_ms: 20,
            goal_revision: 1,
            dag_revision: 1,
            dag_base_goal_revision: 1,
            dag_head_revision: 1,
            node_id: source.node_id,
            node_revision: 1,
        },
        target: NodeAssignmentTargetWire {
            agent_id: target_agent_id,
            session_id: target_session_id,
            agent_revision: 1,
        },
        team_id,
        expected_binding_revision,
        source_offer_bytes_base64: URL_SAFE_NO_PAD.encode(payload),
        source_offer_sha256: digest,
    })
}

fn scope() -> WorkspaceQueryScope {
    WorkspaceQueryScope {
        workspace_id: Uuid::from_u128(1),
        location_id: Uuid::from_u128(2),
        location_revision: 1,
        workspace_revision: 1,
        locator: "hf1:test:1:Lg".to_owned(),
    }
}

fn query() -> WorkspaceChatQueryWire {
    WorkspaceChatQueryWire {
        workspace_id: Uuid::from_u128(1),
        agent_profile_id: Uuid::from_u128(5),
        runtime_session_id: None,
        before_sequence: None,
    }
}

fn fence() -> WorkspaceChatTargetFenceWire {
    WorkspaceChatTargetFenceWire {
        operation_id: Uuid::from_u128(6),
        profile_id: Uuid::from_u128(5),
        profile_name: "first-researcher".to_owned(),
        participation_id: Uuid::from_u128(7),
        participation_revision: 1,
        runtime_session_id: Uuid::from_u128(8),
        runtime_revision: 1,
        provider_conversation_id: "conversation-first".to_owned(),
        project: None,
    }
}

fn query_request() -> WorkspaceChatQueryRequest {
    WorkspaceChatQueryRequest {
        scope: scope(),
        session_id: Uuid::from_u128(3),
        sender_activity_id: Uuid::from_u128(4),
        query: query(),
    }
}

fn send_request() -> WorkspaceChatSendRequest {
    WorkspaceChatSendRequest {
        scope: scope(),
        session_id: Uuid::from_u128(3),
        sender_activity_id: Uuid::from_u128(4),
        request: WorkspaceChatMessageRequest {
            query: query(),
            message_id: Uuid::from_u128(9),
            text: "Research the official macro evidence.".to_owned(),
            target_fence: fence(),
        },
    }
}

#[test]
fn direct_wire_requires_null_query_keys_and_rejects_node_or_leadership_fields()
-> Result<(), Box<dyn Error>> {
    let request = query_request();
    let mut value = serde_json::to_value(&request)?;
    value
        .get_mut("query")
        .and_then(Value::as_object_mut)
        .ok_or("query object")?
        .remove("runtimeSessionId");
    assert!(serde_json::from_value::<WorkspaceChatQueryRequest>(value).is_err());
    let request = send_request();
    let mut value = serde_json::to_value(&request)?;
    value
        .get_mut("request")
        .and_then(|request| request.get_mut("targetFence"))
        .and_then(Value::as_object_mut)
        .ok_or("target fence object")?
        .insert("leadershipRevision".to_owned(), json!(1));
    assert!(serde_json::from_value::<WorkspaceChatSendRequest>(value).is_err());
    let mut value = serde_json::to_value(&request)?;
    value
        .get_mut("request")
        .and_then(|request| request.get_mut("query"))
        .and_then(Value::as_object_mut)
        .ok_or("query object")?
        .insert("nodeScope".to_owned(), Value::Null);
    assert!(serde_json::from_value::<WorkspaceChatSendRequest>(value).is_err());
    Ok(())
}

#[tokio::test]
async fn uncertain_direct_send_retains_exact_intent_while_recovery_stays_pure()
-> Result<(), Box<dyn Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    drop(listener);
    let client = OrchestrationHttpClient::new(address, Duration::from_millis(100))?;
    let authorization = HeaderValue::from_static("Bearer test-credential");
    let request = send_request();
    assert_eq!(
        client.workspace_chat_send(&authorization, &request).await,
        Err(ClientError::UnknownOutcome {
            readback: ReadbackTarget::WorkspaceChat(Box::new(request.clone()))
        })
    );
    assert_eq!(
        client
            .workspace_chat_recover(&authorization, &request)
            .await,
        Err(ClientError::Unavailable)
    );
    Ok(())
}

#[tokio::test]
async fn changed_profile_fence_is_rejected_before_transport() -> Result<(), Box<dyn Error>> {
    let client = OrchestrationHttpClient::new("127.0.0.1:9".parse()?, Duration::from_millis(100))?;
    let authorization = HeaderValue::from_static("Bearer test-credential");
    let mut request = send_request();
    request.request.target_fence.profile_id = Uuid::from_u128(10);
    assert_eq!(
        client.workspace_chat_send(&authorization, &request).await,
        Err(ClientError::InvalidRequest)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN for an isolated native three-node Broker cluster"]
#[allow(
    clippy::too_many_lines,
    reason = "One composed fixture proves the frozen HTTP ordering, provider path, restart recovery and detach negative without live product identities."
)]
async fn fake_forge_backend_routes_preserve_admit_chat_and_restart_contracts()
-> Result<(), Box<dyn Error>> {
    let cluster = broker_cluster::Cluster::start().await?;
    let broker = cluster.orchestration_config()?;
    let scope = fake_scope()?;
    let fixture = ComposedFixture::seed(broker, scope.clone())?;
    let configured_issuer =
        fixture
            .runtime
            .ensure_admission_issuer(ConfigureAdmissionIssuerCommand {
                issuer: OrchestrationIssuerId::from_uuid(Uuid::from_u128(0x716)),
                backend_id: ForgeBackendId::from_uuid(Uuid::from_u128(0x717)),
                admission_ttl_ms: 300_000,
                now_ms: 20,
            })?;
    assert_eq!(
        configured_issuer.issuer().into_uuid(),
        Uuid::from_u128(0x716)
    );
    assert_eq!(
        configured_issuer.backend_id().into_uuid(),
        Uuid::from_u128(0x717)
    );
    let principal_id = match fixture.principal {
        AuthenticatedPrincipal::DirectToken(id) => id.into_uuid(),
        _ => return Err("fixture principal kind".into()),
    };
    let fake_state = FakeForgeState {
        workspace_id: scope.workspace_id,
        activity_id: fixture.sender_activity,
        coordinator_agent_id: fixture.sender_agent,
        coordinator_session_id: fixture.sender_session,
        principal_id,
        goal_id: Uuid::from_u128(0x701),
        node_id: Uuid::from_u128(0x702),
        assignment_id: Uuid::from_u128(0x703),
        target_agent_id: fixture.node_target_agent,
        target_session_id: fixture.node_target_session,
        target_agent_revision: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        target_agent_context_reads: Arc::new(AtomicUsize::new(0)),
        target_agent_revision_on_read: Arc::new(AtomicUsize::new(0)),
        target_agent_revision_after_read: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        assignment_present: Arc::new(AtomicBool::new(true)),
        native_reservation: Arc::new(Mutex::new(None)),
        native_activation: Arc::new(Mutex::new(None)),
    };
    let fake = ExactFakeForge::start(fake_state.clone()).await?;
    assert_eq!(fake.state.workspace_id, scope.workspace_id);
    let (address, cancellation, server) = start_backend(
        Arc::clone(&fixture.runtime),
        &fixture.token,
        fake.authority()?,
    )?;
    let client = OrchestrationHttpClient::new(address, DEADLINE)?;
    let node_scope = NodeTeamScopeWire {
        activity_id: fixture.sender_activity,
        goal_id: fake_state.goal_id,
        goal_created_at_ms: 20,
        dag_revision: 1,
        dag_base_goal_revision: 1,
        node_id: fake_state.node_id,
    };
    let node_query = NodeChatQueryRequest {
        scope: scope.clone(),
        session_id: fixture.sender_session,
        sender_activity_id: fixture.sender_activity,
        query: ChatQueryWire {
            workspace_id: scope.workspace_id,
            agent_profile_id: None,
            runtime_session_id: None,
            node_scope: Some(node_scope),
            before_sequence: None,
        },
    };
    let direct_query = WorkspaceChatQueryRequest {
        scope: scope.clone(),
        session_id: fixture.sender_session,
        sender_activity_id: fixture.sender_activity,
        query: WorkspaceChatQueryWire {
            workspace_id: scope.workspace_id,
            agent_profile_id: fixture.target_profile,
            runtime_session_id: Some(fixture.target_runtime),
            before_sequence: None,
        },
    };
    let first_direct = tokio::time::timeout(DEADLINE, async {
        loop {
            match client
                .workspace_chat_read(&fixture.bearer, &direct_query)
                .await
            {
                Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                result => return result,
            }
        }
    })
    .await?
    .map_err(|error| format!("initial direct query failed: {error:?}"))?;
    assert_eq!(
        first_direct
            .snapshot
            .target
            .as_ref()
            .ok_or("direct target")?
            .operation_id,
        fixture.operation_id
    );
    assert_ne!(fixture.sender_activity, fixture.target_activity);

    // Create a Session through the authenticated HTTP surface, then establish
    // its native W+S identity through the ordinary Broker-backed journal.
    let native_open = client
        .open_session(
            &fixture.bearer,
            &AgentSessionOpenRequest {
                request_id: Uuid::from_u128(0x714).to_string(),
            },
        )
        .await?;
    assert!(native_open.created);
    let native_session = native_open.session.session_id;
    let native_activity = Uuid::from_u128(0x715);
    let native_target = MemberTarget {
        scope: scope.clone(),
        activity_id: native_activity,
        session_id: native_session,
    };
    let native_registration_request = MemberRegisterRequest {
        target: native_target.clone(),
        display_name: "Codex Main".to_owned(),
        declared_role: "coordinator".to_owned(),
        transport: MemberTransportWire::StreamableHttp,
    };
    let native_prepared = client
        .register_member(&fixture.bearer, &native_registration_request)
        .await?;
    assert_eq!(native_prepared.progress.phase, MemberPhaseWire::Reserved);
    let native_participant = native_session_participation_id(scope.workspace_id, native_session);
    assert_eq!(native_prepared.progress.participant_id, native_participant);
    let native_registration_replay = client
        .register_member(&fixture.bearer, &native_registration_request)
        .await?;
    assert_eq!(
        native_registration_replay.progress,
        native_prepared.progress
    );
    let native_request_id = native_prepared.progress.request_id;
    let native_commit = client
        .confirm_member(
            &fixture.bearer,
            &MemberConfirmRequest {
                target: native_target.clone(),
                request_id: native_request_id,
                recipients: Vec::new(),
            },
        )
        .await?;
    assert_eq!(native_commit.commit.participant_id, native_participant);
    let native_activation = client
        .activate_member(
            &fixture.bearer,
            &MemberActivateRequest {
                target: native_target.clone(),
                request_id: native_request_id,
            },
        )
        .await?;
    assert_eq!(native_activation.activation.phase, MemberPhaseWire::Ready);
    assert_eq!(
        native_activation.activation.participant_id,
        native_participant
    );
    let native_registration = client
        .member_registration(
            &fixture.bearer,
            &MemberRegistrationQueryRequest {
                target: native_target.clone(),
                request_id: native_request_id,
            },
        )
        .await?;
    assert_eq!(
        native_registration
            .progress
            .as_ref()
            .ok_or("native registration progress")?
            .phase,
        MemberPhaseWire::Ready
    );
    assert_eq!(
        native_registration.commit,
        Some(native_commit.commit.clone())
    );
    assert_eq!(
        native_registration.activation,
        Some(native_activation.activation.clone())
    );
    let native_agent = native_activation.activation.agent_id;
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    let schema_version: i64 =
        registrations.pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(schema_version, 48);
    let registration_journal_count: i64 = registrations.query_row(
        "SELECT COUNT(*) FROM ops_registration_requests WHERE request_id=?1",
        params![native_request_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(registration_journal_count, 1);
    let profile_scope_count: i64 = registrations.query_row(
        "SELECT COUNT(*) FROM ops_registration_profile_scopes WHERE request_id=?1",
        params![native_request_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(profile_scope_count, 0);
    drop(registrations);

    let alternate_token = fixture.token.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("alternate requester principal".to_owned())?,
        forge_domain::TimestampMillis::new(40)?,
    ))?;
    let alternate_principal_id = alternate_token.metadata().id().into_uuid();
    let alternate_principal = AuthenticatedPrincipal::DirectToken(
        forge_domain::DirectTokenId::from_uuid(alternate_principal_id),
    );
    let mut alternate_bearer =
        HeaderValue::from_str(&format!("Bearer {}", alternate_token.reveal().as_str()))?;
    alternate_bearer.set_sensitive(true);

    let distinct_target_session = fixture
        .runtime
        .open_session(Uuid::from_u128(0x736), alternate_principal, 41)?
        .session
        .session_id;
    let target_participant = Uuid::from_u128(0x737);
    let target_profile = Uuid::from_u128(0x738);
    let target_participation = Uuid::from_u128(0x739);
    let target_mapping_request = Uuid::from_u128(0x73A);
    let distinct_target_base_agent = member_agent_id(
        scope.workspace_id,
        fixture.sender_activity,
        distinct_target_session,
    )
    .into_uuid();
    let native_target_agent =
        member_agent_id(scope.workspace_id, native_activity, distinct_target_session).into_uuid();
    let target_bindings = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    target_bindings.execute(
        "INSERT INTO ops_workspace_participations(
            workspace_id,agent_profile_id,participation_id,created_at_ms,revision
         ) VALUES(?1,?2,?3,41,1)",
        params![
            scope.workspace_id.as_bytes(),
            target_profile.as_bytes(),
            target_participation.as_bytes(),
        ],
    )?;
    target_bindings.execute(
        "INSERT INTO ops_logical_members(
            workspace_id,participant_id,session_id,principal_kind,principal_id,
            display_name,declared_role,transport_kind,membership_revision,created_at_ms
         ) VALUES(?1,?2,?3,1,?4,'Distinct node peer','worker',2,1,41)",
        params![
            scope.workspace_id.as_bytes(),
            target_participant.as_bytes(),
            distinct_target_session.as_bytes(),
            alternate_principal_id.as_bytes(),
        ],
    )?;
    target_bindings.execute(
        "INSERT INTO ops_workspace_participation_mappings(
            request_id,workspace_id,participation_id,agent_profile_id,profile_revision,
            profile_mapping_request_id,session_id,session_revision,participant_id,
            participant_membership_revision,mapped_at_ms
         ) VALUES(?1,?2,?3,?4,1,?1,?5,1,?6,1,41)",
        params![
            target_mapping_request.as_bytes(),
            scope.workspace_id.as_bytes(),
            target_participation.as_bytes(),
            target_profile.as_bytes(),
            distinct_target_session.as_bytes(),
            target_participant.as_bytes(),
        ],
    )?;
    target_bindings.execute(
        "INSERT INTO ops_current_effect_bindings(
            workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id
         ) VALUES(?1,?2,?3,?4,?5,0,?6)",
        params![
            scope.workspace_id.as_bytes(),
            target_participant.as_bytes(),
            fixture.sender_activity.as_bytes(),
            distinct_target_session.as_bytes(),
            distinct_target_base_agent.as_bytes(),
            target_mapping_request.as_bytes(),
        ],
    )?;
    target_bindings.execute(
        "INSERT INTO ops_current_effect_bindings(
            workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id
         ) VALUES(?1,?2,?3,?4,?5,0,?6)",
        params![
            scope.workspace_id.as_bytes(),
            target_participant.as_bytes(),
            native_activity.as_bytes(),
            distinct_target_session.as_bytes(),
            native_target_agent.as_bytes(),
            target_mapping_request.as_bytes(),
        ],
    )?;
    drop(target_bindings);

    let native_source = FakeForgeState {
        workspace_id: scope.workspace_id,
        activity_id: native_activity,
        coordinator_agent_id: native_agent,
        coordinator_session_id: native_session,
        principal_id,
        goal_id: Uuid::from_u128(0x72C),
        node_id: Uuid::from_u128(0x72D),
        assignment_id: Uuid::from_u128(0x72E),
        target_agent_id: native_target_agent,
        target_session_id: distinct_target_session,
        target_agent_revision: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        target_agent_context_reads: Arc::new(AtomicUsize::new(0)),
        target_agent_revision_on_read: Arc::new(AtomicUsize::new(0)),
        target_agent_revision_after_read: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        assignment_present: Arc::new(AtomicBool::new(false)),
        native_reservation: Arc::new(Mutex::new(None)),
        native_activation: Arc::new(Mutex::new(None)),
    };
    let native_fake = ExactFakeForge::start(native_source.clone()).await?;
    let (native_address, native_cancellation, native_server) = start_backend(
        Arc::clone(&fixture.runtime),
        &fixture.token,
        native_fake.authority()?,
    )?;
    let native_client = OrchestrationHttpClient::new(native_address, DEADLINE)?;
    let stale_source_target_revision = offer_request_as(
        &fixture,
        &native_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        None,
        None,
        principal_id,
    )?;
    let recovery_before_admission = tokio::time::timeout(DEADLINE, async {
        loop {
            match native_client
                .recover_node_assignment_admission(&fixture.bearer, &stale_source_target_revision)
                .await
            {
                Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                result => return result,
            }
        }
    })
    .await??;
    assert!(recovery_before_admission.admission.is_none());
    native_source
        .target_agent_context_reads
        .store(0, Ordering::SeqCst);
    native_source
        .target_agent_revision_on_read
        .store(2, Ordering::SeqCst);
    native_source
        .target_agent_revision_after_read
        .store(2, Ordering::SeqCst);
    let stale_response = raw_admission_response(
        native_address,
        &fixture.bearer,
        &stale_source_target_revision,
    )
    .await?;
    assert_eq!(
        stale_response,
        (409, Some("ORCHESTRATION_CONFLICT".to_owned()))
    );
    assert_eq!(
        Connection::open(fixture.root.join("agent-operations.sqlite3"))?.query_row(
            "SELECT COUNT(*) FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![scope.workspace_id.as_bytes(), stale_source_target_revision.assignment_id.as_bytes()],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    assert_eq!(
        native_source
            .target_agent_context_reads
            .load(Ordering::SeqCst),
        2
    );
    native_source
        .target_agent_revision_on_read
        .store(0, Ordering::SeqCst);
    native_source
        .target_agent_revision_after_read
        .store(0, Ordering::SeqCst);
    native_source
        .target_agent_context_reads
        .store(0, Ordering::SeqCst);
    let stale_native_binding_request = offer_request_as(
        &fixture,
        &native_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        None,
        None,
        principal_id,
    )?;
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    assert_eq!(
        registrations.execute(
            "UPDATE ops_current_effect_bindings SET activity_id=?1
             WHERE workspace_id=?2 AND activity_id=?3 AND session_id=?4 AND agent_id=?5",
            params![
                Uuid::from_u128(0x72F).as_bytes(),
                scope.workspace_id.as_bytes(),
                native_activity.as_bytes(),
                native_session.as_bytes(),
                native_agent.as_bytes(),
            ],
        )?,
        1
    );
    drop(registrations);
    assert!(
        native_client
            .admit_node_assignment(&fixture.bearer, &stale_native_binding_request)
            .await
            .is_err()
    );
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    assert_eq!(
        registrations.execute(
            "UPDATE ops_current_effect_bindings SET activity_id=?1
             WHERE workspace_id=?2 AND activity_id=?3 AND session_id=?4 AND agent_id=?5",
            params![
                native_activity.as_bytes(),
                scope.workspace_id.as_bytes(),
                Uuid::from_u128(0x72F).as_bytes(),
                native_session.as_bytes(),
                native_agent.as_bytes(),
            ],
        )?,
        1
    );
    drop(registrations);
    let native_node_scope = NodeTeamBindingScope::new(
        WorkspaceId::from_uuid(scope.workspace_id),
        forge_domain::ActivityId::from_uuid(native_activity),
        forge_domain::GoalId::from_uuid(native_source.goal_id),
        forge_domain::TimestampMillis::new(20)?,
        forge_domain::DagRevisionNumber::new(1)?,
        forge_domain::GoalRevision::new(1)?,
        forge_domain::DagNodeId::from_uuid(native_source.node_id),
    )?;
    let native_source_observation = NodeTeamSourceObservation {
        scope: native_node_scope,
        current_dag_revision: forge_domain::DagRevisionNumber::new(1)?,
        assignment_present: false,
    };
    fixture.runtime.create_team(
        TeamCreateSpec {
            request_id: Uuid::from_u128(0x735),
            workspace_id: scope.workspace_id,
            team_id: Uuid::from_u128(0xA004),
            activity_id: fixture.sender_activity,
            session_id: fixture.sender_session,
            expected_sender_leadership_revision: Some(1),
            name: "Native admission Team guard".to_owned(),
            purpose: "Exercise the Team exclusion for native requesters".to_owned(),
            initial_members: vec![
                TeamRosterMemberSpec {
                    activity_id: fixture.sender_activity,
                    session_id: fixture.sender_session,
                    role: "coordinator".to_owned(),
                },
                TeamRosterMemberSpec {
                    activity_id: fixture.sender_activity,
                    session_id: distinct_target_session,
                    role: "worker".to_owned(),
                },
            ],
            observed_at_ms: 30,
        },
        fixture.principal,
    )?;
    let native_team_binding = fixture.runtime.set_node_team_operator(
        fixture.principal,
        NodeTeamSet {
            scope: native_node_scope,
            expected_revision: None,
            team_id: Some(Uuid::from_u128(0xA004)),
            expected_sender_leadership_revision: None,
            observed_at: forge_domain::TimestampMillis::new(31)?,
        },
        native_source_observation,
    )?;
    assert_eq!(native_team_binding.revision(), 1);

    let native_team_request = offer_request_with_principals(
        &fixture,
        &native_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        Some(Uuid::from_u128(0xA004)),
        Some(1),
        principal_id,
        alternate_principal_id,
    )?;
    assert!(
        native_client
            .admit_node_assignment(&fixture.bearer, &native_team_request)
            .await
            .is_err()
    );
    let native_stale_binding_request = NodeAssignmentAdmissionRequest {
        expected_binding_revision: Some(2),
        ..native_team_request.clone()
    };
    assert!(
        native_client
            .admit_node_assignment(&fixture.bearer, &native_stale_binding_request)
            .await
            .is_err()
    );
    let native_foreign_principal_request = offer_request_as(
        &fixture,
        &native_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        None,
        Some(1),
        alternate_principal_id,
    )?;
    assert!(
        native_client
            .admit_node_assignment(&fixture.bearer, &native_foreign_principal_request)
            .await
            .is_err()
    );

    let native_unassigned_binding = fixture.runtime.set_node_team_operator(
        fixture.principal,
        NodeTeamSet {
            scope: native_node_scope,
            expected_revision: Some(1),
            team_id: None,
            expected_sender_leadership_revision: None,
            observed_at: forge_domain::TimestampMillis::new(32)?,
        },
        native_source_observation,
    )?;
    assert_eq!(native_unassigned_binding.revision(), 2);
    let native_nonready_target_request = offer_request_as(
        &fixture,
        &native_source,
        native_activity,
        native_agent,
        native_session,
        Uuid::from_u128(0x72F),
        Uuid::from_u128(0x730),
        None,
        Some(2),
        principal_id,
    )?;
    assert!(
        native_client
            .admit_node_assignment(&fixture.bearer, &native_nonready_target_request)
            .await
            .is_err()
    );

    let coordinator_principal_as_peer_target = offer_request_as(
        &fixture,
        &native_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        None,
        Some(2),
        principal_id,
    )?;
    assert!(
        native_client
            .admit_node_assignment(&fixture.bearer, &coordinator_principal_as_peer_target)
            .await
            .is_err()
    );
    assert_eq!(
        Connection::open(fixture.root.join("agent-operations.sqlite3"))?.query_row(
            "SELECT COUNT(*) FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![scope.workspace_id.as_bytes(), coordinator_principal_as_peer_target.assignment_id.as_bytes()],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );

    let target_principal_request = offer_request_with_principals(
        &fixture,
        &native_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        None,
        Some(2),
        principal_id,
        alternate_principal_id,
    )?;
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    assert_eq!(
        registrations.execute(
            "UPDATE ops_logical_members SET principal_id=?1
             WHERE workspace_id=?2 AND participant_id=?3",
            params![
                principal_id.as_bytes(),
                scope.workspace_id.as_bytes(),
                target_participant.as_bytes(),
            ],
        )?,
        1
    );
    drop(registrations);
    assert!(
        native_client
            .admit_node_assignment(&fixture.bearer, &target_principal_request)
            .await
            .is_err()
    );
    assert_eq!(
        Connection::open(fixture.root.join("agent-operations.sqlite3"))?.query_row(
            "SELECT COUNT(*) FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![scope.workspace_id.as_bytes(), target_principal_request.assignment_id.as_bytes()],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    assert_eq!(
        registrations.execute(
            "UPDATE ops_logical_members SET principal_id=?1
             WHERE workspace_id=?2 AND participant_id=?3",
            params![
                alternate_principal_id.as_bytes(),
                scope.workspace_id.as_bytes(),
                target_participant.as_bytes(),
            ],
        )?,
        1
    );
    drop(registrations);

    let native_request = offer_request_with_principals(
        &fixture,
        &native_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        None,
        Some(2),
        principal_id,
        alternate_principal_id,
    )?;
    let native_first = native_client
        .admit_node_assignment(&fixture.bearer, &native_request)
        .await?;
    assert!(native_first.created);
    assert!(
        !native_client
            .admit_node_assignment(&fixture.bearer, &native_request)
            .await?
            .created
    );
    let native_receipt_requester = serde_json::to_value(&native_first.admission.requester)?;
    assert_eq!(
        native_receipt_requester["activityId"],
        json!(native_activity)
    );
    assert_eq!(native_receipt_requester["agentId"], json!(native_agent));
    assert_eq!(native_receipt_requester["sessionId"], json!(native_session));
    assert_eq!(native_receipt_requester["participationId"], Value::Null);
    assert_eq!(native_receipt_requester["profileId"], Value::Null);
    assert_eq!(
        native_receipt_requester["participantId"],
        json!(native_participant)
    );
    assert!(!native_first.admission.target_participation_id.is_nil());
    assert!(!native_first.admission.target_participant_id.is_nil());

    let stored_native_requester = Connection::open(fixture.root.join("agent-operations.sqlite3"))?
        .query_row(
            "SELECT substr(requester_activity_id,1,17),substr(requester_agent_id,1,17),
                substr(requester_session_id,1,17),substr(requester_participation_id,1,17),
                substr(requester_profile_id,1,17),substr(requester_participant_id,1,17),
                requester_key,requester_display_name,original_offer_payload
         FROM ops_assignment_team_affinities WHERE assignment_id=?1",
            params![native_request.assignment_id.as_bytes()],
            |row| {
                Ok(StoredNativeRequester {
                    activity: row.get(0)?,
                    agent: row.get(1)?,
                    session: row.get(2)?,
                    participation: row.get(3)?,
                    profile: row.get(4)?,
                    participant: row.get(5)?,
                    key: row.get(6)?,
                    display_name: row.get(7)?,
                    offer_payload: row.get(8)?,
                })
            },
        )?;
    assert_eq!(stored_native_requester.activity, native_activity.as_bytes());
    assert_eq!(stored_native_requester.agent, native_agent.as_bytes());
    assert_eq!(stored_native_requester.session, native_session.as_bytes());
    assert_eq!(stored_native_requester.participation, None);
    assert_eq!(stored_native_requester.profile, None);
    assert_eq!(
        stored_native_requester.participant,
        Some(native_participant.as_bytes().to_vec())
    );
    assert_eq!(
        stored_native_requester.key,
        format!("native-participant:{native_participant}")
    );
    assert_eq!(stored_native_requester.display_name, "Codex Main");
    assert_eq!(
        stored_native_requester.offer_payload,
        URL_SAFE_NO_PAD.decode(&native_request.source_offer_bytes_base64)?
    );

    native_source
        .assignment_present
        .store(true, Ordering::SeqCst);
    assert!(
        native_client
            .admit_node_assignment(&fixture.bearer, &native_request)
            .await
            .is_err()
    );
    assert_eq!(
        native_client
            .recover_node_assignment_admission(&fixture.bearer, &native_request)
            .await?
            .admission,
        Some(native_first.admission.clone())
    );
    let changed_agent = NodeAssignmentAdmissionRequest {
        coordinator_agent_id: Uuid::from_u128(0x731),
        ..native_request.clone()
    };
    assert!(
        native_client
            .recover_node_assignment_admission(&fixture.bearer, &changed_agent)
            .await
            .is_err()
    );
    let changed_session = NodeAssignmentAdmissionRequest {
        coordinator_session_id: Uuid::from_u128(0x732),
        ..native_request.clone()
    };
    assert!(
        native_client
            .recover_node_assignment_admission(&fixture.bearer, &changed_session)
            .await
            .is_err()
    );
    assert!(
        native_client
            .recover_node_assignment_admission(&alternate_bearer, &native_request)
            .await
            .is_err()
    );

    let native_request_registration_id = native_request_id;
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    registrations.execute(
        "INSERT INTO ops_registration_profile_scopes(
            request_id,workspace_id,profile_id,link_request_id,profile_revision,
            session_id,session_revision
         ) VALUES(?1,?2,?3,?4,1,?5,1)",
        params![
            native_request_registration_id.as_bytes(),
            scope.workspace_id.as_bytes(),
            fixture.target_profile.as_bytes(),
            Uuid::from_u128(0x733).as_bytes(),
            native_session.as_bytes(),
        ],
    )?;
    drop(registrations);
    assert!(
        native_client
            .recover_node_assignment_admission(&fixture.bearer, &native_request)
            .await
            .is_err()
    );
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    registrations.execute(
        "DELETE FROM ops_registration_profile_scopes WHERE request_id=?1",
        params![native_request_registration_id.as_bytes()],
    )?;
    registrations.execute(
        "UPDATE ops_current_effect_bindings SET activity_id=?1
         WHERE workspace_id=?2 AND activity_id=?3 AND session_id=?4 AND agent_id=?5",
        params![
            Uuid::from_u128(0x734).as_bytes(),
            scope.workspace_id.as_bytes(),
            native_activity.as_bytes(),
            native_session.as_bytes(),
            native_agent.as_bytes(),
        ],
    )?;
    drop(registrations);
    assert!(
        native_client
            .recover_node_assignment_admission(&fixture.bearer, &native_request)
            .await
            .is_err()
    );
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    registrations.execute(
        "UPDATE ops_current_effect_bindings SET activity_id=?1
         WHERE workspace_id=?2 AND activity_id=?3 AND session_id=?4 AND agent_id=?5",
        params![
            native_activity.as_bytes(),
            scope.workspace_id.as_bytes(),
            Uuid::from_u128(0x734).as_bytes(),
            native_session.as_bytes(),
            native_agent.as_bytes(),
        ],
    )?;
    drop(registrations);
    assert_eq!(
        native_client
            .recover_node_assignment_admission(&fixture.bearer, &native_request)
            .await?
            .admission,
        Some(native_first.admission.clone())
    );

    let unassigned_source = FakeForgeState {
        workspace_id: scope.workspace_id,
        activity_id: native_activity,
        coordinator_agent_id: native_agent,
        coordinator_session_id: native_session,
        principal_id,
        goal_id: Uuid::from_u128(0x73E),
        node_id: Uuid::from_u128(0x73F),
        assignment_id: Uuid::from_u128(0x740),
        target_agent_id: native_target_agent,
        target_session_id: distinct_target_session,
        target_agent_revision: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        target_agent_context_reads: Arc::new(AtomicUsize::new(0)),
        target_agent_revision_on_read: Arc::new(AtomicUsize::new(0)),
        target_agent_revision_after_read: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        assignment_present: Arc::new(AtomicBool::new(false)),
        native_reservation: Arc::new(Mutex::new(None)),
        native_activation: Arc::new(Mutex::new(None)),
    };
    let unassigned_fake = ExactFakeForge::start(unassigned_source.clone()).await?;
    let (unassigned_address, unassigned_cancellation, unassigned_server) = start_backend(
        Arc::clone(&fixture.runtime),
        &fixture.token,
        unassigned_fake.authority()?,
    )?;
    let unassigned_client = OrchestrationHttpClient::new(unassigned_address, DEADLINE)?;
    let unassigned_node_scope = NodeTeamBindingScope::new(
        WorkspaceId::from_uuid(scope.workspace_id),
        forge_domain::ActivityId::from_uuid(native_activity),
        forge_domain::GoalId::from_uuid(unassigned_source.goal_id),
        forge_domain::TimestampMillis::new(20)?,
        forge_domain::DagRevisionNumber::new(1)?,
        forge_domain::GoalRevision::new(1)?,
        forge_domain::DagNodeId::from_uuid(unassigned_source.node_id),
    )?;
    let unassigned_source_observation = NodeTeamSourceObservation {
        scope: unassigned_node_scope,
        current_dag_revision: forge_domain::DagRevisionNumber::new(1)?,
        assignment_present: false,
    };
    let unassigned_request = offer_request_with_principals(
        &fixture,
        &unassigned_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        None,
        None,
        principal_id,
        alternate_principal_id,
    )?;
    let before_unassigned_admission = tokio::time::timeout(DEADLINE, async {
        loop {
            match unassigned_client
                .recover_node_assignment_admission(&fixture.bearer, &unassigned_request)
                .await
            {
                Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                result => return result,
            }
        }
    })
    .await??;
    assert!(before_unassigned_admission.admission.is_none());

    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    let unknown_assignment = Uuid::from_u128(0x741);
    registrations.execute(
        "INSERT INTO ops_assignment_team_affinities(
            workspace_id,assignment_id,activity_id,target_agent_id,target_session_id,
            target_participation_id,target_participant_id,requester_key,requester_display_name,
            assignment_payload_fingerprint,observed_at_ms,disposition
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,'operator','Local operator',?8,1,'LEGACY_UNKNOWN')",
        params![
            scope.workspace_id.as_bytes(),
            unknown_assignment.as_bytes(),
            native_activity.as_bytes(),
            native_target_agent.as_bytes(),
            distinct_target_session.as_bytes(),
            target_participation.as_bytes(),
            target_participant.as_bytes(),
            [0x74_u8; 32].as_slice(),
        ],
    )?;
    drop(registrations);
    assert!(
        unassigned_client
            .admit_node_assignment(&fixture.bearer, &unassigned_request)
            .await
            .is_err()
    );
    assert_eq!(
        Connection::open(fixture.root.join("agent-operations.sqlite3"))?.query_row(
            "SELECT COUNT(*) FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![scope.workspace_id.as_bytes(), unassigned_request.assignment_id.as_bytes()],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    assert_eq!(
        registrations.execute(
            "DELETE FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![scope.workspace_id.as_bytes(), unknown_assignment.as_bytes()],
        )?,
        1
    );
    registrations.execute(
        "INSERT INTO ops_dag_node_team_bindings(
            workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,
            dag_base_goal_revision,node_id,team_id,bound_by,bound_at_ms,revision
         ) VALUES(?1,NULL,?2,NULL,?3,NULL,?4,NULL,'operator',1,1)",
        params![
            scope.workspace_id.as_bytes(),
            unassigned_source.goal_id.as_bytes(),
            1,
            unassigned_source.node_id.as_bytes(),
        ],
    )?;
    drop(registrations);
    assert!(
        unassigned_client
            .admit_node_assignment(&fixture.bearer, &unassigned_request)
            .await
            .is_err()
    );
    assert_eq!(
        Connection::open(fixture.root.join("agent-operations.sqlite3"))?.query_row(
            "SELECT COUNT(*) FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![scope.workspace_id.as_bytes(), unassigned_request.assignment_id.as_bytes()],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    let registrations = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    assert_eq!(
        registrations.execute(
            "DELETE FROM ops_dag_node_team_bindings
             WHERE workspace_id=?1 AND activity_id IS NULL AND goal_id=?2
                AND dag_revision=1 AND node_id=?3 AND legacy_retirement_request_id IS NULL",
            params![
                scope.workspace_id.as_bytes(),
                unassigned_source.goal_id.as_bytes(),
                unassigned_source.node_id.as_bytes(),
            ],
        )?,
        1
    );
    drop(registrations);

    let unassigned_first = unassigned_client
        .admit_node_assignment(&fixture.bearer, &unassigned_request)
        .await?;
    assert!(unassigned_first.created);
    assert_eq!(unassigned_first.admission.binding_revision, None);
    assert_eq!(unassigned_first.admission.team_id, None);
    assert_eq!(unassigned_first.admission.requester.profile_id, None);
    assert_eq!(unassigned_first.admission.requester.participation_id, None);
    assert!(
        !unassigned_client
            .admit_node_assignment(&fixture.bearer, &unassigned_request)
            .await?
            .created
    );
    assert_eq!(
        unassigned_client
            .recover_node_assignment_admission(&fixture.bearer, &unassigned_request)
            .await?
            .admission,
        Some(unassigned_first.admission.clone())
    );

    let second_assignment_source = FakeForgeState {
        assignment_id: Uuid::from_u128(0x742),
        ..unassigned_source.clone()
    };
    let second_unassigned_request = offer_request_with_principals(
        &fixture,
        &second_assignment_source,
        native_activity,
        native_agent,
        native_session,
        native_target_agent,
        distinct_target_session,
        None,
        None,
        principal_id,
        alternate_principal_id,
    )?;
    assert!(matches!(
        unassigned_client
            .admit_node_assignment(&fixture.bearer, &second_unassigned_request)
            .await,
        Err(ClientError::Rejected {
            status: 409,
            code: heterarchy_orchestration_backend::client::ServiceCode::Conflict
        })
    ));
    assert_eq!(
        Connection::open(fixture.root.join("agent-operations.sqlite3"))?.query_row(
            "SELECT COUNT(*) FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![scope.workspace_id.as_bytes(), second_unassigned_request.assignment_id.as_bytes()],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    let stored_unassigned_scope = Connection::open(fixture.root.join("agent-operations.sqlite3"))?
        .query_row(
            "SELECT activity_id,node_goal_id,goal_created_at_ms,node_dag_revision,node_dag_base_goal_revision,node_id,disposition FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![scope.workspace_id.as_bytes(), unassigned_request.assignment_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )?;
    assert_eq!(
        stored_unassigned_scope,
        (
            unassigned_node_scope
                .activity_id()
                .into_uuid()
                .as_bytes()
                .to_vec(),
            unassigned_node_scope
                .goal_id()
                .into_uuid()
                .as_bytes()
                .to_vec(),
            unassigned_node_scope.goal_created_at().get(),
            unassigned_node_scope.dag_revision().get(),
            unassigned_node_scope.dag_base_goal_revision().get(),
            unassigned_node_scope
                .node_id()
                .into_uuid()
                .as_bytes()
                .to_vec(),
            "PENDING".to_owned(),
        )
    );
    let binding_rebind = fixture.runtime.set_node_team_operator(
        fixture.principal,
        NodeTeamSet {
            scope: unassigned_node_scope,
            expected_revision: None,
            team_id: None,
            expected_sender_leadership_revision: None,
            observed_at: forge_domain::TimestampMillis::new(33)?,
        },
        unassigned_source_observation,
    );
    assert!(
        matches!(
            &binding_rebind,
            Err(heterarchy_orchestration_runtime::RuntimeError::Persistence(source))
                if matches!(
                    source.as_ref(),
                    heterarchy_orchestration_operations::OperationsError::Conflict(
                        "node binding has a pending or active assignment"
                    )
                )
        ),
        "unexpected node binding rebind result: {binding_rebind:?}"
    );
    assert!(
        fixture
            .runtime
            .read_node_team_operator(fixture.principal, unassigned_node_scope, None,)?
            .binding
            .is_none()
    );

    let missing = client.node_chat_read(&fixture.bearer, &node_query).await?;
    assert!(missing.snapshot.target.is_none());
    assert_eq!(
        missing.snapshot.unavailable_reason.as_deref(),
        Some(
            "This node assignment has not been admitted by Orchestration. History remains available."
        )
    );

    fake_state.assignment_present.store(false, Ordering::SeqCst);
    let admission_request = offer_request(&fixture, &fake_state)?;
    let first = client
        .admit_node_assignment(&fixture.bearer, &admission_request)
        .await?;
    assert!(first.created);
    let profile_requester = serde_json::to_value(&first.admission.requester)?;
    assert_eq!(
        profile_requester["profileId"],
        json!(Uuid::from_u128(0x609))
    );
    assert_eq!(
        profile_requester["participationId"],
        json!(Uuid::from_u128(0x60A))
    );
    let replay = client
        .admit_node_assignment(&fixture.bearer, &admission_request)
        .await?;
    assert!(!replay.created);
    fake_state.assignment_present.store(true, Ordering::SeqCst);
    assert!(matches!(
        client
            .admit_node_assignment(&fixture.bearer, &admission_request)
            .await,
        Err(ClientError::Rejected {
            status: 409,
            code: heterarchy_orchestration_backend::client::ServiceCode::Conflict
        })
    ));
    let recovered = client
        .recover_node_assignment_admission(&fixture.bearer, &admission_request)
        .await?;
    assert_eq!(recovered.admission, Some(first.admission.clone()));
    assert!(!recovered.retry_authorized);
    let admitted = client.node_chat_read(&fixture.bearer, &node_query).await?;
    let node_target = admitted.snapshot.target.ok_or("admitted node target")?;
    let node_send = NodeChatSendRequest {
        scope: scope.clone(),
        session_id: fixture.sender_session,
        sender_activity_id: fixture.sender_activity,
        sender_leadership_revision: 1,
        request: ChatMessageRequest {
            query: node_query.query.clone(),
            message_id: Uuid::from_u128(0x790),
            text: "node research request".to_owned(),
            target_fence: node_target,
        },
    };
    let node_receipt = client.node_chat_send(&fixture.bearer, &node_send).await?;
    assert_eq!(
        node_receipt.receipt.message_id,
        node_send.request.message_id
    );
    let direct_target = first_direct.snapshot.target.ok_or("direct target")?;
    let direct_send = WorkspaceChatSendRequest {
        scope: scope.clone(),
        session_id: fixture.sender_session,
        sender_activity_id: fixture.sender_activity,
        request: WorkspaceChatMessageRequest {
            query: direct_query.query.clone(),
            message_id: Uuid::from_u128(0x780),
            text: "direct research request".to_owned(),
            target_fence: direct_target,
        },
    };
    let direct_receipt = client
        .workspace_chat_send(&fixture.bearer, &direct_send)
        .await?;
    assert_eq!(
        direct_receipt.receipt.message_id,
        direct_send.request.message_id
    );
    let provider_now = wall_now_ms()?;
    let node_dispatch = fixture
        .runtime
        .provider_worker_next_message(fixture.principal, provider_now)?
        .ok_or("node dispatch")?;
    assert_eq!(
        node_dispatch.outbox.message_id(),
        node_send.request.message_id
    );
    let node_body = node_dispatch.body.ok_or("node dispatch body")?;
    let _node_delivered = fixture.runtime.provider_worker_confirm_message_delivery(
        fixture.principal,
        node_send.request.message_id,
        node_dispatch.outbox.revision(),
        "conversation-node-composed",
        "node-user-turn".to_owned(),
        node_body,
        provider_now + 1,
    )?;
    let dispatch = fixture
        .runtime
        .provider_worker_next_message(fixture.principal, provider_now + 2)?
        .ok_or("direct dispatch")?;
    assert_eq!(dispatch.outbox.message_id(), direct_send.request.message_id);
    let delivered_body = dispatch.body.ok_or("direct dispatch body")?;
    let _delivered = fixture.runtime.provider_worker_confirm_message_delivery(
        fixture.principal,
        direct_send.request.message_id,
        dispatch.outbox.revision(),
        "conversation-composed",
        "user-turn".to_owned(),
        delivered_body,
        provider_now + 3,
    )?;
    let incoming_command = ProviderIncomingCommand {
        reply_to_message_id: direct_send.request.message_id,
        provider_conversation_id: "conversation-composed".to_owned(),
        provider_turn_id: "assistant-turn".to_owned(),
        role: ProviderIncomingRole::Assistant,
        body: "assistant evidence".to_owned(),
    };
    let incoming = fixture.runtime.provider_worker_record_incoming(
        fixture.principal,
        incoming_command.clone(),
        provider_now + 4,
    )?;
    assert!(incoming.created());

    let native_direct_query = WorkspaceChatQueryRequest {
        scope: scope.clone(),
        session_id: native_session,
        sender_activity_id: native_activity,
        query: WorkspaceChatQueryWire {
            workspace_id: scope.workspace_id,
            agent_profile_id: fixture.target_profile,
            runtime_session_id: Some(fixture.target_runtime),
            before_sequence: None,
        },
    };
    let native_direct = client
        .workspace_chat_read(&fixture.bearer, &native_direct_query)
        .await?;
    assert_eq!(
        native_direct
            .snapshot
            .target
            .as_ref()
            .ok_or("native sender Profile target")?
            .operation_id,
        fixture.operation_id
    );
    let native_target_fence = native_direct
        .snapshot
        .target
        .clone()
        .ok_or("native sender Profile target fence")?;
    let native_send = WorkspaceChatSendRequest {
        scope: scope.clone(),
        session_id: native_session,
        sender_activity_id: native_activity,
        request: WorkspaceChatMessageRequest {
            query: native_direct_query.query.clone(),
            message_id: Uuid::from_u128(0x781),
            text: "native sender to the existing Profile recipient".to_owned(),
            target_fence: native_target_fence,
        },
    };
    let native_receipt = client
        .workspace_chat_send(&fixture.bearer, &native_send)
        .await?;
    let native_replay = client
        .workspace_chat_send(&fixture.bearer, &native_send)
        .await?;
    assert_eq!(native_replay.receipt, native_receipt.receipt);
    let native_recovery = client
        .workspace_chat_recover(&fixture.bearer, &native_send)
        .await?
        .receipt
        .ok_or("exact native sender recovery")?;
    assert_eq!(native_recovery, native_receipt.receipt);
    assert_eq!(
        native_receipt.receipt.message_id,
        native_send.request.message_id
    );
    let native_queued = client
        .workspace_chat_read(&fixture.bearer, &native_direct_query)
        .await?;
    let native_entries = native_queued
        .snapshot
        .messages
        .iter()
        .filter(|entry| entry.id == native_send.request.message_id)
        .collect::<Vec<_>>();
    assert_eq!(native_entries.len(), 1);
    assert_eq!(native_entries[0].actor_label, "Codex Main");
    assert_eq!(native_entries[0].body, native_send.request.text);
    assert_eq!(
        native_entries[0].provider_state,
        Some(ChatProviderStateWire::Queued)
    );
    let native_author = Connection::open(fixture.root.join("agent-operations.sqlite3"))?.query_row(
        "SELECT author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name FROM ops_workspace_messages WHERE message_id=?1",
        params![native_send.request.message_id.as_bytes()],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, Option<Vec<u8>>>(3)?,
                row.get::<_, Option<Vec<u8>>>(4)?,
                row.get::<_, String>(5)?,
            ))
        },
    )?;
    assert_eq!(native_author.0, 4);
    assert_eq!(native_author.1, None);
    assert_eq!(native_author.2, None);
    assert_eq!(
        native_author.3,
        Some(native_participant.as_bytes().to_vec())
    );
    assert_eq!(native_author.4, Some(native_session.as_bytes().to_vec()));
    assert_eq!(native_author.5, "Codex Main");
    let native_message_count: i64 =
        Connection::open(fixture.root.join("agent-operations.sqlite3"))?.query_row(
            "SELECT COUNT(*) FROM ops_workspace_messages WHERE message_id=?1",
            params![native_send.request.message_id.as_bytes()],
            |row| row.get(0),
        )?;
    assert_eq!(native_message_count, 1);

    cancellation.cancel();
    server.await??;
    native_cancellation.cancel();
    native_server.await??;
    unassigned_cancellation.cancel();
    unassigned_server.await??;
    let ComposedFixture {
        root,
        runtime,
        bearer,
        token,
        principal,
        scope,
        sender_session,
        sender_activity,
        target_activity,
        target_agent,
        ..
    } = fixture;
    drop(runtime);
    let reopened = Arc::new(
        OrchestrationRuntime::open_with_broker_cluster(&root, broker)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?),
    );
    let (restarted_address, restarted_cancel, restarted_server) =
        start_backend(Arc::clone(&reopened), &token, fake.authority()?)?;
    let restarted = OrchestrationHttpClient::new(restarted_address, DEADLINE)?;
    let (native_restarted_address, native_restarted_cancel, native_restarted_server) =
        start_backend(Arc::clone(&reopened), &token, native_fake.authority()?)?;
    let native_restarted = OrchestrationHttpClient::new(native_restarted_address, DEADLINE)?;
    let (unassigned_restarted_address, unassigned_restarted_cancel, unassigned_restarted_server) =
        start_backend(Arc::clone(&reopened), &token, unassigned_fake.authority()?)?;
    let unassigned_restarted =
        OrchestrationHttpClient::new(unassigned_restarted_address, DEADLINE)?;
    let admission_after_restart = tokio::time::timeout(DEADLINE, async {
        loop {
            match restarted
                .recover_node_assignment_admission(&bearer, &admission_request)
                .await
            {
                Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                result => return result,
            }
        }
    })
    .await??;
    assert_eq!(admission_after_restart.admission, Some(first.admission));
    let native_admission_after_restart = native_restarted
        .recover_node_assignment_admission(&bearer, &native_request)
        .await?;
    assert_eq!(
        native_admission_after_restart.admission,
        Some(native_first.admission.clone())
    );
    let unassigned_admission_after_restart = tokio::time::timeout(DEADLINE, async {
        loop {
            match unassigned_restarted
                .recover_node_assignment_admission(&bearer, &unassigned_request)
                .await
            {
                Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                result => return result,
            }
        }
    })
    .await??;
    assert_eq!(
        unassigned_admission_after_restart.admission,
        Some(unassigned_first.admission.clone())
    );
    let direct_after_restart = restarted
        .workspace_chat_recover(&bearer, &direct_send)
        .await?
        .receipt
        .ok_or("restart direct receipt")?;
    assert_eq!(
        direct_after_restart.message_id,
        direct_send.request.message_id
    );
    let native_after_restart = restarted
        .workspace_chat_recover(&bearer, &native_send)
        .await?
        .receipt
        .ok_or("reopened native sender receipt")?;
    assert_eq!(native_after_restart, native_receipt.receipt);
    let native_history_after_restart = restarted
        .workspace_chat_read(&bearer, &native_direct_query)
        .await?;
    let native_reopened_entries = native_history_after_restart
        .snapshot
        .messages
        .iter()
        .filter(|entry| entry.id == native_send.request.message_id)
        .collect::<Vec<_>>();
    assert_eq!(native_reopened_entries.len(), 1);
    assert_eq!(
        native_reopened_entries[0].provider_state,
        Some(ChatProviderStateWire::Queued)
    );
    let duplicate =
        reopened.provider_worker_record_incoming(principal, incoming_command, provider_now + 5)?;
    assert!(!duplicate.created());
    let detach = Connection::open(root.join("agent-operations.sqlite3"))?;
    assert_eq!(
        detach.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3",
            params![scope.workspace_id.as_bytes(),target_activity.as_bytes(),target_agent.as_bytes()],
        )?,
        1
    );
    drop(detach);
    let unavailable = restarted
        .workspace_chat_read(
            &bearer,
            &WorkspaceChatQueryRequest {
                scope: scope.clone(),
                session_id: sender_session,
                sender_activity_id: sender_activity,
                query: direct_query.query,
            },
        )
        .await?;
    assert!(unavailable.snapshot.target.is_none());
    assert!(unavailable.snapshot.unavailable_reason.is_some());

    restarted_cancel.cancel();
    restarted_server.await??;
    native_restarted_cancel.cancel();
    native_restarted_server.await??;
    unassigned_restarted_cancel.cancel();
    unassigned_restarted_server.await??;
    drop(reopened);
    drop(token);
    fake.shutdown().await?;
    native_fake.shutdown().await?;
    unassigned_fake.shutdown().await?;
    drop(cluster);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
