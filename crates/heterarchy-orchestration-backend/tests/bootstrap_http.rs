//! Real loopback Forge-to-Orchestration bootstrap over isolated source stores.
//! This proves import transport and the Team/AgentRequest read gate, not provider execution.

use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use base64::Engine as _;
use forge_backend::ForgeHttpClientError;
use forge_backend::http::{AppState, BackendClients};
use forge_backend::ops::CommandOp;
use forge_backend::remote_authority::RemoteAgentSessionAuthority;
use forge_backend::{BackendConfig as ForgeConfig, ForgeHttpClient};
use forge_control::{ActivityBootstrapStatus, AgentSessionResolveRequest, ForgeControlClient};
use forge_control::{
    AgentAssignmentGetRequest, AgentAssignmentMutation, AgentAssignmentMutationRequest,
    AgentAssignmentReadAuthority, AgentContextRequest, AgentDagPlanCommitRequest,
    AgentExecutionAuthorityRequest, AgentFocusRequest, ApprovalOnceRequest,
    ApprovalSubmissionStatus, DagNodeStartRequest, EffectFactOutboxReadRequest,
    SubmitExecutionForApproval,
};
use forge_control::{
    AgentOperationsFreezeReadRequest, AgentOperationsFreezeReleaseRequest,
    AgentOperationsFreezeRequest, ForgeApprovalCoordinator, ForgeControl, ForgeControlWorker,
    ForgeExecutionCoordinator, HomeTransferSourceService, ResolvedWorkspaceScope,
};
use forge_domain::{
    ActivityCoordinatorActor, AgentAssignmentId, AgentAssignmentState, AgentAssignmentTarget,
    AgentDagDraft, AgentDagPlanRequest, AgentDagPlanRequestId, AgentFocus, AgentFocusNode,
    AgentInstanceId, AgentListLimit, AgentWorkUpdateId, AgentWorkUpdateSequence, ApprovalRequestId,
    ApprovalRequestState, DagAcceptanceCriterion, DagHeadRevision, DagNodeDefinition, DagNodeId,
    DagNodeObjective, DagNodeOrdinal, DagNodeRevision, DagNodeTitle, DagRevision, DagRevisionId,
    DagRevisionNumber, ExecutionGrantId, GoalAttribution, GoalContract, GoalId,
    GoalNodeAttribution, GoalObjective, GoalRevision, GoalStatement, OfferAgentAssignment,
    RegisteredDagNode, RegisteredGoal,
};
use forge_domain::{ActivityId, AgentSessionId, RuntimeInstanceId};
use forge_domain::{
    AuthenticatedPrincipal, CancelAgentAssignment, DirectTokenId, EstablishWorkspace,
    ForgeBackendId, LocationId, LocationRevision, OrchestrationIssuerId, TimestampMillis,
    WorkspaceId, WorkspaceRoot,
};
use forge_domain::{
    ExecutionAttribution, ExecutionId, ExecutionKind, ExecutionLimits, ExecutionListLimit,
    ExecutionObservationSelection, ExecutionOutputCursor, ExecutionOutputEvent,
    ExecutionOutputLimit, ExecutionRequest, ExecutionState, ExecutionTimeoutMillis,
};
use forge_persistence::{
    AgentOperationsFreeze, AgentOperationsFreezeState, EffectFact, GlobalRegistry, WorkspaceStore,
};
use forge_workspace::{LocationService, WorkspaceStateLayout};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
    McpTokenWorker,
};
use heterarchy_orchestration_backend::client::{
    ClientError, OrchestrationHttpClient, ReadbackTarget,
};
use heterarchy_orchestration_backend::forge_commands::ForgeCommandHttpClient;
use heterarchy_orchestration_backend::forge_source::ForgeSourceHttpClient;
use heterarchy_orchestration_backend::internal_session::{
    InternalSessionServerConfig, InternalSessionServerHandle,
};
use heterarchy_orchestration_backend::wire::agent_request_admission::{
    AdmissionEffectScopeWire, AdmissionIndependenceWire, AgentRequestAdmissionRequest,
};
use heterarchy_orchestration_backend::wire::agent_requests::{
    AgentRequestListQueryRequest, AgentRequestQueryRequest,
};
use heterarchy_orchestration_backend::wire::approval::{
    ApprovalAdmissionWire, ApprovalDecideRequest, ApprovalListQueryRequest, ApprovalPermissionWire,
    ApprovalPolicyQueryRequest, ApprovalPolicySetRequest, ApprovalRequestGetRequest,
    ApprovalSubmitRequest, ApprovalSubmitResponse,
};
use heterarchy_orchestration_backend::wire::coordination::CoordinationQueryRequest;
use heterarchy_orchestration_backend::wire::leadership::LeadershipQueryRequest;
use heterarchy_orchestration_backend::wire::leadership_lifecycle::{
    LeadershipAdvanceRequest, LeadershipLifecycleTarget,
};
use heterarchy_orchestration_backend::wire::membership::{
    MemberActivateRequest, MemberConfirmRequest, MemberPhaseWire, MemberRegisterRequest,
    MemberRegisterResponse, MemberRegistrationQueryRequest, MemberTarget, MemberTransportWire,
};
use heterarchy_orchestration_backend::wire::node_chat::{ChatQueryWire, NodeChatQueryRequest};
use heterarchy_orchestration_backend::wire::node_team::{
    NodeTeamQueryRequest, NodeTeamScopeWire, NodeTeamSetRequest,
};
use heterarchy_orchestration_backend::wire::operations_import::{
    ImportHandoffSealRequest, ImportPhaseWire, ImportPrepareRequest, ImportProgressWire,
    ImportStepRequest,
};
use heterarchy_orchestration_backend::wire::team_commands::{
    TeamCreateV2Request, TeamMutationReaderV2Wire, TeamMutationV2Response, TeamRosterMemberRequest,
};
use heterarchy_orchestration_backend::wire::teams::{TeamGetQueryRequest, TeamListQueryRequest};
use heterarchy_orchestration_backend::wire::workspace_project::{
    WorkspaceProjectConfigureRequest, WorkspaceProjectQueryRequest, WorkspaceProjectReadbackRequest,
};
use heterarchy_orchestration_backend::wire::{
    AgentProfileCreateRequest, AgentSessionOpenRequest, ProfileSessionBindRequest,
    WorkspaceQueryScope,
};
use heterarchy_orchestration_backend::{CoordinatorAuthority, serve_with_runtime};
use heterarchy_orchestration_operations::{BrokerClusterConfig, ConfigureAdmissionIssuerCommand};
use heterarchy_orchestration_runtime::OrchestrationRuntime;
use heterarchy_orchestration_runtime::access::OperatorPolicy;
use heterarchy_orchestration_runtime::membership::member_registration_request_id;
use heterarchy_orchestration_sessions::SessionCatalogAuthority;
use hyper::StatusCode;
use hyper::header::HeaderValue;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[path = "../../heterarchy-orchestration-runtime/tests/support/broker_cluster.rs"]
mod broker_cluster;
#[path = "bootstrap_http/legacy_slot_recovery.rs"]
mod legacy_slot_recovery;
#[path = "bootstrap_http/mission_evaluation.rs"]
mod mission_evaluation;
#[path = "bootstrap_http/mission_evidence.rs"]
mod mission_evidence;
#[path = "bootstrap_http/mission_t2_checkpoint.rs"]
mod mission_t2_checkpoint;
#[path = "bootstrap_http/native_membership.rs"]
mod native_membership;
#[path = "bootstrap_http/p10.rs"]
mod p10;
#[path = "bootstrap_http/reviewer_participation.rs"]
mod reviewer_participation;
#[path = "bootstrap_http/reviewer_qualification_rule.rs"]
mod reviewer_qualification_rule;
#[path = "bootstrap_http/team_expansion.rs"]
mod team_expansion;
#[path = "bootstrap_http/team_expansion_v3_existing_home.rs"]
mod team_expansion_v3_existing_home;
#[path = "bootstrap_http/temporary_dispatch_lifecycle.rs"]
mod temporary_dispatch_lifecycle;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const FORGE_SECRET: &str = "isolated-bootstrap-http-fixture";
const SESSION_RESOLVER_SECRET: &str = "isolated-bootstrap-session-resolver";
const DEADLINE: Duration = Duration::from_secs(5);

struct ForgeHost {
    state: AppState,
    worker: ForgeControlWorker,
    leases: ForgeControlWorker,
    execution: ForgeExecutionCoordinator,
    approval: ForgeApprovalCoordinator,
    control_client: ForgeControlClient,
    scope: ResolvedWorkspaceScope,
    client: ForgeHttpClient,
    issuer: OrchestrationIssuerId,
    address: std::net::SocketAddr,
    boot_instance: RuntimeInstanceId,
    verified_ci: bool,
}

#[derive(Debug, Clone, Copy)]
struct ForgeExecutionService {
    backend: ForgeBackendId,
    issuer: OrchestrationIssuerId,
    activity_runtime: RuntimeInstanceId,
}

struct SourceAssignmentFixture {
    bootstrap_assignment_id: AgentAssignmentId,
    bootstrap_target: AgentAssignmentTarget,
    goal: RegisteredGoal,
    mission_node: RegisteredDagNode,
    dispatch_node: RegisteredDagNode,
    unassigned_node: NodeTeamScopeWire,
}

#[derive(Clone, Copy)]
struct MissionAssignmentSource {
    assignment_id: AgentAssignmentId,
    target: AgentAssignmentTarget,
    source_sequence: u64,
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
        session_resolver: std::net::SocketAddr,
        verified_ci: bool,
    ) -> TestResult<Self> {
        let scope = establish_source(root)?;
        if verified_ci {
            p10::write_sealed_makefile(root)?;
        }
        let registry_path = root.join("registry.sqlite3");
        let owner = ForgeControl::with_session_authority(
            LocationService::new(GlobalRegistry::open_existing(&registry_path)?),
            Box::new(RemoteAgentSessionAuthority::new(
                tokio::runtime::Handle::current(),
                session_resolver,
                SESSION_RESOLVER_SECRET,
                DEADLINE,
            )?),
        );
        let (control, worker) = ForgeControlWorker::start(owner)?;
        let boot_instance = RuntimeInstanceId::from_uuid(Uuid::now_v7());
        let execution =
            p10::start_execution_coordinator(control.clone(), root, boot_instance, verified_ci)
                .await?;
        let approval = ForgeApprovalCoordinator::start(control.clone(), execution.client())?;
        let lease_owner = ForgeControl::with_session_authority(
            LocationService::new(GlobalRegistry::open_existing(&registry_path)?),
            Box::new(RemoteAgentSessionAuthority::new(
                tokio::runtime::Handle::current(),
                session_resolver,
                SESSION_RESOLVER_SECRET,
                DEADLINE,
            )?),
        );
        let (lease_client, leases) = ForgeControlWorker::start_activity_lease(lease_owner)?;
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
                runtime_instance_id: boot_instance.into_uuid(),
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
            control_client: control,
            scope,
            client,
            issuer,
            address,
            boot_instance,
            verified_ci,
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

/// Reserve six distinct loopback ports without serving the Broker protocol.
/// Holding every listener prevents this negative-path test from accidentally
/// connecting to a live Broker cluster owned by another user or agent.
fn unavailable_broker_cluster() -> TestResult<(BrokerClusterConfig, Vec<std::net::TcpListener>)> {
    let listeners = (0..6)
        .map(|_| std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)))
        .collect::<Result<Vec<_>, _>>()?;
    let mut cluster = BrokerClusterConfig::from_node_pairs([
        (listeners[0].local_addr()?, listeners[1].local_addr()?),
        (listeners[2].local_addr()?, listeners[3].local_addr()?),
        (listeners[4].local_addr()?, listeners[5].local_addr()?),
    ])?;
    cluster.timeout = Duration::from_millis(500);
    Ok((cluster, listeners))
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

async fn assert_verified_workspace_query_gate(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &WorkspaceQueryScope,
    verified: bool,
) -> TestResult {
    let team_request = TeamListQueryRequest {
        scope: scope.clone(),
        session_id: None,
        limit: 10,
    };
    let agent_request = AgentRequestListQueryRequest {
        scope: scope.clone(),
        session_id: None,
        limit: 10,
    };
    let teams = client.team_list(bearer, &team_request).await;
    let requests = client.agent_requests(bearer, &agent_request).await;
    if verified {
        let teams = teams?;
        let requests = requests?;
        assert_eq!(teams.scope, *scope);
        assert!(teams.teams.is_empty());
        assert!(!teams.truncated);
        assert_eq!(requests.scope, *scope);
        assert!(requests.items.is_empty());
        assert!(!requests.truncated);
    } else {
        assert!(matches!(
            teams,
            Err(ClientError::Rejected { status, .. })
                if status == StatusCode::CONFLICT.as_u16()
        ));
        assert!(matches!(
            requests,
            Err(ClientError::Rejected { status, .. })
                if status == StatusCode::CONFLICT.as_u16()
        ));
    }
    Ok(())
}

async fn assert_session_open_fails_closed_without_broker(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
) -> TestResult {
    // All Broker addresses belong to bound but unserved test-only listeners.
    // A Session mutation needs a Broker owner before it can write an
    // Orchestration record. On uncertain transport failure, read the exact
    // nonce; never retry the mutation.
    let request_id = Uuid::now_v7();
    let attempted = client
        .open_session(
            bearer,
            &AgentSessionOpenRequest {
                request_id: request_id.to_string(),
            },
        )
        .await;
    match attempted {
        Err(ClientError::UnknownOutcome {
            readback: heterarchy_orchestration_backend::client::ReadbackTarget::SessionRequest(id),
        }) if id == request_id => {}
        Err(ClientError::Rejected { status, .. })
            if status == StatusCode::SERVICE_UNAVAILABLE.as_u16() => {}
        Ok(_) => return Err("Session opened without any Broker owner".into()),
        Err(error) => return Err(format!("unexpected blocked Session result: {error:?}").into()),
    }
    assert!(
        client
            .session_open_readback(bearer, request_id)
            .await?
            .is_none(),
        "Broker-less Session admission did not leave a durable Session"
    );
    Ok(())
}

/// Inputs and authenticated source context for the real Broker flow.
struct RealBrokerAgentRequestFlow<'a> {
    client: &'a OrchestrationHttpClient,
    bearer: &'a HeaderValue,
    backend_address: std::net::SocketAddr,
    principal: AuthenticatedPrincipal,
    scope: &'a WorkspaceQueryScope,
    progress: &'a ImportProgressWire,
    source: &'a ForgeHost,
    activity_id: Uuid,
    service: ForgeExecutionService,
    non_operator: &'a reviewer_qualification_rule::NonOperatorCredential,
    transfer_exercise: TransferExercise,
}

#[derive(Clone, Copy)]
struct VerifiedBrokerOperatorSession {
    session_id: Uuid,
    revision: i64,
}

/// The real Broker authorizes Sessions; a Verified import still requires
/// Forge's exact delegation seal before membership mutation.
async fn assert_real_broker_agent_request_flow(
    flow: &RealBrokerAgentRequestFlow<'_>,
) -> TestResult<u64> {
    let operator_session = open_verified_operator_session(flow).await?;
    let readback_session = open_verified_operator_session(flow).await?;
    assert_ne!(operator_session.session_id, readback_session.session_id);
    let reviewer_rule_flow = reviewer_qualification_rule::ReviewerQualificationRuleFlow {
        client: flow.client,
        operator_bearer: flow.bearer,
        source: flow.source,
        scope: flow.scope,
        operator_principal: flow.principal,
        original_session: reviewer_qualification_rule::RuleSession {
            session_id: operator_session.session_id,
            revision: operator_session.revision,
        },
        readback_session: reviewer_qualification_rule::RuleSession {
            session_id: readback_session.session_id,
            revision: readback_session.revision,
        },
        non_operator: flow.non_operator,
    };
    reviewer_qualification_rule::assert_reviewer_qualification_rule_http_flow(&reviewer_rule_flow)
        .await?;
    let target = MemberTarget {
        scope: flow.scope.clone(),
        activity_id: flow.activity_id,
        session_id: operator_session.session_id,
    };
    let request_id = assert_undelegated_member_absent(flow.client, flow.bearer, &target).await?;
    assert_verified_import_handoff(flow).await?;
    let first_member =
        register_and_admit_operator_member(flow, &target, request_id, operator_session).await?;
    assert_team_flow_and_rebind_second_session(flow, &target, first_member).await
}

async fn open_verified_operator_session(
    flow: &RealBrokerAgentRequestFlow<'_>,
) -> TestResult<VerifiedBrokerOperatorSession> {
    let request_id = Uuid::now_v7();
    let opened = flow
        .client
        .open_session(
            flow.bearer,
            &AgentSessionOpenRequest {
                request_id: request_id.to_string(),
            },
        )
        .await?;
    assert!(opened.created);
    assert_eq!(opened.session.request_id, request_id);
    assert_eq!(opened.session.state, "open");
    assert_eq!(
        flow.client
            .session_open_readback(flow.bearer, request_id)
            .await?
            .ok_or("Broker-backed Session receipt missing")?
            .session_id,
        opened.session.session_id
    );
    let session_id = AgentSessionId::from_uuid(opened.session.session_id);
    let control = flow.source.control_client.clone();
    let principal = flow.principal;
    let original = tokio::task::spawn_blocking(move || {
        control.agent_session_resolve(AgentSessionResolveRequest {
            session_id,
            principal,
        })
    })
    .await??;
    assert_eq!(original.id().into_uuid(), opened.session.session_id);
    assert_eq!(original.principal(), flow.principal);
    Ok(VerifiedBrokerOperatorSession {
        session_id: opened.session.session_id,
        revision: opened.session.revision,
    })
}

async fn assert_verified_import_handoff(flow: &RealBrokerAgentRequestFlow<'_>) -> TestResult {
    let sealed =
        seal_verified_source_import(flow.source, flow.service.issuer, flow.progress).await?;
    assert_eq!(sealed["manifest_digest"], flow.progress.verify_digest);
    assert_eq!(
        flow.service.backend,
        flow.source.client.expected_backend_id()
    );
    assert_eq!(
        flow.progress.destination_id,
        flow.source.client.destination_id()
    );
    let request = ImportHandoffSealRequest {
        scope: flow.scope.clone(),
        destination_id: flow.source.client.destination_id(),
        expected_source_revision: flow.progress.source_revision,
        expected_verified_digest: flow.progress.verify_digest.clone(),
    };
    let handoff = flow
        .client
        .import_handoff_seal(flow.bearer, flow.progress.migration_id, &request)
        .await?;
    assert_eq!(handoff.migration_id, flow.progress.migration_id);
    assert_eq!(handoff.workspace_id, flow.scope.workspace_id);
    assert_eq!(handoff.destination_id, flow.source.client.destination_id());
    assert_eq!(handoff.source_revision, flow.progress.source_revision);
    assert_eq!(handoff.verified_digest, flow.progress.verify_digest);
    assert_eq!(handoff.issuer_id, flow.service.issuer.into_uuid());
    assert_eq!(handoff.backend_id, flow.service.backend.into_uuid());
    assert_eq!(
        flow.client
            .import_handoff_seal_readback(flow.bearer, flow.progress.migration_id, &request)
            .await?,
        Some(handoff)
    );
    Ok(())
}

async fn register_and_admit_operator_member(
    flow: &RealBrokerAgentRequestFlow<'_>,
    target: &MemberTarget,
    request_id: Uuid,
    session: VerifiedBrokerOperatorSession,
) -> TestResult<ReadyFixtureMember> {
    native_membership::assert_native_member_registration(
        flow.client,
        flow.bearer,
        flow.principal,
        flow.source,
        flow.scope,
        flow.activity_id,
        match flow.transfer_exercise {
            TransferExercise::Preparation | TransferExercise::SourceDispatch => {
                native_membership::ProfileLinkTiming::BeforeWorkspaceRegistration
            }
            _ => native_membership::ProfileLinkTiming::AfterWorkspaceRegistration,
        },
    )
    .await?;
    let profile_id = bind_fixture_profile_session(
        flow.client,
        flow.bearer,
        session.session_id,
        "isolated-bootstrap-planner",
    )
    .await?;
    let registration = native_membership::assert_profile_member_registration(
        flow.client,
        flow.bearer,
        target,
        request_id,
    )
    .await?;
    let leadership_revision = assert_live_forge_coordinator(
        flow.source,
        flow.principal,
        flow.activity_id,
        session.session_id,
        registration.progress.agent_id,
    )
    .await?;
    assert!(leadership_revision > 0);
    configure_fixture_workspace_project(flow.client, flow.bearer, flow.scope).await?;
    assert_durable_agent_request(
        flow.client,
        flow.bearer,
        &registration.target,
        registration.progress.agent_id,
        leadership_revision,
    )
    .await?;
    Ok(ReadyFixtureMember {
        session_id: session.session_id,
        agent_id: registration.progress.agent_id,
        participant_id: registration.progress.participant_id,
        profile_id,
        revision: session.revision,
    })
}

/// Supply the required provider route in this isolated fixture before admission
/// prepares a new `RuntimeSession`; this does not dispatch provider work.
async fn configure_fixture_workspace_project(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &WorkspaceQueryScope,
) -> TestResult {
    const PROJECT_ID: &str = "g-p-6ad231f7ad7c8191bfc1d5ff782c171f";

    let query = WorkspaceProjectQueryRequest {
        scope: scope.clone(),
    };
    let before = client.workspace_project_binding(bearer, &query).await?;
    assert!(
        before.binding.is_none(),
        "isolated bootstrap Workspace unexpectedly already has a Project binding"
    );

    let request = WorkspaceProjectConfigureRequest {
        request_id: Uuid::now_v7(),
        scope: scope.clone(),
        expected_revision: None,
        project_id: PROJECT_ID.to_owned(),
    };
    let readback_request = WorkspaceProjectReadbackRequest {
        request_id: request.request_id,
        scope: request.scope.clone(),
    };
    let response_receipt = match client.configure_workspace_project(bearer, &request).await {
        Ok(response) => {
            assert!(!response.replayed);
            assert!(response.work_authorized);
            assert_eq!(response.current_binding.project_id, request.project_id);
            Some(response.receipt)
        }
        Err(
            error @ (ClientError::ReadbackRequired { .. } | ClientError::UnknownOutcome { .. }),
        ) => {
            let readback_matches = match &error {
                ClientError::ReadbackRequired {
                    readback: ReadbackTarget::WorkspaceProjectBinding(original),
                }
                | ClientError::UnknownOutcome {
                    readback: ReadbackTarget::WorkspaceProjectBinding(original),
                } => original.as_ref() == &readback_request,
                _ => false,
            };
            if !readback_matches {
                return Err(format!(
                    "Workspace Project configure has a foreign readback target: {error:?}"
                )
                .into());
            }
            None
        }
        Err(error) => {
            return Err(format!("Workspace Project configure failed: {error:?}").into());
        }
    };

    let readback = client
        .workspace_project_binding_readback(bearer, &readback_request)
        .await?;
    let receipt = readback
        .receipt
        .ok_or("exact Workspace Project configure receipt missing")?;
    assert_eq!(receipt.request_id, request.request_id);
    assert_eq!(receipt.scope, request.scope);
    assert_eq!(receipt.expected_revision, request.expected_revision);
    assert_eq!(receipt.project_id, request.project_id);
    assert_eq!(receipt.binding.workspace_id, request.scope.workspace_id);
    assert_eq!(receipt.binding.project_id, request.project_id);
    assert_eq!(receipt.binding.revision, "1");
    if let Some(response_receipt) = response_receipt {
        assert_eq!(receipt, response_receipt);
    }

    let current = client.workspace_project_binding(bearer, &query).await?;
    let binding = current
        .binding
        .ok_or("configured Workspace Project binding is not current")?;
    assert!(current.work_authorized);
    assert_eq!(binding.project_id, request.project_id);
    assert_eq!(binding.revision, receipt.binding.revision);
    Ok(())
}

async fn assert_team_flow_and_rebind_second_session(
    flow: &RealBrokerAgentRequestFlow<'_>,
    target: &MemberTarget,
    first: ReadyFixtureMember,
) -> TestResult<u64> {
    let members = provision_team_flow_members(flow, target, &first).await?;
    assert_reviewer_participation_for_team_flow(flow, target, &members).await?;
    assert_no_initial_workspace_leadership(flow, target).await?;

    let outcome = assert_team_leadership_and_execution(flow, target, &first, &members).await?;
    if flow.transfer_exercise == TransferExercise::SourceDispatch {
        // Source preparation fences fresh target work; this exercise ends
        // before the ordinary Session-rebind path.
        return Ok(outcome.final_source_assignment.source_sequence);
    }
    let final_source_sequence =
        assert_rebind_preserves_mission_evidence(flow, target, &members, &outcome).await?;
    close_initial_operator_session(flow, target, &first).await?;
    Ok(final_source_sequence)
}

struct TeamFlowMembers {
    reviewer: ReadyFixtureMember,
    mission_assignee: ReadyFixtureMember,
}

struct TeamFlowOutcome {
    evidence: mission_evidence::MissionEvidenceRecoveryFixture,
    mission_assignment: MissionAssignmentSource,
    final_source_assignment: MissionAssignmentSource,
}

async fn provision_team_flow_members(
    flow: &RealBrokerAgentRequestFlow<'_>,
    target: &MemberTarget,
    first: &ReadyFixtureMember,
) -> TestResult<TeamFlowMembers> {
    let reviewer = assert_ready_profile_member(
        flow.client,
        flow.bearer,
        flow.principal,
        flow.source,
        target,
        "isolated-bootstrap-reviewer",
    )
    .await?;
    let mission_assignee = assert_ready_profile_member(
        flow.client,
        flow.bearer,
        flow.principal,
        flow.source,
        target,
        "isolated-bootstrap-mission-assignee",
    )
    .await?;
    assert_ne!(reviewer.session_id, first.session_id);
    assert_ne!(reviewer.agent_id, first.agent_id);
    assert_ne!(reviewer.participant_id, first.participant_id);
    assert_ne!(mission_assignee.session_id, first.session_id);
    assert_ne!(mission_assignee.agent_id, first.agent_id);
    assert_ne!(mission_assignee.participant_id, first.participant_id);
    assert_ne!(mission_assignee.session_id, reviewer.session_id);
    assert_ne!(mission_assignee.agent_id, reviewer.agent_id);
    assert_ne!(mission_assignee.participant_id, reviewer.participant_id);
    Ok(TeamFlowMembers {
        reviewer,
        mission_assignee,
    })
}

async fn assert_reviewer_participation_for_team_flow(
    flow: &RealBrokerAgentRequestFlow<'_>,
    target: &MemberTarget,
    members: &TeamFlowMembers,
) -> TestResult {
    reviewer_participation::assert_reviewer_participation_http_flow(
        &reviewer_participation::ReviewerParticipationFlow {
            client: flow.client,
            operator_bearer: flow.bearer,
            operator_principal: flow.principal,
            non_operator: flow.non_operator,
            source: flow.source,
            scope: &target.scope,
            activity_id: target.activity_id,
            target_a: &members.reviewer,
            target_b: &members.mission_assignee,
        },
    )
    .await
}

async fn assert_no_initial_workspace_leadership(
    flow: &RealBrokerAgentRequestFlow<'_>,
    target: &MemberTarget,
) -> TestResult {
    let leadership = flow
        .client
        .leadership_view(
            flow.bearer,
            &LeadershipQueryRequest {
                scope: target.scope.clone(),
                session_id: Some(target.session_id),
                activity_id: None,
                team_id: None,
                review_after: None,
                limit: 10,
            },
        )
        .await?;
    assert!(
        leadership.leadership.is_none(),
        "This empty-Forge fixture has no AssignmentOffered evidence to authorize Workspace leadership"
    );
    Ok(())
}

async fn assert_team_leadership_and_execution(
    flow: &RealBrokerAgentRequestFlow<'_>,
    target: &MemberTarget,
    first: &ReadyFixtureMember,
    members: &TeamFlowMembers,
) -> TestResult<TeamFlowOutcome> {
    let assignments = issue_bootstrap_forge_assignment_offer(
        flow.source,
        flow.principal,
        target.activity_id,
        &members.reviewer,
    )
    .await?;
    let leadership_case = SourceLeadershipTeamCase {
        client: flow.client,
        bearer: flow.bearer,
        source: flow.source,
        creator: target,
        leader: &members.reviewer,
        third: &members.mission_assignee,
        assignments: &assignments,
        principal: flow.principal,
    };
    let leadership_outcome = assert_team_from_source_leadership(&leadership_case)
        .await
        .map_err(|error| format!("P10 Team/Orchestration approval stage: {error:?}"))?;
    if flow.source.verified_ci {
        assert_real_approved_forge_execution(
            flow.source,
            flow.principal,
            target,
            first,
            flow.service,
            assignments.unassigned_node,
            &leadership_outcome.approved,
        )
        .await
        .map_err(|error| format!("P10 Forge approved execution stage: {error:?}"))?;
        assert_forge_terminal_effect_consumed(
            flow.client,
            flow.bearer,
            flow.source,
            target,
            leadership_outcome.approved.request.id(),
        )
        .await
        .map_err(|error| format!("P10 source-terminal consumption stage: {error:?}"))?;
    }
    let final_source_assignment =
        temporary_dispatch_lifecycle::assert_temporary_dispatch_http_flow(
            &temporary_dispatch_lifecycle::TemporaryDispatchFlowContext {
                client: flow.client,
                bearer: flow.bearer,
                backend_address: flow.backend_address,
                principal: flow.principal,
                source: flow.source,
                creator: target,
                workspace_leader: &members.reviewer,
                workspace_leadership_revision: leadership_outcome.workspace_leadership_revision,
                home_team_id: leadership_outcome.team_id,
                home_leader: &members.mission_assignee,
                target: first,
                assignments: &assignments,
                transfer_exercise: flow.transfer_exercise,
            },
        )
        .await?;
    Ok(TeamFlowOutcome {
        evidence: leadership_outcome.evidence,
        mission_assignment: leadership_outcome.mission_assignment,
        final_source_assignment,
    })
}

async fn assert_rebind_preserves_mission_evidence(
    flow: &RealBrokerAgentRequestFlow<'_>,
    target: &MemberTarget,
    members: &TeamFlowMembers,
    outcome: &TeamFlowOutcome,
) -> TestResult<u64> {
    // Bootstrap is fixture history. Detach vacates its O leadership row; this
    // readback checks the same semantic actor's original evidence receipts.
    mission_evidence::assert_session_rebind_readback(
        flow.client,
        flow.bearer,
        flow.source,
        target,
        flow.principal,
        &members.reviewer,
        &outcome.evidence,
    )
    .await?;
    assert_mission_assignment_remains_offered(
        flow.source,
        outcome.mission_assignment,
        target.activity_id,
    )
    .await?;
    temporary_dispatch_lifecycle::assert_final_source_cursor(
        flow,
        target,
        outcome.final_source_assignment,
    )
    .await
}

async fn close_initial_operator_session(
    flow: &RealBrokerAgentRequestFlow<'_>,
    target: &MemberTarget,
    first: &ReadyFixtureMember,
) -> TestResult {
    let closed = flow
        .client
        .close_session(
            flow.bearer,
            target.session_id,
            &heterarchy_orchestration_backend::wire::AgentSessionCloseRequest {
                expected_revision: first.revision,
            },
        )
        .await?;
    assert_eq!(closed.state, "closed");
    Ok(())
}

fn fixture_registration(target: &MemberTarget) -> MemberRegisterRequest {
    MemberRegisterRequest {
        target: target.clone(),
        display_name: "isolated broker-backed member".to_owned(),
        declared_role: "planner".to_owned(),
        transport: MemberTransportWire::StreamableHttp,
    }
}

/// A Ready member remains bound to one exact Profile, logical Session and
/// source-attested Forge Agent.
struct ReadyFixtureMember {
    session_id: Uuid,
    agent_id: Uuid,
    participant_id: Uuid,
    profile_id: Uuid,
    revision: i64,
}

async fn assert_ready_profile_member(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    principal: AuthenticatedPrincipal,
    source: &ForgeHost,
    first: &MemberTarget,
    stable_name: &str,
) -> TestResult<ReadyFixtureMember> {
    let opened = client
        .open_session(
            bearer,
            &AgentSessionOpenRequest {
                request_id: Uuid::now_v7().to_string(),
            },
        )
        .await?;
    assert!(opened.created);
    let session = opened.session;
    let session_id = AgentSessionId::from_uuid(session.session_id);
    let control = source.control_client.clone();
    let verified = tokio::task::spawn_blocking(move || {
        control.agent_session_resolve(AgentSessionResolveRequest {
            session_id,
            principal,
        })
    })
    .await??;
    assert_eq!(verified.id().into_uuid(), session.session_id);
    assert_eq!(verified.principal(), principal);
    let profile_id =
        bind_fixture_profile_session(client, bearer, session.session_id, stable_name).await?;
    let target = MemberTarget {
        scope: first.scope.clone(),
        activity_id: first.activity_id,
        session_id: session.session_id,
    };
    let registration = client
        .register_member(bearer, &fixture_registration(&target))
        .await?;
    assert_eq!(registration.progress.session_id, session.session_id);
    assert_eq!(registration.progress.phase, MemberPhaseWire::Reserved);
    assert_ne!(registration.progress.session_id, first.session_id);
    assert_confirmed_member(client, bearer, &registration).await?;
    Ok(ReadyFixtureMember {
        session_id: session.session_id,
        agent_id: registration.progress.agent_id,
        participant_id: registration.progress.participant_id,
        profile_id,
        revision: session.revision,
    })
}

async fn assert_undelegated_member_absent(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    target: &MemberTarget,
) -> TestResult<Uuid> {
    let request_id = member_registration_request_id(
        target.scope.workspace_id,
        target.activity_id,
        target.session_id,
    );
    let outcome = client
        .register_member(bearer, &fixture_registration(target))
        .await;
    assert!(
        matches!(
            outcome,
            Err(ClientError::Rejected { status, .. })
                if status == StatusCode::SERVICE_UNAVAILABLE.as_u16()
        ),
        "Membership without the Forge delegation seal must fail: {outcome:?}"
    );
    let historical = client
        .member_registration(
            bearer,
            &MemberRegistrationQueryRequest {
                target: target.clone(),
                request_id,
            },
        )
        .await?;
    assert!(historical.progress.is_none());
    assert!(historical.commit.is_none());
    assert!(historical.activation.is_none());
    Ok(request_id)
}

/// A Reserved registration is not active: only Orchestration's commit digest
/// and Forge's matching activation receipt can establish a Ready member.
async fn assert_confirmed_member(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    registration: &MemberRegisterResponse,
) -> TestResult {
    assert_eq!(registration.progress.phase, MemberPhaseWire::Reserved);
    let confirmed = client
        .confirm_member(
            bearer,
            &MemberConfirmRequest {
                target: registration.target.clone(),
                request_id: registration.progress.request_id,
                recipients: Vec::new(),
            },
        )
        .await?;
    assert_eq!(
        confirmed.commit.request_id,
        registration.progress.request_id
    );
    assert_eq!(confirmed.commit.commit_digest.len(), 64);
    let activated = client
        .activate_member(
            bearer,
            &MemberActivateRequest {
                target: registration.target.clone(),
                request_id: registration.progress.request_id,
            },
        )
        .await?;
    assert_eq!(activated.activation.phase, MemberPhaseWire::Ready);
    assert_eq!(
        activated.activation.agent_id,
        registration.progress.agent_id
    );
    assert_eq!(
        activated.activation.membership_digest,
        confirmed.commit.commit_digest
    );
    let readback = client
        .member_registration(
            bearer,
            &MemberRegistrationQueryRequest {
                target: registration.target.clone(),
                request_id: registration.progress.request_id,
            },
        )
        .await?;
    assert_eq!(
        readback.progress.as_ref().map(|p| p.phase),
        Some(MemberPhaseWire::Ready)
    );
    assert_eq!(readback.commit, Some(confirmed.commit));
    assert_eq!(readback.activation, Some(activated.activation));
    Ok(())
}

/// Claim the actual activated Forge Agent as Activity Coordinator, then
/// re-read its source-attested principal-bound actor through the HTTP owner.
async fn assert_live_forge_coordinator(
    source: &ForgeHost,
    principal: AuthenticatedPrincipal,
    activity_id: Uuid,
    session_id: Uuid,
    agent_id: Uuid,
) -> TestResult<i64> {
    let AuthenticatedPrincipal::DirectToken(token_id) = principal else {
        return Err("fixture requires a real principal-bound token".into());
    };
    let commands = ForgeCommandHttpClient::new(source.address, FORGE_SECRET, DEADLINE)?;
    let claim = commands
        .command(
            CommandOp::ActivityCoordinatorClaim,
            json!({
                "scope": forge_source_scope(source)?,
                "activity_id": activity_id,
                "session_id": session_id,
                "agent_id": agent_id,
                "principal": { "kind": "direct_token", "token_id": token_id.into_uuid() },
                "observed_at_ms": current_test_time()?
            }),
        )
        .await?;
    assert_eq!(claim["coordinator"]["agent_id"], json!(agent_id));
    assert_eq!(claim["coordinator"]["activity_id"], json!(activity_id));
    let actor = ForgeSourceHttpClient::new(commands)
        .coordinator_actor(&source.scope, activity_id, principal)
        .await?
        .ok_or("Forge did not supply a principal-bound Coordinator Actor")?;
    assert_eq!(actor.agent_id().into_uuid(), agent_id);
    assert_eq!(actor.session_id().into_uuid(), session_id);
    assert_eq!(actor.activity_id().into_uuid(), activity_id);
    assert_eq!(actor.principal(), principal);
    let revision = actor.expected_revision().get();
    assert_eq!(claim["coordinator"]["revision"], json!(revision));
    Ok(revision)
}

/// Submit a genuine read-only capability request under the source-attested
/// Coordinator and Broker owner, then recover the immutable receipt by ID.
async fn assert_durable_agent_request(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    target: &MemberTarget,
    coordinator_agent_id: Uuid,
    coordinator_revision: i64,
) -> TestResult {
    let request = AgentRequestAdmissionRequest {
        scope: target.scope.clone(),
        activity_id: target.activity_id,
        coordinator_session_id: target.session_id,
        coordinator_agent_id,
        coordinator_revision,
        request_id: Uuid::now_v7(),
        objective: "Verify an additional independent research capability".to_owned(),
        capability: "verification".to_owned(),
        count: 1,
        effect_scope: AdmissionEffectScopeWire::ReadOnly,
        independence: AdmissionIndependenceWire::IndependentFromCurrentWriter,
        reason: "Independent source-backed evidence review".to_owned(),
    };
    let result = match client.admit_agent_request(bearer, &request).await {
        Ok(result) => result,
        Err(
            error @ (ClientError::ReadbackRequired { .. } | ClientError::UnknownOutcome { .. }),
        ) => {
            use heterarchy_orchestration_backend::client::ReadbackTarget;

            let readback_target_matches = match &error {
                ClientError::ReadbackRequired {
                    readback: ReadbackTarget::AgentRequestAdmission(original),
                }
                | ClientError::UnknownOutcome {
                    readback: ReadbackTarget::AgentRequestAdmission(original),
                } => original.as_ref() == &request,
                _ => false,
            };
            if !readback_target_matches {
                return Err(format!(
                    "AgentRequest admission has a foreign readback target: {error:?}"
                )
                .into());
            }
            let recovered = client
                .recover_agent_request_admission(bearer, &request)
                .await
                .map_err(|readback_error| {
                    format!(
                        "AgentRequest exact readback failed after {error:?}: {readback_error:?}"
                    )
                })?;
            if recovered.request != request {
                return Err("AgentRequest readback changed the original request".into());
            }
            let evidence = match recovered.snapshot {
                Some(snapshot) => format!("snapshot={snapshot:?}"),
                None => "exact request snapshot absent".to_owned(),
            };
            return Err(format!(
                "AgentRequest admission unresolved: original={error:?}; {evidence}; the snapshot cannot prove the original created flag, so no success is reconstructed and no mutation is retried"
            )
            .into());
        }
        Err(error) => return Err(error.into()),
    };
    assert_eq!(result.request, request);
    assert!(result.receipt.created);
    assert_eq!(result.receipt.request_id, request.request_id);
    assert_eq!(result.receipt.activity_id, target.activity_id);
    let historical = client
        .recover_agent_request_admission(bearer, &request)
        .await?;
    assert_eq!(historical.request, request);
    let persisted = historical
        .snapshot
        .ok_or("AgentRequest admission receipt was not durable")?;
    assert_eq!(persisted.receipt.request_id, request.request_id);
    assert_eq!(persisted.receipt.revision, result.receipt.revision);
    let observed = client
        .agent_request(
            bearer,
            &AgentRequestQueryRequest {
                scope: target.scope.clone(),
                session_id: Some(target.session_id),
                request_id: request.request_id,
            },
        )
        .await?;
    let detail = observed
        .request
        .ok_or("source-scoped AgentRequest was not readable")?;
    assert_eq!(detail.request_id, request.request_id);
    assert_eq!(detail.requested_count, request.count);
    Ok(())
}

struct SourceGoalPlan {
    goal: RegisteredGoal,
    bootstrap_node: RegisteredDagNode,
    mission_node: RegisteredDagNode,
    free_node: RegisteredDagNode,
    dispatch_node: RegisteredDagNode,
}

/// Plan one genuine Forge Goal and initial DAG through its Coordinator-owned
/// control worker. This is source planning truth, never an Operations seed.
fn plan_source_goal(
    control: &ForgeControlClient,
    scope: &ResolvedWorkspaceScope,
    actor: ActivityCoordinatorActor,
) -> TestResult<SourceGoalPlan> {
    let now = TimestampMillis::new(current_test_time()?)?;
    let goal = RegisteredGoal::create(
        GoalId::from_uuid(Uuid::now_v7()),
        scope.workspace_id(),
        actor.activity_id(),
        GoalContract::new(
            GoalObjective::new("Review one source-backed assignment".to_owned())?,
            vec![GoalStatement::new(
                "The reviewer receives a bounded assignment".to_owned(),
            )?],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )?,
        now,
    );
    let saved = control.goal_create(scope.clone(), actor, goal)?;
    let definition = DagNodeDefinition::new(
        DagNodeId::from_uuid(Uuid::now_v7()),
        DagNodeOrdinal::new(0)?,
        DagNodeTitle::new("Review evidence".to_owned())?,
        DagNodeObjective::new("Review one bounded source without execution".to_owned())?,
        vec![DagAcceptanceCriterion::new(
            "Review receipt is readable".to_owned(),
        )?],
    )?;
    let mission_definition = DagNodeDefinition::new(
        DagNodeId::from_uuid(Uuid::now_v7()),
        DagNodeOrdinal::new(1)?,
        DagNodeTitle::new("Mission source review".to_owned())?,
        DagNodeObjective::new("Record a second source-backed review obligation".to_owned())?,
        vec![DagAcceptanceCriterion::new(
            "Mission evidence retains the exact offered assignment".to_owned(),
        )?],
    )?;
    let free_definition = DagNodeDefinition::new(
        DagNodeId::from_uuid(Uuid::now_v7()),
        DagNodeOrdinal::new(2)?,
        DagNodeTitle::new("Unassigned team node".to_owned())?,
        DagNodeObjective::new("Keep one exact source node unassigned for Team binding".to_owned())?,
        vec![DagAcceptanceCriterion::new(
            "Team binding retains its unassigned source node identity".to_owned(),
        )?],
    )?;
    let dispatch_definition = dispatch_bootstrap_node_definition()?;
    let revision = DagRevision::new(
        DagRevisionId::from_uuid(Uuid::now_v7()),
        saved.id(),
        saved.revision(),
        DagRevisionNumber::INITIAL,
        None,
        vec![
            definition.clone(),
            mission_definition.clone(),
            free_definition.clone(),
            dispatch_definition.clone(),
        ],
        Vec::new(),
        now,
    )?;
    let node = RegisteredDagNode::create(saved.id(), revision.number(), definition, now);
    let mission_node =
        RegisteredDagNode::create(saved.id(), revision.number(), mission_definition, now);
    let free_node = RegisteredDagNode::create(saved.id(), revision.number(), free_definition, now);
    let dispatch_node =
        RegisteredDagNode::create(saved.id(), revision.number(), dispatch_definition, now);
    let plan = AgentDagPlanRequest::new(
        AgentDagPlanRequestId::from_uuid(Uuid::now_v7()),
        actor,
        GoalAttribution::new(saved.id(), saved.revision()),
        None,
        None,
        AgentDagDraft::new(
            revision,
            vec![
                node.clone(),
                mission_node.clone(),
                free_node.clone(),
                dispatch_node.clone(),
            ],
            Vec::new(),
        )?,
        AgentWorkUpdateId::from_uuid(Uuid::now_v7()),
        AgentWorkUpdateSequence::INITIAL,
        now,
    );
    let committed = control.agent_dag_plan_commit(AgentDagPlanCommitRequest {
        scope: scope.clone(),
        request: plan,
    })?;
    assert_eq!(committed.revision().revision().goal_id(), saved.id());
    Ok(SourceGoalPlan {
        goal: saved,
        bootstrap_node: node,
        mission_node,
        free_node,
        dispatch_node,
    })
}

fn dispatch_bootstrap_node_definition() -> TestResult<DagNodeDefinition> {
    Ok(DagNodeDefinition::new(
        DagNodeId::from_uuid(Uuid::now_v7()),
        DagNodeOrdinal::new(3)?,
        DagNodeTitle::new("Temporary dispatch Team bootstrap".to_owned())?,
        DagNodeObjective::new(
            "Offer one source-backed assignment to the receiving Team Leader".to_owned(),
        )?,
        vec![DagAcceptanceCriterion::new(
            "Receiving Team leadership retains its exact source offer".to_owned(),
        )?],
    )?)
}

/// Create only the bootstrap offer. It is canceled after its durable O
/// leadership evidence is confirmed, before Mission terms are frozen.
async fn issue_bootstrap_forge_assignment_offer(
    source: &ForgeHost,
    principal: AuthenticatedPrincipal,
    activity_id: Uuid,
    second: &ReadyFixtureMember,
) -> TestResult<SourceAssignmentFixture> {
    let actor = ForgeSourceHttpClient::new(ForgeCommandHttpClient::new(
        source.address,
        FORGE_SECRET,
        DEADLINE,
    )?)
    .coordinator_actor(&source.scope, activity_id, principal)
    .await?
    .ok_or("Forge Coordinator Actor is missing before source assignment")?;
    let control = source.control_client.clone();
    let scope = source.scope.clone();
    let target_session = AgentSessionId::from_uuid(second.session_id);
    let target_agent = AgentInstanceId::from_uuid(second.agent_id);
    let assignment = tokio::task::spawn_blocking(move || {
        (|| -> TestResult<SourceAssignmentFixture> {
            let SourceGoalPlan {
                goal,
                bootstrap_node,
                mission_node,
                free_node,
                dispatch_node,
            } = plan_source_goal(&control, &scope, actor)?;
            let current = control
                .agent_get_context(AgentContextRequest {
                    scope: scope.clone(),
                    activity_id: actor.activity_id(),
                    agent_id: target_agent,
                    now: TimestampMillis::new(current_test_time()?)?,
                    focus_event_limit: AgentListLimit::new(4)?,
                    work_update_limit: AgentListLimit::new(4)?,
                })?
                .ok_or("Forge activated Agent context is absent")?;
            let target = current.attachment().agent();
            assert_eq!(target.session_id(), Some(target_session));
            assert_eq!(target.principal(), principal);
            let bootstrap_assignment_id = AgentAssignmentId::from_uuid(Uuid::now_v7());
            let bootstrap_target = AgentAssignmentTarget::new(
                target.id(),
                target_session,
                principal,
                target.revision(),
            );
            let offered = control.agent_assignment_mutate(AgentAssignmentMutationRequest {
                scope,
                mutation: AgentAssignmentMutation::Offer(OfferAgentAssignment::new(
                    bootstrap_assignment_id,
                    actor,
                    bootstrap_target,
                    goal.id(),
                    goal.revision(),
                    goal.timestamps().created_at(),
                    bootstrap_node.dag_revision(),
                    DagHeadRevision::INITIAL,
                    bootstrap_node.id(),
                    bootstrap_node.revision(),
                    None,
                    TimestampMillis::new(current_test_time()?)?,
                )),
            })?;
            assert_eq!(offered.state(), AgentAssignmentState::Offered);
            assert_eq!(offered.target(), bootstrap_target);
            let unassigned_node = NodeTeamScopeWire {
                activity_id: actor.activity_id().into_uuid(),
                goal_id: goal.id().into_uuid(),
                goal_created_at_ms: goal.timestamps().created_at().get(),
                dag_revision: free_node.dag_revision().get(),
                dag_base_goal_revision: goal.revision().get(),
                node_id: free_node.id().into_uuid(),
            };
            Ok(SourceAssignmentFixture {
                bootstrap_assignment_id,
                bootstrap_target,
                goal,
                mission_node,
                dispatch_node,
                unassigned_node,
            })
        })()
        .map_err(|error| error.to_string())
    })
    .await??;
    Ok(assignment)
}

/// Only a Forge `AssignmentOffered` fact can bootstrap this Workspace's
/// leadership. The operator then confirms that exact receipt through the
/// documented Broker-fenced leadership advance, without supplying an epoch.
async fn source_confirmed_team_leadership(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    creator: &MemberTarget,
    second: &ReadyFixtureMember,
    assignment_id: Uuid,
) -> TestResult<i64> {
    assert!(!assignment_id.is_nil());
    let applied = client
        .agent_coordination_get(
            bearer,
            &CoordinationQueryRequest {
                scope: creator.scope.clone(),
                activity_id: creator.activity_id,
                session_id: Some(creator.session_id),
            },
        )
        .await?;
    let catchup = applied
        .catch_up
        .ok_or("Forge effect source cursor unavailable")?;
    assert!(catchup.page_size > 0, "AssignmentOffered was not ingested");
    assert!(catchup.applied_through >= 1);
    let scope = LeadershipQueryRequest {
        scope: creator.scope.clone(),
        activity_id: None,
        team_id: None,
        session_id: Some(second.session_id),
        review_after: None,
        limit: 10,
    };
    let bootstrap = client
        .leadership_view(bearer, &scope)
        .await?
        .leadership
        .ok_or("AssignmentOffered did not bootstrap Workspace leadership")?;
    assert_eq!(bootstrap.state, "bootstrapping");
    assert_eq!(bootstrap.leader_participant_id, Some(second.participant_id));
    let advanced = client
        .advance_leadership(
            bearer,
            &LeadershipAdvanceRequest {
                target: LeadershipLifecycleTarget {
                    scope: creator.scope.clone(),
                    activity_id: None,
                    team_id: None,
                    session_id: second.session_id,
                },
            },
        )
        .await?;
    assert!(
        advanced.advanced,
        "Workspace Bootstrap evidence was not confirmed"
    );
    let confirmed = advanced
        .leadership
        .ok_or("Workspace leadership disappeared after confirm")?;
    assert!(matches!(confirmed.state.as_str(), "stabilizing" | "active"));
    assert_eq!(
        confirmed.leader_participation_id,
        Some(second.participant_id)
    );
    let readback = client
        .leadership_view(bearer, &scope)
        .await?
        .leadership
        .ok_or("Durable leadership readback missing")?;
    assert_eq!(readback.leadership_revision, confirmed.leadership_revision);
    assert_eq!(
        readback.leader_participation_id,
        confirmed.leader_participation_id
    );
    Ok(confirmed.leadership_revision)
}

/// Exact fixture state needed to cancel its now-unbound bootstrap offer.
struct BootstrapOfferCancellationCase<'a> {
    client: &'a OrchestrationHttpClient,
    bearer: &'a HeaderValue,
    source: &'a ForgeHost,
    creator: &'a MemberTarget,
    leader: &'a ReadyFixtureMember,
    leadership_revision: i64,
    assignment: &'a SourceAssignmentFixture,
    principal: AuthenticatedPrincipal,
}

/// Cancel the now-unbound bootstrap-only offer through the current Forge
/// Coordinator after its `AssignmentOffered` fact has established legacy O
/// leadership. An Offered assignment cancels terminally without target ACK.
async fn cancel_bootstrap_offer_after_leadership(
    case: &BootstrapOfferCancellationCase<'_>,
) -> TestResult {
    let commands = ForgeCommandHttpClient::new(case.source.address, FORGE_SECRET, DEADLINE)?;
    let coordinator = ForgeSourceHttpClient::new(commands)
        .coordinator_actor(&case.source.scope, case.creator.activity_id, case.principal)
        .await?
        .ok_or("current Forge Coordinator is missing before bootstrap-offer cancellation")?;
    assert_ne!(coordinator.agent_id().into_uuid(), case.leader.agent_id);
    assert_ne!(coordinator.session_id().into_uuid(), case.leader.session_id);

    cancel_bootstrap_assignment(case, coordinator).await?;
    assert_bootstrap_leadership_retained(case).await
}

async fn cancel_bootstrap_assignment(
    case: &BootstrapOfferCancellationCase<'_>,
    coordinator: ActivityCoordinatorActor,
) -> TestResult {
    let scope = case.source.scope.clone();
    let assignment_id = case.assignment.bootstrap_assignment_id;
    let target = case.assignment.bootstrap_target;
    let cancel_at = TimestampMillis::new(current_test_time()?)?;
    let control = case.source.control_client.clone();
    let (offered, cancelled, readback) = tokio::task::spawn_blocking(move || {
        let get_request = AgentAssignmentGetRequest {
            scope: scope.clone(),
            activity_id: coordinator.activity_id(),
            assignment_id,
            authority: AgentAssignmentReadAuthority {
                session_id: target.session_id(),
                agent_id: target.agent_id(),
                expected_agent_revision: target.agent_revision(),
                principal: target.principal(),
            },
        };
        let offered = control
            .agent_assignment_get(get_request.clone())
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "bootstrap Assignment was absent before cancellation".to_owned())?;
        if offered.state() != AgentAssignmentState::Offered
            || offered.target() != target
            || offered.cancellation_requested()
        {
            return Err(format!(
                "bootstrap Assignment state drifted before cancellation: {:?}",
                offered.state()
            ));
        }
        let expected_revision = offered
            .revision()
            .checked_next()
            .map_err(|error| error.to_string())?;
        let command = CancelAgentAssignment::new(
            assignment_id,
            coordinator,
            offered.revision(),
            cancel_at,
        );
        let cancel_result = control
            .agent_assignment_mutate(AgentAssignmentMutationRequest {
                scope: scope.clone(),
                mutation: AgentAssignmentMutation::Cancel(command),
            })
            .map_err(|error| error.to_string());
        let cancelled = match cancel_result {
            Ok(cancelled) => cancelled,
            Err(mutation_error) => {
                let observed = control
                    .agent_assignment_get(get_request.clone())
                    .map_err(|error| error.to_string());
                match observed {
                    Ok(Some(assignment))
                        if assignment.id() == assignment_id
                            && assignment.target() == target
                            && assignment.state() == AgentAssignmentState::Cancelled
                            && assignment.cancellation_requested()
                            && assignment.revision() == expected_revision =>
                    {
                        assignment
                    }
                    other => {
                        return Err(format!(
                            "bootstrap offer cancellation was not confirmed; mutation={mutation_error:?}, readback={other:?}"
                        ));
                    }
                }
            }
        };
        assert_eq!(cancelled.state(), AgentAssignmentState::Cancelled);
        assert!(cancelled.cancellation_requested());
        assert!(cancelled.result().is_none());
        assert_eq!(cancelled.revision(), expected_revision);
        let readback = control
            .agent_assignment_get(get_request)
            .map_err(|error| error.to_string())?;
        assert_eq!(readback.as_ref(), Some(&cancelled));
        Ok::<_, String>((offered, cancelled, readback))
    })
    .await??;
    assert_eq!(offered.state(), AgentAssignmentState::Offered);
    assert_eq!(cancelled.state(), AgentAssignmentState::Cancelled);
    assert_eq!(readback.as_ref(), Some(&cancelled));
    Ok(())
}

async fn assert_bootstrap_leadership_retained(
    case: &BootstrapOfferCancellationCase<'_>,
) -> TestResult {
    let leadership = case
        .client
        .leadership_view(
            case.bearer,
            &LeadershipQueryRequest {
                scope: case.creator.scope.clone(),
                session_id: Some(case.leader.session_id),
                activity_id: None,
                team_id: None,
                review_after: None,
                limit: 10,
            },
        )
        .await?
        .leadership
        .ok_or("Workspace leadership disappeared after bootstrap-offer cancellation")?;
    assert_eq!(
        leadership.leader_participant_id,
        Some(case.leader.participant_id)
    );
    assert_eq!(leadership.leadership_revision, case.leadership_revision);
    Ok(())
}

async fn assert_mission_assignment_remains_offered(
    source: &ForgeHost,
    mission_assignment: MissionAssignmentSource,
    activity_id: Uuid,
) -> TestResult {
    let scope = source.scope.clone();
    let control = source.control_client.clone();
    let assignment = tokio::task::spawn_blocking(move || {
        control.agent_assignment_get(AgentAssignmentGetRequest {
            scope,
            activity_id: forge_domain::ActivityId::from_uuid(activity_id),
            assignment_id: mission_assignment.assignment_id,
            authority: AgentAssignmentReadAuthority {
                session_id: mission_assignment.target.session_id(),
                agent_id: mission_assignment.target.agent_id(),
                expected_agent_revision: mission_assignment.target.agent_revision(),
                principal: mission_assignment.target.principal(),
            },
        })
    })
    .await??
    .ok_or("Mission source assignment disappeared after evidence cut")?;
    assert_eq!(assignment.state(), AgentAssignmentState::Offered);
    assert!(!assignment.cancellation_requested());
    assert!(assignment.result().is_none());
    assert_eq!(assignment.target(), mission_assignment.target);
    assert_eq!(
        assignment.revision(),
        forge_domain::AgentAssignmentRevision::INITIAL
    );
    Ok(())
}

/// Form three actual Ready members with the current leader's source revision,
/// then inspect the immutable creation receipt, roster and Workspace list.
async fn assert_real_three_member_team(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    first: &MemberTarget,
    second: &ReadyFixtureMember,
    third: &ReadyFixtureMember,
    expected_sender_leadership_revision: i64,
) -> TestResult<Uuid> {
    let request = TeamCreateV2Request {
        scope: first.scope.clone(),
        request_id: Uuid::now_v7(),
        team_id: Uuid::now_v7(),
        activity_id: first.activity_id,
        session_id: second.session_id,
        expected_sender_leadership_revision,
        name: "Source evidence review".to_owned(),
        purpose: "Three real members review a bounded source-backed obligation".to_owned(),
        initial_members: vec![
            TeamRosterMemberRequest {
                activity_id: first.activity_id,
                session_id: first.session_id,
                role: "planner".to_owned(),
            },
            TeamRosterMemberRequest {
                activity_id: first.activity_id,
                session_id: second.session_id,
                role: "reviewer".to_owned(),
            },
            TeamRosterMemberRequest {
                activity_id: first.activity_id,
                session_id: third.session_id,
                role: "reviewer".to_owned(),
            },
        ],
        max_members: 3,
    };
    let created = client
        .create_team_v2(bearer, &request)
        .await
        .map_err(|error| {
            format!(
                "bootstrap H TeamCreateV2 mutation request_id={}: {error:?}",
                request.request_id
            )
        })?;
    assert_team_create_v2_response(&created, &request, second, third)?;
    assert_team_create_v2_readback(client, bearer, &request, &created).await?;

    let readback = client
        .team_get(
            bearer,
            &TeamGetQueryRequest {
                scope: first.scope.clone(),
                session_id: Some(second.session_id),
                team_id: request.team_id,
                request_id: None,
            },
        )
        .await
        .map_err(|error| {
            format!(
                "bootstrap H current Team/roster snapshot team_id={}: {error:?}",
                request.team_id
            )
        })?;
    assert_eq!(readback.team.as_ref(), Some(&created.mutation.team));
    assert_eq!(readback.members.len(), 3);
    assert!(!readback.members_truncated);
    assert!(readback.members.iter().any(|member| {
        member.profile_id == Some(third.profile_id)
            && !member.participation_id.is_nil()
            && member.state == "joined"
    }));
    assert_stale_team_leadership_rejected(client, bearer, &request).await?;
    let listing = client
        .team_list(
            bearer,
            &TeamListQueryRequest {
                scope: first.scope.clone(),
                session_id: Some(second.session_id),
                limit: 10,
            },
        )
        .await
        .map_err(|error| {
            format!(
                "bootstrap H current Team list query workspace_id={}: {error:?}",
                first.scope.workspace_id
            )
        })?;
    assert_eq!(listing.teams.len(), 1);
    assert_eq!(listing.teams[0].team_id, request.team_id);
    Ok(request.team_id)
}

fn assert_team_create_v2_response(
    created: &TeamMutationV2Response,
    request: &TeamCreateV2Request,
    creator: &ReadyFixtureMember,
    third: &ReadyFixtureMember,
) -> TestResult {
    assert!(created.mutation.created);
    assert!(!created.mutation.replayed);
    assert_eq!(created.mutation.team.team_id, request.team_id);
    assert_eq!(created.mutation.initial_members.len(), 3);
    let capacity = created
        .mutation
        .capacity
        .as_ref()
        .ok_or("v2 Team create omitted its cap receipt")?;
    assert_eq!(capacity.team_id, request.team_id);
    assert_eq!(capacity.workspace_id, request.scope.workspace_id);
    assert_eq!(capacity.max_members, request.max_members);
    assert_eq!(capacity.revision, 1);
    assert_eq!(
        capacity.leadership_revision,
        request.expected_sender_leadership_revision
    );
    assert_eq!(capacity.actor.activity_id, request.activity_id);
    assert_eq!(capacity.actor.agent_id, creator.agent_id);
    assert_eq!(capacity.actor.session_id, request.session_id);
    assert_eq!(capacity.actor.participant_id, creator.participant_id);
    assert_eq!(capacity.actor.profile_id, creator.profile_id);
    let creator_membership = created
        .mutation
        .initial_members
        .iter()
        .find(|member| member.profile_id == Some(creator.profile_id))
        .ok_or("Team creator's initial membership receipt is absent")?;
    assert_eq!(
        capacity.actor.participation_id,
        creator_membership.participation_id
    );
    assert!(created.mutation.expansion_slot.is_none());
    assert!(created.mutation.expansion_join_source.is_none());
    assert!(created.mutation.initial_members.iter().any(|member| {
        member.profile_id == Some(third.profile_id)
            && !member.participation_id.is_nil()
            && member.state == "joined"
    }));
    let digest = created
        .mutation
        .initial_roster_digest_v1
        .as_ref()
        .ok_or("v2 Team create omitted its roster digest")?;
    assert_eq!(digest.len(), 64);
    Ok(())
}

async fn assert_team_create_v2_readback(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    request: &TeamCreateV2Request,
    created: &TeamMutationV2Response,
) -> TestResult {
    let source_receipt = client
        .team_create_mutation_readback_v2(
            bearer,
            request,
            TeamMutationReaderV2Wire {
                activity_id: request.activity_id,
                session_id: request.session_id,
            },
        )
        .await
        .map_err(|error| {
            format!(
                "bootstrap H TeamCreateV2 owner readback request_id={}: {error:?}",
                request.request_id
            )
        })?
        .ok_or("owner-scoped v2 Team create receipt is absent")?;
    let mut expected = created.mutation.clone();
    expected.replayed = true;
    assert_eq!(source_receipt.mutation, expected);
    Ok(())
}

/// A different Team request cannot borrow the current leader's identity
/// while submitting a different Workspace leadership revision.
async fn assert_stale_team_leadership_rejected(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    original: &TeamCreateV2Request,
) -> TestResult {
    let incorrect = TeamCreateV2Request {
        request_id: Uuid::now_v7(),
        team_id: Uuid::now_v7(),
        expected_sender_leadership_revision: original
            .expected_sender_leadership_revision
            .checked_add(1)
            .ok_or("leadership revision overflow")?,
        ..original.clone()
    };
    let refused = client.create_team_v2(bearer, &incorrect).await;
    assert!(
        matches!(
            refused,
            Err(ClientError::Rejected { status, .. })
                if status == StatusCode::CONFLICT.as_u16()
        ),
        "TeamCreateV2 stale-leadership rejection request_id={}: {refused:?}",
        incorrect.request_id
    );
    assert!(
        client
            .team_create_mutation_readback_v2(
                bearer,
                &incorrect,
                TeamMutationReaderV2Wire {
                    activity_id: incorrect.activity_id,
                    session_id: incorrect.session_id,
                },
            )
            .await
            .map_err(|error| {
                format!(
                    "TeamCreateV2 stale-request owner readback request_id={}: {error:?}",
                    incorrect.request_id
                )
            })?
            .is_none(),
        "stale v2 Team create produced an owner-scoped receipt"
    );
    Ok(())
}

struct SourceLeadershipTeamCase<'a> {
    client: &'a OrchestrationHttpClient,
    bearer: &'a HeaderValue,
    source: &'a ForgeHost,
    creator: &'a MemberTarget,
    leader: &'a ReadyFixtureMember,
    third: &'a ReadyFixtureMember,
    assignments: &'a SourceAssignmentFixture,
    principal: AuthenticatedPrincipal,
}

struct SourceLeadershipTeamOutcome {
    approved: ApprovedExecutionFixture,
    evidence: mission_evidence::MissionEvidenceRecoveryFixture,
    mission_assignment: MissionAssignmentSource,
    team_id: Uuid,
    workspace_leadership_revision: i64,
}

async fn assert_team_from_source_leadership(
    case: &SourceLeadershipTeamCase<'_>,
) -> TestResult<SourceLeadershipTeamOutcome> {
    let revision = source_confirmed_team_leadership(
        case.client,
        case.bearer,
        case.creator,
        case.leader,
        case.assignments.bootstrap_assignment_id.into_uuid(),
    )
    .await?;
    let cancellation_case = BootstrapOfferCancellationCase {
        client: case.client,
        bearer: case.bearer,
        source: case.source,
        creator: case.creator,
        leader: case.leader,
        leadership_revision: revision,
        assignment: case.assignments,
        principal: case.principal,
    };
    cancel_bootstrap_offer_after_leadership(&cancellation_case).await?;
    let team_id = assert_real_three_member_team(
        case.client,
        case.bearer,
        case.creator,
        case.leader,
        case.third,
        revision,
    )
    .await?;
    let mission_assignment_id = temporary_dispatch_lifecycle::offer_team_affined_assignment(
        &temporary_dispatch_lifecycle::TeamAffinedOfferContext {
            client: case.client,
            bearer: case.bearer,
            source: case.source,
            creator: case.creator,
            workspace_leader: case.leader,
            workspace_leadership_revision: revision,
            principal: case.principal,
            team_id,
            target: case.third,
            assignments: case.assignments,
            node: &case.assignments.mission_node,
        },
    )
    .await?;
    temporary_dispatch_lifecycle::confirm_team_leadership(
        case.client,
        case.bearer,
        case.creator,
        case.third,
        team_id,
    )
    .await?;
    let evidence = record_mission_evidence(case, revision, team_id, &mission_assignment_id).await?;
    let approved = assert_team_observations_and_approval(case, revision, team_id).await?;
    Ok(SourceLeadershipTeamOutcome {
        approved,
        evidence,
        mission_assignment: mission_assignment_id,
        team_id,
        workspace_leadership_revision: revision,
    })
}

async fn record_mission_evidence(
    case: &SourceLeadershipTeamCase<'_>,
    leadership_revision: i64,
    team_id: Uuid,
    assignment: &MissionAssignmentSource,
) -> TestResult<mission_evidence::MissionEvidenceRecoveryFixture> {
    let selectors = mission_evaluation::assert_mission_evaluation_http_flow(
        case.client,
        case.bearer,
        case.creator,
        case.leader,
        case.third,
        leadership_revision,
        team_id,
    )
    .await?;
    mission_t2_checkpoint::assert_mission_t2_checkpoint_http_flow(
        case.client,
        case.bearer,
        case.creator,
        case.leader,
        case.third,
        leadership_revision,
        &selectors,
    )
    .await?;
    let flow = mission_evidence::MissionEvidenceFlow {
        client: case.client,
        bearer: case.bearer,
        source: case.source,
        target: case.creator,
        leader: case.leader,
        assignee: case.third,
        leadership_revision,
        mission_id: selectors.mission_id,
        plan_id: selectors.plan_id,
        plan_revision: selectors.plan_revision,
        obligation_id: selectors.obligation_id,
        work_unit_id: selectors.work_unit_id,
        assignment_id: assignment.assignment_id.into_uuid(),
        t1_submission_cutoff_ms: selectors.t1_submission_cutoff_ms,
    };
    let evidence = mission_evidence::assert_mission_evidence_http_flow(&flow).await?;
    assert_mission_assignment_remains_offered(case.source, *assignment, case.creator.activity_id)
        .await?;
    Ok(evidence)
}

async fn assert_team_observations_and_approval(
    case: &SourceLeadershipTeamCase<'_>,
    leadership_revision: i64,
    team_id: Uuid,
) -> TestResult<ApprovedExecutionFixture> {
    assert_node_team_binding(
        case.client,
        case.bearer,
        case.creator,
        case.leader,
        team_id,
        leadership_revision,
        case.assignments.unassigned_node,
    )
    .await?;
    assert_unprovisioned_team_node_chat(
        case.client,
        case.bearer,
        case.creator,
        case.leader,
        case.assignments.unassigned_node,
    )
    .await?;
    assert_scoped_approval_policy(
        case.client,
        case.bearer,
        case.source,
        case.creator,
        case.leader.session_id,
    )
    .await
}

/// A Ready Team with a genuine Forge Node still has no Provider conversation
/// until a real Runtime/Participation destination is provisioned. Never
/// substitute an invented conversation ID or claim message delivery.
async fn assert_unprovisioned_team_node_chat(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    sender: &MemberTarget,
    leader: &ReadyFixtureMember,
    node: NodeTeamScopeWire,
) -> TestResult {
    let query = NodeChatQueryRequest {
        scope: sender.scope.clone(),
        session_id: leader.session_id,
        sender_activity_id: sender.activity_id,
        query: ChatQueryWire {
            workspace_id: sender.scope.workspace_id,
            agent_profile_id: None,
            runtime_session_id: None,
            node_scope: Some(node),
            before_sequence: None,
        },
    };
    let observed = client.node_chat_read(bearer, &query).await?;
    assert_eq!(observed.request, query);
    assert_eq!(observed.snapshot.workspace_id, sender.scope.workspace_id);
    assert_eq!(observed.snapshot.node_scope, Some(node));
    assert!(observed.snapshot.targets.is_empty());
    assert!(observed.snapshot.target.is_none());
    assert!(observed.snapshot.messages.is_empty());
    assert!(observed.snapshot.unavailable_reason.is_some());
    Ok(())
}

/// This tests real Workspace approval-policy CAS and readback, not approval
/// issuance or executable provider authority.
async fn assert_scoped_approval_policy(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    source: &ForgeHost,
    owner: &MemberTarget,
    session_id: Uuid,
) -> TestResult<ApprovedExecutionFixture> {
    let query = ApprovalPolicyQueryRequest {
        scope: owner.scope.clone(),
        session_id: Some(session_id),
    };
    assert!(client.approval_policy(bearer, &query).await?.is_none());
    let set = ApprovalPolicySetRequest {
        scope: owner.scope.clone(),
        session_id,
        enabled: true,
        auto_scopes: Vec::new(),
        expected_revision: None,
    };
    let policy = client.set_approval_policy(bearer, &set).await?;
    assert_eq!(policy.workspace_id, owner.scope.workspace_id);
    assert!(policy.enabled);
    assert_eq!(policy.revision, 1);
    assert!(policy.auto_scopes.is_empty());
    assert_eq!(
        client.approval_policy(bearer, &query).await?,
        Some(policy.clone())
    );
    let stale = client
        .set_approval_policy(
            bearer,
            &ApprovalPolicySetRequest {
                expected_revision: Some(2),
                ..set
            },
        )
        .await;
    assert!(
        matches!(
            stale,
            Err(ClientError::Rejected { status, .. })
                if status == StatusCode::CONFLICT.as_u16()
        ),
        "stale approval policy revision must fail closed: {stale:?}"
    );
    assert_eq!(client.approval_policy(bearer, &query).await?, Some(policy));
    assert_approval_request_decisions(client, bearer, source, owner, session_id).await
}

/// Construct the same shell shape interpreted by Forge's authenticated
/// execution.begin API, using Forge's own bounded shell analysis. This creates
/// only a typed request; no command is started and no grant is consumed.
fn fixture_forge_execution_request(
    target: &MemberTarget,
    script: &str,
) -> TestResult<ExecutionRequest> {
    let permissions = forge_domain::ExecutionPermissions::new(false, false, false);
    Ok(ExecutionRequest::new(
        ExecutionAttribution::new(
            ExecutionId::from_uuid(Uuid::now_v7()),
            WorkspaceId::from_uuid(target.scope.workspace_id),
            ActivityId::from_uuid(target.activity_id),
        ),
        ExecutionKind::Shell {
            script: forge_domain::ExecutionScript::new(script.to_owned())?,
        },
        None,
        ExecutionLimits::new(
            ExecutionTimeoutMillis::new(10 * 60 * 1_000)?,
            ExecutionOutputLimit::DEFAULT,
        ),
        permissions,
    )?)
}

fn official_execution_digest(request: &ExecutionRequest) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in forge_execution::execution_request_digest(request).as_bytes() {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 15)]));
    }
    output
}

fn fixture_approval_request(
    owner: &MemberTarget,
    session_id: Uuid,
    execution_request: &ExecutionRequest,
) -> ApprovalSubmitRequest {
    let permissions = execution_request.permissions();
    ApprovalSubmitRequest {
        scope: owner.scope.clone(),
        session_id,
        request_id: Uuid::now_v7(),
        action: "analyzed_shell".to_owned(),
        request_digest: official_execution_digest(execution_request),
        requested_permissions: ApprovalPermissionWire {
            workspace_read: permissions.workspace_read(),
            workspace_write: permissions.workspace_write(),
            network: permissions.network(),
        },
        ttl_ms: 60_000,
    }
}

async fn submit_and_prove_pending_approval(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    request: &ApprovalSubmitRequest,
) -> TestResult<ApprovalSubmitResponse> {
    let submitted = client.submit_approval(bearer, request).await?;
    assert!(!submitted.replay);
    assert_eq!(submitted.request.request_id, request.request_id);
    assert_eq!(submitted.request.request_digest, request.request_digest);
    assert_eq!(submitted.request.session_id, request.session_id);
    assert_eq!(submitted.request.state, "pending");
    assert_eq!(submitted.request.revision, 1);
    assert!(submitted.admission.is_none());
    let snapshot = client
        .approval_request(
            bearer,
            &ApprovalRequestGetRequest {
                scope: request.scope.clone(),
                session_id: Some(request.session_id),
                request_id: request.request_id,
            },
        )
        .await?;
    assert_eq!(snapshot.request, Some(submitted.request.clone()));
    assert!(snapshot.admission.is_none());
    let replayed = client.submit_approval(bearer, request).await?;
    assert!(replayed.replay);
    assert_eq!(replayed.request, submitted.request);
    assert!(replayed.admission.is_none());
    let conflicting = client
        .submit_approval(
            bearer,
            &ApprovalSubmitRequest {
                action: "direct_structured".to_owned(),
                ..request.clone()
            },
        )
        .await;
    assert!(
        matches!(
            conflicting,
            Err(ClientError::Rejected { status, .. })
                if status == StatusCode::CONFLICT.as_u16()
        ),
        "reused approval ID with another action must fail: {conflicting:?}"
    );
    Ok(submitted)
}

/// The operator decision is a separate principal-owned Session operation.
/// Rejection must never create an admission envelope or allow re-decision.
async fn assert_rejected_approval(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    request: &ApprovalSubmitRequest,
) -> TestResult {
    submit_and_prove_pending_approval(client, bearer, request).await?;
    let decision = ApprovalDecideRequest {
        scope: request.scope.clone(),
        session_id: request.session_id,
        request_id: request.request_id,
        decision: "reject".to_owned(),
    };
    let rejected = client.decide_approval(bearer, &decision).await?;
    assert_eq!(rejected.request.state, "rejected");
    assert_eq!(rejected.request.revision, 2);
    assert_eq!(rejected.request.decided_by, Some(request.session_id));
    assert!(rejected.admission.is_none());
    let settled = client
        .approval_request(
            bearer,
            &ApprovalRequestGetRequest {
                scope: request.scope.clone(),
                session_id: Some(request.session_id),
                request_id: request.request_id,
            },
        )
        .await?;
    assert_eq!(settled.request, Some(rejected.request.clone()));
    assert!(settled.admission.is_none());
    let rejection_replay = client.submit_approval(bearer, request).await?;
    assert!(rejection_replay.replay);
    assert_eq!(rejection_replay.request, rejected.request);
    assert!(rejection_replay.admission.is_none());
    let repeated = client.decide_approval(bearer, &decision).await;
    assert!(
        matches!(repeated, Err(ClientError::Rejected { status, .. })
            if status == StatusCode::CONFLICT.as_u16()),
        "repeated decision must not alter terminal rejection: {repeated:?}"
    );
    Ok(())
}

async fn assert_approved_approval(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    request: &ApprovalSubmitRequest,
) -> TestResult<ApprovalAdmissionWire> {
    submit_and_prove_pending_approval(client, bearer, request).await?;
    let approved = client
        .decide_approval(
            bearer,
            &ApprovalDecideRequest {
                scope: request.scope.clone(),
                session_id: request.session_id,
                request_id: request.request_id,
                decision: "approve".to_owned(),
            },
        )
        .await?;
    assert_eq!(approved.request.state, "approved");
    assert_eq!(approved.request.revision, 2);
    assert_eq!(approved.request.decided_by, Some(request.session_id));
    let issuance = approved
        .admission
        .ok_or("approved request had no issuance metadata")?;
    assert_eq!(issuance.request_id, request.request_id);
    assert_eq!(issuance.workspace_id, request.scope.workspace_id);
    assert!(issuance.expires_at_ms > issuance.issued_at_ms);
    let readback = client
        .approval_request(
            bearer,
            &ApprovalRequestGetRequest {
                scope: request.scope.clone(),
                session_id: Some(request.session_id),
                request_id: request.request_id,
            },
        )
        .await?;
    assert_eq!(readback.request, Some(approved.request.clone()));
    assert_eq!(readback.admission, Some(issuance.clone()));
    let approval_replay = client.submit_approval(bearer, request).await?;
    assert!(approval_replay.replay);
    assert_eq!(approval_replay.request, approved.request);
    assert_eq!(approval_replay.admission, Some(issuance.clone()));
    let repeated = client
        .decide_approval(
            bearer,
            &ApprovalDecideRequest {
                scope: request.scope.clone(),
                session_id: request.session_id,
                request_id: request.request_id,
                decision: "approve".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(
            repeated,
            Err(ClientError::Rejected { status, .. })
                if status == StatusCode::CONFLICT.as_u16()
        ),
        "repeated approve must not issue another grant: {repeated:?}"
    );
    Ok(issuance)
}

struct ApprovedExecutionFixture {
    request: ExecutionRequest,
    admission: ApprovalAdmissionWire,
    script: String,
    project_command: Option<forge_domain::ProjectCommandEvidence>,
}

async fn assert_approval_request_decisions(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    source: &ForgeHost,
    owner: &MemberTarget,
    session_id: Uuid,
) -> TestResult<ApprovedExecutionFixture> {
    let denied_request = fixture_forge_execution_request(owner, "printf 'source-e2e-denied\\n'")?;
    let denied_digest = official_execution_digest(&denied_request);
    assert_eq!(
        denied_digest,
        official_execution_digest(&fixture_forge_execution_request(
            owner,
            "printf 'source-e2e-denied\\n'",
        )?)
    );
    assert_ne!(
        denied_digest,
        official_execution_digest(&fixture_forge_execution_request(
            owner,
            "printf 'source-e2e-denied-with-changed-content\\n'",
        )?)
    );
    let reject = fixture_approval_request(owner, session_id, &denied_request);
    assert_rejected_approval(client, bearer, &reject).await?;
    let (approved_request, script, project_command) = if source.verified_ci {
        let (request, evidence) =
            p10::discover_project_command(source, ActivityId::from_uuid(owner.activity_id)).await?;
        (request, String::new(), Some(evidence))
    } else {
        let request = fixture_forge_execution_request(owner, "printf 'source-e2e-approved\\n'")?;
        let ExecutionKind::Shell { script } = request.kind() else {
            return Err("Fixture must use analyzed shell".into());
        };
        (request.clone(), script.as_str().to_owned(), None)
    };
    let approved_digest = official_execution_digest(&approved_request);
    assert_ne!(denied_digest, approved_digest);
    let mut approve = fixture_approval_request(owner, session_id, &approved_request);
    if project_command.is_some() {
        "discovered_project_command".clone_into(&mut approve.action);
    }
    assert_ne!(reject.request_id, approve.request_id);
    let admission = assert_approved_approval(client, bearer, &approve).await?;
    let requests = client
        .approval_requests(
            bearer,
            &ApprovalListQueryRequest {
                scope: owner.scope.clone(),
                session_id: Some(session_id),
                state: None,
                limit: 10,
            },
        )
        .await?;
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .any(|value| value.request_id == reject.request_id && value.state == "rejected")
    );
    assert!(
        requests
            .iter()
            .any(|value| value.request_id == approve.request_id && value.state == "approved")
    );
    Ok(ApprovedExecutionFixture {
        request: approved_request,
        admission,
        script,
        project_command,
    })
}

/// Bind an actually unassigned Forge-owned DAG node to the real two-member
/// Team with the current Workspace leader's exact source-backed revision.
async fn assert_node_team_binding(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    first: &MemberTarget,
    leader: &ReadyFixtureMember,
    team_id: Uuid,
    sender_leadership_revision: i64,
    node_scope: NodeTeamScopeWire,
) -> TestResult {
    let query = NodeTeamQueryRequest {
        scope: first.scope.clone(),
        session_id: leader.session_id,
        sender_activity_id: first.activity_id,
        node_scope,
        after_assignment_id: None,
    };
    let before = client.node_team_read(bearer, &query).await?;
    assert!(before.snapshot.binding.is_none());
    let set = client
        .node_team_set(
            bearer,
            &NodeTeamSetRequest {
                scope: query.scope.clone(),
                session_id: query.session_id,
                sender_activity_id: query.sender_activity_id,
                node_scope,
                expected_revision: None,
                team_id: Some(team_id),
                sender_leadership_revision,
            },
        )
        .await?;
    assert_eq!(set.binding.team_id, Some(team_id));
    assert_eq!(set.binding.revision, 1);
    assert_eq!(set.binding.bound_by, "orchestrator");
    let readback = client.node_team_read(bearer, &query).await?;
    assert_eq!(readback.snapshot.binding, Some(set.binding));
    Ok(())
}

struct SourceExecutionPermit {
    approval_id: Uuid,
    grant_id: Uuid,
    grant_revision: i64,
    agent_revision: i64,
    node_revision: i64,
    command_id: String,
}

fn focus_source_execution_agent(
    control: &ForgeControlClient,
    scope: &ResolvedWorkspaceScope,
    actor: ActivityCoordinatorActor,
    principal: AuthenticatedPrincipal,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    node: NodeTeamScopeWire,
) -> TestResult<(GoalNodeAttribution, forge_domain::AgentInstanceRevision)> {
    let now = TimestampMillis::new(current_test_time()?)?;
    let goal_id = GoalId::from_uuid(node.goal_id);
    let goal_revision = GoalRevision::new(node.dag_base_goal_revision)?;
    let current_node = control.dag_node_start(DagNodeStartRequest {
        scope: scope.clone(),
        activity_id: actor.activity_id(),
        session_id,
        principal,
        goal_id,
        expected_goal_revision: goal_revision,
        expected_dag_revision: DagRevisionNumber::new(node.dag_revision)?,
        node_id: DagNodeId::from_uuid(node.node_id),
        expected_revision: DagNodeRevision::INITIAL,
        observed_at: now,
    })?;
    let agent = control
        .agent_get_context(AgentContextRequest {
            scope: scope.clone(),
            activity_id: actor.activity_id(),
            agent_id,
            now,
            focus_event_limit: AgentListLimit::new(4)?,
            work_update_limit: AgentListLimit::new(4)?,
        })?
        .ok_or("source Agent context missing before execution")?;
    let focused = control.agent_set_focus(AgentFocusRequest {
        scope: scope.clone(),
        activity_id: actor.activity_id(),
        actor,
        agent_id,
        expected_revision: agent.attachment().agent().revision(),
        focus: AgentFocus::new(
            Some(GoalAttribution::new(goal_id, goal_revision)),
            Some(current_node.dag_revision()),
            Some(AgentFocusNode::new(
                current_node.id(),
                current_node.revision(),
            )),
        )?,
        observed_at: now,
    })?;
    Ok((
        GoalNodeAttribution::new(
            goal_id,
            goal_revision,
            current_node.dag_revision(),
            current_node.id(),
            current_node.revision(),
        ),
        focused.revision(),
    ))
}

/// Forge must independently reserve the same exact Goal/Node/Agent request
/// and issue its own one-shot grant; the Orchestration decision alone cannot
/// bypass Forge's local approval/Agent/Activity ownership checks.
async fn prepare_forge_execution_permit(
    source: &ForgeHost,
    principal: AuthenticatedPrincipal,
    target: &MemberTarget,
    first: &ReadyFixtureMember,
    service: ForgeExecutionService,
    node: NodeTeamScopeWire,
    approved: &ApprovedExecutionFixture,
) -> TestResult<SourceExecutionPermit> {
    let actor = ForgeSourceHttpClient::new(ForgeCommandHttpClient::new(
        source.address,
        FORGE_SECRET,
        DEADLINE,
    )?)
    .coordinator_actor(&source.scope, target.activity_id, principal)
    .await?
    .ok_or("missing genuine Forge Coordinator for local execution")?;
    let scope = source.scope.clone();
    let control = source.control_client.clone();
    let approval = source.approval.client();
    let request = approved.request.clone();
    let script = approved.script.clone();
    let project_command = approved.project_command;
    let agent_id = AgentInstanceId::from_uuid(first.agent_id);
    let agent_session = AgentSessionId::from_uuid(first.session_id);
    let prepared = tokio::task::spawn_blocking(move || {
        (|| -> TestResult<SourceExecutionPermit> {
            let (attribution, agent_revision) = focus_source_execution_agent(
                &control,
                &scope,
                actor,
                principal,
                agent_id,
                agent_session,
                node,
            )?;
            let (command_id, submission) = p10::source_submission(
                scope.clone(),
                actor.activity_id(),
                service.activity_runtime,
                request.clone(),
                &script,
                project_command,
            )?;
            let authority = AgentExecutionAuthorityRequest {
                session_id: agent_session,
                scope: scope.clone(),
                activity_id: actor.activity_id(),
                agent_id,
                expected_revision: agent_revision,
                principal,
                attribution,
                command_id: command_id.clone(),
            };
            let submitted = approval.submit_execution(
                submission
                    .with_goal_node(attribution)
                    .with_agent_authority(authority),
            )?;
            assert_eq!(submitted.status(), ApprovalSubmissionStatus::Pending);
            let pending = control
                .approval_get(scope.clone(), submitted.approval_id())?
                .map(|(approval, _)| approval)
                .ok_or("Forge pending approval row missing")?;
            assert_eq!(
                pending.request().request_digest().as_bytes(),
                forge_execution::execution_request_digest(&request).as_bytes()
            );
            let approval_at = TimestampMillis::new(current_test_time()?)?;
            let grant = control
                .approve_once(ApprovalOnceRequest {
                    scope,
                    approval_id: pending.id(),
                    expected_revision: pending.revision(),
                    grant_id: ExecutionGrantId::from_uuid(Uuid::now_v7()),
                    observed_at: TimestampMillis::new(
                        approval_at
                            .get()
                            .checked_add(1)
                            .ok_or("approval time overflow")?,
                    )?,
                    grant_expires_at: TimestampMillis::new(
                        approval_at
                            .get()
                            .checked_add(60_000)
                            .ok_or("grant time overflow")?,
                    )?,
                })?
                .1;
            Ok(SourceExecutionPermit {
                approval_id: pending.id().into_uuid(),
                grant_id: grant.id().into_uuid(),
                grant_revision: grant.revision().get(),
                agent_revision: agent_revision.get(),
                node_revision: attribution.expected_node_revision().get(),
                command_id: command_id.as_str().to_owned(),
            })
        })()
        .map_err(|error| error.to_string())
    })
    .await??;
    Ok(prepared)
}

async fn assert_forge_durable_execution_output(
    client: &ForgeCommandHttpClient,
    authority: serde_json::Value,
) -> TestResult {
    let read_request = json!({
        "authority": authority,
        "after_sequence": 0,
        "max_bytes": forge_domain::MIN_EXECUTION_OUTPUT_READ_BYTES,
        "max_events": forge_domain::MAX_EXECUTION_OUTPUT_READ_EVENTS
    });
    let output = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let observed = client
                .command(CommandOp::ExecutionRead, read_request.clone())
                .await?;
            if observed["terminal"] == true {
                return Ok::<_, ForgeHttpClientError>(observed);
            }
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    })
    .await??;
    assert_eq!(output["state"], "completed");
    assert_eq!(output["terminal"], true);
    let events = output["events"]
        .as_array()
        .ok_or("Forge durable output events not available")?;
    let printed = events.iter().any(|event| {
        let Some(encoded) = event["bytes_base64"].as_str() else {
            return false;
        };
        event["kind"] == "chunk"
            && event["stream"] == "stdout"
            && base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .is_ok_and(|bytes| {
                    bytes
                        .windows(b"source-e2e-approved".len())
                        .any(|window| window == b"source-e2e-approved")
                })
    });
    assert!(
        printed,
        "Forge durable execution output lacks expected stdout"
    );
    Ok(())
}

/// The official Forge Evidence API must return the exact terminal execution
/// and immutable stdout from one durable snapshot. No `VerificationRun` is
/// asserted or manufactured by this local command test.
async fn assert_forge_execution_evidence(
    source: &ForgeHost,
    approved: &ApprovedExecutionFixture,
    permit: &SourceExecutionPermit,
) -> TestResult {
    let control = source.control_client.clone();
    let scope = source.scope.clone();
    let request = approved.request.clone();
    let project_command = approved.project_command;
    let forge_approval_id = ApprovalRequestId::from_uuid(permit.approval_id);
    tokio::task::spawn_blocking(move || {
        (|| -> TestResult {
            let (local_approval, attached_execution) = control
                .approval_get(scope.clone(), forge_approval_id)?
                .ok_or("Forge source local execution approval disappeared")?;
            assert_eq!(local_approval.state(), ApprovalRequestState::Approved);
            let attached_execution =
                attached_execution.ok_or("Forge approval has no linked execution")?;
            assert_eq!(attached_execution.id(), request.id());
            assert_eq!(attached_execution.state(), ExecutionState::Completed);
            let selection = ExecutionObservationSelection::new(request.activity_id(), None);
            let inventory = control.execution_evidence_inventory(
                scope.clone(),
                selection,
                ExecutionListLimit::new(4)?,
            )?;
            assert_eq!(inventory.entries().len(), 1);
            assert!(!inventory.has_more());
            let record = &inventory.entries()[0];
            assert_eq!(record.header().id(), request.id());
            assert_eq!(record.header().workspace_id(), request.workspace_id());
            assert_eq!(record.header().activity_id(), request.activity_id());
            assert_eq!(record.header().state(), ExecutionState::Completed);
            assert_eq!(
                record.header().request_digest(),
                forge_execution::execution_request_digest(&request)
            );
            assert!(record.header().provenance().is_some());
            if let Some(evidence) = project_command {
                let verified = record
                    .verification()
                    .ok_or("real VerificationRun not published")?;
                let run = verified.run();
                assert_eq!(run.execution_id(), request.id());
                assert_eq!(run.workspace_id(), request.workspace_id());
                assert_ne!(run.input_id().into_uuid(), Uuid::nil());
                assert_eq!(
                    run.catalog_fingerprint(),
                    Some(evidence.catalog_fingerprint())
                );
                assert_eq!(
                    run.disposition(),
                    Some(forge_domain::VerificationRunDisposition::Accepted),
                );
                assert_eq!(
                    run.terminal_status()
                        .ok_or("real VerificationRun not terminalized")?
                        .output_integrity(),
                    Some(forge_domain::VerificationOutputIntegrity::Complete),
                );
            } else {
                assert!(
                    record.verification().is_none(),
                    "no VerificationRun was executed"
                );
            }
            let durable = control.execution_evidence_read(
                scope,
                selection,
                request.id(),
                ExecutionOutputCursor::initial(request.id()),
                forge_domain::MIN_EXECUTION_OUTPUT_READ_BYTES,
                forge_domain::MAX_EXECUTION_OUTPUT_READ_EVENTS,
            )?;
            assert_eq!(durable.record(), record);
            assert!(durable.output().events().iter().any(|event| {
                matches!(event, ExecutionOutputEvent::Chunk(chunk)
                    if chunk.bytes().windows(b"source-e2e-approved".len())
                        .any(|window| window == b"source-e2e-approved"))
            }));
            Ok(())
        })()
        .map_err(|error| error.to_string())
    })
    .await??;
    Ok(())
}

/// Use the same actual Orchestration decision and Forge-local one-shot grant
/// for a bounded local sandbox execution, never a paid provider or user path.
async fn assert_real_approved_forge_execution(
    source: &ForgeHost,
    principal: AuthenticatedPrincipal,
    target: &MemberTarget,
    first: &ReadyFixtureMember,
    service: ForgeExecutionService,
    node: NodeTeamScopeWire,
    approved: &ApprovedExecutionFixture,
) -> TestResult {
    let AuthenticatedPrincipal::DirectToken(token) = principal else {
        return Err("isolated HTTP execution requires a genuine bearer principal".into());
    };
    assert_eq!(approved.request.workspace_id(), source.scope.workspace_id());
    assert_eq!(
        approved.request.activity_id().into_uuid(),
        target.activity_id
    );
    assert_eq!(approved.admission.workspace_id, target.scope.workspace_id);
    assert_eq!(
        approved.admission.issuer,
        service.issuer.into_uuid().to_string()
    );
    assert_eq!(
        approved.admission.backend_id,
        service.backend.into_uuid().to_string()
    );
    let permit =
        prepare_forge_execution_permit(source, principal, target, first, service, node, approved)
            .await?;
    let authority = json!({
        "scope": forge_source_scope(source)?,
        "activity": {
            "activity_id": target.activity_id,
            "runtime_instance_id": service.activity_runtime.into_uuid(),
            "observed_at_ms": current_test_time()?
        },
        "actor": {
            "session_id": first.session_id,
            "principal": {"kind": "direct_token", "token_id": token.into_uuid()},
            "workspace_id": target.scope.workspace_id,
            "agent_id": first.agent_id,
            "expected_revision": permit.agent_revision
        },
        "goal_id": node.goal_id,
        "expected_goal_revision": node.dag_base_goal_revision,
        "expected_dag_revision": node.dag_revision,
        "node_id": node.node_id,
        "expected_node_revision": permit.node_revision,
        "command_id": permit.command_id,
        "execution_id": approved.request.id().into_uuid()
    });
    let payload = json!({
        "authority": authority.clone(),
        "grant_id": permit.grant_id,
        "expected_grant_revision": permit.grant_revision,
        "mode": p10::execution_mode(approved),
        "admission": {
            "backend_id": service.backend.into_uuid(),
            "issuer": service.issuer.into_uuid(),
            "expected_digest": official_execution_digest(&approved.request),
            "issued_at_ms": approved.admission.issued_at_ms,
            "expires_at_ms": approved.admission.expires_at_ms
        }
    });
    let client = ForgeCommandHttpClient::new(source.address, FORGE_SECRET, DEADLINE)?;
    if !source.verified_ci {
        // Shell admission keeps the four negative mutation probes. A sealed
        // verification can publish an immutable input while validating a
        // request, so do not launch multiple distinct verification mutations.
        assert_forge_rejects_mutated_execution_authority(&client, &payload, &approved.request)
            .await?;
    }
    let control = source.control_client.clone();
    let scope = source.scope.clone();
    let execution_id = approved.request.id();
    let absent =
        tokio::task::spawn_blocking(move || control.execution_get(scope, execution_id)).await??;
    assert!(
        absent.is_none(),
        "invalid digest or unissued Forge grant caused a durable execution"
    );
    let launched = client
        .command(CommandOp::ExecutionBegin, payload)
        .await
        .map_err(|error| {
            format!(
                "Forge original ExecutionBegin ({}) returned {error:?}; do not replay this command",
                approved.request.id().into_uuid()
            )
        })?;
    assert_eq!(
        launched["execution_id"],
        json!(approved.request.id().into_uuid())
    );
    assert_forge_durable_execution_output(&client, authority).await?;
    assert_forge_execution_evidence(source, approved, &permit).await
}

/// An exact approval and grant must never authorize a caller-mutated digest,
/// issuer, backend identity, or unissued Forge grant before any side effect.
async fn assert_forge_rejects_mutated_execution_authority(
    client: &ForgeCommandHttpClient,
    payload: &serde_json::Value,
    request: &ExecutionRequest,
) -> TestResult {
    let mut altered = payload.clone();
    let exact = official_execution_digest(request);
    let wrong_prefix = if exact.starts_with('0') { "1" } else { "0" };
    altered["admission"]["expected_digest"] = json!(format!("{wrong_prefix}{}", &exact[1..]));
    let refused = client.command(CommandOp::ExecutionBegin, altered).await;
    assert!(
        matches!(refused, Err(ForgeHttpClientError::Service { .. })),
        "wrong digest must be rejected by Forge before execution: {refused:?}"
    );
    // A caller-selected issuer or backend must not turn a valid policy
    // decision into a different Forge execution authority.
    for field in ["issuer", "backend_id"] {
        let mut substituted = payload.clone();
        substituted["admission"][field] = json!(Uuid::now_v7());
        let denied = client.command(CommandOp::ExecutionBegin, substituted).await;
        assert!(
            matches!(
                denied,
                Err(ForgeHttpClientError::Authentication | ForgeHttpClientError::Service { .. })
            ),
            "foreign {field} must not be a valid Forge admission: {denied:?}"
        );
    }
    let mut unissued_grant = payload.clone();
    unissued_grant["grant_id"] = json!(Uuid::now_v7());
    let no_grant = client
        .command(CommandOp::ExecutionBegin, unissued_grant)
        .await;
    assert!(
        matches!(no_grant, Err(ForgeHttpClientError::Service { .. })),
        "Orchestration approval alone must not bypass Forge local grant: {no_grant:?}"
    );
    Ok(())
}

/// The actual Forge execution terminal fact must cross the authenticated
/// outbox cursor and survive as one Orchestration-owned source projection.
/// A second HTTP read must not reapply the original terminal transition.
async fn assert_forge_terminal_effect_consumed(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    source: &ForgeHost,
    target: &MemberTarget,
    execution_id: ExecutionId,
) -> TestResult {
    let query = CoordinationQueryRequest {
        scope: target.scope.clone(),
        activity_id: target.activity_id,
        session_id: Some(target.session_id),
    };
    let consumed = client.agent_coordination_get(bearer, &query).await?;
    assert_eq!(consumed.request, query);
    let applied = consumed.catch_up.ok_or("missing Forge terminal catch-up")?;
    assert_eq!(
        applied.page_size, 1,
        "expected one Forge execution terminal fact"
    );
    assert_eq!(applied.applied_through, applied.requested_after + 1);

    let control = source.control_client.clone();
    let scope = source.scope.clone();
    let original = tokio::task::spawn_blocking(move || {
        control.effect_fact_outbox(EffectFactOutboxReadRequest {
            scope,
            migration_id: applied.migration_id,
            destination_id: applied.destination_id,
            after: applied.requested_after,
            limit: 64,
        })
    })
    .await??;
    assert_eq!(original.len(), 1, "Forge source terminal page changed");
    let terminal = &original[0];
    assert_eq!(terminal.sequence(), applied.applied_through);
    assert_eq!(terminal.migration_id(), applied.migration_id);
    assert_eq!(terminal.destination_id(), applied.destination_id);
    assert_eq!(
        terminal.workspace_id().into_uuid(),
        target.scope.workspace_id
    );
    match terminal.fact() {
        EffectFact::ExecutionTerminal(fact) => {
            assert_eq!(fact.execution_id(), execution_id);
            assert_eq!(fact.activity_id().into_uuid(), target.activity_id);
            assert_eq!(fact.state(), ExecutionState::Completed);
            if source.verified_ci {
                let verified = fact
                    .verification()
                    .ok_or("terminal source lost VerificationRun")?;
                assert_ne!(verified.input_id().into_uuid(), Uuid::nil());
                assert_eq!(
                    verified.disposition(),
                    forge_domain::VerificationRunDisposition::Accepted
                );
                assert_eq!(
                    verified.output_integrity(),
                    Some(forge_domain::VerificationOutputIntegrity::Complete),
                );
                assert!(verified.output_fingerprint().is_some());
            } else {
                assert!(
                    fact.verification().is_none(),
                    "analyzed shell did not create a VerificationRun"
                );
            }
        }
        other => return Err(format!("unexpected Forge terminal effect: {other:?}").into()),
    }

    let repeated = client.agent_coordination_get(bearer, &query).await?;
    let unchanged = repeated.catch_up.ok_or("missing replay source cursor")?;
    assert_eq!(unchanged.page_size, 0);
    assert_eq!(unchanged.requested_after, applied.applied_through);
    assert_eq!(unchanged.applied_through, applied.applied_through);
    Ok(())
}

fn forge_source_scope(source: &ForgeHost) -> TestResult<forge_backend::ops::ScopeDto> {
    let locator = source.scope.locator();
    Ok(forge_backend::ops::ScopeDto {
        workspace_id: source.scope.workspace_id().into_uuid(),
        workspace_revision: source.scope.workspace_revision().get(),
        locator: forge_backend::ops::LocatorDto {
            location_id: locator.location_id().into_uuid(),
            expected_revision: locator.expected_revision().get(),
            relative_root: locator
                .relative_root()
                .to_str()
                .ok_or("fixture relative root must be UTF-8")?
                .to_owned(),
        },
    })
}

/// The actual Verified destination digest is the only accepted source seal
/// input; this never synthesizes a manifest digest or modifies a database.
async fn seal_verified_source_import(
    source: &ForgeHost,
    issuer: OrchestrationIssuerId,
    progress: &ImportProgressWire,
) -> TestResult<serde_json::Value> {
    assert_eq!(progress.phase, ImportPhaseWire::Verified);
    assert_eq!(progress.import_digest, progress.verify_digest);
    let command = ForgeCommandHttpClient::new(source.address, FORGE_SECRET, DEADLINE)?;
    let receipt = command
        .command(
            CommandOp::AgentOperationsDelegate,
            json!({
                "scope": forge_source_scope(source)?,
                "issuer_id": issuer.into_uuid(),
                "destination_id": progress.destination_id,
                "reference": {
                    "migration_id": progress.migration_id,
                    "expected_freeze_revision": i64::try_from(progress.source_revision)?
                },
                "manifest_digest": &progress.verify_digest,
                "observed_at_ms": current_test_time()?
            }),
        )
        .await?;
    assert_eq!(receipt["found"], true);
    assert_eq!(receipt["migration_id"], json!(progress.migration_id));
    assert_eq!(receipt["manifest_digest"], progress.verify_digest);
    Ok(receipt)
}

/// A membership registration is bound to an actual global Profile and a
/// principal-owned logical Session, not merely to the Forge reservation.
async fn bind_fixture_profile_session(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    session_id: Uuid,
    stable_name: &str,
) -> TestResult<Uuid> {
    let created = client
        .create_profile(
            bearer,
            &AgentProfileCreateRequest {
                request_id: Uuid::now_v7().to_string(),
                stable_name: stable_name.to_owned(),
                display_name: "isolated broker-backed member".to_owned(),
                preferred_model: None,
                preferred_reasoning: None,
                base_capabilities: vec!["verification".to_owned()],
            },
        )
        .await
        .map_err(|error| format!("canonical Profile create rejected: {error:?}"))?;
    assert!(created.created);
    let binding = client
        .bind_profile_session(
            bearer,
            created.profile.profile_id,
            &ProfileSessionBindRequest {
                request_id: Uuid::now_v7().to_string(),
                session_id: session_id.to_string(),
            },
        )
        .await
        .map_err(|error| format!("canonical Profile/Session bind rejected: {error:?}"))?;
    assert!(binding.created);
    assert_eq!(binding.profile_id, created.profile.profile_id);
    assert_eq!(binding.session_id, session_id);
    let observed = client
        .profile_session_binding(bearer, created.profile.profile_id, session_id)
        .await?;
    assert_eq!(observed.request_id, binding.request_id);
    assert!(!observed.created);
    Ok(created.profile.profile_id)
}

fn current_test_time() -> TestResult<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}

async fn bootstrap_flow(flow: &BootstrapHttpFlow<'_>) -> TestResult {
    let progress = import_and_verify_workspace(flow).await?;
    if let Some(case) = flow.broker_case {
        let non_operator = flow
            .non_operator
            .ok_or("real Broker fixture is missing its non-operator test credential")?;
        let agent_flow = RealBrokerAgentRequestFlow {
            client: flow.client,
            bearer: flow.bearer,
            backend_address: flow.backend_address,
            principal: flow.principal,
            scope: &flow.prepare.scope,
            progress: &progress,
            source: case.0,
            activity_id: case.1,
            service: case.2,
            non_operator,
            transfer_exercise: flow.transfer_exercise,
        };
        let sequence = assert_real_broker_agent_request_flow(&agent_flow).await?;
        flow.final_source_sequence
            .store(sequence, Ordering::Release);
        return Ok(());
    }
    abort_unavailable_broker_flow(flow, &progress).await
}

async fn import_and_verify_workspace(
    flow: &BootstrapHttpFlow<'_>,
) -> TestResult<ImportProgressWire> {
    assert_verified_workspace_query_gate(flow.client, flow.bearer, &flow.prepare.scope, false)
        .await?;
    let mut progress = flow
        .client
        .import_prepare(flow.bearer, flow.prepare)
        .await?;
    assert_eq!(progress.phase, ImportPhaseWire::Importing);
    assert_eq!(progress.page_count, 0);
    let first_request = step_request(flow.prepare, &progress);
    progress = flow
        .client
        .import_next_page(flow.bearer, flow.prepare.migration_id, &first_request)
        .await?;
    assert!(
        matches!(flow.client.import_next_page(flow.bearer, flow.prepare.migration_id, &first_request).await,
        Err(ClientError::Rejected { status, .. }) if status == StatusCode::CONFLICT.as_u16())
    );
    assert_eq!(
        flow.client
            .import_progress(flow.bearer, flow.prepare.migration_id)
            .await?,
        Some(progress.clone())
    );
    // Fresh source has no historical Agent operations. All five table families
    // still cross the real source HTTP and independent destination verification.
    for _ in 1..5 {
        progress = flow
            .client
            .import_next_page(
                flow.bearer,
                flow.prepare.migration_id,
                &step_request(flow.prepare, &progress),
            )
            .await?;
    }
    assert_eq!(progress.phase, ImportPhaseWire::Imported);
    assert_eq!(progress.page_count, 5);
    assert_eq!(progress.record_count, 0);
    assert_eq!(
        flow.client
            .import_prepare(flow.bearer, flow.prepare)
            .await?,
        progress
    );
    for _ in 0..5 {
        progress = flow
            .client
            .import_verify_next_page(
                flow.bearer,
                flow.prepare.migration_id,
                &step_request(flow.prepare, &progress),
            )
            .await?;
    }
    assert_eq!(progress.phase, ImportPhaseWire::Verified);
    assert_eq!(progress.import_digest, progress.verify_digest);
    assert_eq!(progress.verified_pages, progress.page_count);
    assert_eq!(
        flow.client
            .import_prepare(flow.bearer, flow.prepare)
            .await?,
        progress
    );
    assert_verified_workspace_query_gate(flow.client, flow.bearer, &flow.prepare.scope, true)
        .await?;
    Ok(progress)
}

async fn abort_unavailable_broker_flow(
    flow: &BootstrapHttpFlow<'_>,
    progress: &ImportProgressWire,
) -> TestResult {
    assert_session_open_fails_closed_without_broker(flow.client, flow.bearer).await?;
    let aborted = flow
        .client
        .import_abort(
            flow.bearer,
            flow.prepare.migration_id,
            &step_request(flow.prepare, progress),
        )
        .await?;
    assert_eq!(aborted.phase, ImportPhaseWire::Aborted);
    assert_eq!(
        flow.client
            .import_prepare(flow.bearer, flow.prepare)
            .await?,
        aborted
    );
    assert_verified_workspace_query_gate(flow.client, flow.bearer, &flow.prepare.scope, false)
        .await?;
    Ok(())
}

fn bootstrap_fixture_activity(
    source: &ForgeHost,
) -> TestResult<(ActivityId, RuntimeInstanceId, i64)> {
    let activity_id = ActivityId::from_uuid(Uuid::now_v7());
    let runtime_instance_id = source.boot_instance;
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let bootstrap = source.control_client.bootstrap_activity(
        source.scope.clone(),
        activity_id,
        runtime_instance_id,
        TimestampMillis::new(3)?,
        TimestampMillis::new(now_ms.checked_add(120_000).ok_or("lease clock overflow")?)?,
    )?;
    assert_eq!(bootstrap.status(), ActivityBootstrapStatus::Created);
    Ok((activity_id, runtime_instance_id, now_ms))
}

async fn await_import_server(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    migration_id: Uuid,
) -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        loop {
            match client.import_progress(bearer, migration_id).await {
                Ok(None) => return Ok::<(), ClientError>(()),
                Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                Ok(Some(_)) => return Err(ClientError::Protocol),
                Err(error) => return Err(error),
            }
        }
    })
    .await??;
    Ok(())
}

async fn assert_release_source_freeze(
    source: &ForgeHost,
    freeze: &AgentOperationsFreeze,
    migration_id: Uuid,
    destination: Uuid,
) -> TestResult {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_http_bootstrap_verifies_then_aborts_without_native_source_fallback() -> TestResult {
    let (broker_cluster, _broker_port_guards) = unavailable_broker_cluster()?;
    Box::pin(bootstrap_fixture_with_broker(broker_cluster, false, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN for isolated real three-node Broker cluster"]
async fn real_broker_verified_membership_coordinator_and_agent_request_admission() -> TestResult {
    let cluster = broker_cluster::Cluster::start().await?;
    let result = Box::pin(bootstrap_fixture_with_broker(
        cluster.orchestration_config()?,
        true,
        false,
    ))
    .await;
    drop(cluster);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN for isolated real three-node Broker cluster"]
async fn real_broker_synthetic_pre_hold_legacy_slot_recovery_http() -> TestResult {
    let cluster = broker_cluster::Cluster::start().await?;
    let result = Box::pin(legacy_slot_recovery::run_synthetic_pre_hold_recovery(
        cluster.orchestration_config()?,
    ))
    .await;
    drop(cluster);
    result
}

fn fixture_source_authority(source: &ForgeHost) -> TestResult<CoordinatorAuthority> {
    Ok(CoordinatorAuthority::from_http(
        ForgeSourceHttpClient::new(ForgeCommandHttpClient::new(
            source.address,
            FORGE_SECRET,
            DEADLINE,
        )?),
        vec![source.scope.clone()],
    )?
    .with_effect_facts(source.client.clone()))
}

fn configured_fixture_runtime(
    data_root: &Path,
    broker_cluster: BrokerClusterConfig,
    principal: AuthenticatedPrincipal,
    issuer: OrchestrationIssuerId,
    backend: ForgeBackendId,
    now_ms: i64,
) -> TestResult<OrchestrationRuntime> {
    let runtime = OrchestrationRuntime::open_with_broker_cluster(data_root, broker_cluster)?
        .with_operator_policy(OperatorPolicy::new(vec![principal])?);
    let receipt = runtime.ensure_admission_issuer(ConfigureAdmissionIssuerCommand {
        issuer,
        backend_id: backend,
        admission_ttl_ms: 300_000,
        now_ms,
    })?;
    assert_eq!(receipt.issuer(), issuer);
    assert_eq!(receipt.backend_id(), backend);
    Ok(runtime)
}

/// Reopen the exact durable Orchestration destination after the first HTTP
/// owner has stopped. Only source-scoped reads are performed: no replayed
/// Team creation, Approval decision, `AgentRequest` admission, or Provider work.
async fn assert_restarted_control_plane_readback(
    data_root: &Path,
    broker_cluster: BrokerClusterConfig,
    principal: AuthenticatedPrincipal,
    source: &ForgeHost,
    token: &McpTokenRuntime,
    bearer: &HeaderValue,
    prepare: &ImportPrepareRequest,
) -> TestResult {
    let runtime = OrchestrationRuntime::open_with_broker_cluster(data_root, broker_cluster)?
        .with_operator_policy(OperatorPolicy::new(vec![principal])?);
    assert_eq!(runtime.destination_id(), prepare.destination_id);
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = listener.local_addr()?;
    drop(listener);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(serve_with_runtime(
        Arc::new(runtime),
        address,
        token.client(),
        Some(fixture_source_authority(source)?),
        cancellation.clone(),
    ));
    let client = OrchestrationHttpClient::new(address, DEADLINE)?;
    let result: TestResult = async {
        let progress = tokio::time::timeout(DEADLINE, async {
            loop {
                match client.import_progress(bearer, prepare.migration_id).await {
                    Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                    response => return response,
                }
            }
        })
        .await??
        .ok_or("Verified import disappeared after Orchestration restart")?;
        assert_eq!(progress.phase, ImportPhaseWire::Verified);
        let scope = prepare.scope.clone();
        let teams = client
            .team_list(
                bearer,
                &TeamListQueryRequest {
                    scope: scope.clone(),
                    session_id: None,
                    limit: 10,
                },
            )
            .await?;
        assert_eq!(teams.teams.len(), 2);
        assert!(!teams.truncated);
        assert!(
            teams
                .teams
                .iter()
                .any(|team| team.name == "Source evidence review")
        );
        assert!(
            teams
                .teams
                .iter()
                .any(|team| team.name == "Temporary dispatch receiving")
        );
        let requests = client
            .agent_requests(
                bearer,
                &AgentRequestListQueryRequest {
                    scope: scope.clone(),
                    session_id: None,
                    limit: 10,
                },
            )
            .await?;
        assert_eq!(requests.items.len(), 1);
        assert!(!requests.truncated);
        let policy = client
            .approval_policy(
                bearer,
                &ApprovalPolicyQueryRequest {
                    scope: scope.clone(),
                    session_id: None,
                },
            )
            .await?
            .ok_or("approval policy missing after restart")?;
        assert_eq!(policy.revision, 1);
        assert!(policy.enabled);
        let approvals = client
            .approval_requests(
                bearer,
                &ApprovalListQueryRequest {
                    scope,
                    session_id: None,
                    state: None,
                    limit: 10,
                },
            )
            .await?;
        assert_eq!(approvals.len(), 2);
        assert!(approvals.iter().any(|item| item.state == "approved"));
        assert!(approvals.iter().any(|item| item.state == "rejected"));
        Ok(())
    }
    .await;
    cancellation.cancel();
    tokio::time::timeout(DEADLINE, server).await???;
    result
}

fn fixture_storage(broker_cluster: BrokerClusterConfig) -> TestResult<(PathBuf, PathBuf, Uuid)> {
    let root = std::env::temp_dir().join(format!("cooperation-bootstrap-http-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let data_root = root.join("orchestration");
    std::fs::create_dir(&data_root)?;
    let runtime = OrchestrationRuntime::open_with_broker_cluster(&data_root, broker_cluster)?;
    let destination = runtime.destination_id();
    drop(runtime);
    Ok((root, data_root, destination))
}

fn fixture_http_address() -> TestResult<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

fn assert_persisted_bootstrap_catalog(
    data_root: &Path,
    issuer: OrchestrationIssuerId,
    backend: ForgeBackendId,
    workspace: Uuid,
    migration_id: Uuid,
    actual_broker: bool,
    verified_ci: bool,
    expected_final_source_sequence: Option<u64>,
) -> TestResult {
    let catalog = OrchestrationRuntime::open_existing_operations_read_only(data_root)?;
    let configured = catalog
        .admission_issuer_config()?
        .ok_or("issuer not initialized")?;
    assert_eq!(configured.issuer(), issuer);
    assert_eq!(configured.backend_id(), backend);
    let bindings = catalog.node_team_bindings(WorkspaceId::from_uuid(workspace))?;
    assert_eq!(bindings.bindings().len(), if actual_broker { 3 } else { 0 });
    assert!(!bindings.truncated());
    if actual_broker {
        let mut team_ids = Vec::new();
        for binding in bindings.bindings() {
            assert_eq!(binding.bound_by(), "orchestrator");
            assert_eq!(binding.revision(), 1);
            let team_id = binding
                .team_id()
                .ok_or("source-backed Node-Team binding lost its Team ID")?;
            if !team_ids.contains(&team_id) {
                team_ids.push(team_id);
            }
        }
        assert_eq!(team_ids.len(), 2);
    }
    let terminals = catalog.execution_terminal_projection(migration_id)?;
    let executed = actual_broker && verified_ci;
    assert_eq!(terminals.len(), usize::from(executed));
    if executed {
        let terminal = terminals[0];
        assert_eq!(terminal.migration_id(), migration_id);
        assert_eq!(terminal.execution_state(), ExecutionState::Completed);
        assert!(terminal.source_sequence() > 1);
        let progress = catalog
            .effect_fact_progress(migration_id)?
            .ok_or("Forge terminal effect consumer progress missing after restart")?;
        let expected_final_source_sequence = expected_final_source_sequence
            .ok_or("real Broker flow did not return its exact final source sequence")?;
        assert!(expected_final_source_sequence > terminal.source_sequence());
        assert_eq!(progress.last_sequence(), expected_final_source_sequence);
        assert_eq!(progress.workspace_id().into_uuid(), workspace);
    }
    Ok(())
}

/// Restart Forge's official control worker on the same isolated Workspace;
/// read the original execution without reissuing a grant or process command.
fn assert_forge_execution_survives_worker_restart(
    root: &Path,
    scope: &ResolvedWorkspaceScope,
    activity_id: ActivityId,
    actual_broker: bool,
    verified_ci: bool,
) -> TestResult {
    let registry = GlobalRegistry::open_existing(root.join("registry.sqlite3"))?;
    let (client, mut worker) = ForgeControlWorker::start(ForgeControl::new(registry))?;
    let records = client.execution_list(scope.clone(), activity_id, ExecutionListLimit::new(4)?)?;
    let executed = actual_broker && verified_ci;
    assert_eq!(records.len(), usize::from(executed));
    if executed {
        assert_eq!(records[0].state(), ExecutionState::Completed);
        assert_eq!(records[0].activity_id(), activity_id);
        if verified_ci {
            let inventory = client.execution_evidence_inventory(
                scope.clone(),
                ExecutionObservationSelection::new(activity_id, None),
                ExecutionListLimit::new(4)?,
            )?;
            let [record] = inventory.entries() else {
                return Err("restarted Forge lost exact durable RustCI execution".into());
            };
            let verification = record
                .verification()
                .ok_or("restarted Forge lost terminal VerificationRun evidence")?;
            assert_eq!(
                verification.run().disposition(),
                Some(forge_domain::VerificationRunDisposition::Accepted),
            );
        }
    }
    worker.shutdown()?;
    Ok(())
}

async fn start_fixture_session_resolver(
    data_root: &Path,
) -> TestResult<InternalSessionServerHandle> {
    let authority = Arc::new(SessionCatalogAuthority::new(
        data_root.join("agent-sessions.sqlite3"),
    ));
    Ok(InternalSessionServerHandle::start(
        authority,
        InternalSessionServerConfig::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            SESSION_RESOLVER_SECRET,
        )?,
    )
    .await?)
}

async fn freeze_fixture_source(
    source: &ForgeHost,
    destination: Uuid,
) -> TestResult<(AgentOperationsFreeze, ImportPrepareRequest)> {
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
    Ok((freeze, prepare))
}

async fn start_isolated_forge_fixture(
    root: &Path,
    data_root: &Path,
    destination: Uuid,
    verified_ci: bool,
) -> TestResult<(
    InternalSessionServerHandle,
    ForgeBackendId,
    OrchestrationIssuerId,
    ForgeHost,
)> {
    let session_server = start_fixture_session_resolver(data_root).await?;
    let backend = ForgeBackendId::from_uuid(Uuid::now_v7());
    let issuer = OrchestrationIssuerId::from_uuid(Uuid::now_v7());
    let source = ForgeHost::start(
        root,
        backend,
        issuer,
        destination,
        session_server.address(),
        verified_ci,
    )
    .await?;
    Ok((session_server, backend, issuer, source))
}

struct BootstrapHttpFlow<'a> {
    client: &'a OrchestrationHttpClient,
    bearer: &'a HeaderValue,
    backend_address: std::net::SocketAddr,
    final_source_sequence: Arc<AtomicU64>,
    principal: AuthenticatedPrincipal,
    non_operator: Option<&'a reviewer_qualification_rule::NonOperatorCredential>,
    prepare: &'a ImportPrepareRequest,
    source: &'a ForgeHost,
    freeze: &'a AgentOperationsFreeze,
    destination: Uuid,
    actual_broker: bool,
    transfer_exercise: TransferExercise,
    broker_case: Option<(&'a ForgeHost, Uuid, ForgeExecutionService)>,
}

async fn run_bootstrap_http_flow(flow: &BootstrapHttpFlow<'_>) -> TestResult {
    await_import_server(flow.client, flow.bearer, flow.prepare.migration_id).await?;
    bootstrap_flow(flow).await?;
    if !flow.actual_broker {
        assert_release_source_freeze(
            flow.source,
            flow.freeze,
            flow.prepare.migration_id,
            flow.destination,
        )
        .await?;
    }
    Ok(())
}

struct RestartReadbackContext<'a> {
    data_root: &'a Path,
    broker_cluster: BrokerClusterConfig,
    principal: AuthenticatedPrincipal,
    source: &'a ForgeHost,
    token: &'a McpTokenRuntime,
    bearer: &'a HeaderValue,
    prepare: &'a ImportPrepareRequest,
    actual_broker: bool,
}

async fn restart_readback_after_success(
    context: RestartReadbackContext<'_>,
    flow_succeeded: bool,
) -> TestResult {
    if context.actual_broker && flow_succeeded {
        assert_restarted_control_plane_readback(
            context.data_root,
            context.broker_cluster,
            context.principal,
            context.source,
            context.token,
            context.bearer,
            context.prepare,
        )
        .await
    } else {
        Ok(())
    }
}

struct PersistedBootstrapContext<'a> {
    root: &'a Path,
    data_root: &'a Path,
    source_scope: &'a ResolvedWorkspaceScope,
    activity_id: ActivityId,
    actual_broker: bool,
    immutable_verification: bool,
    issuer: OrchestrationIssuerId,
    backend: ForgeBackendId,
    workspace_id: Uuid,
    migration_id: Uuid,
    expected_final_source_sequence: Option<u64>,
}

fn assert_persisted_bootstrap_after_success(
    context: &PersistedBootstrapContext<'_>,
    flow_succeeded: bool,
) -> TestResult {
    if flow_succeeded {
        assert_forge_execution_survives_worker_restart(
            context.root,
            context.source_scope,
            context.activity_id,
            context.actual_broker,
            context.immutable_verification,
        )?;
        assert_persisted_bootstrap_catalog(
            context.data_root,
            context.issuer,
            context.backend,
            context.workspace_id,
            context.migration_id,
            context.actual_broker,
            context.immutable_verification,
            context.expected_final_source_sequence,
        )?;
    }
    Ok(())
}

async fn stop_bootstrap_source_fixtures(
    source: ForgeHost,
    mut session_server: InternalSessionServerHandle,
) -> TestResult<(ResolvedWorkspaceScope, bool)> {
    let source_scope = source.scope.clone();
    let immutable_verification = source.verified_ci;
    let source_stopped = source.shutdown().await;
    let session_stopped = session_server.shutdown(DEADLINE).await;
    source_stopped.map_err(|error| format!("Forge fixture shutdown failed: {error:?}"))?;
    assert!(session_stopped.is_successful());
    Ok((source_scope, immutable_verification))
}

async fn bootstrap_fixture_with_broker(
    broker_cluster: BrokerClusterConfig,
    actual_broker: bool,
    verified_ci: bool,
) -> TestResult {
    Box::pin(bootstrap_fixture_with_broker_options(
        broker_cluster,
        actual_broker,
        verified_ci,
        TransferExercise::None,
    ))
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferExercise {
    None,
    SlowRenewal,
    Preparation,
    UnattributedPreparation,
    SourceDispatch,
}

async fn fixture_source_authority_with_probe(
    source: &ForgeHost,
    backend: ForgeBackendId,
    issuer: OrchestrationIssuerId,
    destination: Uuid,
    transfer_exercise: TransferExercise,
) -> TestResult<(
    CoordinatorAuthority,
    Option<team_expansion_v3_existing_home::home_source::SourceHttpProxy>,
)> {
    let authority = fixture_source_authority(source)?;
    if transfer_exercise != TransferExercise::SourceDispatch {
        return Ok((authority, None));
    }
    let proxy =
        team_expansion_v3_existing_home::home_source::SourceHttpProxy::start(source.address)
            .await?;
    let authority = authority.with_home_transfer_source(
        ForgeHttpClient::new(
            proxy.address(),
            FORGE_SECRET,
            backend,
            destination,
            DEADLINE,
        )?,
        HomeTransferSourceService {
            issuer,
            backend_id: backend,
            destination_id: destination,
        },
        300_000,
    )?;
    Ok((authority, Some(proxy)))
}

async fn bootstrap_fixture_with_broker_options(
    broker_cluster: BrokerClusterConfig,
    actual_broker: bool,
    verified_ci: bool,
    transfer_exercise: TransferExercise,
) -> TestResult {
    let (root, data_root, destination) = fixture_storage(broker_cluster)?;
    let (session_server, backend, issuer, source) =
        start_isolated_forge_fixture(&root, &data_root, destination, verified_ci).await?;
    let (activity_id, activity_runtime, now_ms) = bootstrap_fixture_activity(&source)?;
    let (freeze, prepare) = freeze_fixture_source(&source, destination).await?;
    let (authority, source_proxy) = fixture_source_authority_with_probe(
        &source,
        backend,
        issuer,
        destination,
        transfer_exercise,
    )
    .await?;
    let (token, bearer, principal) = token_authority(&root)?;
    let non_operator = actual_broker
        .then(|| reviewer_qualification_rule::issue_nonoperator_credential(&token))
        .transpose()?;
    let address = fixture_http_address()?;
    let runtime = configured_fixture_runtime(
        &data_root,
        broker_cluster,
        principal,
        issuer,
        backend,
        now_ms,
    )?;
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(serve_with_runtime(
        Arc::new(runtime),
        address,
        token.client(),
        Some(authority),
        cancellation.clone(),
    ));
    let client = OrchestrationHttpClient::new(address, DEADLINE)?;
    let http_flow = BootstrapHttpFlow {
        client: &client,
        bearer: &bearer,
        backend_address: address,
        final_source_sequence: Arc::new(AtomicU64::new(0)),
        principal,
        non_operator: non_operator.as_ref(),
        prepare: &prepare,
        source: &source,
        freeze: &freeze,
        destination,
        actual_broker,
        transfer_exercise,
        broker_case: actual_broker.then_some((
            &source,
            activity_id.into_uuid(),
            ForgeExecutionService {
                backend,
                issuer,
                activity_runtime,
            },
        )),
    };
    let completion =
        run_and_stop_bootstrap_http(&http_flow, cancellation, server, source_proxy).await?;
    let restart_result = restart_readback_after_success(
        RestartReadbackContext {
            data_root: &data_root,
            broker_cluster,
            principal,
            source: &source,
            token: &token,
            bearer: &bearer,
            prepare: &prepare,
            actual_broker,
        },
        completion.result.is_ok(),
    )
    .await;
    let (source_scope, immutable_verification_fixture) =
        stop_bootstrap_source_fixtures(source, session_server).await?;
    // A failed live flow must retain its original error. Do not replace it
    // with a misleading "missing execution" assertion during teardown.
    let persisted = PersistedBootstrapContext {
        root: &root,
        data_root: &data_root,
        source_scope: &source_scope,
        activity_id,
        actual_broker,
        immutable_verification: immutable_verification_fixture,
        issuer,
        backend,
        workspace_id: prepare.scope.workspace_id,
        migration_id: prepare.migration_id,
        expected_final_source_sequence: (completion.final_source_sequence > 0)
            .then_some(completion.final_source_sequence),
    };
    finish_bootstrap_fixture(
        &persisted,
        completion.result,
        restart_result,
        transfer_exercise,
        completion.source_call_counts,
    )
}

struct BootstrapHttpCompletion {
    result: TestResult,
    final_source_sequence: u64,
    source_call_counts: Option<team_expansion_v3_existing_home::home_source::SourceHttpCallCounts>,
}

async fn run_and_stop_bootstrap_http(
    flow: &BootstrapHttpFlow<'_>,
    cancellation: CancellationToken,
    server: tokio::task::JoinHandle<Result<(), heterarchy_orchestration_backend::BackendError>>,
    source_proxy: Option<team_expansion_v3_existing_home::home_source::SourceHttpProxy>,
) -> TestResult<BootstrapHttpCompletion> {
    let result = run_bootstrap_http_flow(flow).await;
    let final_source_sequence = flow.final_source_sequence.load(Ordering::Acquire);
    cancellation.cancel();
    tokio::time::timeout(DEADLINE, server).await???;
    let source_call_counts = if let Some(proxy) = source_proxy {
        Some(proxy.finish().await?)
    } else {
        None
    };
    Ok(BootstrapHttpCompletion {
        result,
        final_source_sequence,
        source_call_counts,
    })
}

fn finish_bootstrap_fixture(
    persisted: &PersistedBootstrapContext<'_>,
    result: TestResult,
    restart_result: TestResult,
    transfer_exercise: TransferExercise,
    counts: Option<team_expansion_v3_existing_home::home_source::SourceHttpCallCounts>,
) -> TestResult {
    assert_persisted_bootstrap_after_success(persisted, result.is_ok())?;
    p10::finish_isolated_fixture(persisted.root, persisted.immutable_verification, &result)?;
    if result.is_ok() && transfer_exercise == TransferExercise::SourceDispatch {
        let counts = counts.ok_or("Home Source dispatch fixture proxy was absent")?;
        assert_eq!(
            counts.get_requests, 2,
            "one preflight Get and one recovery Get"
        );
        assert_eq!(
            counts.prepare_requests, 1,
            "exact replay sent a second Prepare"
        );
    }
    result?;
    restart_result?;
    if persisted.immutable_verification {
        eprintln!("P10 authentic immutable verification evidence retained in isolated test root");
    }
    Ok(())
}
