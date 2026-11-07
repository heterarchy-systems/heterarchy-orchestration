//! Real isolated HTTP recovery of an explicitly synthetic pre-hold slot.
//!
//! The fixture first creates a current V2 approval through the real Broker,
//! Forge, and Orchestration services. It then stops the Orchestration owner
//! and converts only that test-owned isolated decision into the `PRE_HOLD`
//! shape before restarting the current V62 service. This tests the legacy recovery
//! HTTP boundary; it does not claim that the approval came from a V59 writer.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_backend::client::{
    ClientError, OrchestrationHttpClient, ReadbackTarget,
};
use heterarchy_orchestration_backend::wire::leadership::LeadershipQueryRequest;
use heterarchy_orchestration_backend::wire::legacy_slot_recovery::{
    LegacySlotRecoveryIntentWire, LegacySlotRecoveryReadbackRequest, LegacySlotRecoveryReceiptWire,
    LegacySlotRecoveryRequest,
};
use heterarchy_orchestration_backend::wire::profiles::WorkspaceProfilesQueryRequest;
use heterarchy_orchestration_backend::wire::team_commands::{
    TeamExpansionSlotV2Request, TeamJoinV2Request, TeamMutationReaderV2Wire, TeamMutationV2Response,
};
use heterarchy_orchestration_backend::wire::team_expansion as expansion_wire;
use heterarchy_orchestration_backend::wire::team_expansion_v2 as expansion_v2_wire;
use heterarchy_orchestration_backend::wire::teams::{TeamGetQueryRequest, TeamListQueryRequest};
use heterarchy_orchestration_backend::{BackendError, serve_with_runtime};
use heterarchy_orchestration_operations::BrokerClusterConfig;
use hyper::header::HeaderValue;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    BootstrapHttpFlow, ForgeExecutionService, ForgeHost, MemberTarget, ReadyFixtureMember,
    RealBrokerAgentRequestFlow, TestResult,
};

const RECOVERY_PROXY_MAX_REQUEST_BYTES: usize = 72 * 1024;
const FIXTURE_OPERATIONS_SCHEMA_VERSION: i64 = 62;

fn stage<T, E: std::fmt::Debug>(name: &str, result: Result<T, E>) -> Result<T, String> {
    result.map_err(|error| format!("stage={name}: {error:?}"))
}

struct IsolatedFixture {
    root: PathBuf,
    data_root: PathBuf,
    destination: Uuid,
    broker_cluster: BrokerClusterConfig,
    session_server: super::InternalSessionServerHandle,
    backend: super::ForgeBackendId,
    issuer: super::OrchestrationIssuerId,
    source: ForgeHost,
    activity_id: Uuid,
    activity_runtime: super::RuntimeInstanceId,
    now_ms: i64,
    freeze: super::AgentOperationsFreeze,
    prepare: super::ImportPrepareRequest,
    token: heterarchy_orchestration_auth::McpTokenRuntime,
    bearer: HeaderValue,
    principal: AuthenticatedPrincipal,
    non_operator: super::reviewer_qualification_rule::NonOperatorCredential,
}

struct RunningBackend {
    address: SocketAddr,
    client: OrchestrationHttpClient,
    cancellation: CancellationToken,
    task: Option<JoinHandle<Result<(), BackendError>>>,
}

impl Drop for RunningBackend {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct LegacyApprovalSeed {
    workspace: super::WorkspaceQueryScope,
    activity_id: Uuid,
    workspace_leader: MemberIdentity,
    target: MemberIdentity,
    team_id: Uuid,
    request_id: Uuid,
    decision_id: Uuid,
    workspace_leadership_revision: i64,
    requester_leadership_revision: i64,
    expected_team_revision: u64,
    expected_capacity_revision: u64,
    expected_source_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemberIdentity {
    session_id: Uuid,
    agent_id: Uuid,
    logical_participant: Uuid,
    workspace_participation: Uuid,
    profile_id: Uuid,
}

fn member_identity(member: &ReadyFixtureMember, participation_id: Uuid) -> MemberIdentity {
    MemberIdentity {
        session_id: member.session_id,
        agent_id: member.agent_id,
        logical_participant: member.participant_id,
        workspace_participation: participation_id,
        profile_id: member.profile_id,
    }
}

fn assert_distinct_profile(candidate: &ReadyFixtureMember, existing: &ReadyFixtureMember) {
    assert_ne!(candidate.profile_id, existing.profile_id);
    assert_ne!(candidate.session_id, existing.session_id);
    assert_ne!(candidate.agent_id, existing.agent_id);
    assert_ne!(candidate.participant_id, existing.participant_id);
}

async fn assert_workspace_profile_target(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &super::WorkspaceQueryScope,
    reader: &ReadyFixtureMember,
    target: &ReadyFixtureMember,
) -> TestResult {
    let projection = client
        .workspace_profiles(
            bearer,
            &WorkspaceProfilesQueryRequest {
                scope: scope.clone(),
                session_id: Some(reader.session_id),
            },
        )
        .await?;
    let profiles = projection
        .profiles
        .iter()
        .filter(|profile| profile.profile.profile_id == target.profile_id)
        .count();
    assert_eq!(
        profiles, 1,
        "target Profile is not source-backed in Workspace"
    );
    let mappings = projection
        .session_profiles
        .iter()
        .filter(|mapping| mapping.session_id == target.session_id)
        .collect::<Vec<_>>();
    assert_eq!(
        mappings.len(),
        1,
        "target Session Profile mapping is ambiguous"
    );
    assert_eq!(mappings[0].profile_id, target.profile_id);
    Ok(())
}

async fn assert_profile_unassigned_from_all_teams(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &super::WorkspaceQueryScope,
    reader: &ReadyFixtureMember,
    expected_team_id: Uuid,
    target: &ReadyFixtureMember,
) -> TestResult {
    let teams = client
        .team_list(
            bearer,
            &TeamListQueryRequest {
                scope: scope.clone(),
                session_id: Some(reader.session_id),
                limit: 64,
            },
        )
        .await?;
    if teams.truncated {
        return Err("Workspace Team list is truncated during target eligibility check".into());
    }
    assert!(
        teams
            .teams
            .iter()
            .any(|team| team.team_id == expected_team_id)
    );
    for team in teams.teams {
        let snapshot = client
            .team_get(
                bearer,
                &TeamGetQueryRequest {
                    scope: scope.clone(),
                    session_id: Some(reader.session_id),
                    team_id: team.team_id,
                    request_id: None,
                },
            )
            .await?;
        if snapshot.members_truncated {
            return Err(format!(
                "Team roster is truncated during target eligibility check team_id={}",
                team.team_id
            )
            .into());
        }
        assert!(
            !snapshot
                .members
                .iter()
                .any(|member| member.profile_id == Some(target.profile_id)),
            "recovery target already has a Team membership"
        );
    }
    Ok(())
}

async fn read_profile_participation_id(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &super::WorkspaceQueryScope,
    activity_id: Uuid,
    member: &ReadyFixtureMember,
) -> TestResult<Uuid> {
    let target = MemberTarget {
        scope: scope.clone(),
        activity_id,
        session_id: member.session_id,
    };
    let request_id =
        super::member_registration_request_id(scope.workspace_id, activity_id, member.session_id);
    let readback = client
        .member_registration(
            bearer,
            &super::MemberRegistrationQueryRequest {
                target: target.clone(),
                request_id,
            },
        )
        .await?;
    assert_eq!(readback.target, target);
    assert_eq!(readback.request_id, request_id);
    let progress = readback
        .progress
        .as_ref()
        .ok_or("Profile-linked Ready registration progress is absent")?;
    assert_eq!(progress.workspace_id, scope.workspace_id);
    assert_eq!(progress.activity_id, activity_id);
    assert_eq!(progress.session_id, member.session_id);
    assert_eq!(progress.agent_id, member.agent_id);
    assert_eq!(progress.phase, super::MemberPhaseWire::Ready);
    let commit = readback
        .commit
        .as_ref()
        .ok_or("Profile-linked Ready membership commit is absent")?;
    let activation = readback
        .activation
        .as_ref()
        .ok_or("Profile-linked Ready source activation is absent")?;
    assert_eq!(commit.request_id, request_id);
    assert_eq!(activation.request_id, request_id);
    assert_eq!(commit.participant_id, progress.participant_id);
    assert_eq!(activation.participant_id, progress.participant_id);
    assert_eq!(activation.agent_id, member.agent_id);
    assert_eq!(activation.phase, super::MemberPhaseWire::Ready);

    // Profile-linked Ready registration derives this stable identity from the
    // exact Profile and Workspace. Runtime maps the same UUID into
    // WorkspaceParticipationId; take it from the fresh immutable readback,
    // not from the cached fixture member.
    Ok(progress.participant_id)
}

async fn read_team_member_participation_id(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &super::WorkspaceQueryScope,
    reader_session_id: Uuid,
    team_id: Uuid,
    profile_id: Uuid,
) -> TestResult<Uuid> {
    let snapshot = client
        .team_get(
            bearer,
            &TeamGetQueryRequest {
                scope: scope.clone(),
                session_id: Some(reader_session_id),
                team_id,
                request_id: None,
            },
        )
        .await?;
    if snapshot.members_truncated {
        return Err("Team roster truncated while reading source participation".into());
    }
    let matches = snapshot
        .members
        .iter()
        .filter(|member| member.profile_id == Some(profile_id) && member.state == "joined")
        .collect::<Vec<_>>();
    let [member] = matches.as_slice() else {
        return Err("source Team does not have one exact joined Profile member".into());
    };
    Ok(member.participation_id)
}

#[derive(Debug, Clone, Copy)]
struct TeamSnapshot {
    revision: u64,
    capacity_revision: u64,
    max_members: u8,
    member_count: usize,
}

#[derive(Clone, Copy)]
struct RecoveryApprovalContext<'a> {
    client: &'a OrchestrationHttpClient,
    bearer: &'a HeaderValue,
    scope: &'a super::WorkspaceQueryScope,
    activity_id: Uuid,
    workspace_leadership_revision: i64,
    workspace_leader: &'a MemberIdentity,
    team_leader: &'a MemberIdentity,
    team_id: Uuid,
    target: &'a MemberIdentity,
    expected_source_sequence: u64,
}

struct RecoverySourceTeam {
    creator: MemberTarget,
    workspace_leader: ReadyFixtureMember,
    team_leader: ReadyFixtureMember,
    team_id: Uuid,
    workspace_leadership_revision: i64,
    source_sequence: u64,
}

/// Run the source-backed fixture, stop its owner, convert one isolated current
/// approval to the explicit synthetic `PRE_HOLD` fixture, then restart V62 and
/// exercise real recovery, slot assignment, and `TeamJoin` HTTP routes.
pub(super) async fn run_synthetic_pre_hold_recovery(
    broker_cluster: BrokerClusterConfig,
) -> TestResult {
    let fixture = create_fixture(broker_cluster).await?;
    let result = run_two_stage_recovery(&fixture).await;
    let source_stop =
        super::stop_bootstrap_source_fixtures(fixture.source, fixture.session_server).await;
    let (source_scope, immutable_verification) = match source_stop {
        Ok(stopped) => stopped,
        Err(source_error) => {
            eprintln!(
                "preserving isolated synthetic recovery fixture at {} after source shutdown failure; test={:?}, source_shutdown={source_error:?}",
                fixture.root.display(),
                result.as_ref().err(),
            );
            return match result {
                Err(test_error) => Err(test_error),
                Ok(()) => Err(source_error),
            };
        }
    };
    assert_eq!(super::scope_wire(&source_scope), fixture.prepare.scope);
    assert!(!immutable_verification);
    if result.is_err() {
        eprintln!(
            "preserving isolated synthetic recovery fixture at {} after test failure: {:?}",
            fixture.root.display(),
            result.as_ref().err(),
        );
        return result;
    }
    super::p10::finish_isolated_fixture(&fixture.root, immutable_verification, &result)?;
    result
}

async fn create_fixture(broker_cluster: BrokerClusterConfig) -> TestResult<IsolatedFixture> {
    let (root, data_root, destination) = super::fixture_storage(broker_cluster)?;
    eprintln!(
        "synthetic PRE_HOLD recovery test fixture root: {}",
        root.display()
    );
    let (session_server, backend, issuer, source) =
        super::start_isolated_forge_fixture(&root, &data_root, destination, false).await?;
    let setup = async {
        let (activity_id, activity_runtime, now_ms) = super::bootstrap_fixture_activity(&source)?;
        let (freeze, prepare) = super::freeze_fixture_source(&source, destination).await?;
        let (token, bearer, principal) = super::token_authority(&root)?;
        let non_operator =
            super::reviewer_qualification_rule::issue_nonoperator_credential(&token)?;
        Ok((
            activity_id.into_uuid(),
            activity_runtime,
            now_ms,
            freeze,
            prepare,
            token,
            bearer,
            principal,
            non_operator,
        ))
    }
    .await;
    match setup {
        Ok((
            activity_id,
            activity_runtime,
            now_ms,
            freeze,
            prepare,
            token,
            bearer,
            principal,
            non_operator,
        )) => Ok(IsolatedFixture {
            root,
            data_root,
            destination,
            broker_cluster,
            session_server,
            backend,
            issuer,
            source,
            activity_id,
            activity_runtime,
            now_ms,
            freeze,
            prepare,
            token,
            bearer,
            principal,
            non_operator,
        }),
        Err(error) => {
            let source_stop = super::stop_bootstrap_source_fixtures(source, session_server).await;
            if source_stop.is_ok() {
                let failed_setup: TestResult =
                    Err(std::io::Error::other("isolated recovery fixture setup failed").into());
                super::p10::finish_isolated_fixture(&root, false, &failed_setup)?;
            } else {
                eprintln!(
                    "preserving isolated synthetic recovery fixture at {} after setup/shutdown failure; setup={:?}, source_shutdown={:?}",
                    root.display(),
                    error,
                    source_stop.as_ref().err(),
                );
            }
            Err(error)
        }
    }
}

fn start_backend(fixture: &IsolatedFixture) -> TestResult<RunningBackend> {
    let runtime = super::configured_fixture_runtime(
        &fixture.data_root,
        fixture.broker_cluster,
        fixture.principal,
        fixture.issuer,
        fixture.backend,
        fixture.now_ms,
    )?;
    if runtime.destination_id() != fixture.destination {
        return Err("restarted Orchestration owner opened a different destination".into());
    }
    let address = super::fixture_http_address()?;
    let client = OrchestrationHttpClient::new(address, super::DEADLINE)?;
    let source_authority = super::fixture_source_authority(&fixture.source)?;
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(serve_with_runtime(
        Arc::new(runtime),
        address,
        fixture.token.client(),
        Some(source_authority),
        cancellation.clone(),
    ));
    Ok(RunningBackend {
        address,
        client,
        cancellation,
        task: Some(task),
    })
}

async fn stop_backend(mut backend: RunningBackend) -> TestResult {
    backend.cancellation.cancel();
    let mut task = backend
        .task
        .take()
        .ok_or("Orchestration owner task was already joined")?;
    if let Ok(joined) = tokio::time::timeout(super::DEADLINE, &mut task).await {
        joined??;
        return Ok(());
    }
    task.abort();
    match tokio::time::timeout(super::DEADLINE, &mut task).await {
        Ok(Ok(Ok(()))) => {
            Err("Orchestration owner exceeded its deadline and was aborted before joining".into())
        }
        Ok(Ok(Err(error))) => {
            Err(format!("Orchestration owner failed after abort request: {error:?}").into())
        }
        Ok(Err(error)) => {
            Err(format!("Orchestration owner task ended after abort request: {error:?}").into())
        }
        Err(_) => Err(
            "Orchestration owner termination is unconfirmed; preserving the fixture root".into(),
        ),
    }
}

async fn run_two_stage_recovery(fixture: &IsolatedFixture) -> TestResult {
    let first_backend = start_backend(fixture)?;
    let staged = prepare_current_v2_approval(fixture, &first_backend).await;
    stop_backend(first_backend).await?;
    let seed = staged?;

    synthesize_pre_hold_after_owner_stop(&fixture.root, &fixture.data_root, &seed)?;

    let second_backend = start_backend(fixture)?;
    let recovery = match await_restarted_backend(fixture, &second_backend).await {
        Ok(()) => recover_assign_and_join(fixture, &second_backend, &seed).await,
        Err(error) => Err(error),
    };
    let shutdown = stop_backend(second_backend).await;
    match (recovery, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(test_error), Ok(())) => Err(format!(
            "stage=post-restart recovery and membership lifecycle: {test_error:?}"
        )
        .into()),
        (Ok(()), Err(shutdown_error)) => Err(format!(
            "stage=restarted Backend shutdown: {shutdown_error:?}"
        )
        .into()),
        (Err(test_error), Err(shutdown_error)) => Err(format!(
            "stage=post-restart recovery failed ({test_error:?}); Backend shutdown failed ({shutdown_error:?})"
        )
        .into()),
    }
}

async fn await_restarted_backend(
    fixture: &IsolatedFixture,
    backend: &RunningBackend,
) -> TestResult {
    // `start_backend` owns a spawned server task; confirm the reopened durable
    // import before sending a leadership or recovery query to that listener.
    let response = tokio::time::timeout(super::DEADLINE, async {
        loop {
            match backend
                .client
                .import_progress(&fixture.bearer, fixture.prepare.migration_id)
                .await
            {
                Err(ClientError::Unavailable) => tokio::task::yield_now().await,
                response => return response,
            }
        }
    })
    .await
    .map_err(|_| "stage=restarted Backend import readback timed out".to_owned())?;
    let progress = stage("restarted Backend import readback", response)?
        .ok_or("stage=restarted Backend durable import is absent")?;
    if progress.migration_id != fixture.prepare.migration_id
        || progress.workspace_id != fixture.prepare.scope.workspace_id
        || progress.destination_id != fixture.destination
        || progress.phase != super::ImportPhaseWire::Verified
    {
        return Err("stage=restarted Backend durable import identity or phase differs".into());
    }
    Ok(())
}

async fn prepare_current_v2_approval(
    fixture: &IsolatedFixture,
    backend: &RunningBackend,
) -> TestResult<LegacyApprovalSeed> {
    let final_source_sequence = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let service = ForgeExecutionService {
        backend: fixture.backend,
        issuer: fixture.issuer,
        activity_runtime: fixture.activity_runtime,
    };
    let bootstrap = BootstrapHttpFlow {
        client: &backend.client,
        bearer: &fixture.bearer,
        backend_address: backend.address,
        final_source_sequence,
        principal: fixture.principal,
        non_operator: Some(&fixture.non_operator),
        prepare: &fixture.prepare,
        source: &fixture.source,
        freeze: &fixture.freeze,
        destination: fixture.destination,
        actual_broker: true,
        transfer_exercise: super::TransferExercise::None,
        broker_case: Some((&fixture.source, fixture.activity_id, service)),
    };
    super::await_import_server(
        &backend.client,
        &fixture.bearer,
        fixture.prepare.migration_id,
    )
    .await
    .map_err(|error| format!("synthetic recovery Backend readiness failed: {error:?}"))?;
    let progress = super::import_and_verify_workspace(&bootstrap).await?;
    let agent_flow = RealBrokerAgentRequestFlow {
        client: &backend.client,
        bearer: &fixture.bearer,
        backend_address: backend.address,
        principal: fixture.principal,
        scope: &fixture.prepare.scope,
        progress: &progress,
        source: &fixture.source,
        activity_id: fixture.activity_id,
        service,
        non_operator: &fixture.non_operator,
        transfer_exercise: super::TransferExercise::None,
    };
    prepare_workspace_members_and_team(fixture, &agent_flow).await
}

async fn prepare_workspace_members_and_team(
    fixture: &IsolatedFixture,
    flow: &RealBrokerAgentRequestFlow<'_>,
) -> TestResult<LegacyApprovalSeed> {
    let source_team = prepare_source_backed_team(fixture, flow).await?;
    let recovery_target = prepare_unassigned_recovery_target(fixture, flow, &source_team).await?;
    let workspace_leader_participation_id = read_team_member_participation_id(
        flow.client,
        flow.bearer,
        &source_team.creator.scope,
        source_team.workspace_leader.session_id,
        source_team.team_id,
        source_team.workspace_leader.profile_id,
    )
    .await?;
    let workspace_leader = member_identity(
        &source_team.workspace_leader,
        workspace_leader_participation_id,
    );
    let team_leader_participation_id = read_team_member_participation_id(
        flow.client,
        flow.bearer,
        &source_team.creator.scope,
        source_team.workspace_leader.session_id,
        source_team.team_id,
        source_team.team_leader.profile_id,
    )
    .await?;
    let team_leader = member_identity(&source_team.team_leader, team_leader_participation_id);
    let target_participation_id = read_profile_participation_id(
        flow.client,
        flow.bearer,
        &source_team.creator.scope,
        fixture.activity_id,
        &recovery_target,
    )
    .await?;
    let target = member_identity(&recovery_target, target_participation_id);

    create_and_finalize_v2_approval(&RecoveryApprovalContext {
        client: flow.client,
        bearer: flow.bearer,
        scope: &source_team.creator.scope,
        activity_id: fixture.activity_id,
        workspace_leadership_revision: source_team.workspace_leadership_revision,
        workspace_leader: &workspace_leader,
        team_leader: &team_leader,
        team_id: source_team.team_id,
        target: &target,
        expected_source_sequence: source_team.source_sequence,
    })
    .await
}

async fn prepare_source_backed_team(
    fixture: &IsolatedFixture,
    flow: &RealBrokerAgentRequestFlow<'_>,
) -> TestResult<RecoverySourceTeam> {
    let operator_session = super::open_verified_operator_session(flow).await?;
    let creator = MemberTarget {
        scope: fixture.prepare.scope.clone(),
        activity_id: fixture.activity_id,
        session_id: operator_session.session_id,
    };
    let request_id =
        super::assert_undelegated_member_absent(flow.client, flow.bearer, &creator).await?;
    super::assert_verified_import_handoff(flow).await?;
    let first =
        super::register_and_admit_operator_member(flow, &creator, request_id, operator_session)
            .await?;
    let members = super::provision_team_flow_members(flow, &creator, &first).await?;
    super::assert_reviewer_participation_for_team_flow(flow, &creator, &members).await?;
    super::assert_no_initial_workspace_leadership(flow, &creator).await?;

    let assignments = super::issue_bootstrap_forge_assignment_offer(
        &fixture.source,
        fixture.principal,
        fixture.activity_id,
        &members.reviewer,
    )
    .await?;
    let source_team = super::assert_team_from_source_leadership(&super::SourceLeadershipTeamCase {
        client: flow.client,
        bearer: flow.bearer,
        source: &fixture.source,
        creator: &creator,
        leader: &members.reviewer,
        third: &members.mission_assignee,
        assignments: &assignments,
        principal: fixture.principal,
    })
    .await?;
    let source_cursor = read_source_cursor(
        flow.client,
        flow.bearer,
        &creator.scope,
        fixture.activity_id,
        members.reviewer.session_id,
    )
    .await?;
    assert_eq!(
        source_cursor, source_team.mission_assignment.source_sequence,
        "Team setup introduced an unaccounted Forge source fact"
    );

    Ok(RecoverySourceTeam {
        creator,
        workspace_leader: members.reviewer,
        team_leader: members.mission_assignee,
        team_id: source_team.team_id,
        workspace_leadership_revision: source_team.workspace_leadership_revision,
        source_sequence: source_cursor,
    })
}

async fn prepare_unassigned_recovery_target(
    fixture: &IsolatedFixture,
    flow: &RealBrokerAgentRequestFlow<'_>,
    source_team: &RecoverySourceTeam,
) -> TestResult<ReadyFixtureMember> {
    let recovery_target = super::assert_ready_profile_member(
        flow.client,
        flow.bearer,
        fixture.principal,
        &fixture.source,
        &source_team.creator,
        "isolated-synthetic-legacy-recovery-target",
    )
    .await?;
    assert_distinct_profile(&recovery_target, &source_team.workspace_leader);
    assert_distinct_profile(&recovery_target, &source_team.team_leader);
    assert_workspace_profile_target(
        flow.client,
        flow.bearer,
        &source_team.creator.scope,
        &source_team.workspace_leader,
        &recovery_target,
    )
    .await?;
    assert_profile_unassigned_from_all_teams(
        flow.client,
        flow.bearer,
        &source_team.creator.scope,
        &source_team.workspace_leader,
        source_team.team_id,
        &recovery_target,
    )
    .await?;
    Ok(recovery_target)
}

async fn read_team_snapshot(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &super::WorkspaceQueryScope,
    reader_activity_id: Uuid,
    reader_session_id: Uuid,
    team_id: Uuid,
) -> TestResult<TeamSnapshot> {
    let snapshot = client
        .team_get(
            bearer,
            &TeamGetQueryRequest {
                scope: scope.clone(),
                session_id: Some(reader_session_id),
                team_id,
                request_id: None,
            },
        )
        .await?;
    let team = snapshot.team.ok_or("legacy recovery Team disappeared")?;
    if team.state != "active" {
        return Err("legacy recovery Team is not active".into());
    }
    if snapshot.members_truncated {
        return Err("legacy recovery Team roster is truncated".into());
    }
    let capacity = client
        .team_capacity(
            bearer,
            &expansion_wire::TeamCapacityQueryRequest {
                actor: expansion_actor(scope, reader_activity_id, reader_session_id),
                team_id,
            },
        )
        .await?
        .snapshot
        .ok_or("legacy recovery Team has no explicit capacity")?;
    Ok(TeamSnapshot {
        revision: team.revision,
        capacity_revision: capacity.revision,
        max_members: capacity.max_members,
        member_count: snapshot.members.len(),
    })
}

fn expansion_actor(
    scope: &super::WorkspaceQueryScope,
    activity_id: Uuid,
    session_id: Uuid,
) -> expansion_wire::TeamExpansionActorWire {
    expansion_wire::TeamExpansionActorWire {
        scope: scope.clone(),
        activity_id,
        session_id,
    }
}

async fn team_leadership_revision(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &super::WorkspaceQueryScope,
    team_id: Uuid,
    leader: &MemberIdentity,
) -> TestResult<i64> {
    let leadership = client
        .leadership_view(
            bearer,
            &LeadershipQueryRequest {
                scope: scope.clone(),
                session_id: Some(leader.session_id),
                activity_id: None,
                team_id: Some(team_id),
                review_after: None,
                limit: 10,
            },
        )
        .await?
        .leadership
        .ok_or("legacy recovery Team Leader is absent")?;
    assert_eq!(
        leadership.leader_participant_id,
        Some(leader.logical_participant)
    );
    Ok(leadership.leadership_revision)
}

async fn read_source_cursor(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &super::WorkspaceQueryScope,
    activity_id: Uuid,
    reader_session_id: Uuid,
) -> TestResult<u64> {
    client
        .agent_coordination_get(
            bearer,
            &super::CoordinationQueryRequest {
                scope: scope.clone(),
                activity_id,
                session_id: Some(reader_session_id),
            },
        )
        .await?
        .catch_up
        .map(|catch_up| catch_up.applied_through)
        .ok_or_else(|| "legacy recovery fixture has no Forge source cursor".into())
}

async fn create_and_finalize_v2_approval(
    context: &RecoveryApprovalContext<'_>,
) -> TestResult<LegacyApprovalSeed> {
    let initial = read_team_snapshot(
        context.client,
        context.bearer,
        context.scope,
        context.activity_id,
        context.workspace_leader.session_id,
        context.team_id,
    )
    .await?;
    assert_eq!(initial.member_count, 3);
    assert_eq!(initial.max_members, 3);
    let team_leadership_revision = team_leadership_revision(
        context.client,
        context.bearer,
        context.scope,
        context.team_id,
        context.team_leader,
    )
    .await?;
    let current = set_recovery_capacity(context, initial).await?;
    let request_receipt = request_recovery_slot(context, current, team_leadership_revision).await?;
    let decision_id = Uuid::now_v7();
    let approval = v2_approval_intent(context, current, &request_receipt, decision_id);
    record_pending_v2_approval(context, &approval).await?;
    finalize_v2_approval(context, &approval, &request_receipt).await?;
    assert_v2_approval_readback(context, &approval).await?;
    let final_source_sequence = read_source_cursor(
        context.client,
        context.bearer,
        context.scope,
        context.activity_id,
        context.workspace_leader.session_id,
    )
    .await?;
    assert_eq!(final_source_sequence, context.expected_source_sequence);

    Ok(LegacyApprovalSeed {
        workspace: context.scope.clone(),
        activity_id: context.activity_id,
        workspace_leader: *context.workspace_leader,
        target: *context.target,
        team_id: context.team_id,
        request_id: request_receipt.request_id,
        decision_id,
        workspace_leadership_revision: context.workspace_leadership_revision,
        requester_leadership_revision: request_receipt.requester_leadership_revision,
        expected_team_revision: current.revision,
        expected_capacity_revision: current.capacity_revision,
        expected_source_sequence: final_source_sequence,
    })
}

async fn set_recovery_capacity(
    context: &RecoveryApprovalContext<'_>,
    initial: TeamSnapshot,
) -> TestResult<TeamSnapshot> {
    let request_id = Uuid::now_v7();
    let request = expansion_wire::TeamExpansionMutationRequest {
        actor: expansion_actor(
            context.scope,
            context.activity_id,
            context.workspace_leader.session_id,
        ),
        action: expansion_wire::TeamExpansionMutationActionWire::SetCapacity {
            command: expansion_wire::TeamCapacitySetAction {
                team_id: context.team_id,
                request_id,
                expected_team_revision: initial.revision,
                expected_capacity_revision: initial.capacity_revision,
                expected_leadership_revision: context.workspace_leadership_revision,
                max_members: 4,
            },
        },
    };
    let response = context
        .client
        .mutate_team_expansion(context.bearer, &request)
        .await?;
    let expansion_wire::TeamExpansionMutationResultWire::CapacitySet { receipt } = response.result
    else {
        return Err("capacity update returned a non-capacity receipt".into());
    };
    assert_eq!(receipt.request_id, request_id);
    assert!(!receipt.replayed);
    assert_eq!(receipt.snapshot.max_members, 4);
    let current = read_team_snapshot(
        context.client,
        context.bearer,
        context.scope,
        context.activity_id,
        context.workspace_leader.session_id,
        context.team_id,
    )
    .await?;
    assert_eq!(current.revision, initial.revision + 1);
    assert_eq!(current.capacity_revision, initial.capacity_revision + 1);
    assert_eq!(current.max_members, 4);
    Ok(current)
}

async fn request_recovery_slot(
    context: &RecoveryApprovalContext<'_>,
    coordinates: TeamSnapshot,
    team_leadership_revision: i64,
) -> TestResult<expansion_wire::TeamExpansionRequestReceiptWire> {
    let request = expansion_wire::TeamExpansionMutationRequest {
        actor: expansion_actor(
            context.scope,
            context.activity_id,
            context.team_leader.session_id,
        ),
        action: expansion_wire::TeamExpansionMutationActionWire::Request {
            command: expansion_wire::TeamExpansionRequestAction {
                team_id: context.team_id,
                request_id: Uuid::now_v7(),
                expected_team_revision: coordinates.revision,
                expected_capacity_revision: coordinates.capacity_revision,
                expected_leadership_revision: team_leadership_revision,
                requested_count: 1,
                reason: "one synthetic pre-hold recovery slot".to_owned(),
            },
        },
    };
    let response = context
        .client
        .mutate_team_expansion(context.bearer, &request)
        .await?;
    let expansion_wire::TeamExpansionMutationResultWire::Request { receipt } = response.result
    else {
        return Err("Team Leader request returned a non-request receipt".into());
    };
    assert_eq!(receipt.requested_count, 1);
    assert_eq!(receipt.expected_team_revision, coordinates.revision);
    assert_eq!(receipt.capacity_revision, coordinates.capacity_revision);
    assert_eq!(
        receipt.requester_leadership_revision,
        team_leadership_revision
    );
    assert_eq!(receipt.reason, "one synthetic pre-hold recovery slot");
    assert!(!receipt.replayed);
    assert_member_actor_receipt(&receipt.actor, context.activity_id, context.team_leader);
    Ok(*receipt)
}

fn v2_approval_intent(
    context: &RecoveryApprovalContext<'_>,
    coordinates: TeamSnapshot,
    request: &expansion_wire::TeamExpansionRequestReceiptWire,
    decision_id: Uuid,
) -> expansion_v2_wire::TeamExpansionDecisionV2Request {
    expansion_v2_wire::TeamExpansionDecisionV2Request {
        actor: expansion_actor(
            context.scope,
            context.activity_id,
            context.workspace_leader.session_id,
        ),
        command: expansion_wire::TeamExpansionDecisionAction {
            team_id: context.team_id,
            request_id: request.request_id,
            decision_id,
            expected_team_revision: coordinates.revision,
            expected_capacity_revision: coordinates.capacity_revision,
            expected_leadership_revision: context.workspace_leadership_revision,
            expected_requester_leadership_revision: request.requester_leadership_revision,
            decision: expansion_wire::TeamExpansionDecisionWire::Approve { approved_count: 1 },
        },
        slot_plan: vec![expansion_v2_wire::TeamExpansionApprovalSlotV2Wire {
            ordinal: 0,
            target: expansion_v2_wire::TeamExpansionApprovalTargetV2Wire {
                profile_id: context.target.profile_id,
                participation_id: context.target.workspace_participation,
            },
        }],
    }
}

async fn record_pending_v2_approval(
    context: &RecoveryApprovalContext<'_>,
    approval: &expansion_v2_wire::TeamExpansionDecisionV2Request,
) -> TestResult {
    let response = context
        .client
        .mutate_team_expansion_decision_v2(context.bearer, approval)
        .await?;
    assert_eq!(response.request, *approval);
    assert_eq!(response.receipt.slot_plan, approval.slot_plan);
    assert_eq!(
        response.receipt.decision.state,
        expansion_wire::TeamExpansionDecisionStateWire::ApprovedPendingAnchor
    );
    assert!(!response.receipt.decision.replayed);
    assert_eq!(
        response.receipt.decision.request_id,
        approval.command.request_id
    );
    assert_eq!(
        response.receipt.decision.decision_id,
        approval.command.decision_id
    );
    assert_member_actor_receipt(
        &response.receipt.decision.actor,
        context.activity_id,
        context.workspace_leader,
    );
    Ok(())
}

async fn finalize_v2_approval(
    context: &RecoveryApprovalContext<'_>,
    approval: &expansion_v2_wire::TeamExpansionDecisionV2Request,
    request: &expansion_wire::TeamExpansionRequestReceiptWire,
) -> TestResult {
    let finalizer = expansion_wire::TeamExpansionMutationRequest {
        actor: expansion_actor(
            context.scope,
            context.activity_id,
            context.workspace_leader.session_id,
        ),
        action: expansion_wire::TeamExpansionMutationActionWire::FinalizeAnchor {
            command: expansion_wire::TeamExpansionFinalizeAnchorAction {
                request_id: request.request_id,
                decision_id: approval.command.decision_id,
                expected_leadership_revision: context.workspace_leadership_revision,
            },
        },
    };
    let response = context
        .client
        .mutate_team_expansion(context.bearer, &finalizer)
        .await?;
    let expansion_wire::TeamExpansionMutationResultWire::Decision { receipt } = response.result
    else {
        return Err("cooldown finalizer returned a non-decision receipt".into());
    };
    assert!(!receipt.replayed);
    assert_finalized_recovery_anchor(
        context,
        &receipt,
        request.request_id,
        approval.command.decision_id,
    )?;

    let readback = context
        .client
        .team_expansion_readback(
            context.bearer,
            &expansion_wire::TeamExpansionReadbackRequest {
                intent: finalizer,
                current_actor: expansion_actor(
                    context.scope,
                    context.activity_id,
                    context.workspace_leader.session_id,
                ),
            },
        )
        .await?;
    let Some(expansion_wire::TeamExpansionMutationResultWire::Decision { receipt: stored }) =
        readback.result
    else {
        return Err("finalized Team decision readback is absent".into());
    };
    assert_finalized_recovery_anchor(
        context,
        &stored,
        request.request_id,
        approval.command.decision_id,
    )
}

fn assert_finalized_recovery_anchor(
    context: &RecoveryApprovalContext<'_>,
    receipt: &expansion_wire::TeamExpansionDecisionReceiptWire,
    request_id: Uuid,
    decision_id: Uuid,
) -> TestResult {
    assert_eq!(receipt.request_id, request_id);
    assert_eq!(receipt.decision_id, decision_id);
    assert_eq!(receipt.team_id, context.team_id);
    assert_eq!(receipt.approved_count, 1);
    assert_eq!(
        receipt.state,
        expansion_wire::TeamExpansionDecisionStateWire::Approved
    );
    let started = receipt
        .cooldown_started_at_ms
        .ok_or("cooldown start is absent")?;
    let until = receipt.cooldown_until_ms.ok_or("cooldown end is absent")?;
    assert_eq!(until.checked_sub(started), Some(60 * 60 * 1_000_i64));
    assert_eq!(
        receipt.finalized_by_leadership_revision,
        Some(context.workspace_leadership_revision)
    );
    let actor = receipt
        .finalized_by
        .as_ref()
        .ok_or("cooldown finalizer is absent")?;
    assert_member_actor_receipt(actor, context.activity_id, context.workspace_leader);
    Ok(())
}

fn assert_member_actor_receipt(
    actual: &expansion_wire::TeamExpansionActorReceiptWire,
    activity_id: Uuid,
    member: &MemberIdentity,
) {
    assert_eq!(actual.activity_id, activity_id);
    assert_eq!(actual.agent_id, member.agent_id);
    assert_eq!(actual.session_id, member.session_id);
    assert_eq!(actual.participation_id, member.workspace_participation);
    assert_eq!(actual.profile_id, member.profile_id);
    assert_eq!(actual.participant_id, member.logical_participant);
}

async fn assert_v2_approval_readback(
    context: &RecoveryApprovalContext<'_>,
    approval: &expansion_v2_wire::TeamExpansionDecisionV2Request,
) -> TestResult {
    let readback_request = expansion_v2_wire::TeamExpansionDecisionV2ReadbackRequest {
        intent: approval.clone(),
        current_actor: expansion_actor(
            context.scope,
            context.activity_id,
            context.workspace_leader.session_id,
        ),
    };
    let readback = context
        .client
        .team_expansion_decision_v2_readback(context.bearer, &readback_request)
        .await?;
    assert_eq!(readback.request, readback_request);
    let stored = readback
        .result
        .ok_or("source-backed V2 target plan disappeared before conversion")?;
    assert_eq!(stored.slot_plan, approval.slot_plan);
    assert_eq!(stored.decision.decision_id, approval.command.decision_id);
    assert_eq!(stored.decision.request_id, approval.command.request_id);
    assert_eq!(
        stored.decision.state,
        expansion_wire::TeamExpansionDecisionStateWire::Approved
    );
    Ok(())
}

fn synthesize_pre_hold_after_owner_stop(
    root: &Path,
    data_root: &Path,
    seed: &LegacyApprovalSeed,
) -> TestResult {
    validate_test_operations_path(root, data_root)?;
    let database = data_root.join("agent-operations.sqlite3");
    if !std::fs::symlink_metadata(&database)?.is_file() {
        return Err("test Operations path is not a regular database file".into());
    }
    let mut connection = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let source = validate_synthetic_v2_approval(&transaction, seed)?;
    install_synthetic_pre_hold_marker(&transaction, seed, &source)?;
    transaction.commit()?;
    verify_synthetic_conversion(&connection, &source.schema_objects)?;
    Ok(())
}

struct SyntheticHoldSource {
    schema_objects: Vec<(String, String, Option<String>)>,
    plans_delete_trigger: String,
    manifests_delete_trigger: String,
}

fn validate_test_operations_path(root: &Path, data_root: &Path) -> TestResult {
    let expected_data_root = root.join("orchestration");
    if root.parent() != Some(std::env::temp_dir().as_path())
        || !root
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.starts_with("cooperation-bootstrap-http-"))
        || data_root != expected_data_root
    {
        return Err("refusing to convert a non-test Operations database".into());
    }
    Ok(())
}

fn validate_synthetic_v2_approval(
    transaction: &Transaction<'_>,
    seed: &LegacyApprovalSeed,
) -> TestResult<SyntheticHoldSource> {
    let version: i64 = transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(
        version, FIXTURE_OPERATIONS_SCHEMA_VERSION,
        "fixture decision must be converted in the current test schema"
    );
    let schema_objects = schema_snapshot(transaction)?;
    assert_synthetic_approval_identity(transaction, seed)?;
    assert_synthetic_hold_plan(transaction, seed)?;
    ensure_slot_is_unassigned(transaction, seed)?;
    let existing_marker: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_workspace_hold_pre_hold_decisions WHERE workspace_id=?1 AND decision_id=?2)",
        params![seed.workspace.workspace_id.as_bytes(), seed.decision_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert!(
        !existing_marker,
        "new V2 approval already has a PRE_HOLD marker"
    );
    Ok(SyntheticHoldSource {
        schema_objects,
        plans_delete_trigger: immutable_delete_trigger_sql(
            transaction,
            "ops_team_expansion_workspace_hold_plans_no_delete",
        )?,
        manifests_delete_trigger: immutable_delete_trigger_sql(
            transaction,
            "ops_team_expansion_workspace_hold_manifests_no_delete",
        )?,
    })
}

fn assert_synthetic_approval_identity(
    transaction: &Transaction<'_>,
    seed: &LegacyApprovalSeed,
) -> TestResult {
    let decision: (String, Vec<u8>, Vec<u8>, i64) = transaction.query_row(
        "SELECT state,request_id,team_id,approved_count FROM ops_team_expansion_decisions WHERE workspace_id=?1 AND decision_id=?2",
        params![seed.workspace.workspace_id.as_bytes(), seed.decision_id.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    assert_eq!(decision.0, "APPROVED");
    assert_eq!(decision.1.as_slice(), seed.request_id.as_bytes());
    assert_eq!(decision.2.as_slice(), seed.team_id.as_bytes());
    assert_eq!(decision.3, 1);
    let manifest: (Vec<u8>, Vec<u8>, i64) = transaction.query_row(
        "SELECT request_id,team_id,slot_count FROM ops_team_expansion_workspace_hold_manifests WHERE workspace_id=?1 AND decision_id=?2",
        params![seed.workspace.workspace_id.as_bytes(), seed.decision_id.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(manifest.0.as_slice(), seed.request_id.as_bytes());
    assert_eq!(manifest.1.as_slice(), seed.team_id.as_bytes());
    assert_eq!(manifest.2, 1);
    Ok(())
}

fn assert_synthetic_hold_plan(
    transaction: &Transaction<'_>,
    seed: &LegacyApprovalSeed,
) -> TestResult {
    let plan: (i64, Vec<u8>, Vec<u8>) = transaction.query_row(
        "SELECT target_kind,target_profile_id,target_participation_id FROM ops_team_expansion_workspace_hold_plans WHERE workspace_id=?1 AND decision_id=?2 AND ordinal=0",
        params![seed.workspace.workspace_id.as_bytes(), seed.decision_id.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(
        plan.0, 0,
        "fixture slot must target an existing Workspace Profile"
    );
    assert_eq!(plan.1.as_slice(), seed.target.profile_id.as_bytes());
    assert_eq!(
        plan.2.as_slice(),
        seed.target.workspace_participation.as_bytes()
    );
    Ok(())
}

fn install_synthetic_pre_hold_marker(
    transaction: &Transaction<'_>,
    seed: &LegacyApprovalSeed,
    source: &SyntheticHoldSource,
) -> TestResult {
    transaction.execute_batch(
        "DROP TRIGGER ops_team_expansion_workspace_hold_plans_no_delete;
         DROP TRIGGER ops_team_expansion_workspace_hold_manifests_no_delete;",
    )?;
    let deleted_plans = transaction.execute(
        "DELETE FROM ops_team_expansion_workspace_hold_plans WHERE workspace_id=?1 AND decision_id=?2",
        params![seed.workspace.workspace_id.as_bytes(), seed.decision_id.as_bytes()],
    )?;
    assert_eq!(
        deleted_plans, 1,
        "synthetic conversion removed wrong plan count"
    );
    let deleted_manifests = transaction.execute(
        "DELETE FROM ops_team_expansion_workspace_hold_manifests WHERE workspace_id=?1 AND decision_id=?2",
        params![seed.workspace.workspace_id.as_bytes(), seed.decision_id.as_bytes()],
    )?;
    assert_eq!(
        deleted_manifests, 1,
        "synthetic conversion removed wrong manifest count"
    );
    transaction.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_pre_hold_decisions(decision_id,request_id,workspace_id,team_id,decision_state,classification) VALUES(?1,?2,?3,?4,'APPROVED','PRE_HOLD')",
        params![
            seed.decision_id.as_bytes(),
            seed.request_id.as_bytes(),
            seed.workspace.workspace_id.as_bytes(),
            seed.team_id.as_bytes(),
        ],
    )?;
    transaction.execute_batch(&source.plans_delete_trigger)?;
    transaction.execute_batch(&source.manifests_delete_trigger)?;
    Ok(())
}

fn verify_synthetic_conversion(
    connection: &Connection,
    schema_objects_before: &[(String, String, Option<String>)],
) -> TestResult {
    assert_eq!(
        schema_snapshot(connection)?,
        schema_objects_before,
        "synthetic conversion changed V62 schema objects or triggers"
    );
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    assert_eq!(
        integrity, "ok",
        "synthetic PRE_HOLD conversion damaged SQLite"
    );
    let foreign_key_violation: Option<(String, i64, String, i64)> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .optional()?;
    assert!(
        foreign_key_violation.is_none(),
        "synthetic conversion made a foreign-key violation"
    );
    Ok(())
}

fn schema_snapshot(connection: &Connection) -> TestResult<Vec<(String, String, Option<String>)>> {
    let mut statement = connection.prepare(
        "SELECT type,name,sql FROM sqlite_schema
         WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn immutable_delete_trigger_sql(
    transaction: &Transaction<'_>,
    trigger_name: &str,
) -> TestResult<String> {
    transaction
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?1",
            params![trigger_name],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| format!("missing immutable trigger {trigger_name}").into())
}

fn ensure_slot_is_unassigned(
    transaction: &Transaction<'_>,
    seed: &LegacyApprovalSeed,
) -> TestResult {
    let assigned: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_slot_assignments WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3 AND ordinal=0)",
        params![
            seed.workspace.workspace_id.as_bytes(),
            seed.team_id.as_bytes(),
            seed.decision_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    let used: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_slot_uses WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3 AND ordinal=0)",
        params![
            seed.workspace.workspace_id.as_bytes(),
            seed.team_id.as_bytes(),
            seed.decision_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    let bound: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_workspace_hold_bindings WHERE workspace_id=?1 AND decision_id=?2 AND ordinal=0)",
        params![
            seed.workspace.workspace_id.as_bytes(),
            seed.decision_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    let recovered: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_legacy_slot_recoveries WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3 AND ordinal=0)",
        params![
            seed.workspace.workspace_id.as_bytes(),
            seed.team_id.as_bytes(),
            seed.decision_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    assert!(!assigned && !used && !bound && !recovered);
    Ok(())
}

async fn recover_assign_and_join(
    fixture: &IsolatedFixture,
    backend: &RunningBackend,
    seed: &LegacyApprovalSeed,
) -> TestResult {
    let coordinates = assert_recovery_preconditions(backend, fixture, seed).await?;
    let request = recovery_request(seed, coordinates);
    let (readback_request, direct_receipt) = stage(
        "single recovery POST through response-drop proxy",
        send_recovery_through_response_drop(
            backend.address,
            &fixture.bearer,
            &request,
            seed.request_id,
        )
        .await,
    )?;
    let response = stage(
        "exact O1 recovery receipt readback",
        backend
            .client
            .legacy_team_expansion_slot_recovery_readback(&fixture.bearer, &readback_request)
            .await,
    )?;
    assert_eq!(response.request, readback_request);
    let receipt = response
        .result
        .ok_or("committed synthetic pre-hold recovery is absent after response loss")?;
    assert_eq!(receipt.intent, request.intent);
    assert_eq!(receipt.approval_request_id, seed.request_id);
    assert!(!receipt.replayed);
    assert_eq!(
        receipt.leadership_revision,
        seed.workspace_leadership_revision
    );
    assert!(receipt.recorded_at_ms >= 0);
    assert_recovery_actor(&receipt.actor, seed);
    assert_eq!(
        receipt, direct_receipt,
        "readback differs from dropped success"
    );

    stage(
        "original approval readback after recovery",
        assert_original_legacy_decision_readback(backend, fixture, seed).await,
    )?;
    let assignment = stage(
        "recovered slot assignment and exact receipt readback",
        assign_recovered_slot(fixture, backend, seed, coordinates).await,
    )?;
    stage(
        "target TeamJoin and exact receipt readback",
        join_recovered_slot(fixture, backend, seed, coordinates, &assignment).await,
    )?;
    assert_final_recovery_state(backend, fixture, seed).await
}

async fn assert_recovery_preconditions(
    backend: &RunningBackend,
    fixture: &IsolatedFixture,
    seed: &LegacyApprovalSeed,
) -> TestResult<TeamSnapshot> {
    stage(
        "current Workspace leadership readback",
        assert_current_workspace_leader(backend, fixture, seed).await,
    )?;
    let coordinates = stage(
        "current Team and capacity readback",
        read_team_snapshot(
            &backend.client,
            &fixture.bearer,
            &seed.workspace,
            seed.activity_id,
            seed.workspace_leader.session_id,
            seed.team_id,
        )
        .await,
    )?;
    assert_eq!(coordinates.revision, seed.expected_team_revision);
    assert_eq!(
        coordinates.capacity_revision,
        seed.expected_capacity_revision
    );
    assert_eq!(coordinates.member_count, 3);
    let source_cursor = stage(
        "Forge source cursor readback",
        read_source_cursor(
            &backend.client,
            &fixture.bearer,
            &seed.workspace,
            seed.activity_id,
            seed.workspace_leader.session_id,
        )
        .await,
    )?;
    assert_eq!(source_cursor, seed.expected_source_sequence);

    stage(
        "original decision readback",
        assert_original_legacy_decision_readback(backend, fixture, seed).await,
    )?;
    Ok(coordinates)
}

async fn assert_final_recovery_state(
    backend: &RunningBackend,
    fixture: &IsolatedFixture,
    seed: &LegacyApprovalSeed,
) -> TestResult {
    let after_join = stage(
        "final Team roster readback",
        backend
            .client
            .team_get(
                &fixture.bearer,
                &TeamGetQueryRequest {
                    scope: seed.workspace.clone(),
                    session_id: Some(seed.workspace_leader.session_id),
                    team_id: seed.team_id,
                    request_id: None,
                },
            )
            .await,
    )?;
    assert_eq!(after_join.members.len(), 4);
    assert!(!after_join.members_truncated);
    assert!(after_join.members.iter().any(|member| {
        member.profile_id == Some(seed.target.profile_id)
            && member.participation_id == seed.target.workspace_participation
            && member.state == "joined"
    }));
    let final_source_cursor = stage(
        "final Forge source cursor readback",
        read_source_cursor(
            &backend.client,
            &fixture.bearer,
            &seed.workspace,
            seed.activity_id,
            seed.workspace_leader.session_id,
        )
        .await,
    )?;
    assert_eq!(final_source_cursor, seed.expected_source_sequence);
    Ok(())
}

async fn assert_current_workspace_leader(
    backend: &RunningBackend,
    fixture: &IsolatedFixture,
    seed: &LegacyApprovalSeed,
) -> TestResult {
    let leadership = backend
        .client
        .leadership_view(
            &fixture.bearer,
            &LeadershipQueryRequest {
                scope: seed.workspace.clone(),
                session_id: Some(seed.workspace_leader.session_id),
                activity_id: None,
                team_id: None,
                review_after: None,
                limit: 10,
            },
        )
        .await?
        .leadership
        .ok_or("O1 Workspace leadership disappeared across the offline conversion")?;
    assert_eq!(
        leadership.leader_participant_id,
        Some(seed.workspace_leader.logical_participant)
    );
    assert_eq!(
        leadership.leadership_revision,
        seed.workspace_leadership_revision
    );
    assert!(matches!(
        leadership.state.as_str(),
        "stabilizing" | "active"
    ));
    Ok(())
}

fn recovery_request(
    seed: &LegacyApprovalSeed,
    coordinates: TeamSnapshot,
) -> LegacySlotRecoveryRequest {
    LegacySlotRecoveryRequest {
        actor: expansion_actor(
            &seed.workspace,
            seed.activity_id,
            seed.workspace_leader.session_id,
        ),
        intent: LegacySlotRecoveryIntentWire {
            workspace_id: seed.workspace.workspace_id,
            recovery_request_id: Uuid::now_v7(),
            approval_id: seed.decision_id,
            ordinal: 0,
            team_id: seed.team_id,
            expected_team_revision: coordinates.revision,
            expected_capacity_revision: coordinates.capacity_revision,
            target_profile_id: seed.target.profile_id,
            target_participation_id: seed.target.workspace_participation,
        },
        expected_leadership_revision: seed.workspace_leadership_revision,
    }
}

fn assert_recovery_actor(
    actor: &expansion_wire::TeamExpansionActorReceiptWire,
    seed: &LegacyApprovalSeed,
) {
    assert_member_actor_receipt(actor, seed.activity_id, &seed.workspace_leader);
}

async fn assert_original_legacy_decision_readback(
    backend: &RunningBackend,
    fixture: &IsolatedFixture,
    seed: &LegacyApprovalSeed,
) -> TestResult {
    let intent = expansion_wire::TeamExpansionMutationRequest {
        actor: expansion_actor(
            &seed.workspace,
            seed.activity_id,
            seed.workspace_leader.session_id,
        ),
        action: expansion_wire::TeamExpansionMutationActionWire::Decide {
            command: expansion_wire::TeamExpansionDecisionAction {
                team_id: seed.team_id,
                request_id: seed.request_id,
                decision_id: seed.decision_id,
                expected_team_revision: seed.expected_team_revision,
                expected_capacity_revision: seed.expected_capacity_revision,
                expected_leadership_revision: seed.workspace_leadership_revision,
                expected_requester_leadership_revision: seed.requester_leadership_revision,
                decision: expansion_wire::TeamExpansionDecisionWire::Approve { approved_count: 1 },
            },
        },
    };
    let readback = backend
        .client
        .team_expansion_readback(
            &fixture.bearer,
            &expansion_wire::TeamExpansionReadbackRequest {
                intent,
                current_actor: expansion_actor(
                    &seed.workspace,
                    seed.activity_id,
                    seed.workspace_leader.session_id,
                ),
            },
        )
        .await?;
    let Some(expansion_wire::TeamExpansionMutationResultWire::Decision { receipt }) =
        readback.result
    else {
        return Err("original PRE_HOLD approval is absent on exact decision readback".into());
    };
    assert_eq!(receipt.decision_id, seed.decision_id);
    assert_eq!(receipt.request_id, seed.request_id);
    assert_eq!(receipt.team_id, seed.team_id);
    assert_eq!(
        receipt.state,
        expansion_wire::TeamExpansionDecisionStateWire::Approved
    );
    assert_eq!(receipt.approved_count, 1);
    assert_recovery_actor(&receipt.actor, seed);
    Ok(())
}

async fn assign_recovered_slot(
    fixture: &IsolatedFixture,
    backend: &RunningBackend,
    seed: &LegacyApprovalSeed,
    coordinates: TeamSnapshot,
) -> TestResult<expansion_wire::TeamExpansionSlotAssignmentReceiptWire> {
    let assignment_request_id = Uuid::now_v7();
    let request = expansion_wire::TeamExpansionMutationRequest {
        actor: expansion_actor(
            &seed.workspace,
            seed.activity_id,
            seed.workspace_leader.session_id,
        ),
        action: expansion_wire::TeamExpansionMutationActionWire::AssignSlot {
            command: expansion_wire::TeamExpansionSlotAssignmentAction {
                team_id: seed.team_id,
                approval_id: seed.decision_id,
                ordinal: 0,
                request_id: assignment_request_id,
                expected_team_revision: coordinates.revision,
                expected_capacity_revision: coordinates.capacity_revision,
                expected_leadership_revision: seed.workspace_leadership_revision,
                target: expansion_wire::TeamExpansionTargetWire {
                    activity_id: seed.activity_id,
                    session_id: seed.target.session_id,
                },
            },
        },
    };
    let response = backend
        .client
        .mutate_team_expansion(&fixture.bearer, &request)
        .await?;
    let expansion_wire::TeamExpansionMutationResultWire::SlotAssignment { receipt } =
        response.result
    else {
        return Err("legacy recovery slot assignment returned another receipt".into());
    };
    assert_eq!(receipt.request_id, assignment_request_id);
    assert!(!receipt.replayed);
    assert_eq!(receipt.approval_id, seed.decision_id);
    assert_eq!(receipt.ordinal, 0);
    assert_eq!(receipt.team_id, seed.team_id);
    assert_eq!(receipt.profile_id, seed.target.profile_id);
    assert_eq!(
        receipt.participation_id,
        seed.target.workspace_participation
    );
    assert_eq!(receipt.target.activity_id, seed.activity_id);
    assert_eq!(receipt.target.agent_id, seed.target.agent_id);
    assert_eq!(receipt.target.session_id, seed.target.session_id);
    assert_eq!(
        receipt.target.participation_id,
        seed.target.workspace_participation
    );
    assert_eq!(receipt.target.profile_id, seed.target.profile_id);
    assert_eq!(
        receipt.target.participant_id,
        seed.target.logical_participant
    );
    assert_recovery_actor(&receipt.actor, seed);

    let readback = backend
        .client
        .team_expansion_readback(
            &fixture.bearer,
            &expansion_wire::TeamExpansionReadbackRequest {
                intent: request,
                current_actor: expansion_actor(
                    &seed.workspace,
                    seed.activity_id,
                    seed.workspace_leader.session_id,
                ),
            },
        )
        .await?;
    let Some(expansion_wire::TeamExpansionMutationResultWire::SlotAssignment { receipt: stored }) =
        readback.result
    else {
        return Err("legacy recovered slot assignment readback is absent".into());
    };
    assert_eq!(*stored, *receipt);
    Ok(*receipt)
}

async fn join_recovered_slot(
    fixture: &IsolatedFixture,
    backend: &RunningBackend,
    seed: &LegacyApprovalSeed,
    coordinates: TeamSnapshot,
    assignment: &expansion_wire::TeamExpansionSlotAssignmentReceiptWire,
) -> TestResult {
    let request = TeamJoinV2Request {
        scope: seed.workspace.clone(),
        request_id: Uuid::now_v7(),
        team_id: seed.team_id,
        activity_id: seed.activity_id,
        session_id: seed.target.session_id,
        expected_team_revision: coordinates.revision,
        role: "recovered member".to_owned(),
        expansion_slot: TeamExpansionSlotV2Request {
            decision_id: assignment.approval_id,
            ordinal: assignment.ordinal,
        },
    };
    let (joined, recovered) = submit_join_v2_or_readback(fixture, backend, seed, &request).await?;
    assert_eq!(joined.mutation.replayed, recovered);
    assert_eq!(joined.request_id, request.request_id);
    assert_eq!(joined.mutation.team.team_id, seed.team_id);
    assert_eq!(joined.mutation.team.revision, coordinates.revision + 1);
    assert_eq!(joined.mutation.expansion_slot, Some(request.expansion_slot));
    let join_source = joined
        .mutation
        .expansion_join_source
        .as_ref()
        .ok_or("recovered legacy Join omitted its Team slot source proof")?;
    assert_eq!(join_source.activity_id, request.activity_id);
    assert_eq!(join_source.agent_id, seed.target.agent_id);
    assert_eq!(join_source.session_id, request.session_id);
    assert_eq!(join_source.decision_id, assignment.approval_id);
    assert_eq!(join_source.ordinal, assignment.ordinal);
    assert_eq!(join_source.role, request.role);
    let membership = joined
        .mutation
        .membership
        .as_ref()
        .ok_or("recovered legacy Join has no membership receipt")?;
    assert_eq!(membership.profile_id, Some(seed.target.profile_id));
    assert_eq!(membership.participation_id, assignment.participation_id);
    assert_eq!(membership.state, "joined");
    assert_eq!(membership.role.as_deref(), Some("recovered member"));

    let readback = backend
        .client
        .team_join_mutation_readback_v2(
            &fixture.bearer,
            &request,
            TeamMutationReaderV2Wire {
                activity_id: request.activity_id,
                session_id: request.session_id,
            },
        )
        .await?
        .ok_or("recovered legacy TeamJoin receipt is absent")?;
    assert!(readback.mutation.replayed);
    let mut direct_as_readback = joined;
    direct_as_readback.mutation.replayed = true;
    assert_eq!(readback, direct_as_readback);
    Ok(())
}

async fn submit_join_v2_or_readback(
    fixture: &IsolatedFixture,
    backend: &RunningBackend,
    seed: &LegacyApprovalSeed,
    request: &TeamJoinV2Request,
) -> TestResult<(TeamMutationV2Response, bool)> {
    match backend.client.join_team_v2(&fixture.bearer, request).await {
        Ok(response) => Ok((response, false)),
        Err(
            error @ (ClientError::ReadbackRequired { .. } | ClientError::UnknownOutcome { .. }),
        ) => {
            let exact = matches!(
                &error,
                ClientError::ReadbackRequired {
                    readback: ReadbackTarget::TeamJoinV2Mutation(original),
                }
                | ClientError::UnknownOutcome {
                    readback: ReadbackTarget::TeamJoinV2Mutation(original),
                } if original.as_ref() == request
            );
            if !exact {
                return Err(format!(
                    "legacy TeamJoin unknown outcome has a different request: {error:?}"
                )
                .into());
            }
            let response = backend
                .client
                .team_join_mutation_readback_v2(
                    &fixture.bearer,
                    request,
                    TeamMutationReaderV2Wire {
                        activity_id: seed.activity_id,
                        session_id: seed.target.session_id,
                    },
                )
                .await?
                .ok_or("legacy TeamJoin source readback returned None")?;
            Ok((response, true))
        }
        Err(error) => Err(format!("legacy TeamJoin rejected: {error:?}").into()),
    }
}

async fn send_recovery_through_response_drop(
    backend_address: SocketAddr,
    bearer: &HeaderValue,
    request: &LegacySlotRecoveryRequest,
    approval_request_id: Uuid,
) -> TestResult<(
    LegacySlotRecoveryReadbackRequest,
    LegacySlotRecoveryReceiptWire,
)> {
    if !heterarchy_orchestration_backend::wire::legacy_slot_recovery::valid_legacy_slot_recovery_request(request) {
        return Err("response-drop fixture built an invalid recovery request".into());
    }
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let proxy_address = listener.local_addr()?;
    let proxy_client = OrchestrationHttpClient::new(proxy_address, super::DEADLINE)?;
    let expected = request.clone();
    let expected_bearer = bearer.clone();
    let forwarded = Arc::new(AtomicUsize::new(0));
    let proxy_forwarded = Arc::clone(&forwarded);
    let proxy = tokio::spawn(async move {
        forward_and_drop_recovery_response(
            listener,
            backend_address,
            &expected_bearer,
            &expected,
            approval_request_id,
            &proxy_forwarded,
        )
        .await
    });

    let outcome = proxy_client
        .recover_legacy_team_expansion_slot(bearer, request)
        .await;
    let readback = LegacySlotRecoveryReadbackRequest {
        intent: request.intent,
        current_actor: request.actor.clone(),
    };
    let outcome_check = match outcome {
        Err(ClientError::UnknownOutcome {
            readback: ReadbackTarget::LegacyTeamExpansionSlotRecovery(original),
        }) if original.as_ref() == &readback => Ok(()),
        other => Err(format!(
            "response-drop request did not retain its exact legacy recovery intent: {other:?}"
        )),
    };

    let receipt = proxy.await??;
    outcome_check?;
    assert_eq!(forwarded.load(Ordering::SeqCst), 1);
    Ok((readback, receipt))
}

async fn forward_and_drop_recovery_response(
    listener: TcpListener,
    backend_address: SocketAddr,
    expected_bearer: &HeaderValue,
    expected: &LegacySlotRecoveryRequest,
    approval_request_id: Uuid,
    forwarded: &AtomicUsize,
) -> Result<LegacySlotRecoveryReceiptWire, String> {
    let (mut downstream, _) = listener.accept().await.map_err(|error| error.to_string())?;
    let request_bytes = read_bounded_http_request(&mut downstream).await?;
    let (method, path, body) = http_request_parts(&request_bytes)?;
    let expected_path = format!(
        "/internal/v2/workspaces/{}/teams/expansion/legacy-slot-recovery",
        expected.actor.scope.workspace_id
    );
    if method != "POST" || path != expected_path.as_str() {
        return Err("response-drop proxy received a different HTTP route".to_owned());
    }
    let parsed: LegacySlotRecoveryRequest =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    if &parsed != expected {
        return Err("response-drop proxy received a different recovery intent".to_owned());
    }
    if request_authorization(&request_bytes)?
        != expected_bearer
            .to_str()
            .map_err(|error| error.to_string())?
    {
        return Err("response-drop proxy received a different bearer".to_owned());
    }

    let upstream = OrchestrationHttpClient::new(backend_address, super::DEADLINE)
        .map_err(|error| error.to_string())?;
    let response = upstream
        .recover_legacy_team_expansion_slot(expected_bearer, &parsed)
        .await
        .map_err(|error| format!("real recovery failed before response drop: {error:?}"))?;
    if response.request != *expected {
        return Err("upstream response did not echo the exact recovery command".to_owned());
    }
    let receipt = response.receipt;
    if receipt.intent != expected.intent
        || receipt.approval_request_id != approval_request_id
        || receipt.leadership_revision != expected.expected_leadership_revision
        || receipt.actor.activity_id != expected.actor.activity_id
        || receipt.actor.session_id != expected.actor.session_id
        || receipt.recorded_at_ms < 0
        || receipt.replayed
    {
        return Err("upstream response did not prove a fresh exact recovery".to_owned());
    }
    if forwarded.swap(1, Ordering::SeqCst) != 0 {
        return Err("response-drop proxy forwarded more than one recovery".to_owned());
    }
    // The real service returned a checked success. Closing this stream forces
    // the caller to recover through the exact intent query.
    drop(downstream);
    Ok(receipt)
}

async fn read_bounded_http_request(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("client closed before sending the full recovery request".to_owned());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > RECOVERY_PROXY_MAX_REQUEST_BYTES {
            return Err("response-drop proxy request exceeded its fixture bound".to_owned());
        }
        let Some(end) = header_end(&bytes) else {
            continue;
        };
        let body_length = content_length(&bytes[..end])?;
        let total = end
            .checked_add(4)
            .and_then(|header_length| header_length.checked_add(body_length))
            .ok_or_else(|| "recovery HTTP request length overflowed".to_owned())?;
        if total > RECOVERY_PROXY_MAX_REQUEST_BYTES {
            return Err("response-drop proxy body exceeded its fixture bound".to_owned());
        }
        if bytes.len() >= total {
            bytes.truncate(total);
            return Ok(bytes);
        }
    }
}

fn http_request_parts(bytes: &[u8]) -> Result<(&str, &str, &[u8]), String> {
    let end = header_end(bytes).ok_or_else(|| "HTTP request header is incomplete".to_owned())?;
    let header = std::str::from_utf8(&bytes[..end]).map_err(|error| error.to_string())?;
    let request_line = header
        .lines()
        .next()
        .ok_or_else(|| "HTTP request line is missing".to_owned())?;
    let mut fields = request_line.split_whitespace();
    let method = fields
        .next()
        .ok_or_else(|| "HTTP request method is missing".to_owned())?;
    let path = fields
        .next()
        .ok_or_else(|| "HTTP request path is missing".to_owned())?;
    Ok((method, path, &bytes[end + 4..]))
}

fn request_authorization(bytes: &[u8]) -> Result<&str, String> {
    let end = header_end(bytes).ok_or_else(|| "HTTP request header is incomplete".to_owned())?;
    let header = std::str::from_utf8(&bytes[..end]).map_err(|error| error.to_string())?;
    header
        .lines()
        .skip(1)
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                .map(|(_, value)| value.trim())
        })
        .ok_or_else(|| "HTTP bearer header is missing".to_owned())
}

fn header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(header: &[u8]) -> Result<usize, String> {
    let header = std::str::from_utf8(header).map_err(|error| error.to_string())?;
    header
        .lines()
        .skip(1)
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim())
        })
        .ok_or_else(|| "HTTP Content-Length is missing".to_owned())?
        .parse::<usize>()
        .map_err(|error| error.to_string())
}
