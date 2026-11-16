use std::collections::VecDeque;
use std::error::Error;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::os::unix::process::ExitStatusExt as _;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use forge_domain::{
    ActivityId, AuthenticatedPrincipal, DirectTokenId, GoalId, SwarmRunId, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AddSwarmMemberCommand, AssignmentExecutionSnapshot, CreateSwarmRunCommand, DagSettledFact,
    EffectFactInput, ExecutionPermissions, ExecutionTimeouts, ForgeWorktreeRef, LeaseRef,
    MAX_SNAPSHOT_COALESCED_BYTES, MAX_SNAPSHOT_COALESCED_MESSAGES, McpGrant, MessageContext,
    PostSwarmMessageCommand, ProcessOwnershipIdentity, RecordSwarmConvergenceCommand,
    RecordSwarmVerdictCommand, ReviewVerdictOutcome, SWARM_MAIN_CHANNEL_NAME, SwarmChannelRecord,
    SwarmCommitOid, SwarmMemberIdentity, SwarmMemberRecord, SwarmMemberState, SwarmMessageAuthor,
    SwarmRoleKind, SwarmRunMode, SwarmRunRecord, SwarmRunState, SwarmTurnOutput,
    SwarmWorkspaceMode,
};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileRevision, CliEngine, ExecutionPreference, RolePromptDigest,
};
use tokio::sync::Notify;
use uuid::Uuid;

use super::dispatcher::WorkingTurnDropGuard;
use super::*;
use crate::execution_supervisor::{
    CliExecutableResolver, CliExecutionContext, ExecutionRunner, ExecutionSupervisorError,
    OwnershipVerification, SpawnedProcessHandle, TerminationDisposition,
};
use crate::{AgentProfileCreateCommand, OrchestrationRuntime};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct TestRuntimeDir(PathBuf);

impl TestRuntimeDir {
    fn new(label: &str) -> Result<Self, std::io::Error> {
        let base = std::fs::canonicalize(std::env::temp_dir())?;
        let path = base.join(format!("swarm-disp-{label}-{}", Uuid::now_v7()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestRuntimeDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct TestNonSsdScratch(PathBuf);

impl TestNonSsdScratch {
    fn new(label: &str) -> Result<Self, std::io::Error> {
        let ambient = std::env::temp_dir();
        let base = if ambient.starts_with(crate::execution_supervisor::FORBIDDEN_SSD_BUILD_PREFIX) {
            PathBuf::from("/var/tmp")
        } else {
            ambient
        };
        let path = base.join(format!("swarm-disp-{label}-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestNonSsdScratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

enum ScriptedSpawnBehavior {
    Exit(i32),
    ParkUntilRelease(Arc<std::sync::Condvar>, Arc<Mutex<bool>>, i32),
}

struct ScriptedRunnerInner {
    boot_id: String,
    next_pid: AtomicU32,
    scripted: Mutex<VecDeque<ScriptedSpawnBehavior>>,
    spawn_observed: Mutex<Vec<(AssignmentExecutionSnapshot, CliExecutionContext)>>,
    spawn_notify: Notify,
}

#[derive(Clone)]
struct ScriptedRunner(Arc<ScriptedRunnerInner>);

impl ScriptedRunner {
    fn new() -> Self {
        Self(Arc::new(ScriptedRunnerInner {
            boot_id: "boot-swarm-test-v1".to_owned(),
            next_pid: AtomicU32::new(10_000),
            scripted: Mutex::new(VecDeque::new()),
            spawn_observed: Mutex::new(Vec::new()),
            spawn_notify: Notify::new(),
        }))
    }

    fn push_exit(&self, code: i32) -> TestResult {
        self.0
            .scripted
            .lock()
            .map_err(|_| "scripted lock")?
            .push_back(ScriptedSpawnBehavior::Exit(code));
        Ok(())
    }

    fn push_parked(
        &self,
        cvar: Arc<std::sync::Condvar>,
        released: Arc<Mutex<bool>>,
        code: i32,
    ) -> TestResult {
        self.0
            .scripted
            .lock()
            .map_err(|_| "scripted lock")?
            .push_back(ScriptedSpawnBehavior::ParkUntilRelease(
                cvar, released, code,
            ));
        Ok(())
    }

    fn recorded_snapshots(&self) -> TestResult<Vec<AssignmentExecutionSnapshot>> {
        Ok(self
            .0
            .spawn_observed
            .lock()
            .map_err(|_| "snapshots lock")?
            .iter()
            .map(|(snap, _)| snap.clone())
            .collect())
    }

    fn recorded_invocations(
        &self,
    ) -> TestResult<Vec<(AssignmentExecutionSnapshot, CliExecutionContext)>> {
        Ok(self
            .0
            .spawn_observed
            .lock()
            .map_err(|_| "snapshots lock")?
            .clone())
    }
}

impl ExecutionRunner for ScriptedRunner {
    fn current_boot_id(&self) -> Result<String, ExecutionSupervisorError> {
        Ok(self.0.boot_id.clone())
    }

    fn spawn_prepared(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
    ) -> Result<SpawnedProcessHandle, ExecutionSupervisorError> {
        self.spawn_prepared_with_context(snapshot, &CliExecutionContext::default())
    }

    fn spawn_prepared_with_context(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
        context: &CliExecutionContext,
    ) -> Result<SpawnedProcessHandle, ExecutionSupervisorError> {
        self.0
            .spawn_observed
            .lock()
            .map_err(|_| ExecutionSupervisorError::CatalogPoisoned)?
            .push((snapshot.clone(), context.clone()));
        self.0.spawn_notify.notify_waiters();

        let behavior = self
            .0
            .scripted
            .lock()
            .map_err(|_| ExecutionSupervisorError::CatalogPoisoned)?
            .pop_front()
            .unwrap_or(ScriptedSpawnBehavior::Exit(0));

        let pid = self.0.next_pid.fetch_add(1, Ordering::Relaxed);
        let ownership =
            ProcessOwnershipIdentity::new(pid, pid, format!("start-{pid}"), self.0.boot_id.clone())
                .map_err(ExecutionSupervisorError::invalid_ownership_identity)?;

        let raw_code = match behavior {
            ScriptedSpawnBehavior::Exit(code) => code,
            ScriptedSpawnBehavior::ParkUntilRelease(cvar, released, code) => {
                let mut guard = released
                    .lock()
                    .map_err(|_| ExecutionSupervisorError::CatalogPoisoned)?;
                while !*guard {
                    guard = cvar
                        .wait(guard)
                        .map_err(|_| ExecutionSupervisorError::CatalogPoisoned)?;
                }
                code
            }
        };
        let status = ExitStatus::from_raw((raw_code & 0xff) << 8);
        Ok(SpawnedProcessHandle::unpinned(ownership, Some(status)))
    }

    fn verify_ownership(
        &self,
        _ownership: &ProcessOwnershipIdentity,
    ) -> Result<OwnershipVerification, ExecutionSupervisorError> {
        Ok(OwnershipVerification::ProcessAbsent)
    }

    fn terminate_owned_group(
        &self,
        _handle: &mut SpawnedProcessHandle,
        _grace_period: Duration,
    ) -> Result<TerminationDisposition, ExecutionSupervisorError> {
        Ok(TerminationDisposition::AlreadyExited)
    }
}

fn valid_worktree() -> TestResult<ForgeWorktreeRef> {
    let manifest_dir = std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")))?;
    Ok(ForgeWorktreeRef::new(manifest_dir)?)
}

fn create_active_profile(
    runtime: &OrchestrationRuntime,
    seed: u128,
    name: &str,
    engine: CliEngine,
    model: &str,
) -> TestResult<(AgentProfileId, AgentProfileRevision)> {
    let digest = RolePromptDigest::new([0x5A; 32])?;
    let pref = ExecutionPreference::new(
        engine,
        model.to_owned(),
        Some("high".to_owned()),
        vec!["rust".to_owned()],
        Some(digest),
    )?;
    let created = runtime.create_agent_profile_with_execution_preference(
        AgentProfileCreateCommand {
            request_id: Uuid::from_u128(seed),
            stable_name: name.to_owned(),
            display_name: format!("Agent {name}"),
            preferred_model: Some(model.to_owned()),
            preferred_reasoning: Some("high".to_owned()),
            base_capabilities: vec!["rust".to_owned()],
            created_at_ms: 10,
        },
        Some(pref),
    )?;
    Ok((
        AgentProfileId::from_uuid(created.profile.profile_id)?,
        AgentProfileRevision::new(created.profile.revision)?,
    ))
}

fn member_identity(profile_id: AgentProfileId, seed: u128) -> TestResult<SwarmMemberIdentity> {
    Ok(SwarmMemberIdentity::new(
        profile_id,
        Uuid::from_u128(seed),
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(seed + 1))),
    )?)
}

fn sample_dispatch_request(
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    member_id: Uuid,
    dispatched_at_ms: i64,
) -> TestResult<SwarmDispatchTurnRequest> {
    Ok(SwarmDispatchTurnRequest {
        snapshot_request_id: Uuid::now_v7(),
        snapshot_id: Uuid::now_v7(),
        workspace_id,
        swarm_run_id,
        member_id,
        activity_id: ActivityId::from_uuid(Uuid::from_u128(0xAA01)),
        default_worktree: valid_worktree()?,
        lease: LeaseRef::new(Uuid::from_u128(0xAA02), 1, 900_000)?,
        mcp_grant: Some(McpGrant::new(
            DirectTokenId::from_uuid(Uuid::from_u128(0xAA03)),
            10,
            900_000,
        )?),
        permissions: ExecutionPermissions::new(true, true, false),
        timeouts: ExecutionTimeouts::new(2_000, 10_000, 500)?,
        dispatched_at_ms,
        clean_exit_output: None,
    })
}

fn setup_swarm_run_with_coordinator(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
) -> TestResult<(SwarmRunRecord, SwarmChannelRecord, SwarmMemberRecord)> {
    let run = runtime
        .create_swarm_run(&CreateSwarmRunCommand {
            request_id: Uuid::now_v7(),
            workspace_id,
            title: "Swarm Slice 4 Test".to_owned(),
            mode: SwarmRunMode::Standalone,
            max_members: 32,
            max_spawn_depth: 2,
            max_total_turns: 500,
            required_verifier_quorum: 1,
            allow_worker_all_mention: false,
            baseline_commit_oid: SwarmCommitOid::new("0123456789abcdef0123456789abcdef01234567")?,
            baseline_tree_blake3: [0x11; 32],
            created_at_ms: 100,
        })?
        .map_err(|e| format!("create_swarm_run: {e}"))?;
    let main_channel = runtime
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), SWARM_MAIN_CHANNEL_NAME)?
        .map_err(|e| format!("get_swarm_channel_by_name: {e}"))?
        .ok_or("missing main channel")?;
    let (coord_profile, _) = create_active_profile(
        runtime,
        0x1001,
        "coord-prof",
        CliEngine::Codex,
        "gpt-6-astra",
    )?;
    let coordinator = runtime
        .add_swarm_member(&AddSwarmMemberCommand {
            member_id: Uuid::from_u128(0x2001),
            workspace_id,
            swarm_run_id: run.swarm_run_id(),
            roster_name: "coordinator".to_owned(),
            role_kind: SwarmRoleKind::Coordinator,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            worktree_path: None,
            identity: member_identity(coord_profile, 0x3001)?,
            initial_channels: Vec::new(),
            added_at_ms: 110,
        })?
        .map_err(|e| format!("add coordinator: {e}"))?;
    Ok((run, main_channel, coordinator))
}

#[tokio::test]
async fn single_flight_coalescing_caps_32_messages_and_64_kib_and_requeues_remainder() -> TestResult
{
    let dir = TestRuntimeDir::new("coalesce")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7001));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;

    let (worker_profile, _) = create_active_profile(
        &runtime,
        0x1002,
        "worker-prof",
        CliEngine::Codex,
        "gpt-6-luna",
    )?;
    let worker = runtime
        .add_swarm_member(&AddSwarmMemberCommand {
            member_id: Uuid::from_u128(0x2002),
            workspace_id,
            swarm_run_id: run.swarm_run_id(),
            roster_name: "worker-alpha".to_owned(),
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coordinator.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            worktree_path: Some(valid_worktree()?),
            identity: member_identity(worker_profile, 0x3010)?,
            initial_channels: Vec::new(),
            added_at_ms: 120,
        })?
        .map_err(|e| format!("add worker: {e}"))?;

    for idx in 0..35_u32 {
        runtime
            .post_swarm_message(&PostSwarmMessageCommand {
                message_id: Uuid::now_v7(),
                workspace_id,
                swarm_run_id: run.swarm_run_id(),
                channel_id: main_channel.channel_id(),
                author: SwarmMessageAuthor::Member(coordinator.member_id()),
                body: format!("@worker-alpha item #{idx}"),
                context: MessageContext::default(),
                submitted_at_ms: 200 + i64::from(idx),
            })?
            .map_err(|e| format!("post msg {idx}: {e}"))?;
    }

    let dispatcher = runtime.swarm_dispatcher(ScriptedRunner::new());
    let first_turn = dispatcher
        .dispatch_member_turn(sample_dispatch_request(
            workspace_id,
            run.swarm_run_id(),
            worker.member_id(),
            300,
        )?)
        .await?;

    assert_eq!(
        first_turn.wakeup_batch.messages().len(),
        MAX_SNAPSHOT_COALESCED_MESSAGES
    );
    assert!(first_turn.wakeup_batch.total_body_bytes() <= MAX_SNAPSHOT_COALESCED_BYTES);
    assert_eq!(first_turn.wakeup_batch.committed_seen_seq(), 0);
    assert_eq!(first_turn.wakeup_batch.inflight_target_seq(), 32);
    let post_first = first_turn.disposition.member();
    assert_eq!(post_first.committed_seen_seq(), 32);
    assert_eq!(post_first.member_state(), SwarmMemberState::WakeQueued);

    let second_turn = dispatcher
        .dispatch_member_turn(sample_dispatch_request(
            workspace_id,
            run.swarm_run_id(),
            worker.member_id(),
            400,
        )?)
        .await?;
    assert_eq!(second_turn.wakeup_batch.messages().len(), 3);
    assert_eq!(second_turn.wakeup_batch.committed_seen_seq(), 32);
    assert_eq!(second_turn.wakeup_batch.inflight_target_seq(), 35);
    assert_eq!(second_turn.disposition.member().committed_seen_seq(), 35);
    assert_eq!(
        second_turn.disposition.member().member_state(),
        SwarmMemberState::Waiting
    );
    Ok(())
}

#[tokio::test]
async fn dispatcher_wires_wakeup_delta_and_metadata_into_cli_context() -> TestResult {
    let dir = TestRuntimeDir::new("cli-ctx-wiring")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7010));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;
    let (worker, _, _) = add_share_worker(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0xD100,
            name: "ctx-worker",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 120,
        },
    )?;

    post_board_msg(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        main_channel.channel_id(),
        coordinator.member_id(),
        "@ctx-worker -c model_reasoning_effort=\"low\" implement slice",
        150,
    )?;

    let runner = ScriptedRunner::new();
    runner.push_exit(0)?;
    let backend_addr: SocketAddr = "127.0.0.1:8798".parse()?;
    let bearer_scratch = TestNonSsdScratch::new("disp-bearer")?;
    let bearer_file = bearer_scratch.0.join("worker.bearer");
    std::fs::write(&bearer_file, "tok-secret-never-logged")?;
    let dispatcher = runtime
        .swarm_dispatcher(runner.clone())
        .with_orchestration_backend_addr(backend_addr)?
        .with_optional_worker_bearer_file(None)?
        .with_worker_bearer_file(bearer_file.clone())?;
    assert_eq!(dispatcher.worker_bearer_file(), Some(bearer_file.as_path()));
    assert!(!format!("{dispatcher:?}").contains(&bearer_file.display().to_string()));

    let outcome = dispatcher
        .dispatch_member_turn(sample_dispatch_request(
            workspace_id,
            run.swarm_run_id(),
            worker.member_id(),
            200,
        )?)
        .await?;
    assert_eq!(outcome.wakeup_batch.messages().len(), 1);

    let recorded = runner.recorded_invocations()?;
    assert_eq!(recorded.len(), 1);
    let (snapshot, context) = &recorded[0];
    assert_eq!(context.workspace_id(), Some(workspace_id));
    assert_eq!(context.swarm_run_id(), Some(run.swarm_run_id()));
    assert_eq!(context.swarm_member_id(), Some(worker.member_id()));
    assert_eq!(context.orchestration_backend_addr(), Some(backend_addr));
    assert_eq!(context.bearer_file(), Some(bearer_file.as_path()));
    let prompt = context.prompt().ok_or("expected formatted wakeup prompt")?;
    assert!(prompt.contains("@ctx-worker"));
    assert!(prompt.contains("seq=0..=1"));
    assert!(prompt.contains("[#1 #main:@coordinator"));
    assert!(prompt.contains("-c model_reasoning_effort=\"low\" implement slice"));
    assert!(prompt.contains("## Swarm Board CLI"));
    assert!(prompt.contains("HETERARCHY_ORCHESTRATION_BEARER_FILE"));
    assert!(
        prompt
            .contains("heterarchy-orchestration-backend swarm post --channel main --body \"...\"")
    );
    assert!(prompt.contains("heterarchy-orchestration-backend swarm status"));

    verify_dispatched_codex_plan(
        snapshot,
        context,
        workspace_id,
        run.swarm_run_id(),
        worker.member_id(),
        &bearer_file,
        prompt,
    )
}

fn verify_dispatched_codex_plan(
    snapshot: &AssignmentExecutionSnapshot,
    context: &CliExecutionContext,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    member_id: Uuid,
    bearer_file: &std::path::Path,
    prompt: &str,
) -> TestResult {
    let resolver = CliExecutableResolver::new().with_codex_binary("/usr/local/bin/codex");
    let plan = resolver.resolve_with_env_and_context(
        snapshot,
        [
            ("PATH", "/usr/bin:/bin"),
            ("OPENAI_API_KEY", "sk-must-be-scrubbed"),
        ],
        context,
    )?;
    assert!(!plan.env().contains_key("OPENAI_API_KEY"));
    assert_eq!(
        plan.env().get("HETERARCHY_WORKSPACE_ID"),
        Some(&OsString::from(workspace_id.to_string()))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_SWARM_RUN_ID"),
        Some(&OsString::from(swarm_run_id.to_string()))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_SWARM_MEMBER_ID"),
        Some(&OsString::from(member_id.to_string()))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_ORCHESTRATION_BACKEND_ADDR"),
        Some(&OsString::from("127.0.0.1:8798"))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_ORCHESTRATION_BEARER_FILE"),
        Some(&bearer_file.as_os_str().to_os_string())
    );
    assert_eq!(
        plan.args(),
        &[
            OsString::from("exec"),
            OsString::from("-m"),
            OsString::from("gpt-6-luna"),
            OsString::from("-c"),
            OsString::from("model_reasoning_effort=\"high\""),
            OsString::from("-s"),
            OsString::from("workspace-write"),
            OsString::from("-c"),
            OsString::from("sandbox_workspace_write.network_access=false"),
            OsString::from("--"),
            OsString::from(prompt),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn agy_verifier_dispatch_snapshot_guards_flag_like_prompt_with_double_dash() -> TestResult {
    let dir = TestRuntimeDir::new("agy-cli-guard")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7011));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;
    let dispatcher = runtime.swarm_dispatcher(ScriptedRunner::new());
    let agy_member = add_inherit_verifier(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        0xD500,
        "agy-verifier",
        210,
    )?;
    post_board_msg(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        main_channel.channel_id(),
        coordinator.member_id(),
        "@agy-verifier audit",
        215,
    )?;

    let agy_req = sample_dispatch_request(
        workspace_id,
        run.swarm_run_id(),
        agy_member.member_id(),
        220,
    )?;
    let (_, agy_snapshot) = dispatcher.claim_and_seal_snapshot(&agy_req)?;
    let flag_like_ctx = CliExecutionContext::new()
        .with_prompt("--dangerously-skip-permissions")?
        .with_workspace_id(workspace_id)?;
    let agy_resolver = CliExecutableResolver::new().with_agy_binary("/usr/local/bin/agy");
    let agy_plan = agy_resolver.resolve_with_env_and_context(
        &agy_snapshot,
        [("PATH", "/usr/bin:/bin")],
        &flag_like_ctx,
    )?;
    assert_eq!(
        agy_plan.args(),
        &[
            OsString::from("--print"),
            OsString::from("--disable-slash-commands"),
            OsString::from("--model"),
            OsString::from("claude-opus-5-5-medium"),
            OsString::from("--effort"),
            OsString::from("high"),
            OsString::from("--mode"),
            OsString::from("plan"),
            OsString::from("--sandbox"),
            OsString::from("--"),
            OsString::from("--dangerously-skip-permissions"),
        ]
    );
    Ok(())
}

struct ShareWorkerSeed<'a> {
    seed: u128,
    name: &'a str,
    engine: CliEngine,
    model: &'a str,
    added_at_ms: i64,
}

fn add_share_worker(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    parent_id: Uuid,
    spec: &ShareWorkerSeed<'_>,
) -> TestResult<(SwarmMemberRecord, AgentProfileId, AgentProfileRevision)> {
    let (prof_id, rev) =
        create_active_profile(runtime, spec.seed, spec.name, spec.engine, spec.model)?;
    let member = runtime
        .add_swarm_member(&AddSwarmMemberCommand {
            member_id: Uuid::from_u128(spec.seed + 0x0100),
            workspace_id,
            swarm_run_id,
            roster_name: spec.name.to_owned(),
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(parent_id),
            workspace_mode: SwarmWorkspaceMode::Share,
            worktree_path: Some(valid_worktree()?),
            identity: member_identity(prof_id, spec.seed + 0x0200)?,
            initial_channels: Vec::new(),
            added_at_ms: spec.added_at_ms,
        })?
        .map_err(|e| format!("add worker {}: {e}", spec.name))?;
    Ok((member, prof_id, rev))
}

fn add_inherit_verifier(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    seed: u128,
    name: &str,
    added_at_ms: i64,
) -> TestResult<SwarmMemberRecord> {
    let (prof_id, _) = create_active_profile(
        runtime,
        seed,
        name,
        CliEngine::Agy,
        "claude-opus-5-5-medium",
    )?;
    Ok(runtime
        .add_swarm_member(&AddSwarmMemberCommand {
            member_id: Uuid::from_u128(seed + 0x0100),
            workspace_id,
            swarm_run_id,
            roster_name: name.to_owned(),
            role_kind: SwarmRoleKind::Verifier,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            worktree_path: None,
            identity: member_identity(prof_id, seed + 0x0200)?,
            initial_channels: Vec::new(),
            added_at_ms,
        })?
        .map_err(|e| format!("add verifier {name}: {e}"))?)
}

fn post_board_msg(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    channel_id: Uuid,
    author_member_id: Uuid,
    body: &str,
    submitted_at_ms: i64,
) -> TestResult {
    runtime
        .post_swarm_message(&PostSwarmMessageCommand {
            message_id: Uuid::now_v7(),
            workspace_id,
            swarm_run_id,
            channel_id,
            author: SwarmMessageAuthor::Member(author_member_id),
            body: body.to_owned(),
            context: MessageContext::default(),
            submitted_at_ms,
        })?
        .map_err(|e| format!("post_swarm_message: {e}"))?;
    Ok(())
}

fn seed_queued_parent_workers(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
    run: &SwarmRunRecord,
    channel_id: Uuid,
    coordinator_id: Uuid,
    count: u128,
) -> TestResult<Vec<Uuid>> {
    let mut parent_ids = Vec::new();
    for idx in 0..count {
        let name = format!("parent-worker-{idx}");
        let (member, _, _) = add_share_worker(
            runtime,
            workspace_id,
            run.swarm_run_id(),
            coordinator_id,
            &ShareWorkerSeed {
                seed: 0x4100 + idx * 0x1000,
                name: &name,
                engine: CliEngine::Codex,
                model: "gpt-6-luna",
                added_at_ms: 130,
            },
        )?;
        assert!(member_requires_parent_spawner_slot(run, &member));
        post_board_msg(
            runtime,
            workspace_id,
            run.swarm_run_id(),
            channel_id,
            coordinator_id,
            &format!("@{name} go"),
            140 + i64::try_from(idx)?,
        )?;
        parent_ids.push(member.member_id());
    }
    Ok(parent_ids)
}

#[tokio::test]
async fn parent_slot_headroom_reserves_two_slots_for_leaf_helpers_and_verifiers() -> TestResult {
    let dir = TestRuntimeDir::new("headroom")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7002));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;

    let runner = ScriptedRunner::new();
    let parent_cvar = Arc::new(std::sync::Condvar::new());
    let parent_released = Arc::new(Mutex::new(false));
    for _ in 0..MAX_SPAWNING_PARENT_SLOTS {
        runner.push_parked(Arc::clone(&parent_cvar), Arc::clone(&parent_released), 0)?;
    }
    runner.push_exit(0)?;
    runner.push_exit(0)?;

    let parent_ids = seed_queued_parent_workers(
        &runtime,
        workspace_id,
        &run,
        main_channel.channel_id(),
        coordinator.member_id(),
        7,
    )?;

    let verifier = add_inherit_verifier(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        0xE000,
        "verifier-one",
        150,
    )?;
    assert!(!member_requires_parent_spawner_slot(&run, &verifier));
    post_board_msg(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        main_channel.channel_id(),
        coordinator.member_id(),
        "@verifier-one audit",
        160,
    )?;

    let dispatcher = runtime.swarm_dispatcher(runner);
    assert_eq!(
        dispatcher.available_total_slots(),
        DEFAULT_SWARM_SUPERVISION_SLOTS
    );
    assert_eq!(
        dispatcher.available_leaf_headroom_slots(),
        MIN_LEAF_HEADROOM_SLOTS
    );
    let mut parent_handles = Vec::new();
    for &pid in &parent_ids {
        let d = dispatcher.clone();
        let req = sample_dispatch_request(workspace_id, run.swarm_run_id(), pid, 200)?;
        parent_handles.push(tokio::spawn(
            async move { d.dispatch_member_turn(req).await },
        ));
    }

    tokio::time::timeout(Duration::from_secs(3), async {
        while dispatcher.available_parent_spawner_slots() > 0
            || dispatcher.available_total_slots() != MIN_LEAF_HEADROOM_SLOTS
            || dispatcher.available_leaf_headroom_slots() != MIN_LEAF_HEADROOM_SLOTS
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let verifier_outcome = tokio::time::timeout(
        Duration::from_secs(3),
        dispatcher.dispatch_member_turn(sample_dispatch_request(
            workspace_id,
            run.swarm_run_id(),
            verifier.member_id(),
            250,
        )?),
    )
    .await??;
    assert!(!verifier_outcome.used_parent_spawner_slot);
    assert!(
        !verifier_outcome
            .committed_snapshot
            .permissions()
            .allow_workspace_write()
    );

    *parent_released.lock().map_err(|_| "release lock")? = true;
    parent_cvar.notify_all();
    for handle in parent_handles {
        let res = handle.await??;
        assert!(res.used_parent_spawner_slot);
    }
    Ok(())
}

async fn dispatch_turn_at(
    dispatcher: &SwarmRuntimeDispatcher<ScriptedRunner>,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    member_id: Uuid,
    at_ms: i64,
) -> TestResult<SwarmDispatchedTurnOutcome> {
    Ok(dispatcher
        .dispatch_member_turn(sample_dispatch_request(
            workspace_id,
            swarm_run_id,
            member_id,
            at_ms,
        )?)
        .await?)
}

#[tokio::test]
async fn crash_watermark_preservation_and_attempt_boundary_quota_failover_lose_zero_messages()
-> TestResult {
    let dir = TestRuntimeDir::new("crash-quota")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7003));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;
    let run_id = run.swarm_run_id();
    let (worker, worker_prof, initial_rev) = add_share_worker(
        &runtime,
        workspace_id,
        run_id,
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0x7100,
            name: "author-one",
            engine: CliEngine::Agy,
            model: "gemini-4-carbon",
            added_at_ms: 120,
        },
    )?;
    for seq_idx in 1..=2_i64 {
        post_board_msg(
            &runtime,
            workspace_id,
            run_id,
            main_channel.channel_id(),
            coordinator.member_id(),
            &format!("@author-one instruction {seq_idx}"),
            150 + seq_idx,
        )?;
    }

    let runner = ScriptedRunner::new();
    runner.push_exit(1)?;
    runner.push_exit(DEFAULT_QUOTA_EXIT_CODE)?;
    runner.push_exit(0)?;
    let dispatcher = runtime.swarm_dispatcher(runner.clone());

    let crashed =
        dispatch_turn_at(&dispatcher, workspace_id, run_id, worker.member_id(), 200).await?;
    assert!(matches!(
        crashed.disposition,
        SwarmTurnDisposition::TransientFailed {
            exit_code: Some(1),
            ..
        }
    ));
    assert_eq!(crashed.disposition.member().committed_seen_seq(), 0);
    assert_eq!(
        crashed.disposition.member().member_state(),
        SwarmMemberState::WakeQueued
    );

    let quota_hit =
        dispatch_turn_at(&dispatcher, workspace_id, run_id, worker.member_id(), 250).await?;
    assert_eq!(quota_hit.disposition.member().committed_seen_seq(), 0);
    assert_eq!(
        quota_hit.disposition.member().member_state(),
        SwarmMemberState::SuspendedQuota
    );

    let fallback_pref = ExecutionPreference::new(
        CliEngine::Codex,
        "gpt-6-luna".to_owned(),
        Some("max".to_owned()),
        vec!["rust".to_owned()],
        Some(RolePromptDigest::new([0x5A; 32])?),
    )?;
    let switched = dispatcher.switch_member_model_and_requeue(&SwitchSwarmMemberModelCommand {
        workspace_id,
        swarm_run_id: run_id,
        member_id: worker.member_id(),
        target: SwarmModelSwitchTarget::UpdateProfilePreference {
            request_id: Uuid::now_v7(),
            profile_id: worker_prof,
            expected_profile_revision: initial_rev,
            new_preference: fallback_pref,
        },
        updated_at_ms: 300,
    })?;
    assert_eq!(switched.member.member_state(), SwarmMemberState::WakeQueued);
    assert_eq!(switched.member.committed_seen_seq(), 0);

    let recovered =
        dispatch_turn_at(&dispatcher, workspace_id, run_id, worker.member_id(), 350).await?;
    assert_eq!(recovered.wakeup_batch.messages().len(), 2);
    assert_eq!(
        recovered.committed_snapshot.resolved_engine(),
        CliEngine::Codex
    );
    assert_eq!(recovered.committed_snapshot.resolved_model(), "gpt-6-luna");
    assert!(
        recovered.committed_snapshot.attempt_key().attempt()
            > quota_hit.committed_snapshot.attempt_key().attempt()
    );
    assert_ne!(
        recovered.committed_snapshot.snapshot_id(),
        quota_hit.committed_snapshot.snapshot_id()
    );
    assert_eq!(recovered.disposition.member().committed_seen_seq(), 2);
    assert_eq!(runner.recorded_snapshots()?.len(), 3);
    Ok(())
}

#[test]
fn stall_monitor_nudges_single_bottleneck_and_aborts_when_max_nudges_exceeded() -> TestResult {
    let dir = TestRuntimeDir::new("stall-monitor")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7004));
    let (run, _main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;
    let _worker = add_share_worker(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0x8100,
            name: "worker-idle",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 120,
        },
    )?;

    let policy = SwarmStallMonitorPolicy {
        stall_grace_ms: 500,
        max_stall_nudges: 2,
        max_ping_pong_streak: 4,
        max_member_turns: 16,
    };

    let before_grace = runtime
        .evaluate_swarm_stall_and_livelock(workspace_id, run.swarm_run_id(), policy, 520)?
        .map_err(|e| format!("before_grace: {e}"))?;
    assert!(matches!(
        before_grace,
        SwarmMonitorActionReport::NoAction { .. }
    ));

    let nudge_1 = runtime
        .evaluate_swarm_stall_and_livelock(workspace_id, run.swarm_run_id(), policy, 700)?
        .map_err(|e| format!("nudge_1: {e}"))?;
    assert!(matches!(
        nudge_1,
        SwarmMonitorActionReport::StallNudged {
            target_member_id,
            reason: SwarmBottleneckReason::AllMembersWaitingCoordinatorFallback,
            ..
        } if target_member_id == coordinator.member_id()
    ));

    let nudge_2 = runtime
        .evaluate_swarm_stall_and_livelock(workspace_id, run.swarm_run_id(), policy, 1_300)?
        .map_err(|e| format!("nudge_2: {e}"))?;
    assert!(matches!(
        nudge_2,
        SwarmMonitorActionReport::StallNudged { .. }
    ));

    let escalated = runtime
        .evaluate_swarm_stall_and_livelock(workspace_id, run.swarm_run_id(), policy, 1_900)?
        .map_err(|e| format!("escalated: {e}"))?;
    match escalated {
        SwarmMonitorActionReport::StallAborted { outcome, .. } => {
            assert!(outcome.escalated_to_aborted());
            assert_eq!(outcome.run().state(), SwarmRunState::Aborted);
        }
        other => return Err(format!("expected StallAborted, got {other:?}").into()),
    }
    Ok(())
}

#[test]
fn livelock_breaker_halts_ping_pong_pair_and_escalates_only_to_coordinator() -> TestResult {
    let dir = TestRuntimeDir::new("livelock")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7005));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;

    let (worker_a, _, _) = add_share_worker(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0x9100,
            name: "worker-a",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 120,
        },
    )?;
    let (worker_b, _, _) = add_share_worker(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0x9500,
            name: "worker-b",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 125,
        },
    )?;

    for round in 0..2_i64 {
        post_board_msg(
            &runtime,
            workspace_id,
            run.swarm_run_id(),
            main_channel.channel_id(),
            worker_a.member_id(),
            "@worker-b your turn",
            200 + round * 10,
        )?;
        post_board_msg(
            &runtime,
            workspace_id,
            run.swarm_run_id(),
            main_channel.channel_id(),
            worker_b.member_id(),
            "@worker-a no, your turn",
            205 + round * 10,
        )?;
    }

    let policy = SwarmStallMonitorPolicy {
        stall_grace_ms: 60_000,
        max_stall_nudges: 3,
        max_ping_pong_streak: 4,
        max_member_turns: 20,
    };
    let report = runtime
        .evaluate_swarm_stall_and_livelock(workspace_id, run.swarm_run_id(), policy, 250)?
        .map_err(|e| format!("livelock check: {e}"))?;

    match report {
        SwarmMonitorActionReport::LivelockBroken {
            reason: SwarmLivelockReason::PingPongStreakExceeded { observed_streak: 4 },
            outcome,
        } => {
            assert_eq!(outcome.stopped_members().len(), 2);
            assert!(
                outcome
                    .stopped_members()
                    .iter()
                    .all(|m| m.member_state() == SwarmMemberState::Stopped)
            );
            assert!(outcome.escalation_message().body().contains("[livelock]"));
        }
        other => return Err(format!("expected LivelockBroken, got {other:?}").into()),
    }

    let second_report = runtime
        .evaluate_swarm_stall_and_livelock(workspace_id, run.swarm_run_id(), policy, 260)?
        .map_err(|e| format!("second livelock check: {e}"))?;
    assert!(matches!(
        second_report,
        SwarmMonitorActionReport::NoAction { .. }
    ));
    Ok(())
}

fn seed_candidate_ready_run_with_accepted_verdict(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
    tree_digest: [u8; 32],
    diff_digest: [u8; 32],
) -> TestResult<Uuid> {
    let (run, _main_channel, coordinator) =
        setup_swarm_run_with_coordinator(runtime, workspace_id)?;
    let _author = add_share_worker(
        runtime,
        workspace_id,
        run.swarm_run_id(),
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0xA100,
            name: "author",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 120,
        },
    )?;
    let verifier = add_inherit_verifier(
        runtime,
        workspace_id,
        run.swarm_run_id(),
        0xA500,
        "verifier",
        125,
    )?;
    runtime
        .record_swarm_convergence_candidate(&RecordSwarmConvergenceCommand {
            workspace_id,
            swarm_run_id: run.swarm_run_id(),
            converged_commit_oid: SwarmCommitOid::new("fedcba9876543210fedcba9876543210fedcba98")?,
            converged_tree_blake3: tree_digest,
            converged_diff_digest: diff_digest,
            recorded_at_ms: 200,
        })?
        .map_err(|e| format!("record candidate: {e}"))?;
    runtime
        .record_swarm_verdict(&RecordSwarmVerdictCommand {
            request_id: Uuid::now_v7(),
            workspace_id,
            swarm_run_id: run.swarm_run_id(),
            panel_id: None,
            verifier_member_id: verifier.member_id(),
            outcome: ReviewVerdictOutcome::Accepted,
            reviewed_diff_digest: diff_digest,
            rationale: "All checks green; diff matches converged digest.".to_owned(),
            recorded_at_ms: 220,
        })?
        .map_err(|e| format!("record verdict: {e}"))?;
    Ok(run.swarm_run_id())
}

#[test]
fn autonomy_outbox_bridge_consumes_swarm_settled_kind_6_alongside_dag_settled_kind_5() -> TestResult
{
    assert_eq!(
        AutonomyOutboxFactKind::from_raw_kind(DAG_SETTLED_KIND)?,
        AutonomyOutboxFactKind::DagSettled
    );
    assert_eq!(
        AutonomyOutboxFactKind::from_raw_kind(SWARM_SETTLED_KIND)?,
        AutonomyOutboxFactKind::SwarmSettled
    );

    let dir = TestRuntimeDir::new("outbox-bridge")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7006));
    let tree_digest = [0x44; 32];
    let diff_digest = [0x55; 32];
    let swarm_run_id = seed_candidate_ready_run_with_accepted_verdict(
        &runtime,
        workspace_id,
        tree_digest,
        diff_digest,
    )?;

    let migration_id = Uuid::from_u128(0xA900);
    let swarm_fact = AutonomySettlementOutboxFact::SwarmSettled(SwarmSettledFact {
        sequence: 1,
        migration_id,
        destination_id: runtime.destination_id(),
        settlement_id: Uuid::from_u128(0xA901),
        workspace_id,
        activity_id: Some(ActivityId::from_uuid(Uuid::from_u128(0xA902))),
        swarm_run_id: SwarmRunId::from_uuid(swarm_run_id),
        settled_file_count: 3,
        settled_tree_digest: tree_digest,
        converged_diff_digest: diff_digest,
        coordinator_agent_id: forge_domain::AgentInstanceId::from_uuid(Uuid::from_u128(0xA903)),
        coordinator_session_id: Some(forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(
            0xA904,
        ))),
        settled_at_ms: 250,
    });
    assert_eq!(swarm_fact.raw_fact_kind(), SWARM_SETTLED_KIND);

    let dag_fact =
        AutonomySettlementOutboxFact::CoreEffect(EffectFactInput::DagSettled(DagSettledFact {
            sequence: 1,
            migration_id,
            destination_id: runtime.destination_id(),
            settlement_id: Uuid::from_u128(0xA999),
            workspace_id,
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0xA902)),
            goal_id: GoalId::from_uuid(Uuid::from_u128(0xA905)),
            goal_revision: 1,
            dag_revision: 1,
            settled_file_count: 1,
            settled_tree_digest: [0x66; 32],
            coordinator_agent_id: forge_domain::AgentInstanceId::from_uuid(Uuid::from_u128(0xA903)),
            coordinator_session_id: None,
            settled_at_ms: 250,
        }));
    assert_eq!(dag_fact.raw_fact_kind(), DAG_SETTLED_KIND);

    let bridge = runtime.autonomy_outbox_bridge();
    let applied = bridge.apply_outbox_facts(migration_id, &[swarm_fact])?;
    assert!(!applied.swarm_settlements[0].replayed);
    assert_eq!(
        applied.swarm_settlements[0].run.state(),
        SwarmRunState::Settled
    );
    assert_eq!(bridge.last_swarm_settled_sequence(migration_id)?, 1);
    assert!(
        bridge
            .apply_outbox_facts(migration_id, &[swarm_fact])?
            .swarm_settlements[0]
            .replayed
    );
    Ok(())
}

fn read_member_state_and_watermark(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    member_id: Uuid,
) -> TestResult<(SwarmMemberState, u64, Option<Uuid>)> {
    let snap = runtime
        .get_swarm_run_snapshot(workspace_id, swarm_run_id)?
        .map_err(|e| format!("snapshot: {e}"))?
        .ok_or("missing run snapshot")?;
    let member = snap
        .members()
        .iter()
        .find(|m| m.member_id() == member_id)
        .ok_or("missing member")?;
    Ok((
        member.member_state(),
        member.committed_seen_seq(),
        member.active_snapshot_id(),
    ))
}

#[tokio::test]
async fn dropping_dispatch_member_turn_future_mid_supervision_reconciles_working_state()
-> TestResult {
    let dir = TestRuntimeDir::new("drop-safety")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7007));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;
    let (worker, _, _) = add_share_worker(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0xB100,
            name: "drop-worker",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 120,
        },
    )?;
    post_board_msg(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        main_channel.channel_id(),
        coordinator.member_id(),
        "@drop-worker start work",
        150,
    )?;

    let runner = ScriptedRunner::new();
    let cvar = Arc::new(std::sync::Condvar::new());
    let released = Arc::new(Mutex::new(false));
    runner.push_parked(Arc::clone(&cvar), Arc::clone(&released), 1)?;
    let dispatcher = runtime.swarm_dispatcher(runner);

    let timed_out = tokio::time::timeout(
        Duration::from_millis(30),
        dispatcher.dispatch_member_turn(sample_dispatch_request(
            workspace_id,
            run.swarm_run_id(),
            worker.member_id(),
            200,
        )?),
    )
    .await;
    assert!(
        timed_out.is_err(),
        "caller future must drop mid-supervision"
    );

    let (mid_state, mid_seq, mid_snap) = read_member_state_and_watermark(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        worker.member_id(),
    )?;
    assert_eq!(mid_state, SwarmMemberState::Working);
    assert_eq!(mid_seq, 0);
    assert!(mid_snap.is_some());

    let dup = dispatcher
        .dispatch_member_turn(sample_dispatch_request(
            workspace_id,
            run.swarm_run_id(),
            worker.member_id(),
            205,
        )?)
        .await;
    assert!(matches!(
        dup,
        Err(SwarmDispatchError::MemberAlreadyInFlight { .. })
    ));

    *released.lock().map_err(|_| "release lock")? = true;
    cvar.notify_all();
    tokio::time::timeout(Duration::from_secs(3), async {
        while dispatcher.available_total_slots() != DEFAULT_SWARM_SUPERVISION_SLOTS {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let (final_state, final_seq, final_snap) = read_member_state_and_watermark(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        worker.member_id(),
    )?;
    assert_eq!(final_state, SwarmMemberState::WakeQueued);
    assert_eq!(final_seq, 0);
    assert!(final_snap.is_none());
    Ok(())
}

#[test]
fn working_turn_drop_guard_reconciles_aborted_turn_without_stranding_working_state() -> TestResult {
    let dir = TestRuntimeDir::new("working-guard")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7008));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;
    let (worker, _, _) = add_share_worker(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0xB500,
            name: "guard-worker",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 120,
        },
    )?;
    post_board_msg(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        main_channel.channel_id(),
        coordinator.member_id(),
        "@guard-worker check",
        150,
    )?;
    let dispatcher = runtime.swarm_dispatcher(ScriptedRunner::new());
    let req = sample_dispatch_request(workspace_id, run.swarm_run_id(), worker.member_id(), 200)?;
    let (_, committed_snapshot) = dispatcher.claim_and_seal_snapshot(&req)?;
    let guard = WorkingTurnDropGuard::arm(
        Arc::clone(&dispatcher.operations),
        workspace_id,
        run.swarm_run_id(),
        worker.member_id(),
        committed_snapshot.snapshot_id(),
        committed_snapshot.attempt_key(),
        req.dispatched_at_ms,
    );
    drop(guard);

    let (state, seq, active_snap) = read_member_state_and_watermark(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        worker.member_id(),
    )?;
    assert_eq!(state, SwarmMemberState::WakeQueued);
    assert_eq!(seq, 0);
    assert!(active_snap.is_none());
    Ok(())
}

struct FixVerifyRoundCtx<'a> {
    runtime: &'a OrchestrationRuntime,
    dispatcher: &'a SwarmRuntimeDispatcher<ScriptedRunner>,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    channel_id: Uuid,
    worker_id: Uuid,
    verifier_id: Uuid,
}

async fn run_fix_and_verify_round(
    ctx: &FixVerifyRoundCtx<'_>,
    round: u8,
    outcome: ReviewVerdictOutcome,
) -> TestResult {
    let base_ms = 200 + i64::from(round) * 100;
    let mut worker_req =
        sample_dispatch_request(ctx.workspace_id, ctx.swarm_run_id, ctx.worker_id, base_ms)?;
    let tree_digest = [round; 32];
    let diff_digest = [round.wrapping_add(0x80); 32];
    let oid = format!("{round:02x}23456789abcdef0123456789abcdef01234567");
    worker_req.clean_exit_output = Some(SwarmTurnOutput {
        commit_oid: SwarmCommitOid::new(&oid)?,
        tree_blake3: tree_digest,
    });
    post_board_msg(
        ctx.runtime,
        ctx.workspace_id,
        ctx.swarm_run_id,
        ctx.channel_id,
        ctx.worker_id,
        &format!("@verifier-rev ready round {round}"),
        base_ms + 5,
    )?;
    ctx.dispatcher.dispatch_member_turn(worker_req).await?;
    ctx.runtime
        .record_swarm_convergence_candidate(&RecordSwarmConvergenceCommand {
            workspace_id: ctx.workspace_id,
            swarm_run_id: ctx.swarm_run_id,
            converged_commit_oid: SwarmCommitOid::new(&oid)?,
            converged_tree_blake3: tree_digest,
            converged_diff_digest: diff_digest,
            recorded_at_ms: base_ms + 15,
        })?
        .map_err(|e| format!("candidate round {round}: {e}"))?;
    ctx.runtime
        .record_swarm_verdict(&RecordSwarmVerdictCommand {
            request_id: Uuid::now_v7(),
            workspace_id: ctx.workspace_id,
            swarm_run_id: ctx.swarm_run_id,
            panel_id: None,
            verifier_member_id: ctx.verifier_id,
            outcome,
            reviewed_diff_digest: diff_digest,
            rationale: format!("round {round} review"),
            recorded_at_ms: base_ms + 30,
        })?
        .map_err(|e| format!("verdict round {round}: {e}"))?;
    post_board_msg(
        ctx.runtime,
        ctx.workspace_id,
        ctx.swarm_run_id,
        ctx.channel_id,
        ctx.verifier_id,
        &format!("@worker-rev feedback round {round}"),
        base_ms + 40,
    )?;
    dispatch_turn_at(
        ctx.dispatcher,
        ctx.workspace_id,
        ctx.swarm_run_id,
        ctx.verifier_id,
        base_ms + 50,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn multi_round_needs_changes_cycles_never_false_positive_livelock_until_stagnation()
-> TestResult {
    let dir = TestRuntimeDir::new("multi-round-verify")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7009));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;
    let run_id = run.swarm_run_id();
    let (worker, _, _) = add_share_worker(
        &runtime,
        workspace_id,
        run_id,
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0xC100,
            name: "worker-rev",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 120,
        },
    )?;
    let verifier =
        add_inherit_verifier(&runtime, workspace_id, run_id, 0xC500, "verifier-rev", 125)?;
    post_board_msg(
        &runtime,
        workspace_id,
        run_id,
        main_channel.channel_id(),
        coordinator.member_id(),
        "@worker-rev implement feature",
        140,
    )?;

    let runner = ScriptedRunner::new();
    for _ in 0..16 {
        runner.push_exit(0)?;
    }
    let dispatcher = runtime.swarm_dispatcher(runner);
    let ctx = FixVerifyRoundCtx {
        runtime: &runtime,
        dispatcher: &dispatcher,
        workspace_id,
        swarm_run_id: run_id,
        channel_id: main_channel.channel_id(),
        worker_id: worker.member_id(),
        verifier_id: verifier.member_id(),
    };
    let policy = SwarmStallMonitorPolicy {
        stall_grace_ms: 10_000,
        max_stall_nudges: 3,
        max_ping_pong_streak: 3,
        max_member_turns: 4,
    };

    for round in 1..=5_u8 {
        run_fix_and_verify_round(&ctx, round, ReviewVerdictOutcome::NeedsChanges).await?;
        let report = runtime
            .evaluate_swarm_stall_and_livelock(
                workspace_id,
                run_id,
                policy,
                260 + i64::from(round) * 100,
            )?
            .map_err(|e| format!("monitor round {round}: {e}"))?;
        assert!(matches!(report, SwarmMonitorActionReport::NoAction { .. }));
    }

    for step in 0..2_i64 {
        post_board_msg(
            &runtime,
            workspace_id,
            run_id,
            main_channel.channel_id(),
            worker.member_id(),
            "@verifier-rev still no diff",
            900 + step * 20,
        )?;
        post_board_msg(
            &runtime,
            workspace_id,
            run_id,
            main_channel.channel_id(),
            verifier.member_id(),
            "@worker-rev still failing",
            910 + step * 20,
        )?;
    }
    let stagnant = runtime
        .evaluate_swarm_stall_and_livelock(workspace_id, run_id, policy, 990)?
        .map_err(|e| format!("stagnant monitor: {e}"))?;
    assert!(matches!(
        stagnant,
        SwarmMonitorActionReport::LivelockBroken {
            reason: SwarmLivelockReason::PingPongStreakExceeded { .. },
            ..
        }
    ));
    Ok(())
}

#[test]
fn dispatcher_worker_bearer_file_builder_validates_and_forwards_optional_paths() -> TestResult {
    let dir = TestRuntimeDir::new("bearer-builder")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let scratch = TestNonSsdScratch::new("disp-builder-bearer")?;
    let valid_file = scratch.0.join("valid.bearer");
    std::fs::write(&valid_file, "tok-valid")?;
    let symlink_file = scratch.0.join("link.bearer");
    std::os::unix::fs::symlink(&valid_file, &symlink_file)?;
    let oversized_file = scratch.0.join("oversized.bearer");
    std::fs::write(&oversized_file, vec![b'a'; 4_097])?;
    let ssd_file = dir.0.join("ssd.bearer");
    std::fs::write(&ssd_file, "tok-ssd")?;

    let none_disp = runtime
        .swarm_dispatcher(ScriptedRunner::new())
        .with_optional_worker_bearer_file(None)?;
    assert_eq!(none_disp.worker_bearer_file(), None);

    let some_disp = runtime
        .swarm_dispatcher(ScriptedRunner::new())
        .with_optional_worker_bearer_file(Some(valid_file.clone()))?;
    assert_eq!(some_disp.worker_bearer_file(), Some(valid_file.as_path()));

    for bad_path in [symlink_file, oversized_file, ssd_file] {
        assert!(matches!(
            runtime
                .swarm_dispatcher(ScriptedRunner::new())
                .with_worker_bearer_file(bad_path.clone()),
            Err(ExecutionSupervisorError::InvalidExecutionContext(_))
        ));
        assert!(matches!(
            runtime
                .swarm_dispatcher(ScriptedRunner::new())
                .with_optional_worker_bearer_file(Some(bad_path)),
            Err(ExecutionSupervisorError::InvalidExecutionContext(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn clean_exit_hook_captures_output_on_zero_exit_skips_nonzero_and_recovers_on_error()
-> TestResult {
    let dir = TestRuntimeDir::new("clean-exit-hook")?;
    let runtime = OrchestrationRuntime::open(&dir.0)?;
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x7025));
    let (run, main_channel, coordinator) =
        setup_swarm_run_with_coordinator(&runtime, workspace_id)?;
    let (worker, _, _) = add_share_worker(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        coordinator.member_id(),
        &ShareWorkerSeed {
            seed: 0x7900,
            name: "hook-worker",
            engine: CliEngine::Codex,
            model: "gpt-6-luna",
            added_at_ms: 120,
        },
    )?;
    post_board_msg(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        main_channel.channel_id(),
        coordinator.member_id(),
        "@hook-worker build candidate",
        150,
    )?;

    let runner = ScriptedRunner::new();
    runner.push_exit(1)?;
    runner.push_exit(0)?;
    runner.push_exit(0)?;
    let dispatcher = runtime.swarm_dispatcher(runner);

    let crash_outcome = dispatcher
        .dispatch_member_turn_with_clean_exit_hook(
            sample_dispatch_request(workspace_id, run.swarm_run_id(), worker.member_id(), 200)?,
            |_, _| async { Err(SwarmDispatchError::ForgeBridge("must not run on crash")) },
        )
        .await?;
    assert!(matches!(
        crash_outcome.disposition,
        SwarmTurnDisposition::TransientFailed {
            exit_code: Some(1),
            ..
        }
    ));

    let expected_oid = SwarmCommitOid::new("0123456789abcdef0123456789abcdef01234567")?;
    let captured_oid = expected_oid.clone();
    let clean_outcome = dispatcher
        .dispatch_member_turn_with_clean_exit_hook(
            sample_dispatch_request(workspace_id, run.swarm_run_id(), worker.member_id(), 250)?,
            move |_, _| async move {
                Ok(Some(SwarmTurnOutput {
                    commit_oid: captured_oid,
                    tree_blake3: [0xAB; 32],
                }))
            },
        )
        .await?;
    assert_eq!(
        clean_outcome.disposition.member().last_output_commit_oid(),
        Some(&expected_oid)
    );
    assert_eq!(
        clean_outcome.disposition.member().last_output_tree_blake3(),
        Some([0xAB; 32])
    );

    post_board_msg(
        &runtime,
        workspace_id,
        run.swarm_run_id(),
        main_channel.channel_id(),
        coordinator.member_id(),
        "@hook-worker second task",
        300,
    )?;
    let hook_err = dispatcher
        .dispatch_member_turn_with_clean_exit_hook(
            sample_dispatch_request(workspace_id, run.swarm_run_id(), worker.member_id(), 350)?,
            |_, _| async { Err(SwarmDispatchError::ForgeBridge("unavailable")) },
        )
        .await;
    assert!(matches!(
        hook_err,
        Err(SwarmDispatchError::ForgeBridge("unavailable"))
    ));
    let snapshot = runtime
        .get_swarm_run_snapshot(workspace_id, run.swarm_run_id())?
        .map_err(|e| format!("snapshot: {e}"))?
        .ok_or("missing snapshot")?;
    let recovered_member = snapshot
        .members()
        .iter()
        .find(|m| m.member_id() == worker.member_id())
        .ok_or("missing worker")?;
    assert_eq!(recovered_member.member_state(), SwarmMemberState::Stopped);
    assert_eq!(recovered_member.committed_seen_seq(), 1);
    Ok(())
}
