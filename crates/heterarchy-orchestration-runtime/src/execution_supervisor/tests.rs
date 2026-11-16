//! `Builder + Dictionary` test suite for [`super::ExecutionSupervisor`],
//! [`super::LocalProcessRunner`], and [`super::CliExecutableResolver`].

use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use forge_domain::{
    ActivityId, AgentAssignmentId, DagNodeId, DagRevisionNumber, DirectTokenId, GoalId, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AssignmentExecutionSnapshot, AssignmentExecutionSnapshotParts, ExecutionAttemptKey,
    ExecutionPermissions, ExecutionTimeouts, ForgeWorktreeRef, LaunchLedgerRecord,
    LaunchLedgerState, LaunchUnknownReason, LeaseRef, McpGrant, OperationsCatalog,
    ProcessOwnershipIdentity,
};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileRevision, CliEngine, ExecutionPreference, RolePromptDigest,
};
use uuid::Uuid;

use super::invocation::{is_forbidden_ssd_path, validate_worktree_dir_with_forbidden};
use super::local_runner::{
    IMMEDIATE_EXIT_START_TIME, UnconfirmedSpawnGuard, inspect_process_group,
};
use super::{
    BootRecoveryCleanup, CliExecutableResolver, CliExecutionContext, CommandOverride,
    ExecutionRunner, ExecutionSupervisor, ExecutionSupervisorError, FORBIDDEN_SSD_BUILD_PREFIX,
    LocalProcessRunner, LocalProcessRunnerConfig, MAX_CLI_PROMPT_BYTES, OwnershipVerification,
    SpawnedProcessHandle, SupervisionOutcome, TerminationDisposition, current_system_boot_id,
    validate_worktree_dir, wall_clock_ms,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

/// Canonical domain fixture dictionary shared across supervisor test scenarios.
mod dict {
    use uuid::Uuid;

    pub const DESTINATION_ID: Uuid = Uuid::from_u128(0x5105_0001);
    pub const WORKSPACE_UUID: Uuid = Uuid::from_u128(0x5105_0002);
    pub const ACTIVITY_UUID: Uuid = Uuid::from_u128(0x5105_0003);
    pub const GOAL_UUID: Uuid = Uuid::from_u128(0x5105_0004);
    pub const NODE_UUID: Uuid = Uuid::from_u128(0x5105_0005);
    pub const ASSIGNMENT_UUID: Uuid = Uuid::from_u128(0x5105_0006);
    pub const PROFILE_UUID: Uuid = Uuid::from_u128(0x5105_0007);
    pub const LEASE_UUID: Uuid = Uuid::from_u128(0x5105_0008);
    pub const MCP_TOKEN_UUID: Uuid = Uuid::from_u128(0x5105_0009);

    pub const DAG_REVISION: i64 = 1;
    pub const PROFILE_REVISION: i64 = 1;
    pub const LEASE_REVISION: u64 = 1;

    pub const COMMITTED_AT_MS: i64 = 1_700_000_000_000;
    pub const MCP_ISSUED_AT_MS: i64 = COMMITTED_AT_MS - 1_000;
    pub const LEASE_EXPIRES_AT_MS: i64 = COMMITTED_AT_MS + 60_000;
    pub const MCP_EXPIRES_AT_MS: i64 = COMMITTED_AT_MS + 60_000;

    pub const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 1_000;
    pub const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 5_000;
    pub const DEFAULT_TERMINATION_GRACE_MS: u64 = 300;
    pub const FAST_EXECUTION_TIMEOUT_MS: u64 = 150;

    pub const MODEL_CODEX: &str = "gpt-6-luna";
    pub const MODEL_AGY: &str = "claude-sonnet-5-5-medium";
    pub const REASONING_HIGH: &str = "high";
    pub const SHELL_BIN: &str = "/bin/sh";
    pub const PRIOR_OS_BOOT_ID: &str = "boot-epoch-prior-os-instance";
    pub const FORBIDDEN_WORKTREE_SUBPATH: &str = "/Volumes/256.SSD/RustBuild/forbidden-worktree";
}

/// RAII temporary `OperationsCatalog` directory rooted under `std::env::temp_dir()`
/// (which resolves onto `/Volumes/256.SSD/RustBuild/tmp` under `.codex/rust-ssd`).
struct TestCatalogHarness {
    root: PathBuf,
    catalog: Arc<Mutex<OperationsCatalog>>,
}

impl TestCatalogHarness {
    fn open() -> TestResult<Self> {
        let root = std::env::temp_dir().join(format!("exec-supervisor-{}", Uuid::now_v7()));
        fs::create_dir_all(&root)?;
        let db_path = root.join("agent-operations.sqlite3");
        let catalog = OperationsCatalog::open(&db_path, dict::DESTINATION_ID)?;
        Ok(Self {
            root,
            catalog: Arc::new(Mutex::new(catalog)),
        })
    }

    fn supervisor(&self, config: LocalProcessRunnerConfig) -> ExecutionSupervisor {
        ExecutionSupervisor::new(Arc::clone(&self.catalog), LocalProcessRunner::new(config))
    }

    fn read_attempt(
        &self,
        attempt_key: ExecutionAttemptKey,
    ) -> TestResult<(AssignmentExecutionSnapshot, LaunchLedgerRecord)> {
        let guard = self
            .catalog
            .lock()
            .map_err(|_| "operations catalog poisoned")?;
        guard
            .get_assignment_execution_snapshot(attempt_key)?
            .ok_or_else(|| "expected committed snapshot in catalog".into())
    }
}

impl Drop for TestCatalogHarness {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Fluent builder constructing validated [`AssignmentExecutionSnapshot`] fixtures.
struct SnapshotFixtureBuilder {
    snapshot_id: Uuid,
    request_id: Uuid,
    attempt: u32,
    engine: CliEngine,
    model_override: Option<String>,
    reasoning: Option<String>,
    allow_workspace_write: bool,
    allow_local_shell: bool,
    allow_network: bool,
    startup_timeout_ms: u64,
    execution_timeout_ms: u64,
    termination_grace_ms: u64,
    worktree_path: PathBuf,
}

impl SnapshotFixtureBuilder {
    fn new(attempt: u32) -> Self {
        Self {
            snapshot_id: Uuid::now_v7(),
            request_id: Uuid::now_v7(),
            attempt,
            engine: CliEngine::Codex,
            model_override: None,
            reasoning: Some(dict::REASONING_HIGH.to_owned()),
            allow_workspace_write: true,
            allow_local_shell: true,
            allow_network: false,
            startup_timeout_ms: dict::DEFAULT_STARTUP_TIMEOUT_MS,
            execution_timeout_ms: dict::DEFAULT_EXECUTION_TIMEOUT_MS,
            termination_grace_ms: dict::DEFAULT_TERMINATION_GRACE_MS,
            worktree_path: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        }
    }

    fn engine(mut self, engine: CliEngine) -> Self {
        self.engine = engine;
        self
    }

    fn model_override(mut self, model: impl Into<String>) -> Self {
        self.model_override = Some(model.into());
        self
    }

    fn reasoning(mut self, reasoning: Option<&str>) -> Self {
        self.reasoning = reasoning.map(str::to_owned);
        self
    }

    fn allow_workspace_write(mut self, allow: bool) -> Self {
        self.allow_workspace_write = allow;
        self
    }

    fn permissions(mut self, write: bool, local_shell: bool, network: bool) -> Self {
        self.allow_workspace_write = write;
        self.allow_local_shell = local_shell;
        self.allow_network = network;
        self
    }

    fn timeouts(mut self, startup_ms: u64, execution_ms: u64, grace_ms: u64) -> Self {
        self.startup_timeout_ms = startup_ms;
        self.execution_timeout_ms = execution_ms;
        self.termination_grace_ms = grace_ms;
        self
    }

    fn worktree_path(mut self, worktree_path: PathBuf) -> Self {
        self.worktree_path = worktree_path;
        self
    }

    fn build(&self) -> TestResult<AssignmentExecutionSnapshot> {
        let attempt_key = ExecutionAttemptKey::new(
            WorkspaceId::from_uuid(dict::WORKSPACE_UUID),
            GoalId::from_uuid(dict::GOAL_UUID),
            DagRevisionNumber::new(dict::DAG_REVISION)?,
            DagNodeId::from_uuid(dict::NODE_UUID),
            AgentAssignmentId::from_uuid(dict::ASSIGNMENT_UUID),
            self.attempt,
        )?;
        let profile_revision = AgentProfileRevision::new(dict::PROFILE_REVISION)?;
        let digest = RolePromptDigest::new([0xAB; 32])?;
        let model = self.model_override.as_deref().unwrap_or(match self.engine {
            CliEngine::Codex => dict::MODEL_CODEX,
            CliEngine::Agy => dict::MODEL_AGY,
        });
        let preference = ExecutionPreference::new(
            self.engine,
            model.to_owned(),
            self.reasoning.clone(),
            vec!["rust".to_owned()],
            Some(digest),
        )?;
        let parts = AssignmentExecutionSnapshotParts {
            snapshot_id: self.snapshot_id,
            attempt_key,
            activity_id: ActivityId::from_uuid(dict::ACTIVITY_UUID),
            profile_id: AgentProfileId::from_uuid(dict::PROFILE_UUID)?,
            profile_revision,
            resolved_engine: self.engine,
            resolved_model: model.to_owned(),
            resolved_reasoning: self.reasoning.clone(),
            role_prompt_digest: Some(digest),
            worktree: ForgeWorktreeRef::new(self.worktree_path.clone())?,
            lease: LeaseRef::new(
                dict::LEASE_UUID,
                dict::LEASE_REVISION,
                dict::LEASE_EXPIRES_AT_MS,
            )?,
            mcp_grant: Some(McpGrant::new(
                DirectTokenId::from_uuid(dict::MCP_TOKEN_UUID),
                dict::MCP_ISSUED_AT_MS,
                dict::MCP_EXPIRES_AT_MS,
            )?),
            permissions: ExecutionPermissions::new(
                self.allow_workspace_write,
                self.allow_local_shell,
                self.allow_network,
            ),
            timeouts: ExecutionTimeouts::new(
                self.startup_timeout_ms,
                self.execution_timeout_ms,
                self.termination_grace_ms,
            )?,
            committed_at_ms: dict::COMMITTED_AT_MS,
        };
        Ok(AssignmentExecutionSnapshot::resolve_from_profile(
            parts,
            Some(&preference),
            profile_revision,
        )?)
    }

    fn commit(
        &self,
        harness: &TestCatalogHarness,
    ) -> TestResult<(AssignmentExecutionSnapshot, LaunchLedgerRecord)> {
        let snapshot = self.build()?;
        let mut catalog = harness
            .catalog
            .lock()
            .map_err(|_| "operations catalog poisoned")?;
        Ok(catalog.commit_assignment_execution_snapshot(self.request_id, &snapshot)?)
    }
}

fn shell_override(script: &str) -> LocalProcessRunnerConfig {
    LocalProcessRunnerConfig::new()
        .with_command_override(CommandOverride::new(dict::SHELL_BIN, ["-c", script]))
}

fn wait_for_group_member_count(pgid: u32, min_members: usize, timeout: Duration) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let group = inspect_process_group(pgid)?;
        if group.live_pids.len() >= min_members {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for pgid {pgid} to reach {min_members} live members; saw {:?}",
                group.live_pids
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn direct_runner_proves_parent_and_background_child_share_pgid_and_both_reap() -> TestResult {
    let snapshot = SnapshotFixtureBuilder::new(1).build()?;
    let runner = LocalProcessRunner::new(shell_override("sleep 30 & wait"));

    let mut handle = runner.spawn_prepared(&snapshot)?;
    assert!(handle.has_pinned_child());
    let ownership = handle.ownership().clone();
    assert_eq!(ownership.pid(), ownership.pgid());
    assert_eq!(
        runner.verify_ownership(&ownership)?,
        OwnershipVerification::OwnedAlive
    );

    wait_for_group_member_count(ownership.pgid(), 2, Duration::from_secs(2))?;

    let disposition = runner.terminate_owned_group(
        &mut handle,
        Duration::from_millis(dict::DEFAULT_TERMINATION_GRACE_MS),
    )?;
    assert!(matches!(
        disposition,
        TerminationDisposition::TerminatedAfterTerm | TerminationDisposition::TerminatedAfterKill
    ));
    assert!(handle.has_pinned_child());
    let _ = handle.reap()?;
    assert!(!handle.has_pinned_child());

    let after = inspect_process_group(ownership.pgid())?;
    assert!(after.live_pids.is_empty());
    assert!(after.zombie_pids.is_empty());
    Ok(())
}

#[test]
fn timeout_reaps_entire_process_group_and_transitions_ledger_to_unknown_timed_out() -> TestResult {
    let harness = TestCatalogHarness::open()?;
    let (snapshot, prepared_ledger) = SnapshotFixtureBuilder::new(1)
        .timeouts(
            dict::DEFAULT_STARTUP_TIMEOUT_MS,
            dict::FAST_EXECUTION_TIMEOUT_MS,
            dict::DEFAULT_TERMINATION_GRACE_MS,
        )
        .commit(&harness)?;
    assert_eq!(prepared_ledger.state(), &LaunchLedgerState::Prepared);

    let supervisor = harness.supervisor(shell_override("sleep 30 & wait"));
    let attempt_key = snapshot.attempt_key();
    let outcome = supervisor.supervise_attempt_blocking(attempt_key)?;

    let SupervisionOutcome::TimedOut {
        ledger,
        termination,
    } = outcome
    else {
        return Err("expected TimedOut supervision outcome".into());
    };
    assert!(matches!(
        termination,
        TerminationDisposition::TerminatedAfterTerm | TerminationDisposition::TerminatedAfterKill
    ));

    let LaunchLedgerState::Unknown {
        ownership: Some(ownership),
        reason,
        ..
    } = ledger.state()
    else {
        return Err("expected LaunchLedgerState::Unknown with recorded ownership".into());
    };
    assert_eq!(*reason, LaunchUnknownReason::ExecutionTimedOut);

    let (_, persisted) = harness.read_attempt(attempt_key)?;
    assert_eq!(persisted, ledger);

    let group_after = inspect_process_group(ownership.pgid())?;
    assert!(group_after.live_pids.is_empty());
    assert!(group_after.zombie_pids.is_empty());
    Ok(())
}

#[test]
fn normal_exit_captures_exit_code_and_rejects_same_attempt_rerun() -> TestResult {
    let harness = TestCatalogHarness::open()?;
    let (snapshot, prepared_ledger) = SnapshotFixtureBuilder::new(1).commit(&harness)?;
    let attempt_key = snapshot.attempt_key();

    let supervisor = harness.supervisor(shell_override("exit 42"));
    let outcome = supervisor.supervise_attempt_blocking(attempt_key)?;

    let SupervisionOutcome::Exited { ledger, exit_code } = outcome else {
        return Err("expected Exited supervision outcome".into());
    };
    assert_eq!(exit_code, 42);
    assert!(ledger.revision() > prepared_ledger.revision());
    assert!(matches!(
        ledger.state(),
        LaunchLedgerState::Exited { exit_code: 42, .. }
    ));

    let rerun_err = supervisor
        .supervise_attempt_blocking(attempt_key)
        .err()
        .ok_or("expected re-running exited attempt to be rejected")?;
    assert!(matches!(
        rerun_err,
        ExecutionSupervisorError::AttemptNotPrepared(_)
    ));

    let (_, persisted_after_rerun) = harness.read_attempt(attempt_key)?;
    assert_eq!(persisted_after_rerun.revision(), ledger.revision());
    Ok(())
}

#[test]
fn boot_recovery_reconciles_prepared_and_spawned_without_signaling_unpinned_orphan() -> TestResult {
    let harness = TestCatalogHarness::open()?;
    let (snapshot_prepared, _) = SnapshotFixtureBuilder::new(1).commit(&harness)?;
    let (snapshot_spawned, ledger_spawned_prep) =
        SnapshotFixtureBuilder::new(2).commit(&harness)?;

    let runner = LocalProcessRunner::new(shell_override("sleep 30 & wait"));
    let mut orphan_handle = runner.spawn_prepared(&snapshot_spawned)?;
    let ownership = orphan_handle.ownership().clone();
    wait_for_group_member_count(ownership.pgid(), 2, Duration::from_secs(2))?;

    let spawned_at_ms = wall_clock_ms()?.max(dict::COMMITTED_AT_MS);
    {
        let mut catalog = harness.catalog.lock().map_err(|_| "catalog poisoned")?;
        catalog.record_execution_spawned(
            snapshot_spawned.attempt_key(),
            ledger_spawned_prep.revision(),
            ownership.clone(),
            spawned_at_ms,
        )?;
    }

    let supervisor = harness.supervisor(LocalProcessRunnerConfig::new());
    let reconciled_at_ms = spawned_at_ms.saturating_add(25);
    let report = supervisor.recover_on_boot_blocking(
        Some(WorkspaceId::from_uuid(dict::WORKSPACE_UUID)),
        reconciled_at_ms,
    )?;
    assert_eq!(report.entries.len(), 2);
    assert!(report.entries.iter().any(|entry| {
        entry.ledger.attempt_key() == snapshot_prepared.attempt_key()
            && entry.cleanup == BootRecoveryCleanup::NotApplicable
    }));
    assert!(report.entries.iter().any(|entry| {
        entry.ledger.attempt_key() == snapshot_spawned.attempt_key()
            && entry.cleanup == BootRecoveryCleanup::UnpinnedOrphanSkipped
    }));

    let (_, attempt1_after) = harness.read_attempt(snapshot_prepared.attempt_key())?;
    assert!(matches!(
        attempt1_after.state(),
        LaunchLedgerState::NotStarted { .. }
    ));

    let (_, attempt2_after) = harness.read_attempt(snapshot_spawned.attempt_key())?;
    assert!(matches!(
        attempt2_after.state(),
        LaunchLedgerState::Unknown {
            ownership: Some(_),
            reason: LaunchUnknownReason::BootRecoveryAfterSpawn,
            ..
        }
    ));

    // Unpinned boot recovery must NOT signal `-pgid` without a pinned `Child`.
    let group_after_recovery = inspect_process_group(ownership.pgid())?;
    assert!(group_after_recovery.live_pids.len() >= 2);

    assert!(matches!(
        supervisor.supervise_attempt_blocking(snapshot_prepared.attempt_key()),
        Err(ExecutionSupervisorError::AttemptNotPrepared(_))
    ));
    assert!(matches!(
        supervisor.supervise_attempt_blocking(snapshot_spawned.attempt_key()),
        Err(ExecutionSupervisorError::AttemptNotPrepared(_))
    ));

    let _ = runner.terminate_owned_group(
        &mut orphan_handle,
        Duration::from_millis(dict::DEFAULT_TERMINATION_GRACE_MS),
    )?;
    let _ = orphan_handle.reap()?;
    let after_cleanup = inspect_process_group(ownership.pgid())?;
    assert!(after_cleanup.live_pids.is_empty());
    assert!(after_cleanup.zombie_pids.is_empty());
    Ok(())
}

#[test]
fn boot_mismatch_and_identity_mismatch_never_signal_unrelated_processes() -> TestResult {
    let harness = TestCatalogHarness::open()?;
    let (snapshot_stale_boot, prepared_stale) = SnapshotFixtureBuilder::new(3).commit(&harness)?;
    let (snapshot_mismatch, prepared_mismatch) = SnapshotFixtureBuilder::new(4).commit(&harness)?;
    let self_pid = std::process::id();
    let stale_boot_ownership = ProcessOwnershipIdentity::new(
        self_pid,
        self_pid,
        "Mon Jan  1 00:00:00 2024".to_owned(),
        dict::PRIOR_OS_BOOT_ID.to_owned(),
    )?;
    let mismatched_same_boot = ProcessOwnershipIdentity::new(
        self_pid,
        self_pid,
        "Thu Jan  1 00:00:01 1970".to_owned(),
        current_system_boot_id()?,
    )?;

    let spawned_at_ms = wall_clock_ms()?.max(dict::COMMITTED_AT_MS);
    {
        let mut catalog = harness.catalog.lock().map_err(|_| "catalog poisoned")?;
        catalog.record_execution_spawned(
            snapshot_stale_boot.attempt_key(),
            prepared_stale.revision(),
            stale_boot_ownership,
            spawned_at_ms,
        )?;
        catalog.record_execution_spawned(
            snapshot_mismatch.attempt_key(),
            prepared_mismatch.revision(),
            mismatched_same_boot,
            spawned_at_ms,
        )?;
    }

    let supervisor = harness.supervisor(LocalProcessRunnerConfig::new());
    let report = supervisor.recover_on_boot_blocking(
        Some(WorkspaceId::from_uuid(dict::WORKSPACE_UUID)),
        spawned_at_ms.saturating_add(10),
    )?;
    assert_eq!(report.entries.len(), 2);
    assert!(report.entries.iter().any(|entry| {
        entry.ledger.attempt_key() == snapshot_stale_boot.attempt_key()
            && entry.cleanup == BootRecoveryCleanup::BootMismatchSkipped
    }));
    assert!(report.entries.iter().any(|entry| {
        entry.ledger.attempt_key() == snapshot_mismatch.attempt_key()
            && entry.cleanup == BootRecoveryCleanup::IdentityMismatchSkipped
    }));

    let runner = LocalProcessRunner::new(shell_override("sleep 30"));
    let mut live_handle = runner.spawn_prepared(&snapshot_stale_boot)?;
    let prior_boot_runner = LocalProcessRunner::new(
        LocalProcessRunnerConfig::new().with_boot_id_override(dict::PRIOR_OS_BOOT_ID),
    );
    assert_eq!(
        prior_boot_runner.terminate_owned_group(&mut live_handle, Duration::from_millis(100))?,
        TerminationDisposition::BootMismatchSkipped
    );
    assert_eq!(
        runner.verify_ownership(live_handle.ownership())?,
        OwnershipVerification::OwnedAlive
    );

    let _ = runner.terminate_owned_group(
        &mut live_handle,
        Duration::from_millis(dict::DEFAULT_TERMINATION_GRACE_MS),
    )?;
    let _ = live_handle.reap()?;
    Ok(())
}

#[test]
fn resolver_rejects_forbidden_ssd_worktree_and_scrubs_sensitive_and_ssd_env() -> TestResult {
    let forbidden_err = validate_worktree_dir(Path::new(dict::FORBIDDEN_WORKTREE_SUBPATH))
        .err()
        .ok_or("expected /Volumes/256.SSD/RustBuild worktree rejection")?;
    assert!(matches!(
        forbidden_err,
        ExecutionSupervisorError::ForbiddenSsdBuildWorktree(_)
    ));

    let snapshot = SnapshotFixtureBuilder::new(4)
        .engine(CliEngine::Agy)
        .allow_workspace_write(false)
        .build()?;
    let resolver = CliExecutableResolver::new().with_agy_binary("/usr/local/bin/agy");
    let forbidden_tmpdir = format!("{FORBIDDEN_SSD_BUILD_PREFIX}/tmp");
    let synthetic_env = [
        ("PATH", "/usr/bin:/bin"),
        ("HOME", "/Users/tester"),
        ("LANG", "en_US.UTF-8"),
        ("TMPDIR", forbidden_tmpdir.as_str()),
        ("OPENAI_API_KEY", "sk-sensitive-secret"),
        ("HETERARCHY_MCP_RAW_SECRET", "raw-token"),
    ];
    let plan = resolver.resolve_with_env(&snapshot, synthetic_env)?;

    let expected_agy = fs::canonicalize("/usr/local/bin/agy")
        .unwrap_or_else(|_| PathBuf::from("/usr/local/bin/agy"));
    assert_eq!(plan.executable(), expected_agy.as_path());
    assert_eq!(
        plan.args(),
        &[
            OsString::from("--print"),
            OsString::from("--disable-slash-commands"),
            OsString::from("--model"),
            OsString::from(dict::MODEL_AGY),
            OsString::from("--effort"),
            OsString::from(dict::REASONING_HIGH),
            OsString::from("--mode"),
            OsString::from("plan"),
            OsString::from("--sandbox"),
        ]
    );
    assert!(!plan.env().contains_key("OPENAI_API_KEY"));
    assert!(!plan.env().contains_key("HETERARCHY_MCP_RAW_SECRET"));
    assert!(!plan.env().contains_key("TMPDIR"));
    assert_eq!(
        plan.env().get("HETERARCHY_ATTEMPT"),
        Some(&OsString::from("4"))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_WORKSPACE_ID"),
        Some(&OsString::from(
            WorkspaceId::from_uuid(dict::WORKSPACE_UUID).to_string()
        ))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_SNAPSHOT_ID"),
        Some(&OsString::from(snapshot.snapshot_id().to_string()))
    );
    Ok(())
}

/// RAII temporary scratch directory rooted under `std::env::temp_dir()`.
struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    fn new(prefix: &str) -> TestResult<Self> {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn new_non_ssd(prefix: &str) -> TestResult<Self> {
        let ambient_tmp = std::env::temp_dir();
        let base = if is_forbidden_ssd_path(&ambient_tmp) {
            PathBuf::from("/var/tmp")
        } else {
            ambient_tmp
        };
        let path = base.join(format!("{prefix}-{}", Uuid::now_v7()));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn worktree_validation_rejects_case_insensitive_and_symlinked_ssd_paths() -> TestResult {
    assert!(is_forbidden_ssd_path(Path::new(FORBIDDEN_SSD_BUILD_PREFIX)));
    assert!(is_forbidden_ssd_path(Path::new(
        "/volumes/256.ssd/rustbuild/case-variant"
    )));
    assert!(is_forbidden_ssd_path(Path::new(
        "/VOLUMES/256.ssd/RUSTBUILD/nested"
    )));
    assert!(!is_forbidden_ssd_path(Path::new(
        "/Volumes/256.SSD/RustBuildSibling"
    )));
    assert!(!is_forbidden_ssd_path(Path::new("/Users/tester/worktree")));

    assert!(matches!(
        validate_worktree_dir(Path::new("/volumes/256.ssd/rustbuild/case-variant")),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(_))
    ));

    let scratch = ScratchDir::new("exec-supervisor-symlink")?;
    let forbidden_target = scratch.path.join("ForbiddenBuildRoot");
    let allowed_parent = scratch.path.join("allowed-parent");
    let real_worktree = allowed_parent.join("real-worktree");
    fs::create_dir_all(&forbidden_target)?;
    fs::create_dir_all(&real_worktree)?;

    let symlink_worktree = allowed_parent.join("worktree-link");
    symlink(&forbidden_target, &symlink_worktree)?;

    let canonical_forbidden = fs::canonicalize(&forbidden_target)?;
    let mixed_case_segments: Vec<String> = canonical_forbidden
        .components()
        .filter_map(|component| match component {
            Component::Normal(seg) => seg.to_str().map(str::to_ascii_lowercase),
            _ => None,
        })
        .collect();
    let segment_refs: Vec<&str> = mixed_case_segments.iter().map(String::as_str).collect();

    validate_worktree_dir_with_forbidden(&real_worktree, &segment_refs)?;
    assert!(matches!(
        validate_worktree_dir_with_forbidden(&symlink_worktree, &segment_refs),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(_))
    ));

    if Path::new(FORBIDDEN_SSD_BUILD_PREFIX).is_dir() {
        let ssd_link = scratch.path.join("ssd-readonly-link");
        symlink(Path::new(FORBIDDEN_SSD_BUILD_PREFIX), &ssd_link)?;
        assert!(matches!(
            validate_worktree_dir(&ssd_link),
            Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(_))
        ));
    }
    Ok(())
}

#[test]
fn absent_leader_in_terminate_owned_group_returns_already_exited_without_signaling() -> TestResult {
    let snapshot = SnapshotFixtureBuilder::new(5).build()?;
    let canary_runner = LocalProcessRunner::new(shell_override("sleep 30"));
    let mut canary = canary_runner.spawn_prepared(&snapshot)?;
    let canary_pgid = canary.ownership().pgid();

    let fast_runner = LocalProcessRunner::new(shell_override("exit 0"));
    let mut exited = fast_runner.spawn_prepared(&snapshot)?;
    let _ = exited.reap()?;
    assert!(!exited.has_pinned_child());

    let runner = LocalProcessRunner::new(LocalProcessRunnerConfig::new());
    assert_eq!(
        runner.terminate_owned_group(&mut exited, Duration::from_millis(100))?,
        TerminationDisposition::AlreadyExited
    );

    let mut unpinned_canary_handle =
        SpawnedProcessHandle::from_parts(canary.ownership().clone(), None, None);
    assert!(!unpinned_canary_handle.has_pinned_child());
    assert_eq!(
        runner.terminate_owned_group(&mut unpinned_canary_handle, Duration::from_millis(100))?,
        TerminationDisposition::AlreadyExited
    );

    let immediate_exit_ownership = ProcessOwnershipIdentity::new(
        canary.ownership().pid(),
        canary_pgid,
        IMMEDIATE_EXIT_START_TIME.to_owned(),
        current_system_boot_id()?,
    )?;
    let mut unpinned_immediate =
        SpawnedProcessHandle::from_parts(immediate_exit_ownership, None, None);
    assert_eq!(
        runner.terminate_owned_group(&mut unpinned_immediate, Duration::from_millis(100))?,
        TerminationDisposition::AlreadyExited
    );

    // Proves `-canary_pgid` was never signaled when `has_pinned_child()` was false.
    assert_eq!(
        runner.verify_ownership(canary.ownership())?,
        OwnershipVerification::OwnedAlive
    );

    let _ = runner.terminate_owned_group(
        &mut canary,
        Duration::from_millis(dict::DEFAULT_TERMINATION_GRACE_MS),
    )?;
    let _ = canary.reap()?;
    Ok(())
}

#[test]
fn natural_leader_exit_drains_background_child_while_zombie_pinned_and_records_exit_code()
-> TestResult {
    let harness = TestCatalogHarness::open()?;
    let (snapshot, _) = SnapshotFixtureBuilder::new(6).commit(&harness)?;
    let attempt_key = snapshot.attempt_key();

    let supervisor = harness.supervisor(shell_override("sleep 30 & sleep 0.05; exit 7"));
    let started = Instant::now();
    let outcome = supervisor.supervise_attempt_blocking(attempt_key)?;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(4),
        "expected natural exit + descendant drain well before 5s timeout, took {elapsed:?}"
    );
    let SupervisionOutcome::Exited { ledger, exit_code } = outcome else {
        return Err("expected Exited supervision outcome".into());
    };
    assert_eq!(exit_code, 7);

    let LaunchLedgerState::Exited {
        ownership,
        exit_code: recorded_code,
        ..
    } = ledger.state()
    else {
        return Err("expected LaunchLedgerState::Exited".into());
    };
    assert_eq!(*recorded_code, 7);

    let (_, persisted) = harness.read_attempt(attempt_key)?;
    assert_eq!(persisted, ledger);

    let group_after = inspect_process_group(ownership.pgid())?;
    assert!(group_after.live_pids.is_empty());
    assert!(group_after.zombie_pids.is_empty());
    Ok(())
}

#[test]
fn regression_forbidden_path_search() -> TestResult {
    let snapshot = SnapshotFixtureBuilder::new(7).build()?;
    let resolver =
        CliExecutableResolver::new().with_command_override(CommandOverride::new("codex", ["exec"]));

    assert!(matches!(
        resolver.resolve_with_env(&snapshot, [("PATH", FORBIDDEN_SSD_BUILD_PREFIX)]),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(_))
    ));
    assert!(matches!(
        resolver.resolve_with_env(
            &snapshot,
            [("PATH", "/usr/bin:/volumes/256.ssd/rustbuild/bin")]
        ),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(_))
    ));

    let path_bin_dir = ScratchDir::new_non_ssd("exec-supervisor-path-bin")?;
    let symlinked_codex = path_bin_dir.path.join("codex");
    symlink(
        Path::new("/Volumes/256.SSD/RustBuild/bin/codex"),
        &symlinked_codex,
    )?;
    let path_override = path_bin_dir.path.as_os_str();
    assert!(matches!(
        resolver.resolve_with_env(&snapshot, [("PATH", path_override)]),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(_))
    ));
    Ok(())
}

#[test]
fn regression_relative_executable_symlink() -> TestResult {
    let worktree = ScratchDir::new_non_ssd("exec-supervisor-rel-worktree")?;
    let dangling_ssd_target = Path::new("/Volumes/256.SSD/RustBuild/bin/codex");
    let link_path = worktree.path.join("audit-executable-link");
    symlink(dangling_ssd_target, &link_path)?;

    let snapshot = SnapshotFixtureBuilder::new(8)
        .worktree_path(worktree.path.clone())
        .build()?;
    let resolver = CliExecutableResolver::new().with_command_override(CommandOverride::new(
        "./audit-executable-link",
        ["--version"],
    ));
    assert!(matches!(
        resolver.resolve_with_env(&snapshot, [("PATH", "/usr/bin:/bin")]),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(_))
    ));

    let nested_bin = worktree.path.join("bin");
    fs::create_dir_all(&nested_bin)?;
    symlink(
        Path::new("/Volumes/256.SSD/RustBuild/bin/agy"),
        nested_bin.join("nested-link"),
    )?;
    let nested_resolver = CliExecutableResolver::new()
        .with_command_override(CommandOverride::new("bin/nested-link", ["--version"]));
    assert!(matches!(
        nested_resolver.resolve_with_env(&snapshot, [("PATH", "/usr/bin:/bin")]),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(_))
    ));
    Ok(())
}

#[test]
fn an_already_waited_child_must_not_count_as_a_kernel_pin() -> TestResult {
    use std::process::Command;

    let mut child = Command::new("/bin/sh").args(["-c", "exit 0"]).spawn()?;
    let pid = child.id();
    let _status = child.wait()?;
    let ownership = ProcessOwnershipIdentity::new(
        pid,
        pid,
        "Mon Nov 16 00:00:00 2026".to_owned(),
        "test-boot".to_owned(),
    )?;
    let handle = SpawnedProcessHandle::from_parts(ownership, Some(child), None);
    assert!(
        !handle.has_pinned_child(),
        "A retained Child that has already been waited does not pin its PID"
    );
    Ok(())
}

#[test]
fn mismatched_live_child_id_must_not_count_as_a_kernel_pin() -> TestResult {
    use std::process::Command;

    let snapshot = SnapshotFixtureBuilder::new(9).build()?;
    let runner = LocalProcessRunner::new(shell_override("sleep 30"));
    let mut target = runner.spawn_prepared(&snapshot)?;

    let unrelated_child = Command::new("/bin/sh").args(["-c", "sleep 0.2"]).spawn()?;
    let mut forged =
        SpawnedProcessHandle::from_parts(target.ownership().clone(), Some(unrelated_child), None);
    assert!(!forged.has_pinned_child());
    assert_eq!(forged.pinned_child_id(), None);
    assert_eq!(
        runner.terminate_owned_group(&mut forged, Duration::from_millis(100))?,
        TerminationDisposition::AlreadyExited
    );
    assert_eq!(
        runner.verify_ownership(target.ownership())?,
        OwnershipVerification::OwnedAlive
    );

    let _ = runner.terminate_owned_group(
        &mut target,
        Duration::from_millis(dict::DEFAULT_TERMINATION_GRACE_MS),
    )?;
    let _ = target.reap()?;
    Ok(())
}

#[test]
fn mutable_worktree_executables_and_empty_path_segments_are_rejected_and_spawn_revalidates()
-> TestResult {
    let worktree = ScratchDir::new_non_ssd("exec-supervisor-mutable-wt")?;
    let local_script = worktree.path.join("local-tool.sh");
    fs::write(&local_script, "#!/bin/sh\nexit 0\n")?;

    let snapshot = SnapshotFixtureBuilder::new(10)
        .worktree_path(worktree.path.clone())
        .build()?;
    let bare_resolver =
        CliExecutableResolver::new().with_command_override(CommandOverride::new("sh", ["-c", ":"]));

    for bad_path in ["", "/usr/bin::/bin", ":/usr/bin", "/usr/bin:", "."] {
        assert!(matches!(
            bare_resolver.resolve_with_env(&snapshot, [("PATH", bad_path)]),
            Err(ExecutionSupervisorError::InvalidExecutablePath(_))
        ));
    }
    assert!(matches!(
        bare_resolver.resolve_with_env(&snapshot, [("PATH", worktree.path.as_os_str())]),
        Err(ExecutionSupervisorError::InvalidExecutablePath(_))
    ));

    for worktree_exec in [PathBuf::from("./local-tool.sh"), local_script] {
        let exec_resolver = CliExecutableResolver::new().with_command_override(
            CommandOverride::new(worktree_exec, std::iter::empty::<&str>()),
        );
        assert!(matches!(
            exec_resolver.resolve_with_env(&snapshot, [("PATH", "/usr/bin:/bin")]),
            Err(ExecutionSupervisorError::InvalidExecutablePath(_))
        ));
    }

    let trusted_bin = ScratchDir::new_non_ssd("exec-supervisor-trusted-bin")?;
    let sh_alias = trusted_bin.path.join("sh-link");
    symlink(Path::new("/bin/sh"), &sh_alias)?;
    let alias_resolver = CliExecutableResolver::new()
        .with_command_override(CommandOverride::new(&sh_alias, ["-c", ":"]));
    let plan = alias_resolver.resolve_with_env(&snapshot, [("PATH", "/usr/bin:/bin")])?;
    assert_eq!(plan.executable(), fs::canonicalize("/bin/sh")?.as_path());
    assert_eq!(
        plan.revalidate_before_spawn()?,
        fs::canonicalize("/bin/sh")?
    );

    // Replacing `sh_alias` after resolution cannot redirect `plan.executable()`,
    // which already pinned the canonical `/bin/sh` path.
    fs::remove_file(&sh_alias)?;
    symlink(Path::new("/Volumes/256.SSD/RustBuild/bin/codex"), &sh_alias)?;
    assert_eq!(
        plan.revalidate_before_spawn()?,
        fs::canonicalize("/bin/sh")?
    );

    // A bare command whose `PATH` directory later acquires a forbidden symlink
    // is caught by `revalidate_before_spawn()`.
    let deferred_resolver = CliExecutableResolver::new()
        .with_command_override(CommandOverride::new("deferred-cli", ["--version"]));
    let deferred_bin = ScratchDir::new_non_ssd("exec-supervisor-deferred-bin")?;
    let deferred_plan =
        deferred_resolver.resolve_with_env(&snapshot, [("PATH", deferred_bin.path.as_os_str())])?;
    symlink(
        Path::new("/Volumes/256.SSD/RustBuild/bin/codex"),
        deferred_bin.path.join("deferred-cli"),
    )?;
    assert!(matches!(
        deferred_plan.revalidate_before_spawn(),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(_))
    ));

    // Simulate post-resolution worktree replacement with a symlink into SSD root.
    fs::remove_dir_all(&worktree.path)?;
    symlink(
        Path::new("/Volumes/256.SSD/RustBuild/forbidden-worktree"),
        &worktree.path,
    )?;
    assert!(matches!(
        plan.revalidate_before_spawn(),
        Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(_))
    ));
    let _ = fs::remove_file(&worktree.path);
    Ok(())
}

#[test]
fn post_spawn_probe_failure_guard_terminates_and_reaps_isolated_process_group() -> TestResult {
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};

    let mut command = Command::new(dict::SHELL_BIN);
    command
        .args(["-c", "sleep 30 & wait"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);

    let child = command.spawn()?;
    let leader_pid = child.id();
    let guard = UnconfirmedSpawnGuard::new(child, leader_pid);
    wait_for_group_member_count(leader_pid, 2, Duration::from_secs(2))?;

    // Dropping `guard` simulates an error returned by `inspect_process_leader(pid)?`
    // right after `command.spawn()?`.
    drop(guard);

    let after = inspect_process_group(leader_pid)?;
    assert!(after.live_pids.is_empty());
    assert!(after.zombie_pids.is_empty());
    Ok(())
}

#[test]
fn codex_emits_canonical_exec_flags_and_defends_prompt_with_double_dash() -> TestResult {
    let codex_snapshot = SnapshotFixtureBuilder::new(11)
        .engine(CliEngine::Codex)
        .permissions(true, true, false)
        .build()?;
    let codex_resolver = CliExecutableResolver::new().with_codex_binary("/usr/local/bin/codex");
    let malicious_prompt = "-c model_reasoning_effort=\"low\" --dangerously-skip-permissions";
    let swarm_run_id = Uuid::from_u128(0xFEED_0001);
    let swarm_member_id = Uuid::from_u128(0xFEED_0002);
    let backend_addr: std::net::SocketAddr = "127.0.0.1:8798".parse()?;
    let context = CliExecutionContext::new()
        .with_prompt(malicious_prompt)?
        .with_workspace_id(WorkspaceId::from_uuid(dict::WORKSPACE_UUID))?
        .with_swarm_run_id(swarm_run_id)?
        .with_swarm_member_id(swarm_member_id)?
        .with_orchestration_backend_addr(backend_addr)?;

    let codex_plan = codex_resolver.resolve_with_env_and_context(
        &codex_snapshot,
        [
            ("PATH", "/usr/bin:/bin"),
            ("HETERARCHY_SWARM_RUN_ID", "spoofed-run-id"),
            ("ANTHROPIC_API_KEY", "sk-secret"),
        ],
        &context,
    )?;
    assert_eq!(
        codex_plan.args(),
        &[
            OsString::from("exec"),
            OsString::from("-m"),
            OsString::from(dict::MODEL_CODEX),
            OsString::from("-c"),
            OsString::from("model_reasoning_effort=\"high\""),
            OsString::from("-s"),
            OsString::from("workspace-write"),
            OsString::from("-c"),
            OsString::from("sandbox_workspace_write.network_access=false"),
            OsString::from("--"),
            OsString::from(malicious_prompt),
        ]
    );
    assert!(!codex_plan.env().contains_key("ANTHROPIC_API_KEY"));
    assert_eq!(
        codex_plan.env().get("HETERARCHY_SNAPSHOT_ID"),
        Some(&OsString::from(codex_snapshot.snapshot_id().to_string()))
    );
    assert_eq!(
        codex_plan.env().get("HETERARCHY_SWARM_RUN_ID"),
        Some(&OsString::from(swarm_run_id.to_string()))
    );
    assert_eq!(
        codex_plan.env().get("HETERARCHY_SWARM_MEMBER_ID"),
        Some(&OsString::from(swarm_member_id.to_string()))
    );
    assert_eq!(
        codex_plan
            .env()
            .get("HETERARCHY_ORCHESTRATION_BACKEND_ADDR"),
        Some(&OsString::from("127.0.0.1:8798"))
    );

    let codex_readonly_net = SnapshotFixtureBuilder::new(12)
        .engine(CliEngine::Codex)
        .reasoning(None)
        .permissions(false, false, true)
        .build()?;
    let codex_net_plan = codex_resolver.resolve_with_env_and_context(
        &codex_readonly_net,
        [("PATH", "/usr/bin:/bin")],
        &context,
    )?;
    assert_eq!(
        codex_net_plan.args(),
        &[
            OsString::from("exec"),
            OsString::from("-m"),
            OsString::from(dict::MODEL_CODEX),
            OsString::from("-s"),
            OsString::from("read-only"),
            OsString::from("--"),
            OsString::from(malicious_prompt),
        ]
    );
    Ok(())
}

#[test]
fn agy_emits_canonical_non_interactive_flags_and_defends_prompt_with_double_dash() -> TestResult {
    let agy_resolver = CliExecutableResolver::new().with_agy_binary("/usr/local/bin/agy");
    let malicious_prompt = "--dangerously-skip-permissions -c foo=bar";
    let context = CliExecutionContext::new().with_prompt(malicious_prompt)?;
    let agy_full_open = SnapshotFixtureBuilder::new(13)
        .engine(CliEngine::Agy)
        .reasoning(None)
        .permissions(true, true, true)
        .build()?;
    let agy_open_plan = agy_resolver.resolve_with_env_and_context(
        &agy_full_open,
        [("PATH", "/usr/bin:/bin")],
        &context,
    )?;
    assert_eq!(
        agy_open_plan.args(),
        &[
            OsString::from("--print"),
            OsString::from("--disable-slash-commands"),
            OsString::from("--model"),
            OsString::from(dict::MODEL_AGY),
            OsString::from("--mode"),
            OsString::from("accept-edits"),
            OsString::from("--"),
            OsString::from(malicious_prompt),
        ]
    );

    let agy_no_shell = SnapshotFixtureBuilder::new(14)
        .engine(CliEngine::Agy)
        .permissions(true, false, true)
        .build()?;
    let agy_sandbox_plan = agy_resolver.resolve_with_env_and_context(
        &agy_no_shell,
        [("PATH", "/usr/bin:/bin")],
        &CliExecutionContext::default(),
    )?;
    assert_eq!(
        agy_sandbox_plan.args(),
        &[
            OsString::from("--print"),
            OsString::from("--disable-slash-commands"),
            OsString::from("--model"),
            OsString::from(dict::MODEL_AGY),
            OsString::from("--effort"),
            OsString::from(dict::REASONING_HIGH),
            OsString::from("--mode"),
            OsString::from("accept-edits"),
            OsString::from("--sandbox"),
        ]
    );
    Ok(())
}

#[test]
fn resolver_and_context_reject_toml_injection_bad_tokens_and_invalid_context() -> TestResult {
    let codex_resolver = CliExecutableResolver::new().with_codex_binary("/usr/local/bin/codex");
    for bad_reasoning in ["high\"\n[features]\n", "high low", "-high", "high=1"] {
        let bad_snap = SnapshotFixtureBuilder::new(15)
            .engine(CliEngine::Codex)
            .reasoning(Some(bad_reasoning))
            .build()?;
        assert!(matches!(
            codex_resolver.resolve_with_env(&bad_snap, [("PATH", "/usr/bin:/bin")]),
            Err(ExecutionSupervisorError::InvalidCliArgument { field: "reasoning" })
        ));
    }

    let bad_model_snap = SnapshotFixtureBuilder::new(16)
        .engine(CliEngine::Codex)
        .model_override("-danger")
        .build()?;
    assert!(matches!(
        codex_resolver.resolve_with_env(&bad_model_snap, [("PATH", "/usr/bin:/bin")]),
        Err(ExecutionSupervisorError::InvalidCliArgument { field: "model" })
    ));

    assert!(matches!(
        CliExecutionContext::new().with_prompt("has\0nul"),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    assert!(matches!(
        CliExecutionContext::new().with_prompt("   "),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    let oversized = "a".repeat(MAX_CLI_PROMPT_BYTES + 1);
    assert!(matches!(
        CliExecutionContext::new().with_prompt(oversized),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    assert!(matches!(
        CliExecutionContext::new().with_orchestration_backend_addr("10.0.0.1:8798".parse()?),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    let codex_snapshot = SnapshotFixtureBuilder::new(17)
        .engine(CliEngine::Codex)
        .build()?;
    let wrong_ws_ctx = CliExecutionContext::new()
        .with_workspace_id(WorkspaceId::from_uuid(Uuid::from_u128(0x9999_0001)))?;
    assert!(matches!(
        codex_resolver.resolve_with_env_and_context(
            &codex_snapshot,
            [("PATH", "/usr/bin:/bin")],
            &wrong_ws_ctx,
        ),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    Ok(())
}
