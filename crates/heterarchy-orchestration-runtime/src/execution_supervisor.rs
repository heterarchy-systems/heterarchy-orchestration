//! Process-group isolated `CLI` execution supervision and boot recovery over
//! durable `OperationsCatalog` launch-ledger records.
//!
//! # Ownership & `PID`-Reuse Safety Invariants
//! 1. **Isolated Process Groups**: Every spawned local execution enters a fresh
//!    process group (`pgid == pid`) via `CommandExt::process_group(0)` before
//!    `exec`, with scrubbed host environment variables and `/dev/null` standard
//!    streams so child pipes can never block indefinitely.
//! 2. **Live Zombie-Pinning**: During live supervision, [`SpawnedProcessHandle`]
//!    retains [`std::process::Child`] un-waited while timeouts or group cleanup
//!    run. Even if the group leader exits early into zombie state `Z`, the OS
//!    kernel cannot recycle `pid` or `pgid` until [`SpawnedProcessHandle::reap`]
//!    calls `wait()`.
//! 3. **Post-Restart Boot Reconciliation**: When a daemon restarts on the same
//!    or a newer OS boot, the original [`std::process::Child`] handle is gone,
//!    so the leader is no longer zombie-pinned by this process and could be
//!    reaped asynchronously by `PID` 1 between `/bin/ps` and `kill(-pgid)`.
//!    `recover_on_boot` therefore transitions `Prepared -> NotStarted` and
//!    `Spawned -> Unknown { BootRecoveryAfterSpawn }`, never auto-retries any
//!    `attempt`, and classifies orphan state via read-only `verify_ownership`
//!    without sending unpinned `-pgid` signals.
//! 4. **Lock Discipline**: `Mutex<OperationsCatalog>` is held only for bounded,
//!    in-memory/`SQLite` state transitions and is never held across process
//!    waits, sleeps, OS probes, signals, or `.await` points.

mod invocation;
mod local_runner;

use std::os::unix::process::ExitStatusExt as _;
use std::path::PathBuf;
use std::process::{Child, ExitStatus};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::{
    AssignmentExecutionSnapshot, ExecutionAttemptKey, LaunchLedgerRecord, LaunchLedgerState,
    LaunchUnknownReason, OperationsCatalog, OperationsError, ProcessOwnershipIdentity,
    SnapshotValidationError,
};
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::{CatalogHandle, OrchestrationRuntime};

pub use invocation::{
    ALLOWLISTED_HOST_ENV_KEYS, CliExecutableResolver, CliExecutionContext, CliInvocationPlan,
    CommandOverride, FORBIDDEN_SSD_BUILD_PREFIX, MAX_CLI_PROMPT_BYTES, validate_worktree_dir,
};
pub use local_runner::{LocalProcessRunner, LocalProcessRunnerConfig, current_system_boot_id};

const DEFAULT_MAX_CONCURRENT_SUPERVISIONS: usize = 8;
const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Diagnostic payload when `supervise_attempt` refuses to launch an attempt
/// whose durable ledger state is not [`LaunchLedgerState::Prepared`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptNotPreparedError {
    pub attempt_key: ExecutionAttemptKey,
    pub state: LaunchLedgerState,
}

/// Typed failures produced by `CLI` invocation resolution, local process-group
/// supervision, and boot-time launch-ledger reconciliation.
#[derive(Debug, Error)]
pub enum ExecutionSupervisorError {
    #[error("operations catalog mutex is poisoned")]
    CatalogPoisoned,
    #[error("supervision concurrency semaphore is closed")]
    ConcurrencyBridgeClosed,
    #[error("blocking supervision task failed to join")]
    BlockingTaskJoinFailed,
    #[error("max_concurrent_supervisions must be at least 1")]
    InvalidConcurrencyLimit,
    #[error("system wall clock precedes UNIX_EPOCH")]
    ClockBeforeUnixEpoch,
    #[error("execution attempt was not found in operations catalog")]
    AttemptNotFound(Box<ExecutionAttemptKey>),
    #[error("execution attempt is not in Prepared state and cannot be launched")]
    AttemptNotPrepared(Box<AttemptNotPreparedError>),
    #[error("worktree path must be absolute: {0}")]
    InvalidWorktreePath(PathBuf),
    #[error("worktree directory does not exist on disk: {0}")]
    MissingWorktreeDirectory(PathBuf),
    #[error("worktree path inside forbidden SSD build root is prohibited: {0}")]
    ForbiddenSsdBuildWorktree(PathBuf),
    #[error("CLI executable path is empty or invalid: {0}")]
    InvalidExecutablePath(PathBuf),
    #[error("CLI executable inside forbidden SSD build root is prohibited: {0}")]
    ForbiddenSsdBuildExecutable(PathBuf),
    #[error("CLI argument {field} violates canonical safety rules")]
    InvalidCliArgument { field: &'static str },
    #[error("CLI execution context is invalid: {0}")]
    InvalidExecutionContext(&'static str),
    #[error("OS boot identifier is unavailable: {0}")]
    BootIdUnavailable(String),
    #[error(
        "spawned process {pid} did not isolate into its own process group (pgid={observed_pgid})"
    )]
    ProcessGroupNotIsolated { pid: u32, observed_pgid: u32 },
    #[error("process table probe failed: {0}")]
    ProcessProbeFailed(String),
    #[error("malformed /bin/ps output row: {0}")]
    ProcessProbeParse(String),
    #[error(
        "process ownership mismatch for pid {pid}: expected pgid={expected_pgid} start_time={expected_start_time:?}, observed pgid={observed_pgid} start_time={observed_start_time:?}"
    )]
    OwnershipMismatch {
        pid: u32,
        expected_pgid: u32,
        observed_pgid: u32,
        expected_start_time: String,
        observed_start_time: String,
    },
    #[error("failed to signal process group -{pgid} with {signal}: {detail}")]
    KillCommandFailed {
        pgid: u32,
        signal: String,
        detail: String,
    },
    #[error("process group -{pgid} still has {remaining_live} live member(s) after SIGKILL")]
    GroupTerminationUnconfirmed { pgid: u32, remaining_live: usize },
    #[error("invalid process ownership identity: {0}")]
    InvalidOwnershipIdentity(#[source] Box<SnapshotValidationError>),
    #[error("operations catalog error: {0}")]
    Operations(#[source] Box<OperationsError>),
    #[error("local process I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl ExecutionSupervisorError {
    #[must_use]
    pub fn operations(error: OperationsError) -> Self {
        Self::Operations(Box::new(error))
    }

    #[must_use]
    pub fn invalid_ownership_identity(error: SnapshotValidationError) -> Self {
        Self::InvalidOwnershipIdentity(Box::new(error))
    }

    #[must_use]
    pub fn attempt_not_found(attempt_key: ExecutionAttemptKey) -> Self {
        Self::AttemptNotFound(Box::new(attempt_key))
    }

    #[must_use]
    pub fn attempt_not_prepared(
        attempt_key: ExecutionAttemptKey,
        state: LaunchLedgerState,
    ) -> Self {
        Self::AttemptNotPrepared(Box::new(AttemptNotPreparedError { attempt_key, state }))
    }
}

impl From<OperationsError> for ExecutionSupervisorError {
    fn from(error: OperationsError) -> Self {
        Self::operations(error)
    }
}

/// Result of verifying a recorded [`ProcessOwnershipIdentity`] against the
/// current OS kernel boot ID and process table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipVerification {
    OwnedAlive,
    ProcessAbsent,
    IdentityMismatch,
}

/// Outcome of an ownership-verified process-group termination sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationDisposition {
    AlreadyExited,
    TerminatedAfterTerm,
    TerminatedAfterKill,
    BootMismatchSkipped,
}

/// Live handle to a spawned `CLI` process group.
///
/// Retaining [`std::process::Child`] un-waited pins the leader `PID` in the
/// kernel process table until [`Self::reap`] is called, preventing `PID` reuse
/// while group signals (`-pgid`) are delivered.
#[derive(Debug)]
pub struct SpawnedProcessHandle {
    ownership: ProcessOwnershipIdentity,
    child: Option<Child>,
    trusted_pin: bool,
    pre_reaped_status: Option<ExitStatus>,
}

impl SpawnedProcessHandle {
    /// Construct an unpinned handle from external parts while retaining reaping
    /// responsibility for any supplied [`Child`].
    ///
    /// External callers cannot prove that a supplied [`Child`] has not already
    /// been waited (which would allow `PID` reuse to spoof a kernel pin), so
    /// `trusted_pin` is set to `false` (never granting `-pgid` group-signaling
    /// authority), while `Child` is retained for [`Self::try_wait`],
    /// [`Self::reap`], and `Drop` reaping so consumed children never leak as
    /// zombies.
    #[must_use]
    pub fn from_parts(
        ownership: ProcessOwnershipIdentity,
        child: Option<Child>,
        pre_reaped_status: Option<ExitStatus>,
    ) -> Self {
        let mut child = child;
        let mut pre_reaped_status = pre_reaped_status;
        if pre_reaped_status.is_none()
            && let Some(candidate) = child.as_mut()
            && let Ok(Some(status)) = candidate.try_wait()
        {
            pre_reaped_status = Some(status);
            child = None;
        }
        Self {
            ownership,
            child,
            trusted_pin: false,
            pre_reaped_status,
        }
    }

    /// Construct a handle without a live [`Child`] descriptor (never grants
    /// `-pgid` group-signaling authority).
    #[must_use]
    pub const fn unpinned(
        ownership: ProcessOwnershipIdentity,
        pre_reaped_status: Option<ExitStatus>,
    ) -> Self {
        Self {
            ownership,
            child: None,
            trusted_pin: false,
            pre_reaped_status,
        }
    }

    fn from_verified_spawn(ownership: ProcessOwnershipIdentity, child: Child) -> Self {
        let matches_pid = child.id() == ownership.pid();
        Self {
            ownership,
            child: Some(child),
            trusted_pin: matches_pid,
            pre_reaped_status: None,
        }
    }

    #[must_use]
    pub const fn ownership(&self) -> &ProcessOwnershipIdentity {
        &self.ownership
    }

    /// Return the OS `PID` of the retained trusted un-reaped [`Child`], if any.
    #[must_use]
    pub fn pinned_child_id(&self) -> Option<u32> {
        if !self.trusted_pin {
            return None;
        }
        self.child.as_ref().map(Child::id)
    }

    /// Returns `true` while a trusted, un-waited [`Child`] handle matching
    /// `self.ownership().pid()` is retained, pinning `(pid, pgid)` in the
    /// kernel process table (even in zombie state `'Z'`).
    #[must_use]
    pub fn has_pinned_child(&self) -> bool {
        self.trusted_pin && self.pinned_child_id() == Some(self.ownership.pid())
    }

    /// Alias for [`Self::has_pinned_child`].
    #[must_use]
    pub fn has_unreaped_child(&self) -> bool {
        self.has_pinned_child()
    }

    /// Non-blocking poll for leader exit status that reaps `Child` when exited.
    ///
    /// Live supervision uses [`ExecutionRunner::poll_leader_exited`] first so
    /// [`LocalProcessRunner`] detects zombie state `'Z'` without reaping `Child`
    /// until [`ExecutionRunner::terminate_owned_group`] finishes draining any
    /// surviving descendants.
    ///
    /// # Errors
    /// Returns [`ExecutionSupervisorError::Io`] if `try_wait` fails.
    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, ExecutionSupervisorError> {
        if let Some(status) = self.pre_reaped_status {
            return Ok(Some(status));
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        if let Some(status) = child.try_wait()? {
            self.pre_reaped_status = Some(status);
            self.child = None;
            self.trusted_pin = false;
            return Ok(Some(status));
        }
        Ok(None)
    }

    /// Reap the held leader `Child` after group termination has completed.
    ///
    /// # Errors
    /// Returns [`ExecutionSupervisorError::Io`] if `Child::wait` fails.
    pub fn reap(&mut self) -> Result<Option<ExitStatus>, ExecutionSupervisorError> {
        if let Some(status) = self.pre_reaped_status {
            return Ok(Some(status));
        }
        self.trusted_pin = false;
        let Some(mut child) = self.child.take() else {
            return Ok(None);
        };
        let status = child.wait()?;
        self.pre_reaped_status = Some(status);
        Ok(Some(status))
    }
}

impl Drop for SpawnedProcessHandle {
    fn drop(&mut self) {
        self.trusted_pin = false;
        if let Some(mut child) = self.child.take()
            && let Ok(None) = child.try_wait()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Execution runner boundary supporting isolated group spawn, ownership
/// verification, zombie-pinned exit polling, and `-pgid` termination.
pub trait ExecutionRunner: Send + Sync {
    /// Observe the current host OS boot identifier.
    ///
    /// # Errors
    /// Returns an error if the host boot identifier cannot be determined.
    fn current_boot_id(&self) -> Result<String, ExecutionSupervisorError>;

    /// Spawn a prepared [`AssignmentExecutionSnapshot`] into a fresh process
    /// group (`pgid == pid`).
    ///
    /// # Errors
    /// Rejects invalid worktrees/executables or OS process spawn/probe errors.
    fn spawn_prepared(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
    ) -> Result<SpawnedProcessHandle, ExecutionSupervisorError>;

    /// Spawn a prepared [`AssignmentExecutionSnapshot`] with an explicit
    /// non-secret [`CliExecutionContext`] into a fresh process group
    /// (`pgid == pid`).
    ///
    /// Runners that do not inspect [`CliExecutionContext`] fall back to
    /// [`Self::spawn_prepared`].
    ///
    /// # Errors
    /// Rejects invalid contexts, worktrees/executables, or OS process spawn/probe
    /// errors.
    fn spawn_prepared_with_context(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
        _context: &CliExecutionContext,
    ) -> Result<SpawnedProcessHandle, ExecutionSupervisorError> {
        self.spawn_prepared(snapshot)
    }

    /// Poll whether the group leader has exited **without** reaping an
    /// un-reaped OS [`Child`] before group cleanup runs.
    ///
    /// [`LocalProcessRunner`] overrides this to inspect `/bin/ps` for zombie
    /// state `'Z'` while keeping `Child` un-waited so `(pid, pgid)` stays
    /// pinned in the kernel process table until [`Self::terminate_owned_group`]
    /// drains surviving non-zombie descendants. Custom/in-memory test runners
    /// without an OS `Child` fall back to `handle.try_wait()`.
    ///
    /// # Errors
    /// Returns [`ExecutionSupervisorError::OwnershipMismatch`] if the observed
    /// leader no longer matches `handle.ownership()`, or probe/I/O errors.
    fn poll_leader_exited(
        &self,
        handle: &mut SpawnedProcessHandle,
    ) -> Result<bool, ExecutionSupervisorError> {
        Ok(handle.try_wait()?.is_some())
    }

    /// Verify whether the recorded leader still matches `(pid, pgid, start_time, boot_id)`.
    ///
    /// # Errors
    /// Returns an error if OS boot ID or process table inspection fails.
    fn verify_ownership(
        &self,
        ownership: &ProcessOwnershipIdentity,
    ) -> Result<OwnershipVerification, ExecutionSupervisorError>;

    /// Terminate an owned process group (`SIGTERM` -> `grace_period` -> `SIGKILL`)
    /// while `handle` retains a pinned [`Child`] and positively matches `boot_id`
    /// and leader `(pid, pgid, start_time)`.
    ///
    /// # Errors
    /// Returns [`ExecutionSupervisorError::OwnershipMismatch`] if leader
    /// identity differs, or signaling/probe errors.
    fn terminate_owned_group(
        &self,
        handle: &mut SpawnedProcessHandle,
        grace_period: Duration,
    ) -> Result<TerminationDisposition, ExecutionSupervisorError>;
}

/// Terminal supervision result recorded in `OperationsCatalog` for one attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisionOutcome {
    Exited {
        ledger: LaunchLedgerRecord,
        exit_code: i32,
    },
    TimedOut {
        ledger: LaunchLedgerRecord,
        termination: TerminationDisposition,
    },
    Unknown {
        ledger: LaunchLedgerRecord,
        reason: LaunchUnknownReason,
    },
}

impl SupervisionOutcome {
    #[must_use]
    pub const fn ledger(&self) -> &LaunchLedgerRecord {
        match self {
            Self::Exited { ledger, .. }
            | Self::TimedOut { ledger, .. }
            | Self::Unknown { ledger, .. } => ledger,
        }
    }
}

/// Per-record read-only classification during `recover_on_boot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootRecoveryCleanup {
    NotApplicable,
    BootMismatchSkipped,
    LeaderAbsentSkipped,
    IdentityMismatchSkipped,
    UnpinnedOrphanSkipped,
}

/// Reconciled ledger record paired with any same-boot orphan-cleanup action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootRecoveryEntry {
    pub ledger: LaunchLedgerRecord,
    pub cleanup: BootRecoveryCleanup,
}

/// Structured summary returned by [`ExecutionSupervisor::recover_on_boot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootRecoveryReport {
    pub current_boot_id: String,
    pub entries: Vec<BootRecoveryEntry>,
}

/// Coordinates durable `OperationsCatalog` launch-ledger transitions with an
/// [`ExecutionRunner`] without holding catalog locks across waits or `.await`.
pub struct ExecutionSupervisor<R = LocalProcessRunner> {
    operations: Arc<Mutex<OperationsCatalog>>,
    owner_pin: Option<Arc<Mutex<rusqlite::Connection>>>,
    runner: Arc<R>,
    supervision_slots: Arc<Semaphore>,
}

impl<R> Clone for ExecutionSupervisor<R> {
    fn clone(&self) -> Self {
        Self {
            operations: Arc::clone(&self.operations),
            owner_pin: self.owner_pin.as_ref().map(Arc::clone),
            runner: Arc::clone(&self.runner),
            supervision_slots: Arc::clone(&self.supervision_slots),
        }
    }
}

impl<R> std::fmt::Debug for ExecutionSupervisor<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionSupervisor")
            .field(
                "available_slots",
                &self.supervision_slots.available_permits(),
            )
            .finish_non_exhaustive()
    }
}

impl<R: ExecutionRunner + 'static> ExecutionSupervisor<R> {
    /// Construct a supervisor over a shared `OperationsCatalog` mutex and runner.
    #[must_use]
    pub fn new(operations: Arc<Mutex<OperationsCatalog>>, runner: R) -> Self {
        Self {
            operations,
            owner_pin: None,
            runner: Arc::new(runner),
            supervision_slots: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_SUPERVISIONS)),
        }
    }

    /// Construct a supervisor retaining the `OrchestrationRuntime` data-root
    /// owner lease pin.
    #[must_use]
    pub fn from_catalog_handle(handle: &CatalogHandle<OperationsCatalog>, runner: R) -> Self {
        Self {
            operations: Arc::clone(&handle.catalog),
            owner_pin: Some(Arc::clone(&handle.owner_pin)),
            runner: Arc::new(runner),
            supervision_slots: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_SUPERVISIONS)),
        }
    }

    /// Construct a supervisor directly from an [`OrchestrationRuntime`].
    #[must_use]
    pub fn from_runtime(runtime: &OrchestrationRuntime, runner: R) -> Self {
        let handle = runtime.operations_catalog();
        Self::from_catalog_handle(&handle, runner)
    }

    /// Configure the bounded async supervision concurrency limit.
    ///
    /// # Errors
    /// Rejects `0`.
    pub fn with_max_concurrent_supervisions(
        mut self,
        max_concurrent: usize,
    ) -> Result<Self, ExecutionSupervisorError> {
        if max_concurrent == 0 {
            return Err(ExecutionSupervisorError::InvalidConcurrencyLimit);
        }
        self.supervision_slots = Arc::new(Semaphore::new(max_concurrent));
        Ok(self)
    }

    #[must_use]
    pub fn runner(&self) -> &R {
        &self.runner
    }

    /// Asynchronously supervise one prepared execution attempt on the bounded
    /// blocking pool without holding any synchronous mutex across `.await`.
    ///
    /// # Errors
    /// Rejects missing or non-`Prepared` attempts, spawn/probe failures, or
    /// durable catalog persistence errors.
    pub async fn supervise_attempt(
        &self,
        attempt_key: ExecutionAttemptKey,
    ) -> Result<SupervisionOutcome, ExecutionSupervisorError> {
        self.supervise_attempt_with_context(attempt_key, CliExecutionContext::default())
            .await
    }

    /// Asynchronously supervise one prepared execution attempt with a bounded,
    /// non-secret [`CliExecutionContext`].
    ///
    /// # Errors
    /// Rejects invalid execution contexts, missing or non-`Prepared` attempts,
    /// spawn/probe failures, or durable catalog persistence errors.
    pub async fn supervise_attempt_with_context(
        &self,
        attempt_key: ExecutionAttemptKey,
        context: CliExecutionContext,
    ) -> Result<SupervisionOutcome, ExecutionSupervisorError> {
        let permit = Arc::clone(&self.supervision_slots)
            .acquire_owned()
            .await
            .map_err(|_| ExecutionSupervisorError::ConcurrencyBridgeClosed)?;
        let supervisor = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _owner = supervisor.owner_pin.as_ref();
            supervisor.supervise_attempt_blocking_with_context(attempt_key, &context)
        })
        .await
        .map_err(|_| ExecutionSupervisorError::BlockingTaskJoinFailed)?
    }

    /// Synchronously supervise one prepared execution attempt.
    ///
    /// `Mutex<OperationsCatalog>` is acquired only for short point reads and
    /// state transitions, never while spawning, polling, sleeping, or signaling.
    ///
    /// # Errors
    /// Rejects missing or non-`Prepared` attempts, spawn/probe failures, or
    /// durable catalog persistence errors.
    pub fn supervise_attempt_blocking(
        &self,
        attempt_key: ExecutionAttemptKey,
    ) -> Result<SupervisionOutcome, ExecutionSupervisorError> {
        self.supervise_attempt_blocking_with_context(attempt_key, &CliExecutionContext::default())
    }

    /// Synchronously supervise one prepared execution attempt with a bounded,
    /// non-secret [`CliExecutionContext`].
    ///
    /// # Errors
    /// Rejects invalid execution contexts, missing or non-`Prepared` attempts,
    /// spawn/probe failures, or durable catalog persistence errors.
    pub fn supervise_attempt_blocking_with_context(
        &self,
        attempt_key: ExecutionAttemptKey,
        context: &CliExecutionContext,
    ) -> Result<SupervisionOutcome, ExecutionSupervisorError> {
        let (snapshot, prepared_revision) = self.load_prepared_snapshot(attempt_key)?;
        context.validate_against_snapshot(&snapshot)?;
        let (mut handle, spawned_ledger) =
            self.spawn_and_record(attempt_key, &snapshot, prepared_revision, context)?;
        self.wait_until_exit_or_timeout(attempt_key, &snapshot, &mut handle, &spawned_ledger)
    }

    /// Asynchronously reconcile durable launch-ledger state on host/daemon
    /// startup without signaling unpinned recovery process groups.
    ///
    /// # Errors
    /// Returns boot-ID probe, catalog persistence, or read-only inspection errors.
    pub async fn recover_on_boot(
        &self,
        workspace_id: Option<WorkspaceId>,
        reconciled_at_ms: i64,
    ) -> Result<BootRecoveryReport, ExecutionSupervisorError> {
        let permit = Arc::clone(&self.supervision_slots)
            .acquire_owned()
            .await
            .map_err(|_| ExecutionSupervisorError::ConcurrencyBridgeClosed)?;
        let supervisor = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _owner = supervisor.owner_pin.as_ref();
            supervisor.recover_on_boot_blocking(workspace_id, reconciled_at_ms)
        })
        .await
        .map_err(|_| ExecutionSupervisorError::BlockingTaskJoinFailed)?
    }

    /// Synchronously reconcile `Prepared -> NotStarted` and
    /// `Spawned -> Unknown { BootRecoveryAfterSpawn }`, classifying orphan
    /// rows read-only via `verify_ownership` without auto-retrying any attempt
    /// or signaling unpinned `-pgid` groups.
    ///
    /// # Errors
    /// Returns boot-ID probe, catalog persistence, or read-only inspection errors.
    pub fn recover_on_boot_blocking(
        &self,
        workspace_id: Option<WorkspaceId>,
        reconciled_at_ms: i64,
    ) -> Result<BootRecoveryReport, ExecutionSupervisorError> {
        let current_boot_id = self.runner.current_boot_id()?;
        let transitioned = {
            let mut catalog = self.lock_catalog()?;
            catalog.reconcile_launch_ledger_on_boot(
                workspace_id,
                &current_boot_id,
                reconciled_at_ms,
            )?
        };

        let mut entries = Vec::with_capacity(transitioned.len());
        for ledger in transitioned {
            let cleanup = self.classify_recovered_record(&ledger, &current_boot_id)?;
            entries.push(BootRecoveryEntry { ledger, cleanup });
        }
        Ok(BootRecoveryReport {
            current_boot_id,
            entries,
        })
    }

    fn lock_catalog(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, OperationsCatalog>, ExecutionSupervisorError> {
        self.operations
            .lock()
            .map_err(|_| ExecutionSupervisorError::CatalogPoisoned)
    }

    fn load_prepared_snapshot(
        &self,
        attempt_key: ExecutionAttemptKey,
    ) -> Result<(AssignmentExecutionSnapshot, u64), ExecutionSupervisorError> {
        let maybe_record = {
            let catalog = self.lock_catalog()?;
            catalog.get_assignment_execution_snapshot(attempt_key)?
        };
        let Some((snapshot, ledger)) = maybe_record else {
            return Err(ExecutionSupervisorError::attempt_not_found(attempt_key));
        };
        if !matches!(ledger.state(), LaunchLedgerState::Prepared) {
            return Err(ExecutionSupervisorError::attempt_not_prepared(
                attempt_key,
                ledger.state().clone(),
            ));
        }
        Ok((snapshot, ledger.revision()))
    }

    fn spawn_and_record(
        &self,
        attempt_key: ExecutionAttemptKey,
        snapshot: &AssignmentExecutionSnapshot,
        prepared_revision: u64,
        context: &CliExecutionContext,
    ) -> Result<(SpawnedProcessHandle, LaunchLedgerRecord), ExecutionSupervisorError> {
        let mut handle = self.runner.spawn_prepared_with_context(snapshot, context)?;
        let ownership = handle.ownership().clone();
        let spawned_at_ms = wall_clock_ms()?.max(snapshot.committed_at_ms());

        let record_result = {
            let mut catalog = self.lock_catalog()?;
            catalog.record_execution_spawned(
                attempt_key,
                prepared_revision,
                ownership.clone(),
                spawned_at_ms,
            )
        };

        match record_result {
            Ok(ledger) => Ok((handle, ledger)),
            Err(catalog_err) => {
                let grace_period =
                    Duration::from_millis(snapshot.timeouts().termination_grace_ms());
                let _ = self.runner.terminate_owned_group(&mut handle, grace_period);
                let _ = handle.reap();
                Err(ExecutionSupervisorError::operations(catalog_err))
            }
        }
    }

    fn wait_until_exit_or_timeout(
        &self,
        attempt_key: ExecutionAttemptKey,
        snapshot: &AssignmentExecutionSnapshot,
        handle: &mut SpawnedProcessHandle,
        spawned_ledger: &LaunchLedgerRecord,
    ) -> Result<SupervisionOutcome, ExecutionSupervisorError> {
        let timeout = Duration::from_millis(snapshot.timeouts().execution_timeout_ms());
        let deadline = Instant::now() + timeout;

        loop {
            match self.runner.poll_leader_exited(handle) {
                Ok(true) => {
                    return self.finalize_exited_attempt(
                        attempt_key,
                        snapshot,
                        handle,
                        spawned_ledger,
                    );
                }
                Ok(false) => {}
                Err(poll_err) => {
                    return self.record_poll_failure_and_reap(
                        attempt_key,
                        handle,
                        spawned_ledger,
                        poll_err,
                    );
                }
            }
            let now = Instant::now();
            if now >= deadline {
                return self.finalize_timed_out_attempt(
                    attempt_key,
                    snapshot,
                    handle,
                    spawned_ledger,
                );
            }
            let remaining = deadline.saturating_duration_since(now);
            thread::sleep(remaining.min(SUPERVISION_POLL_INTERVAL));
        }
    }

    fn record_poll_failure_and_reap(
        &self,
        attempt_key: ExecutionAttemptKey,
        handle: &mut SpawnedProcessHandle,
        spawned_ledger: &LaunchLedgerRecord,
        poll_err: ExecutionSupervisorError,
    ) -> Result<SupervisionOutcome, ExecutionSupervisorError> {
        let _ = handle.reap();
        if matches!(poll_err, ExecutionSupervisorError::OwnershipMismatch { .. }) {
            let recorded_at_ms = wall_clock_ms()?.max(spawned_ledger.updated_at_ms());
            let mut catalog = self.lock_catalog()?;
            let _ = catalog.record_execution_unknown(
                attempt_key,
                spawned_ledger.revision(),
                LaunchUnknownReason::OwnershipMismatch,
                recorded_at_ms,
            );
        }
        Err(poll_err)
    }

    fn finalize_exited_attempt(
        &self,
        attempt_key: ExecutionAttemptKey,
        snapshot: &AssignmentExecutionSnapshot,
        handle: &mut SpawnedProcessHandle,
        spawned_ledger: &LaunchLedgerRecord,
    ) -> Result<SupervisionOutcome, ExecutionSupervisorError> {
        let grace_period = Duration::from_millis(snapshot.timeouts().termination_grace_ms());
        // `handle` still retains `Child` un-reaped in zombie state `'Z'`,
        // pinning `(pid, pgid)` in the kernel process table while surviving
        // non-zombie descendants in `handle.ownership().pgid()` are drained.
        if let Err(cleanup_err) = self.runner.terminate_owned_group(handle, grace_period) {
            let _ = handle.reap();
            let reason = match &cleanup_err {
                ExecutionSupervisorError::OwnershipMismatch { .. } => {
                    LaunchUnknownReason::OwnershipMismatch
                }
                _ => LaunchUnknownReason::ForcedTermination,
            };
            let recorded_at_ms = wall_clock_ms()?.max(spawned_ledger.updated_at_ms());
            let mut catalog = self.lock_catalog()?;
            let _ = catalog.record_execution_unknown(
                attempt_key,
                spawned_ledger.revision(),
                reason,
                recorded_at_ms,
            );
            return Err(cleanup_err);
        }

        let exit_status = handle.reap()?.ok_or_else(|| {
            ExecutionSupervisorError::ProcessProbeFailed(
                "reaped process handle yielded no ExitStatus".to_owned(),
            )
        })?;
        let exit_code = exit_code_from_status(exit_status);
        let exited_at_ms = wall_clock_ms()?.max(spawned_ledger.updated_at_ms());
        let ledger = {
            let mut catalog = self.lock_catalog()?;
            catalog.record_execution_exited(
                attempt_key,
                spawned_ledger.revision(),
                exit_code,
                exited_at_ms,
            )?
        };
        Ok(SupervisionOutcome::Exited { ledger, exit_code })
    }

    fn finalize_timed_out_attempt(
        &self,
        attempt_key: ExecutionAttemptKey,
        snapshot: &AssignmentExecutionSnapshot,
        handle: &mut SpawnedProcessHandle,
        spawned_ledger: &LaunchLedgerRecord,
    ) -> Result<SupervisionOutcome, ExecutionSupervisorError> {
        let grace_period = Duration::from_millis(snapshot.timeouts().termination_grace_ms());
        // `handle` still holds `Child` un-waited here, pinning `(pid, pgid)`
        // in the kernel process table until `terminate_owned_group` finishes.
        let termination_result = self.runner.terminate_owned_group(handle, grace_period);
        let _ = handle.reap();

        let recorded_at_ms = wall_clock_ms()?.max(spawned_ledger.updated_at_ms());
        match termination_result {
            Ok(termination) => {
                let ledger = {
                    let mut catalog = self.lock_catalog()?;
                    catalog.record_execution_unknown(
                        attempt_key,
                        spawned_ledger.revision(),
                        LaunchUnknownReason::ExecutionTimedOut,
                        recorded_at_ms,
                    )?
                };
                Ok(SupervisionOutcome::TimedOut {
                    ledger,
                    termination,
                })
            }
            Err(term_err) => {
                let reason = match &term_err {
                    ExecutionSupervisorError::OwnershipMismatch { .. } => {
                        LaunchUnknownReason::OwnershipMismatch
                    }
                    _ => LaunchUnknownReason::ExecutionTimedOut,
                };
                let mut catalog = self.lock_catalog()?;
                let _ = catalog.record_execution_unknown(
                    attempt_key,
                    spawned_ledger.revision(),
                    reason,
                    recorded_at_ms,
                );
                Err(term_err)
            }
        }
    }

    fn classify_recovered_record(
        &self,
        ledger: &LaunchLedgerRecord,
        current_boot_id: &str,
    ) -> Result<BootRecoveryCleanup, ExecutionSupervisorError> {
        let LaunchLedgerState::Unknown {
            ownership: Some(ownership),
            reason: LaunchUnknownReason::BootRecoveryAfterSpawn,
            ..
        } = ledger.state()
        else {
            return Ok(BootRecoveryCleanup::NotApplicable);
        };

        if ownership.boot_id() != current_boot_id {
            return Ok(BootRecoveryCleanup::BootMismatchSkipped);
        }

        match self.runner.verify_ownership(ownership)? {
            OwnershipVerification::ProcessAbsent => Ok(BootRecoveryCleanup::LeaderAbsentSkipped),
            OwnershipVerification::IdentityMismatch => {
                Ok(BootRecoveryCleanup::IdentityMismatchSkipped)
            }
            OwnershipVerification::OwnedAlive => Ok(BootRecoveryCleanup::UnpinnedOrphanSkipped),
        }
    }
}

fn exit_code_from_status(status: ExitStatus) -> i32 {
    status.code().unwrap_or_else(|| {
        status
            .signal()
            .map_or(-1, |sig| 128_i32.saturating_add(sig))
    })
}

fn wall_clock_ms() -> Result<i64, ExecutionSupervisorError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ExecutionSupervisorError::ClockBeforeUnixEpoch)?;
    i64::try_from(duration.as_millis()).map_err(|_| ExecutionSupervisorError::ClockBeforeUnixEpoch)
}

#[cfg(test)]
mod tests;
