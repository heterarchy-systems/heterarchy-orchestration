//! Safe Unix process-group runner using `CommandExt::process_group(0)`,
//! OS boot identity checks, `/bin/ps` ownership verification, and `/bin/kill`
//! process-group termination without `unsafe` or `libc`.

use std::fs;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use heterarchy_orchestration_operations::{AssignmentExecutionSnapshot, ProcessOwnershipIdentity};

use super::invocation::{
    CliExecutableResolver, CliExecutionContext, CliInvocationPlan, CommandOverride,
};
use super::{
    ExecutionRunner, ExecutionSupervisorError, OwnershipVerification, SpawnedProcessHandle,
    TerminationDisposition,
};

const PS_BINARY: &str = "/bin/ps";
const KILL_BINARY: &str = "/bin/kill";
const MACOS_SYSCTL_BINARY: &str = "/usr/sbin/sysctl";
const LINUX_BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";
pub(super) const IMMEDIATE_EXIT_START_TIME: &str = "exited-immediately";
const GROUP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const UNCONFIRMED_SPAWN_TERM_GRACE: Duration = Duration::from_millis(50);
const POST_SIGKILL_REAP_TIMEOUT: Duration = Duration::from_millis(500);

/// Configuration for [`LocalProcessRunner`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalProcessRunnerConfig {
    resolver: CliExecutableResolver,
    boot_id_override: Option<String>,
}

impl LocalProcessRunnerConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_resolver(mut self, resolver: CliExecutableResolver) -> Self {
        self.resolver = resolver;
        self
    }

    #[must_use]
    pub fn with_codex_binary<P: Into<PathBuf>>(mut self, binary: P) -> Self {
        self.resolver = self.resolver.with_codex_binary(binary);
        self
    }

    #[must_use]
    pub fn with_agy_binary<P: Into<PathBuf>>(mut self, binary: P) -> Self {
        self.resolver = self.resolver.with_agy_binary(binary);
        self
    }

    #[must_use]
    pub fn with_command_override(mut self, command_override: CommandOverride) -> Self {
        self.resolver = self.resolver.with_command_override(command_override);
        self
    }

    /// Override the observed host boot identifier for deterministic cross-boot
    /// recovery tests.
    #[must_use]
    pub fn with_boot_id_override<S: Into<String>>(mut self, boot_id: S) -> Self {
        self.boot_id_override = Some(boot_id.into());
        self
    }

    #[must_use]
    pub const fn resolver(&self) -> &CliExecutableResolver {
        &self.resolver
    }
}

/// Safe local Unix process-group runner (`unsafe_code = "forbid"`).
#[derive(Debug, Clone, Default)]
pub struct LocalProcessRunner {
    config: LocalProcessRunnerConfig,
}

impl LocalProcessRunner {
    #[must_use]
    pub const fn new(config: LocalProcessRunnerConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub const fn config(&self) -> &LocalProcessRunnerConfig {
        &self.config
    }

    fn effective_boot_id(&self) -> Result<String, ExecutionSupervisorError> {
        if let Some(override_id) = &self.config.boot_id_override {
            let trimmed = override_id.trim();
            if trimmed.is_empty() {
                return Err(ExecutionSupervisorError::BootIdUnavailable(
                    "configured boot_id_override is empty".to_owned(),
                ));
            }
            return Ok(trimmed.to_owned());
        }
        current_system_boot_id()
    }
}

impl ExecutionRunner for LocalProcessRunner {
    fn current_boot_id(&self) -> Result<String, ExecutionSupervisorError> {
        self.effective_boot_id()
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
        let plan = self
            .config
            .resolver
            .resolve_with_context(snapshot, context)?;
        let boot_id = self.effective_boot_id()?;
        spawn_isolated_process_group(&plan, boot_id)
    }

    fn poll_leader_exited(
        &self,
        handle: &mut SpawnedProcessHandle,
    ) -> Result<bool, ExecutionSupervisorError> {
        let expected_pid = handle.ownership().pid();
        if !handle.has_pinned_child() || handle.pinned_child_id() != Some(expected_pid) {
            return Ok(handle.try_wait()?.is_some());
        }
        let ownership = handle.ownership();
        let current_boot = self.effective_boot_id()?;
        if current_boot != ownership.boot_id() {
            return Err(ExecutionSupervisorError::OwnershipMismatch {
                pid: ownership.pid(),
                expected_pgid: ownership.pgid(),
                observed_pgid: 0,
                expected_start_time: ownership.start_time().to_owned(),
                observed_start_time: format!("boot_id={current_boot}"),
            });
        }
        let Some(observed) = inspect_process_leader(ownership.pid())? else {
            return Ok(handle.try_wait()?.is_some());
        };
        if observed.pid != ownership.pid()
            || observed.pgid != ownership.pgid()
            || observed.start_time != ownership.start_time()
        {
            return Err(ExecutionSupervisorError::OwnershipMismatch {
                pid: ownership.pid(),
                expected_pgid: ownership.pgid(),
                observed_pgid: observed.pgid,
                expected_start_time: ownership.start_time().to_owned(),
                observed_start_time: observed.start_time,
            });
        }
        Ok(observed.is_zombie)
    }

    fn verify_ownership(
        &self,
        ownership: &ProcessOwnershipIdentity,
    ) -> Result<OwnershipVerification, ExecutionSupervisorError> {
        let current_boot = self.effective_boot_id()?;
        if current_boot != ownership.boot_id() {
            return Ok(OwnershipVerification::IdentityMismatch);
        }
        if ownership.start_time() == IMMEDIATE_EXIT_START_TIME {
            return Ok(OwnershipVerification::ProcessAbsent);
        }
        let Some(observed) = inspect_process_leader(ownership.pid())? else {
            return Ok(OwnershipVerification::ProcessAbsent);
        };
        if observed.pid == ownership.pid()
            && observed.pgid == ownership.pgid()
            && observed.start_time == ownership.start_time()
        {
            Ok(OwnershipVerification::OwnedAlive)
        } else {
            Ok(OwnershipVerification::IdentityMismatch)
        }
    }

    fn terminate_owned_group(
        &self,
        handle: &mut SpawnedProcessHandle,
        grace_period: Duration,
    ) -> Result<TerminationDisposition, ExecutionSupervisorError> {
        let expected_pid = handle.ownership().pid();
        if !handle.has_pinned_child() || handle.pinned_child_id() != Some(expected_pid) {
            return Ok(TerminationDisposition::AlreadyExited);
        }
        let ownership = handle.ownership();
        let current_boot = self.effective_boot_id()?;
        if current_boot != ownership.boot_id() {
            return Ok(TerminationDisposition::BootMismatchSkipped);
        }
        if ownership.start_time() == IMMEDIATE_EXIT_START_TIME {
            return Ok(TerminationDisposition::AlreadyExited);
        }
        // `handle.child` is `Some(Child)` and has not had `.wait()` called yet,
        // keeping `(pid, pgid)` kernel-pinned (live or zombie `'Z'`) throughout
        // `-pgid` signaling until `handle.reap()` runs afterward.
        let Some(observed) = inspect_process_leader(ownership.pid())? else {
            return Ok(TerminationDisposition::AlreadyExited);
        };
        if observed.pid != ownership.pid()
            || observed.pgid != ownership.pgid()
            || observed.start_time != ownership.start_time()
        {
            return Err(ExecutionSupervisorError::OwnershipMismatch {
                pid: ownership.pid(),
                expected_pgid: ownership.pgid(),
                observed_pgid: observed.pgid,
                expected_start_time: ownership.start_time().to_owned(),
                observed_start_time: observed.start_time,
            });
        }
        signal_and_drain_process_group(ownership.pgid(), grace_period)
    }
}

/// Read the current host kernel boot identity on `macOS` or Linux.
///
/// # Errors
/// Returns [`ExecutionSupervisorError::BootIdUnavailable`] if the OS boot
/// identifier probe fails or yields empty output.
pub fn current_system_boot_id() -> Result<String, ExecutionSupervisorError> {
    if cfg!(target_os = "macos") {
        let output = Command::new(MACOS_SYSCTL_BINARY)
            .args(["-n", "kern.boottime"])
            .stdin(Stdio::null())
            .output()
            .map_err(|err| {
                ExecutionSupervisorError::BootIdUnavailable(format!(
                    "failed to execute {MACOS_SYSCTL_BINARY}: {err}"
                ))
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ExecutionSupervisorError::BootIdUnavailable(format!(
                "{MACOS_SYSCTL_BINARY} exited with {}: {}",
                output.status,
                stderr.trim()
            )));
        }
        let boot_id = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if boot_id.is_empty() {
            return Err(ExecutionSupervisorError::BootIdUnavailable(
                "kern.boottime returned empty output".to_owned(),
            ));
        }
        return Ok(boot_id);
    }

    if cfg!(target_os = "linux") {
        let raw = fs::read_to_string(Path::new(LINUX_BOOT_ID_PATH)).map_err(|err| {
            ExecutionSupervisorError::BootIdUnavailable(format!(
                "failed to read {LINUX_BOOT_ID_PATH}: {err}"
            ))
        })?;
        let boot_id = raw.trim().to_owned();
        if boot_id.is_empty() {
            return Err(ExecutionSupervisorError::BootIdUnavailable(format!(
                "{LINUX_BOOT_ID_PATH} is empty"
            )));
        }
        return Ok(boot_id);
    }

    Err(ExecutionSupervisorError::BootIdUnavailable(
        "unsupported operating system for boot_id probe".to_owned(),
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedProcessLeader {
    pid: u32,
    pgid: u32,
    is_zombie: bool,
    start_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProcessGroupSnapshot {
    pub(super) live_pids: Vec<u32>,
    pub(super) zombie_pids: Vec<u32>,
}

/// RAII cleanup guard ensuring any post-`spawn` probe or validation failure
/// terminates the freshly isolated process group while `Child` still pins
/// `(pid, pgid)` and reaps `Child` before returning.
pub(super) struct UnconfirmedSpawnGuard {
    child: Option<Child>,
    leader_pid: u32,
    signal_isolated_group: bool,
}

impl UnconfirmedSpawnGuard {
    pub(super) const fn new(child: Child, leader_pid: u32) -> Self {
        Self {
            child: Some(child),
            leader_pid,
            signal_isolated_group: true,
        }
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>, std::io::Error> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        let status = child.try_wait()?;
        if status.is_some() {
            self.child = None;
            self.signal_isolated_group = false;
        }
        Ok(status)
    }

    fn into_pinned_child(mut self) -> Option<Child> {
        self.signal_isolated_group = false;
        self.child.take()
    }
}

impl Drop for UnconfirmedSpawnGuard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if self.signal_isolated_group {
            abort_unconfirmed_spawn(&mut child, self.leader_pid);
        } else {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub(super) fn abort_unconfirmed_spawn(child: &mut Child, leader_pid: u32) {
    let _ = send_group_signal(leader_pid, "TERM");
    if !wait_for_group_drain(leader_pid, UNCONFIRMED_SPAWN_TERM_GRACE).unwrap_or(false) {
        let _ = send_group_signal(leader_pid, "KILL");
        let _ = wait_for_group_drain(leader_pid, POST_SIGKILL_REAP_TIMEOUT);
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn spawn_isolated_process_group(
    plan: &CliInvocationPlan,
    boot_id: String,
) -> Result<SpawnedProcessHandle, ExecutionSupervisorError> {
    let (verified_executable, canonical_worktree, pinned_exec_fd, pinned_wt_fd) =
        open_and_verify_immutable_launch_boundary(plan)?;

    let mut command = Command::new(&verified_executable);
    command
        .args(plan.args())
        .current_dir(&canonical_worktree)
        .env_clear()
        .envs(plan.env())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);

    let child = command.spawn()?;
    let leader_pid = child.id();
    let mut guard = UnconfirmedSpawnGuard::new(child, leader_pid);

    verify_post_spawn_descriptors(
        plan,
        &verified_executable,
        &canonical_worktree,
        &pinned_exec_fd,
        &pinned_wt_fd,
    )?;

    // Inspect BEFORE calling `try_wait()` so even a fast-exiting child
    // remains un-reaped in state `'Z'` with its real `(pid, pgid, start_time)`
    // pinned in the kernel process table. Any `Err` below drops `guard`,
    // terminating the original isolated group `-leader_pid` while still pinned
    // and separately killing/reaping `Child`.
    if let Some(leader) = inspect_process_leader(leader_pid)? {
        if leader.pgid != leader_pid {
            return Err(ExecutionSupervisorError::ProcessGroupNotIsolated {
                pid: leader_pid,
                observed_pgid: leader.pgid,
            });
        }
        let ownership =
            ProcessOwnershipIdentity::new(leader_pid, leader.pgid, leader.start_time, boot_id)
                .map_err(ExecutionSupervisorError::invalid_ownership_identity)?;
        let pinned_child = guard.into_pinned_child().ok_or_else(|| {
            ExecutionSupervisorError::ProcessProbeFailed(
                "unconfirmed spawn guard lost child handle".to_owned(),
            )
        })?;
        return Ok(SpawnedProcessHandle::from_verified_spawn(
            ownership,
            pinned_child,
        ));
    }

    if let Some(exit_status) = guard.try_wait()? {
        let ownership = ProcessOwnershipIdentity::new(
            leader_pid,
            leader_pid,
            IMMEDIATE_EXIT_START_TIME.to_owned(),
            boot_id,
        )
        .map_err(ExecutionSupervisorError::invalid_ownership_identity)?;
        return Ok(SpawnedProcessHandle::unpinned(ownership, Some(exit_status)));
    }

    Err(ExecutionSupervisorError::ProcessProbeFailed(format!(
        "spawned process {leader_pid} was not visible to {PS_BINARY} while still un-reaped"
    )))
}

fn open_and_verify_immutable_launch_boundary(
    plan: &CliInvocationPlan,
) -> Result<(PathBuf, PathBuf, fs::File, fs::File), ExecutionSupervisorError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let verified_executable = plan.revalidate_before_spawn()?;
    let canonical_worktree = fs::canonicalize(plan.current_dir())?;
    let pinned_exec_fd = fs::File::open(&verified_executable)?;
    let pinned_wt_fd = fs::File::open(&canonical_worktree)?;
    let exec_fd_meta = pinned_exec_fd.metadata()?;
    let wt_fd_meta = pinned_wt_fd.metadata()?;
    let pre_exec_meta = fs::symlink_metadata(&verified_executable)?;
    let pre_wt_meta = fs::symlink_metadata(&canonical_worktree)?;

    if !exec_fd_meta.is_file()
        || (exec_fd_meta.permissions().mode() & 0o022) != 0
        || exec_fd_meta.dev() != pre_exec_meta.dev()
        || exec_fd_meta.ino() != pre_exec_meta.ino()
    {
        return Err(ExecutionSupervisorError::InvalidExecutablePath(
            verified_executable,
        ));
    }
    for ancestor in verified_executable.ancestors().skip(1) {
        let ancestor_meta = fs::symlink_metadata(ancestor)?;
        if !ancestor_meta.is_dir() || (ancestor_meta.permissions().mode() & 0o022) != 0 {
            return Err(ExecutionSupervisorError::InvalidExecutablePath(
                verified_executable,
            ));
        }
    }
    if !wt_fd_meta.is_dir()
        || wt_fd_meta.dev() != pre_wt_meta.dev()
        || wt_fd_meta.ino() != pre_wt_meta.ino()
    {
        return Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(
            plan.current_dir().to_path_buf(),
        ));
    }
    Ok((
        verified_executable,
        canonical_worktree,
        pinned_exec_fd,
        pinned_wt_fd,
    ))
}

fn verify_post_spawn_descriptors(
    plan: &CliInvocationPlan,
    verified_executable: &Path,
    canonical_worktree: &Path,
    pinned_exec_fd: &fs::File,
    pinned_wt_fd: &fs::File,
) -> Result<(), ExecutionSupervisorError> {
    use std::os::unix::fs::MetadataExt as _;

    let exec_fd_meta = pinned_exec_fd.metadata()?;
    let wt_fd_meta = pinned_wt_fd.metadata()?;
    let post_exec_path = plan.revalidate_before_spawn()?;
    let post_exec_meta = fs::symlink_metadata(&post_exec_path)?;
    let post_wt_canonical = fs::canonicalize(plan.current_dir())?;
    let post_wt_meta = fs::symlink_metadata(&post_wt_canonical)?;
    if post_exec_path != verified_executable
        || post_exec_meta.dev() != exec_fd_meta.dev()
        || post_exec_meta.ino() != exec_fd_meta.ino()
    {
        return Err(ExecutionSupervisorError::InvalidExecutablePath(
            verified_executable.to_path_buf(),
        ));
    }
    if post_wt_canonical != canonical_worktree
        || post_wt_meta.dev() != wt_fd_meta.dev()
        || post_wt_meta.ino() != wt_fd_meta.ino()
    {
        return Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(
            plan.current_dir().to_path_buf(),
        ));
    }
    Ok(())
}

fn inspect_process_leader(
    pid: u32,
) -> Result<Option<ObservedProcessLeader>, ExecutionSupervisorError> {
    let pid_arg = pid.to_string();
    let output = Command::new(PS_BINARY)
        .args(["-o", "pid=,pgid=,state=,lstart=", "-p", &pid_arg])
        .stdin(Stdio::null())
        .output()
        .map_err(|err| {
            ExecutionSupervisorError::ProcessProbeFailed(format!(
                "failed to execute {PS_BINARY} for pid {pid}: {err}"
            ))
        })?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stdout.trim().is_empty() {
        if stderr.trim().is_empty() || stderr.contains("process id too large") {
            return Ok(None);
        }
        return Err(ExecutionSupervisorError::ProcessProbeFailed(format!(
            "{PS_BINARY} failed for pid {pid}: {}",
            stderr.trim()
        )));
    }
    parse_ps_leader_row(&stdout)
}

fn parse_ps_leader_row(
    stdout: &str,
) -> Result<Option<ObservedProcessLeader>, ExecutionSupervisorError> {
    let Some(line) = stdout.lines().map(str::trim).find(|line| !line.is_empty()) else {
        return Ok(None);
    };
    let (process_id_token, rest) = line
        .split_once(char::is_whitespace)
        .ok_or_else(|| ExecutionSupervisorError::ProcessProbeParse(line.to_owned()))?;
    let (group_id_token, rest) = rest
        .trim_start()
        .split_once(char::is_whitespace)
        .ok_or_else(|| ExecutionSupervisorError::ProcessProbeParse(line.to_owned()))?;
    let (state_token, rest) = rest
        .trim_start()
        .split_once(char::is_whitespace)
        .ok_or_else(|| ExecutionSupervisorError::ProcessProbeParse(line.to_owned()))?;
    let start_time = rest.trim();
    if state_token.is_empty() || start_time.is_empty() {
        return Err(ExecutionSupervisorError::ProcessProbeParse(line.to_owned()));
    }
    let process_id = process_id_token
        .parse::<u32>()
        .map_err(|_| ExecutionSupervisorError::ProcessProbeParse(line.to_owned()))?;
    let group_id = group_id_token
        .parse::<u32>()
        .map_err(|_| ExecutionSupervisorError::ProcessProbeParse(line.to_owned()))?;
    let is_zombie = state_token.starts_with('Z') || state_token.starts_with('z');
    Ok(Some(ObservedProcessLeader {
        pid: process_id,
        pgid: group_id,
        is_zombie,
        start_time: start_time.to_owned(),
    }))
}

fn has_non_zombie_group_members(pgid: u32) -> Result<bool, ExecutionSupervisorError> {
    Ok(!inspect_process_group(pgid)?.live_pids.is_empty())
}

pub(super) fn inspect_process_group(
    target_pgid: u32,
) -> Result<ProcessGroupSnapshot, ExecutionSupervisorError> {
    let output = Command::new(PS_BINARY)
        .args(["-axo", "pid=,pgid=,state="])
        .stdin(Stdio::null())
        .output()
        .map_err(|err| {
            ExecutionSupervisorError::ProcessProbeFailed(format!(
                "failed to execute {PS_BINARY} -axo: {err}"
            ))
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ExecutionSupervisorError::ProcessProbeFailed(format!(
            "{PS_BINARY} -axo exited with {}: {}",
            output.status,
            stderr.trim()
        )));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_ps_group_table(&stdout, target_pgid)
}

fn parse_ps_group_table(
    stdout: &str,
    target_pgid: u32,
) -> Result<ProcessGroupSnapshot, ExecutionSupervisorError> {
    let mut live_pids = Vec::new();
    let mut zombie_pids = Vec::new();
    for raw_line in stdout.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let mut tokens = line.split_whitespace();
        let (Some(process_id_str), Some(group_id_str), Some(state_str)) =
            (tokens.next(), tokens.next(), tokens.next())
        else {
            return Err(ExecutionSupervisorError::ProcessProbeParse(line.to_owned()));
        };
        let process_id = process_id_str
            .parse::<u32>()
            .map_err(|_| ExecutionSupervisorError::ProcessProbeParse(line.to_owned()))?;
        let group_id = group_id_str
            .parse::<u32>()
            .map_err(|_| ExecutionSupervisorError::ProcessProbeParse(line.to_owned()))?;
        if group_id != target_pgid {
            continue;
        }
        if state_str.starts_with('Z') || state_str.starts_with('z') {
            zombie_pids.push(process_id);
        } else {
            live_pids.push(process_id);
        }
    }
    Ok(ProcessGroupSnapshot {
        live_pids,
        zombie_pids,
    })
}

fn signal_and_drain_process_group(
    pgid: u32,
    grace_period: Duration,
) -> Result<TerminationDisposition, ExecutionSupervisorError> {
    if !has_non_zombie_group_members(pgid)? {
        return Ok(TerminationDisposition::AlreadyExited);
    }

    send_group_signal(pgid, "TERM")?;
    if wait_for_group_drain(pgid, grace_period)? {
        return Ok(TerminationDisposition::TerminatedAfterTerm);
    }

    send_group_signal(pgid, "KILL")?;
    if wait_for_group_drain(pgid, POST_SIGKILL_REAP_TIMEOUT)? {
        return Ok(TerminationDisposition::TerminatedAfterKill);
    }

    let remaining = inspect_process_group(pgid)?.live_pids.len();
    if remaining == 0 {
        return Ok(TerminationDisposition::TerminatedAfterKill);
    }
    Err(ExecutionSupervisorError::GroupTerminationUnconfirmed {
        pgid,
        remaining_live: remaining,
    })
}

fn send_group_signal(pgid: u32, signal_name: &str) -> Result<(), ExecutionSupervisorError> {
    let target = format!("-{pgid}");
    let output = Command::new(KILL_BINARY)
        .args(["-s", signal_name, "--", &target])
        .stdin(Stdio::null())
        .output()
        .map_err(|err| ExecutionSupervisorError::KillCommandFailed {
            pgid,
            signal: signal_name.to_owned(),
            detail: err.to_string(),
        })?;
    if output.status.success() {
        return Ok(());
    }
    // If all non-zombie members exited concurrently right before `/bin/kill`
    // ran, Darwin/Linux `killpg` can return ESRCH or EPERM (when only a pinned
    // zombie leader remains). Confirm via group table before reporting failure.
    if !has_non_zombie_group_members(pgid)? {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Err(ExecutionSupervisorError::KillCommandFailed {
        pgid,
        signal: signal_name.to_owned(),
        detail: if stderr.is_empty() {
            format!("exit status {}", output.status)
        } else {
            stderr
        },
    })
}

fn wait_for_group_drain(pgid: u32, timeout: Duration) -> Result<bool, ExecutionSupervisorError> {
    let deadline = Instant::now() + timeout;
    loop {
        if !has_non_zombie_group_members(pgid)? {
            return Ok(true);
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(false);
        }
        let remaining = deadline.saturating_duration_since(now);
        thread::sleep(remaining.min(GROUP_POLL_INTERVAL));
    }
}
