mod guards;
mod prompt;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use forge_domain::{
    ActivityId, AgentAssignmentId, DagNodeId, DagRevisionNumber, GoalId, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AssignmentExecutionSnapshot, AssignmentExecutionSnapshotParts, ClaimSwarmWakeupCommand,
    CompleteSwarmTurnCommand, ExecutionAttemptKey, ExecutionPermissions, ExecutionTimeouts,
    FailOrSuspendSwarmTurnCommand, ForgeWorktreeRef, LaunchUnknownReason, LeaseRef, McpGrant,
    OperationsCatalog, SnapshotValidationError, SwarmError, SwarmMemberRecord, SwarmMemberState,
    SwarmRoleKind, SwarmRunMode, SwarmTurnFailureDisposition, SwarmTurnOutput, SwarmWakeupBatch,
    SwarmWorkspaceMode,
};
use heterarchy_orchestration_sessions::{
    AgentProfileCatalog, AgentProfileId, AgentProfileLifecycleState, AgentProfileRevision,
    ExecutionPreference,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

use self::guards::InflightMemberGuard;
pub(super) use self::guards::WorkingTurnDropGuard;
use self::prompt::build_swarm_execution_context;
use crate::execution_supervisor::{
    CliExecutionContext, ExecutionRunner, ExecutionSupervisor, ExecutionSupervisorError,
    LocalProcessRunner, SupervisionOutcome,
};
use crate::{CatalogHandle, OrchestrationRuntime};

use super::{
    DEFAULT_QUOTA_EXIT_CODE, DEFAULT_SWARM_SUPERVISION_SLOTS, MAX_SPAWNING_PARENT_SLOTS,
    SwarmDispatchError, member_requires_parent_spawner_slot,
};

/// Classification of a spawned `CLI` exit code at an attempt boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmExitClassification {
    CleanSuccess,
    QuotaExhausted,
    FatalStop,
    TransientCrash,
}

/// Deterministic exit-code policy mapping process exit codes to Swarm member transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmExitPolicy {
    quota_exit_codes: BTreeSet<i32>,
    fatal_stop_exit_codes: BTreeSet<i32>,
}

impl Default for SwarmExitPolicy {
    fn default() -> Self {
        let mut quota_exit_codes = BTreeSet::new();
        quota_exit_codes.insert(DEFAULT_QUOTA_EXIT_CODE);
        Self {
            quota_exit_codes,
            fatal_stop_exit_codes: BTreeSet::new(),
        }
    }
}

impl SwarmExitPolicy {
    /// Add an exit code that triggers `Working -> SuspendedQuota` while preserving `committed_seen_seq`.
    #[must_use]
    pub fn with_quota_exit_code(mut self, exit_code: i32) -> Self {
        if exit_code != 0 {
            self.fatal_stop_exit_codes.remove(&exit_code);
            self.quota_exit_codes.insert(exit_code);
        }
        self
    }

    /// Add an exit code that transitions a failing member directly to `Stopped`.
    #[must_use]
    pub fn with_fatal_stop_exit_code(mut self, exit_code: i32) -> Self {
        if exit_code != 0 {
            self.quota_exit_codes.remove(&exit_code);
            self.fatal_stop_exit_codes.insert(exit_code);
        }
        self
    }

    /// Deterministically classify an observed process `exit_code`.
    #[must_use]
    pub fn classify_exit_code(&self, exit_code: i32) -> SwarmExitClassification {
        if exit_code == 0 {
            SwarmExitClassification::CleanSuccess
        } else if self.quota_exit_codes.contains(&exit_code) {
            SwarmExitClassification::QuotaExhausted
        } else if self.fatal_stop_exit_codes.contains(&exit_code) {
            SwarmExitClassification::FatalStop
        } else {
            SwarmExitClassification::TransientCrash
        }
    }
}

/// Request parameters for dispatching one single-flight `WakeQueued` Swarm member turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmDispatchTurnRequest {
    pub snapshot_request_id: Uuid,
    pub snapshot_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub activity_id: ActivityId,
    pub default_worktree: ForgeWorktreeRef,
    pub lease: LeaseRef,
    pub mcp_grant: Option<McpGrant>,
    pub permissions: ExecutionPermissions,
    pub timeouts: ExecutionTimeouts,
    pub dispatched_at_ms: i64,
    pub clean_exit_output: Option<SwarmTurnOutput>,
}

/// Durable member-state transition applied when a supervised Swarm turn finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwarmTurnDisposition {
    Completed {
        member: SwarmMemberRecord,
        exit_code: i32,
    },
    SuspendedQuota {
        member: SwarmMemberRecord,
        exit_code: i32,
    },
    TransientFailed {
        member: SwarmMemberRecord,
        exit_code: Option<i32>,
        timed_out: bool,
    },
    Stopped {
        member: SwarmMemberRecord,
        exit_code: Option<i32>,
    },
    CancelledWhileWorking {
        member: SwarmMemberRecord,
    },
}

impl SwarmTurnDisposition {
    /// Return the post-turn [`SwarmMemberRecord`].
    #[must_use]
    pub const fn member(&self) -> &SwarmMemberRecord {
        match self {
            Self::Completed { member, .. }
            | Self::SuspendedQuota { member, .. }
            | Self::TransientFailed { member, .. }
            | Self::Stopped { member, .. }
            | Self::CancelledWhileWorking { member } => member,
        }
    }
}

/// Complete receipt returned by [`SwarmRuntimeDispatcher::dispatch_member_turn`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmDispatchedTurnOutcome {
    pub wakeup_batch: SwarmWakeupBatch,
    pub committed_snapshot: AssignmentExecutionSnapshot,
    pub supervision: SupervisionOutcome,
    pub disposition: SwarmTurnDisposition,
    pub used_parent_spawner_slot: bool,
}

/// Single-flight on-demand Swarm doorbell dispatcher enforcing `6/8` parent-slot
/// headroom, lazy pre-spawn [`AssignmentExecutionSnapshot`] freezing, and lossless
/// quota failover.
pub struct SwarmRuntimeDispatcher<R = LocalProcessRunner> {
    pub(super) operations: Arc<Mutex<OperationsCatalog>>,
    pub(super) profiles: Arc<Mutex<AgentProfileCatalog>>,
    owner_pin: Option<Arc<Mutex<rusqlite::Connection>>>,
    pub(super) supervisor: ExecutionSupervisor<R>,
    total_dispatch_slots: Arc<Semaphore>,
    parent_spawner_slots: Arc<Semaphore>,
    inflight_members: Arc<Mutex<BTreeSet<(Uuid, Uuid)>>>,
    exit_policy: SwarmExitPolicy,
    orchestration_backend_addr: Option<SocketAddr>,
    worker_bearer_file: Option<PathBuf>,
    total_slot_capacity: usize,
    parent_slot_capacity: usize,
}

impl<R> Clone for SwarmRuntimeDispatcher<R> {
    fn clone(&self) -> Self {
        Self {
            operations: Arc::clone(&self.operations),
            profiles: Arc::clone(&self.profiles),
            owner_pin: self.owner_pin.as_ref().map(Arc::clone),
            supervisor: self.supervisor.clone(),
            total_dispatch_slots: Arc::clone(&self.total_dispatch_slots),
            parent_spawner_slots: Arc::clone(&self.parent_spawner_slots),
            inflight_members: Arc::clone(&self.inflight_members),
            exit_policy: self.exit_policy.clone(),
            orchestration_backend_addr: self.orchestration_backend_addr,
            worker_bearer_file: self.worker_bearer_file.clone(),
            total_slot_capacity: self.total_slot_capacity,
            parent_slot_capacity: self.parent_slot_capacity,
        }
    }
}

impl<R> std::fmt::Debug for SwarmRuntimeDispatcher<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SwarmRuntimeDispatcher")
            .field("total_slot_capacity", &self.total_slot_capacity)
            .field("parent_slot_capacity", &self.parent_slot_capacity)
            .field(
                "available_total_slots",
                &self.total_dispatch_slots.available_permits(),
            )
            .field(
                "available_parent_slots",
                &self.parent_spawner_slots.available_permits(),
            )
            .field(
                "orchestration_backend_addr",
                &self.orchestration_backend_addr,
            )
            .field(
                "worker_bearer_file",
                &self.worker_bearer_file.as_ref().map(|_| "[REDACTED_PATH]"),
            )
            .finish_non_exhaustive()
    }
}

impl<R: ExecutionRunner + 'static> SwarmRuntimeDispatcher<R> {
    /// Construct a [`SwarmRuntimeDispatcher`] over shared catalog mutexes and an [`ExecutionRunner`],
    /// initialized to `8` total slots and `6` spawning-parent slots (`2` reserved leaf headroom slots).
    #[must_use]
    pub fn new(
        operations: Arc<Mutex<OperationsCatalog>>,
        profiles: Arc<Mutex<AgentProfileCatalog>>,
        runner: R,
    ) -> Self {
        let supervisor = ExecutionSupervisor::new(Arc::clone(&operations), runner);
        Self {
            operations,
            profiles,
            owner_pin: None,
            supervisor,
            total_dispatch_slots: Arc::new(Semaphore::new(DEFAULT_SWARM_SUPERVISION_SLOTS)),
            parent_spawner_slots: Arc::new(Semaphore::new(MAX_SPAWNING_PARENT_SLOTS)),
            inflight_members: Arc::new(Mutex::new(BTreeSet::new())),
            exit_policy: SwarmExitPolicy::default(),
            orchestration_backend_addr: None,
            worker_bearer_file: None,
            total_slot_capacity: DEFAULT_SWARM_SUPERVISION_SLOTS,
            parent_slot_capacity: MAX_SPAWNING_PARENT_SLOTS,
        }
    }

    /// Construct a [`SwarmRuntimeDispatcher`] from runtime [`CatalogHandle`]s, retaining the
    /// data-root owner pin across background tasks.
    #[must_use]
    pub fn from_catalog_handles(
        operations_handle: &CatalogHandle<OperationsCatalog>,
        profiles_handle: &CatalogHandle<AgentProfileCatalog>,
        runner: R,
    ) -> Self {
        let supervisor = ExecutionSupervisor::from_catalog_handle(operations_handle, runner);
        Self {
            operations: Arc::clone(&operations_handle.catalog),
            profiles: Arc::clone(&profiles_handle.catalog),
            owner_pin: Some(Arc::clone(&operations_handle.owner_pin)),
            supervisor,
            total_dispatch_slots: Arc::new(Semaphore::new(DEFAULT_SWARM_SUPERVISION_SLOTS)),
            parent_spawner_slots: Arc::new(Semaphore::new(MAX_SPAWNING_PARENT_SLOTS)),
            inflight_members: Arc::new(Mutex::new(BTreeSet::new())),
            exit_policy: SwarmExitPolicy::default(),
            orchestration_backend_addr: None,
            worker_bearer_file: None,
            total_slot_capacity: DEFAULT_SWARM_SUPERVISION_SLOTS,
            parent_slot_capacity: MAX_SPAWNING_PARENT_SLOTS,
        }
    }

    /// Construct a [`SwarmRuntimeDispatcher`] directly from an [`OrchestrationRuntime`].
    #[must_use]
    pub fn from_runtime(runtime: &OrchestrationRuntime, runner: R) -> Self {
        let ops_handle = runtime.operations_catalog();
        let profiles_handle = runtime.profile_catalog();
        Self::from_catalog_handles(&ops_handle, &profiles_handle, runner)
    }

    /// Override the exit-code classification policy (e.g. custom quota or fatal-stop exit codes).
    #[must_use]
    pub fn with_exit_policy(mut self, exit_policy: SwarmExitPolicy) -> Self {
        self.exit_policy = exit_policy;
        self
    }

    /// Configure the loopback Orchestration HTTP backend socket address forwarded
    /// into spawned worker environments via `HETERARCHY_ORCHESTRATION_BACKEND_ADDR`.
    ///
    /// # Errors
    /// Rejects non-loopback or zero-port addresses.
    pub fn with_orchestration_backend_addr(
        mut self,
        addr: SocketAddr,
    ) -> Result<Self, SwarmDispatchError> {
        let _ = CliExecutionContext::new().with_orchestration_backend_addr(addr)?;
        self.orchestration_backend_addr = Some(addr);
        Ok(self)
    }

    /// Configure a validated bearer-token file path forwarded into spawned worker
    /// environments via `HETERARCHY_ORCHESTRATION_BEARER_FILE`.
    ///
    /// # Errors
    /// Rejects empty, relative, `/Volumes/256.SSD/RustBuild`-scoped, missing,
    /// symlinked, non-regular, zero-byte, or oversized (`> 4096` byte) paths
    /// without reading token bytes.
    pub fn with_worker_bearer_file(
        mut self,
        path: PathBuf,
    ) -> Result<Self, ExecutionSupervisorError> {
        let validated = CliExecutionContext::new().with_bearer_file(path)?;
        self.worker_bearer_file = validated.bearer_file().map(Path::to_path_buf);
        Ok(self)
    }

    /// Optionally configure a validated bearer-token file path forwarded into
    /// spawned worker environments via `HETERARCHY_ORCHESTRATION_BEARER_FILE`.
    ///
    /// # Errors
    /// Propagates [`Self::with_worker_bearer_file`] validation failures when
    /// `Some(path)` is supplied.
    pub fn with_optional_worker_bearer_file(
        self,
        path: Option<PathBuf>,
    ) -> Result<Self, ExecutionSupervisorError> {
        match path {
            Some(path) => self.with_worker_bearer_file(path),
            None => Ok(self),
        }
    }

    /// Configure custom `(total_slots, parent_slots)` capacities while enforcing positive leaf headroom
    /// (`total_slots > parent_slots`) and `parent_slots <= MAX_SPAWNING_PARENT_SLOTS`.
    ///
    /// # Errors
    /// Rejects `parent_slots == 0`, `parent_slots > MAX_SPAWNING_PARENT_SLOTS`, or `total_slots <= parent_slots`.
    pub fn with_slot_limits(
        mut self,
        total_slots: usize,
        parent_slots: usize,
    ) -> Result<Self, SwarmDispatchError> {
        if parent_slots == 0
            || parent_slots > MAX_SPAWNING_PARENT_SLOTS
            || total_slots <= parent_slots
        {
            return Err(SwarmDispatchError::InvalidSlotConfiguration {
                total_slots,
                parent_slots,
            });
        }
        self.supervisor = self
            .supervisor
            .with_max_concurrent_supervisions(total_slots)?;
        self.total_dispatch_slots = Arc::new(Semaphore::new(total_slots));
        self.parent_spawner_slots = Arc::new(Semaphore::new(parent_slots));
        self.total_slot_capacity = total_slots;
        self.parent_slot_capacity = parent_slots;
        Ok(self)
    }

    #[must_use]
    pub const fn supervisor(&self) -> &ExecutionSupervisor<R> {
        &self.supervisor
    }

    #[must_use]
    pub fn worker_bearer_file(&self) -> Option<&Path> {
        self.worker_bearer_file.as_deref()
    }

    #[must_use]
    pub fn available_total_slots(&self) -> usize {
        self.total_dispatch_slots.available_permits()
    }

    #[must_use]
    pub fn available_parent_spawner_slots(&self) -> usize {
        self.parent_spawner_slots.available_permits()
    }

    /// Returns the number of reserved leaf-only slots currently guaranteed immune to
    /// spawning-parent acquisition (`available_total_slots() - available_parent_spawner_slots()`).
    ///
    /// Leaf `Helper`s and `Verifier`s may consume up to [`Self::available_total_slots`], of which
    /// at least these headroom slots cannot be claimed by spawning-capable parents.
    #[must_use]
    pub fn available_leaf_headroom_slots(&self) -> usize {
        self.total_dispatch_slots
            .available_permits()
            .saturating_sub(self.parent_spawner_slots.available_permits())
    }

    /// Dispatch one `WakeQueued` Swarm member turn:
    /// 1. Enforces single-flight per `(swarm_run_id, member_id)`.
    /// 2. Acquires a parent-spawner permit first (if `member_requires_parent_spawner_slot`)
    ///    and then a total supervision permit, keeping at least `2/8` slots free for leaf
    ///    workers and verifiers.
    /// 3. Spawns a drop-safe completion task owning the in-flight guard, slot permits, and
    ///    [`WorkingTurnDropGuard`] so dropping the caller future mid-supervision never strands
    ///    `ops_swarm_members` in `WORKING` or leaks `inflight_members`.
    /// 4. Lazily resolves the live `AgentProfile` `ExecutionPreference`, claims the coalesced
    ///    `(committed_seen_seq, inflight_target_seq]` batch (`<= 32` msgs, `<= 64 KiB`), and
    ///    commits an immutable [`AssignmentExecutionSnapshot`] right before process spawn.
    /// 5. Formats the bounded wakeup delta into [`CliExecutionContext`] and runs
    ///    [`ExecutionSupervisor::supervise_attempt_with_context`] with zero catalog locks held
    ///    across `.await`.
    /// 6. Finalizes two-phase watermarks (`complete_swarm_member_turn` on clean exit `0`,
    ///    or `fail_or_suspend_swarm_member_turn` preserving `committed_seen_seq` on crash,
    ///    timeout, or quota exhaustion).
    ///
    /// # Errors
    /// Rejects duplicate in-flight turns, non-`WakeQueued` members, inactive/missing profiles,
    /// snapshot validation mismatches, spawn/supervision errors, or catalog failures.
    pub async fn dispatch_member_turn(
        &self,
        request: SwarmDispatchTurnRequest,
    ) -> Result<SwarmDispatchedTurnOutcome, SwarmDispatchError> {
        let inflight_guard = InflightMemberGuard::try_acquire(
            &self.inflight_members,
            request.swarm_run_id,
            request.member_id,
        )?;

        let needs_parent_slot = self.inspect_pre_slot_requirement(
            request.workspace_id,
            request.swarm_run_id,
            request.member_id,
        )?;

        let parent_permit = if needs_parent_slot {
            Some(
                Arc::clone(&self.parent_spawner_slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| SwarmDispatchError::SemaphoreClosed)?,
            )
        } else {
            None
        };

        let total_permit = Arc::clone(&self.total_dispatch_slots)
            .acquire_owned()
            .await
            .map_err(|_| SwarmDispatchError::SemaphoreClosed)?;

        let dispatcher = self.clone();
        tokio::spawn(async move {
            dispatcher
                .execute_claimed_turn_owned(
                    request,
                    needs_parent_slot,
                    inflight_guard,
                    parent_permit,
                    total_permit,
                )
                .await
        })
        .await
        .map_err(|_| {
            SwarmDispatchError::Supervisor(ExecutionSupervisorError::BlockingTaskJoinFailed)
        })?
    }

    async fn execute_claimed_turn_owned(
        self,
        request: SwarmDispatchTurnRequest,
        needs_parent_slot: bool,
        inflight_guard: InflightMemberGuard,
        parent_permit: Option<tokio::sync::OwnedSemaphorePermit>,
        total_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<SwarmDispatchedTurnOutcome, SwarmDispatchError> {
        let _keep_inflight = inflight_guard;
        let _keep_parent = parent_permit;
        let _keep_total = total_permit;

        let (wakeup_batch, committed_snapshot) = self.claim_and_seal_snapshot(&request)?;
        let attempt_key = committed_snapshot.attempt_key();
        let mut working_guard = WorkingTurnDropGuard::arm(
            Arc::clone(&self.operations),
            request.workspace_id,
            request.swarm_run_id,
            request.member_id,
            committed_snapshot.snapshot_id(),
            attempt_key,
            request.dispatched_at_ms,
        );
        let supervision_result = match build_swarm_execution_context(
            &request,
            &wakeup_batch,
            self.orchestration_backend_addr,
            self.worker_bearer_file.as_deref(),
        ) {
            Ok(context) => {
                self.supervisor
                    .supervise_attempt_with_context(attempt_key, context)
                    .await
            }
            Err(context_err) => Err(context_err),
        };
        let (supervision, disposition) =
            self.finalize_supervised_turn(&request, &committed_snapshot, supervision_result)?;
        working_guard.disarm();

        Ok(SwarmDispatchedTurnOutcome {
            wakeup_batch,
            committed_snapshot,
            supervision,
            disposition,
            used_parent_spawner_slot: needs_parent_slot,
        })
    }

    pub(super) fn lock_operations(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, OperationsCatalog>, SwarmDispatchError> {
        self.operations
            .lock()
            .map_err(|_| SwarmDispatchError::OperationsCatalogPoisoned)
    }

    pub(super) fn lock_profiles(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, AgentProfileCatalog>, SwarmDispatchError> {
        self.profiles
            .lock()
            .map_err(|_| SwarmDispatchError::ProfileCatalogPoisoned)
    }

    fn inspect_pre_slot_requirement(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        member_id: Uuid,
    ) -> Result<bool, SwarmDispatchError> {
        let catalog = self.lock_operations()?;
        let snapshot = catalog
            .get_swarm_run_snapshot(workspace_id, swarm_run_id)?
            .ok_or(SwarmError::NotFound("swarm run"))?;
        if snapshot.run().state().is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot dispatch member on terminal Swarm run",
            )
            .into());
        }
        let member = snapshot
            .members()
            .iter()
            .find(|candidate| candidate.member_id() == member_id)
            .ok_or(SwarmError::NotFound("swarm member"))?;
        if member.member_state() != SwarmMemberState::WakeQueued {
            return Err(SwarmDispatchError::MemberNotWakeQueued {
                member_id,
                state: member.member_state(),
            });
        }
        Ok(member_requires_parent_spawner_slot(snapshot.run(), member))
    }

    fn resolve_live_profile(
        &self,
        profile_id: AgentProfileId,
    ) -> Result<(ExecutionPreference, AgentProfileRevision), SwarmDispatchError> {
        let guard = self.lock_profiles()?;
        let profile = guard
            .resolve(profile_id)?
            .ok_or(SwarmDispatchError::ProfileMissing(profile_id))?;
        if profile.lifecycle_state() != AgentProfileLifecycleState::Active {
            return Err(SwarmDispatchError::ProfileInactive(profile_id));
        }
        let preference = profile
            .execution_preference()
            .cloned()
            .ok_or(SnapshotValidationError::MissingExecutionPreference)?;
        Ok((preference, profile.revision()))
    }

    pub(super) fn claim_and_seal_snapshot(
        &self,
        request: &SwarmDispatchTurnRequest,
    ) -> Result<(SwarmWakeupBatch, AssignmentExecutionSnapshot), SwarmDispatchError> {
        let (run_mode, profile_id) = {
            let catalog = self.lock_operations()?;
            let run_snap = catalog
                .get_swarm_run_snapshot(request.workspace_id, request.swarm_run_id)?
                .ok_or(SwarmError::NotFound("swarm run"))?;
            let member = run_snap
                .members()
                .iter()
                .find(|candidate| candidate.member_id() == request.member_id)
                .ok_or(SwarmError::NotFound("swarm member"))?;
            if member.member_state() != SwarmMemberState::WakeQueued {
                return Err(SwarmDispatchError::MemberNotWakeQueued {
                    member_id: request.member_id,
                    state: member.member_state(),
                });
            }
            (run_snap.run().mode(), member.identity().profile_id())
        };

        let (preference, profile_revision) = self.resolve_live_profile(profile_id)?;

        let mut catalog = self.lock_operations()?;
        let wakeup_batch = catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
            workspace_id: request.workspace_id,
            swarm_run_id: request.swarm_run_id,
            member_id: request.member_id,
            snapshot_id: request.snapshot_id,
            claimed_at_ms: request.dispatched_at_ms,
        })?;

        let seal_result = build_and_commit_member_snapshot(
            &mut catalog,
            request,
            run_mode,
            wakeup_batch.member(),
            &preference,
            profile_revision,
        );

        match seal_result {
            Ok(committed_snapshot) => Ok((wakeup_batch, committed_snapshot)),
            Err(seal_err) => {
                let _ = catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
                    workspace_id: request.workspace_id,
                    swarm_run_id: request.swarm_run_id,
                    member_id: request.member_id,
                    expected_snapshot_id: request.snapshot_id,
                    disposition: SwarmTurnFailureDisposition::TransientRetry,
                    consume_turn: false,
                    observed_at_ms: request.dispatched_at_ms,
                });
                Err(seal_err)
            }
        }
    }

    fn finalize_supervised_turn(
        &self,
        request: &SwarmDispatchTurnRequest,
        snapshot: &AssignmentExecutionSnapshot,
        supervision_result: Result<SupervisionOutcome, ExecutionSupervisorError>,
    ) -> Result<(SupervisionOutcome, SwarmTurnDisposition), SwarmDispatchError> {
        match supervision_result {
            Ok(outcome) => {
                let observed_at_ms = outcome
                    .ledger()
                    .updated_at_ms()
                    .max(request.dispatched_at_ms);
                let mut catalog = self.lock_operations()?;
                if let Some(killed) = find_concurrently_killed_member(
                    &catalog,
                    request.workspace_id,
                    request.swarm_run_id,
                    request.member_id,
                )? {
                    return Ok((
                        outcome,
                        SwarmTurnDisposition::CancelledWhileWorking { member: killed },
                    ));
                }
                let disposition = self.apply_outcome_to_catalog(
                    &mut catalog,
                    request,
                    snapshot.snapshot_id(),
                    &outcome,
                    observed_at_ms,
                )?;
                Ok((outcome, disposition))
            }
            Err(supervisor_err) => {
                let failure_disposition = if matches!(
                    supervisor_err,
                    ExecutionSupervisorError::OwnershipMismatch { .. }
                ) {
                    SwarmTurnFailureDisposition::Stopped
                } else {
                    SwarmTurnFailureDisposition::TransientRetry
                };
                if let Ok(mut catalog) = self.lock_operations() {
                    let _ =
                        catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
                            workspace_id: request.workspace_id,
                            swarm_run_id: request.swarm_run_id,
                            member_id: request.member_id,
                            expected_snapshot_id: snapshot.snapshot_id(),
                            disposition: failure_disposition,
                            consume_turn: false,
                            observed_at_ms: request.dispatched_at_ms,
                        });
                }
                Err(SwarmDispatchError::Supervisor(supervisor_err))
            }
        }
    }

    fn apply_outcome_to_catalog(
        &self,
        catalog: &mut OperationsCatalog,
        request: &SwarmDispatchTurnRequest,
        snapshot_id: Uuid,
        outcome: &SupervisionOutcome,
        observed_at_ms: i64,
    ) -> Result<SwarmTurnDisposition, SwarmDispatchError> {
        match outcome {
            SupervisionOutcome::Exited { exit_code, .. } => self.apply_exited_outcome_to_catalog(
                catalog,
                request,
                snapshot_id,
                *exit_code,
                observed_at_ms,
            ),
            SupervisionOutcome::TimedOut { .. } => {
                let member =
                    catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
                        workspace_id: request.workspace_id,
                        swarm_run_id: request.swarm_run_id,
                        member_id: request.member_id,
                        expected_snapshot_id: snapshot_id,
                        disposition: SwarmTurnFailureDisposition::TransientRetry,
                        consume_turn: true,
                        observed_at_ms,
                    })?;
                Ok(SwarmTurnDisposition::TransientFailed {
                    member,
                    exit_code: None,
                    timed_out: true,
                })
            }
            SupervisionOutcome::Unknown { reason, .. } => {
                let disposition = match reason {
                    LaunchUnknownReason::ExecutionTimedOut => {
                        SwarmTurnFailureDisposition::TransientRetry
                    }
                    LaunchUnknownReason::BootRecoveryAfterSpawn
                    | LaunchUnknownReason::OwnershipMismatch
                    | LaunchUnknownReason::ForcedTermination => {
                        SwarmTurnFailureDisposition::Stopped
                    }
                };
                let member =
                    catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
                        workspace_id: request.workspace_id,
                        swarm_run_id: request.swarm_run_id,
                        member_id: request.member_id,
                        expected_snapshot_id: snapshot_id,
                        disposition,
                        consume_turn: false,
                        observed_at_ms,
                    })?;
                if matches!(disposition, SwarmTurnFailureDisposition::TransientRetry) {
                    Ok(SwarmTurnDisposition::TransientFailed {
                        member,
                        exit_code: None,
                        timed_out: true,
                    })
                } else {
                    Ok(SwarmTurnDisposition::Stopped {
                        member,
                        exit_code: None,
                    })
                }
            }
        }
    }

    fn apply_exited_outcome_to_catalog(
        &self,
        catalog: &mut OperationsCatalog,
        request: &SwarmDispatchTurnRequest,
        snapshot_id: Uuid,
        exit_code: i32,
        observed_at_ms: i64,
    ) -> Result<SwarmTurnDisposition, SwarmDispatchError> {
        match self.exit_policy.classify_exit_code(exit_code) {
            SwarmExitClassification::CleanSuccess => {
                let member = catalog.complete_swarm_member_turn(&CompleteSwarmTurnCommand {
                    workspace_id: request.workspace_id,
                    swarm_run_id: request.swarm_run_id,
                    member_id: request.member_id,
                    expected_snapshot_id: snapshot_id,
                    output: request.clean_exit_output.clone(),
                    completed_at_ms: observed_at_ms,
                })?;
                Ok(SwarmTurnDisposition::Completed { member, exit_code })
            }
            SwarmExitClassification::QuotaExhausted => {
                let member =
                    catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
                        workspace_id: request.workspace_id,
                        swarm_run_id: request.swarm_run_id,
                        member_id: request.member_id,
                        expected_snapshot_id: snapshot_id,
                        disposition: SwarmTurnFailureDisposition::QuotaExceeded,
                        consume_turn: false,
                        observed_at_ms,
                    })?;
                Ok(SwarmTurnDisposition::SuspendedQuota { member, exit_code })
            }
            SwarmExitClassification::FatalStop => {
                let member =
                    catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
                        workspace_id: request.workspace_id,
                        swarm_run_id: request.swarm_run_id,
                        member_id: request.member_id,
                        expected_snapshot_id: snapshot_id,
                        disposition: SwarmTurnFailureDisposition::Stopped,
                        consume_turn: true,
                        observed_at_ms,
                    })?;
                Ok(SwarmTurnDisposition::Stopped {
                    member,
                    exit_code: Some(exit_code),
                })
            }
            SwarmExitClassification::TransientCrash => {
                let member =
                    catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
                        workspace_id: request.workspace_id,
                        swarm_run_id: request.swarm_run_id,
                        member_id: request.member_id,
                        expected_snapshot_id: snapshot_id,
                        disposition: SwarmTurnFailureDisposition::TransientRetry,
                        consume_turn: true,
                        observed_at_ms,
                    })?;
                Ok(SwarmTurnDisposition::TransientFailed {
                    member,
                    exit_code: Some(exit_code),
                    timed_out: false,
                })
            }
        }
    }
}

fn build_and_commit_member_snapshot(
    catalog: &mut OperationsCatalog,
    request: &SwarmDispatchTurnRequest,
    run_mode: SwarmRunMode,
    claimed_member: &SwarmMemberRecord,
    preference: &ExecutionPreference,
    profile_revision: AgentProfileRevision,
) -> Result<AssignmentExecutionSnapshot, SwarmDispatchError> {
    let attempt = u32::try_from(claimed_member.attempt_epoch()).map_err(|_| {
        SnapshotValidationError::InvalidIdentity("swarm member attempt_epoch exceeds u32::MAX")
    })?;
    let (goal_id, dag_revision, node_id) = match run_mode {
        SwarmRunMode::DagNodeHybrid {
            goal_id,
            dag_revision,
            node_id,
            ..
        } => (goal_id, dag_revision, node_id),
        SwarmRunMode::Standalone => (
            GoalId::from_uuid(request.swarm_run_id),
            DagRevisionNumber::INITIAL,
            DagNodeId::from_uuid(request.swarm_run_id),
        ),
    };
    let attempt_key = ExecutionAttemptKey::new(
        request.workspace_id,
        goal_id,
        dag_revision,
        node_id,
        AgentAssignmentId::from_uuid(request.member_id),
        attempt,
    )?;
    let worktree = claimed_member
        .worktree_path()
        .cloned()
        .unwrap_or_else(|| request.default_worktree.clone());
    let allow_write = request.permissions.allow_workspace_write()
        && claimed_member.workspace_mode() != SwarmWorkspaceMode::Inherit
        && claimed_member.role_kind() != SwarmRoleKind::Verifier;
    let effective_permissions = ExecutionPermissions::new(
        allow_write,
        request.permissions.allow_local_shell(),
        request.permissions.allow_network(),
    );
    let parts = AssignmentExecutionSnapshotParts {
        snapshot_id: request.snapshot_id,
        attempt_key,
        activity_id: request.activity_id,
        profile_id: claimed_member.identity().profile_id(),
        profile_revision,
        resolved_engine: preference.cli_engine(),
        resolved_model: preference.preferred_model().to_owned(),
        resolved_reasoning: preference.preferred_reasoning().map(str::to_owned),
        role_prompt_digest: preference.role_prompt_ref(),
        worktree,
        lease: request.lease,
        mcp_grant: request.mcp_grant,
        permissions: effective_permissions,
        timeouts: request.timeouts,
        committed_at_ms: request.dispatched_at_ms,
    };
    let snapshot = AssignmentExecutionSnapshot::resolve_from_profile(
        parts,
        Some(preference),
        profile_revision,
    )?;
    let (committed, _ledger) =
        catalog.commit_assignment_execution_snapshot(request.snapshot_request_id, &snapshot)?;
    Ok(committed)
}

fn find_concurrently_killed_member(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    member_id: Uuid,
) -> Result<Option<SwarmMemberRecord>, SwarmError> {
    let Some(snapshot) = catalog.get_swarm_run_snapshot(workspace_id, swarm_run_id)? else {
        return Ok(None);
    };
    Ok(snapshot
        .members()
        .iter()
        .find(|m| m.member_id() == member_id && m.member_state() == SwarmMemberState::Killed)
        .cloned())
}
