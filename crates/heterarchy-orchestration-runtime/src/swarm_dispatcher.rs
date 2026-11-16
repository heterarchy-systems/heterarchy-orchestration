//! Runtime single-flight Swarm doorbell dispatcher, `6/8` parent-slot headroom
//! scheduler, attempt-boundary lossless quota failover, zero-token deterministic
//! stall/livelock monitor, and `SWARM_SETTLED_KIND = 6` outbox consumer bridge.
//!
//! # Concurrency & Lock-Discipline Invariants
//! 1. **Parent-Slot Headroom (`6/8`)**: Total concurrent `CLI` supervisions are
//!    bounded by [`DEFAULT_SWARM_SUPERVISION_SLOTS`] (`8`), while spawning-capable
//!    parents (`Coordinator`, and `Worker`/`Helper` below `max_spawn_depth`) must
//!    additionally acquire [`MAX_SPAWNING_PARENT_SLOTS`] (`6`) before claiming a
//!    supervision slot. At least [`MIN_LEAF_HEADROOM_SLOTS`] (`2`) slots remain
//!    permanently available to leaf `Helper`s and `Verifier`s, preventing
//!    parent-child starvation deadlocks.
//! 2. **Lazy Just-Before-Spawn Snapshot Sealing**: While queued in `WakeQueued`,
//!    members hold no frozen [`AssignmentExecutionSnapshot`], allowing incoming
//!    `@mention` doorbells to coalesce (`<= 32` messages, `<= 64 KiB`) and
//!    allowing profile model updates. Only after slot acquisition does the
//!    dispatcher resolve the live `AgentProfile`, claim `(committed_seen_seq,
//!    inflight_target_seq]`, and commit the immutable snapshot.
//! 3. **Strict Lock Discipline**: `Mutex<OperationsCatalog>` and
//!    `Mutex<AgentProfileCatalog>` are never held simultaneously and are never
//!    held across `.await`, semaphore waits, process spawns, sleeps, or signals.

mod dispatcher;
mod outbox_bridge;
mod quota_and_recovery;
mod runtime_facade;
mod stall_monitor;
#[cfg(test)]
mod tests;

use heterarchy_orchestration_operations::{
    MAX_SWARM_SPAWN_DEPTH, OperationsError, SnapshotValidationError, SwarmError, SwarmMemberRecord,
    SwarmMemberState, SwarmRoleKind, SwarmRunRecord,
};
use heterarchy_orchestration_sessions::{AgentProfileError, AgentProfileId};
use thiserror::Error;
use uuid::Uuid;

use crate::execution_supervisor::ExecutionSupervisorError;

pub use dispatcher::{
    SwarmDispatchTurnRequest, SwarmDispatchedTurnOutcome, SwarmExitClassification, SwarmExitPolicy,
    SwarmRuntimeDispatcher, SwarmTurnDisposition,
};
pub use outbox_bridge::{
    AutonomyOutboxApplyReport, AutonomyOutboxBridge, AutonomyOutboxFactKind,
    AutonomySettlementOutboxFact, SwarmSettledFact, SwarmSettledProjectionReceipt,
};
pub use quota_and_recovery::{
    SwarmBootRecoveryReport, SwarmModelSwitchOutcome, SwarmModelSwitchTarget,
    SwitchSwarmMemberModelCommand,
};
pub use stall_monitor::{
    SwarmBottleneckReason, SwarmLivelockReason, SwarmMonitorActionReport, SwarmMonitorDiagnosis,
    SwarmStallMonitor, SwarmStallMonitorPolicy,
};

/// Default total concurrent `CLI` executions supervised by [`SwarmRuntimeDispatcher`].
pub const DEFAULT_SWARM_SUPERVISION_SLOTS: usize = 8;
/// Maximum slots simultaneously usable by spawning-capable parent members (`6/8`).
pub const MAX_SPAWNING_PARENT_SLOTS: usize = 6;
/// Minimum execution slots reserved exclusively for leaf `Helper`s, `Verifier`s, and `Monitor`s.
pub const MIN_LEAF_HEADROOM_SLOTS: usize =
    DEFAULT_SWARM_SUPERVISION_SLOTS - MAX_SPAWNING_PARENT_SLOTS;
/// Default Unix exit code (`EX_TEMPFAIL = 75`) classified as provider quota/rate-limit exhaustion.
pub const DEFAULT_QUOTA_EXIT_CODE: i32 = 75;
/// Default inactivity grace window (`30s`) before [`SwarmStallMonitor`] emits a `[stall]` nudge.
pub const DEFAULT_STALL_GRACE_MS: i64 = 30_000;
/// Default maximum `[stall]` nudges emitted before escalating an unprogressed Swarm run to `Aborted`.
pub const DEFAULT_MAX_STALL_NUDGES: u32 = 3;
/// Default consecutive non-progressing direct ping-pong threshold before livelock intervention.
pub const DEFAULT_MAX_PING_PONG_STREAK: u32 = 4;
/// Default per-member turn cap before [`SwarmStallMonitor`] halts a non-progressing member.
pub const DEFAULT_MAX_MEMBER_TURNS: u64 = 32;
/// Canonical Forge `effect_fact_outbox_entries.fact_kind` tag for `DagSettled` facts.
pub const DAG_SETTLED_KIND: i64 = 5;
/// Canonical Forge `effect_fact_outbox_entries.fact_kind` tag for `StandaloneSwarm` `SwarmSettled` facts.
pub const SWARM_SETTLED_KIND: i64 = 6;

/// Typed errors produced by [`SwarmRuntimeDispatcher`] and [`AutonomyOutboxBridge`].
#[derive(Debug, Error)]
pub enum SwarmDispatchError {
    #[error("operations catalog mutex is poisoned")]
    OperationsCatalogPoisoned,
    #[error("agent profile catalog mutex is poisoned")]
    ProfileCatalogPoisoned,
    #[error("in-flight member tracking mutex is poisoned")]
    InflightGuardPoisoned,
    #[error("swarm dispatch concurrency semaphore is closed")]
    SemaphoreClosed,
    #[error(
        "invalid swarm slot configuration (total_slots={total_slots}, parent_slots={parent_slots}): requires parent_slots in 1..={MAX_SPAWNING_PARENT_SLOTS} and total_slots > parent_slots"
    )]
    InvalidSlotConfiguration {
        total_slots: usize,
        parent_slots: usize,
    },
    #[error(
        "swarm member {member_id} in run {swarm_run_id} already has a single-flight turn running"
    )]
    MemberAlreadyInFlight { swarm_run_id: Uuid, member_id: Uuid },
    #[error("swarm member {member_id} is in state {state:?}, expected WakeQueued")]
    MemberNotWakeQueued {
        member_id: Uuid,
        state: SwarmMemberState,
    },
    #[error("agent profile {0} does not exist")]
    ProfileMissing(AgentProfileId),
    #[error("agent profile {0} is not Active")]
    ProfileInactive(AgentProfileId),
    #[error("unknown outbox fact_kind {0}")]
    UnsupportedOutboxFactKind(i64),
    #[error("outbox destination mismatch: expected {expected}, got {actual}")]
    OutboxDestinationMismatch { expected: Uuid, actual: Uuid },
    #[error(
        "non-monotonic swarm settled outbox sequence for migration {migration_id}: last={last_sequence}, got={got_sequence}"
    )]
    OutboxSequenceRegression {
        migration_id: Uuid,
        last_sequence: u64,
        got_sequence: u64,
    },
    #[error("swarm settled outbox digest does not match converged Swarm run state")]
    OutboxDigestMismatch,
    #[error("forge swarm bridge error: {0}")]
    ForgeBridge(&'static str),
    #[error("snapshot validation failed: {0}")]
    SnapshotValidation(#[from] SnapshotValidationError),
    #[error("agent profile catalog error: {0}")]
    ProfileCatalog(#[from] AgentProfileError),
    #[error("swarm domain error: {0}")]
    Swarm(#[from] SwarmError),
    #[error("operations catalog error: {0}")]
    Operations(#[from] OperationsError),
    #[error("execution supervisor error: {0}")]
    Supervisor(#[from] ExecutionSupervisorError),
}

/// Returns `true` when `member` is capable of spawning child members in `run`
/// and therefore must acquire a bounded [`MAX_SPAWNING_PARENT_SLOTS`] permit
/// before claiming a supervision slot.
///
/// Leaf `Helper`s (`spawn_depth >= effective_max_depth`), leaf `Worker`s when
/// `max_spawn_depth == 0`, `Verifier`s, and `Monitor`s return `false` so they
/// can always use reserved leaf headroom slots without parent-child deadlock.
#[must_use]
pub fn member_requires_parent_spawner_slot(
    run: &SwarmRunRecord,
    member: &SwarmMemberRecord,
) -> bool {
    let effective_max_depth = run.max_spawn_depth().min(MAX_SWARM_SPAWN_DEPTH);
    match member.role_kind() {
        SwarmRoleKind::Coordinator => true,
        SwarmRoleKind::Verifier | SwarmRoleKind::Monitor => false,
        SwarmRoleKind::Worker | SwarmRoleKind::Helper => member.spawn_depth() < effective_max_depth,
    }
}
