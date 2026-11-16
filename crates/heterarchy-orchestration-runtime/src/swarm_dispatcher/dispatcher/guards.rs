//! Synchronous RAII guards enforcing single-flight member dispatch and
//! drop-safe `WORKING` turn recovery when caller futures panic or abort.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::{
    ExecutionAttemptKey, FailOrSuspendSwarmTurnCommand, LaunchLedgerState, OperationsCatalog,
    SwarmTurnFailureDisposition,
};
use uuid::Uuid;

use super::super::SwarmDispatchError;

pub(super) struct InflightMemberGuard {
    inflight: Arc<Mutex<BTreeSet<(Uuid, Uuid)>>>,
    key: (Uuid, Uuid),
}

impl InflightMemberGuard {
    pub(super) fn try_acquire(
        inflight: &Arc<Mutex<BTreeSet<(Uuid, Uuid)>>>,
        swarm_run_id: Uuid,
        member_id: Uuid,
    ) -> Result<Self, SwarmDispatchError> {
        let key = (swarm_run_id, member_id);
        let mut guard = inflight
            .lock()
            .map_err(|_| SwarmDispatchError::InflightGuardPoisoned)?;
        if !guard.insert(key) {
            return Err(SwarmDispatchError::MemberAlreadyInFlight {
                swarm_run_id,
                member_id,
            });
        }
        drop(guard);
        Ok(Self {
            inflight: Arc::clone(inflight),
            key,
        })
    }
}

impl Drop for InflightMemberGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.inflight.lock() {
            guard.remove(&self.key);
        }
    }
}

/// Synchronous RAII safety net armed immediately after `claim_and_seal_snapshot` transitions a
/// member into `WORKING`. If the supervision task panics, fails before catalog reconciliation, or
/// is aborted during Tokio runtime shutdown, `Drop` synchronously transitions the member out of
/// `WORKING` (`Prepared`/`NotStarted` -> `TransientRetry`, `Spawned`/other -> `Stopped`) while
/// preserving `committed_seen_seq`.
pub(in crate::swarm_dispatcher) struct WorkingTurnDropGuard {
    operations: Arc<Mutex<OperationsCatalog>>,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    member_id: Uuid,
    snapshot_id: Uuid,
    attempt_key: ExecutionAttemptKey,
    dispatched_at_ms: i64,
    armed: bool,
}

impl WorkingTurnDropGuard {
    pub(in crate::swarm_dispatcher) fn arm(
        operations: Arc<Mutex<OperationsCatalog>>,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        member_id: Uuid,
        snapshot_id: Uuid,
        attempt_key: ExecutionAttemptKey,
        dispatched_at_ms: i64,
    ) -> Self {
        Self {
            operations,
            workspace_id,
            swarm_run_id,
            member_id,
            snapshot_id,
            attempt_key,
            dispatched_at_ms,
            armed: true,
        }
    }

    pub(super) const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WorkingTurnDropGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(mut catalog) = self.operations.lock() else {
            return;
        };
        let (disposition, observed_at_ms) =
            match catalog.get_assignment_execution_snapshot(self.attempt_key) {
                Ok(Some((_, ledger))) => {
                    let disp = if matches!(
                        ledger.state(),
                        LaunchLedgerState::Prepared | LaunchLedgerState::NotStarted { .. }
                    ) {
                        SwarmTurnFailureDisposition::TransientRetry
                    } else {
                        SwarmTurnFailureDisposition::Stopped
                    };
                    (disp, ledger.updated_at_ms().max(self.dispatched_at_ms))
                }
                _ => (SwarmTurnFailureDisposition::Stopped, self.dispatched_at_ms),
            };
        let _ = catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
            workspace_id: self.workspace_id,
            swarm_run_id: self.swarm_run_id,
            member_id: self.member_id,
            expected_snapshot_id: self.snapshot_id,
            disposition,
            consume_turn: false,
            observed_at_ms,
        });
    }
}
