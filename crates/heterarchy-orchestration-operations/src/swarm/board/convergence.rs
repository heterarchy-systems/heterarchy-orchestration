//! Swarm run convergence, standalone settlement gating, and abort transitions.

use forge_domain::WorkspaceId;
use rusqlite::{Transaction, TransactionBehavior, params};
use uuid::Uuid;

use super::super::sql::{
    UPDATE_SWARM_MEMBER_KILL_SQL, UPDATE_SWARM_RUN_ABORTED_SQL,
    UPDATE_SWARM_RUN_CANDIDATE_READY_SQL, UPDATE_SWARM_RUN_CONVERGING_SQL,
    UPDATE_SWARM_RUN_SETTLED_SQL,
};
use super::super::{
    SwarmError, check_timestamp_non_negative, validate_non_nil_uuid, validate_non_zero_digest,
    validate_workspace_id,
};
use super::codecs::{load_members_by_run, require_run_by_id};
use super::{
    RecordSwarmConvergenceCommand, SettleStandaloneSwarmCommand, SwarmMemberState, SwarmRunMode,
    SwarmRunRecord, SwarmRunState,
};
use crate::{OperationsCatalog, OperationsError};

impl OperationsCatalog {
    /// Transition an `Active` Swarm run to `Converging`.
    ///
    /// # Errors
    /// Rejects non-`Active` runs or storage failures.
    pub fn mark_swarm_converging(
        &mut self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<SwarmRunRecord, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        check_timestamp_non_negative(observed_at_ms, "swarm observed_at_ms")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, workspace_id, swarm_run_id)?;
        if observed_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm observed_at_ms precedes run creation").into(),
            );
        }
        if run.state != SwarmRunState::Active {
            return Err(SwarmError::InvalidStateTransition(
                "only Active Swarm run can transition to Converging",
            ));
        }
        let changed = tx
            .execute(
                UPDATE_SWARM_RUN_CONVERGING_SQL,
                params![
                    workspace_id.into_uuid().as_bytes(),
                    swarm_run_id.as_bytes(),
                    observed_at_ms,
                    crate::catalog::signed(run.revision)?,
                ],
            )
            .map_err(OperationsError::from)?;
        if changed != 1 {
            return Err(SwarmError::InvalidStateTransition(
                "swarm converging CAS mismatch",
            ));
        }
        let updated = require_run_by_id(&tx, workspace_id, swarm_run_id)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(updated)
    }

    /// Record a converged candidate commit/tree/diff digest on a Swarm run and transition it to `CandidateReady`.
    /// Both `Standalone` and `DagNodeHybrid` runs stop at `CandidateReady` here; `DagNodeHybrid` runs
    /// remain at `CandidateReady` for parent DAG node verification and settlement.
    ///
    /// # Errors
    /// Rejects terminal (`Settled`/`Aborted`) runs, zero digests, or storage failures.
    pub fn record_swarm_convergence_candidate(
        &mut self,
        command: &RecordSwarmConvergenceCommand,
    ) -> Result<SwarmRunRecord, SwarmError> {
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_zero_digest(command.converged_tree_blake3, "swarm converged_tree_blake3")?;
        validate_non_zero_digest(command.converged_diff_digest, "swarm converged_diff_digest")?;
        check_timestamp_non_negative(command.recorded_at_ms, "swarm recorded_at_ms")?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if command.recorded_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm recorded_at_ms precedes run creation").into(),
            );
        }
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot record convergence candidate on terminal Swarm run",
            ));
        }
        let changed = tx
            .execute(
                UPDATE_SWARM_RUN_CANDIDATE_READY_SQL,
                params![
                    command.workspace_id.into_uuid().as_bytes(),
                    command.swarm_run_id.as_bytes(),
                    command.converged_commit_oid.as_str(),
                    command.converged_tree_blake3.as_slice(),
                    command.converged_diff_digest.as_slice(),
                    command.recorded_at_ms,
                    crate::catalog::signed(run.revision)?,
                ],
            )
            .map_err(OperationsError::from)?;
        if changed != 1 {
            return Err(SwarmError::InvalidStateTransition(
                "swarm convergence candidate CAS mismatch",
            ));
        }
        let updated = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(updated)
    }

    /// Abort a non-terminal Swarm run and cascade-kill all active members.
    ///
    /// # Errors
    /// Rejects `Settled` runs or storage failures.
    pub fn abort_swarm_run(
        &mut self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<SwarmRunRecord, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        check_timestamp_non_negative(observed_at_ms, "swarm observed_at_ms")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, workspace_id, swarm_run_id)?;
        if run.state == SwarmRunState::Settled {
            return Err(SwarmError::InvalidStateTransition(
                "cannot abort an already Settled Swarm run",
            ));
        }
        if run.state == SwarmRunState::Aborted {
            tx.commit().map_err(OperationsError::from)?;
            return Ok(run);
        }
        if observed_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm observed_at_ms precedes run creation").into(),
            );
        }
        let aborted = abort_run_in_tx(&tx, &run, observed_at_ms)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(aborted)
    }

    /// Settle a `Standalone` Swarm run after verifying the adversarial verifier quorum gate
    /// and matching `converged_diff_digest`. `DagNodeHybrid` runs fail closed and cannot settle directly.
    ///
    /// # Errors
    /// Fails closed with [`SwarmError::DagNodeHybridCannotSettleDirectly`],
    /// [`SwarmError::VerdictDiffDigestMismatch`], [`SwarmError::ReviewNotAccepted`],
    /// [`SwarmError::SybilLineageViolation`], or [`SwarmError::InvalidStateTransition`].
    pub fn settle_standalone_swarm_run(
        &mut self,
        command: SettleStandaloneSwarmCommand,
    ) -> Result<SwarmRunRecord, SwarmError> {
        validate_non_nil_uuid(command.request_id, "swarm settlement request_id")?;
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        check_timestamp_non_negative(command.settled_at_ms, "swarm settled_at_ms")?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if !matches!(run.mode, SwarmRunMode::Standalone) {
            return Err(SwarmError::DagNodeHybridCannotSettleDirectly);
        }
        if run.state == SwarmRunState::Settled {
            if run.settlement_request_id == Some(command.request_id) {
                let mut replayed = run;
                replayed.replayed = true;
                tx.commit().map_err(OperationsError::from)?;
                return Ok(replayed);
            }
            return Err(OperationsError::Conflict(
                "swarm run already settled under another request_id",
            )
            .into());
        }
        if run.state != SwarmRunState::CandidateReady {
            return Err(SwarmError::InvalidStateTransition(
                "only CandidateReady Standalone Swarm run can settle",
            ));
        }
        if command.settled_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm settled_at_ms precedes run creation").into(),
            );
        }
        let _gate = super::super::verifier_panel::verify_swarm_convergence_gate_in_tx(&tx, &run)?;
        let changed = tx
            .execute(
                UPDATE_SWARM_RUN_SETTLED_SQL,
                params![
                    command.workspace_id.into_uuid().as_bytes(),
                    command.swarm_run_id.as_bytes(),
                    command.request_id.as_bytes(),
                    command.settled_at_ms,
                    crate::catalog::signed(run.revision)?,
                ],
            )
            .map_err(OperationsError::from)?;
        if changed != 1 {
            return Err(SwarmError::InvalidStateTransition(
                "settle_standalone_swarm_run CAS mismatch",
            ));
        }
        let settled = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(settled)
    }
}

pub(super) fn abort_run_in_tx(
    tx: &Transaction<'_>,
    run: &SwarmRunRecord,
    observed_at_ms: i64,
) -> Result<SwarmRunRecord, SwarmError> {
    let changed = tx
        .execute(
            UPDATE_SWARM_RUN_ABORTED_SQL,
            params![
                run.workspace_id.into_uuid().as_bytes(),
                run.swarm_run_id.as_bytes(),
                observed_at_ms,
                crate::catalog::signed(run.revision)?,
            ],
        )
        .map_err(OperationsError::from)?;
    if changed != 1 {
        return Err(SwarmError::InvalidStateTransition(
            "abort_swarm_run CAS mismatch",
        ));
    }
    let members = load_members_by_run(tx, run.swarm_run_id)?;
    for member in members {
        if member.member_state != SwarmMemberState::Killed {
            tx.execute(
                UPDATE_SWARM_MEMBER_KILL_SQL,
                params![
                    run.swarm_run_id.as_bytes(),
                    member.member_id.as_bytes(),
                    observed_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        }
    }
    require_run_by_id(tx, run.workspace_id, run.swarm_run_id)
}
