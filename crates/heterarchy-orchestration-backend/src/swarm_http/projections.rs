//! Inspection snapshot assembly, convergence quorum evaluation, and stall-policy
//! helpers for Swarm HTTP handlers.

use std::collections::BTreeSet;

use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::{
    RecordSwarmConvergenceCommand, ReviewVerdictOutcome, SettleStandaloneSwarmCommand,
    SwarmCommitOid, SwarmConvergenceGateOutcome, SwarmError, SwarmRunMode, SwarmRunRecord,
    SwarmRunState, SwarmVerdictRecord, SwarmVerificationPanelRecord,
};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError, SwarmStallMonitorPolicy,
};
use uuid::Uuid;

use crate::wire::swarm::{
    SwarmConvergenceTarget, SwarmRunInspectionResponse, SwarmStallCheckRequest,
};
use crate::{ApiError, current_timestamp_millis};

pub(super) struct ParsedConvergenceInput {
    pub(super) request_id: Uuid,
    pub(super) workspace_id: WorkspaceId,
    pub(super) swarm_run_id: Uuid,
    pub(super) converged_commit_oid: SwarmCommitOid,
    pub(super) converged_tree_blake3: [u8; 32],
    pub(super) converged_diff_digest: [u8; 32],
    pub(super) target: Option<SwarmConvergenceTarget>,
    pub(super) stage_candidate_only: bool,
    pub(super) observed_at_ms: i64,
}

pub(super) type ConvergenceExecutionOutcome =
    Result<(SwarmRunRecord, bool, Option<SwarmConvergenceGateOutcome>), SwarmError>;

pub(super) fn load_full_inspection(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
) -> Result<Result<Option<SwarmRunInspectionResponse>, SwarmError>, RuntimeError> {
    let snapshot = match runtime.get_swarm_run_snapshot(workspace_id, swarm_run_id)? {
        Ok(Some(snap)) => snap,
        Ok(None) => return Ok(Ok(None)),
        Err(err) => return Ok(Err(err)),
    };
    let panel = match runtime.get_swarm_verification_panel(workspace_id, swarm_run_id, None)? {
        Ok(panel) => panel,
        Err(err) => return Ok(Err(err)),
    };
    let verdicts = if let Some(active_panel) = &panel {
        match runtime.list_swarm_verdicts(workspace_id, swarm_run_id, active_panel.panel_id())? {
            Ok(items) => items,
            Err(err) => return Ok(Err(err)),
        }
    } else {
        Vec::new()
    };
    Ok(Ok(Some(SwarmRunInspectionResponse::from_parts(
        &snapshot,
        panel.as_ref(),
        &verdicts,
    ))))
}

pub(super) fn execute_converge_swarm(
    runtime: &OrchestrationRuntime,
    input: &ParsedConvergenceInput,
) -> Result<ConvergenceExecutionOutcome, RuntimeError> {
    let existing = match runtime.get_swarm_run(input.workspace_id, input.swarm_run_id)? {
        Ok(Some(run)) => run,
        Ok(None) => return Ok(Err(SwarmError::NotFound("swarm run"))),
        Err(err) => return Ok(Err(err)),
    };

    if input.target == Some(SwarmConvergenceTarget::Settled)
        && matches!(existing.mode(), SwarmRunMode::DagNodeHybrid { .. })
    {
        return Ok(Err(SwarmError::DagNodeHybridCannotSettleDirectly));
    }

    if existing.state() == SwarmRunState::Settled {
        return replay_settled_convergence(runtime, &existing, input);
    }

    let candidate_cmd = RecordSwarmConvergenceCommand {
        workspace_id: input.workspace_id,
        swarm_run_id: input.swarm_run_id,
        converged_commit_oid: input.converged_commit_oid.clone(),
        converged_tree_blake3: input.converged_tree_blake3,
        converged_diff_digest: input.converged_diff_digest,
        recorded_at_ms: input.observed_at_ms,
    };

    if input.stage_candidate_only {
        return match runtime.record_swarm_convergence_candidate(&candidate_cmd)? {
            Ok(run) => Ok(Ok((run, false, None))),
            Err(err) => Ok(Err(err)),
        };
    }

    if let Err(gate_err) = precheck_convergence_quorum(
        runtime,
        input.workspace_id,
        input.swarm_run_id,
        input.converged_diff_digest,
    )? {
        return Ok(Err(gate_err));
    }

    let candidate_run = match runtime.record_swarm_convergence_candidate(&candidate_cmd)? {
        Ok(run) => run,
        Err(err) => return Ok(Err(err)),
    };
    let gate =
        match runtime.verify_swarm_convergence_gate(input.workspace_id, input.swarm_run_id)? {
            Ok(gate) => gate,
            Err(err) => return Ok(Err(err)),
        };

    if matches!(candidate_run.mode(), SwarmRunMode::Standalone)
        && input.target != Some(SwarmConvergenceTarget::CandidateReady)
    {
        let settled_run =
            match runtime.settle_standalone_swarm_run(SettleStandaloneSwarmCommand {
                request_id: input.request_id,
                workspace_id: input.workspace_id,
                swarm_run_id: input.swarm_run_id,
                settled_at_ms: input.observed_at_ms,
            })? {
                Ok(run) => run,
                Err(err) => return Ok(Err(err)),
            };
        return Ok(Ok((settled_run, true, Some(gate))));
    }

    Ok(Ok((candidate_run, false, Some(gate))))
}

fn replay_settled_convergence(
    runtime: &OrchestrationRuntime,
    existing: &SwarmRunRecord,
    input: &ParsedConvergenceInput,
) -> Result<ConvergenceExecutionOutcome, RuntimeError> {
    if existing.converged_commit_oid() != Some(&input.converged_commit_oid)
        || existing.converged_tree_blake3() != Some(input.converged_tree_blake3)
        || existing.converged_diff_digest() != Some(input.converged_diff_digest)
    {
        return Err(RuntimeError::Conflict);
    }
    let settled = match runtime.settle_standalone_swarm_run(SettleStandaloneSwarmCommand {
        request_id: input.request_id,
        workspace_id: input.workspace_id,
        swarm_run_id: input.swarm_run_id,
        settled_at_ms: input.observed_at_ms,
    })? {
        Ok(run) => run,
        Err(err) => return Ok(Err(err)),
    };
    let gate =
        match runtime.verify_swarm_convergence_gate(input.workspace_id, input.swarm_run_id)? {
            Ok(gate) => gate,
            Err(err) => return Ok(Err(err)),
        };
    Ok(Ok((settled, true, Some(gate))))
}

fn precheck_convergence_quorum(
    runtime: &OrchestrationRuntime,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    target_diff_digest: [u8; 32],
) -> Result<Result<(), SwarmError>, RuntimeError> {
    let Some(panel) =
        (match runtime.get_swarm_verification_panel(workspace_id, swarm_run_id, None)? {
            Ok(panel) => panel,
            Err(err) => return Ok(Err(err)),
        })
    else {
        return Ok(Err(SwarmError::ReviewNotAccepted));
    };
    let verdicts =
        match runtime.list_swarm_verdicts(workspace_id, swarm_run_id, panel.panel_id())? {
            Ok(items) => items,
            Err(err) => return Ok(Err(err)),
        };
    Ok(evaluate_panel_verdicts_against_digest(
        &panel,
        &verdicts,
        target_diff_digest,
    ))
}

fn evaluate_panel_verdicts_against_digest(
    panel: &SwarmVerificationPanelRecord,
    verdicts: &[SwarmVerdictRecord],
    target_diff_digest: [u8; 32],
) -> Result<(), SwarmError> {
    let verifier_ids: BTreeSet<Uuid> = panel.verifier_member_ids().into_iter().collect();
    if panel.author_member_ids().is_empty() || verifier_ids.is_empty() {
        return Err(SwarmError::ReviewNotAccepted);
    }
    let mut accepted_verifiers = BTreeSet::new();
    let mut mismatched_accepted = false;

    for verdict in verdicts {
        if !verifier_ids.contains(&verdict.verifier_member_id()) {
            continue;
        }
        match verdict.outcome() {
            ReviewVerdictOutcome::Accepted => {
                if verdict.reviewed_diff_digest() == target_diff_digest {
                    accepted_verifiers.insert(verdict.verifier_member_id());
                } else {
                    mismatched_accepted = true;
                }
            }
            ReviewVerdictOutcome::Rejected | ReviewVerdictOutcome::NeedsChanges => {
                if verdict.reviewed_diff_digest() == target_diff_digest {
                    return Err(SwarmError::ReviewNotAccepted);
                }
            }
        }
    }

    let required = usize::try_from(panel.required_quorum()).unwrap_or(1);
    if accepted_verifiers.len() >= required {
        return Ok(());
    }
    if mismatched_accepted {
        return Err(SwarmError::VerdictDiffDigestMismatch);
    }
    Err(SwarmError::ReviewNotAccepted)
}

pub(super) fn build_stall_policy_and_timestamp(
    request: &SwarmStallCheckRequest,
) -> Result<(SwarmStallMonitorPolicy, i64), ApiError> {
    let defaults = SwarmStallMonitorPolicy::default();
    let policy = SwarmStallMonitorPolicy {
        stall_grace_ms: request.stall_grace_ms.unwrap_or(defaults.stall_grace_ms),
        max_stall_nudges: request
            .max_stall_nudges
            .unwrap_or(defaults.max_stall_nudges),
        max_ping_pong_streak: request
            .max_ping_pong_streak
            .unwrap_or(defaults.max_ping_pong_streak),
        max_member_turns: request
            .max_member_turns
            .unwrap_or(defaults.max_member_turns),
    };
    policy
        .validate()
        .map_err(RuntimeError::from)
        .map_err(ApiError::Runtime)?;
    let observed_at_ms = match request.observed_at_ms {
        Some(ts) if ts < 0 => return Err(ApiError::Runtime(RuntimeError::InvalidInput)),
        Some(ts) => ts,
        None => current_timestamp_millis()?,
    };
    Ok((policy, observed_at_ms))
}
