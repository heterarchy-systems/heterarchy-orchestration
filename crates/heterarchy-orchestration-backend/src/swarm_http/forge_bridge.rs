//! East-West `Orchestration <-> Forge` Swarm auto-provision/capture, 3-way merge
//! convergence, standalone settlement, and autonomy outbox sync HTTP handlers.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
};
use forge_backend::ops::effect_facts::wire::{EffectFactOutboxResponse, EffectFactWire};
use forge_control::SwarmConvergenceCandidate;
use forge_domain::{AuthenticatedPrincipal, SwarmMemberId, WorkspaceId};
use heterarchy_orchestration_operations::{
    EffectFactConsumerProgress, SettleStandaloneSwarmCommand, SwarmError, SwarmMemberRecord,
    SwarmMemberState, SwarmRunMode, SwarmRunRecord, SwarmRunSnapshot, SwarmRunState,
    SwarmWorkspaceMode,
};
use heterarchy_orchestration_runtime::{
    AutonomyOutboxApplyReport, AutonomyOutboxBridge, OrchestrationRuntime, SwarmDispatchError,
    SwarmDispatchTurnRequest,
};
use uuid::Uuid;

use crate::coordinator_authority::{
    CoordinatorAuthority, CoordinatorAuthorityError, ForgeAutonomyOutboxPageInput,
    ForgeSwarmCaptureInput, ForgeSwarmConvergeBridgeOutcome, ForgeSwarmConvergeInput,
    ForgeSwarmProvisionInput, ForgeSwarmPurgeInput, ForgeSwarmSettleInput,
};
use crate::realtime::RealtimeEventKind;
use crate::wire::swarm::{
    DispatchSwarmTurnRequest, DispatchSwarmTurnResponse, ForgeConvergeSwarmRequest,
    ForgeConvergeSwarmResponse, ForgeSettleSwarmRequest, ForgeSettleSwarmResponse,
    SwarmConvergenceGateWire, SwarmConvergenceTarget, SwarmErrorResponse, SwarmRunWire,
    SyncAutonomyOutboxRequest, SyncAutonomyOutboxResponse,
};
use crate::{ApiError, BackendState, RuntimeError, canonical_uuid, current_timestamp_millis};

use super::authorize_run_scope;
use super::authorize_workspace_scope;
use super::error_mapping::{SwarmReply, map_swarm_dispatch_error, map_swarm_error};
use super::projections::{ParsedConvergenceInput, execute_converge_swarm};

const DEFAULT_OUTBOX_SYNC_LIMIT: u32 = 64;
const MAX_OUTBOX_SYNC_LIMIT: u32 = 64;

pub(super) async fn dispatch_with_forge_session(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    mut dispatch_request: SwarmDispatchTurnRequest,
    raw_session_id: &str,
) -> Result<SwarmReply<DispatchSwarmTurnResponse>, ApiError> {
    let forge_session_id = canonical_uuid(raw_session_id)?;
    let authority = state
        .coordinator_authority
        .clone()
        .ok_or(ApiError::Unavailable)?;
    let workspace_id = dispatch_request.workspace_id;
    let swarm_run_id = dispatch_request.swarm_run_id;
    let member_id = dispatch_request.member_id;

    let (run, member) =
        match load_run_and_member(state, principal, workspace_id, swarm_run_id, member_id).await? {
            Ok(pair) => pair,
            Err(err) => return map_swarm_dispatch_error(err),
        };
    let worktree = match provision_member_worktree(
        state,
        &authority,
        principal,
        forge_session_id,
        &dispatch_request,
        (&run, &member),
    )
    .await?
    {
        Ok(valid) => valid,
        Err(rejected) => return Ok(rejected),
    };
    dispatch_request.default_worktree = worktree;

    let should_capture = matches!(
        member.workspace_mode(),
        SwarmWorkspaceMode::Share | SwarmWorkspaceMode::Branch
    );
    let capture_slots = Arc::clone(&state.forge_query_slots);
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_swarm_dispatch_with_clean_exit_hook(
            principal,
            dispatch_request,
            move |req, _snapshot| {
                let dispatched_at_ms = req.dispatched_at_ms;
                let run_id = req.swarm_run_id;
                let target_member_id = req.member_id;
                async move {
                    if !should_capture {
                        return Ok(None);
                    }
                    let capture_permit = capture_slots
                        .acquire_owned()
                        .await
                        .map_err(|_| SwarmDispatchError::SemaphoreClosed)?;
                    let captured_at_ms = current_timestamp_millis()
                        .unwrap_or(dispatched_at_ms)
                        .max(dispatched_at_ms);
                    let output = authority
                        .swarm_member_capture(
                            ForgeSwarmCaptureInput {
                                workspace_id: ws_uuid,
                                session_id: forge_session_id,
                                principal,
                                swarm_run_id: run_id,
                                member_id: target_member_id,
                                captured_at_ms,
                            },
                            capture_permit,
                        )
                        .await
                        .map_err(map_bridge_error_to_dispatch)?;
                    Ok(Some(output))
                }
            },
        )
        .await?;
    match outcome {
        Ok(dispatched) => Ok(SwarmReply::Success(
            StatusCode::OK,
            DispatchSwarmTurnResponse::from(&dispatched),
        )),
        Err(err) => map_swarm_dispatch_error(err),
    }
}

async fn provision_member_worktree(
    state: &BackendState,
    authority: &CoordinatorAuthority,
    principal: AuthenticatedPrincipal,
    forge_session_id: Uuid,
    dispatch_request: &SwarmDispatchTurnRequest,
    run_and_member: (&SwarmRunRecord, &SwarmMemberRecord),
) -> Result<
    Result<
        heterarchy_orchestration_operations::ForgeWorktreeRef,
        SwarmReply<DispatchSwarmTurnResponse>,
    >,
    ApiError,
> {
    let (run, member) = run_and_member;
    let candidate_commit_oid = select_provision_candidate_oid(run, member);
    let provision_permit = Arc::clone(&state.forge_query_slots)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    let provisioned = match authority
        .swarm_workspace_provision(
            ForgeSwarmProvisionInput {
                workspace_id: dispatch_request.workspace_id.into_uuid(),
                session_id: forge_session_id,
                principal,
                swarm_run_id: dispatch_request.swarm_run_id,
                member_id: dispatch_request.member_id,
                mode: member.workspace_mode(),
                run_mode: run.mode(),
                candidate_commit_oid,
                observed_at_ms: dispatch_request.dispatched_at_ms,
            },
            provision_permit,
        )
        .await
    {
        Ok(receipt) => receipt,
        Err(err) => return map_coordinator_bridge_error(err).map(Err),
    };
    if let Some(existing_wt) = member.worktree_path()
        && existing_wt != &provisioned.worktree
    {
        return Ok(Err(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent(
                "INVALID_WORKTREE_PATH",
                "stored Swarm member worktree_path conflicts with Forge provisioned worktree",
            ),
        )));
    }
    Ok(Ok(provisioned.worktree))
}

pub(super) async fn forge_converge_swarm(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ForgeConvergeSwarmRequest>,
) -> Result<SwarmReply<ForgeConvergeSwarmResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let session_id = canonical_uuid(&request.session_id)?;
    let explicit_ids = request
        .candidate_member_ids
        .iter()
        .map(|raw| canonical_uuid(raw))
        .collect::<Result<Vec<_>, _>>()?;
    let observed_at_ms = current_timestamp_millis()?;
    let authority = state
        .coordinator_authority
        .clone()
        .ok_or(ApiError::Unavailable)?;

    let candidates = match resolve_convergence_candidates(
        &state,
        principal,
        workspace_id,
        swarm_run_id,
        explicit_ids,
    )
    .await?
    {
        Ok(Some(list)) => list,
        Ok(None) => {
            return map_swarm_error(SwarmError::InvalidStateTransition(
                "no Share/Branch Swarm members have captured output to converge",
            ));
        }
        Err(err) => return map_swarm_error(err),
    };

    let permit = Arc::clone(&state.forge_query_slots)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    let bridge_outcome = match authority
        .swarm_converge(
            ForgeSwarmConvergeInput {
                workspace_id: workspace_id.into_uuid(),
                session_id,
                principal,
                swarm_run_id,
                candidates,
                observed_at_ms,
            },
            permit,
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(err) => return map_coordinator_bridge_error(err),
    };

    match bridge_outcome {
        ForgeSwarmConvergeBridgeOutcome::Conflict {
            conflicting_member_id,
            conflicting_paths,
        } => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent(
                "ORCHESTRATION_CONFLICT",
                format!(
                    "SWARM_MERGE_CONFLICT on member {conflicting_member_id}: {}",
                    conflicting_paths.join(", ")
                ),
            ),
        )),
        ForgeSwarmConvergeBridgeOutcome::Converged {
            converged_commit_oid,
            converged_tree_blake3,
            converged_diff_digest,
            changed_files,
        } => {
            let input = ParsedConvergenceInput {
                request_id,
                workspace_id,
                swarm_run_id,
                converged_commit_oid,
                converged_tree_blake3,
                converged_diff_digest,
                target: Some(SwarmConvergenceTarget::CandidateReady),
                stage_candidate_only: request.stage_candidate_only,
                observed_at_ms,
            };
            stage_forge_converged_candidate(&state, principal, input, changed_files).await
        }
    }
}

pub(super) async fn forge_settle_swarm(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ForgeSettleSwarmRequest>,
) -> Result<SwarmReply<ForgeSettleSwarmResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let parsed = parse_forge_settle_request(&request)?;
    let observed_at_ms = current_timestamp_millis()?;
    let authority = state
        .coordinator_authority
        .clone()
        .ok_or(ApiError::Unavailable)?;

    let (candidate_run, gate) = match verify_settle_preflight(
        &state,
        principal,
        workspace_id,
        swarm_run_id,
        parsed.request,
    )
    .await?
    {
        Ok(pair) => pair,
        Err(err) => return map_swarm_error(err),
    };
    let forge_settled = match settle_candidate_on_forge(
        &state,
        &authority,
        principal,
        &parsed,
        (workspace_id, swarm_run_id, observed_at_ms),
        &candidate_run,
    )
    .await?
    {
        Ok(receipt) => receipt,
        Err(rejected) => return Ok(rejected),
    };

    let settled_run = match commit_standalone_settled_run(
        &state,
        principal,
        workspace_id,
        swarm_run_id,
        parsed.request,
        forge_settled.settled_at_ms.max(observed_at_ms),
    )
    .await?
    {
        Ok(run) => run,
        Err(err) => return map_swarm_error(err),
    };

    finalize_post_settle_sync_and_purge(
        &state,
        &authority,
        principal,
        (workspace_id.into_uuid(), swarm_run_id),
        &parsed,
        (settled_run, gate, forge_settled),
    )
    .await
}

pub(super) async fn sync_autonomy_outbox(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SyncAutonomyOutboxRequest>,
) -> Result<SwarmReply<SyncAutonomyOutboxResponse>, ApiError> {
    let workspace_id = authorize_workspace_scope(&state, principal, &workspace_path)?;
    let migration_id = canonical_uuid(&request.migration_id)?;
    let limit = match request.limit {
        Some(0) => return Err(ApiError::Runtime(RuntimeError::InvalidInput)),
        Some(explicit) if explicit > MAX_OUTBOX_SYNC_LIMIT => {
            return Err(ApiError::Runtime(RuntimeError::InvalidInput));
        }
        Some(explicit) => explicit,
        None => DEFAULT_OUTBOX_SYNC_LIMIT,
    };
    let authority = state
        .coordinator_authority
        .clone()
        .ok_or(ApiError::Unavailable)?;
    execute_outbox_sync(
        &state,
        &authority,
        principal,
        OutboxSyncParams {
            workspace_id: workspace_id.into_uuid(),
            migration_id,
            after_sequence: request.after_sequence,
            limit,
        },
    )
    .await
}

struct ParsedForgeSettleRequest {
    request: Uuid,
    session: Uuid,
    activity: Option<Uuid>,
    coordinator_agent: Uuid,
    outbox_migration: Option<Uuid>,
    after_sequence: u64,
    purge_ephemeral: bool,
}

fn parse_forge_settle_request(
    request: &ForgeSettleSwarmRequest,
) -> Result<ParsedForgeSettleRequest, ApiError> {
    Ok(ParsedForgeSettleRequest {
        request: canonical_uuid(&request.request_id)?,
        session: canonical_uuid(&request.session_id)?,
        activity: request
            .activity_id
            .as_deref()
            .map(canonical_uuid)
            .transpose()?,
        coordinator_agent: canonical_uuid(&request.coordinator_agent_id)?,
        outbox_migration: request
            .outbox_migration_id
            .as_deref()
            .map(canonical_uuid)
            .transpose()?,
        after_sequence: request.outbox_after_sequence.unwrap_or(0),
        purge_ephemeral: request.purge_ephemeral,
    })
}

async fn settle_candidate_on_forge(
    state: &BackendState,
    authority: &CoordinatorAuthority,
    principal: AuthenticatedPrincipal,
    parsed: &ParsedForgeSettleRequest,
    coords: (WorkspaceId, Uuid, i64),
    candidate_run: &SwarmRunRecord,
) -> Result<
    Result<
        crate::coordinator_authority::ForgeSwarmSettleBridgeOutcome,
        SwarmReply<ForgeSettleSwarmResponse>,
    >,
    ApiError,
> {
    let (workspace_id, swarm_run_id, observed_at_ms) = coords;
    let Some(converged_oid) = candidate_run.converged_commit_oid().cloned() else {
        return map_swarm_error(SwarmError::InvalidStateTransition(
            "candidate run is missing converged_commit_oid",
        ))
        .map(Err);
    };
    let (Some(tree_blake3), Some(diff_digest)) = (
        candidate_run.converged_tree_blake3(),
        candidate_run.converged_diff_digest(),
    ) else {
        return map_swarm_error(SwarmError::InvalidStateTransition(
            "candidate run is missing converged digests",
        ))
        .map(Err);
    };
    let settle_permit = Arc::clone(&state.forge_query_slots)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    match authority
        .swarm_settle_standalone(
            ForgeSwarmSettleInput {
                workspace_id: workspace_id.into_uuid(),
                session_id: parsed.session,
                principal,
                activity_id: parsed.activity,
                coordinator_agent_id: parsed.coordinator_agent,
                swarm_run_id,
                settlement_id: parsed.request,
                converged_commit_oid: converged_oid.as_str().to_owned(),
                expected_converged_tree_blake3: tree_blake3,
                expected_converged_diff_digest: diff_digest,
                observed_at_ms,
            },
            settle_permit,
        )
        .await
    {
        Ok(receipt) => Ok(Ok(receipt)),
        Err(err) => map_coordinator_bridge_error(err).map(Err),
    }
}

async fn finalize_post_settle_sync_and_purge(
    state: &BackendState,
    authority: &CoordinatorAuthority,
    principal: AuthenticatedPrincipal,
    scope_ids: (Uuid, Uuid),
    parsed: &ParsedForgeSettleRequest,
    settled_parts: (
        SwarmRunRecord,
        heterarchy_orchestration_operations::SwarmConvergenceGateOutcome,
        crate::coordinator_authority::ForgeSwarmSettleBridgeOutcome,
    ),
) -> Result<SwarmReply<ForgeSettleSwarmResponse>, ApiError> {
    let (ws_uuid, swarm_run_id) = scope_ids;
    let (settled_run, gate, forge_settled) = settled_parts;
    let outbox_sync = match parsed.outbox_migration {
        Some(migration_id) => {
            match execute_outbox_sync(
                state,
                authority,
                principal,
                OutboxSyncParams {
                    workspace_id: ws_uuid,
                    migration_id,
                    after_sequence: parsed.after_sequence,
                    limit: DEFAULT_OUTBOX_SYNC_LIMIT,
                },
            )
            .await?
            {
                SwarmReply::Success(_, sync_resp) => Some(sync_resp),
                SwarmReply::DomainRejected(status, err) => {
                    return Ok(SwarmReply::DomainRejected(status, err));
                }
            }
        }
        None => None,
    };

    let purged_ephemeral = if parsed.purge_ephemeral {
        match purge_forge_swarm_ephemeral(
            state,
            authority,
            principal,
            ws_uuid,
            parsed.session,
            swarm_run_id,
        )
        .await?
        {
            Ok(purged) => purged,
            Err(rejected) => return Ok(rejected),
        }
    } else {
        false
    };

    Ok(SwarmReply::Success(
        StatusCode::OK,
        ForgeSettleSwarmResponse {
            run: SwarmRunWire::from(&settled_run),
            gate: SwarmConvergenceGateWire::from(&gate),
            settlement_id: forge_settled.settlement_id,
            settled_file_count: forge_settled.settled_file_count,
            settled_at_ms: forge_settled.settled_at_ms,
            outbox_sequence: forge_settled.outbox_sequence,
            outbox_sync,
            purged_ephemeral,
        },
    ))
}

async fn load_run_and_member(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    member_id: Uuid,
) -> Result<Result<(SwarmRunRecord, SwarmMemberRecord), SwarmDispatchError>, ApiError> {
    state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            let snapshot = match runtime.get_swarm_run_snapshot(workspace_id, swarm_run_id)? {
                Ok(Some(snap)) => snap,
                Ok(None) => {
                    return Ok(Err(SwarmDispatchError::Swarm(SwarmError::NotFound(
                        "swarm run",
                    ))));
                }
                Err(err) => return Ok(Err(SwarmDispatchError::Swarm(err))),
            };
            if snapshot.run().state().is_terminal() {
                return Ok(Err(SwarmDispatchError::Swarm(
                    SwarmError::InvalidStateTransition(
                        "cannot dispatch member on terminal Swarm run",
                    ),
                )));
            }
            let Some(member) = snapshot
                .members()
                .iter()
                .find(|m| m.member_id() == member_id)
                .cloned()
            else {
                return Ok(Err(SwarmDispatchError::Swarm(SwarmError::NotFound(
                    "swarm member",
                ))));
            };
            if member.member_state() != SwarmMemberState::WakeQueued {
                return Ok(Err(SwarmDispatchError::MemberNotWakeQueued {
                    member_id,
                    state: member.member_state(),
                }));
            }
            Ok(Ok((snapshot.run().clone(), member)))
        })
        .await
}

fn select_provision_candidate_oid(
    run: &SwarmRunRecord,
    member: &SwarmMemberRecord,
) -> Option<String> {
    let _previous_member_output = member.last_output_commit_oid();
    if member.workspace_mode() == SwarmWorkspaceMode::Inherit && member.turns_used() == 0 {
        run.converged_commit_oid()
            .map(|oid| oid.as_str().to_owned())
    } else {
        None
    }
}

async fn resolve_convergence_candidates(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    explicit_ids: Vec<Uuid>,
) -> Result<Result<Option<Vec<SwarmConvergenceCandidate>>, SwarmError>, ApiError> {
    state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            let snapshot = match runtime.get_swarm_run_snapshot(workspace_id, swarm_run_id)? {
                Ok(Some(snap)) => snap,
                Ok(None) => return Ok(Err(SwarmError::NotFound("swarm run"))),
                Err(err) => return Ok(Err(err)),
            };
            Ok(extract_candidates_from_snapshot(&snapshot, &explicit_ids))
        })
        .await
}

fn extract_candidates_from_snapshot(
    snapshot: &SwarmRunSnapshot,
    explicit_ids: &[Uuid],
) -> Result<Option<Vec<SwarmConvergenceCandidate>>, SwarmError> {
    if snapshot.run().state().is_terminal() {
        return Err(SwarmError::InvalidStateTransition(
            "cannot forge-converge a terminal Swarm run",
        ));
    }
    if explicit_ids.is_empty() {
        let auto_candidates = snapshot
            .members()
            .iter()
            .filter(|m| {
                matches!(
                    m.workspace_mode(),
                    SwarmWorkspaceMode::Share | SwarmWorkspaceMode::Branch
                )
            })
            .filter_map(|m| {
                m.last_output_commit_oid()
                    .map(|oid| SwarmConvergenceCandidate {
                        member_id: SwarmMemberId::from_uuid(m.member_id()),
                        expected_output_commit_oid: Some(oid.as_str().to_owned()),
                        expected_diff_digest: None,
                    })
            })
            .collect::<Vec<_>>();
        if auto_candidates.is_empty() {
            return Ok(None);
        }
        return Ok(Some(auto_candidates));
    }

    let mut ordered = Vec::with_capacity(explicit_ids.len());
    for &candidate_id in explicit_ids {
        let member = snapshot
            .members()
            .iter()
            .find(|m| m.member_id() == candidate_id)
            .ok_or(SwarmError::NotFound("swarm candidate member"))?;
        if !matches!(
            member.workspace_mode(),
            SwarmWorkspaceMode::Share | SwarmWorkspaceMode::Branch
        ) {
            return Err(SwarmError::InvalidStateTransition(
                "convergence candidate must be a Share or Branch member",
            ));
        }
        let Some(oid) = member.last_output_commit_oid() else {
            return Err(SwarmError::InvalidStateTransition(
                "convergence candidate member has no captured turn output",
            ));
        };
        ordered.push(SwarmConvergenceCandidate {
            member_id: SwarmMemberId::from_uuid(candidate_id),
            expected_output_commit_oid: Some(oid.as_str().to_owned()),
            expected_diff_digest: None,
        });
    }
    Ok(Some(ordered))
}

async fn stage_forge_converged_candidate(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    mut input: ParsedConvergenceInput,
    changed_files: Vec<String>,
) -> Result<SwarmReply<ForgeConvergeSwarmResponse>, ApiError> {
    let publisher = state.clone();
    let ws_uuid = input.workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let result = match execute_converge_swarm(runtime, &input)? {
                Ok(res) => Ok(res),
                Err(SwarmError::ReviewNotAccepted) if !input.stage_candidate_only => {
                    input.stage_candidate_only = true;
                    execute_converge_swarm(runtime, &input)?
                }
                Err(err) => Err(err),
            };
            if let Ok((ref run, _, _)) = result
                && !run.replayed()
            {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmRunChanged,
                    run.swarm_run_id(),
                    run.revision(),
                    Some(ws_uuid),
                );
            }
            Ok(result)
        })
        .await?;
    match outcome {
        Ok((run, _settled, gate)) => Ok(SwarmReply::Success(
            StatusCode::OK,
            ForgeConvergeSwarmResponse {
                run: SwarmRunWire::from(&run),
                changed_files,
                gate: gate.as_ref().map(SwarmConvergenceGateWire::from),
            },
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn verify_settle_preflight(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    expected_request_id: Uuid,
) -> Result<
    Result<
        (
            SwarmRunRecord,
            heterarchy_orchestration_operations::SwarmConvergenceGateOutcome,
        ),
        SwarmError,
    >,
    ApiError,
> {
    state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            let run = match runtime.get_swarm_run(workspace_id, swarm_run_id)? {
                Ok(Some(run)) => run,
                Ok(None) => return Ok(Err(SwarmError::NotFound("swarm run"))),
                Err(err) => return Ok(Err(err)),
            };
            if matches!(run.mode(), SwarmRunMode::DagNodeHybrid { .. }) {
                return Ok(Err(SwarmError::DagNodeHybridCannotSettleDirectly));
            }
            match run.state() {
                SwarmRunState::CandidateReady => {}
                SwarmRunState::Settled
                    if run.settlement_request_id() == Some(expected_request_id) => {}
                _ => {
                    return Ok(Err(SwarmError::InvalidStateTransition(
                        "forge-settle requires CandidateReady or matching Settled replay",
                    )));
                }
            }
            let gate = match runtime.verify_swarm_convergence_gate(workspace_id, swarm_run_id)? {
                Ok(gate) => gate,
                Err(err) => return Ok(Err(err)),
            };
            Ok(Ok((run, gate)))
        })
        .await
}

async fn commit_standalone_settled_run(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    request_id: Uuid,
    settled_at_ms: i64,
) -> Result<Result<SwarmRunRecord, SwarmError>, ApiError> {
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    state
        .run_operator_mutation(principal, move |runtime| {
            let outcome = runtime.settle_standalone_swarm_run(SettleStandaloneSwarmCommand {
                request_id,
                workspace_id,
                swarm_run_id,
                settled_at_ms,
            })?;
            if let Ok(ref settled_run) = outcome
                && !settled_run.replayed()
            {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmRunChanged,
                    settled_run.swarm_run_id(),
                    settled_run.revision(),
                    Some(ws_uuid),
                );
            }
            Ok(outcome)
        })
        .await
}

async fn purge_forge_swarm_ephemeral(
    state: &BackendState,
    authority: &CoordinatorAuthority,
    principal: AuthenticatedPrincipal,
    workspace_id: Uuid,
    session_id: Uuid,
    swarm_run_id: Uuid,
) -> Result<Result<bool, SwarmReply<ForgeSettleSwarmResponse>>, ApiError> {
    let purge_permit = Arc::clone(&state.forge_query_slots)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    match authority
        .swarm_purge(
            ForgeSwarmPurgeInput {
                workspace_id,
                session_id,
                principal,
                swarm_run_id,
            },
            purge_permit,
        )
        .await
    {
        Ok(purged) => Ok(Ok(purged)),
        Err(err) => map_coordinator_bridge_error(err).map(Err),
    }
}

#[derive(Clone, Copy)]
struct OutboxSyncParams {
    workspace_id: Uuid,
    migration_id: Uuid,
    after_sequence: u64,
    limit: u32,
}

async fn execute_outbox_sync(
    state: &BackendState,
    authority: &CoordinatorAuthority,
    principal: AuthenticatedPrincipal,
    params: OutboxSyncParams,
) -> Result<SwarmReply<SyncAutonomyOutboxResponse>, ApiError> {
    let permit = Arc::clone(&state.forge_query_slots)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    let destination_id = state.runtime.destination_id();
    let page = match authority
        .autonomy_outbox_page(
            ForgeAutonomyOutboxPageInput {
                workspace_id: params.workspace_id,
                migration_id: params.migration_id,
                destination_id,
                after_sequence: params.after_sequence,
                limit: params.limit,
            },
            permit,
        )
        .await
    {
        Ok(page) => page,
        Err(err) => return map_coordinator_bridge_error(err),
    };

    let outbox_bridge = state.autonomy_outbox.clone();
    let publisher = state.clone();
    let apply_result = state
        .run_operator_mutation(principal, move |runtime| {
            Ok(apply_page_and_publish_events(
                runtime,
                principal,
                &outbox_bridge,
                &publisher,
                params,
                &page,
            ))
        })
        .await?;
    match apply_result {
        Ok(response) => Ok(SwarmReply::Success(StatusCode::OK, response)),
        Err(err) => map_swarm_dispatch_error(*err),
    }
}

struct OutboxPublishScope<'a> {
    publisher: &'a BackendState,
    workspace_id: Uuid,
    after_sequence: u64,
    prev_core_seq: u64,
    prev_core_applied: u64,
    prev_swarm_seq: u64,
    post_swarm_seq: u64,
}

fn apply_page_and_publish_events(
    runtime: &OrchestrationRuntime,
    principal: AuthenticatedPrincipal,
    outbox_bridge: &AutonomyOutboxBridge,
    publisher: &BackendState,
    params: OutboxSyncParams,
    page: &EffectFactOutboxResponse,
) -> Result<SyncAutonomyOutboxResponse, Box<SwarmDispatchError>> {
    let (prev_core_seq, prev_core_applied) = runtime
        .coordination_source_cursor(params.workspace_id, None, principal)
        .ok()
        .flatten()
        .filter(|progress| progress.migration_id() == params.migration_id)
        .map_or((0, 0), |progress| {
            (progress.last_sequence(), progress.applied_count())
        });
    let prev_swarm_seq = outbox_bridge
        .last_swarm_settled_sequence(params.migration_id)
        .map_err(Box::new)?;
    let report = crate::effect_fact_http::apply_autonomy_outbox_page(outbox_bridge, page)?;
    let post_swarm_seq = outbox_bridge
        .last_swarm_settled_sequence(params.migration_id)
        .map_err(Box::new)?;
    let scope = OutboxPublishScope {
        publisher,
        workspace_id: params.workspace_id,
        after_sequence: params.after_sequence,
        prev_core_seq,
        prev_core_applied,
        prev_swarm_seq,
        post_swarm_seq,
    };
    Ok(summarize_and_publish_outbox_apply(&scope, page, &report))
}

fn summarize_and_publish_outbox_apply(
    scope: &OutboxPublishScope<'_>,
    page: &EffectFactOutboxResponse,
    report: &AutonomyOutboxApplyReport,
) -> SyncAutonomyOutboxResponse {
    let core_applied = report
        .core_progress
        .and_then(|progress| {
            usize::try_from(
                progress
                    .applied_count()
                    .saturating_sub(scope.prev_core_applied),
            )
            .ok()
        })
        .unwrap_or(0);
    let newly_applied_dag_entries = page
        .entries
        .iter()
        .filter(|entry| entry.sequence > scope.prev_core_seq)
        .filter_map(|entry| match &entry.fact {
            EffectFactWire::DagSettled { settlement_id, .. } => {
                Some((*settlement_id, entry.sequence))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let dag_settled_count = newly_applied_dag_entries.len();

    let newly_settled_swarms = report
        .swarm_settlements
        .iter()
        .filter(|receipt| receipt.sequence > scope.prev_swarm_seq)
        .collect::<Vec<_>>();
    let swarm_settled_count = newly_settled_swarms.len();
    let applied_count = core_applied.saturating_add(swarm_settled_count);
    let core_last = report.core_progress.map_or(
        scope.prev_core_seq,
        EffectFactConsumerProgress::last_sequence,
    );
    let last_sequence = scope
        .after_sequence
        .max(core_last)
        .max(scope.post_swarm_seq);

    for &(settlement_id, sequence) in &newly_applied_dag_entries {
        scope.publisher.publish_state_changed(
            RealtimeEventKind::SettlementProjected,
            settlement_id,
            sequence.max(1),
            Some(scope.workspace_id),
        );
    }
    for receipt in &newly_settled_swarms {
        scope.publisher.publish_state_changed(
            RealtimeEventKind::SettlementProjected,
            receipt.settlement_id,
            receipt.sequence.max(1),
            Some(scope.workspace_id),
        );
    }

    SyncAutonomyOutboxResponse {
        applied_count,
        dag_settled_count,
        swarm_settled_count,
        last_sequence,
    }
}

pub(super) fn should_use_forge_bridge(request: &DispatchSwarmTurnRequest) -> Option<&str> {
    request.forge_session_id.as_deref()
}

fn map_coordinator_bridge_error<T>(
    error: CoordinatorAuthorityError,
) -> Result<SwarmReply<T>, ApiError> {
    match error {
        CoordinatorAuthorityError::Unavailable => Err(ApiError::Unavailable),
        CoordinatorAuthorityError::Denied => Ok(SwarmReply::DomainRejected(
            StatusCode::FORBIDDEN,
            SwarmErrorResponse::permanent("FORGE_BRIDGE_DENIED", error.to_string()),
        )),
        CoordinatorAuthorityError::Stale => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent("STALE_REVISION", error.to_string()),
        )),
    }
}

const fn map_bridge_error_to_dispatch(error: CoordinatorAuthorityError) -> SwarmDispatchError {
    match error {
        CoordinatorAuthorityError::Unavailable => SwarmDispatchError::ForgeBridge("unavailable"),
        CoordinatorAuthorityError::Denied => SwarmDispatchError::ForgeBridge("denied"),
        CoordinatorAuthorityError::Stale => SwarmDispatchError::ForgeBridge("stale"),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use forge_backend::ops::effect_facts::wire::{
        EffectFactOutboxEntryWire, EffectFactOutboxResponse, EffectFactWire, ExecutionStateWire,
    };
    use forge_domain::{AuthenticatedPrincipal, DirectTokenId, WorkspaceId};
    use heterarchy_orchestration_operations::{
        AddSwarmMemberCommand, CreateSwarmRunCommand, RecordSwarmConvergenceCommand,
        RecordSwarmVerdictCommand, ReviewVerdictOutcome, SwarmCommitOid, SwarmError,
        SwarmMemberIdentity, SwarmRoleKind, SwarmRunMode, SwarmTurnOutput, SwarmWorkspaceMode,
    };
    use heterarchy_orchestration_runtime::{OrchestrationRuntime, access::OperatorPolicy};
    use heterarchy_orchestration_sessions::AgentProfileId;
    use rusqlite::{Connection, params};
    use uuid::Uuid;

    use super::*;
    use crate::realtime::RealtimeHub;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    struct TempRuntimeDir(PathBuf);

    impl TempRuntimeDir {
        fn new(label: &str) -> TestResult<Self> {
            let dir = std::env::temp_dir().join(format!(
                "bridge-unit-{label}-{}-{}",
                std::process::id(),
                Uuid::now_v7()
            ));
            fs::create_dir_all(&dir)?;
            Ok(Self(dir))
        }
    }

    impl Drop for TempRuntimeDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn exec_entry_wire(
        seq: u64,
        migration_id: Uuid,
        destination_id: Uuid,
        workspace_id: Uuid,
    ) -> EffectFactOutboxEntryWire {
        EffectFactOutboxEntryWire {
            sequence: seq,
            migration_id,
            destination_id,
            workspace_id,
            submitted_source_refs: None,
            fact: EffectFactWire::ExecutionTerminal {
                execution_id: Uuid::from_u128(0x900 + u128::from(seq)),
                activity_id: Uuid::from_u128(0x910),
                execution_revision: 1,
                state: ExecutionStateWire::Completed,
                observed_at_ms: 1_000 + i64::try_from(seq).unwrap_or(0),
                agent_id: Uuid::from_u128(0x920),
                agent_revision: 1,
                session_id: None,
                goal_id: Uuid::from_u128(0x930),
                goal_revision: 1,
                dag_revision: 1,
                node_id: Uuid::from_u128(0x940),
                node_revision: 1,
                verification: None,
            },
        }
    }

    fn dag_settled_entry_wire(
        seq: u64,
        migration_id: Uuid,
        destination_id: Uuid,
        workspace_id: Uuid,
        settlement_id: Uuid,
    ) -> EffectFactOutboxEntryWire {
        EffectFactOutboxEntryWire {
            sequence: seq,
            migration_id,
            destination_id,
            workspace_id,
            submitted_source_refs: None,
            fact: EffectFactWire::DagSettled {
                settlement_id,
                workspace_id,
                activity_id: Uuid::from_u128(0x910),
                goal_id: Uuid::from_u128(0x930),
                goal_revision: 1,
                dag_revision: 2,
                settled_file_count: 3,
                settled_tree_digest: "5a".repeat(32),
                coordinator_agent_id: Uuid::from_u128(0x920),
                coordinator_session_id: None,
                settled_at_ms: 1_500,
            },
        }
    }

    fn build_outbox_page(
        ws: Uuid,
        migration_id: Uuid,
        dest: Uuid,
        after: u64,
        entries: Vec<EffectFactOutboxEntryWire>,
    ) -> TestResult<EffectFactOutboxResponse> {
        let scope: forge_backend::ops::ScopeDto = serde_json::from_value(serde_json::json!({
            "workspace_id": ws,
            "workspace_revision": 1,
            "locator": {
                "location_id": Uuid::from_u128(0x888),
                "expected_revision": 1,
                "relative_root": "ws",
            }
        }))?;
        Ok(EffectFactOutboxResponse {
            scope,
            backend_id: Uuid::from_u128(0x889),
            migration_id,
            destination_id: dest,
            after,
            entries,
        })
    }

    #[test]
    fn outbox_sync_uses_per_call_core_delta_and_suppresses_replay_and_non_settlement_events()
    -> TestResult {
        let dir = TempRuntimeDir::new("outbox-delta")?;
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x800)));
        let runtime = OrchestrationRuntime::open(&dir.0)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?);
        let ws = Uuid::from_u128(0x801);
        let migration_id = Uuid::from_u128(0x802);
        let dest = runtime.destination_id();
        let conn = Connection::open(dir.0.join("agent-operations.sqlite3"))?;
        conn.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![migration_id.as_bytes(), ws.as_bytes(), dest.as_bytes(), [0_u8; 32].as_slice()],
        )?;
        drop(conn);

        let hub = Arc::new(RealtimeHub::new());
        let state = BackendState::new(runtime, Arc::clone(&hub));
        let bridge = state.autonomy_outbox.clone();
        let settlement_id = Uuid::from_u128(0x803);
        let page1 = build_outbox_page(
            ws,
            migration_id,
            dest,
            0,
            vec![
                exec_entry_wire(1, migration_id, dest, ws),
                dag_settled_entry_wire(2, migration_id, dest, ws, settlement_id),
            ],
        )?;
        let params = OutboxSyncParams {
            workspace_id: ws,
            migration_id,
            after_sequence: 0,
            limit: 64,
        };
        let first = apply_page_and_publish_events(
            &state.runtime,
            principal,
            &bridge,
            &state,
            params,
            &page1,
        )?;
        assert_eq!(first.applied_count, 2);
        assert_eq!(first.dag_settled_count, 1);
        assert_eq!(first.swarm_settled_count, 0);
        assert_eq!(first.last_sequence, 2);

        let replay = apply_page_and_publish_events(
            &state.runtime,
            principal,
            &bridge,
            &state,
            params,
            &page1,
        )?;
        assert_eq!(replay.applied_count, 0);
        assert_eq!(replay.dag_settled_count, 0);
        assert_eq!(replay.swarm_settled_count, 0);
        assert_eq!(replay.last_sequence, 2);

        let page2 = build_outbox_page(
            ws,
            migration_id,
            dest,
            2,
            vec![exec_entry_wire(3, migration_id, dest, ws)],
        )?;
        let third = apply_page_and_publish_events(
            &state.runtime,
            principal,
            &bridge,
            &state,
            OutboxSyncParams {
                after_sequence: 2,
                ..params
            },
            &page2,
        )?;
        assert_eq!(third.applied_count, 1);
        assert_eq!(third.dag_settled_count, 0);
        assert_eq!(third.last_sequence, 3);
        Ok(())
    }

    fn seed_aborted_candidate_ready_run(
        runtime: &OrchestrationRuntime,
        ws: WorkspaceId,
    ) -> TestResult<Uuid> {
        let run = runtime
            .create_swarm_run(&CreateSwarmRunCommand {
                request_id: Uuid::now_v7(),
                workspace_id: ws,
                title: "Aborted Candidate".to_owned(),
                mode: SwarmRunMode::Standalone,
                max_members: 8,
                max_spawn_depth: 2,
                max_total_turns: 32,
                required_verifier_quorum: 1,
                allow_worker_all_mention: false,
                baseline_commit_oid: SwarmCommitOid::new("1".repeat(40))?,
                baseline_tree_blake3: [0x11; 32],
                created_at_ms: 100,
            })?
            .map_err(|e| format!("create: {e}"))?;
        let author = runtime
            .add_swarm_member(&AddSwarmMemberCommand {
                member_id: Uuid::from_u128(0x852),
                workspace_id: ws,
                swarm_run_id: run.swarm_run_id(),
                roster_name: "author".to_owned(),
                role_kind: SwarmRoleKind::Worker,
                parent_member_id: None,
                workspace_mode: SwarmWorkspaceMode::Share,
                worktree_path: None,
                identity: SwarmMemberIdentity::new(
                    AgentProfileId::from_uuid(Uuid::from_u128(0x860))?,
                    Uuid::from_u128(0x861),
                    AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                        0x862,
                    ))),
                )?,
                initial_channels: Vec::new(),
                added_at_ms: 110,
            })?
            .map_err(|e| format!("add author: {e}"))?;
        let verifier = runtime
            .add_swarm_member(&AddSwarmMemberCommand {
                member_id: Uuid::from_u128(0x853),
                workspace_id: ws,
                swarm_run_id: run.swarm_run_id(),
                roster_name: "verifier".to_owned(),
                role_kind: SwarmRoleKind::Verifier,
                parent_member_id: None,
                workspace_mode: SwarmWorkspaceMode::Inherit,
                worktree_path: None,
                identity: SwarmMemberIdentity::new(
                    AgentProfileId::from_uuid(Uuid::from_u128(0x870))?,
                    Uuid::from_u128(0x871),
                    AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                        0x872,
                    ))),
                )?,
                initial_channels: Vec::new(),
                added_at_ms: 120,
            })?
            .map_err(|e| format!("add verifier: {e}"))?;
        let _ = (
            author.last_output_commit_oid(),
            SwarmTurnOutput {
                commit_oid: SwarmCommitOid::new("2".repeat(40))?,
                tree_blake3: [0x22; 32],
            },
        );
        runtime
            .record_swarm_convergence_candidate(&RecordSwarmConvergenceCommand {
                workspace_id: ws,
                swarm_run_id: run.swarm_run_id(),
                converged_commit_oid: SwarmCommitOid::new("3".repeat(40))?,
                converged_tree_blake3: [0x33; 32],
                converged_diff_digest: [0x44; 32],
                recorded_at_ms: 150,
            })?
            .map_err(|e| format!("converge: {e}"))?;
        runtime
            .record_swarm_verdict(&RecordSwarmVerdictCommand {
                request_id: Uuid::now_v7(),
                workspace_id: ws,
                swarm_run_id: run.swarm_run_id(),
                panel_id: None,
                verifier_member_id: verifier.member_id(),
                outcome: ReviewVerdictOutcome::Accepted,
                reviewed_diff_digest: [0x44; 32],
                rationale: "ok".to_owned(),
                recorded_at_ms: 160,
            })?
            .map_err(|e| format!("verdict: {e}"))?;
        runtime
            .abort_swarm_run(ws, run.swarm_run_id(), 170)?
            .map_err(|e| format!("abort: {e}"))?;
        Ok(run.swarm_run_id())
    }

    #[tokio::test]
    async fn settle_preflight_and_converge_reject_aborted_candidate_ready_run() -> TestResult {
        let dir = TempRuntimeDir::new("aborted-preflight")?;
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x850)));
        let runtime = OrchestrationRuntime::open(&dir.0)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?);
        let ws = WorkspaceId::from_uuid(Uuid::from_u128(0x851));
        let run_id = seed_aborted_candidate_ready_run(&runtime, ws)?;

        let state = BackendState::new(runtime, Arc::new(RealtimeHub::new()));
        let preflight = verify_settle_preflight(&state, principal, ws, run_id, Uuid::now_v7())
            .await
            .map_err(|e| format!("preflight api err: {e:?}"))?;
        assert!(matches!(
            preflight,
            Err(SwarmError::InvalidStateTransition(_))
        ));
        let conv = resolve_convergence_candidates(&state, principal, ws, run_id, Vec::new())
            .await
            .map_err(|e| format!("converge api err: {e:?}"))?;
        assert!(matches!(conv, Err(SwarmError::InvalidStateTransition(_))));
        Ok(())
    }
}
