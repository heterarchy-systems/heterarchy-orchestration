//! Operator-gated Axum HTTP adapters for Native Swarm coordination.
//!
//! Exposes `Standalone` and `DagNodeHybrid` Swarm run creation, full roster and
//! two-phase watermark inspection, area channels and subscriptions, spawn-depth
//! and role-gated member admission, `#seq` board messages with `@mention`
//! doorbells and incremental reads, single-flight turn claim/complete/fail/dispatch
//! and quota model switching, lineage-separated adversarial verifier panels and
//! verdicts, `converged_diff_digest` quorum convergence/settlement, and
//! deterministic zero-token Rust stall/livelock remediation.

mod error_mapping;
mod forge_bridge;
mod projections;

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    AddSwarmMemberCommand, ClaimSwarmWakeupCommand, CompleteSwarmTurnCommand,
    CreateSwarmChannelCommand, CreateSwarmRunCommand, CreateSwarmVerificationPanelCommand,
    FailOrSuspendSwarmTurnCommand, KillSwarmMemberCommand, MAX_SWARM_BOARD_PAGE_SIZE,
    MessageContext, PostSwarmMessageCommand, ReadSwarmBoardQuery, RecordSwarmVerdictCommand,
    SWARM_MAIN_CHANNEL_NAME, SettleStandaloneSwarmCommand, SubscribeSwarmChannelCommand,
    SwarmError, SwarmMemberIdentity, SwarmMemberState, SwarmMessageAuthor, SwarmRunState,
};
use heterarchy_orchestration_runtime::{RuntimeError, SwitchSwarmMemberModelCommand};
use uuid::Uuid;

use crate::realtime::RealtimeEventKind;
use crate::wire::autonomy_loop::parse_review_verdict_outcome;
use crate::wire::swarm::{
    AbortSwarmRunRequest, AddSwarmMemberRequest, ClaimSwarmWakeupRequest, CompleteSwarmTurnRequest,
    ConvergeSwarmRequest, ConvergeSwarmResponse, CreateSwarmChannelRequest, CreateSwarmRunRequest,
    CreateSwarmRunResponse, CreateSwarmVerificationPanelRequest, DispatchSwarmTurnRequest,
    DispatchSwarmTurnResponse, FailSwarmTurnRequest, KillSwarmMemberRequest,
    KillSwarmMemberResponse, ListSwarmRunsQuery, MAX_SWARM_HTTP_BODY_BYTES,
    PostSwarmMessageRequest, PostSwarmMessageResponse, ReadSwarmMessagesQuery,
    RecordSwarmVerdictRequest, SettleSwarmRequest, SubscribeSwarmChannelRequest,
    SubscribeSwarmChannelResponse, SwarmBoardPageResponse, SwarmChannelWire,
    SwarmConvergenceGateWire, SwarmConvergenceTarget, SwarmMemberWire, SwarmModelSwitchResponse,
    SwarmRunInspectionResponse, SwarmRunListResponse, SwarmRunWire, SwarmStallCheckRequest,
    SwarmStallCheckResponse, SwarmVerdictWire, SwarmVerificationPanelWire, SwarmWakeupBatchWire,
    SwitchSwarmMemberModelRequest, parse_non_zero_digest_hex, parse_optional_worktree,
    parse_swarm_commit_oid, parse_swarm_convergence_target, parse_swarm_role_kind,
    parse_swarm_turn_failure_disposition, parse_swarm_workspace_mode,
};
use crate::{ApiError, BackendState, canonical_uuid, current_timestamp_millis};
use error_mapping::{SwarmReply, map_swarm_dispatch_error, map_swarm_error};
use projections::{
    ParsedConvergenceInput, build_stall_policy_and_timestamp, execute_converge_swarm,
    load_full_inspection,
};

/// Register strict bearer-authenticated, operator-gated Swarm HTTP endpoints.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/v1/workspaces/{workspace_id}/swarms",
            get(list_swarms).post(create_swarm),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}",
            get(get_swarm_inspection),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/channels",
            post(create_channel),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/channels/{channel_id}/subscriptions",
            post(subscribe_channel),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members",
            post(add_member),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/kill",
            post(kill_member),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/claim",
            post(claim_wakeup_turn),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/complete",
            post(complete_member_turn),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/fail",
            post(fail_or_suspend_member_turn),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/dispatch",
            post(dispatch_member_turn),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/model-switch",
            post(switch_member_model),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/messages",
            get(read_messages).post(post_message),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/panels",
            post(create_panel),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/verdicts",
            post(record_verdict),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/converge",
            post(converge_swarm),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/forge-converge",
            post(forge_bridge::forge_converge_swarm),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/settle",
            post(settle_swarm),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/forge-settle",
            post(forge_bridge::forge_settle_swarm),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/abort",
            post(abort_swarm),
        )
        .route(
            "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/stall-check",
            post(check_stall),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/autonomy-outbox/sync",
            post(forge_bridge::sync_autonomy_outbox),
        )
        .layer(DefaultBodyLimit::max(MAX_SWARM_HTTP_BODY_BYTES))
}

fn authorize_workspace_scope(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    workspace_path: &str,
) -> Result<WorkspaceId, ApiError> {
    state
        .runtime
        .require_operator(principal)
        .map_err(ApiError::Runtime)?;
    Ok(WorkspaceId::from_uuid(canonical_uuid(workspace_path)?))
}

fn authorize_run_scope(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    workspace_path: &str,
    run_path: &str,
) -> Result<(WorkspaceId, Uuid), ApiError> {
    let workspace_id = authorize_workspace_scope(state, principal, workspace_path)?;
    let swarm_run_id = canonical_uuid(run_path)?;
    Ok((workspace_id, swarm_run_id))
}

fn authorize_member_scope(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    workspace_path: &str,
    run_path: &str,
    member_path: &str,
) -> Result<(WorkspaceId, Uuid, Uuid), ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(state, principal, workspace_path, run_path)?;
    let member_id = canonical_uuid(member_path)?;
    Ok((workspace_id, swarm_run_id, member_id))
}

async fn create_swarm(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreateSwarmRunRequest>,
) -> Result<SwarmReply<CreateSwarmRunResponse>, ApiError> {
    let workspace_id = authorize_workspace_scope(&state, principal, &workspace_path)?;
    let workspace_uuid = workspace_id.into_uuid();
    if let Some(scope) = &request.scope {
        crate::app_support::validate_workspace_reference(scope, None)?;
        if scope.workspace_id != workspace_uuid {
            return Err(ApiError::InvalidIdentity);
        }
        state.workspace_scope_proof(scope.clone()).await?;
    }
    let request_id = canonical_uuid(&request.request_id)?;
    let mode = request.mode.to_domain()?;
    let baseline_commit_oid = parse_swarm_commit_oid(&request.baseline_commit_oid)?;
    let baseline_tree_blake3 = parse_non_zero_digest_hex(&request.baseline_tree_blake3_hex)?;
    let created_at_ms = current_timestamp_millis()?;
    let command = CreateSwarmRunCommand {
        request_id,
        workspace_id,
        title: request.title,
        mode,
        max_members: request.max_members,
        max_spawn_depth: request.max_spawn_depth,
        max_total_turns: request.max_total_turns,
        required_verifier_quorum: request.required_verifier_quorum,
        allow_worker_all_mention: request.allow_worker_all_mention,
        baseline_commit_oid,
        baseline_tree_blake3,
        created_at_ms,
    };
    let publisher = state.clone();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let run = match runtime.create_swarm_run(&command)? {
                Ok(run) => run,
                Err(err) => return Ok(Err(err)),
            };
            let main_channel = match runtime.get_swarm_channel_by_name(
                workspace_id,
                run.swarm_run_id(),
                SWARM_MAIN_CHANNEL_NAME,
            )? {
                Ok(Some(ch)) => ch,
                Ok(None) => return Ok(Err(SwarmError::NotFound("swarm main channel"))),
                Err(err) => return Ok(Err(err)),
            };
            if !run.replayed() {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmRunChanged,
                    run.swarm_run_id(),
                    run.revision(),
                    Some(workspace_uuid),
                );
            }
            Ok(Ok((run, main_channel)))
        })
        .await?;
    match outcome {
        Ok((run, main_channel)) => {
            let status = if run.replayed() {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            Ok(SwarmReply::Success(
                status,
                CreateSwarmRunResponse {
                    run: SwarmRunWire::from(&run),
                    main_channel: SwarmChannelWire::from(&main_channel),
                },
            ))
        }
        Err(err) => map_swarm_error(err),
    }
}

async fn list_swarms(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Query(query): Query<ListSwarmRunsQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<SwarmReply<SwarmRunListResponse>, ApiError> {
    let workspace_id = authorize_workspace_scope(&state, principal, &workspace_path)?;
    let limit = query.limit.unwrap_or(MAX_SWARM_BOARD_PAGE_SIZE);
    let outcome = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.list_swarm_runs(workspace_id, limit)
        })
        .await?;
    match outcome {
        Ok(runs) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SwarmRunListResponse {
                runs: runs.iter().map(SwarmRunWire::from).collect(),
            },
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn get_swarm_inspection(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<SwarmReply<SwarmRunInspectionResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let outcome = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            load_full_inspection(runtime, workspace_id, swarm_run_id)
        })
        .await?;
    match outcome {
        Ok(Some(inspection)) => Ok(SwarmReply::Success(StatusCode::OK, inspection)),
        Ok(None) => Err(ApiError::NotFound),
        Err(err) => map_swarm_error(err),
    }
}

async fn create_channel(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreateSwarmChannelRequest>,
) -> Result<SwarmReply<SwarmChannelWire>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let channel_id = canonical_uuid(&request.channel_id)?;
    let created_by_member_id = request
        .created_by_member_id
        .as_deref()
        .map(canonical_uuid)
        .transpose()?;
    let created_at_ms = current_timestamp_millis()?;
    let command = CreateSwarmChannelCommand {
        channel_id,
        workspace_id,
        swarm_run_id,
        name: request.name,
        purpose: request.purpose,
        created_by_member_id,
        created_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let already_existed = matches!(
                runtime.get_swarm_run_snapshot(workspace_id, swarm_run_id)?,
                Ok(Some(snap))
                    if snap.channels().iter().any(|ch| ch.channel_id() == command.channel_id)
            );
            let created = runtime.create_swarm_channel(&command)?;
            if let Ok(ref channel) = created
                && !already_existed
            {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmChannelChanged,
                    channel.channel_id(),
                    1,
                    Some(ws_uuid),
                );
            }
            Ok(created)
        })
        .await?;
    match outcome {
        Ok(channel) => Ok(SwarmReply::Success(
            StatusCode::CREATED,
            SwarmChannelWire::from(&channel),
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn subscribe_channel(
    State(state): State<BackendState>,
    Path((workspace_path, run_path, channel_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SubscribeSwarmChannelRequest>,
) -> Result<SwarmReply<SubscribeSwarmChannelResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let channel_id = canonical_uuid(&channel_path)?;
    let member_id = canonical_uuid(&request.member_id)?;
    let subscribed_at_ms = current_timestamp_millis()?;
    let command = SubscribeSwarmChannelCommand {
        workspace_id,
        swarm_run_id,
        channel_id,
        member_id,
        subscribed_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let subscribed = runtime.subscribe_swarm_channel(command)?;
            if let Ok(true) = subscribed {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmChannelChanged,
                    channel_id,
                    1,
                    Some(ws_uuid),
                );
            }
            Ok(subscribed)
        })
        .await?;
    match outcome {
        Ok(newly_subscribed) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SubscribeSwarmChannelResponse {
                swarm_run_id,
                channel_id,
                member_id,
                newly_subscribed,
            },
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn add_member(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<AddSwarmMemberRequest>,
) -> Result<SwarmReply<SwarmMemberWire>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let member_id = canonical_uuid(&request.member_id)?;
    let role_kind = parse_swarm_role_kind(&request.role_kind)?;
    let parent_member_id = request
        .parent_member_id
        .as_deref()
        .map(canonical_uuid)
        .transpose()?;
    let workspace_mode = parse_swarm_workspace_mode(&request.workspace_mode)?;
    let worktree_path = match parse_optional_worktree(request.worktree_path.as_deref())? {
        Ok(valid) => valid,
        Err(rejected) => return Ok(SwarmReply::DomainRejected(StatusCode::CONFLICT, rejected)),
    };
    let identity = SwarmMemberIdentity::from(request.identity.to_author_identity()?);
    let added_at_ms = current_timestamp_millis()?;
    let command = AddSwarmMemberCommand {
        member_id,
        workspace_id,
        swarm_run_id,
        roster_name: request.roster_name,
        role_kind,
        parent_member_id,
        workspace_mode,
        worktree_path,
        identity,
        initial_channels: request.initial_channels,
        added_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let already_existed = matches!(
                runtime.get_swarm_run_snapshot(workspace_id, swarm_run_id)?,
                Ok(Some(snap))
                    if snap.members().iter().any(|m| m.member_id() == command.member_id)
            );
            let added = runtime.add_swarm_member(&command)?;
            if let Ok(ref member) = added
                && !already_existed
            {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmMemberStateChanged,
                    member.member_id(),
                    member.attempt_epoch().max(1),
                    Some(ws_uuid),
                );
            }
            Ok(added)
        })
        .await?;
    match outcome {
        Ok(member) => Ok(SwarmReply::Success(
            StatusCode::CREATED,
            SwarmMemberWire::from(&member),
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn kill_member(
    State(state): State<BackendState>,
    Path((workspace_path, run_path, member_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(_request): Json<KillSwarmMemberRequest>,
) -> Result<SwarmReply<KillSwarmMemberResponse>, ApiError> {
    let (workspace_id, swarm_run_id, member_id) =
        authorize_member_scope(&state, principal, &workspace_path, &run_path, &member_path)?;
    let killed_at_ms = current_timestamp_millis()?;
    let command = KillSwarmMemberCommand {
        workspace_id,
        swarm_run_id,
        member_id,
        killed_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let previously_killed =
                match runtime.get_swarm_run_snapshot(workspace_id, swarm_run_id)? {
                    Ok(Some(snap)) => snap
                        .members()
                        .iter()
                        .filter(|m| m.member_state() == SwarmMemberState::Killed)
                        .map(heterarchy_orchestration_operations::SwarmMemberRecord::member_id)
                        .collect::<std::collections::BTreeSet<_>>(),
                    _ => std::collections::BTreeSet::new(),
                };
            let result = runtime.kill_swarm_member(command)?;
            if let Ok(ref killed) = result {
                for member in killed {
                    if !previously_killed.contains(&member.member_id()) {
                        publisher.publish_state_changed(
                            RealtimeEventKind::SwarmMemberStateChanged,
                            member.member_id(),
                            member.attempt_epoch().max(1),
                            Some(ws_uuid),
                        );
                    }
                }
            }
            Ok(result)
        })
        .await?;
    match outcome {
        Ok(killed) => Ok(SwarmReply::Success(
            StatusCode::OK,
            KillSwarmMemberResponse {
                killed_members: killed.iter().map(SwarmMemberWire::from).collect(),
            },
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn post_message(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<PostSwarmMessageRequest>,
) -> Result<SwarmReply<PostSwarmMessageResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let message_id = canonical_uuid(&request.message_id)?;
    let explicit_channel_id = request
        .channel_id
        .as_deref()
        .map(canonical_uuid)
        .transpose()?;
    let author = match request.author_member_id.as_deref() {
        Some(member_raw) => SwarmMessageAuthor::Member(canonical_uuid(member_raw)?),
        None => SwarmMessageAuthor::System,
    };
    let submitted_at_ms = current_timestamp_millis()?;
    let body = request.body;
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let channel_id = match explicit_channel_id {
                Some(id) => id,
                None => {
                    match runtime.get_swarm_channel_by_name(
                        workspace_id,
                        swarm_run_id,
                        SWARM_MAIN_CHANNEL_NAME,
                    )? {
                        Ok(Some(ch)) => ch.channel_id(),
                        Ok(None) => return Ok(Err(SwarmError::NotFound("swarm run"))),
                        Err(err) => return Ok(Err(err)),
                    }
                }
            };
            let command = PostSwarmMessageCommand {
                message_id,
                workspace_id,
                swarm_run_id,
                channel_id,
                author,
                body,
                context: MessageContext::default(),
                submitted_at_ms,
            };
            let posted = runtime.post_swarm_message(&command)?;
            if let Ok(ref outcome) = posted
                && !outcome.replayed()
            {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmBoardMessagePosted,
                    outcome.message().message_id(),
                    outcome.message().sequence().max(1),
                    Some(ws_uuid),
                );
            }
            Ok(posted)
        })
        .await?;
    match outcome {
        Ok(posted) => {
            let status = if posted.replayed() {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            Ok(SwarmReply::Success(
                status,
                PostSwarmMessageResponse::from(&posted),
            ))
        }
        Err(err) => map_swarm_error(err),
    }
}

async fn read_messages(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Query(query): Query<ReadSwarmMessagesQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<SwarmReply<SwarmBoardPageResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let channel_id = query
        .channel_id
        .as_deref()
        .map(canonical_uuid)
        .transpose()?;
    let limit = query.limit.unwrap_or(MAX_SWARM_BOARD_PAGE_SIZE);
    let board_query = ReadSwarmBoardQuery {
        workspace_id,
        swarm_run_id,
        channel_id,
        after_sequence: query.since_sequence,
        limit,
    };
    let outcome = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.read_swarm_board_since(board_query)
        })
        .await?;
    match outcome {
        Ok(page) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SwarmBoardPageResponse::from(&page),
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn claim_wakeup_turn(
    State(state): State<BackendState>,
    Path((workspace_path, run_path, member_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ClaimSwarmWakeupRequest>,
) -> Result<SwarmReply<SwarmWakeupBatchWire>, ApiError> {
    let (workspace_id, swarm_run_id, member_id) =
        authorize_member_scope(&state, principal, &workspace_path, &run_path, &member_path)?;
    let snapshot_id = canonical_uuid(&request.snapshot_id)?;
    let claimed_at_ms = current_timestamp_millis()?;
    let command = ClaimSwarmWakeupCommand {
        workspace_id,
        swarm_run_id,
        member_id,
        snapshot_id,
        claimed_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let claimed = runtime.claim_swarm_wakeup_batch(command)?;
            if let Ok(ref batch) = claimed {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmMemberStateChanged,
                    batch.member().member_id(),
                    batch.member().attempt_epoch().max(1),
                    Some(ws_uuid),
                );
            }
            Ok(claimed)
        })
        .await?;
    match outcome {
        Ok(batch) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SwarmWakeupBatchWire::from(&batch),
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn complete_member_turn(
    State(state): State<BackendState>,
    Path((workspace_path, run_path, member_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CompleteSwarmTurnRequest>,
) -> Result<SwarmReply<SwarmMemberWire>, ApiError> {
    let (workspace_id, swarm_run_id, member_id) =
        authorize_member_scope(&state, principal, &workspace_path, &run_path, &member_path)?;
    let expected_snapshot_id = canonical_uuid(&request.expected_snapshot_id)?;
    let output = request.to_turn_output()?;
    let completed_at_ms = current_timestamp_millis()?;
    let command = CompleteSwarmTurnCommand {
        workspace_id,
        swarm_run_id,
        member_id,
        expected_snapshot_id,
        output,
        completed_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let completed = runtime.complete_swarm_member_turn(&command)?;
            if let Ok(ref member) = completed {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmMemberStateChanged,
                    member.member_id(),
                    member.attempt_epoch().max(1),
                    Some(ws_uuid),
                );
            }
            Ok(completed)
        })
        .await?;
    match outcome {
        Ok(member) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SwarmMemberWire::from(&member),
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn fail_or_suspend_member_turn(
    State(state): State<BackendState>,
    Path((workspace_path, run_path, member_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<FailSwarmTurnRequest>,
) -> Result<SwarmReply<SwarmMemberWire>, ApiError> {
    let (workspace_id, swarm_run_id, member_id) =
        authorize_member_scope(&state, principal, &workspace_path, &run_path, &member_path)?;
    let expected_snapshot_id = canonical_uuid(&request.expected_snapshot_id)?;
    let disposition = parse_swarm_turn_failure_disposition(&request.disposition)?;
    let observed_at_ms = current_timestamp_millis()?;
    let command = FailOrSuspendSwarmTurnCommand {
        workspace_id,
        swarm_run_id,
        member_id,
        expected_snapshot_id,
        disposition,
        consume_turn: request.consume_turn,
        observed_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let released = runtime.fail_or_suspend_swarm_member_turn(command)?;
            if let Ok(ref member) = released {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmMemberStateChanged,
                    member.member_id(),
                    member.attempt_epoch().max(1),
                    Some(ws_uuid),
                );
            }
            Ok(released)
        })
        .await?;
    match outcome {
        Ok(member) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SwarmMemberWire::from(&member),
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn dispatch_member_turn(
    State(state): State<BackendState>,
    Path((workspace_path, run_path, member_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<DispatchSwarmTurnRequest>,
) -> Result<SwarmReply<DispatchSwarmTurnResponse>, ApiError> {
    let (workspace_id, swarm_run_id, member_id) =
        authorize_member_scope(&state, principal, &workspace_path, &run_path, &member_path)?;
    let fallback_now_ms = current_timestamp_millis()?;
    let dispatch_request =
        match request.to_domain(workspace_id, swarm_run_id, member_id, fallback_now_ms)? {
            Ok(valid) => valid,
            Err(rejected) => return Ok(SwarmReply::DomainRejected(StatusCode::CONFLICT, rejected)),
        };
    if let Some(raw_session_id) = forge_bridge::should_use_forge_bridge(&request) {
        return forge_bridge::dispatch_with_forge_session(
            &state,
            principal,
            dispatch_request,
            raw_session_id,
        )
        .await;
    }
    match state
        .run_swarm_dispatch(principal, dispatch_request)
        .await?
    {
        Ok(outcome) => Ok(SwarmReply::Success(
            StatusCode::OK,
            DispatchSwarmTurnResponse::from(&outcome),
        )),
        Err(err) => map_swarm_dispatch_error(err),
    }
}

async fn switch_member_model(
    State(state): State<BackendState>,
    Path((workspace_path, run_path, member_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SwitchSwarmMemberModelRequest>,
) -> Result<SwarmReply<SwarmModelSwitchResponse>, ApiError> {
    let (workspace_id, swarm_run_id, member_id) =
        authorize_member_scope(&state, principal, &workspace_path, &run_path, &member_path)?;
    let target = request.to_target()?;
    let updated_at_ms = current_timestamp_millis()?;
    let command = SwitchSwarmMemberModelCommand {
        workspace_id,
        swarm_run_id,
        member_id,
        target,
        updated_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let switched = runtime.switch_swarm_member_model_and_requeue(&command)?;
            if let Ok(ref res) = switched {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmMemberStateChanged,
                    res.member.member_id(),
                    res.member.attempt_epoch().max(1),
                    Some(ws_uuid),
                );
            }
            Ok(switched)
        })
        .await?;
    match outcome {
        Ok(switched) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SwarmModelSwitchResponse::from(&switched),
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn create_panel(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreateSwarmVerificationPanelRequest>,
) -> Result<SwarmReply<SwarmVerificationPanelWire>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let author_member_ids = request
        .author_member_ids
        .iter()
        .map(|raw| canonical_uuid(raw))
        .collect::<Result<Vec<_>, _>>()?;
    let verifier_member_ids = request
        .verifier_member_ids
        .iter()
        .map(|raw| canonical_uuid(raw))
        .collect::<Result<Vec<_>, _>>()?;
    let created_at_ms = current_timestamp_millis()?;
    let command = CreateSwarmVerificationPanelCommand {
        request_id,
        workspace_id,
        swarm_run_id,
        author_member_ids,
        verifier_member_ids,
        required_quorum: request.required_quorum,
        created_at_ms,
    };
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            runtime.create_swarm_verification_panel(&command)
        })
        .await?;
    match outcome {
        Ok(panel) => {
            let status = if panel.replayed() {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            Ok(SwarmReply::Success(
                status,
                SwarmVerificationPanelWire::from(&panel),
            ))
        }
        Err(err) => map_swarm_error(err),
    }
}

async fn record_verdict(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<RecordSwarmVerdictRequest>,
) -> Result<SwarmReply<SwarmVerdictWire>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let panel_id = request
        .panel_id
        .as_deref()
        .map(canonical_uuid)
        .transpose()?;
    let verifier_member_id = canonical_uuid(&request.verifier_member_id)?;
    let outcome = parse_review_verdict_outcome(&request.outcome)?;
    let reviewed_diff_digest = parse_non_zero_digest_hex(&request.reviewed_diff_digest_hex)?;
    let recorded_at_ms = current_timestamp_millis()?;
    let command = RecordSwarmVerdictCommand {
        request_id,
        workspace_id,
        swarm_run_id,
        panel_id,
        verifier_member_id,
        outcome,
        reviewed_diff_digest,
        rationale: request.rationale,
        recorded_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let result = state
        .run_operator_mutation(principal, move |runtime| {
            let recorded = runtime.record_swarm_verdict(&command)?;
            if let Ok(ref verdict) = recorded
                && !verdict.replayed()
            {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmVerdictRecorded,
                    verdict.verdict_id(),
                    1,
                    Some(ws_uuid),
                );
            }
            Ok(recorded)
        })
        .await?;
    match result {
        Ok(verdict) => {
            let status = if verdict.replayed() {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            Ok(SwarmReply::Success(
                status,
                SwarmVerdictWire::from(&verdict),
            ))
        }
        Err(err) => map_swarm_error(err),
    }
}

async fn converge_swarm(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ConvergeSwarmRequest>,
) -> Result<SwarmReply<ConvergeSwarmResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let converged_commit_oid = parse_swarm_commit_oid(&request.converged_commit_oid)?;
    let converged_tree_blake3 = parse_non_zero_digest_hex(&request.converged_tree_blake3_hex)?;
    let converged_diff_digest = parse_non_zero_digest_hex(&request.converged_diff_digest_hex)?;
    let target = request
        .target_state
        .as_deref()
        .map(parse_swarm_convergence_target)
        .transpose()?;
    if request.stage_candidate_only && target == Some(SwarmConvergenceTarget::Settled) {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }
    let observed_at_ms = current_timestamp_millis()?;
    let input = ParsedConvergenceInput {
        request_id,
        workspace_id,
        swarm_run_id,
        converged_commit_oid,
        converged_tree_blake3,
        converged_diff_digest,
        target,
        stage_candidate_only: request.stage_candidate_only,
        observed_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let converged = execute_converge_swarm(runtime, &input)?;
            if let Ok((ref run, _, _)) = converged
                && !run.replayed()
            {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmRunChanged,
                    run.swarm_run_id(),
                    run.revision(),
                    Some(ws_uuid),
                );
            }
            Ok(converged)
        })
        .await?;
    match outcome {
        Ok((run, settled, gate)) => Ok(SwarmReply::Success(
            StatusCode::OK,
            ConvergeSwarmResponse {
                run: SwarmRunWire::from(&run),
                settled,
                gate: gate.as_ref().map(SwarmConvergenceGateWire::from),
            },
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn settle_swarm(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SettleSwarmRequest>,
) -> Result<SwarmReply<ConvergeSwarmResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let settled_at_ms = current_timestamp_millis()?;
    let command = SettleStandaloneSwarmCommand {
        request_id,
        workspace_id,
        swarm_run_id,
        settled_at_ms,
    };
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let settled_run = match runtime.settle_standalone_swarm_run(command)? {
                Ok(run) => run,
                Err(err) => return Ok(Err(err)),
            };
            let gate = match runtime.verify_swarm_convergence_gate(workspace_id, swarm_run_id)? {
                Ok(gate) => gate,
                Err(err) => return Ok(Err(err)),
            };
            if !settled_run.replayed() {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmRunChanged,
                    settled_run.swarm_run_id(),
                    settled_run.revision(),
                    Some(ws_uuid),
                );
            }
            Ok(Ok((settled_run, gate)))
        })
        .await?;
    match outcome {
        Ok((run, gate)) => Ok(SwarmReply::Success(
            StatusCode::OK,
            ConvergeSwarmResponse {
                run: SwarmRunWire::from(&run),
                settled: true,
                gate: Some(SwarmConvergenceGateWire::from(&gate)),
            },
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn abort_swarm(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(_request): Json<AbortSwarmRunRequest>,
) -> Result<SwarmReply<SwarmRunWire>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let observed_at_ms = current_timestamp_millis()?;
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let was_aborted = matches!(
                runtime.get_swarm_run(workspace_id, swarm_run_id)?,
                Ok(Some(existing)) if existing.state() == SwarmRunState::Aborted
            );
            let aborted = runtime.abort_swarm_run(workspace_id, swarm_run_id, observed_at_ms)?;
            if let Ok(ref run) = aborted
                && !was_aborted
            {
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmRunChanged,
                    run.swarm_run_id(),
                    run.revision(),
                    Some(ws_uuid),
                );
            }
            Ok(aborted)
        })
        .await?;
    match outcome {
        Ok(aborted) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SwarmRunWire::from(&aborted),
        )),
        Err(err) => map_swarm_error(err),
    }
}

async fn check_stall(
    State(state): State<BackendState>,
    Path((workspace_path, run_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SwarmStallCheckRequest>,
) -> Result<SwarmReply<SwarmStallCheckResponse>, ApiError> {
    let (workspace_id, swarm_run_id) =
        authorize_run_scope(&state, principal, &workspace_path, &run_path)?;
    let (policy, observed_at_ms) = build_stall_policy_and_timestamp(&request)?;
    let publisher = state.clone();
    let ws_uuid = workspace_id.into_uuid();
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            let evaluated = runtime.evaluate_swarm_stall_and_livelock(
                workspace_id,
                swarm_run_id,
                policy,
                observed_at_ms,
            )?;
            if let Ok(ref _report) = evaluated {
                let rev = runtime
                    .get_swarm_run(workspace_id, swarm_run_id)?
                    .ok()
                    .flatten()
                    .map_or(1, |run| run.revision().max(1));
                publisher.publish_state_changed(
                    RealtimeEventKind::SwarmStallEvaluated,
                    swarm_run_id,
                    rev,
                    Some(ws_uuid),
                );
            }
            Ok(evaluated)
        })
        .await?;
    match outcome {
        Ok(report) => Ok(SwarmReply::Success(
            StatusCode::OK,
            SwarmStallCheckResponse::from(&report),
        )),
        Err(err) => map_swarm_error(err),
    }
}
