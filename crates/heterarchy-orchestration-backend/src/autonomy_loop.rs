//! Operator-gated Axum HTTP adapters for Orchestration autonomy-loop operations.
//!
//! Covers Workspace project scope registration, independent reviewer assignment,
//! diff-digest-bound review verdicts, pre-completion review gating, immutable
//! pre-spawn `AssignmentExecutionSnapshot` preparation, and `DagSettled`
//! authority/governance queries. All business invariants live in `Runtime` and
//! `Operations`; handlers only extract, syntactically convert, delegate, and
//! project typed outcomes into HTTP responses.

use std::path::PathBuf;

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use forge_domain::{
    ActivityId, AgentAssignmentId, AuthenticatedPrincipal, DagNodeId, DagRevisionNumber,
    DirectTokenId, GoalId, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AssignmentExecutionSnapshotParts, ExecutionAttemptKey, ExecutionPermissions, ExecutionTimeouts,
    ForgeWorktreeRef, LeaseRef, MAX_REVIEWER_CANDIDATE_POOL, McpGrant, OperationalActor,
    OperationsError, ReviewVerdictCommand, ReviewerAssignmentCommand, ReviewerAssignmentError,
    ReviewerAssignmentRecord, ScopeRegistrationCommand, ScopeUnregisterCommand,
    SnapshotValidationError, WorkspaceProjectScope,
    reviewer_qualification_rule::QualificationDomain,
};
use heterarchy_orchestration_runtime::RuntimeError;
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileRevision, CliEngine, RolePromptDigest,
};
use uuid::Uuid;

use crate::wire::autonomy_loop::{
    AssignReviewerRequest, DagSettlementAuthorityCheckRequest, DagSettlementAuthorityResponse,
    DagSettlementGovernanceQuery, DagSettlementGovernanceResponse, ExecutionAuthorWire,
    ExecutionSnapshotOutcomeResponse, ExecutionSnapshotQuery, ExecutionSnapshotQueryResponse,
    LaunchLedgerRecordWire, MAX_AUTONOMY_LOOP_BODY_BYTES, McpGrantWire,
    PrepareExecutionSnapshotRequest, RecordReviewVerdictRequest, RegisteredWorkspaceScopeResponse,
    ReviewerCandidateWire, ReviewerOperationResponse, ScopeRegisterRequest, ScopeUnregisterRequest,
    VerifyNodeCompletionGateRequest, WorkspaceScopeQueryResponse, parse_review_verdict_outcome,
    parse_sha256_hex,
};
use crate::{ApiError, BackendState, canonical_uuid, current_timestamp_millis};

/// Register strict operator-only autonomy-loop HTTP endpoints.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/scopes",
            get(list_scopes).post(register_scope),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/scopes",
            get(get_scope),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/scopes/unregister",
            post(unregister_scope),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-assignments",
            post(assign_reviewer),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-verdicts",
            post(record_verdict),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/node-completion-gate",
            post(verify_completion_gate),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/execution-snapshots",
            get(get_execution_snapshot).post(prepare_execution_snapshot),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/dag-settlement-authority",
            post(check_dag_settlement_authority),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/dag-settlement-governance",
            get(get_dag_settlement_governance),
        )
        .layer(DefaultBodyLimit::max(MAX_AUTONOMY_LOOP_BODY_BYTES))
}

async fn get_scope(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<WorkspaceScopeQueryResponse>, ApiError> {
    let workspace_id = canonical_uuid(&workspace_path)?;
    let scope = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            let records = runtime.list_active_workspace_scopes()?;
            Ok(records
                .into_iter()
                .find(|record| record.scope().workspace_id() == workspace_id)
                .map(|record| RegisteredWorkspaceScopeResponse::from(&record)))
        })
        .await?;
    Ok(Json(WorkspaceScopeQueryResponse {
        workspace_id,
        scope,
    }))
}

async fn list_scopes(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<Vec<RegisteredWorkspaceScopeResponse>>, ApiError> {
    let records = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.list_active_workspace_scopes()
        })
        .await?;
    Ok(Json(
        records
            .iter()
            .map(RegisteredWorkspaceScopeResponse::from)
            .collect(),
    ))
}

async fn register_scope(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ScopeRegisterRequest>,
) -> Result<Json<RegisteredWorkspaceScopeResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&request.scope, None)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let observed_at_ms = current_timestamp_millis()?;
    let scope = WorkspaceProjectScope::new(
        request.scope.workspace_id,
        request.scope.location_id,
        request.scope.location_revision,
        request.scope.workspace_revision,
        request.scope.locator,
    )
    .map_err(RuntimeError::from)
    .map_err(ApiError::Runtime)?;
    let command =
        ScopeRegistrationCommand::new(request_id, request.expected_revision, scope, observed_at_ms)
            .map_err(RuntimeError::from)
            .map_err(ApiError::Runtime)?;
    let record = state
        .run_operator_mutation(principal, move |runtime| {
            runtime.register_workspace_scope(&command)
        })
        .await?;
    Ok(Json(RegisteredWorkspaceScopeResponse::from(&record)))
}

async fn unregister_scope(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ScopeUnregisterRequest>,
) -> Result<Json<RegisteredWorkspaceScopeResponse>, ApiError> {
    let workspace_id = canonical_uuid(&workspace_path)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let observed_at_ms = current_timestamp_millis()?;
    let command = ScopeUnregisterCommand::new(
        request_id,
        WorkspaceId::from_uuid(workspace_id),
        request.expected_revision,
        observed_at_ms,
    )
    .map_err(RuntimeError::from)
    .map_err(ApiError::Runtime)?;
    let record = state
        .run_operator_mutation(principal, move |runtime| {
            runtime.unregister_workspace_scope(&command)
        })
        .await?;
    Ok(Json(RegisteredWorkspaceScopeResponse::from(&record)))
}

async fn assign_reviewer(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<AssignReviewerRequest>,
) -> Result<(StatusCode, Json<ReviewerOperationResponse>), ApiError> {
    if request.candidates.len() > MAX_REVIEWER_CANDIDATE_POOL {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }
    let workspace_id = canonical_uuid(&workspace_path)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let correlation = request.correlation.to_correlation(workspace_id)?;
    let executor = request.executor.to_author_identity()?;
    let assigning_leader = request.assigning_leader.to_author_identity()?;
    let domain = QualificationDomain::new(request.required_domain)
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    let candidates = request
        .candidates
        .iter()
        .map(ReviewerCandidateWire::to_candidate_identity)
        .collect::<Result<Vec<_>, _>>()?;
    let assigned_at_ms = current_timestamp_millis()?;
    let command = ReviewerAssignmentCommand {
        request_id,
        correlation,
        assigning_leader,
        executor,
        candidates,
        qualification_domain: Some(domain),
        assigned_at_ms,
    };
    let outcome = state
        .run_operator_mutation(principal, move |runtime| {
            runtime.assign_independent_reviewer(&command)
        })
        .await?;
    map_reviewer_outcome(outcome)
}

async fn record_verdict(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<RecordReviewVerdictRequest>,
) -> Result<(StatusCode, Json<ReviewerOperationResponse>), ApiError> {
    if request.expected_revision == 0 || i64::try_from(request.expected_revision).is_err() {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }
    let workspace_id = canonical_uuid(&workspace_path)?;
    let request_id = canonical_uuid(&request.request_id)?;
    let reviewer_agent_id = canonical_uuid(&request.reviewer_agent_id)?;
    let reviewer_session_id = canonical_uuid(&request.reviewer_session_id)?;
    let submitted_reviewer = request
        .reviewer
        .as_ref()
        .map(ExecutionAuthorWire::to_author_identity)
        .transpose()?;
    let correlation = request.correlation.to_correlation(workspace_id)?;
    let outcome = parse_review_verdict_outcome(&request.verdict)?;
    let handoff_diff_digest = parse_sha256_hex(&request.handoff_diff_digest_hex)?;
    if handoff_diff_digest == [0_u8; 32] {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }
    let expected_revision = request.expected_revision;
    let recorded_at_ms = current_timestamp_millis()?;
    let result = state
        .run_operator_mutation(principal, move |runtime| {
            let Some(existing) = runtime.get_reviewer_assignment(correlation)? else {
                return Ok(Err(ReviewerAssignmentError::Operations(
                    OperationsError::Conflict("reviewer assignment missing for correlation"),
                )));
            };
            if let Err(err) = runtime.verify_reviewer_session_binding(
                WorkspaceId::from_uuid(workspace_id),
                existing.reviewer().participation_id(),
                reviewer_agent_id,
                reviewer_session_id,
            ) {
                return Ok(Err(err));
            }
            let (reviewer_principal, reviewer_profile_id, reviewer_participant_id) =
                if let Some(submitted) = submitted_reviewer {
                    (
                        submitted.principal(),
                        submitted.profile_id(),
                        submitted.participant_id(),
                    )
                } else {
                    let reviewer = existing.reviewer();
                    (
                        reviewer.principal(),
                        reviewer.profile_id(),
                        reviewer.participant_id(),
                    )
                };
            let command = ReviewVerdictCommand {
                request_id,
                correlation,
                expected_revision,
                reviewer_principal,
                reviewer_profile_id,
                reviewer_participant_id,
                outcome,
                handoff_diff_digest,
                rationale: format!(
                    "verdict {} recorded for agent {reviewer_agent_id} session {reviewer_session_id}",
                    outcome.as_str()
                ),
                recorded_at_ms,
            };
            runtime.record_review_verdict(&command)
        })
        .await?;
    map_reviewer_outcome(result)
}

async fn verify_completion_gate(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<VerifyNodeCompletionGateRequest>,
) -> Result<(StatusCode, Json<ReviewerOperationResponse>), ApiError> {
    let workspace_id = canonical_uuid(&workspace_path)?;
    let correlation = request.correlation.to_correlation(workspace_id)?;
    let current_handoff_diff_digest = parse_sha256_hex(&request.current_handoff_diff_digest_hex)?;
    let result = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.verify_node_completion_review_gate(correlation, current_handoff_diff_digest)
        })
        .await?;
    map_reviewer_outcome(result)
}

async fn prepare_execution_snapshot(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<PrepareExecutionSnapshotRequest>,
) -> Result<(StatusCode, Json<ExecutionSnapshotOutcomeResponse>), ApiError> {
    let workspace_id = canonical_uuid(&workspace_path)?;
    let committed_at_ms = current_timestamp_millis()?;
    let prepared_input = build_snapshot_parts(workspace_id, committed_at_ms, request)?;
    match prepared_input {
        Err(validation_err) => {
            state
                .run_catalog(move |runtime| runtime.require_operator(principal))
                .await?;
            map_snapshot_validation_error(&validation_err)
        }
        Ok((request_id, parts)) => {
            let outcome = state
                .run_operator_mutation(principal, move |runtime| {
                    runtime.prepare_assignment_execution_snapshot(request_id, parts)
                })
                .await?;
            match outcome {
                Ok(record) => Ok((
                    StatusCode::OK,
                    Json(ExecutionSnapshotOutcomeResponse::committed(&record)),
                )),
                Err(validation_err) => map_snapshot_validation_error(&validation_err),
            }
        }
    }
}

async fn get_execution_snapshot(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Query(query): Query<ExecutionSnapshotQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ExecutionSnapshotQueryResponse>, ApiError> {
    let workspace_id = canonical_uuid(&workspace_path)?;
    let attempt_key = query.to_attempt_key(workspace_id)?;
    let record = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.get_assignment_execution_snapshot(attempt_key)
        })
        .await?;
    Ok(Json(ExecutionSnapshotQueryResponse {
        workspace_id,
        goal_id: attempt_key.goal_id().into_uuid(),
        dag_revision: attempt_key.dag_revision().get(),
        node_id: attempt_key.node_id().into_uuid(),
        assignment_id: attempt_key.assignment_id().into_uuid(),
        attempt: attempt_key.attempt(),
        record: record.as_ref().map(LaunchLedgerRecordWire::from),
    }))
}

async fn check_dag_settlement_authority(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<DagSettlementAuthorityCheckRequest>,
) -> Result<Json<DagSettlementAuthorityResponse>, ApiError> {
    if request.expected_leadership_revision < 0 {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }
    let workspace_id = canonical_uuid(&workspace_path)?;
    let actor = match &request.participant {
        Some(participant) => participant.to_actor()?,
        None => OperationalActor::LocalOperator,
    };
    let expected_revision = request.expected_leadership_revision;
    let authority = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.authorize_workspace_dag_settlement(
                WorkspaceId::from_uuid(workspace_id),
                actor,
                expected_revision,
            )
        })
        .await?;
    Ok(Json(DagSettlementAuthorityResponse::from(authority)))
}

async fn get_dag_settlement_governance(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Query(query): Query<DagSettlementGovernanceQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<DagSettlementGovernanceResponse>, ApiError> {
    if query.dag_revision < 1 {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }
    let workspace_id = canonical_uuid(&workspace_path)?;
    let goal_id = canonical_uuid(&query.goal_id)?;
    let dag_revision = query.dag_revision;
    let link = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            runtime.dag_settlement_governance_link(
                WorkspaceId::from_uuid(workspace_id),
                GoalId::from_uuid(goal_id),
                dag_revision,
            )
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(DagSettlementGovernanceResponse::from(link)))
}

fn build_snapshot_parts(
    workspace_id: Uuid,
    committed_at_ms: i64,
    request: PrepareExecutionSnapshotRequest,
) -> Result<Result<(Uuid, AssignmentExecutionSnapshotParts), SnapshotValidationError>, ApiError> {
    let request_id = canonical_uuid(&request.request_id)?;
    let snapshot_id = canonical_uuid(&request.snapshot_id)?;
    let goal_id = GoalId::from_uuid(canonical_uuid(&request.goal_id)?);
    let dag_revision = DagRevisionNumber::new(request.dag_revision)
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    let node_id = DagNodeId::from_uuid(canonical_uuid(&request.node_id)?);
    let assignment_id = AgentAssignmentId::from_uuid(canonical_uuid(&request.assignment_id)?);
    let attempt_key = ExecutionAttemptKey::new(
        WorkspaceId::from_uuid(workspace_id),
        goal_id,
        dag_revision,
        node_id,
        assignment_id,
        request.attempt,
    )
    .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    let activity_id = ActivityId::from_uuid(canonical_uuid(&request.activity_id)?);
    let profile_id = AgentProfileId::from_uuid(canonical_uuid(&request.profile_id)?)
        .map_err(|_| ApiError::InvalidIdentity)?;
    let profile_revision = AgentProfileRevision::new(request.profile_revision)
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    let resolved_engine = CliEngine::parse(&request.resolved_engine)
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    let role_prompt_digest = request
        .role_prompt_digest_hex
        .as_deref()
        .map(RolePromptDigest::from_hex)
        .transpose()
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    let lease = LeaseRef::new(
        canonical_uuid(&request.lease_id)?,
        request.lease_revision,
        request.lease_expires_at_ms,
    )
    .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    let mcp_grant = parse_mcp_grant(request.mcp_grant.as_ref())?;
    let permissions = ExecutionPermissions::new(
        request.permissions.allow_workspace_write,
        request.permissions.allow_local_shell,
        request.permissions.allow_network,
    );
    let timeouts = ExecutionTimeouts::new(
        request.timeouts.startup_timeout_ms,
        request.timeouts.execution_timeout_ms,
        request.timeouts.termination_grace_ms,
    )
    .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
    let worktree = match ForgeWorktreeRef::new(PathBuf::from(request.worktree_path)) {
        Ok(valid) => valid,
        Err(validation_err) => return Ok(Err(validation_err)),
    };
    Ok(Ok((
        request_id,
        AssignmentExecutionSnapshotParts {
            snapshot_id,
            attempt_key,
            activity_id,
            profile_id,
            profile_revision,
            resolved_engine,
            resolved_model: request.resolved_model,
            resolved_reasoning: request.resolved_reasoning,
            role_prompt_digest,
            worktree,
            lease,
            mcp_grant,
            permissions,
            timeouts,
            committed_at_ms,
        },
    )))
}

fn parse_mcp_grant(raw: Option<&McpGrantWire>) -> Result<Option<McpGrant>, ApiError> {
    raw.map(|grant| {
        let token_uuid = canonical_uuid(&grant.token_id)?;
        McpGrant::new(
            DirectTokenId::from_uuid(token_uuid),
            grant.issued_at_ms,
            grant.expires_at_ms,
        )
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))
    })
    .transpose()
}

fn map_reviewer_outcome(
    outcome: Result<ReviewerAssignmentRecord, ReviewerAssignmentError>,
) -> Result<(StatusCode, Json<ReviewerOperationResponse>), ApiError> {
    match outcome {
        Ok(record) => Ok((
            StatusCode::OK,
            Json(ReviewerOperationResponse::admitted(&record)),
        )),
        Err(error) => map_reviewer_error(error),
    }
}

fn map_reviewer_error(
    error: ReviewerAssignmentError,
) -> Result<(StatusCode, Json<ReviewerOperationResponse>), ApiError> {
    let detail = error.to_string();
    let code = match error {
        ReviewerAssignmentError::SelfReview { reason } => classify_self_review_reason(reason),
        ReviewerAssignmentError::NoQualifiedReviewer => "NO_QUALIFIED_REVIEWER",
        ReviewerAssignmentError::QualificationMissing => "QUALIFICATION_MISSING",
        ReviewerAssignmentError::VerdictDiffDigestMismatch => "VERDICT_DIFF_DIGEST_MISMATCH",
        ReviewerAssignmentError::ReviewNotAccepted => "REVIEW_NOT_ACCEPTED",
        ReviewerAssignmentError::StaleRevision => "REVISION_MISMATCH",
        ReviewerAssignmentError::Operations(ops_error) => {
            return map_reviewer_operations_error(ops_error);
        }
    };
    Ok((
        StatusCode::CONFLICT,
        Json(ReviewerOperationResponse::rejected(code, detail)),
    ))
}

fn classify_self_review_reason(reason: &str) -> &'static str {
    match reason {
        "reviewer shares AgentProfileId with executor" => "SELF_REVIEW_PROFILE",
        "reviewer shares participant_id with executor" => "SELF_REVIEW_PARTICIPANT",
        "reviewer shares AuthenticatedPrincipal with executor" => "SELF_REVIEW_PRINCIPAL",
        "reviewer shares AgentProfileId with assigning Team Leader"
        | "reviewer shares participant_id with assigning Team Leader"
        | "reviewer shares AuthenticatedPrincipal with assigning Team Leader" => {
            "SELF_REVIEW_ASSIGNING_LEADER"
        }
        _ => "SELF_REVIEW",
    }
}

fn map_reviewer_operations_error(
    error: OperationsError,
) -> Result<(StatusCode, Json<ReviewerOperationResponse>), ApiError> {
    match error {
        OperationsError::Bounds(_) => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
        OperationsError::Conflict(reason) => {
            if matches!(
                reason,
                "conflicting duplicate reviewer candidate participation"
                    | "conflicting duplicate reviewer candidate profile"
            ) {
                return Err(ApiError::Runtime(RuntimeError::InvalidInput));
            }
            let code = match reason {
                "reviewer assignment missing for correlation" => "ASSIGNMENT_MISSING",
                "review verdict caller does not match assigned reviewer" => {
                    "REVIEWER_IDENTITY_MISMATCH"
                }
                "reviewer assignment request_id reused with different payload"
                | "review verdict request_id reused with different payload" => "REQUEST_CONFLICT",
                _ => "CONFLICT",
            };
            Ok((
                StatusCode::CONFLICT,
                Json(ReviewerOperationResponse::rejected(code, reason.to_owned())),
            ))
        }
        other => Err(ApiError::Runtime(RuntimeError::from(other))),
    }
}

fn map_snapshot_validation_error(
    error: &SnapshotValidationError,
) -> Result<(StatusCode, Json<ExecutionSnapshotOutcomeResponse>), ApiError> {
    let code = match error {
        SnapshotValidationError::MissingExecutionPreference => "MISSING_EXECUTION_PREFERENCE",
        SnapshotValidationError::ProfileRevisionMismatch => "PROFILE_REVISION_MISMATCH",
        SnapshotValidationError::EngineMismatch => "ENGINE_MISMATCH",
        SnapshotValidationError::ModelMismatch => "MODEL_MISMATCH",
        SnapshotValidationError::ReasoningMismatch => "REASONING_MISMATCH",
        SnapshotValidationError::RolePromptMismatch => "ROLE_PROMPT_MISMATCH",
        SnapshotValidationError::IncompatibleEngineModel => "INCOMPATIBLE_ENGINE_MODEL",
        SnapshotValidationError::ForbiddenSsdBuildWorktree => "FORBIDDEN_SSD_BUILD_WORKTREE",
        SnapshotValidationError::InvalidWorktreePath(_) => "INVALID_WORKTREE_PATH",
        SnapshotValidationError::InvalidIdentity(_)
        | SnapshotValidationError::InvalidGrantWindow
        | SnapshotValidationError::InvalidTimeout(_) => {
            return Err(ApiError::Runtime(RuntimeError::InvalidInput));
        }
    };
    Ok((
        StatusCode::CONFLICT,
        Json(ExecutionSnapshotOutcomeResponse::rejected(
            code,
            error.to_string(),
        )),
    ))
}
