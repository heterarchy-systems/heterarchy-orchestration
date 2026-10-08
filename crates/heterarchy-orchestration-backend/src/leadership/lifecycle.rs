//! Source-fenced leadership lifecycle adapters over the existing Runtime saga.
use crate::{
    ApiError, BackendState,
    app_support::{LaneError, validate_workspace_reference},
    wire::leadership_lifecycle::{
        LeadershipAdvanceRequest, LeadershipAdvanceResponse, LeadershipConfirmRequest,
        LeadershipConfirmResponse, LeadershipConfirmationQueryRequest,
        LeadershipConfirmationQueryResponse, LeadershipConfirmationSnapshotWire,
        LeadershipIntentStateWire, LeadershipLifecycleTarget, LeadershipReceiptWire,
        LeadershipReconcileRequest, LeadershipReconcileResponse, LeadershipRecoveryResolutionWire,
        LeadershipStateWire,
    },
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::post,
};
use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    FencedConfirmCommand, LeadershipConfirmIntentState, LeadershipConfirmReadback,
    LeadershipMutationReceipt, LeadershipScope, LeadershipState,
};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError,
    leadership::{
        AdvanceCommand, FencedConfirmOutcome, LeadershipRecoveryOutcome, LeadershipViewSummary,
    },
};
pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/advance",
            post(advance),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/confirm",
            post(confirm),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/reconcile",
            post(reconcile),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/confirmation/query",
            post(query),
        )
}
fn validate(path: &str, target: &LeadershipLifecycleTarget) -> Result<LeadershipScope, LaneError> {
    validate_workspace_reference(&target.scope, Some(target.session_id))?;
    if crate::canonical_uuid(path)? != target.scope.workspace_id
        || target.activity_id.is_some_and(|id| id.is_nil())
        || target.team_id.is_some_and(|id| id.is_nil())
    {
        return Err(LaneError::InvalidIdentity);
    }
    let workspace = WorkspaceId::from_uuid(target.scope.workspace_id);
    match (target.activity_id, target.team_id) {
        (None, None) => Ok(LeadershipScope::workspace(workspace)),
        (Some(id), None) => Ok(LeadershipScope::activity(
            workspace,
            forge_domain::ActivityId::from_uuid(id),
        )),
        (None, Some(id)) => Ok(LeadershipScope::team(workspace, id)),
        _ => Err(LaneError::InvalidIdentity),
    }
}
fn authorize(
    runtime: &OrchestrationRuntime,
    target: &LeadershipLifecycleTarget,
    principal: AuthenticatedPrincipal,
) -> Result<(), RuntimeError> {
    runtime.require_operator(principal)?;
    runtime.require_open_session(target.session_id, principal)?;
    runtime.require_workspace_read(
        target.scope.workspace_id,
        Some(target.session_id),
        principal,
    )
}
fn runtime_error(error: ApiError) -> RuntimeError {
    match error {
        ApiError::Runtime(error) => error,
        ApiError::InvalidIdentity => RuntimeError::InvalidInput,
        _ => RuntimeError::SourceAuthorityUnavailable,
    }
}
impl BackendState {
    async fn lifecycle_check(
        &self,
        target: LeadershipLifecycleTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<(), RuntimeError> {
        let auth = target.clone();
        self.catalog_job(move |runtime| authorize(runtime, &auth, principal))
            .await
            .map_err(runtime_error)?;
        self.workspace_scope_proof(target.scope.clone())
            .await
            .map_err(runtime_error)?;
        self.catalog_job(move |runtime| authorize(runtime, &target, principal))
            .await
            .map_err(runtime_error)
    }
    async fn lifecycle_admit(
        &self,
        target: &LeadershipLifecycleTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<(), ApiError> {
        self.lifecycle_check(target.clone(), principal)
            .await
            .map_err(ApiError::Runtime)?;
        self.runtime
            .ensure_broker_owner()
            .await
            .map_err(RuntimeError::BrokerOwner)
            .map_err(ApiError::Runtime)?;
        self.lifecycle_check(target.clone(), principal)
            .await
            .map_err(ApiError::Runtime)
    }
}
async fn advance(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipAdvanceRequest>,
) -> Result<Json<LeadershipAdvanceResponse>, LaneError> {
    let _scope = validate(&path, &body.target)?;
    let target = body.target;
    let owner = state.clone();
    let selected = target.clone();
    let summary = state
        .run_owned(async move {
            owner.lifecycle_admit(&selected, principal).await?;
            let publisher = owner.clone();
            owner
                .catalog_job(move |runtime| {
                    authorize(runtime, &selected, principal)?;
                    let summary = runtime.advance_leadership(AdvanceCommand {
                        workspace_id: selected.scope.workspace_id,
                        activity_id: selected.activity_id,
                        team_id: selected.team_id,
                        now_ms: crate::current_timestamp_millis()
                            .map_err(|_| RuntimeError::InvalidInput)?,
                    })?;
                    if summary.advanced
                        && let Some(view) = &summary.leadership
                        && let Ok(revision) = u64::try_from(view.leadership_revision())
                    {
                        publisher.publish_state_changed(
                            crate::realtime::RealtimeEventKind::LeadershipChanged,
                            selected
                                .team_id
                                .or(selected.activity_id)
                                .unwrap_or(selected.scope.workspace_id),
                            revision,
                            Some(selected.scope.workspace_id),
                        );
                    }
                    Ok(summary)
                })
                .await
        })
        .await?;
    let leadership = summary
        .leadership
        .as_ref()
        .map(LeadershipViewSummary::from_view);
    let projected =
        super::LeadershipViewResponse::from_parts(leadership.as_ref(), &super::empty_history());
    Ok(Json(LeadershipAdvanceResponse {
        target,
        advanced: summary.advanced,
        leadership: projected.leadership,
    }))
}
async fn confirm(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipConfirmRequest>,
) -> Result<Json<LeadershipConfirmResponse>, LaneError> {
    let scope = validate(&path, &body.target)?;
    if body.request_id.is_nil()
        || body.election_id.is_nil()
        || body.expected_leadership_revision < 0
    {
        return Err(LaneError::InvalidIdentity);
    }
    let command = FencedConfirmCommand {
        scope,
        election_id: body.election_id,
        expected_revision: body.expected_leadership_revision,
        request_id: body.request_id,
    };
    let selected = body.target.clone();
    let owner = state.clone();
    let outcome = state
        .run_owned(async move {
            owner.lifecycle_admit(&selected, principal).await?;
            let validator = owner.clone();
            let target = selected.clone();
            owner
                .runtime
                .confirm_elected_leader(command, move || {
                    let state = validator.clone();
                    let target = target.clone();
                    async move { state.lifecycle_check(target, principal).await }
                })
                .await
                .map_err(ApiError::Runtime)
                .inspect(|outcome| {
                    if let FencedConfirmOutcome::Committed(receipt) = outcome
                        && let Ok(revision) = u64::try_from(receipt.leadership_revision())
                    {
                        owner.publish_state_changed(
                            crate::realtime::RealtimeEventKind::LeadershipChanged,
                            selected
                                .team_id
                                .or(selected.activity_id)
                                .unwrap_or(selected.scope.workspace_id),
                            revision,
                            Some(selected.scope.workspace_id),
                        );
                    }
                })
        })
        .await?;
    let (intent_state, record) = match outcome {
        FencedConfirmOutcome::Committed(receipt) => {
            (LeadershipIntentStateWire::Committed, receipt_wire(&receipt))
        }
        FencedConfirmOutcome::Resolved(readback) => (
            intent_wire(readback.intent_state()),
            readback_wire(&readback),
        ),
    };
    Ok(Json(LeadershipConfirmResponse {
        target: body.target,
        request_id: body.request_id,
        election_id: body.election_id,
        intent_state,
        record,
    }))
}
async fn reconcile(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipReconcileRequest>,
) -> Result<Json<LeadershipReconcileResponse>, LaneError> {
    let scope = validate(&path, &body.target)?;
    if body.request_id.is_nil() || body.expected_leadership_revision < 0 {
        return Err(LaneError::InvalidIdentity);
    }
    let selected = body.target.clone();
    let owner = state.clone();
    let request = body.request_id;
    let revision = body.expected_leadership_revision;
    let outcome = state
        .run_owned(async move {
            owner.lifecycle_admit(&selected, principal).await?;
            let validator = owner.clone();
            let target = selected.clone();
            owner
                .runtime
                .reconcile_leadership_recovery(scope, request, revision, move || {
                    let state = validator.clone();
                    let target = target.clone();
                    async move { state.lifecycle_check(target, principal).await }
                })
                .await
                .map_err(ApiError::Runtime)
                .inspect(|outcome| {
                    // A replay may finish an earlier uncertain commit. Its
                    // outcome cannot attribute a new revision to this call;
                    // force canonical refresh without inventing an event.
                    if matches!(outcome, LeadershipRecoveryOutcome::Replayed { .. }) {
                        owner.require_snapshot_resync();
                    }
                    if let LeadershipRecoveryOutcome::Reconciled(resolution) = outcome {
                        use heterarchy_orchestration_operations::LeadershipRecoveryResolution as R;
                        let (R::ConfirmCompleted(receipt)
                        | R::Resolved(receipt)
                        | R::Unresolved(receipt)) = resolution;
                        if let Ok(revision) = u64::try_from(receipt.leadership_revision()) {
                            owner.publish_state_changed(
                                crate::realtime::RealtimeEventKind::LeadershipChanged,
                                selected
                                    .team_id
                                    .or(selected.activity_id)
                                    .unwrap_or(selected.scope.workspace_id),
                                revision,
                                Some(selected.scope.workspace_id),
                            );
                        }
                    }
                })
        })
        .await?;
    let resolution = match outcome.resolution_label()? {
        "confirm_completed" => LeadershipRecoveryResolutionWire::ConfirmCompleted,
        "resolved" => LeadershipRecoveryResolutionWire::Resolved,
        "unresolved" => LeadershipRecoveryResolutionWire::Unresolved,
        _ => return Err(RuntimeError::Conflict.into()),
    };
    let record = match outcome {
        LeadershipRecoveryOutcome::Replayed { readback, .. } => readback_wire(&readback),
        LeadershipRecoveryOutcome::Reconciled(resolution) => {
            use heterarchy_orchestration_operations::LeadershipRecoveryResolution as R;
            match resolution {
                R::ConfirmCompleted(receipt) | R::Resolved(receipt) | R::Unresolved(receipt) => {
                    receipt_wire(&receipt)
                }
            }
        }
    };
    Ok(Json(LeadershipReconcileResponse {
        target: body.target,
        request_id: request,
        resolution,
        record,
    }))
}
async fn query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipConfirmationQueryRequest>,
) -> Result<Json<LeadershipConfirmationQueryResponse>, LaneError> {
    let scope = validate(&path, &body.target)?;
    if body.request_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    state
        .lifecycle_check(body.target.clone(), principal)
        .await?;
    let target = body.target.clone();
    let request = body.request_id;
    let snapshot = state
        .catalog_job(move |runtime| {
            runtime.leadership_confirmation(scope, request, target.session_id, principal)
        })
        .await?;
    let confirmation =
        snapshot
            .as_ref()
            .map(|(intent, readback)| LeadershipConfirmationSnapshotWire {
                election_id: intent.election_id(),
                elected_participant_id: intent.elected_participant_id(),
                expected_local_revision: intent.expected_local_revision(),
                requested_fence_revision: intent.requested_fence_revision(),
                remote_recorded_revision: intent.remote_recorded_revision(),
                confirmed_at_ms: intent.confirmed_at_ms(),
                intent_state: intent_wire(intent.state()),
                record: readback_wire(readback),
            });
    Ok(Json(LeadershipConfirmationQueryResponse {
        target: body.target,
        request_id: request,
        confirmation,
    }))
}
fn state_wire(state: LeadershipState) -> LeadershipStateWire {
    match state {
        LeadershipState::Leaderless => LeadershipStateWire::Leaderless,
        LeadershipState::Bootstrapping => LeadershipStateWire::Bootstrapping,
        LeadershipState::Stabilizing => LeadershipStateWire::Stabilizing,
        LeadershipState::Active => LeadershipStateWire::Active,
        LeadershipState::Electing => LeadershipStateWire::Electing,
        LeadershipState::AuthorityPending => LeadershipStateWire::AuthorityPending,
        LeadershipState::FailoverRequired => LeadershipStateWire::FailoverRequired,
        LeadershipState::Vacant => LeadershipStateWire::Vacant,
        LeadershipState::RecoveryRequired => LeadershipStateWire::RecoveryRequired,
    }
}
fn intent_wire(state: LeadershipConfirmIntentState) -> LeadershipIntentStateWire {
    match state {
        LeadershipConfirmIntentState::Pending => LeadershipIntentStateWire::Pending,
        LeadershipConfirmIntentState::RemoteRecorded => LeadershipIntentStateWire::RemoteRecorded,
        LeadershipConfirmIntentState::Committed => LeadershipIntentStateWire::Committed,
        LeadershipConfirmIntentState::Superseded => LeadershipIntentStateWire::Superseded,
        LeadershipConfirmIntentState::Rejected => LeadershipIntentStateWire::Rejected,
    }
}
fn receipt_wire(value: &LeadershipMutationReceipt) -> LeadershipReceiptWire {
    LeadershipReceiptWire {
        workspace_id: value.scope().workspace_id().into_uuid(),
        activity_id: value
            .scope()
            .activity_id()
            .map(forge_domain::ActivityId::into_uuid),
        team_id: value.scope().team_id(),
        state: state_wire(value.state()),
        leader_participant_id: value.leader_participant_id(),
        leadership_revision: value.leadership_revision(),
        stabilization_until_ms: value.stabilization_until_ms(),
        updated_at_ms: value.updated_at_ms(),
    }
}
fn readback_wire(value: &LeadershipConfirmReadback) -> LeadershipReceiptWire {
    LeadershipReceiptWire {
        workspace_id: value.scope().workspace_id().into_uuid(),
        activity_id: value
            .scope()
            .activity_id()
            .map(forge_domain::ActivityId::into_uuid),
        team_id: value.scope().team_id(),
        state: state_wire(value.state()),
        leader_participant_id: value.leader_participant_id(),
        leadership_revision: value.leadership_revision(),
        stabilization_until_ms: value.stabilization_until_ms(),
        updated_at_ms: value.updated_at_ms(),
    }
}
