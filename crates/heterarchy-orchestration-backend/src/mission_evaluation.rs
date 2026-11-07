//! Authenticated HTTP adapters for durable Mission planning facts.

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    MissionEvaluationPlanStartRecord, MissionEvaluationPlanVersion,
    MissionEvaluationRecordedMutation, MissionEvaluationRequestRecord, MissionObligationRecord,
    MissionRecord,
};
use heterarchy_orchestration_runtime::RuntimeError;
use heterarchy_orchestration_runtime::mission_evaluation::{
    MissionCreateSpec, MissionEvaluationParticipantSpec, MissionEvaluationPlanPublishSpec,
    MissionEvaluationPlanStartSpec, MissionObligationRecordSpec,
};
use uuid::Uuid;

use crate::app_support::validate_workspace_reference;
use crate::catalog_execution::CatalogLane;
use crate::wire::mission_evaluation::{
    MissionCreateRequest, MissionCreateResponse, MissionEvaluationMutationContextWire,
    MissionEvaluationPlanObligationWire, MissionEvaluationPlanPublishRequest,
    MissionEvaluationPlanPublishResponse, MissionEvaluationPlanStartReadRequest,
    MissionEvaluationPlanStartReadResponse, MissionEvaluationPlanStartRequest,
    MissionEvaluationPlanStartResponse, MissionEvaluationPlanStartWire,
    MissionEvaluationPlanVersionReadRequest, MissionEvaluationPlanVersionReadResponse,
    MissionEvaluationPlanVersionWire, MissionEvaluationRecordedMutationWire,
    MissionEvaluationRequestReadRequest, MissionEvaluationRequestReadResponse,
    MissionObligationReadRequest, MissionObligationReadResponse, MissionObligationRecordRequest,
    MissionObligationRecordResponse, MissionObligationWire, MissionReadRequest,
    MissionReadResponse, MissionWire,
};
use crate::{ApiError, BackendState, ScopedMutationAuthority};

const MAX_PLAN_COHORT: usize = 256;

/// Register the complete Mission persistence slice.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/create",
            post(create_mission),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/query",
            post(read_mission),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/obligations/record",
            post(record_obligation),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/obligations/{obligation_id}/query",
            post(read_obligation),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/publish",
            post(publish_plan),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/versions/{revision}/query",
            post(read_plan_version),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/start",
            post(start_plan),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/start/query",
            post(read_plan_start),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/mission-evaluation-requests/{request_id}/query",
            post(read_request),
        )
}

async fn create_mission(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionCreateRequest>,
) -> Result<Json<MissionCreateResponse>, ApiError> {
    validate_mutation_context(&workspace_path, &request.context)?;
    require_identity(request.request_id)?;
    require_identity(request.mission_id)?;
    require_identity(request.team_id)?;

    let scope = request.context.scope.clone();
    let request_id = request.request_id;
    let mission_id = request.mission_id;
    let spec = MissionCreateSpec {
        request_id,
        mission_id,
        workspace_id: scope.workspace_id,
        team_id: request.team_id,
        purpose: request.purpose,
        participant: participant_spec(&request.context),
    };
    let record = state
        .run_workspace_reference_mutation(
            scope.clone(),
            request.context.session_id,
            principal,
            ScopedMutationAuthority::WorkspaceMember,
            CatalogLane::Ordinary,
            move |runtime, _| {
                let now_ms =
                    crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)?;
                runtime.create_mission(spec, principal, now_ms)
            },
        )
        .await?;
    Ok(Json(MissionCreateResponse {
        scope,
        request_id,
        mission: mission_wire(record.mission()),
        replayed: record.replayed(),
    }))
}

async fn read_mission(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionReadRequest>,
) -> Result<Json<MissionReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    if crate::canonical_uuid(&mission_path)? != request.mission_id {
        return Err(ApiError::InvalidIdentity);
    }
    require_identity(request.mission_id)?;

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let mission_id = request.mission_id;
    let workspace_id = scope.workspace_id;
    let mission = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(session_id),
            principal,
            move |runtime, _| runtime.mission(workspace_id, mission_id),
        )
        .await?;
    Ok(Json(MissionReadResponse {
        scope,
        session_id,
        mission_id,
        mission: mission.as_ref().map(mission_wire),
    }))
}

async fn record_obligation(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionObligationRecordRequest>,
) -> Result<Json<MissionObligationRecordResponse>, ApiError> {
    validate_mutation_context(&workspace_path, &request.context)?;
    if crate::canonical_uuid(&mission_path)? != request.mission_id {
        return Err(ApiError::InvalidIdentity);
    }
    for value in [
        request.request_id,
        request.mission_id,
        request.obligation_id,
        request.participation_id,
        request.work_unit_id,
    ] {
        require_identity(value)?;
    }

    let scope = request.context.scope.clone();
    let request_id = request.request_id;
    let mission_id = request.mission_id;
    let obligation_id = request.obligation_id;
    let spec = MissionObligationRecordSpec {
        request_id,
        obligation_id,
        workspace_id: scope.workspace_id,
        mission_id,
        participation_id: request.participation_id,
        work_unit_id: request.work_unit_id,
        weight: request.weight,
        acceptance_criteria: request.acceptance_criteria,
        due_at_ms: request.due_at_ms,
        participant: participant_spec(&request.context),
    };
    let record = state
        .run_workspace_reference_mutation(
            scope.clone(),
            request.context.session_id,
            principal,
            ScopedMutationAuthority::WorkspaceMember,
            CatalogLane::Ordinary,
            move |runtime, _| {
                let now_ms =
                    crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)?;
                runtime.record_mission_obligation(spec, principal, now_ms)
            },
        )
        .await?;
    Ok(Json(MissionObligationRecordResponse {
        scope,
        request_id,
        obligation: obligation_wire(record.obligation()),
        replayed: record.replayed(),
    }))
}

async fn read_obligation(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path, obligation_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionObligationReadRequest>,
) -> Result<Json<MissionObligationReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    if crate::canonical_uuid(&mission_path)? != request.mission_id
        || crate::canonical_uuid(&obligation_path)? != request.obligation_id
    {
        return Err(ApiError::InvalidIdentity);
    }
    require_identity(request.mission_id)?;
    require_identity(request.obligation_id)?;

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let mission_id = request.mission_id;
    let obligation_id = request.obligation_id;
    let workspace_id = scope.workspace_id;
    let obligation = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(session_id),
            principal,
            move |runtime, _| {
                let record = runtime.mission_obligation(workspace_id, obligation_id)?;
                if record
                    .as_ref()
                    .is_some_and(|record| record.mission_id().into_uuid() != mission_id)
                {
                    return Err(RuntimeError::Conflict);
                }
                Ok(record)
            },
        )
        .await?;
    Ok(Json(MissionObligationReadResponse {
        scope,
        session_id,
        mission_id,
        obligation_id,
        obligation: obligation.as_ref().map(obligation_wire),
    }))
}

async fn publish_plan(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvaluationPlanPublishRequest>,
) -> Result<Json<MissionEvaluationPlanPublishResponse>, ApiError> {
    validate_mutation_context(&workspace_path, &request.context)?;
    if crate::canonical_uuid(&mission_path)? != request.mission_id {
        return Err(ApiError::InvalidIdentity);
    }
    for value in [
        request.request_id,
        request.mission_id,
        request.plan_id,
        request.version_id,
    ] {
        require_identity(value)?;
    }
    if request.obligation_ids.len() > MAX_PLAN_COHORT
        || request.obligation_ids.iter().any(Uuid::is_nil)
        || request.expected_head_revision >= i64::MAX.unsigned_abs()
    {
        return Err(ApiError::InvalidIdentity);
    }

    let scope = request.context.scope.clone();
    let request_id = request.request_id;
    let mission_id = request.mission_id;
    let plan_id = request.plan_id;
    let spec = MissionEvaluationPlanPublishSpec {
        request_id,
        plan_id,
        version_id: request.version_id,
        workspace_id: scope.workspace_id,
        mission_id,
        expected_head_revision: request.expected_head_revision,
        t1_submission_cutoff_ms: request.t1_submission_cutoff_ms,
        t2_checkpoint_ms: request.t2_checkpoint_ms,
        obligation_ids: request.obligation_ids,
        participant: participant_spec(&request.context),
    };
    let outcome = state
        .run_workspace_reference_mutation(
            scope.clone(),
            request.context.session_id,
            principal,
            ScopedMutationAuthority::WorkspaceMember,
            CatalogLane::Ordinary,
            move |runtime, _| {
                let now_ms =
                    crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)?;
                runtime.publish_mission_evaluation_plan(spec, principal, now_ms)
            },
        )
        .await?;
    Ok(Json(MissionEvaluationPlanPublishResponse {
        scope,
        request_id,
        version: plan_version_wire(outcome.version()),
        replayed: outcome.replayed(),
    }))
}

async fn read_plan_version(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path, plan_path, revision_path)): Path<(
        String,
        String,
        String,
        String,
    )>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvaluationPlanVersionReadRequest>,
) -> Result<Json<MissionEvaluationPlanVersionReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    if crate::canonical_uuid(&mission_path)? != request.mission_id
        || crate::canonical_uuid(&plan_path)? != request.plan_id
        || canonical_revision(&revision_path)? != request.revision
    {
        return Err(ApiError::InvalidIdentity);
    }
    require_identity(request.mission_id)?;
    require_identity(request.plan_id)?;
    require_revision(request.revision)?;

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let mission_id = request.mission_id;
    let plan_id = request.plan_id;
    let revision = request.revision;
    let workspace_id = scope.workspace_id;
    let version = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(session_id),
            principal,
            move |runtime, _| {
                runtime.mission_evaluation_plan_version(workspace_id, mission_id, plan_id, revision)
            },
        )
        .await?;
    Ok(Json(MissionEvaluationPlanVersionReadResponse {
        scope,
        session_id,
        mission_id,
        plan_id,
        revision,
        version: version.as_ref().map(plan_version_wire),
    }))
}

async fn start_plan(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path, plan_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvaluationPlanStartRequest>,
) -> Result<Json<MissionEvaluationPlanStartResponse>, ApiError> {
    validate_mutation_context(&workspace_path, &request.context)?;
    if crate::canonical_uuid(&mission_path)? != request.mission_id
        || crate::canonical_uuid(&plan_path)? != request.plan_id
    {
        return Err(ApiError::InvalidIdentity);
    }
    for value in [request.request_id, request.mission_id, request.plan_id] {
        require_identity(value)?;
    }
    require_revision(request.expected_revision)?;

    let scope = request.context.scope.clone();
    let request_id = request.request_id;
    let mission_id = request.mission_id;
    let plan_id = request.plan_id;
    let spec = MissionEvaluationPlanStartSpec {
        request_id,
        workspace_id: scope.workspace_id,
        mission_id,
        plan_id,
        expected_revision: request.expected_revision,
        participant: participant_spec(&request.context),
    };
    let outcome = state
        .run_workspace_reference_mutation(
            scope.clone(),
            request.context.session_id,
            principal,
            ScopedMutationAuthority::WorkspaceMember,
            CatalogLane::Ordinary,
            move |runtime, _| {
                runtime.start_mission_evaluation_plan(spec, principal, || {
                    crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)
                })
            },
        )
        .await?;
    Ok(Json(MissionEvaluationPlanStartResponse {
        scope,
        request_id,
        start: start_wire(outcome.start()),
        replayed: outcome.replayed(),
    }))
}

async fn read_plan_start(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path, plan_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvaluationPlanStartReadRequest>,
) -> Result<Json<MissionEvaluationPlanStartReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    if crate::canonical_uuid(&mission_path)? != request.mission_id
        || crate::canonical_uuid(&plan_path)? != request.plan_id
    {
        return Err(ApiError::InvalidIdentity);
    }
    require_identity(request.mission_id)?;
    require_identity(request.plan_id)?;

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let mission_id = request.mission_id;
    let plan_id = request.plan_id;
    let workspace_id = scope.workspace_id;
    let start = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(session_id),
            principal,
            move |runtime, _| {
                runtime.mission_evaluation_plan_start(workspace_id, mission_id, plan_id)
            },
        )
        .await?;
    Ok(Json(MissionEvaluationPlanStartReadResponse {
        scope,
        session_id,
        mission_id,
        plan_id,
        start: start.as_ref().map(start_wire),
    }))
}

async fn read_request(
    State(state): State<BackendState>,
    Path((workspace_path, request_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvaluationRequestReadRequest>,
) -> Result<Json<MissionEvaluationRequestReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    if crate::canonical_uuid(&request_path)? != request.request_id {
        return Err(ApiError::InvalidIdentity);
    }
    require_identity(request.request_id)?;
    require_identity(request.sender_activity_id)?;

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let sender_activity_id = request.sender_activity_id;
    let request_id = request.request_id;
    let workspace_id = scope.workspace_id;
    let record = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(session_id),
            principal,
            move |runtime, _| {
                runtime.mission_evaluation_request_for_actor(
                    workspace_id,
                    request_id,
                    sender_activity_id,
                    session_id,
                    principal,
                )
            },
        )
        .await?;
    let mutation = record
        .as_ref()
        .map(|record| mutation_wire(record, workspace_id, request_id))
        .transpose()?;
    Ok(Json(MissionEvaluationRequestReadResponse {
        scope,
        session_id,
        sender_activity_id,
        request_id,
        mutation,
    }))
}

fn validate_mutation_context(
    workspace_path: &str,
    context: &MissionEvaluationMutationContextWire,
) -> Result<(), ApiError> {
    validate_read_scope(workspace_path, &context.scope, context.session_id)?;
    if context.sender_activity_id.is_nil() || context.expected_workspace_leadership_revision < 1 {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

fn validate_read_scope(
    workspace_path: &str,
    scope: &crate::wire::WorkspaceQueryScope,
    session_id: Uuid,
) -> Result<(), ApiError> {
    validate_workspace_reference(scope, Some(session_id))?;
    if session_id.is_nil() || crate::canonical_uuid(workspace_path)? != scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

fn participant_spec(
    context: &MissionEvaluationMutationContextWire,
) -> MissionEvaluationParticipantSpec {
    MissionEvaluationParticipantSpec {
        activity_id: context.sender_activity_id,
        session_id: context.session_id,
        expected_workspace_leadership_revision: context.expected_workspace_leadership_revision,
    }
}

fn require_identity(value: Uuid) -> Result<(), ApiError> {
    if value.is_nil() {
        Err(ApiError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn require_revision(value: u64) -> Result<(), ApiError> {
    if value == 0 || value > i64::MAX.unsigned_abs() {
        Err(ApiError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn canonical_revision(value: &str) -> Result<u64, ApiError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| ApiError::InvalidIdentity)?;
    if parsed.to_string() != value {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(parsed)
}

fn mission_wire(record: &MissionRecord) -> MissionWire {
    MissionWire {
        mission_id: record.mission_id().into_uuid(),
        workspace_id: record.workspace_id().into_uuid(),
        team_id: record.team_id(),
        purpose: record.purpose().to_owned(),
        created_at_ms: record.created_at_ms(),
        revision: record.revision(),
    }
}

fn obligation_wire(record: &MissionObligationRecord) -> MissionObligationWire {
    MissionObligationWire {
        obligation_id: record.obligation_id().into_uuid(),
        workspace_id: record.workspace_id().into_uuid(),
        mission_id: record.mission_id().into_uuid(),
        participation_id: record.participation_id().into_uuid(),
        work_unit_id: record.work_unit_id().into_uuid(),
        weight: record.weight().get(),
        acceptance_criteria: record.acceptance_criteria().to_owned(),
        due_at_ms: record.due_at_ms(),
        recorded_at_ms: record.recorded_at_ms(),
        revision: record.revision(),
    }
}

fn plan_version_wire(version: &MissionEvaluationPlanVersion) -> MissionEvaluationPlanVersionWire {
    MissionEvaluationPlanVersionWire {
        plan_id: version.plan_id().into_uuid(),
        version_id: version.version_id().into_uuid(),
        workspace_id: version.workspace_id().into_uuid(),
        mission_id: version.mission_id().into_uuid(),
        revision: version.revision(),
        t0_commit_at_ms: version.t0_commit_at_ms(),
        t1_submission_cutoff_ms: version.t1_submission_cutoff_ms(),
        t2_checkpoint_ms: version.t2_checkpoint_ms(),
        cohort: version
            .cohort()
            .iter()
            .map(|obligation| MissionEvaluationPlanObligationWire {
                obligation_id: obligation.obligation_id().into_uuid(),
                weight: obligation.weight().get(),
            })
            .collect(),
    }
}

fn start_wire(record: &MissionEvaluationPlanStartRecord) -> MissionEvaluationPlanStartWire {
    MissionEvaluationPlanStartWire {
        request_id: record.request_id(),
        workspace_id: record.workspace_id().into_uuid(),
        mission_id: record.mission_id().into_uuid(),
        plan_id: record.plan_id().into_uuid(),
        revision: record.revision(),
        started_at_ms: record.started_at_ms(),
    }
}

fn mutation_wire(
    record: &MissionEvaluationRequestRecord,
    workspace_id: Uuid,
    request_id: Uuid,
) -> Result<MissionEvaluationRecordedMutationWire, ApiError> {
    if record.request_id() != request_id || record.workspace_id().into_uuid() != workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(match record.mutation() {
        MissionEvaluationRecordedMutation::MissionCreated(record) => {
            MissionEvaluationRecordedMutationWire::MissionCreated(mission_wire(record))
        }
        MissionEvaluationRecordedMutation::ObligationRecorded(record) => {
            MissionEvaluationRecordedMutationWire::ObligationRecorded(obligation_wire(record))
        }
        MissionEvaluationRecordedMutation::PlanPublished(record) => {
            MissionEvaluationRecordedMutationWire::PlanPublished(plan_version_wire(record))
        }
        MissionEvaluationRecordedMutation::PlanStarted(record) => {
            MissionEvaluationRecordedMutationWire::PlanStarted(start_wire(record))
        }
    })
}
