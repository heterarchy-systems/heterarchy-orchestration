//! Typed, bounded Orchestration client methods for Mission planning.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::mission_evaluation::{
    MissionCreateRequest, MissionCreateResponse, MissionEvaluationMutationRequest,
    MissionEvaluationPlanPublishRequest, MissionEvaluationPlanPublishResponse,
    MissionEvaluationPlanStartReadRequest, MissionEvaluationPlanStartReadResponse,
    MissionEvaluationPlanStartRequest, MissionEvaluationPlanStartResponse,
    MissionEvaluationPlanStartWire, MissionEvaluationPlanVersionReadRequest,
    MissionEvaluationPlanVersionReadResponse, MissionEvaluationPlanVersionWire,
    MissionEvaluationRecordedMutationWire, MissionEvaluationRequestReadActor,
    MissionEvaluationRequestReadRequest, MissionEvaluationRequestReadResponse,
    MissionObligationReadRequest, MissionObligationReadResponse, MissionObligationRecordRequest,
    MissionObligationRecordResponse, MissionObligationWire, MissionReadRequest,
    MissionReadResponse, MissionWire,
};

const MAX_PURPOSE_BYTES: usize = 2_048;
const MAX_ACCEPTANCE_CRITERIA_BYTES: usize = 8_192;
const MAX_PLAN_COHORT: usize = 256;
const MAX_SQLITE_INTEGER: u64 = i64::MAX.unsigned_abs();

impl OrchestrationHttpClient {
    /// Create one Mission and preserve its exact request for uncertainty recovery.
    ///
    /// No automatic retry is performed. On an uncertain outcome, use the exact
    /// request held by `UnknownOutcome` to query its durable mutation receipt.
    ///
    /// # Errors
    /// Rejects malformed scope/content, typed server rejections, bounded
    /// transport failures, or a response that does not match the request.
    pub async fn mission_create(
        &self,
        authorization: &HeaderValue,
        request: &MissionCreateRequest,
    ) -> Result<MissionCreateResponse, ClientError> {
        if !valid_create_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let target = mutation_readback(MissionEvaluationMutationRequest::Create(request.clone()));
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_create_path(request.context.scope.workspace_id),
                Some(request),
                Some(target.clone()),
            )
            .await?;
        if valid_create_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::UnknownOutcome { readback: target })
        }
    }

    /// Read one immutable Mission under exact Workspace scope.
    ///
    /// # Errors
    /// Rejects malformed requests, bounded transport failures, and foreign or
    /// malformed projections.
    pub async fn mission_read(
        &self,
        authorization: &HeaderValue,
        request: &MissionReadRequest,
    ) -> Result<MissionReadResponse, ClientError> {
        if !valid_read_scope(&request.scope, request.session_id) || request.mission_id.is_nil() {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_read_path(request.scope.workspace_id, request.mission_id),
                Some(request),
                None,
            )
            .await?;
        if valid_mission_read_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Record one stable personal obligation and preserve exact recovery identity.
    ///
    /// # Errors
    /// Rejects malformed requests, typed server rejections, bounded transport
    /// failures, or a response that does not match the request.
    pub async fn mission_obligation_record(
        &self,
        authorization: &HeaderValue,
        request: &MissionObligationRecordRequest,
    ) -> Result<MissionObligationRecordResponse, ClientError> {
        if !valid_obligation_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let target = mutation_readback(MissionEvaluationMutationRequest::RecordObligation(
            request.clone(),
        ));
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_obligation_record_path(
                    request.context.scope.workspace_id,
                    request.mission_id,
                ),
                Some(request),
                Some(target.clone()),
            )
            .await?;
        if valid_obligation_record_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::UnknownOutcome { readback: target })
        }
    }

    /// Read one immutable obligation under exact Workspace and Mission scope.
    ///
    /// # Errors
    /// Rejects malformed requests, bounded transport failures, and foreign or
    /// malformed projections.
    pub async fn mission_obligation_read(
        &self,
        authorization: &HeaderValue,
        request: &MissionObligationReadRequest,
    ) -> Result<MissionObligationReadResponse, ClientError> {
        if !valid_read_scope(&request.scope, request.session_id)
            || request.mission_id.is_nil()
            || request.obligation_id.is_nil()
        {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_obligation_read_path(
                    request.scope.workspace_id,
                    request.mission_id,
                    request.obligation_id,
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_obligation_read_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Publish one immutable evaluation-plan version.
    ///
    /// No automatic retry is performed. An uncertain response retains the
    /// complete original request for a later exact mutation-receipt read.
    ///
    /// # Errors
    /// Rejects malformed scope, revision, timeline or cohort; typed server
    /// rejections; bounded transport failures; or a mismatched response.
    pub async fn mission_evaluation_plan_publish(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvaluationPlanPublishRequest,
    ) -> Result<MissionEvaluationPlanPublishResponse, ClientError> {
        if !valid_plan_publish_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let target = mutation_readback(MissionEvaluationMutationRequest::PublishPlan(
            request.clone(),
        ));
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_plan_publish_path(request.context.scope.workspace_id, request.mission_id),
                Some(request),
                Some(target.clone()),
            )
            .await?;
        if valid_plan_publish_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::UnknownOutcome { readback: target })
        }
    }

    /// Read one immutable plan version by exact Mission, plan and revision.
    ///
    /// # Errors
    /// Rejects malformed selectors, bounded transport failures, and foreign or
    /// malformed projections.
    pub async fn mission_evaluation_plan_version_read(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvaluationPlanVersionReadRequest,
    ) -> Result<MissionEvaluationPlanVersionReadResponse, ClientError> {
        if !valid_read_scope(&request.scope, request.session_id)
            || request.mission_id.is_nil()
            || request.plan_id.is_nil()
            || !valid_revision(request.revision)
        {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_plan_version_read_path(
                    request.scope.workspace_id,
                    request.mission_id,
                    request.plan_id,
                    request.revision,
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_plan_version_read_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Explicitly start one exact immutable plan revision.
    ///
    /// # Errors
    /// Rejects malformed selectors, typed server rejections, bounded transport
    /// failures, or a response that does not match the exact request.
    pub async fn mission_evaluation_plan_start(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvaluationPlanStartRequest,
    ) -> Result<MissionEvaluationPlanStartResponse, ClientError> {
        if !valid_plan_start_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let target =
            mutation_readback(MissionEvaluationMutationRequest::StartPlan(request.clone()));
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_plan_start_path(
                    request.context.scope.workspace_id,
                    request.mission_id,
                    request.plan_id,
                ),
                Some(request),
                Some(target.clone()),
            )
            .await?;
        if valid_plan_start_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::UnknownOutcome { readback: target })
        }
    }

    /// Read the immutable start receipt for one exact Mission and plan.
    ///
    /// # Errors
    /// Rejects malformed selectors, bounded transport failures, and foreign or
    /// malformed projections.
    pub async fn mission_evaluation_plan_start_read(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvaluationPlanStartReadRequest,
    ) -> Result<MissionEvaluationPlanStartReadResponse, ClientError> {
        if !valid_read_scope(&request.scope, request.session_id)
            || request.mission_id.is_nil()
            || request.plan_id.is_nil()
        {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_plan_start_read_path(
                    request.scope.workspace_id,
                    request.mission_id,
                    request.plan_id,
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_plan_start_read_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Read the durable receipt for an exact Mission mutation request.
    ///
    /// A missing receipt is not permission to retry: the original admitted
    /// mutation may still be completing. Correlation verifies the stored
    /// operation and caller-owned content against the retained original intent.
    /// Supply the caller's current Activity/Session selector; a valid current
    /// Session rebind is accepted only when Operations resolves the same logical actor.
    ///
    /// # Errors
    /// Rejects malformed retained requests, bounded transport failures, or a
    /// receipt that does not match the exact original body and scope.
    pub async fn mission_evaluation_mutation_readback(
        &self,
        authorization: &HeaderValue,
        mutation: &MissionEvaluationMutationRequest,
        reader: MissionEvaluationRequestReadActor,
    ) -> Result<MissionEvaluationRequestReadResponse, ClientError> {
        if !valid_mutation_intent(mutation)
            || reader.sender_activity_id.is_nil()
            || reader.session_id.is_nil()
        {
            return Err(ClientError::InvalidRequest);
        }
        let request = MissionEvaluationRequestReadRequest {
            scope: mutation.scope().clone(),
            session_id: reader.session_id,
            sender_activity_id: reader.sender_activity_id,
            request_id: mutation.request_id(),
        };
        let readback = mutation_readback(mutation.clone());
        let response = self
            .call(
                authorization,
                Method::POST,
                mission_request_read_path(request.scope.workspace_id, request.request_id),
                Some(&request),
                None,
            )
            .await
            .map_err(|_| ClientError::UnknownOutcome {
                readback: readback.clone(),
            })?;
        if valid_request_read_response(&response, mutation, &request) {
            Ok(response)
        } else {
            Err(ClientError::UnknownOutcome { readback })
        }
    }
}

fn mutation_readback(request: MissionEvaluationMutationRequest) -> ReadbackTarget {
    ReadbackTarget::MissionEvaluationMutation(Box::new(request))
}

fn valid_mutation_intent(request: &MissionEvaluationMutationRequest) -> bool {
    match request {
        MissionEvaluationMutationRequest::Create(request) => valid_create_request(request),
        MissionEvaluationMutationRequest::RecordObligation(request) => {
            valid_obligation_request(request)
        }
        MissionEvaluationMutationRequest::PublishPlan(request) => {
            valid_plan_publish_request(request)
        }
        MissionEvaluationMutationRequest::StartPlan(request) => valid_plan_start_request(request),
    }
}

fn valid_mutation_context(
    context: &crate::wire::mission_evaluation::MissionEvaluationMutationContextWire,
) -> bool {
    valid_read_scope(&context.scope, context.session_id)
        && !context.sender_activity_id.is_nil()
        && context.expected_workspace_leadership_revision >= 1
}

fn valid_create_request(request: &MissionCreateRequest) -> bool {
    valid_mutation_context(&request.context)
        && !request.request_id.is_nil()
        && !request.mission_id.is_nil()
        && !request.team_id.is_nil()
        && valid_text(&request.purpose, MAX_PURPOSE_BYTES)
}

fn valid_obligation_request(request: &MissionObligationRecordRequest) -> bool {
    valid_mutation_context(&request.context)
        && !request.request_id.is_nil()
        && !request.mission_id.is_nil()
        && !request.obligation_id.is_nil()
        && !request.participation_id.is_nil()
        && !request.work_unit_id.is_nil()
        && valid_weight(request.weight)
        && valid_text(&request.acceptance_criteria, MAX_ACCEPTANCE_CRITERIA_BYTES)
        && request.due_at_ms >= 0
}

fn valid_plan_publish_request(request: &MissionEvaluationPlanPublishRequest) -> bool {
    if !valid_mutation_context(&request.context)
        || request.request_id.is_nil()
        || request.mission_id.is_nil()
        || request.plan_id.is_nil()
        || request.version_id.is_nil()
        || request.expected_head_revision >= MAX_SQLITE_INTEGER
        || request.t1_submission_cutoff_ms < 0
        || request.t2_checkpoint_ms < request.t1_submission_cutoff_ms
        || request.obligation_ids.len() > MAX_PLAN_COHORT
    {
        return false;
    }
    let mut sorted = request.obligation_ids.clone();
    if sorted.iter().any(Uuid::is_nil) {
        return false;
    }
    sorted.sort_unstable();
    !sorted.windows(2).any(|pair| pair[0] == pair[1])
}

fn valid_plan_start_request(request: &MissionEvaluationPlanStartRequest) -> bool {
    valid_mutation_context(&request.context)
        && !request.request_id.is_nil()
        && !request.mission_id.is_nil()
        && !request.plan_id.is_nil()
        && valid_revision(request.expected_revision)
}

fn valid_read_scope(scope: &crate::wire::WorkspaceQueryScope, session_id: Uuid) -> bool {
    super::teams::valid_scope(scope) && !session_id.is_nil()
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.as_bytes().contains(&0)
}

fn valid_weight(weight: u64) -> bool {
    (1..=MAX_SQLITE_INTEGER).contains(&weight)
}

fn valid_revision(revision: u64) -> bool {
    (1..=MAX_SQLITE_INTEGER).contains(&revision)
}

fn valid_mission(record: &MissionWire, workspace_id: Uuid, mission_id: Uuid) -> bool {
    record.mission_id == mission_id
        && !record.mission_id.is_nil()
        && record.workspace_id == workspace_id
        && !record.team_id.is_nil()
        && valid_text(&record.purpose, MAX_PURPOSE_BYTES)
        && record.created_at_ms >= 0
        && record.revision == 1
}

fn valid_obligation(
    record: &MissionObligationWire,
    workspace_id: Uuid,
    mission_id: Uuid,
    obligation_id: Uuid,
) -> bool {
    record.obligation_id == obligation_id
        && !record.obligation_id.is_nil()
        && record.workspace_id == workspace_id
        && record.mission_id == mission_id
        && !record.mission_id.is_nil()
        && !record.participation_id.is_nil()
        && !record.work_unit_id.is_nil()
        && valid_weight(record.weight)
        && valid_text(&record.acceptance_criteria, MAX_ACCEPTANCE_CRITERIA_BYTES)
        && record.due_at_ms >= 0
        && record.recorded_at_ms >= 0
        && record.revision == 1
}

fn valid_plan_version(
    version: &MissionEvaluationPlanVersionWire,
    workspace_id: Uuid,
    mission_id: Uuid,
    plan_id: Uuid,
) -> bool {
    if version.plan_id != plan_id
        || version.plan_id.is_nil()
        || version.version_id.is_nil()
        || version.workspace_id != workspace_id
        || version.mission_id != mission_id
        || version.mission_id.is_nil()
        || !valid_revision(version.revision)
        || version.t0_commit_at_ms < 0
        || version.t1_submission_cutoff_ms <= version.t0_commit_at_ms
        || version.t2_checkpoint_ms < version.t1_submission_cutoff_ms
        || version.cohort.len() > MAX_PLAN_COHORT
    {
        return false;
    }
    version
        .cohort
        .iter()
        .all(|item| !item.obligation_id.is_nil() && valid_weight(item.weight))
        && version
            .cohort
            .windows(2)
            .all(|pair| pair[0].obligation_id.as_bytes() < pair[1].obligation_id.as_bytes())
}

fn valid_start(
    start: &MissionEvaluationPlanStartWire,
    request_id: Option<Uuid>,
    workspace_id: Uuid,
    mission_id: Uuid,
    plan_id: Uuid,
    revision: Option<u64>,
) -> bool {
    request_id.is_none_or(|expected| start.request_id == expected)
        && !start.request_id.is_nil()
        && start.workspace_id == workspace_id
        && start.mission_id == mission_id
        && start.plan_id == plan_id
        && valid_revision(start.revision)
        && revision.is_none_or(|expected| start.revision == expected)
        && start.started_at_ms >= 0
}

fn valid_create_response(response: &MissionCreateResponse, request: &MissionCreateRequest) -> bool {
    response.scope == request.context.scope
        && response.request_id == request.request_id
        && valid_mission(
            &response.mission,
            request.context.scope.workspace_id,
            request.mission_id,
        )
        && response.mission.team_id == request.team_id
        && response.mission.purpose == request.purpose
}

fn valid_mission_read_response(
    response: &MissionReadResponse,
    request: &MissionReadRequest,
) -> bool {
    response.scope == request.scope
        && response.session_id == request.session_id
        && response.mission_id == request.mission_id
        && response.mission.as_ref().is_none_or(|mission| {
            valid_mission(mission, request.scope.workspace_id, request.mission_id)
        })
}

fn valid_obligation_record_response(
    response: &MissionObligationRecordResponse,
    request: &MissionObligationRecordRequest,
) -> bool {
    response.scope == request.context.scope
        && response.request_id == request.request_id
        && valid_obligation(
            &response.obligation,
            request.context.scope.workspace_id,
            request.mission_id,
            request.obligation_id,
        )
        && response.obligation.participation_id == request.participation_id
        && response.obligation.work_unit_id == request.work_unit_id
        && response.obligation.weight == request.weight
        && response.obligation.acceptance_criteria == request.acceptance_criteria
        && response.obligation.due_at_ms == request.due_at_ms
}

fn valid_obligation_read_response(
    response: &MissionObligationReadResponse,
    request: &MissionObligationReadRequest,
) -> bool {
    response.scope == request.scope
        && response.session_id == request.session_id
        && response.mission_id == request.mission_id
        && response.obligation_id == request.obligation_id
        && response.obligation.as_ref().is_none_or(|obligation| {
            valid_obligation(
                obligation,
                request.scope.workspace_id,
                request.mission_id,
                request.obligation_id,
            )
        })
}

fn valid_plan_publish_response(
    response: &MissionEvaluationPlanPublishResponse,
    request: &MissionEvaluationPlanPublishRequest,
) -> bool {
    let Some(expected_revision) = request.expected_head_revision.checked_add(1) else {
        return false;
    };
    let expected_cohort = canonical_cohort(&request.obligation_ids);
    response.scope == request.context.scope
        && response.request_id == request.request_id
        && response.version.revision == expected_revision
        && valid_plan_version(
            &response.version,
            request.context.scope.workspace_id,
            request.mission_id,
            request.plan_id,
        )
        && response.version.version_id == request.version_id
        && response.version.t1_submission_cutoff_ms == request.t1_submission_cutoff_ms
        && response.version.t2_checkpoint_ms == request.t2_checkpoint_ms
        && response
            .version
            .cohort
            .iter()
            .map(|item| item.obligation_id)
            .eq(expected_cohort)
}

fn valid_plan_version_read_response(
    response: &MissionEvaluationPlanVersionReadResponse,
    request: &MissionEvaluationPlanVersionReadRequest,
) -> bool {
    response.scope == request.scope
        && response.session_id == request.session_id
        && response.mission_id == request.mission_id
        && response.plan_id == request.plan_id
        && response.revision == request.revision
        && response.version.as_ref().is_none_or(|version| {
            version.revision == request.revision
                && valid_plan_version(
                    version,
                    request.scope.workspace_id,
                    request.mission_id,
                    request.plan_id,
                )
        })
}

fn valid_plan_start_response(
    response: &MissionEvaluationPlanStartResponse,
    request: &MissionEvaluationPlanStartRequest,
) -> bool {
    response.scope == request.context.scope
        && response.request_id == request.request_id
        && valid_start(
            &response.start,
            Some(request.request_id),
            request.context.scope.workspace_id,
            request.mission_id,
            request.plan_id,
            Some(request.expected_revision),
        )
}

fn valid_plan_start_read_response(
    response: &MissionEvaluationPlanStartReadResponse,
    request: &MissionEvaluationPlanStartReadRequest,
) -> bool {
    response.scope == request.scope
        && response.session_id == request.session_id
        && response.mission_id == request.mission_id
        && response.plan_id == request.plan_id
        && response.start.as_ref().is_none_or(|start| {
            valid_start(
                start,
                None,
                request.scope.workspace_id,
                request.mission_id,
                request.plan_id,
                None,
            )
        })
}

fn valid_request_read_response(
    response: &MissionEvaluationRequestReadResponse,
    request: &MissionEvaluationMutationRequest,
    reader: &MissionEvaluationRequestReadRequest,
) -> bool {
    if response.scope != reader.scope
        || response.session_id != reader.session_id
        || response.sender_activity_id != reader.sender_activity_id
        || response.request_id != request.request_id()
    {
        return false;
    }
    match (&response.mutation, request) {
        (None, _) => true,
        (
            Some(MissionEvaluationRecordedMutationWire::MissionCreated(record)),
            MissionEvaluationMutationRequest::Create(request),
        ) => valid_create_response(
            &MissionCreateResponse {
                scope: response.scope.clone(),
                request_id: response.request_id,
                mission: record.clone(),
                replayed: true,
            },
            request,
        ),
        (
            Some(MissionEvaluationRecordedMutationWire::ObligationRecorded(record)),
            MissionEvaluationMutationRequest::RecordObligation(request),
        ) => valid_obligation_record_response(
            &MissionObligationRecordResponse {
                scope: response.scope.clone(),
                request_id: response.request_id,
                obligation: record.clone(),
                replayed: true,
            },
            request,
        ),
        (
            Some(MissionEvaluationRecordedMutationWire::PlanPublished(version)),
            MissionEvaluationMutationRequest::PublishPlan(request),
        ) => valid_plan_publish_response(
            &MissionEvaluationPlanPublishResponse {
                scope: response.scope.clone(),
                request_id: response.request_id,
                version: version.clone(),
                replayed: true,
            },
            request,
        ),
        (
            Some(MissionEvaluationRecordedMutationWire::PlanStarted(start)),
            MissionEvaluationMutationRequest::StartPlan(request),
        ) => valid_plan_start_response(
            &MissionEvaluationPlanStartResponse {
                scope: response.scope.clone(),
                request_id: response.request_id,
                start: start.clone(),
                replayed: true,
            },
            request,
        ),
        _ => false,
    }
}

fn canonical_cohort(ids: &[Uuid]) -> Vec<Uuid> {
    let mut ids = ids.to_vec();
    ids.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    ids
}

fn mission_create_path(workspace_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/missions/create")
}

fn mission_read_path(workspace_id: Uuid, mission_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/query")
}

fn mission_obligation_record_path(workspace_id: Uuid, mission_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/obligations/record")
}

fn mission_obligation_read_path(
    workspace_id: Uuid,
    mission_id: Uuid,
    obligation_id: Uuid,
) -> String {
    format!(
        "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/obligations/{obligation_id}/query"
    )
}

fn mission_plan_publish_path(workspace_id: Uuid, mission_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/publish")
}

fn mission_plan_version_read_path(
    workspace_id: Uuid,
    mission_id: Uuid,
    plan_id: Uuid,
    revision: u64,
) -> String {
    format!(
        "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/versions/{revision}/query"
    )
}

fn mission_plan_start_path(workspace_id: Uuid, mission_id: Uuid, plan_id: Uuid) -> String {
    format!(
        "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/start"
    )
}

fn mission_plan_start_read_path(workspace_id: Uuid, mission_id: Uuid, plan_id: Uuid) -> String {
    format!(
        "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/start/query"
    )
}

fn mission_request_read_path(workspace_id: Uuid, request_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/mission-evaluation-requests/{request_id}/query")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{WorkspaceQueryScope, mission_evaluation::*};

    fn context() -> crate::wire::mission_evaluation::MissionEvaluationMutationContextWire {
        crate::wire::mission_evaluation::MissionEvaluationMutationContextWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            session_id: Uuid::from_u128(5),
            sender_activity_id: Uuid::from_u128(6),
            expected_workspace_leadership_revision: 7,
        }
    }

    #[test]
    fn mission_mutation_validators_reject_client_timestamps_and_authority_shortcuts() {
        let request = MissionCreateRequest {
            context: context(),
            request_id: Uuid::from_u128(8),
            mission_id: Uuid::from_u128(9),
            team_id: Uuid::from_u128(10),
            purpose: "deliver bounded work".to_owned(),
        };
        assert!(valid_create_request(&request));

        let mut invalid = request.clone();
        invalid.context.expected_workspace_leadership_revision = 0;
        assert!(!valid_create_request(&invalid));

        let mut invalid = request;
        invalid.context.sender_activity_id = Uuid::nil();
        assert!(!valid_create_request(&invalid));
    }

    #[test]
    fn plan_wire_response_requires_exact_sorted_unique_cohort_and_scope() {
        let request = MissionEvaluationPlanPublishRequest {
            context: context(),
            request_id: Uuid::from_u128(8),
            mission_id: Uuid::from_u128(9),
            plan_id: Uuid::from_u128(10),
            version_id: Uuid::from_u128(11),
            expected_head_revision: 2,
            t1_submission_cutoff_ms: 20,
            t2_checkpoint_ms: 30,
            obligation_ids: vec![Uuid::from_u128(13), Uuid::from_u128(12)],
        };
        assert!(valid_plan_publish_request(&request));
        let response = MissionEvaluationPlanPublishResponse {
            scope: request.context.scope.clone(),
            request_id: request.request_id,
            version: MissionEvaluationPlanVersionWire {
                plan_id: request.plan_id,
                version_id: request.version_id,
                workspace_id: request.context.scope.workspace_id,
                mission_id: request.mission_id,
                revision: 3,
                t0_commit_at_ms: 10,
                t1_submission_cutoff_ms: 20,
                t2_checkpoint_ms: 30,
                cohort: vec![
                    crate::wire::mission_evaluation::MissionEvaluationPlanObligationWire {
                        obligation_id: Uuid::from_u128(12),
                        weight: 3,
                    },
                    crate::wire::mission_evaluation::MissionEvaluationPlanObligationWire {
                        obligation_id: Uuid::from_u128(13),
                        weight: 5,
                    },
                ],
            },
            replayed: false,
        };
        assert!(valid_plan_publish_response(&response, &request));

        let mut duplicate = response.clone();
        duplicate.version.cohort[1].obligation_id = duplicate.version.cohort[0].obligation_id;
        assert!(!valid_plan_publish_response(&duplicate, &request));
        let mut foreign = response;
        foreign.version.mission_id = Uuid::from_u128(99);
        assert!(!valid_plan_publish_response(&foreign, &request));
    }

    #[test]
    fn request_receipt_readback_correlates_exact_mutation_body_and_scope() {
        let request = MissionCreateRequest {
            context: context(),
            request_id: Uuid::from_u128(8),
            mission_id: Uuid::from_u128(9),
            team_id: Uuid::from_u128(10),
            purpose: "deliver bounded work".to_owned(),
        };
        let mutation = MissionEvaluationMutationRequest::Create(request.clone());
        // The read may come from the current Session after a legitimate rebind.
        // Operations, not this transport projection, verifies the logical actor.
        let reader = MissionEvaluationRequestReadRequest {
            scope: request.context.scope.clone(),
            session_id: Uuid::from_u128(50),
            sender_activity_id: Uuid::from_u128(60),
            request_id: request.request_id,
        };
        let response = MissionEvaluationRequestReadResponse {
            scope: request.context.scope.clone(),
            session_id: reader.session_id,
            sender_activity_id: reader.sender_activity_id,
            request_id: request.request_id,
            mutation: Some(MissionEvaluationRecordedMutationWire::MissionCreated(
                MissionWire {
                    mission_id: request.mission_id,
                    workspace_id: request.context.scope.workspace_id,
                    team_id: request.team_id,
                    purpose: request.purpose.clone(),
                    created_at_ms: 20,
                    revision: 1,
                },
            )),
        };
        assert!(valid_request_read_response(&response, &mutation, &reader));

        let mut changed_body = response.clone();
        if let Some(MissionEvaluationRecordedMutationWire::MissionCreated(record)) =
            changed_body.mutation.as_mut()
        {
            record.purpose.push('!');
        }
        assert!(!valid_request_read_response(
            &changed_body,
            &mutation,
            &reader
        ));

        let mut foreign_scope = response;
        foreign_scope.scope.workspace_id = Uuid::from_u128(99);
        assert!(!valid_request_read_response(
            &foreign_scope,
            &mutation,
            &reader
        ));
    }
}
