//! Mission HTTP steps run inside the existing ignored real-Broker fixture.

use super::{MemberTarget, ReadyFixtureMember, TestResult, current_test_time};
use heterarchy_orchestration_backend::client::OrchestrationHttpClient;
use heterarchy_orchestration_backend::wire::TeamGetQueryRequest;
use heterarchy_orchestration_backend::wire::mission_evaluation::{
    MissionCreateRequest, MissionCreateResponse, MissionEvaluationMutationContextWire,
    MissionEvaluationMutationRequest, MissionEvaluationPlanPublishRequest,
    MissionEvaluationPlanPublishResponse, MissionEvaluationPlanStartReadRequest,
    MissionEvaluationPlanStartRequest, MissionEvaluationPlanVersionReadRequest,
    MissionEvaluationRecordedMutationWire, MissionEvaluationRequestReadActor,
    MissionObligationReadRequest, MissionObligationRecordRequest, MissionReadRequest,
};
use hyper::header::HeaderValue;
use uuid::Uuid;

struct MissionFlow<'a> {
    client: &'a OrchestrationHttpClient,
    bearer: &'a HeaderValue,
    member: &'a MemberTarget,
    leader: &'a ReadyFixtureMember,
    assignee: &'a ReadyFixtureMember,
    leadership_revision: i64,
    team_id: Uuid,
}

/// Exact local plan/cohort selectors needed by the source evidence fixture.
pub(super) struct MissionEvaluationEvidenceSelectors {
    pub(super) mission_id: Uuid,
    pub(super) plan_id: Uuid,
    pub(super) plan_revision: u64,
    pub(super) obligation_id: Uuid,
    pub(super) work_unit_id: Uuid,
    pub(super) t1_submission_cutoff_ms: i64,
}

impl MissionFlow<'_> {
    fn mutation_context(&self) -> MissionEvaluationMutationContextWire {
        MissionEvaluationMutationContextWire {
            scope: self.member.scope.clone(),
            session_id: self.leader.session_id,
            sender_activity_id: self.member.activity_id,
            expected_workspace_leadership_revision: self.leadership_revision,
        }
    }
}

/// Main calls this after the real Broker fixture creates its Team and before
/// either the Mission assignee or leader Session is closed.
pub(super) async fn assert_mission_evaluation_http_flow(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    member: &MemberTarget,
    leader: &ReadyFixtureMember,
    assignee: &ReadyFixtureMember,
    leadership_revision: i64,
    team_id: Uuid,
) -> TestResult<MissionEvaluationEvidenceSelectors> {
    let flow = MissionFlow {
        client,
        bearer,
        member,
        leader,
        assignee,
        leadership_revision,
        team_id,
    };
    let (mission_request, mission_response, obligation_request, t1_submission_cutoff_ms) =
        create_mission_and_record_obligation(&flow)
            .await
            .map_err(|error| format!("Mission create/obligation stage: {error:?}"))?;
    let (plan_request, plan_response) = publish_and_read_plan(
        &flow,
        &mission_request,
        &obligation_request,
        t1_submission_cutoff_ms,
    )
    .await
    .map_err(|error| format!("Mission plan publish/read stage: {error:?}"))?;
    start_and_read_plan(&flow, &mission_request, &plan_request, &plan_response)
        .await
        .map_err(|error| format!("Mission plan start/read stage: {error:?}"))?;
    let selectors = MissionEvaluationEvidenceSelectors {
        mission_id: mission_request.mission_id,
        plan_id: plan_request.plan_id,
        plan_revision: plan_response.version.revision,
        obligation_id: obligation_request.obligation_id,
        work_unit_id: obligation_request.work_unit_id,
        t1_submission_cutoff_ms: plan_response.version.t1_submission_cutoff_ms,
    };
    assert_actor_bound_receipt(&flow, mission_request, mission_response)
        .await
        .map_err(|error| format!("Mission actor receipt stage: {error:?}"))?;
    Ok(selectors)
}

async fn create_mission_and_record_obligation(
    flow: &MissionFlow<'_>,
) -> TestResult<(
    MissionCreateRequest,
    MissionCreateResponse,
    MissionObligationRecordRequest,
    i64,
)> {
    let (mission_request, mission_response, participation_id) =
        create_mission_and_read(flow).await?;
    let t1_submission_cutoff_ms = current_test_time()?
        .checked_add(5 * 1_000)
        .ok_or("T1 overflow")?;
    let obligation_request = record_obligation_and_read(
        flow,
        mission_request.mission_id,
        participation_id,
        t1_submission_cutoff_ms,
    )
    .await?;
    Ok((
        mission_request,
        mission_response,
        obligation_request,
        t1_submission_cutoff_ms,
    ))
}

async fn create_mission_and_read(
    flow: &MissionFlow<'_>,
) -> TestResult<(MissionCreateRequest, MissionCreateResponse, Uuid)> {
    let team = flow
        .client
        .team_get(
            flow.bearer,
            &TeamGetQueryRequest {
                scope: flow.member.scope.clone(),
                session_id: Some(flow.leader.session_id),
                team_id: flow.team_id,
                request_id: None,
            },
        )
        .await?;
    assert!(!team.members_truncated);
    let mission_assignee = team
        .members
        .iter()
        .find(|candidate| {
            candidate.state == "joined" && candidate.profile_id == Some(flow.assignee.profile_id)
        })
        .ok_or("Mission test Team has no joined Mission assignee")?;
    if mission_assignee.profile_id.is_none() {
        return Err("Mission assignee must be a Profile participant".into());
    }
    let participation_id = mission_assignee.participation_id;

    let mission_request = MissionCreateRequest {
        context: flow.mutation_context(),
        request_id: Uuid::now_v7(),
        mission_id: Uuid::now_v7(),
        team_id: flow.team_id,
        purpose: "deliver a bounded reviewable change".to_owned(),
    };
    let mission = flow
        .client
        .mission_create(flow.bearer, &mission_request)
        .await?;
    assert!(!mission.replayed);
    assert_eq!(mission.mission.mission_id, mission_request.mission_id);
    assert_eq!(mission.mission.team_id, flow.team_id);

    let replay = flow
        .client
        .mission_create(flow.bearer, &mission_request)
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.mission, mission.mission);
    let mission_read = flow
        .client
        .mission_read(
            flow.bearer,
            &MissionReadRequest {
                scope: flow.member.scope.clone(),
                session_id: flow.leader.session_id,
                mission_id: mission_request.mission_id,
            },
        )
        .await?;
    assert_eq!(mission_read.mission.as_ref(), Some(&mission.mission));
    Ok((mission_request, mission, participation_id))
}

async fn record_obligation_and_read(
    flow: &MissionFlow<'_>,
    mission_id: Uuid,
    participation_id: Uuid,
    due_at_ms: i64,
) -> TestResult<MissionObligationRecordRequest> {
    let obligation_request = MissionObligationRecordRequest {
        context: flow.mutation_context(),
        request_id: Uuid::now_v7(),
        mission_id,
        obligation_id: Uuid::now_v7(),
        participation_id,
        work_unit_id: Uuid::now_v7(),
        weight: 5,
        acceptance_criteria: "The review is complete and its evidence is linked".to_owned(),
        due_at_ms,
    };
    let obligation = flow
        .client
        .mission_obligation_record(flow.bearer, &obligation_request)
        .await?;
    assert!(!obligation.replayed);
    assert_eq!(
        obligation.obligation.obligation_id,
        obligation_request.obligation_id
    );
    assert!(obligation.obligation.recorded_at_ms >= 0);

    let replay = flow
        .client
        .mission_obligation_record(flow.bearer, &obligation_request)
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.obligation, obligation.obligation);
    let obligation_read = flow
        .client
        .mission_obligation_read(
            flow.bearer,
            &MissionObligationReadRequest {
                scope: flow.member.scope.clone(),
                session_id: flow.leader.session_id,
                mission_id,
                obligation_id: obligation_request.obligation_id,
            },
        )
        .await?;
    assert_eq!(
        obligation_read.obligation.as_ref(),
        Some(&obligation.obligation)
    );
    Ok(obligation_request)
}

async fn publish_and_read_plan(
    flow: &MissionFlow<'_>,
    mission: &MissionCreateRequest,
    obligation: &MissionObligationRecordRequest,
    t1_submission_cutoff_ms: i64,
) -> TestResult<(
    MissionEvaluationPlanPublishRequest,
    MissionEvaluationPlanPublishResponse,
)> {
    let plan_request = MissionEvaluationPlanPublishRequest {
        context: flow.mutation_context(),
        request_id: Uuid::now_v7(),
        mission_id: mission.mission_id,
        plan_id: Uuid::now_v7(),
        version_id: Uuid::now_v7(),
        expected_head_revision: 0,
        t1_submission_cutoff_ms,
        t2_checkpoint_ms: t1_submission_cutoff_ms
            .checked_add(60 * 1_000)
            .ok_or("T2 overflow")?,
        obligation_ids: vec![obligation.obligation_id],
    };
    let published = flow
        .client
        .mission_evaluation_plan_publish(flow.bearer, &plan_request)
        .await?;
    assert!(!published.replayed);
    assert!(published.version.t0_commit_at_ms < published.version.t1_submission_cutoff_ms);
    assert_eq!(published.version.cohort.len(), 1);

    let replay = flow
        .client
        .mission_evaluation_plan_publish(flow.bearer, &plan_request)
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.version, published.version);

    let readback = flow
        .client
        .mission_evaluation_plan_version_read(
            flow.bearer,
            &MissionEvaluationPlanVersionReadRequest {
                scope: flow.member.scope.clone(),
                session_id: flow.leader.session_id,
                mission_id: mission.mission_id,
                plan_id: plan_request.plan_id,
                revision: published.version.revision,
            },
        )
        .await?;
    assert_eq!(readback.version.as_ref(), Some(&published.version));
    Ok((plan_request, published))
}

async fn start_and_read_plan(
    flow: &MissionFlow<'_>,
    mission: &MissionCreateRequest,
    plan: &MissionEvaluationPlanPublishRequest,
    published: &MissionEvaluationPlanPublishResponse,
) -> TestResult {
    let request = MissionEvaluationPlanStartRequest {
        context: flow.mutation_context(),
        request_id: Uuid::now_v7(),
        mission_id: mission.mission_id,
        plan_id: plan.plan_id,
        expected_revision: published.version.revision,
    };
    let started = flow
        .client
        .mission_evaluation_plan_start(flow.bearer, &request)
        .await?;
    assert!(!started.replayed);
    assert_eq!(started.start.request_id, request.request_id);
    let readback = flow
        .client
        .mission_evaluation_plan_start_read(
            flow.bearer,
            &MissionEvaluationPlanStartReadRequest {
                scope: flow.member.scope.clone(),
                session_id: flow.leader.session_id,
                mission_id: mission.mission_id,
                plan_id: plan.plan_id,
            },
        )
        .await?;
    assert_eq!(readback.start.as_ref(), Some(&started.start));
    Ok(())
}

async fn assert_actor_bound_receipt(
    flow: &MissionFlow<'_>,
    mission_request: MissionCreateRequest,
    mission_response: MissionCreateResponse,
) -> TestResult {
    let intent = MissionEvaluationMutationRequest::Create(mission_request);
    let foreign_reader = flow
        .client
        .mission_evaluation_mutation_readback(
            flow.bearer,
            &intent,
            MissionEvaluationRequestReadActor {
                sender_activity_id: flow.member.activity_id,
                session_id: flow.member.session_id,
            },
        )
        .await;
    assert!(
        matches!(
            foreign_reader,
            Err(super::ClientError::UnknownOutcome { .. })
        ),
        "a different Ready Team member must not read this actor's receipt"
    );

    // The rejected read is followed by a read as the original actor. No
    // Mission mutation is retried while resolving its request receipt.
    let receipt = flow
        .client
        .mission_evaluation_mutation_readback(
            flow.bearer,
            &intent,
            MissionEvaluationRequestReadActor {
                sender_activity_id: flow.member.activity_id,
                session_id: flow.leader.session_id,
            },
        )
        .await?;
    let Some(MissionEvaluationRecordedMutationWire::MissionCreated(readback)) = receipt.mutation
    else {
        return Err("exact actor-bound Mission create receipt missing".into());
    };
    assert_eq!(readback, mission_response.mission);
    Ok(())
}
