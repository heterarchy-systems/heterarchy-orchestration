//! Real isolated HTTP checks for one administrative T2 defer observation.
//!
//! The plan is created and started by the sibling Mission fixture. This file
//! waits for its published T2 using the real server clock; it never rewrites a
//! deadline or interprets the defer as review evidence, a score, or a Leader
//! decision.

use std::time::Duration;

use heterarchy_orchestration_backend::client::OrchestrationHttpClient;
use heterarchy_orchestration_backend::wire::mission_evaluation::{
    MissionEvaluationMutationContextWire, MissionEvaluationPlanVersionReadRequest,
    MissionEvaluationPlanVersionWire,
};
use heterarchy_orchestration_backend::wire::mission_t2_checkpoint::{
    MissionT2CheckpointReaderWire, MissionT2DeferContentWire, MissionT2DeferReadbackRequest,
    MissionT2DeferRequest,
};
use hyper::header::HeaderValue;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

use super::mission_evaluation::MissionEvaluationEvidenceSelectors;
use super::{MemberTarget, ReadyFixtureMember, TestResult, current_test_time};

const WAIT_SLICE: Duration = Duration::from_secs(2);
const MAX_CHECKPOINT_WAIT: Duration = Duration::from_secs(90);
const NEXT_CHECK_DELAY_MS: i64 = 60 * 1_000;

struct MissionT2HttpFlow<'a> {
    client: &'a OrchestrationHttpClient,
    bearer: &'a HeaderValue,
    member: &'a MemberTarget,
    leader: &'a ReadyFixtureMember,
    peer: &'a ReadyFixtureMember,
    leadership_revision: i64,
    selectors: &'a MissionEvaluationEvidenceSelectors,
}

/// Verify one source-backed, already-started plan can record and read an
/// administrative T2 defer through the real isolated HTTP service.
///
/// The caller should invoke this before closing the leader and peer Sessions.
/// `peer` is used only for the unauthorized mutation check.
pub(super) async fn assert_mission_t2_checkpoint_http_flow(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    member: &MemberTarget,
    leader: &ReadyFixtureMember,
    peer: &ReadyFixtureMember,
    leadership_revision: i64,
    selectors: &MissionEvaluationEvidenceSelectors,
) -> TestResult {
    let flow = MissionT2HttpFlow {
        client,
        bearer,
        member,
        leader,
        peer,
        leadership_revision,
        selectors,
    };
    let t2_checkpoint_ms = flow
        .selectors
        .t1_submission_cutoff_ms
        .checked_add(NEXT_CHECK_DELAY_MS)
        .ok_or("Mission T2 checkpoint overflow")?;
    let before = read_plan(&flow).await?;
    assert_eq!(
        before.t1_submission_cutoff_ms,
        flow.selectors.t1_submission_cutoff_ms
    );
    assert_eq!(before.t2_checkpoint_ms, t2_checkpoint_ms);

    wait_for_checkpoint(t2_checkpoint_ms).await?;

    let next_check_at_ms = current_test_time()?
        .checked_add(NEXT_CHECK_DELAY_MS)
        .ok_or("Mission T2 next-check overflow")?;
    let request = defer_request(&flow, flow.leader.session_id, next_check_at_ms);
    let deferred = flow.client.mission_t2_defer(flow.bearer, &request).await?;
    assert!(!deferred.replayed);
    assert_eq!(deferred.checkpoint.request_id, request.request_id);
    assert_eq!(deferred.checkpoint.checkpoint_id, request.checkpoint_id);
    assert!(deferred.checkpoint.recorded_at_ms >= t2_checkpoint_ms);
    assert_eq!(deferred.checkpoint.reason, request.reason);
    assert_eq!(
        deferred.checkpoint.next_check_condition,
        request.next_check_condition
    );
    assert_eq!(
        deferred.checkpoint.next_check_at_ms,
        request.next_check_at_ms
    );

    let replay = flow.client.mission_t2_defer(flow.bearer, &request).await?;
    assert!(replay.replayed);
    assert_eq!(replay.checkpoint, deferred.checkpoint);

    let readback = MissionT2DeferReadbackRequest {
        intent: defer_content(&request),
        reader: reader(&flow, flow.leader.session_id),
    };
    let observed = flow
        .client
        .mission_t2_defer_readback(flow.bearer, &readback)
        .await?;
    assert_eq!(observed.request, readback);
    assert_eq!(observed.checkpoint.as_ref(), Some(&deferred.checkpoint));

    assert_unauthorized_defer_is_absent(&flow, next_check_at_ms).await?;
    assert_malformed_scope_is_rejected(&flow, next_check_at_ms).await?;

    let after = read_plan(&flow).await?;
    assert_eq!(after, before, "T2 defer must not rewrite the frozen plan");
    Ok(())
}

async fn read_plan(flow: &MissionT2HttpFlow<'_>) -> TestResult<MissionEvaluationPlanVersionWire> {
    let response = flow
        .client
        .mission_evaluation_plan_version_read(
            flow.bearer,
            &MissionEvaluationPlanVersionReadRequest {
                scope: flow.member.scope.clone(),
                session_id: flow.leader.session_id,
                mission_id: flow.selectors.mission_id,
                plan_id: flow.selectors.plan_id,
                revision: flow.selectors.plan_revision,
            },
        )
        .await?;
    response
        .version
        .ok_or_else(|| "Mission T2 plan missing".into())
}

async fn wait_for_checkpoint(t2_checkpoint_ms: i64) -> TestResult {
    let deadline = Instant::now() + MAX_CHECKPOINT_WAIT;
    loop {
        let remaining_ms = t2_checkpoint_ms.saturating_sub(current_test_time()?);
        if remaining_ms <= 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("Mission T2 checkpoint did not arrive within the bounded wait".into());
        }
        let bounded_ms = u64::try_from(remaining_ms)?.min(
            u64::try_from(WAIT_SLICE.as_millis()).map_err(|_| "Mission T2 wait slice overflow")?,
        );
        sleep(
            Duration::from_millis(bounded_ms.max(1))
                .min(deadline.saturating_duration_since(Instant::now())),
        )
        .await;
    }
}

fn defer_request(
    flow: &MissionT2HttpFlow<'_>,
    session_id: Uuid,
    next_check_at_ms: i64,
) -> MissionT2DeferRequest {
    MissionT2DeferRequest {
        context: MissionEvaluationMutationContextWire {
            scope: flow.member.scope.clone(),
            session_id,
            sender_activity_id: flow.member.activity_id,
            expected_workspace_leadership_revision: flow.leadership_revision,
        },
        request_id: Uuid::now_v7(),
        checkpoint_id: Uuid::now_v7(),
        mission_id: flow.selectors.mission_id,
        plan_id: flow.selectors.plan_id,
        expected_revision: flow.selectors.plan_revision,
        expected_previous_checkpoint_id: None,
        reason: "independent review remains pending".to_owned(),
        next_check_condition: "read the independent review receipt".to_owned(),
        next_check_at_ms,
    }
}

fn defer_content(request: &MissionT2DeferRequest) -> MissionT2DeferContentWire {
    MissionT2DeferContentWire {
        request_id: request.request_id,
        checkpoint_id: request.checkpoint_id,
        workspace_id: request.context.scope.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        expected_revision: request.expected_revision,
        expected_previous_checkpoint_id: request.expected_previous_checkpoint_id,
        reason: request.reason.clone(),
        next_check_condition: request.next_check_condition.clone(),
        next_check_at_ms: request.next_check_at_ms,
    }
}

fn reader(flow: &MissionT2HttpFlow<'_>, session_id: Uuid) -> MissionT2CheckpointReaderWire {
    MissionT2CheckpointReaderWire {
        scope: flow.member.scope.clone(),
        session_id,
        sender_activity_id: flow.member.activity_id,
        expected_workspace_leadership_revision: flow.leadership_revision,
    }
}

async fn assert_unauthorized_defer_is_absent(
    flow: &MissionT2HttpFlow<'_>,
    next_check_at_ms: i64,
) -> TestResult {
    let request = defer_request(flow, flow.peer.session_id, next_check_at_ms);
    assert!(
        flow.client
            .mission_t2_defer(flow.bearer, &request)
            .await
            .is_err()
    );
    let readback = MissionT2DeferReadbackRequest {
        intent: defer_content(&request),
        reader: reader(flow, flow.leader.session_id),
    };
    let observed = flow
        .client
        .mission_t2_defer_readback(flow.bearer, &readback)
        .await?;
    assert!(observed.checkpoint.is_none());
    Ok(())
}

async fn assert_malformed_scope_is_rejected(
    flow: &MissionT2HttpFlow<'_>,
    next_check_at_ms: i64,
) -> TestResult {
    let mut request = defer_request(flow, flow.leader.session_id, next_check_at_ms);
    request.context.scope.location_revision = 0;
    assert!(
        flow.client
            .mission_t2_defer(flow.bearer, &request)
            .await
            .is_err()
    );
    Ok(())
}
