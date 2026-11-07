//! Real Broker/Forge/Orchestration coverage for Agent-session-bound interest.

use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_backend::client::{ClientError, OrchestrationHttpClient};
use heterarchy_orchestration_backend::wire::AgentSessionCloseRequest;
use heterarchy_orchestration_backend::wire::WorkspaceQueryScope;
use heterarchy_orchestration_backend::wire::reviewer_participation::{
    ReviewerParticipationHistoryRequest, ReviewerParticipationMutationRequest,
    ReviewerParticipationRequestReadRequest, ReviewerParticipationStatementWire,
    ReviewerParticipationTargetWire,
};
use hyper::StatusCode;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{
    AgentSessionOpenRequest, AgentSessionResolveRequest, ForgeHost, ReadyFixtureMember, TestResult,
    reviewer_qualification_rule::NonOperatorCredential,
};

/// Context for the real, isolated HTTP statement cases.
pub(super) struct ReviewerParticipationFlow<'a> {
    pub(super) client: &'a OrchestrationHttpClient,
    pub(super) operator_bearer: &'a HeaderValue,
    pub(super) operator_principal: AuthenticatedPrincipal,
    pub(super) non_operator: &'a NonOperatorCredential,
    pub(super) source: &'a ForgeHost,
    pub(super) scope: &'a WorkspaceQueryScope,
    pub(super) activity_id: Uuid,
    pub(super) target_a: &'a ReadyFixtureMember,
    pub(super) target_b: &'a ReadyFixtureMember,
}

/// Verify exact Session-owned self statement, mismatch rejection and
/// different-principal Session denial over the real isolated service stack.
pub(super) async fn assert_reviewer_participation_http_flow(
    flow: &ReviewerParticipationFlow<'_>,
) -> TestResult {
    assert!(
        matches!(
            flow.operator_principal,
            AuthenticatedPrincipal::DirectToken(_)
        ),
        "this fixture exercises a configured DirectToken that also owns a Ready Profile Session"
    );
    assert_ne!(flow.target_a.session_id, flow.target_b.session_id);
    assert_ne!(flow.target_a.agent_id, flow.target_b.agent_id);
    let target_a = target_wire(flow.scope, flow.activity_id, flow.target_a);

    let interest = ReviewerParticipationMutationRequest {
        target: target_a.clone(),
        request_id: Uuid::now_v7(),
        expected_revision: 0,
        statement: ReviewerParticipationStatementWire::Interested,
    };
    let accepted = flow
        .client
        .reviewer_participation_mutate(flow.operator_bearer, &interest)
        .await?;
    assert_eq!(accepted.target, target_a);
    assert_eq!(accepted.receipt.workspace_id, flow.scope.workspace_id);
    assert_eq!(accepted.receipt.profile_id, flow.target_a.profile_id);
    assert!(!accepted.receipt.participation_id.is_nil());
    assert_eq!(accepted.receipt.session_id, flow.target_a.session_id);
    assert_eq!(accepted.receipt.agent_id, flow.target_a.agent_id);
    assert_eq!(
        accepted.receipt.participant_id,
        flow.target_a.participant_id
    );
    assert_eq!(accepted.receipt.request_id, interest.request_id);
    assert_eq!(accepted.receipt.revision, 1);
    assert!(!accepted.receipt.replayed);
    let replay = flow
        .client
        .reviewer_participation_mutate(flow.operator_bearer, &interest)
        .await?;
    assert!(replay.receipt.replayed);
    assert_same_statement(&replay.receipt, &accepted.receipt);
    let history = flow
        .client
        .reviewer_participation_history(
            flow.operator_bearer,
            &ReviewerParticipationHistoryRequest {
                target: target_a.clone(),
                after_revision: None,
                limit: 8,
            },
        )
        .await?;
    assert_eq!(history.records.len(), 1);
    assert_eq!(history.records[0], accepted.receipt);

    assert_wrong_agent_binding_has_no_record(flow, &interest).await?;
    assert_other_principal_session_has_no_record(flow).await?;
    Ok(())
}

fn assert_same_statement(
    replayed: &heterarchy_orchestration_backend::wire::reviewer_participation::ReviewerParticipationReceiptWire,
    original: &heterarchy_orchestration_backend::wire::reviewer_participation::ReviewerParticipationReceiptWire,
) {
    assert_eq!(replayed.workspace_id, original.workspace_id);
    assert_eq!(replayed.participation_id, original.participation_id);
    assert_eq!(replayed.profile_id, original.profile_id);
    assert_eq!(replayed.participant_id, original.participant_id);
    assert_eq!(replayed.request_id, original.request_id);
    assert_eq!(replayed.revision, original.revision);
    assert_eq!(replayed.expected_revision, original.expected_revision);
    assert_eq!(replayed.statement, original.statement);
    assert_eq!(replayed.activity_id, original.activity_id);
    assert_eq!(replayed.agent_id, original.agent_id);
    assert_eq!(replayed.session_id, original.session_id);
    assert_eq!(replayed.display_name, original.display_name);
    assert_eq!(replayed.recorded_at_ms, original.recorded_at_ms);
}

async fn assert_wrong_agent_binding_has_no_record(
    flow: &ReviewerParticipationFlow<'_>,
    original: &ReviewerParticipationMutationRequest,
) -> TestResult {
    let mut mismatched_target = original.target.clone();
    mismatched_target.agent_id = flow.target_b.agent_id;
    let attempted = ReviewerParticipationMutationRequest {
        target: mismatched_target,
        request_id: Uuid::now_v7(),
        expected_revision: 0,
        statement: ReviewerParticipationStatementWire::Interested,
    };
    let result = flow
        .client
        .reviewer_participation_mutate(flow.operator_bearer, &attempted)
        .await;
    assert!(
        matches!(result, Err(ClientError::Rejected { status, .. })
            if status == StatusCode::FORBIDDEN.as_u16()
                || status == StatusCode::CONFLICT.as_u16()),
        "one Agent Session must not record for another Agent binding: {result:?}"
    );
    let readback = flow
        .client
        .reviewer_participation_request(
            flow.operator_bearer,
            &ReviewerParticipationRequestReadRequest {
                target: original.target.clone(),
                request_id: attempted.request_id,
            },
        )
        .await?;
    assert!(readback.receipt.is_none());
    Ok(())
}

async fn assert_other_principal_session_has_no_record(
    flow: &ReviewerParticipationFlow<'_>,
) -> TestResult {
    // Establish that the non-operator credential owns a valid independent
    // Broker Session. Its request then presents the original operator-owned
    // Agent Session selector and must fail before any statement is recorded.
    let open_request_id = Uuid::now_v7();
    let opened = flow
        .client
        .open_session(
            &flow.non_operator.bearer,
            &AgentSessionOpenRequest {
                request_id: open_request_id.to_string(),
            },
        )
        .await?;
    assert!(opened.created);
    assert_eq!(opened.session.state, "open");
    let readback = flow
        .client
        .session_open_readback(&flow.non_operator.bearer, open_request_id)
        .await?
        .ok_or("non-operator Broker Session readback missing")?;
    assert_eq!(readback.session_id, opened.session.session_id);
    let session_id = forge_domain::AgentSessionId::from_uuid(opened.session.session_id);
    let control = flow.source.control_client.clone();
    let principal = flow.non_operator.principal;
    let verified = tokio::task::spawn_blocking(move || {
        control.agent_session_resolve(AgentSessionResolveRequest {
            session_id,
            principal,
        })
    })
    .await??;
    assert_eq!(verified.id().into_uuid(), opened.session.session_id);
    assert_eq!(verified.principal(), flow.non_operator.principal);

    let target = target_wire(flow.scope, flow.activity_id, flow.target_a);
    let attempted = ReviewerParticipationMutationRequest {
        target: target.clone(),
        request_id: Uuid::now_v7(),
        expected_revision: 0,
        statement: ReviewerParticipationStatementWire::Interested,
    };
    let result = flow
        .client
        .reviewer_participation_mutate(&flow.non_operator.bearer, &attempted)
        .await;
    assert!(
        matches!(result, Err(ClientError::Rejected { status, .. })
            if status == StatusCode::FORBIDDEN.as_u16()),
        "a principal cannot use another principal's Profile Session: {result:?}"
    );
    let readback = flow
        .client
        .reviewer_participation_request(
            flow.operator_bearer,
            &ReviewerParticipationRequestReadRequest {
                target,
                request_id: attempted.request_id,
            },
        )
        .await?;
    assert!(readback.receipt.is_none());
    let closed = flow
        .client
        .close_session(
            &flow.non_operator.bearer,
            opened.session.session_id,
            &AgentSessionCloseRequest {
                expected_revision: opened.session.revision,
            },
        )
        .await?;
    assert_eq!(closed.state, "closed");
    Ok(())
}

fn target_wire(
    scope: &WorkspaceQueryScope,
    activity_id: Uuid,
    member: &ReadyFixtureMember,
) -> ReviewerParticipationTargetWire {
    ReviewerParticipationTargetWire {
        scope: scope.clone(),
        activity_id,
        agent_id: member.agent_id,
        session_id: member.session_id,
    }
}
