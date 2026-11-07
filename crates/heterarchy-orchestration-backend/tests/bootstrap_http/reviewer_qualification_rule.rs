//! Real bearer, `OperatorPolicy`, and Session boundaries for qualification rules.

use forge_domain::{AgentSessionId, AuthenticatedPrincipal, DirectTokenId, TimestampMillis};
use heterarchy_orchestration_auth::{McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime};
use heterarchy_orchestration_backend::client::{ClientError, OrchestrationHttpClient};
use heterarchy_orchestration_backend::wire::WorkspaceQueryScope;
use heterarchy_orchestration_backend::wire::reviewer_qualification_rule::{
    ActualRuntimeFactWire, EvidenceOwnerNamespaceWire, EvidenceSourceOwnerWire,
    QualificationEvidenceRequirementWire, ReviewerQualificationRuleChangeWire,
    ReviewerQualificationRuleMutationRequest, ReviewerQualificationRuleMutationResponse,
    ReviewerQualificationRulePolicyWire, ReviewerQualificationRuleQueryRequest,
    ReviewerQualificationRuleStateWire, RuntimeFactDigestWire, RuntimeFactPredicateWire,
};
use heterarchy_orchestration_backend::wire::{AgentSessionCloseRequest, AgentSessionOpenRequest};
use hyper::StatusCode;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{ForgeHost, TestResult, current_test_time};

/// A valid bearer whose principal is intentionally absent from `OperatorPolicy`.
pub(super) struct NonOperatorCredential {
    pub(super) bearer: HeaderValue,
    pub(super) principal: AuthenticatedPrincipal,
}

/// Exact open Broker-backed Session used at one side of the rule boundary.
#[derive(Clone, Copy)]
pub(super) struct RuleSession {
    pub(super) session_id: Uuid,
    pub(super) revision: i64,
}

/// Real HTTP authorities and Session identities for one rule API scenario.
pub(super) struct ReviewerQualificationRuleFlow<'a> {
    pub(super) client: &'a OrchestrationHttpClient,
    pub(super) operator_bearer: &'a HeaderValue,
    pub(super) source: &'a ForgeHost,
    pub(super) scope: &'a WorkspaceQueryScope,
    pub(super) operator_principal: AuthenticatedPrincipal,
    pub(super) original_session: RuleSession,
    pub(super) readback_session: RuleSession,
    pub(super) non_operator: &'a NonOperatorCredential,
}

struct PublishedRule {
    request: ReviewerQualificationRuleMutationRequest,
    response: ReviewerQualificationRuleMutationResponse,
}

pub(super) fn issue_nonoperator_credential(
    token_runtime: &McpTokenRuntime,
) -> TestResult<NonOperatorCredential> {
    let issued = token_runtime.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("isolated bootstrap non-operator".to_owned())?,
        TimestampMillis::new(current_test_time()?)?,
    ))?;
    let mut bearer = HeaderValue::from_str(&format!("Bearer {}", issued.reveal().as_str()))?;
    bearer.set_sensitive(true);
    Ok(NonOperatorCredential {
        bearer,
        principal: AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
            issued.metadata().id().into_uuid(),
        )),
    })
}

/// Exercise policy publication, exact replay, CAS rejection, and actor-bound
/// readback through the isolated real Broker, Forge, and Orchestration stack.
pub(super) async fn assert_reviewer_qualification_rule_http_flow(
    flow: &ReviewerQualificationRuleFlow<'_>,
) -> TestResult {
    assert_ne!(
        flow.operator_principal, flow.non_operator.principal,
        "non-operator fixture token must have a distinct DirectToken identity"
    );
    let non_operator_session = open_verified_session(
        flow.client,
        &flow.non_operator.bearer,
        flow.source,
        flow.non_operator.principal,
    )
    .await?;
    let rule_id = Uuid::now_v7();
    let published = publish_first_generation(flow, rule_id).await?;
    assert_same_session_replay_and_current(flow, &published).await?;
    assert_new_session_cannot_retry_original_mutation(flow, &published).await?;
    assert_other_session_reads_original_receipt(flow, &published).await?;
    assert_stale_cas_conflicts_without_write(flow, rule_id).await?;
    assert_nonoperator_is_forbidden_without_write(flow, rule_id, &non_operator_session).await?;
    close_session(flow.client, flow.operator_bearer, &flow.readback_session).await?;
    close_session(
        flow.client,
        &flow.non_operator.bearer,
        &non_operator_session,
    )
    .await
}

async fn open_verified_session(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    source: &ForgeHost,
    principal: AuthenticatedPrincipal,
) -> TestResult<RuleSession> {
    let request_id = Uuid::now_v7();
    let opened = client
        .open_session(
            bearer,
            &AgentSessionOpenRequest {
                request_id: request_id.to_string(),
            },
        )
        .await?;
    assert!(opened.created);
    assert_eq!(opened.session.request_id, request_id);
    assert_eq!(opened.session.state, "open");
    assert_eq!(
        client
            .session_open_readback(bearer, request_id)
            .await?
            .ok_or("Broker-backed rule-test Session receipt missing")?
            .session_id,
        opened.session.session_id
    );

    let session_id = AgentSessionId::from_uuid(opened.session.session_id);
    let control = source.control_client.clone();
    let verified = tokio::task::spawn_blocking(move || {
        control.agent_session_resolve(super::AgentSessionResolveRequest {
            session_id,
            principal,
        })
    })
    .await??;
    assert_eq!(verified.id().into_uuid(), opened.session.session_id);
    assert_eq!(verified.principal(), principal);
    Ok(RuleSession {
        session_id: opened.session.session_id,
        revision: opened.session.revision,
    })
}

async fn publish_first_generation(
    flow: &ReviewerQualificationRuleFlow<'_>,
    rule_id: Uuid,
) -> TestResult<PublishedRule> {
    let AuthenticatedPrincipal::DirectToken(operator_token) = flow.operator_principal else {
        return Err("bootstrap rule writer is not a DirectToken".into());
    };
    let request = publish_request(
        flow.scope,
        flow.original_session.session_id,
        rule_id,
        Uuid::now_v7(),
        None,
    );
    let response = flow
        .client
        .reviewer_qualification_rule_mutate(flow.operator_bearer, &request)
        .await?;
    assert!(!response.replayed);
    assert_eq!(response.scope, *flow.scope);
    assert_eq!(response.session_id, flow.original_session.session_id);
    assert_eq!(response.rule_id, rule_id);
    assert_eq!(response.request_id, request.request_id);
    assert_eq!(response.generation.generation, 1);
    assert_eq!(
        response.generation.original_direct_token_id,
        operator_token.into_uuid()
    );
    assert_eq!(
        response.generation.original_session_id,
        flow.original_session.session_id
    );
    assert_eq!(
        response.generation.state,
        ReviewerQualificationRuleStateWire::ActiveForFuturePools
    );
    assert_eq!(
        response.generation.policy,
        match &request.change {
            ReviewerQualificationRuleChangeWire::Publish { policy } => policy.clone(),
            ReviewerQualificationRuleChangeWire::DeactivateForFuturePools => {
                return Err("rule fixture unexpectedly deactivates".into());
            }
        }
    );
    Ok(PublishedRule { request, response })
}

async fn assert_same_session_replay_and_current(
    flow: &ReviewerQualificationRuleFlow<'_>,
    published: &PublishedRule,
) -> TestResult {
    let replay = flow
        .client
        .reviewer_qualification_rule_mutate(flow.operator_bearer, &published.request)
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.generation, published.response.generation);

    let current = read_current_rule(
        flow,
        flow.operator_bearer,
        flow.original_session.session_id,
        published.response.rule_id,
    )
    .await?;
    assert_eq!(current.current, Some(published.response.generation.clone()));
    Ok(())
}

async fn assert_other_session_reads_original_receipt(
    flow: &ReviewerQualificationRuleFlow<'_>,
    published: &PublishedRule,
) -> TestResult {
    let readback = flow
        .client
        .reviewer_qualification_rule_mutation_readback(
            flow.operator_bearer,
            &published.request,
            flow.scope,
            flow.readback_session.session_id,
        )
        .await?;
    assert_eq!(readback.scope, *flow.scope);
    assert_eq!(readback.session_id, flow.readback_session.session_id);
    assert_eq!(readback.rule_id, published.response.rule_id);
    assert_eq!(readback.request_id, published.request.request_id);
    let generation = readback
        .generation
        .ok_or("cross-Session operator readback omitted the original rule receipt")?;
    assert_eq!(generation, published.response.generation);
    assert_eq!(
        generation.original_session_id,
        flow.original_session.session_id
    );
    Ok(())
}

async fn assert_new_session_cannot_retry_original_mutation(
    flow: &ReviewerQualificationRuleFlow<'_>,
    published: &PublishedRule,
) -> TestResult {
    let changed_session_retry = ReviewerQualificationRuleMutationRequest {
        session_id: flow.readback_session.session_id,
        ..published.request.clone()
    };
    let rejected = flow
        .client
        .reviewer_qualification_rule_mutate(flow.operator_bearer, &changed_session_retry)
        .await;
    assert!(
        matches!(rejected, Err(ClientError::Rejected { status, .. }) if status == StatusCode::CONFLICT.as_u16()),
        "same request replay from a different Session must conflict: {rejected:?}"
    );
    Ok(())
}

async fn assert_stale_cas_conflicts_without_write(
    flow: &ReviewerQualificationRuleFlow<'_>,
    rule_id: Uuid,
) -> TestResult {
    let stale = publish_request(
        flow.scope,
        flow.original_session.session_id,
        rule_id,
        Uuid::now_v7(),
        None,
    );
    let result = flow
        .client
        .reviewer_qualification_rule_mutate(flow.operator_bearer, &stale)
        .await;
    assert!(
        matches!(result, Err(ClientError::Rejected { status, .. }) if status == StatusCode::CONFLICT.as_u16()),
        "stale first-generation CAS should conflict: {result:?}"
    );
    let readback = flow
        .client
        .reviewer_qualification_rule_mutation_readback(
            flow.operator_bearer,
            &stale,
            flow.scope,
            flow.readback_session.session_id,
        )
        .await?;
    assert!(readback.generation.is_none());
    Ok(())
}

async fn assert_nonoperator_is_forbidden_without_write(
    flow: &ReviewerQualificationRuleFlow<'_>,
    rule_id: Uuid,
    session: &RuleSession,
) -> TestResult {
    let nonoperator_request = publish_request(
        flow.scope,
        session.session_id,
        rule_id,
        Uuid::now_v7(),
        Some(1),
    );
    let rejected = flow
        .client
        .reviewer_qualification_rule_mutate(&flow.non_operator.bearer, &nonoperator_request)
        .await;
    assert!(
        matches!(rejected, Err(ClientError::Rejected { status, .. }) if status == StatusCode::FORBIDDEN.as_u16()),
        "valid non-operator DirectToken should pass bearer auth and fail OperatorPolicy: {rejected:?}"
    );

    let readback = flow
        .client
        .reviewer_qualification_rule_mutation_readback(
            flow.operator_bearer,
            &nonoperator_request,
            flow.scope,
            flow.readback_session.session_id,
        )
        .await?;
    assert!(readback.generation.is_none());
    let current = read_current_rule(
        flow,
        flow.operator_bearer,
        flow.readback_session.session_id,
        rule_id,
    )
    .await?;
    assert_eq!(
        current.current.as_ref().map(|record| record.generation),
        Some(1)
    );
    assert_eq!(
        current
            .current
            .as_ref()
            .map(|record| record.original_direct_token_id),
        match flow.operator_principal {
            AuthenticatedPrincipal::DirectToken(id) => Some(id.into_uuid()),
            AuthenticatedPrincipal::TrustedStdio | AuthenticatedPrincipal::OAuthGrant(_) => None,
        }
    );
    Ok(())
}

async fn read_current_rule(
    flow: &ReviewerQualificationRuleFlow<'_>,
    bearer: &HeaderValue,
    session_id: Uuid,
    rule_id: Uuid,
) -> TestResult<heterarchy_orchestration_backend::wire::reviewer_qualification_rule::ReviewerQualificationRuleQueryResponse>{
    Ok(flow
        .client
        .reviewer_qualification_rule(
            bearer,
            &ReviewerQualificationRuleQueryRequest {
                scope: flow.scope.clone(),
                session_id,
                rule_id,
            },
        )
        .await?)
}

async fn close_session(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    session: &RuleSession,
) -> TestResult {
    let closed = client
        .close_session(
            bearer,
            session.session_id,
            &AgentSessionCloseRequest {
                expected_revision: session.revision,
            },
        )
        .await?;
    assert_eq!(closed.state, "closed");
    Ok(())
}

fn publish_request(
    scope: &WorkspaceQueryScope,
    session_id: Uuid,
    rule_id: Uuid,
    request_id: Uuid,
    expected_current_generation: Option<u64>,
) -> ReviewerQualificationRuleMutationRequest {
    ReviewerQualificationRuleMutationRequest {
        scope: scope.clone(),
        session_id,
        rule_id,
        request_id,
        expected_current_generation,
        change: ReviewerQualificationRuleChangeWire::Publish {
            policy: user_selected_policy(),
        },
    }
}

fn user_selected_policy() -> ReviewerQualificationRulePolicyWire {
    ReviewerQualificationRulePolicyWire {
        domain: "bootstrap-review".to_owned(),
        permitted_scope: "workspace_review".to_owned(),
        evidence_requirements: vec![
            QualificationEvidenceRequirementWire::ActualRuntimeProvenance {
                source_owner: EvidenceSourceOwnerWire {
                    namespace: EvidenceOwnerNamespaceWire::RuntimeAdapter,
                    id: Uuid::from_u128(0xE901),
                },
                fact_predicates: vec![
                    RuntimeFactPredicateWire::Text {
                        fact: ActualRuntimeFactWire::Model,
                        allowed_values: vec!["fixture-selected-model".to_owned()],
                    },
                    RuntimeFactPredicateWire::Text {
                        fact: ActualRuntimeFactWire::ToolVersions,
                        allowed_values: vec!["fixture-selected-tools".to_owned()],
                    },
                    RuntimeFactPredicateWire::Digest {
                        fact: ActualRuntimeFactWire::EnvironmentFingerprint,
                        allowed_values: vec![RuntimeFactDigestWire {
                            algorithm: "sha256".to_owned(),
                            bytes: vec![0x5a; 32],
                        }],
                    },
                ],
            },
        ],
    }
}
