//! Authenticated Backend adapters for immutable Reviewer qualification-rule history.
//!
//! The routes publish and read explicit typed policy data only. They never
//! evaluate a person, grant evidence access, appoint a Reviewer, or change a
//! frozen case.

use axum::extract::DefaultBodyLimit;
use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError,
    reviewer_qualification_rule::ReviewerQualificationRuleMutationSpec,
};
use uuid::Uuid;

use crate::wire::reviewer_qualification_rule::{
    MAX_REVIEWER_QUALIFICATION_RULE_BODY_BYTES, MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_PAGE_SIZE,
    ReviewerQualificationRuleHistoryRequest, ReviewerQualificationRuleHistoryResponse,
    ReviewerQualificationRuleMutationRequest, ReviewerQualificationRuleMutationResponse,
    ReviewerQualificationRuleQueryRequest, ReviewerQualificationRuleQueryResponse,
    ReviewerQualificationRuleRequestReadRequest, ReviewerQualificationRuleRequestReadResponse,
    generation_wire, history_response, valid_generation, valid_scope,
};
use crate::{ApiError, BackendState};

/// Register strict Workspace-scoped operator-only qualification-rule APIs.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-qualification-rules/publish",
            post(publish_rule),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-qualification-rules/deactivate",
            post(deactivate_rule),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-qualification-rules/query",
            post(read_current),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-qualification-rules/request",
            post(read_request),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-qualification-rules/history",
            post(read_history),
        )
        .layer(DefaultBodyLimit::max(
            MAX_REVIEWER_QUALIFICATION_RULE_BODY_BYTES,
        ))
}

async fn publish_rule(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ReviewerQualificationRuleMutationRequest>,
) -> Result<Json<ReviewerQualificationRuleMutationResponse>, ApiError> {
    mutate_rule(state, workspace_path, principal, request, true).await
}

async fn deactivate_rule(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ReviewerQualificationRuleMutationRequest>,
) -> Result<Json<ReviewerQualificationRuleMutationResponse>, ApiError> {
    mutate_rule(state, workspace_path, principal, request, false).await
}

async fn mutate_rule(
    state: BackendState,
    workspace_path: String,
    principal: AuthenticatedPrincipal,
    request: ReviewerQualificationRuleMutationRequest,
    is_publish: bool,
) -> Result<Json<ReviewerQualificationRuleMutationResponse>, ApiError> {
    validate_mutation_scope(&workspace_path, &request)?;
    if request.change.is_publish() != is_publish {
        return Err(ApiError::InvalidIdentity);
    }
    let change = request
        .operations_change()
        .map_err(|error| ApiError::Runtime(RuntimeError::from(error)))?;
    let spec = ReviewerQualificationRuleMutationSpec {
        session_id: request.session_id,
        rule_id: request.rule_id,
        request_id: request.request_id,
        expected_current_generation: request.expected_current_generation,
        change,
    };
    let scope = request.scope.clone();
    let outcome = state
        .run_scoped_operator_mutation(
            scope.clone(),
            request.session_id,
            principal,
            move |runtime, _| {
                runtime.mutate_reviewer_qualification_rule(scope.workspace_id, spec, principal)
            },
        )
        .await?;
    let generation = generation_wire(outcome.generation()).map_err(ApiError::Runtime)?;
    let issuer_matches = match principal {
        AuthenticatedPrincipal::DirectToken(id) => {
            generation.original_direct_token_id == id.into_uuid()
        }
        AuthenticatedPrincipal::TrustedStdio | AuthenticatedPrincipal::OAuthGrant(_) => false,
    };
    if !generation.matches_mutation(&request) || !issuer_matches {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Json(ReviewerQualificationRuleMutationResponse {
        scope: request.scope,
        session_id: request.session_id,
        rule_id: request.rule_id,
        request_id: request.request_id,
        replayed: outcome.replayed(),
        generation,
    }))
}

async fn read_current(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ReviewerQualificationRuleQueryRequest>,
) -> Result<Json<ReviewerQualificationRuleQueryResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    require_rule_id(request.rule_id)?;
    let scope = request.scope.clone();
    let workspace_id = scope.workspace_id;
    let rule_id = request.rule_id;
    let current = run_scoped_operator_query(
        &state,
        scope.clone(),
        request.session_id,
        principal,
        move |runtime| runtime.reviewer_qualification_rule(workspace_id, rule_id, principal),
    )
    .await?
    .as_ref()
    .map(generation_wire)
    .transpose()
    .map_err(ApiError::Runtime)?;
    if current.as_ref().is_some_and(|record| {
        record.workspace_id != request.scope.workspace_id || record.rule_id != request.rule_id
    }) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Json(ReviewerQualificationRuleQueryResponse {
        scope: request.scope,
        session_id: request.session_id,
        rule_id: request.rule_id,
        current,
    }))
}

async fn read_request(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ReviewerQualificationRuleRequestReadRequest>,
) -> Result<Json<ReviewerQualificationRuleRequestReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    require_rule_id(request.rule_id)?;
    require_rule_id(request.request_id)?;
    let scope = request.scope.clone();
    let workspace_id = scope.workspace_id;
    let rule_id = request.rule_id;
    let request_id = request.request_id;
    let generation = run_scoped_operator_query(
        &state,
        scope.clone(),
        request.session_id,
        principal,
        move |runtime| {
            runtime.reviewer_qualification_rule_request(
                workspace_id,
                rule_id,
                request_id,
                principal,
            )
        },
    )
    .await?
    .as_ref()
    .map(generation_wire)
    .transpose()
    .map_err(ApiError::Runtime)?;
    if generation.as_ref().is_some_and(|record| {
        record.workspace_id != request.scope.workspace_id
            || record.rule_id != request.rule_id
            || record.request_id != request.request_id
    }) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Json(ReviewerQualificationRuleRequestReadResponse {
        scope: request.scope,
        session_id: request.session_id,
        rule_id: request.rule_id,
        request_id: request.request_id,
        generation,
    }))
}

async fn read_history(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ReviewerQualificationRuleHistoryRequest>,
) -> Result<Json<ReviewerQualificationRuleHistoryResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    require_rule_id(request.rule_id)?;
    if request.limit == 0
        || request.limit > MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_PAGE_SIZE
        || request
            .after_generation
            .is_some_and(|value| !valid_generation(value))
    {
        return Err(ApiError::InvalidIdentity);
    }
    let scope = request.scope.clone();
    let workspace_id = scope.workspace_id;
    let rule_id = request.rule_id;
    let after_generation = request.after_generation;
    let limit = request.limit;
    let page = run_scoped_operator_query(
        &state,
        scope.clone(),
        request.session_id,
        principal,
        move |runtime| {
            runtime.reviewer_qualification_rule_history(
                workspace_id,
                rule_id,
                after_generation,
                limit,
                principal,
            )
        },
    )
    .await?;
    let response = history_response(
        request.scope.clone(),
        request.session_id,
        request.rule_id,
        &request,
        &page,
    )
    .map_err(ApiError::Runtime)?;
    if response.entries.iter().any(|entry| {
        entry.workspace_id != request.scope.workspace_id || entry.rule_id != request.rule_id
    }) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Json(response))
}

/// Operator-only read lane. It deliberately skips Workspace-member access:
/// operator authorization and the live source proof are the selected policy.
/// The principal and its open Session are checked before the potentially
/// blocking source call and again at catalog admission after that call.
async fn run_scoped_operator_query<T, F>(
    state: &BackendState,
    scope: crate::wire::WorkspaceQueryScope,
    session_id: Uuid,
    principal: AuthenticatedPrincipal,
    operation: F,
) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
{
    let owned_state = state.clone();
    state
        .run_owned(async move {
            owned_state
                .catalog_job(move |runtime| {
                    runtime.require_operator(principal)?;
                    runtime.require_open_session(session_id, principal)
                })
                .await?;
            owned_state.workspace_scope_proof(scope.clone()).await?;
            owned_state
                .catalog_job(move |runtime| {
                    runtime.require_operator(principal)?;
                    runtime.require_open_session(session_id, principal)?;
                    operation(runtime)
                })
                .await
        })
        .await
}

fn validate_mutation_scope(
    workspace_path: &str,
    request: &ReviewerQualificationRuleMutationRequest,
) -> Result<(), ApiError> {
    validate_read_scope(workspace_path, &request.scope, request.session_id)?;
    if !request.validate_identity() {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

fn validate_read_scope(
    workspace_path: &str,
    scope: &crate::wire::WorkspaceQueryScope,
    session_id: Uuid,
) -> Result<(), ApiError> {
    crate::app_support::validate_workspace_reference(scope, Some(session_id))?;
    if !valid_scope(scope, session_id)
        || crate::canonical_uuid(workspace_path)? != scope.workspace_id
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

fn require_rule_id(value: Uuid) -> Result<(), ApiError> {
    if value.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use axum::extract::{Extension, Path, State};
    use forge_domain::{AuthenticatedPrincipal, DirectTokenId};
    use heterarchy_orchestration_runtime::{OrchestrationRuntime, access::OperatorPolicy};
    use uuid::Uuid;

    use super::*;
    use crate::wire::reviewer_qualification_rule::{
        ActualRuntimeFactWire, EvidenceOwnerNamespaceWire, EvidenceSourceOwnerWire,
        QualificationEvidenceRequirementWire, ReviewerQualificationRuleChangeWire,
        ReviewerQualificationRulePolicyWire, RuntimeFactPredicateWire,
    };
    use crate::{RealtimeHub, wire::WorkspaceQueryScope};

    fn request(
        workspace_id: Uuid,
        principal_session_id: Uuid,
    ) -> ReviewerQualificationRuleMutationRequest {
        ReviewerQualificationRuleMutationRequest {
            scope: WorkspaceQueryScope {
                workspace_id,
                location_id: Uuid::from_u128(0xE703),
                location_revision: 1,
                workspace_revision: 1,
                locator: "hf1:qualification-test".to_owned(),
            },
            session_id: principal_session_id,
            rule_id: Uuid::from_u128(0xE705),
            request_id: Uuid::from_u128(0xE706),
            expected_current_generation: None,
            change: ReviewerQualificationRuleChangeWire::Publish {
                policy: ReviewerQualificationRulePolicyWire {
                    domain: "user-selected-domain".to_owned(),
                    permitted_scope: "user-selected-scope".to_owned(),
                    evidence_requirements: vec![
                        QualificationEvidenceRequirementWire::ActualRuntimeProvenance {
                            source_owner: EvidenceSourceOwnerWire {
                                namespace: EvidenceOwnerNamespaceWire::RuntimeAdapter,
                                id: Uuid::from_u128(0xE707),
                            },
                            fact_predicates: vec![
                                RuntimeFactPredicateWire::Text {
                                    fact: ActualRuntimeFactWire::Model,
                                    allowed_values: vec!["user-selected-model".to_owned()],
                                },
                                RuntimeFactPredicateWire::Text {
                                    fact: ActualRuntimeFactWire::ToolVersions,
                                    allowed_values: vec!["user-selected-tools".to_owned()],
                                },
                                RuntimeFactPredicateWire::Text {
                                    fact: ActualRuntimeFactWire::EnvironmentFingerprint,
                                    allowed_values: vec!["user-selected-environment".to_owned()],
                                },
                            ],
                        },
                    ],
                },
            },
        }
    }

    #[tokio::test]
    async fn authenticated_non_operator_is_denied_before_workspace_source_proof()
    -> Result<(), Box<dyn Error>> {
        let data_root =
            std::env::temp_dir().join(format!("qualification-rule-auth-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&data_root)?;
        let runtime = OrchestrationRuntime::open(&data_root)?
            .with_operator_policy(OperatorPolicy::new(Vec::new())?);
        let state = BackendState::new(runtime, Arc::new(RealtimeHub::new()));
        // This principal represents successful bearer authentication. The
        // route has no configured coordinator/source authority, so reaching
        // that proof would return WorkspaceScopeUnavailable instead.
        let authenticated_participant =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xE701)));
        let workspace_id = Uuid::from_u128(0xE702);
        let response = publish_rule(
            State(state.clone()),
            Path(workspace_id.to_string()),
            Extension(authenticated_participant),
            Json(request(workspace_id, Uuid::from_u128(0xE704))),
        )
        .await;
        assert!(matches!(
            response,
            Err(ApiError::Runtime(RuntimeError::Forbidden))
        ));

        state.catalog_execution.stop_admission();
        state.catalog_execution.drain().await;
        drop(state);
        std::fs::remove_dir_all(data_root)?;
        Ok(())
    }
}
