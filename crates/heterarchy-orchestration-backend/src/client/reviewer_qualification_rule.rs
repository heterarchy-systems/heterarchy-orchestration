//! Bounded typed HTTP client for user-authored Reviewer qualification-rule history.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::reviewer_qualification_rule::{
    MAX_REVIEWER_QUALIFICATION_RULE_BODY_BYTES, MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_PAGE_SIZE,
    ReviewerQualificationRuleHistoryRequest, ReviewerQualificationRuleHistoryResponse,
    ReviewerQualificationRuleMutationRequest, ReviewerQualificationRuleMutationResponse,
    ReviewerQualificationRuleQueryRequest, ReviewerQualificationRuleQueryResponse,
    ReviewerQualificationRuleRequestReadRequest, ReviewerQualificationRuleRequestReadResponse,
    ReviewerQualificationRuleStateWire, valid_generation, valid_scope,
};

/// 64 immutable rows may each carry both the stored policy and its publish
/// change payload, each bounded by Operations to 16 KiB, plus row/envelope data.
/// This route-specific allowance stays below the shared 8 MiB hard ceiling.
const MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

impl OrchestrationHttpClient {
    /// Publish or deactivate one immutable future-pool rule generation.
    ///
    /// No automatic retry occurs. An uncertain response retains the exact
    /// Workspace/rule/request/CAS/policy payload for an explicit read-only
    /// readback under a current authorized operator Session.
    ///
    /// # Errors
    /// Rejects malformed requests, typed service failures, bounded transport
    /// failures, or an uncorrelated response with exact readback identity.
    pub async fn reviewer_qualification_rule_mutate(
        &self,
        authorization: &HeaderValue,
        request: &ReviewerQualificationRuleMutationRequest,
    ) -> Result<ReviewerQualificationRuleMutationResponse, ClientError> {
        if !valid_mutation_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::ReviewerQualificationRuleMutation(Box::new(request.clone()));
        let action = if request.change.is_publish() {
            "publish"
        } else {
            "deactivate"
        };
        let response: ReviewerQualificationRuleMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/reviewer-qualification-rules/{action}",
                    request.scope.workspace_id,
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if valid_mutation_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::UnknownOutcome { readback })
        }
    }

    /// Read one current rule generation under a current authorized operator.
    ///
    /// # Errors
    /// Rejects malformed selectors, bounded transport failures, or an invalid
    /// response scope/identity.
    pub async fn reviewer_qualification_rule(
        &self,
        authorization: &HeaderValue,
        request: &ReviewerQualificationRuleQueryRequest,
    ) -> Result<ReviewerQualificationRuleQueryResponse, ClientError> {
        if !valid_scope(&request.scope, request.session_id) || request.rule_id.is_nil() {
            return Err(ClientError::InvalidRequest);
        }
        let response: ReviewerQualificationRuleQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/reviewer-qualification-rules/query",
                    request.scope.workspace_id,
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_current_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Read one exact immutable mutation receipt without replaying the writer.
    ///
    /// # Errors
    /// Rejects malformed selectors, bounded transport failures, or a response
    /// with foreign Workspace/rule/request identity.
    pub async fn reviewer_qualification_rule_request(
        &self,
        authorization: &HeaderValue,
        request: &ReviewerQualificationRuleRequestReadRequest,
    ) -> Result<ReviewerQualificationRuleRequestReadResponse, ClientError> {
        if !valid_scope(&request.scope, request.session_id)
            || request.rule_id.is_nil()
            || request.request_id.is_nil()
        {
            return Err(ClientError::InvalidRequest);
        }
        let response: ReviewerQualificationRuleRequestReadResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/reviewer-qualification-rules/request",
                    request.scope.workspace_id,
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_request_read_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Resolve an uncertain mutation using its exact identity and a current
    /// Session. A missing receipt is explicit but does not prove that a
    /// concurrent original commit cannot still become visible.
    ///
    /// This operation is read-only and never automatically resends the write.
    ///
    /// # Errors
    /// Rejects invalid original/current scope or Session selectors, bounded
    /// transport failures, typed service rejections, or a receipt whose
    /// original Session or immutable mutation identity differs from the target.
    pub async fn reviewer_qualification_rule_mutation_readback(
        &self,
        authorization: &HeaderValue,
        original: &ReviewerQualificationRuleMutationRequest,
        current_scope: &crate::wire::WorkspaceQueryScope,
        current_session_id: Uuid,
    ) -> Result<ReviewerQualificationRuleRequestReadResponse, ClientError> {
        if !valid_mutation_request(original)
            || !valid_scope(current_scope, current_session_id)
            || current_scope.workspace_id != original.scope.workspace_id
        {
            return Err(ClientError::InvalidRequest);
        }
        let request = original
            .readback_request(current_scope, current_session_id)
            .ok_or(ClientError::InvalidRequest)?;
        let response = self
            .reviewer_qualification_rule_request(authorization, &request)
            .await?;
        if response
            .generation
            .as_ref()
            .is_some_and(|generation| !generation.matches_mutation(original))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read one bounded ascending page of immutable rule generations.
    ///
    /// # Errors
    /// Rejects invalid scope, Session, rule, cursor, or page-limit selectors;
    /// bounded transport failures; or a response with mismatched identity,
    /// generation ordering, cursor, or payload bounds.
    pub async fn reviewer_qualification_rule_history(
        &self,
        authorization: &HeaderValue,
        request: &ReviewerQualificationRuleHistoryRequest,
    ) -> Result<ReviewerQualificationRuleHistoryResponse, ClientError> {
        if !valid_scope(&request.scope, request.session_id)
            || request.rule_id.is_nil()
            || request.limit == 0
            || request.limit > MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_PAGE_SIZE
            || request
                .after_generation
                .is_some_and(|value| !valid_generation(value))
        {
            return Err(ClientError::InvalidRequest);
        }
        let response: ReviewerQualificationRuleHistoryResponse = self
            .call_with_response_limit(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/reviewer-qualification-rules/history",
                    request.scope.workspace_id,
                ),
                Some(request),
                None,
                MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_RESPONSE_BYTES,
            )
            .await?;
        if valid_history_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }
}

fn valid_mutation_request(request: &ReviewerQualificationRuleMutationRequest) -> bool {
    super::bounded_json_with_limit(request, MAX_REVIEWER_QUALIFICATION_RULE_BODY_BYTES).is_ok()
        && valid_scope(&request.scope, request.session_id)
        && !request.rule_id.is_nil()
        && !request.request_id.is_nil()
        && request
            .expected_current_generation
            .is_none_or(|value| valid_generation(value) && value < i64::MAX as u64)
        && request.operations_change().is_ok()
}

fn valid_mutation_response(
    response: &ReviewerQualificationRuleMutationResponse,
    request: &ReviewerQualificationRuleMutationRequest,
) -> bool {
    response.scope == request.scope
        && response.session_id == request.session_id
        && response.rule_id == request.rule_id
        && response.request_id == request.request_id
        && response.generation.valid()
        && response.generation.matches_mutation(request)
}

fn valid_current_response(
    response: &ReviewerQualificationRuleQueryResponse,
    request: &ReviewerQualificationRuleQueryRequest,
) -> bool {
    response.scope == request.scope
        && response.session_id == request.session_id
        && response.rule_id == request.rule_id
        && response.current.as_ref().is_none_or(|generation| {
            generation.valid()
                && generation.workspace_id == request.scope.workspace_id
                && generation.rule_id == request.rule_id
                && valid_state_policy_pair(generation)
        })
}

fn valid_request_read_response(
    response: &ReviewerQualificationRuleRequestReadResponse,
    request: &ReviewerQualificationRuleRequestReadRequest,
) -> bool {
    response.scope == request.scope
        && response.session_id == request.session_id
        && response.rule_id == request.rule_id
        && response.request_id == request.request_id
        && response.generation.as_ref().is_none_or(|generation| {
            generation.valid()
                && generation.workspace_id == request.scope.workspace_id
                && generation.rule_id == request.rule_id
                && generation.request_id == request.request_id
                && valid_state_policy_pair(generation)
        })
}

fn valid_history_response(
    response: &ReviewerQualificationRuleHistoryResponse,
    request: &ReviewerQualificationRuleHistoryRequest,
) -> bool {
    let Ok(page_limit) = usize::try_from(request.limit) else {
        return false;
    };
    if response.scope != request.scope
        || response.session_id != request.session_id
        || response.rule_id != request.rule_id
        || response.after_generation != request.after_generation
        || response.limit != request.limit
        || response.entries.len() > page_limit
        || response.entries.iter().any(|generation| {
            !generation.valid()
                || generation.workspace_id != request.scope.workspace_id
                || generation.rule_id != request.rule_id
                || request
                    .after_generation
                    .is_some_and(|after| generation.generation <= after)
                || !valid_state_policy_pair(generation)
        })
        || response
            .entries
            .windows(2)
            .any(|pair| pair[0].generation >= pair[1].generation)
    {
        return false;
    }
    match (response.has_more, response.next_after_generation) {
        (false, None) => true,
        (true, Some(next)) => {
            response.entries.len() == page_limit
                && response
                    .entries
                    .last()
                    .is_some_and(|last| last.generation == next)
        }
        _ => false,
    }
}

fn valid_state_policy_pair(
    generation: &crate::wire::reviewer_qualification_rule::ReviewerQualificationRuleGenerationWire,
) -> bool {
    match (&generation.change, generation.state) {
        (
            crate::wire::reviewer_qualification_rule::ReviewerQualificationRuleChangeWire::Publish { policy },
            ReviewerQualificationRuleStateWire::ActiveForFuturePools,
        ) => &generation.policy == policy,
        (
            crate::wire::reviewer_qualification_rule::ReviewerQualificationRuleChangeWire::DeactivateForFuturePools,
            ReviewerQualificationRuleStateWire::DeactivatedForFuturePools,
        ) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use heterarchy_orchestration_operations::reviewer_qualification_rule::ReviewerQualificationRuleChange as OperationsRuleChange;
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::wire::reviewer_qualification_rule::{
        ActualRuntimeFactWire, EvidenceOwnerNamespaceWire, EvidenceSourceOwnerWire,
        QualificationEvidenceRequirementWire, ReviewerQualificationRuleChangeWire,
        ReviewerQualificationRuleHistoryResponse, ReviewerQualificationRulePolicyWire,
        ReviewerQualificationRuleQueryRequest, ReviewerQualificationRuleStateWire,
        RuntimeFactPredicateWire,
    };

    fn scope() -> crate::wire::WorkspaceQueryScope {
        crate::wire::WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "hf1:qualification-test".to_owned(),
        }
    }

    fn request() -> ReviewerQualificationRuleMutationRequest {
        ReviewerQualificationRuleMutationRequest {
            scope: scope(),
            session_id: Uuid::from_u128(3),
            rule_id: Uuid::from_u128(4),
            request_id: Uuid::from_u128(5),
            expected_current_generation: None,
            change: ReviewerQualificationRuleChangeWire::Publish {
                policy: ReviewerQualificationRulePolicyWire {
                    domain: "user-selected-domain".to_owned(),
                    permitted_scope: "user-selected-scope".to_owned(),
                    evidence_requirements: vec![
                        QualificationEvidenceRequirementWire::ActualRuntimeProvenance {
                            source_owner: EvidenceSourceOwnerWire {
                                namespace: EvidenceOwnerNamespaceWire::RuntimeAdapter,
                                id: Uuid::from_u128(6),
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

    #[test]
    fn readback_selector_uses_current_session_without_changing_mutation_identity()
    -> Result<(), Box<dyn Error>> {
        let original = request();
        let current_scope = crate::wire::WorkspaceQueryScope {
            workspace_id: original.scope.workspace_id,
            location_id: Uuid::from_u128(30),
            location_revision: 31,
            workspace_revision: 32,
            locator: "hf1:current-scope".to_owned(),
        };
        let current_session = Uuid::from_u128(99);
        let readback = original
            .readback_request(&current_scope, current_session)
            .ok_or_else(|| std::io::Error::other("same Workspace readback scope"))?;
        assert_eq!(readback.scope, current_scope);
        assert_eq!(readback.session_id, current_session);
        assert_eq!(readback.rule_id, original.rule_id);
        assert_eq!(readback.request_id, original.request_id);
        let foreign_scope = crate::wire::WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(101),
            ..current_scope
        };
        assert!(
            original
                .readback_request(&foreign_scope, current_session)
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn mutation_receipt_correlation_checks_scope_cas_payload_and_original_session()
    -> Result<(), Box<dyn Error>> {
        let request = request();
        let ReviewerQualificationRuleChangeWire::Publish { policy } = &request.change else {
            return Err(std::io::Error::other("expected publish fixture").into());
        };
        let generation =
            crate::wire::reviewer_qualification_rule::ReviewerQualificationRuleGenerationWire {
                workspace_id: request.scope.workspace_id,
                rule_id: request.rule_id,
                generation: 1,
                expected_current_generation: None,
                request_id: request.request_id,
                original_direct_token_id: Uuid::from_u128(7),
                original_session_id: request.session_id,
                state: ReviewerQualificationRuleStateWire::ActiveForFuturePools,
                policy: policy.clone(),
                change: request.change.clone(),
                created_at_ms: 8,
            };
        let mut response = ReviewerQualificationRuleMutationResponse {
            scope: request.scope.clone(),
            session_id: request.session_id,
            rule_id: request.rule_id,
            request_id: request.request_id,
            replayed: false,
            generation,
        };
        assert!(valid_mutation_response(&response, &request));

        let current_scope = crate::wire::WorkspaceQueryScope {
            workspace_id: request.scope.workspace_id,
            location_id: Uuid::from_u128(30),
            location_revision: 31,
            workspace_revision: 32,
            locator: "hf1:current-scope".to_owned(),
        };
        let current_session_id = Uuid::from_u128(99);
        let readback_request = request
            .readback_request(&current_scope, current_session_id)
            .ok_or_else(|| std::io::Error::other("readback stays in the original Workspace"))?;
        let readback_response = ReviewerQualificationRuleRequestReadResponse {
            scope: current_scope,
            session_id: current_session_id,
            rule_id: request.rule_id,
            request_id: request.request_id,
            generation: Some(response.generation.clone()),
        };
        assert!(valid_request_read_response(
            &readback_response,
            &readback_request
        ));
        assert!(
            readback_response
                .generation
                .as_ref()
                .is_some_and(|generation| generation.matches_mutation(&request))
        );

        let mut different_original_session = response.clone();
        different_original_session.generation.original_session_id = Uuid::from_u128(8);
        assert!(!valid_mutation_response(
            &different_original_session,
            &request
        ));

        response.generation.expected_current_generation = Some(1);
        assert!(!valid_mutation_response(&response, &request));
        Ok(())
    }

    #[tokio::test]
    async fn malformed_success_retains_exact_write_for_readback_without_retry()
    -> Result<(), Box<dyn Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(
            listener.local_addr()?,
            std::time::Duration::from_secs(1),
        )?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut bytes = [0_u8; 16_384];
            let count = socket.read(&mut bytes).await?;
            let request_text = String::from_utf8_lossy(&bytes[..count]).to_ascii_lowercase();
            let body = b"{}";
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await?;
            socket.write_all(body).await?;
            Ok::<_, std::io::Error>(request_text)
        });
        let original = request();
        let result = client
            .reviewer_qualification_rule_mutate(
                &HeaderValue::from_static("Bearer current-operator"),
                &original,
            )
            .await;
        assert_eq!(
            result,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::ReviewerQualificationRuleMutation(Box::new(
                    original.clone()
                ))
            })
        );
        let received = server.await??;
        assert!(received.contains(
            "post /internal/v1/workspaces/00000000-0000-0000-0000-000000000001/reviewer-qualification-rules/publish http/1.1"
        ));
        assert!(received.contains("authorization: bearer current-operator"));
        Ok(())
    }

    fn max_shaped_policy() -> ReviewerQualificationRulePolicyWire {
        let allowed_values = (0_u8..9)
            .map(|index| format!("{}{:02}", "x".repeat(510), index))
            .collect::<Vec<_>>();
        ReviewerQualificationRulePolicyWire {
            domain: "D".repeat(128),
            permitted_scope: "S".repeat(128),
            evidence_requirements: vec![
                QualificationEvidenceRequirementWire::ActualRuntimeProvenance {
                    source_owner: EvidenceSourceOwnerWire {
                        namespace: EvidenceOwnerNamespaceWire::RuntimeAdapter,
                        id: Uuid::from_u128(600),
                    },
                    fact_predicates: vec![
                        RuntimeFactPredicateWire::Text {
                            fact: ActualRuntimeFactWire::Model,
                            allowed_values: allowed_values.clone(),
                        },
                        RuntimeFactPredicateWire::Text {
                            fact: ActualRuntimeFactWire::ToolVersions,
                            allowed_values: allowed_values.clone(),
                        },
                        RuntimeFactPredicateWire::Text {
                            fact: ActualRuntimeFactWire::EnvironmentFingerprint,
                            allowed_values,
                        },
                    ],
                },
            ],
        }
    }

    fn max_shaped_history() -> Result<ReviewerQualificationRuleHistoryResponse, Box<dyn Error>> {
        let scope = scope();
        let rule_id = Uuid::from_u128(601);
        let policy = max_shaped_policy();
        let change = ReviewerQualificationRuleChangeWire::Publish {
            policy: policy.clone(),
        };
        let operations_change = change.operations_change()?;
        let OperationsRuleChange::Publish {
            policy: operations_policy,
        } = &operations_change
        else {
            return Err("expected publish fixture".into());
        };
        assert!(serde_json::to_vec(operations_policy)?.len() <= 16_384);
        assert!(serde_json::to_vec(&operations_change)?.len() <= 16_384);

        let entries = (1_u64..=64)
            .map(|generation| -> Result<_, Box<dyn Error>> {
                Ok(
                    crate::wire::reviewer_qualification_rule::ReviewerQualificationRuleGenerationWire {
                    workspace_id: scope.workspace_id,
                    rule_id,
                    generation,
                    expected_current_generation: (generation > 1).then_some(generation - 1),
                    request_id: Uuid::from_u128(700 + u128::from(generation)),
                    original_direct_token_id: Uuid::from_u128(602),
                    original_session_id: Uuid::from_u128(604),
                    state: ReviewerQualificationRuleStateWire::ActiveForFuturePools,
                    policy: policy.clone(),
                    change: change.clone(),
                    created_at_ms: i64::try_from(generation)?,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ReviewerQualificationRuleHistoryResponse {
            scope,
            session_id: Uuid::from_u128(603),
            rule_id,
            after_generation: None,
            limit: 64,
            entries,
            next_after_generation: None,
            has_more: false,
        })
    }

    #[tokio::test]
    async fn max_shaped_history_uses_route_specific_limit_and_keeps_correlation()
    -> Result<(), Box<dyn Error>> {
        let response = max_shaped_history()?;
        let request = ReviewerQualificationRuleHistoryRequest {
            scope: response.scope.clone(),
            session_id: response.session_id,
            rule_id: response.rule_id,
            after_generation: None,
            limit: 64,
        };
        let body = serde_json::to_vec(&response)?;
        assert!(body.len() > crate::client::MAX_RESPONSE_BYTES);
        assert!(body.len() <= MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_RESPONSE_BYTES);
        assert!(valid_history_response(&response, &request));

        let mut misordered = response.clone();
        misordered.entries.swap(0, 1);
        assert!(!valid_history_response(&misordered, &request));
        let mut foreign_scope = response.clone();
        foreign_scope.scope.workspace_id = Uuid::from_u128(604);
        assert!(!valid_history_response(&foreign_scope, &request));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(
            listener.local_addr()?,
            std::time::Duration::from_secs(3),
        )?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request_bytes = [0_u8; 4096];
            let _read = socket.read(&mut request_bytes).await?;
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await?;
            socket.write_all(&body).await?;
            Ok::<_, std::io::Error>(())
        });
        let actual = client
            .reviewer_qualification_rule_history(
                &HeaderValue::from_static("Bearer current-operator"),
                &request,
            )
            .await?;
        assert_eq!(actual, response);
        assert_eq!(actual.entries.len(), 64);
        assert_eq!(
            actual.entries.first().map(|entry| entry.generation),
            Some(1)
        );
        assert_eq!(
            actual.entries.last().map(|entry| entry.generation),
            Some(64)
        );
        server.await??;
        Ok(())
    }

    #[test]
    fn current_query_requires_operator_session_selector_and_exact_scope() {
        let mut query = ReviewerQualificationRuleQueryRequest {
            scope: scope(),
            session_id: Uuid::from_u128(3),
            rule_id: Uuid::from_u128(4),
        };
        assert!(valid_scope(&query.scope, query.session_id));
        query.session_id = Uuid::nil();
        assert!(!valid_scope(&query.scope, query.session_id));
    }
}
