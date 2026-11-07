//! Bounded HTTP client for Session-bound Reviewer participation history.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::reviewer_participation::{
    ReviewerParticipationHistoryRequest, ReviewerParticipationHistoryResponse,
    ReviewerParticipationMutationRequest, ReviewerParticipationMutationResponse,
    ReviewerParticipationReceiptWire, ReviewerParticipationRequestReadRequest,
    ReviewerParticipationRequestReadResponse, ReviewerParticipationStatementWire,
    ReviewerParticipationTargetWire,
};

impl OrchestrationHttpClient {
    /// Record interest or withdrawal through this exact Agent Session.
    ///
    /// The exact request is retained for read-only recovery. This never retries
    /// the mutation or resends a statement automatically.
    ///
    /// # Errors
    /// Rejects malformed selectors or responses, typed service failures,
    /// bounded transport failures, or an uncertain mutation outcome.
    pub async fn reviewer_participation_mutate(
        &self,
        authorization: &HeaderValue,
        request: &ReviewerParticipationMutationRequest,
    ) -> Result<ReviewerParticipationMutationResponse, ClientError> {
        if !valid_mutation_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::ReviewerParticipationMutation(Box::new(request.clone()));
        let response: ReviewerParticipationMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/reviewer-participation/statements",
                    request.target.scope.workspace_id
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

    /// Read one exact immutable statement under this current Agent Session.
    ///
    /// # Errors
    /// Rejects malformed selectors, bounded transport failures, typed
    /// service errors, and a response with a foreign request identity.
    pub async fn reviewer_participation_request(
        &self,
        authorization: &HeaderValue,
        request: &ReviewerParticipationRequestReadRequest,
    ) -> Result<ReviewerParticipationRequestReadResponse, ClientError> {
        if !valid_target(&request.target) || request.request_id.is_nil() {
            return Err(ClientError::InvalidRequest);
        }
        let response: ReviewerParticipationRequestReadResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/reviewer-participation/request",
                    request.target.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_request_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Resolve an uncertain statement write with an exact read-only query.
    ///
    /// The current target may name a new Session. The server verifies that it
    /// resolves to the same stable Profile participation before exposing the
    /// receipt; the receipt still carries the original writer Session. This
    /// does not attest that one principal belongs to only one Agent.
    ///
    /// # Errors
    /// Rejects malformed identities, a Workspace change, transport failures,
    /// typed rejections, or a receipt that differs from the original request.
    pub async fn reviewer_participation_mutation_readback(
        &self,
        authorization: &HeaderValue,
        original: &ReviewerParticipationMutationRequest,
        current_target: &ReviewerParticipationTargetWire,
    ) -> Result<ReviewerParticipationRequestReadResponse, ClientError> {
        if !valid_mutation_request(original)
            || !valid_target(current_target)
            || original.target.scope.workspace_id != current_target.scope.workspace_id
        {
            return Err(ClientError::InvalidRequest);
        }
        let request = ReviewerParticipationRequestReadRequest {
            target: current_target.clone(),
            request_id: original.request_id,
        };
        let response = self
            .reviewer_participation_request(authorization, &request)
            .await?;
        if response
            .receipt
            .as_ref()
            .is_some_and(|receipt| !receipt_matches_mutation(receipt, original))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read a bounded ascending history page for the current participation.
    ///
    /// # Errors
    /// Rejects malformed selectors/cursors/limits, bounded transport failures,
    /// typed errors, and a foreign or unordered history page.
    pub async fn reviewer_participation_history(
        &self,
        authorization: &HeaderValue,
        request: &ReviewerParticipationHistoryRequest,
    ) -> Result<ReviewerParticipationHistoryResponse, ClientError> {
        if !valid_target(&request.target)
            || request.limit == 0
            || request.limit > 64
            || request.after_revision == Some(0)
        {
            return Err(ClientError::InvalidRequest);
        }
        let response: ReviewerParticipationHistoryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/reviewer-participation/history",
                    request.target.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_history_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }
}

fn valid_target(target: &ReviewerParticipationTargetWire) -> bool {
    let scope = &target.scope;
    !scope.workspace_id.is_nil()
        && !scope.location_id.is_nil()
        && scope.location_revision >= 1
        && scope.workspace_revision >= 1
        && !scope.locator.is_empty()
        && scope.locator.len() <= 4096
        && !scope.locator.chars().any(char::is_control)
        && !target.activity_id.is_nil()
        && !target.agent_id.is_nil()
        && !target.session_id.is_nil()
}

fn valid_mutation_request(request: &ReviewerParticipationMutationRequest) -> bool {
    valid_target(&request.target)
        && !request.request_id.is_nil()
        && request.expected_revision < i64::MAX as u64
        && match request.statement {
            ReviewerParticipationStatementWire::Interested => true,
            ReviewerParticipationStatementWire::WithdrawInterest {
                withdraws_request_id,
                withdraws_revision,
            } => {
                !withdraws_request_id.is_nil()
                    && withdraws_revision != 0
                    && withdraws_revision == request.expected_revision
            }
        }
}

fn valid_mutation_response(
    response: &ReviewerParticipationMutationResponse,
    request: &ReviewerParticipationMutationRequest,
) -> bool {
    response.target == request.target
        && receipt_matches_mutation(&response.receipt, request)
        && response.receipt.revision == request.expected_revision + 1
        && response.receipt.expected_revision == request.expected_revision
}

fn valid_request_response(
    response: &ReviewerParticipationRequestReadResponse,
    request: &ReviewerParticipationRequestReadRequest,
) -> bool {
    response.target == request.target
        && response.receipt.as_ref().is_none_or(|receipt| {
            receipt.workspace_id == request.target.scope.workspace_id
                && receipt.request_id == request.request_id
                && valid_receipt(receipt)
        })
}

fn valid_history_response(
    response: &ReviewerParticipationHistoryResponse,
    request: &ReviewerParticipationHistoryRequest,
) -> bool {
    let Ok(limit) = usize::try_from(request.limit) else {
        return false;
    };
    if response.target != request.target || response.records.len() > limit {
        return false;
    }
    let mut prior_revision = request.after_revision.unwrap_or(0);
    let mut identity: Option<(Uuid, Uuid)> = None;
    for receipt in &response.records {
        if !valid_receipt(receipt)
            || receipt.workspace_id != request.target.scope.workspace_id
            || receipt.revision <= prior_revision
        {
            return false;
        }
        let current_identity = (receipt.profile_id, receipt.participation_id);
        if identity.is_some_and(|previous| previous != current_identity) {
            return false;
        }
        identity = Some(current_identity);
        prior_revision = receipt.revision;
    }
    match response.next_after_revision {
        Some(cursor) => {
            request
                .after_revision
                .is_none_or(|previous| cursor > previous)
                && response
                    .records
                    .last()
                    .is_some_and(|last| cursor == last.revision)
        }
        None => true,
    }
}

fn receipt_matches_mutation(
    receipt: &ReviewerParticipationReceiptWire,
    request: &ReviewerParticipationMutationRequest,
) -> bool {
    valid_receipt(receipt)
        && receipt.workspace_id == request.target.scope.workspace_id
        && receipt.activity_id == request.target.activity_id
        && receipt.agent_id == request.target.agent_id
        && receipt.session_id == request.target.session_id
        && receipt.request_id == request.request_id
        && receipt.expected_revision == request.expected_revision
        && receipt.statement == request.statement
}

fn valid_receipt(receipt: &ReviewerParticipationReceiptWire) -> bool {
    !receipt.workspace_id.is_nil()
        && !receipt.participation_id.is_nil()
        && !receipt.profile_id.is_nil()
        && !receipt.participant_id.is_nil()
        && !receipt.request_id.is_nil()
        && receipt.expected_revision < i64::MAX as u64
        && receipt.revision == receipt.expected_revision + 1
        && match receipt.statement {
            ReviewerParticipationStatementWire::Interested => true,
            ReviewerParticipationStatementWire::WithdrawInterest {
                withdraws_request_id,
                withdraws_revision,
            } => {
                !withdraws_request_id.is_nil()
                    && withdraws_revision != 0
                    && withdraws_revision == receipt.expected_revision
            }
        }
        && !receipt.activity_id.is_nil()
        && !receipt.agent_id.is_nil()
        && !receipt.session_id.is_nil()
        && !receipt.display_name.trim().is_empty()
        && receipt.display_name.len() <= 128
        && receipt.recorded_at_ms >= 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::WorkspaceQueryScope;
    use crate::wire::reviewer_participation::ReviewerParticipationTargetWire;

    fn target() -> ReviewerParticipationTargetWire {
        ReviewerParticipationTargetWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "fixture".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            agent_id: Uuid::from_u128(6),
            session_id: Uuid::from_u128(7),
        }
    }

    fn history_request(after_revision: Option<u64>) -> ReviewerParticipationHistoryRequest {
        ReviewerParticipationHistoryRequest {
            target: target(),
            after_revision,
            limit: 4,
        }
    }

    #[test]
    fn empty_history_cannot_return_a_nonadvancing_cursor() {
        let request = history_request(Some(8));
        let response = ReviewerParticipationHistoryResponse {
            target: request.target.clone(),
            records: Vec::new(),
            next_after_revision: Some(8),
        };
        assert!(!valid_history_response(&response, &request));
    }

    #[test]
    fn next_history_cursor_must_match_a_returned_advanced_revision() {
        let request = history_request(Some(3));
        let receipt = ReviewerParticipationReceiptWire {
            workspace_id: request.target.scope.workspace_id,
            participation_id: Uuid::from_u128(10),
            profile_id: Uuid::from_u128(11),
            participant_id: Uuid::from_u128(12),
            request_id: Uuid::from_u128(13),
            revision: 4,
            expected_revision: 3,
            statement: ReviewerParticipationStatementWire::Interested,
            activity_id: request.target.activity_id,
            agent_id: request.target.agent_id,
            session_id: request.target.session_id,
            display_name: "participant".to_owned(),
            recorded_at_ms: 20,
            replayed: false,
        };
        let matching = ReviewerParticipationHistoryResponse {
            target: request.target.clone(),
            records: vec![receipt.clone()],
            next_after_revision: Some(4),
        };
        assert!(valid_history_response(&matching, &request));

        let skipped = ReviewerParticipationHistoryResponse {
            next_after_revision: Some(5),
            ..matching.clone()
        };
        assert!(!valid_history_response(&skipped, &request));

        let terminal = ReviewerParticipationHistoryResponse {
            next_after_revision: None,
            ..matching
        };
        assert!(valid_history_response(&terminal, &request));
    }

    #[test]
    fn withdrawal_receipt_rejects_missing_or_wrong_target_revision() {
        let request = ReviewerParticipationMutationRequest {
            target: target(),
            request_id: Uuid::from_u128(13),
            expected_revision: 1,
            statement: ReviewerParticipationStatementWire::WithdrawInterest {
                withdraws_request_id: Uuid::from_u128(14),
                withdraws_revision: 1,
            },
        };
        let mut receipt = ReviewerParticipationReceiptWire {
            workspace_id: request.target.scope.workspace_id,
            participation_id: Uuid::from_u128(10),
            profile_id: Uuid::from_u128(11),
            participant_id: Uuid::from_u128(12),
            request_id: request.request_id,
            revision: 2,
            expected_revision: 1,
            statement: request.statement,
            activity_id: request.target.activity_id,
            agent_id: request.target.agent_id,
            session_id: request.target.session_id,
            display_name: "participant".to_owned(),
            recorded_at_ms: 20,
            replayed: false,
        };
        assert!(valid_mutation_response(
            &ReviewerParticipationMutationResponse {
                target: request.target.clone(),
                receipt: receipt.clone(),
            },
            &request,
        ));

        receipt.statement = ReviewerParticipationStatementWire::WithdrawInterest {
            withdraws_request_id: Uuid::from_u128(14),
            withdraws_revision: 2,
        };
        assert!(!valid_mutation_response(
            &ReviewerParticipationMutationResponse {
                target: request.target.clone(),
                receipt,
            },
            &request,
        ));
        let malformed_history = ReviewerParticipationHistoryResponse {
            target: request.target.clone(),
            records: vec![receipt_from_withdrawal(&request, 2)],
            next_after_revision: None,
        };
        assert!(!valid_history_response(
            &malformed_history,
            &history_request(None),
        ));
    }

    fn receipt_from_withdrawal(
        request: &ReviewerParticipationMutationRequest,
        withdraws_revision: u64,
    ) -> ReviewerParticipationReceiptWire {
        ReviewerParticipationReceiptWire {
            workspace_id: request.target.scope.workspace_id,
            participation_id: Uuid::from_u128(10),
            profile_id: Uuid::from_u128(11),
            participant_id: Uuid::from_u128(12),
            request_id: request.request_id,
            revision: 2,
            expected_revision: 1,
            statement: ReviewerParticipationStatementWire::WithdrawInterest {
                withdraws_request_id: Uuid::from_u128(14),
                withdraws_revision,
            },
            activity_id: request.target.activity_id,
            agent_id: request.target.agent_id,
            session_id: request.target.session_id,
            display_name: "participant".to_owned(),
            recorded_at_ms: 20,
            replayed: false,
        }
    }
}
