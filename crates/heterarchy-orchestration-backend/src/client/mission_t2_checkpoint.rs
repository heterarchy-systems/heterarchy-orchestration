//! Bounded client methods for T2 defer submission and exact request readback.
//!
//! An uncertain mutation never triggers an automatic retry. The typed error
//! retains the original caller intent for `mission_t2_defer_readback`.

use hyper::Method;
use hyper::header::HeaderValue;
use thiserror::Error;
use uuid::Uuid;

use super::{ClientError, OrchestrationHttpClient, ServiceCode};
use crate::wire::mission_t2_checkpoint as wire;

/// Client outcome classification for one T2 defer request.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MissionT2DeferClientError {
    #[error("Mission T2 defer was rejected before a successful receipt")]
    Rejected(#[source] ClientError),
    #[error("Mission T2 defer outcome is unknown; read its original request ID")]
    UnknownOutcome {
        request: Box<wire::MissionT2DeferRequest>,
        #[source]
        source: ClientError,
    },
}

impl MissionT2DeferClientError {
    /// Build a pure readback request while retaining the exact original intent.
    ///
    /// A missing receipt is only an observation and does not authorize retry.
    #[must_use]
    pub fn readback_request(
        &self,
        reader: wire::MissionT2CheckpointReaderWire,
    ) -> Option<wire::MissionT2DeferReadbackRequest> {
        match self {
            Self::UnknownOutcome { request, .. } => Some(wire::MissionT2DeferReadbackRequest {
                intent: wire::MissionT2DeferContentWire {
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
                },
                reader,
            }),
            Self::Rejected(_) => None,
        }
    }
}

impl OrchestrationHttpClient {
    /// Submit one explicit T2 defer intent without automatically retrying it.
    ///
    /// `UnknownOutcome` retains the original IDs, reason, follow-up and scope.
    /// Read back with the current authenticated Workspace actor before taking
    /// any later action.
    ///
    /// # Errors
    /// Returns an explicit rejection or an uncertain outcome with the original
    /// request retained for pure readback.
    pub async fn mission_t2_defer(
        &self,
        authorization: &HeaderValue,
        request: &wire::MissionT2DeferRequest,
    ) -> Result<wire::MissionT2DeferResponse, MissionT2DeferClientError> {
        if !wire::valid_defer_request(request) {
            return Err(MissionT2DeferClientError::Rejected(
                ClientError::InvalidRequest,
            ));
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                defer_path(
                    request.context.scope.workspace_id,
                    request.mission_id,
                    request.plan_id,
                ),
                Some(request),
                None,
            )
            .await
            .map_err(|error| classify_mutation_error(request, error))?;
        if wire::valid_defer_response(&response, request) {
            Ok(response)
        } else {
            Err(MissionT2DeferClientError::UnknownOutcome {
                request: Box::new(request.clone()),
                source: ClientError::Protocol,
            })
        }
    }

    /// Read one exact T2 defer request without retrying or sampling a clock.
    ///
    /// # Errors
    /// Rejects malformed requests, bounded transport errors, or a foreign or
    /// malformed receipt projection.
    pub async fn mission_t2_defer_readback(
        &self,
        authorization: &HeaderValue,
        request: &wire::MissionT2DeferReadbackRequest,
    ) -> Result<wire::MissionT2DeferReadbackResponse, ClientError> {
        if !wire::valid_readback_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                readback_path(
                    request.reader.scope.workspace_id,
                    request.intent.mission_id,
                    request.intent.plan_id,
                ),
                Some(request),
                None,
            )
            .await?;
        if wire::valid_readback_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }
}

fn classify_mutation_error(
    request: &wire::MissionT2DeferRequest,
    error: ClientError,
) -> MissionT2DeferClientError {
    let outcome_is_uncertain = match &error {
        ClientError::Unavailable
        | ClientError::Protocol
        | ClientError::UnknownOutcome { .. }
        | ClientError::ReadbackRequired { .. } => true,
        ClientError::Rejected { status, code } => *status >= 500 || *code == ServiceCode::Unknown,
        ClientError::Configuration | ClientError::InvalidRequest | ClientError::Capacity => false,
    };
    if outcome_is_uncertain {
        MissionT2DeferClientError::UnknownOutcome {
            request: Box::new(request.clone()),
            source: error,
        }
    } else {
        MissionT2DeferClientError::Rejected(error)
    }
}

fn defer_path(workspace_id: Uuid, mission_id: Uuid, plan_id: Uuid) -> String {
    format!(
        "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/t2/defer"
    )
}

fn readback_path(workspace_id: Uuid, mission_id: Uuid, plan_id: Uuid) -> String {
    format!(
        "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/t2/readback"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        WorkspaceQueryScope, mission_evaluation::MissionEvaluationMutationContextWire,
    };

    fn request() -> wire::MissionT2DeferRequest {
        wire::MissionT2DeferRequest {
            context: MissionEvaluationMutationContextWire {
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
            },
            request_id: Uuid::from_u128(8),
            checkpoint_id: Uuid::from_u128(9),
            mission_id: Uuid::from_u128(10),
            plan_id: Uuid::from_u128(11),
            expected_revision: 12,
            expected_previous_checkpoint_id: None,
            reason: "review pending".to_owned(),
            next_check_condition: "read review receipt".to_owned(),
            next_check_at_ms: 100,
        }
    }

    #[test]
    fn unknown_transport_retains_exact_original_request_for_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let error = classify_mutation_error(&request, ClientError::Unavailable);
        assert!(matches!(
            &error,
            MissionT2DeferClientError::UnknownOutcome { .. }
        ));
        let readback = error
            .readback_request(wire::MissionT2CheckpointReaderWire {
                scope: request.context.scope.clone(),
                session_id: Uuid::from_u128(5),
                sender_activity_id: Uuid::from_u128(6),
                expected_workspace_leadership_revision: 7,
            })
            .ok_or("uncertain outcome lost its readback intent")?;
        assert_eq!(readback.intent.request_id, request.request_id);
        assert_eq!(readback.intent.reason, request.reason);
        Ok(())
    }

    #[test]
    fn explicit_conflict_is_not_reclassified_as_unknown() {
        let request = request();
        let error = classify_mutation_error(
            &request,
            ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict,
            },
        );
        assert!(matches!(&error, MissionT2DeferClientError::Rejected(_)));
        assert!(
            error
                .readback_request(wire::MissionT2CheckpointReaderWire {
                    scope: request.context.scope,
                    session_id: Uuid::from_u128(5),
                    sender_activity_id: Uuid::from_u128(6),
                    expected_workspace_leadership_revision: 7,
                })
                .is_none()
        );
    }
}
