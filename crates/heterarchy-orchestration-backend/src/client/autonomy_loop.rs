//! Bounded typed HTTP client for Orchestration autonomy-loop operations.
//!
//! Each call presents its caller-supplied bearer credential, enforces strict
//! request validation and response correlation, and never retries automatically.

use std::collections::HashSet;

use heterarchy_orchestration_operations::{
    MAX_REVIEWER_CANDIDATE_POOL, MAX_WORKSPACE_AUTHORITY_SCOPES,
};
use hyper::header::HeaderValue;
use hyper::{Method, StatusCode};
use uuid::Uuid;

use crate::wire::WorkspaceQueryScope;
use crate::wire::autonomy_loop::{
    AssignReviewerRequest, DagSettlementAuthorityCheckRequest, DagSettlementAuthorityResponse,
    DagSettlementGovernanceQuery, DagSettlementGovernanceResponse,
    ExecutionSnapshotOutcomeResponse, ExecutionSnapshotQuery, ExecutionSnapshotQueryResponse,
    LaunchLedgerRecordWire, McpGrantWire, PrepareExecutionSnapshotRequest,
    RecordReviewVerdictRequest, RegisteredWorkspaceScopeResponse, ReviewerAssignmentWire,
    ReviewerCandidateWire, ReviewerOperationResponse, ScopeRegisterRequest, ScopeUnregisterRequest,
    VerifyNodeCompletionGateRequest, WorkCorrelationWire, WorkspaceScopeMutationRequest,
    WorkspaceScopeQueryResponse, parse_review_verdict_outcome, parse_sha256_hex,
};

use super::{
    ClientError, OrchestrationHttpClient, ReadbackTarget, canonical_identity, require_identity,
};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_SQLITE_REVISION: u64 = i64::MAX as u64;

impl OrchestrationHttpClient {
    /// Read the active registered `Workspace` project scope for `workspace_id`, if present.
    ///
    /// # Errors
    /// Rejects nil `workspace_id`, bounded transport failures, typed service rejections,
    /// or uncorrelated response payloads.
    pub async fn get_workspace_scope(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
    ) -> Result<Option<RegisteredWorkspaceScopeResponse>, ClientError> {
        require_identity(workspace_id)?;
        let response: WorkspaceScopeQueryResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/workspaces/{workspace_id}/scopes"),
                None,
                None,
            )
            .await?;
        if response.workspace_id != workspace_id
            || response
                .scope
                .as_ref()
                .is_some_and(|scope| !valid_active_scope_record(scope, workspace_id))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response.scope)
    }

    /// List all currently active registered `Workspace` project scopes.
    ///
    /// # Errors
    /// Returns bounded transport failures, typed service rejections, or protocol
    /// errors when response bounds or identities are violated.
    pub async fn list_workspace_scopes(
        &self,
        authorization: &HeaderValue,
    ) -> Result<Vec<RegisteredWorkspaceScopeResponse>, ClientError> {
        let scopes: Vec<RegisteredWorkspaceScopeResponse> = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                "/internal/v1/workspaces/scopes".to_owned(),
                None,
                None,
            )
            .await?;
        if scopes.len() > MAX_WORKSPACE_AUTHORITY_SCOPES {
            return Err(ClientError::Protocol);
        }
        let mut seen = HashSet::with_capacity(scopes.len());
        if !scopes.iter().all(|scope| {
            valid_active_scope_record(scope, scope.workspace_id) && seen.insert(scope.workspace_id)
        }) {
            return Err(ClientError::Protocol);
        }
        Ok(scopes)
    }

    /// Register, advance, or unregister a `Workspace` project scope without automatic retry.
    ///
    /// # Errors
    /// Rejects malformed identities/revisions, bounded transport failures, typed
    /// service rejections, or an uncertain outcome carrying the mutation readback target.
    pub async fn mutate_workspace_scope(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceScopeMutationRequest,
    ) -> Result<RegisteredWorkspaceScopeResponse, ClientError> {
        match request {
            WorkspaceScopeMutationRequest::Register {
                request_id,
                expected_revision,
                scope,
            } => {
                self.register_workspace_scope_call(
                    authorization,
                    request_id,
                    *expected_revision,
                    scope,
                )
                .await
            }
            WorkspaceScopeMutationRequest::Unregister {
                workspace_id,
                request_id,
                expected_revision,
            } => {
                self.unregister_workspace_scope_call(
                    authorization,
                    workspace_id,
                    request_id,
                    *expected_revision,
                )
                .await
            }
        }
    }

    async fn register_workspace_scope_call(
        &self,
        authorization: &HeaderValue,
        request_id: &str,
        expected_revision: u64,
        scope: &WorkspaceQueryScope,
    ) -> Result<RegisteredWorkspaceScopeResponse, ClientError> {
        let nonce = canonical_identity(request_id)?;
        validate_query_scope(scope)?;
        if expected_revision >= MAX_SQLITE_REVISION {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::WorkspaceScopeMutation {
            workspace_id: scope.workspace_id,
            request_id: nonce,
            expected_revision,
        };
        let wire_req = ScopeRegisterRequest {
            request_id: request_id.to_owned(),
            expected_revision,
            scope: scope.clone(),
        };
        let response: RegisteredWorkspaceScopeResponse = self
            .call(
                authorization,
                Method::POST,
                "/internal/v1/workspaces/scopes".to_owned(),
                Some(&wire_req),
                Some(readback.clone()),
            )
            .await?;
        if !valid_active_scope_record(&response, scope.workspace_id)
            || response.location_id != scope.location_id
            || response.location_revision != scope.location_revision
            || response.workspace_revision != scope.workspace_revision
            || response.locator != scope.locator
            || response.revision <= expected_revision
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    async fn unregister_workspace_scope_call(
        &self,
        authorization: &HeaderValue,
        workspace_id_raw: &str,
        request_id: &str,
        expected_revision: u64,
    ) -> Result<RegisteredWorkspaceScopeResponse, ClientError> {
        let workspace_id = canonical_identity(workspace_id_raw)?;
        let nonce = canonical_identity(request_id)?;
        if expected_revision == 0 || expected_revision >= MAX_SQLITE_REVISION {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::WorkspaceScopeMutation {
            workspace_id,
            request_id: nonce,
            expected_revision,
        };
        let wire_req = ScopeUnregisterRequest {
            request_id: request_id.to_owned(),
            expected_revision,
        };
        let response: RegisteredWorkspaceScopeResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/workspaces/{workspace_id}/scopes/unregister"),
                Some(&wire_req),
                Some(readback.clone()),
            )
            .await?;
        if response.workspace_id != workspace_id
            || response.active
            || response.revision <= expected_revision
            || response.generation == 0
            || response.updated_at_ms < 0
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Deterministically select and bind an independent reviewer for one node attempt.
    ///
    /// Structured domain rejections (`409 Conflict` carrying `admitted: false`) are
    /// returned in `Ok(ReviewerOperationResponse)`.
    ///
    /// # Errors
    /// Rejects malformed identities/candidates, bounded transport failures, typed
    /// service rejections, or uncertain mutation outcomes.
    pub async fn assign_reviewer(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        request: &AssignReviewerRequest,
    ) -> Result<ReviewerOperationResponse, ClientError> {
        require_identity(workspace_id)?;
        let nonce = validate_assign_reviewer_request(workspace_id, request)?;
        let readback = ReadbackTarget::ReviewerAssignment {
            workspace_id,
            request_id: nonce,
            correlation: Box::new(request.correlation.clone()),
        };
        let (status, response): (StatusCode, ReviewerOperationResponse) = self
            .call_with_conflict_outcome(
                authorization,
                Method::POST,
                format!("/internal/v1/workspaces/{workspace_id}/reviewer-assignments"),
                Some(request),
                Some(readback.clone()),
                valid_rejected_reviewer_response,
            )
            .await?;
        if status == StatusCode::OK
            && !valid_admitted_reviewer_response(&response, workspace_id, &request.correlation)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Record a diff-digest-bound reviewer verdict using compare-and-swap on `expected_revision`.
    ///
    /// # Errors
    /// Rejects malformed identities/digests, bounded transport failures, typed
    /// service rejections, or uncertain mutation outcomes.
    pub async fn record_reviewer_verdict(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        request: &RecordReviewVerdictRequest,
    ) -> Result<ReviewerOperationResponse, ClientError> {
        require_identity(workspace_id)?;
        let nonce = validate_record_verdict_request(workspace_id, request)?;
        let readback = ReadbackTarget::ReviewerVerdict {
            workspace_id,
            request_id: nonce,
            correlation: Box::new(request.correlation.clone()),
            expected_revision: request.expected_revision,
        };
        let (status, response): (StatusCode, ReviewerOperationResponse) = self
            .call_with_conflict_outcome(
                authorization,
                Method::POST,
                format!("/internal/v1/workspaces/{workspace_id}/reviewer-verdicts"),
                Some(request),
                Some(readback.clone()),
                valid_rejected_reviewer_response,
            )
            .await?;
        if status == StatusCode::OK
            && (!valid_admitted_reviewer_response(&response, workspace_id, &request.correlation)
                || response
                    .assignment
                    .as_ref()
                    .is_none_or(|assignment| assignment.verdict.is_none()))
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Verify the fail-closed review gate for `correlation` against `current_handoff_diff_digest_hex`.
    ///
    /// This is a pure read-only gate check and carries no mutation readback target.
    ///
    /// # Errors
    /// Rejects malformed correlation/digest inputs, bounded transport failures,
    /// typed service rejections, or malformed protocol responses.
    pub async fn verify_reviewer_gate(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        request: &VerifyNodeCompletionGateRequest,
    ) -> Result<ReviewerOperationResponse, ClientError> {
        require_identity(workspace_id)?;
        request
            .correlation
            .to_correlation(workspace_id)
            .map_err(|_| ClientError::InvalidRequest)?;
        parse_sha256_hex(&request.current_handoff_diff_digest_hex)
            .map_err(|_| ClientError::InvalidRequest)?;
        let (status, response): (StatusCode, ReviewerOperationResponse) = self
            .call_with_conflict_outcome(
                authorization,
                Method::POST,
                format!("/internal/v1/workspaces/{workspace_id}/node-completion-gate"),
                Some(request),
                None,
                valid_rejected_reviewer_response,
            )
            .await?;
        if status == StatusCode::OK
            && !valid_admitted_reviewer_response(&response, workspace_id, &request.correlation)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Validate and commit an immutable `AssignmentExecutionSnapshot` and `Prepared` Launch Ledger row.
    ///
    /// Structured validation rejections (`409 Conflict` with `committed: false`) are
    /// returned in `Ok(ExecutionSnapshotOutcomeResponse)`.
    ///
    /// # Errors
    /// Rejects malformed identities/bounds, bounded transport failures, typed
    /// service rejections, or uncertain mutation outcomes.
    pub async fn prepare_execution_snapshot(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        request: &PrepareExecutionSnapshotRequest,
    ) -> Result<ExecutionSnapshotOutcomeResponse, ClientError> {
        require_identity(workspace_id)?;
        let (snapshot_id, correlation) = validate_prepare_snapshot_request(workspace_id, request)?;
        let readback = ReadbackTarget::ExecutionSnapshot {
            workspace_id,
            snapshot_id,
            correlation: Box::new(correlation.clone()),
        };
        let (status, response): (StatusCode, ExecutionSnapshotOutcomeResponse) = self
            .call_with_conflict_outcome(
                authorization,
                Method::POST,
                format!("/internal/v1/workspaces/{workspace_id}/execution-snapshots"),
                Some(request),
                Some(readback.clone()),
                valid_rejected_snapshot_response,
            )
            .await?;
        if status == StatusCode::OK
            && !valid_committed_snapshot_response(
                &response,
                workspace_id,
                snapshot_id,
                &correlation,
            )
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the durable Launch Ledger record for an exact node-assignment attempt, if prepared.
    ///
    /// # Errors
    /// Rejects malformed attempt coordinates, bounded transport failures, typed
    /// service rejections, or uncorrelated response payloads.
    pub async fn get_execution_snapshot(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        query: &ExecutionSnapshotQuery,
    ) -> Result<Option<LaunchLedgerRecordWire>, ClientError> {
        require_identity(workspace_id)?;
        let attempt_key = query
            .to_attempt_key(workspace_id)
            .map_err(|_| ClientError::InvalidRequest)?;
        let goal_id = attempt_key.goal_id().into_uuid();
        let dag_revision = attempt_key.dag_revision().get();
        let node_id = attempt_key.node_id().into_uuid();
        let assignment_id = attempt_key.assignment_id().into_uuid();
        let attempt = attempt_key.attempt();
        let path = format!(
            "/internal/v1/workspaces/{workspace_id}/execution-snapshots?goalId={goal_id}&dagRevision={dag_revision}&nodeId={node_id}&assignmentId={assignment_id}&attempt={attempt}"
        );
        let response: ExecutionSnapshotQueryResponse = self
            .call::<_, ()>(authorization, Method::GET, path, None, None)
            .await?;
        if response.workspace_id != workspace_id
            || response.goal_id != goal_id
            || response.dag_revision != dag_revision
            || response.node_id != node_id
            || response.assignment_id != assignment_id
            || response.attempt != attempt
            || response.record.as_ref().is_some_and(|record| {
                !valid_ledger_record(
                    record,
                    workspace_id,
                    goal_id,
                    dag_revision,
                    node_id,
                    assignment_id,
                    attempt,
                )
            })
        {
            return Err(ClientError::Protocol);
        }
        Ok(response.record)
    }

    /// Verify `Workspace Orchestrator` leadership revision fence before `dag.settle`.
    ///
    /// # Errors
    /// Rejects negative revisions or malformed participant coordinates, bounded
    /// transport failures, typed service rejections, or uncorrelated responses.
    pub async fn check_dag_settlement_authority(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        request: &DagSettlementAuthorityCheckRequest,
    ) -> Result<DagSettlementAuthorityResponse, ClientError> {
        require_identity(workspace_id)?;
        if request.expected_leadership_revision < 0 {
            return Err(ClientError::InvalidRequest);
        }
        if let Some(participant) = &request.participant {
            participant
                .to_actor()
                .map_err(|_| ClientError::InvalidRequest)?;
        }
        let response: DagSettlementAuthorityResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/workspaces/{workspace_id}/dag-settlement-authority"),
                Some(request),
                None,
            )
            .await?;
        if response.workspace_id != workspace_id
            || response.leadership_revision != request.expected_leadership_revision
            || response
                .leader_participant_id
                .is_some_and(|leader| leader.is_nil())
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read the durable governance link projected atomically from a `DagSettled` effect fact.
    ///
    /// # Errors
    /// Rejects non-canonical `goal_id` or non-positive `dag_revision`, bounded
    /// transport failures, typed service rejections (`404` when absent), or
    /// uncorrelated protocol responses.
    pub async fn get_dag_settlement_governance(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        query: &DagSettlementGovernanceQuery,
    ) -> Result<DagSettlementGovernanceResponse, ClientError> {
        require_identity(workspace_id)?;
        let goal_id = canonical_identity(&query.goal_id)?;
        if query.dag_revision < 1 {
            return Err(ClientError::InvalidRequest);
        }
        let dag_revision = query.dag_revision;
        let path = format!(
            "/internal/v1/workspaces/{workspace_id}/dag-settlement-governance?goalId={goal_id}&dagRevision={dag_revision}"
        );
        let response: DagSettlementGovernanceResponse = self
            .call::<_, ()>(authorization, Method::GET, path, None, None)
            .await?;
        if !valid_dag_settlement_governance(&response, workspace_id, goal_id, dag_revision) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_query_scope(scope: &WorkspaceQueryScope) -> Result<(), ClientError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_active_scope_record(
    record: &RegisteredWorkspaceScopeResponse,
    expected_workspace_id: Uuid,
) -> bool {
    record.workspace_id == expected_workspace_id
        && !record.workspace_id.is_nil()
        && !record.location_id.is_nil()
        && record.location_revision > 0
        && record.workspace_revision > 0
        && !record.locator.is_empty()
        && record.locator.len() <= MAX_LOCATOR_BYTES
        && record.active
        && record.revision > 0
        && record.generation > 0
        && record.updated_at_ms >= 0
}

fn validate_assign_reviewer_request(
    workspace_id: Uuid,
    request: &AssignReviewerRequest,
) -> Result<Uuid, ClientError> {
    let nonce = canonical_identity(&request.request_id)?;
    if request.candidates.len() > MAX_REVIEWER_CANDIDATE_POOL
        || request.required_domain.trim().is_empty()
    {
        return Err(ClientError::InvalidRequest);
    }
    request
        .correlation
        .to_correlation(workspace_id)
        .map_err(|_| ClientError::InvalidRequest)?;
    request
        .executor
        .to_author_identity()
        .map_err(|_| ClientError::InvalidRequest)?;
    request
        .assigning_leader
        .to_author_identity()
        .map_err(|_| ClientError::InvalidRequest)?;
    for candidate in &request.candidates {
        ReviewerCandidateWire::to_candidate_identity(candidate)
            .map_err(|_| ClientError::InvalidRequest)?;
    }
    Ok(nonce)
}

fn validate_record_verdict_request(
    workspace_id: Uuid,
    request: &RecordReviewVerdictRequest,
) -> Result<Uuid, ClientError> {
    let nonce = canonical_identity(&request.request_id)?;
    if request.expected_revision == 0 || i64::try_from(request.expected_revision).is_err() {
        return Err(ClientError::InvalidRequest);
    }
    let _reviewer_agent = canonical_identity(&request.reviewer_agent_id)?;
    let _reviewer_session = canonical_identity(&request.reviewer_session_id)?;
    if let Some(reviewer) = &request.reviewer {
        reviewer
            .to_author_identity()
            .map_err(|_| ClientError::InvalidRequest)?;
    }
    request
        .correlation
        .to_correlation(workspace_id)
        .map_err(|_| ClientError::InvalidRequest)?;
    parse_review_verdict_outcome(&request.verdict).map_err(|_| ClientError::InvalidRequest)?;
    let digest = parse_sha256_hex(&request.handoff_diff_digest_hex)
        .map_err(|_| ClientError::InvalidRequest)?;
    if digest == [0_u8; 32] {
        return Err(ClientError::InvalidRequest);
    }
    Ok(nonce)
}

fn valid_rejected_reviewer_response(response: &ReviewerOperationResponse) -> bool {
    !response.admitted
        && response
            .rejection_code
            .as_deref()
            .is_some_and(|code| !code.is_empty())
        && response.assignment.is_none()
}

fn valid_admitted_reviewer_response(
    response: &ReviewerOperationResponse,
    workspace_id: Uuid,
    correlation: &WorkCorrelationWire,
) -> bool {
    response.admitted
        && response.rejection_code.is_none()
        && response.assignment.as_ref().is_some_and(|assignment| {
            valid_reviewer_assignment(assignment, workspace_id, correlation)
        })
}

fn valid_reviewer_assignment(
    assignment: &ReviewerAssignmentWire,
    workspace_id: Uuid,
    correlation: &WorkCorrelationWire,
) -> bool {
    let Ok(goal_id) = canonical_identity(&correlation.goal_id) else {
        return false;
    };
    let Ok(node_id) = canonical_identity(&correlation.node_id) else {
        return false;
    };
    let Ok(assignment_id) = canonical_identity(&correlation.assignment_id) else {
        return false;
    };
    !assignment.assignment_record_id.is_nil()
        && assignment.workspace_id == workspace_id
        && assignment.goal_id == goal_id
        && assignment.dag_revision == correlation.dag_revision
        && assignment.node_id == node_id
        && assignment.assignment_id == assignment_id
        && assignment.attempt == correlation.attempt
        && assignment.revision > 0
}

fn validate_prepare_snapshot_request(
    workspace_id: Uuid,
    request: &PrepareExecutionSnapshotRequest,
) -> Result<(Uuid, WorkCorrelationWire), ClientError> {
    let _nonce = canonical_identity(&request.request_id)?;
    let snapshot_id = canonical_identity(&request.snapshot_id)?;
    let _activity_id = canonical_identity(&request.activity_id)?;
    let _profile_id = canonical_identity(&request.profile_id)?;
    let _lease_id = canonical_identity(&request.lease_id)?;
    if request.profile_revision <= 0
        || request.lease_revision == 0
        || request.lease_expires_at_ms <= 0
        || request.resolved_engine.trim().is_empty()
        || request.resolved_model.trim().is_empty()
        || request.worktree_path.trim().is_empty()
    {
        return Err(ClientError::InvalidRequest);
    }
    if let Some(grant) = &request.mcp_grant {
        validate_mcp_grant(grant)?;
    }
    let correlation = WorkCorrelationWire {
        goal_id: request.goal_id.clone(),
        dag_revision: request.dag_revision,
        node_id: request.node_id.clone(),
        assignment_id: request.assignment_id.clone(),
        attempt: request.attempt,
    };
    correlation
        .to_correlation(workspace_id)
        .map_err(|_| ClientError::InvalidRequest)?;
    Ok((snapshot_id, correlation))
}

fn validate_mcp_grant(grant: &McpGrantWire) -> Result<(), ClientError> {
    let _token_id = canonical_identity(&grant.token_id)?;
    if grant.issued_at_ms < 0 || grant.expires_at_ms <= grant.issued_at_ms {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_rejected_snapshot_response(response: &ExecutionSnapshotOutcomeResponse) -> bool {
    !response.committed
        && response
            .rejection_code
            .as_deref()
            .is_some_and(|code| !code.is_empty())
        && response.record.is_none()
}

fn valid_committed_snapshot_response(
    response: &ExecutionSnapshotOutcomeResponse,
    workspace_id: Uuid,
    snapshot_id: Uuid,
    correlation: &WorkCorrelationWire,
) -> bool {
    let Ok(goal_id) = canonical_identity(&correlation.goal_id) else {
        return false;
    };
    let Ok(node_id) = canonical_identity(&correlation.node_id) else {
        return false;
    };
    let Ok(assignment_id) = canonical_identity(&correlation.assignment_id) else {
        return false;
    };
    response.committed
        && response.rejection_code.is_none()
        && response.record.as_ref().is_some_and(|record| {
            record.snapshot_id == snapshot_id
                && valid_ledger_record(
                    record,
                    workspace_id,
                    goal_id,
                    correlation.dag_revision,
                    node_id,
                    assignment_id,
                    correlation.attempt,
                )
        })
}

fn valid_ledger_record(
    record: &LaunchLedgerRecordWire,
    workspace_id: Uuid,
    goal_id: Uuid,
    dag_revision: i64,
    node_id: Uuid,
    assignment_id: Uuid,
    attempt: u32,
) -> bool {
    !record.snapshot_id.is_nil()
        && record.workspace_id == workspace_id
        && record.goal_id == goal_id
        && record.dag_revision == dag_revision
        && record.node_id == node_id
        && record.assignment_id == assignment_id
        && record.attempt == attempt
        && record.revision > 0
        && !record.state.status.is_empty()
}

fn valid_dag_settlement_governance(
    response: &DagSettlementGovernanceResponse,
    workspace_id: Uuid,
    goal_id: Uuid,
    dag_revision: i64,
) -> bool {
    response.workspace_id == workspace_id
        && response.goal_id == goal_id
        && response.dag_revision == dag_revision
        && !response.migration_id.is_nil()
        && response.source_sequence > 0
        && !response.settlement_id.is_nil()
        && !response.coordinator_agent_id.is_nil()
        && response
            .coordinator_participant_id
            .is_none_or(|id| !id.is_nil())
        && response
            .started_mission_plan_id
            .is_none_or(|id| !id.is_nil())
        && response.settled_at_ms >= 0
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use hyper::header::HeaderValue;

    use super::*;

    #[tokio::test]
    async fn scope_mutation_validates_inputs_and_preserves_readback_on_transport_loss()
    -> Result<(), Box<dyn Error>> {
        let client =
            OrchestrationHttpClient::new("127.0.0.1:1".parse()?, Duration::from_millis(50))?;
        let auth = HeaderValue::from_static("Bearer test-operator");
        let bad = WorkspaceScopeMutationRequest::Unregister {
            workspace_id: Uuid::from_u128(1).to_string(),
            request_id: Uuid::from_u128(2).to_string(),
            expected_revision: 0,
        };
        assert_eq!(
            client.mutate_workspace_scope(&auth, &bad).await,
            Err(ClientError::InvalidRequest)
        );

        let ws = Uuid::from_u128(0x10);
        let req_id = Uuid::from_u128(0x20);
        let valid = WorkspaceScopeMutationRequest::Register {
            request_id: req_id.to_string(),
            expected_revision: 0,
            scope: WorkspaceQueryScope {
                workspace_id: ws,
                location_id: Uuid::from_u128(0x30),
                location_revision: 1,
                workspace_revision: 1,
                locator: "hf1:test".to_owned(),
            },
        };
        assert_eq!(
            client.mutate_workspace_scope(&auth, &valid).await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::WorkspaceScopeMutation {
                    workspace_id: ws,
                    request_id: req_id,
                    expected_revision: 0,
                }
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn dag_settlement_client_methods_reject_invalid_inputs_before_network()
    -> Result<(), Box<dyn Error>> {
        let client =
            OrchestrationHttpClient::new("127.0.0.1:1".parse()?, Duration::from_millis(50))?;
        let auth = HeaderValue::from_static("Bearer test-operator");
        let ws = Uuid::from_u128(0x55);

        assert_eq!(
            client
                .check_dag_settlement_authority(
                    &auth,
                    ws,
                    &DagSettlementAuthorityCheckRequest {
                        expected_leadership_revision: -1,
                        participant: None,
                    },
                )
                .await,
            Err(ClientError::InvalidRequest)
        );

        assert_eq!(
            client
                .get_dag_settlement_governance(
                    &auth,
                    ws,
                    &DagSettlementGovernanceQuery {
                        goal_id: Uuid::from_u128(0x56).to_string(),
                        dag_revision: 0,
                    },
                )
                .await,
            Err(ClientError::InvalidRequest)
        );
        Ok(())
    }
}
