//! Source-proven handoff command and immutable full-intent recovery query.

use crate::{
    ApiError, BackendState, RuntimeError,
    app_support::validate_workspace_reference,
    coordinator_authority::HandoffTargetSelector,
    departure::receipt_wire,
    forge_membership::MembershipSourceReference,
    membership::{source_effect_error, source_error},
    wire::{
        handoff::{HandoffQueryResponse, HandoffRequest, HandoffResponse},
        membership::MemberTarget,
    },
};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentInstanceRevision, AgentSessionId, AuthenticatedPrincipal,
};
use heterarchy_orchestration_runtime::departure::{DepartureTarget, handoff::HandoffTarget};

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/handoff",
            post(command),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/handoff/query",
            post(query),
        )
}

async fn command(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<HandoffRequest>,
) -> Result<Json<HandoffResponse>, ApiError> {
    let target = validate(&path, &body)?;
    let owner = state.clone();
    state
        .run_owned(async move { owner.handoff_owned(body, target, principal).await })
        .await
        .map(Json)
}

async fn query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<HandoffRequest>,
) -> Result<Json<HandoffQueryResponse>, ApiError> {
    let target = validate(&path, &body)?;
    let owner = state.clone();
    state
        .run_owned(async move {
            let reference = owner.handoff_reference(&body, principal).await?;
            let authority = owner
                .coordinator_authority
                .clone()
                .ok_or(ApiError::WorkspaceScopeUnavailable)?;
            let source = authority
                .departure_by_identity(
                    body.source.scope.clone(),
                    reference,
                    body.request_id,
                    AgentSessionId::from_uuid(body.source.session_id),
                    principal,
                    owner.member_source_slot().await?,
                )
                .await
                .map_err(source_error)?;
            let receipt = if let Some(source) = source {
                owner
                    .catalog_job(move |runtime| {
                        runtime.handoff_readback(target, principal, &source)
                    })
                    .await?
                    .ok_or(ApiError::SourceBindingUnconfirmed)
                    .map(Some)?
            } else {
                if owner
                    .catalog_job(move |runtime| runtime.handoff_journal_present(target, principal))
                    .await?
                {
                    return Err(ApiError::SourceBindingUnconfirmed);
                }
                None
            };
            let latest = owner.handoff_reference(&body, principal).await?;
            if latest != reference {
                return Err(ApiError::Runtime(RuntimeError::Conflict));
            }
            Ok(Json(HandoffQueryResponse {
                request: body,
                receipt: receipt.as_ref().map(receipt_wire),
            }))
        })
        .await
}

impl BackendState {
    async fn handoff_reference(
        &self,
        body: &HandoffRequest,
        principal: AuthenticatedPrincipal,
    ) -> Result<MembershipSourceReference, ApiError> {
        let ws = body.source.scope.workspace_id;
        let session = body.source.session_id;
        let context = self
            .catalog_job(move |runtime| {
                runtime.membership_audit_source_context(ws, session, principal)
            })
            .await?;
        self.workspace_scope_proof(body.source.scope.clone())
            .await?;
        MembershipSourceReference::new(
            context.issuer.issuer().into_uuid(),
            context.import.destination_id(),
            context.import.migration_id(),
            context.import.source_revision(),
            context.import.digest(),
        )
        .map_err(|_| ApiError::WorkspaceScopeUnavailable)
    }

    async fn handoff_source_effect(
        &self,
        body: &HandoffRequest,
        target: HandoffTarget,
        principal: AuthenticatedPrincipal,
        reference: MembershipSourceReference,
        authority: &crate::CoordinatorAuthority,
        observed_at: i64,
    ) -> Result<heterarchy_orchestration_operations::DepartureObservation, ApiError> {
        let selector = HandoffTargetSelector {
            activity_id: ActivityId::from_uuid(body.source.activity_id),
            agent_id: AgentInstanceId::from_uuid(body.target_agent_id),
            session_id: AgentSessionId::from_uuid(body.target_session_id),
            revision: AgentInstanceRevision::new(body.expected_target_agent_revision)
                .map_err(|_| ApiError::InvalidIdentity)?,
        };
        let target_principal = authority
            .handoff_target_principal(
                body.source.scope.clone(),
                selector,
                self.member_source_slot().await?,
            )
            .await
            .map_err(source_error)?;
        let request = target
            .source_request(principal, target_principal, observed_at)
            .map_err(ApiError::Runtime)?;
        let member = MemberTarget {
            scope: body.source.scope.clone(),
            activity_id: body.source.activity_id,
            session_id: body.source.session_id,
        };
        self.member_refresh(&member, principal, reference).await?;
        authority
            .departure_reconcile(
                body.source.scope.clone(),
                reference,
                request,
                self.member_source_slot().await?,
            )
            .await
            .map_err(source_effect_error)
    }

    async fn handoff_commit(
        &self,
        member: &MemberTarget,
        target: HandoffTarget,
        principal: AuthenticatedPrincipal,
        reference: MembershipSourceReference,
        source: heterarchy_orchestration_operations::DepartureObservation,
        observed_at: i64,
    ) -> Result<
        (
            heterarchy_orchestration_operations::LogicalDepartureReceipt,
            bool,
        ),
        ApiError,
    > {
        self.member_refresh(member, principal, reference)
            .await
            .map_err(|_| ApiError::SourceBindingUnconfirmed)?;
        let committed_at =
            crate::current_timestamp_millis().map_err(|_| ApiError::SourceBindingUnconfirmed)?;
        let publisher = self.clone();
        self.catalog_job(move |runtime| {
            if let Some(receipt) = runtime.handoff_readback(target, principal, &source)? {
                return Ok((receipt, true));
            }
            let receipt =
                runtime.commit_handoff(target, principal, &source, observed_at, committed_at)?;
            publisher.publish_state_changed(
                crate::RealtimeEventKind::MembershipChanged,
                receipt.participant_id(),
                receipt.membership_revision(),
                Some(receipt.workspace_id().into_uuid()),
            );
            Ok((receipt, false))
        })
        .await
        .map_err(|_| ApiError::SourceBindingUnconfirmed)
    }

    async fn handoff_owned(
        &self,
        body: HandoffRequest,
        target: HandoffTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<HandoffResponse, ApiError> {
        let reference = self.handoff_reference(&body, principal).await?;
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::WorkspaceScopeUnavailable)?;
        let existing = authority
            .departure_by_identity(
                body.source.scope.clone(),
                reference,
                body.request_id,
                AgentSessionId::from_uuid(body.source.session_id),
                principal,
                self.member_source_slot().await?,
            )
            .await
            .map_err(source_error)?;
        if existing.is_none()
            && self
                .catalog_job(move |runtime| runtime.handoff_journal_present(target, principal))
                .await?
        {
            return Err(ApiError::SourceBindingUnconfirmed);
        }
        if let Some(source) = existing
            && let Some(receipt) = self
                .catalog_job(move |runtime| runtime.handoff_readback(target, principal, &source))
                .await?
        {
            let latest = self.handoff_reference(&body, principal).await?;
            if latest != reference {
                return Err(ApiError::Runtime(RuntimeError::Conflict));
            }
            return Ok(HandoffResponse {
                request: body,
                receipt: receipt_wire(&receipt),
                replayed: true,
            });
        }
        self.runtime
            .ensure_broker_owner()
            .await
            .map_err(RuntimeError::BrokerOwner)
            .map_err(|error| {
                if existing.is_some() {
                    ApiError::SourceBindingUnconfirmed
                } else {
                    ApiError::Runtime(error)
                }
            })?;
        let member = MemberTarget {
            scope: body.source.scope.clone(),
            activity_id: body.source.activity_id,
            session_id: body.source.session_id,
        };
        self.member_refresh(&member, principal, reference)
            .await
            .map_err(|error| {
                if existing.is_some() {
                    ApiError::SourceBindingUnconfirmed
                } else {
                    error
                }
            })?;
        let observed_at = crate::current_timestamp_millis()?;
        let source = if let Some(source) = existing {
            source
        } else {
            self.handoff_source_effect(&body, target, principal, reference, &authority, observed_at)
                .await?
        };
        let (receipt, replayed) = self
            .handoff_commit(&member, target, principal, reference, source, observed_at)
            .await?;
        Ok(HandoffResponse {
            request: body,
            receipt: receipt_wire(&receipt),
            replayed,
        })
    }
}

fn validate(path: &str, body: &HandoffRequest) -> Result<HandoffTarget, ApiError> {
    validate_workspace_reference(&body.source.scope, Some(body.source.session_id))?;
    if crate::canonical_uuid(path)? != body.source.scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    let source = DepartureTarget::new(
        body.source.scope.workspace_id,
        body.source.activity_id,
        body.source.agent_id,
        body.source.session_id,
        body.source.expected_agent_revision,
    )
    .map_err(ApiError::Runtime)?;
    HandoffTarget::new(
        source,
        body.target_agent_id,
        body.target_session_id,
        body.expected_target_agent_revision,
        body.expected_coordinator_revision,
        plan(&body.expected_plan)?,
        body.request_id,
    )
    .map_err(ApiError::Runtime)
}

fn plan(
    value: &crate::wire::handoff::HandoffPlanWire,
) -> Result<forge_domain::CoordinationSnapshot, ApiError> {
    use forge_domain::{
        AgentFocusNode, CoordinationSnapshot, DagHeadRevision, DagNodeId, DagNodeRevision,
        DagRevisionNumber, GoalAttribution, GoalId, GoalRevision,
    };
    let goal = value
        .goal
        .as_ref()
        .map(|v| {
            GoalRevision::new(v.expected_revision)
                .map(|r| GoalAttribution::new(GoalId::from_uuid(v.goal_id), r))
        })
        .transpose()
        .map_err(|_| ApiError::InvalidIdentity)?;
    let node = value
        .node
        .as_ref()
        .map(|v| {
            DagNodeRevision::new(v.expected_revision)
                .map(|r| AgentFocusNode::new(DagNodeId::from_uuid(v.node_id), r))
        })
        .transpose()
        .map_err(|_| ApiError::InvalidIdentity)?;
    let dag = value
        .dag_revision
        .map(DagRevisionNumber::new)
        .transpose()
        .map_err(|_| ApiError::InvalidIdentity)?;
    let head = value
        .dag_head_revision
        .map(DagHeadRevision::new)
        .transpose()
        .map_err(|_| ApiError::InvalidIdentity)?;
    CoordinationSnapshot::new(goal, dag, head, node).map_err(|_| ApiError::InvalidIdentity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        WorkspaceQueryScope, departure::DepartureTarget as WireTarget, handoff::HandoffPlanWire,
    };
    use uuid::Uuid;

    #[test]
    fn handoff_route_rejects_foreign_path_and_invalid_plan_before_authority() {
        let body = HandoffRequest {
            source: WireTarget {
                scope: WorkspaceQueryScope {
                    workspace_id: Uuid::from_u128(1),
                    location_id: Uuid::from_u128(2),
                    location_revision: 1,
                    workspace_revision: 1,
                    locator: "workspace".to_owned(),
                },
                activity_id: Uuid::from_u128(3),
                agent_id: Uuid::from_u128(4),
                session_id: Uuid::from_u128(5),
                expected_agent_revision: 1,
            },
            target_agent_id: Uuid::from_u128(6),
            target_session_id: Uuid::from_u128(7),
            expected_target_agent_revision: 1,
            expected_coordinator_revision: 1,
            expected_plan: HandoffPlanWire {
                goal: None,
                dag_revision: None,
                dag_head_revision: None,
                node: None,
            },
            request_id: Uuid::from_u128(8),
        };
        let path = body.source.scope.workspace_id.to_string();
        assert!(validate(&path, &body).is_ok());
        assert!(validate(&Uuid::from_u128(9).to_string(), &body).is_err());
        let mut changed = body.clone();
        changed.expected_plan.dag_revision = Some(1);
        assert!(validate(&path, &changed).is_err());
        changed = body.clone();
        changed.target_session_id = changed.source.session_id;
        assert!(validate(&path, &changed).is_err());
        changed = body;
        changed.expected_coordinator_revision = i64::MAX;
        assert!(validate(&path, &changed).is_err());
    }
}
