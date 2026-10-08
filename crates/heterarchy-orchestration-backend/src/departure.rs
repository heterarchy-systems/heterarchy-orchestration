//! Source-fenced detach commands and immutable logical departure observations.

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentInstanceRevision, AgentSessionId, AgentTransportKind,
    AuthenticatedPrincipal, DetachAgent, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AgentDepartureCommand, AgentDepartureReconciliationRequest, LogicalDepartureReceipt,
};
use heterarchy_orchestration_runtime::departure::{
    DepartureTarget as RuntimeTarget, departure_identity_digest, detach_request_id,
};

use crate::{
    ApiError, BackendState, RuntimeError,
    app_support::validate_workspace_reference,
    membership::source_effect_error,
    wire::{
        departure::{
            DepartureKindWire, DepartureQueryRequest, DepartureQueryResponse, DepartureReceiptWire,
            DepartureRequest, DepartureResponse, DepartureTarget,
        },
        membership::{MemberTarget, MemberTransportWire},
    },
};

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/detach",
            post(detach),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/departure/query",
            post(query),
        )
}

async fn detach(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<DepartureRequest>,
) -> Result<Json<DepartureResponse>, ApiError> {
    let target = validate(&path, &body.target)?;
    let owner = state.clone();
    state
        .run_owned(async move { owner.detach_owned(body, target, principal).await })
        .await
        .map(Json)
}

impl BackendState {
    async fn detach_owned(
        &self,
        body: DepartureRequest,
        target: RuntimeTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<DepartureResponse, ApiError> {
        let prepared = self
            .catalog_job(move |runtime| runtime.prepare_detach(target, principal))
            .await?;
        self.workspace_scope_proof(body.target.scope.clone())
            .await?;
        if prepared.receipt.is_some() {
            let latest = self
                .catalog_job(move |runtime| runtime.prepare_detach(target, principal))
                .await?;
            let receipt = latest
                .receipt
                .ok_or(ApiError::Runtime(RuntimeError::Conflict))?;
            return Ok(DepartureResponse {
                target: body.target,
                request_id: latest.request_id,
                receipt: receipt_wire(&receipt),
                replayed: true,
            });
        }
        self.runtime
            .ensure_broker_owner()
            .await
            .map_err(RuntimeError::BrokerOwner)
            .map_err(ApiError::Runtime)?;
        let member = member_target(&body.target);
        let reference = self.member_authorize(&member, principal, true).await?;
        let observed_at = crate::current_timestamp_millis()?;
        let request = source_request(&body.target, principal, observed_at)?;
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::WorkspaceScopeUnavailable)?;
        let existing = authority
            .departure_read(
                body.target.scope.clone(),
                reference,
                request.clone(),
                self.member_source_slot().await?,
            )
            .await
            .map_err(source_effect_error)?;
        let source = if let Some(source) = existing {
            source
        } else {
            self.member_refresh(&member, principal, reference).await?;
            authority
                .departure_reconcile(
                    body.target.scope.clone(),
                    reference,
                    request,
                    self.member_source_slot().await?,
                )
                .await
                .map_err(source_effect_error)?
        };
        // The exact source receipt proves its effect is already durable. A
        // later denial or owner failure cannot claim the composite did nothing.
        self.member_refresh(&member, principal, reference)
            .await
            .map_err(|_| ApiError::SourceBindingUnconfirmed)?;
        let committed_at =
            crate::current_timestamp_millis().map_err(|_| ApiError::SourceBindingUnconfirmed)?;
        let publisher = self.clone();
        let (receipt, replayed) = self
            .catalog_job(move |runtime| {
                if let Some(receipt) = runtime.detach_readback(target, principal)? {
                    return Ok((receipt, true));
                }
                let receipt =
                    runtime.commit_detach(target, principal, &source, observed_at, committed_at)?;
                publisher.publish_state_changed(
                    crate::RealtimeEventKind::MembershipChanged,
                    receipt.participant_id(),
                    receipt.membership_revision(),
                    Some(receipt.workspace_id().into_uuid()),
                );
                Ok((receipt, false))
            })
            .await
            .map_err(|_| ApiError::SourceBindingUnconfirmed)?;
        Ok(DepartureResponse {
            target: body.target,
            request_id: prepared.request_id,
            receipt: receipt_wire(&receipt),
            replayed,
        })
    }
}

async fn query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<DepartureQueryRequest>,
) -> Result<Json<DepartureQueryResponse>, ApiError> {
    let target = validate(&path, &body.target)?;
    if body.request_id != request_id(&body.target) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let owner = state.clone();
    state
        .run_owned(async move {
            let _session = owner
                .catalog_job(move |runtime| runtime.session(body.target.session_id, principal))
                .await?;
            owner
                .workspace_scope_proof(body.target.scope.clone())
                .await?;
            let receipt = owner
                .catalog_job(move |runtime| runtime.detach_readback(target, principal))
                .await?;
            // None means the logical journal has no receipt. It never proves an
            // uncertain Forge effect absent or grants an automatic retry.
            Ok(Json(DepartureQueryResponse {
                target: body.target,
                request_id: body.request_id,
                receipt: receipt.as_ref().map(receipt_wire),
            }))
        })
        .await
}

fn validate(path: &str, target: &DepartureTarget) -> Result<RuntimeTarget, ApiError> {
    validate_workspace_reference(&target.scope, Some(target.session_id))?;
    if crate::canonical_uuid(path)? != target.scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    RuntimeTarget::new(
        target.scope.workspace_id,
        target.activity_id,
        target.agent_id,
        target.session_id,
        target.expected_agent_revision,
    )
    .map_err(ApiError::Runtime)
}

fn request_id(target: &DepartureTarget) -> uuid::Uuid {
    detach_request_id(
        target.scope.workspace_id,
        target.activity_id,
        target.agent_id,
        target.expected_agent_revision,
    )
}

fn member_target(target: &DepartureTarget) -> MemberTarget {
    MemberTarget {
        scope: target.scope.clone(),
        activity_id: target.activity_id,
        session_id: target.session_id,
    }
}

fn source_request(
    target: &DepartureTarget,
    principal: AuthenticatedPrincipal,
    observed_at: i64,
) -> Result<AgentDepartureReconciliationRequest, ApiError> {
    let workspace = WorkspaceId::from_uuid(target.scope.workspace_id);
    let activity = ActivityId::from_uuid(target.activity_id);
    let agent = AgentInstanceId::from_uuid(target.agent_id);
    let session = AgentSessionId::from_uuid(target.session_id);
    let revision = AgentInstanceRevision::new(target.expected_agent_revision)
        .map_err(|_| ApiError::InvalidIdentity)?;
    let now = TimestampMillis::new(observed_at).map_err(|_| ApiError::Unavailable)?;
    let nonce = request_id(target);
    AgentDepartureReconciliationRequest::new(
        nonce,
        AgentDepartureCommand::Detach(DetachAgent::new(agent, activity, revision, now)),
        session,
        principal,
        departure_identity_digest(workspace, activity, agent, session, principal, nonce),
    )
    .map_err(|_| ApiError::InvalidIdentity)
}

pub(super) fn receipt_wire(receipt: &LogicalDepartureReceipt) -> DepartureReceiptWire {
    DepartureReceiptWire {
        request_id: receipt.request_id(),
        workspace_id: receipt.workspace_id().into_uuid(),
        participant_id: receipt.participant_id(),
        activity_id: receipt.activity_id().into_uuid(),
        agent_id: receipt.agent_id().into_uuid(),
        session_id: receipt.session_id().into_uuid(),
        kind: match receipt.kind() {
            heterarchy_orchestration_operations::LogicalDepartureKind::Detach => {
                DepartureKindWire::Detach
            }
            heterarchy_orchestration_operations::LogicalDepartureKind::Handoff => {
                DepartureKindWire::Handoff
            }
        },
        expected_agent_revision: receipt.expected_agent_revision().get(),
        result_agent_revision: receipt.result_agent_revision().get(),
        target_participant_id: receipt.target_participant_id(),
        target_agent_id: receipt
            .target_agent_id()
            .map(forge_domain::AgentInstanceId::into_uuid),
        target_session_id: receipt
            .target_session_id()
            .map(forge_domain::AgentSessionId::into_uuid),
        result_coordinator_revision: receipt
            .result_coordinator_revision()
            .map(forge_domain::ActivityCoordinatorRevision::get),
        departed_event_id: receipt.departed_event_id(),
        handoff_started_event_id: receipt.handoff_started_event_id(),
        handoff_completed_event_id: receipt.handoff_completed_event_id(),
        membership_revision: receipt.membership_revision(),
        presence_cleared: receipt.presence_cleared(),
        source_reconciled_at_ms: receipt.source_reconciled_at().get(),
        committed_at_ms: receipt.committed_at().get(),
        display_name: receipt.display_name().as_str().to_owned(),
        declared_role: receipt.declared_role().as_str().to_owned(),
        transport: match receipt.transport() {
            AgentTransportKind::Retired => MemberTransportWire::Mcp,
            AgentTransportKind::Stdio => MemberTransportWire::Stdio,
            AgentTransportKind::StreamableHttp => MemberTransportWire::StreamableHttp,
            AgentTransportKind::Other => MemberTransportWire::Other,
        },
    }
}
