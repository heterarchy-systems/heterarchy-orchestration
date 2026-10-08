//! Principal-bound participation in an already confirmed provider operation.

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{AgentTransportKind, AuthenticatedPrincipal};
use heterarchy_orchestration_runtime::{
    RuntimeError,
    provisioning::participation::{
        AgentRequestParticipationTarget, agent_request_session_open_request_id,
    },
};
use uuid::Uuid;

use crate::wire::membership::{
    MemberActivateRequest, MemberConfirmRequest, MemberRegisterRequest,
    MemberRegistrationQueryRequest, MemberTarget, MemberTransportWire,
};
use crate::wire::provisioning_participation::{
    AgentRequestActivateRequest, AgentRequestJoinRequest, AgentRequestParticipationQueryRequest,
    AgentRequestParticipationQueryResponse, AgentRequestParticipationResponse,
    AgentRequestParticipationTargetWire,
};
use crate::wire::{AgentSessionResponse, WorkspaceQueryScope};
use crate::{BackendState, app_support::LaneError};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/provision-operations/{operation_id}/join",
            post(join),
        )
        .route(
            "/internal/v1/provision-operations/{operation_id}/activate",
            post(activate),
        )
        .route(
            "/internal/v1/provision-operations/{operation_id}/participation/query",
            post(query),
        )
}

fn validate_operation(path: &str, operation_id: Uuid) -> Result<(), LaneError> {
    if crate::canonical_uuid(path)? != operation_id || operation_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

async fn join(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentRequestJoinRequest>,
) -> Result<Json<AgentRequestParticipationResponse>, LaneError> {
    validate_operation(&path, body.operation_id)?;
    if body.session_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    state
        .join_confirmed_request(body.operation_id, body.session_id, principal)
        .await
        .map(Json)
}

async fn activate(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentRequestActivateRequest>,
) -> Result<Json<AgentRequestParticipationResponse>, LaneError> {
    validate_operation(&path, body.operation_id)?;
    let operation_id = body.operation_id;
    let target = state
        .catalog_job(move |runtime| runtime.agent_request_participation_target(operation_id))
        .await?;
    let _scope = state.participation_reference(&target, true).await?;
    let opened = state
        .catalog_job(move |runtime| {
            runtime.open_session(
                agent_request_session_open_request_id(operation_id),
                principal,
                crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)?,
            )
        })
        .await
        .map_err(partial_stage_error)?;
    if opened.session.state != "open" {
        return Err(LaneError::Runtime(RuntimeError::Forbidden));
    }
    let mut response = state
        .join_confirmed_request(operation_id, opened.session.session_id, principal)
        .await
        .map_err(partial_stage_error)?;
    response.session_created = Some(opened.created);
    response.session_revision = Some(opened.session.revision);
    Ok(Json(response))
}

impl BackendState {
    async fn participation_reference(
        &self,
        target: &AgentRequestParticipationTarget,
        activity: bool,
    ) -> Result<WorkspaceQueryScope, LaneError> {
        let authority = self
            .coordinator_authority
            .as_ref()
            .ok_or(crate::ApiError::WorkspaceScopeUnavailable)?;
        let scope = authority
            .workspace_reference(target.workspace_id)
            .map_err(crate::membership::source_error)?;
        self.workspace_scope_proof(scope.clone()).await?;
        if activity {
            self.activity_scope_proof(scope.clone(), target.activity_id)
                .await?;
        }
        Ok(scope)
    }

    // Each admitted stage owns/tracks its actual work. Disconnect may stop the
    // composition between stages; explicit query observes their durable receipts.
    async fn join_confirmed_request(
        &self,
        operation_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<AgentRequestParticipationResponse, LaneError> {
        let target = self
            .catalog_job(move |runtime| {
                runtime.require_open_session(session_id, principal)?;
                runtime.agent_request_participation_target(operation_id)
            })
            .await?;
        let scope = self.participation_reference(&target, true).await?;
        let member_target = MemberTarget {
            scope,
            activity_id: target.activity_id,
            session_id,
        };
        let _source = self.member_admit(&member_target, principal).await?;
        let bound = self
            .catalog_job(move |runtime| {
                runtime.bind_agent_request_participation(
                    operation_id,
                    session_id,
                    principal,
                    crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)?,
                )
            })
            .await
            .map_err(partial_stage_error)?;
        if bound != target {
            return Err(LaneError::Runtime(RuntimeError::Conflict));
        }
        self.register_confirmed_participation(operation_id, principal, &member_target)
            .await
            .map_err(partial_stage_error)?;
        let _source = self
            .member_admit(&member_target, principal)
            .await
            .map_err(partial_stage_error)?;
        let completed = self
            .catalog_job(move |runtime| {
                runtime.complete_agent_request_participation(
                    operation_id,
                    session_id,
                    principal,
                    crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)?,
                )
            })
            .await
            .map_err(partial_stage_error)?;
        Ok(AgentRequestParticipationResponse {
            target: member_target,
            operation_id: completed.operation_id,
            agent_request_id: completed.request_id,
            agent_request_state: completed.state.to_owned(),
            agent_request_revision: completed.revision,
            profile_id: completed.profile_id,
            runtime_session_id: completed.runtime_session_id,
            membership: crate::membership::activation_wire(&completed.membership),
            session_created: None,
            session_revision: None,
        })
    }

    async fn register_confirmed_participation(
        &self,
        operation_id: Uuid,
        principal: AuthenticatedPrincipal,
        target: &MemberTarget,
    ) -> Result<(), LaneError> {
        let session_id = target.session_id;
        let (_, reference) = self.member_admit(target, principal).await?;
        let command = self
            .catalog_job(move |runtime| {
                runtime.agent_request_member_register_command(
                    operation_id,
                    session_id,
                    principal,
                    reference.migration_id(),
                    AgentTransportKind::StreamableHttp,
                    crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)?,
                )
            })
            .await?;
        if command.target.workspace_id != target.scope.workspace_id
            || command.target.activity_id != target.activity_id
        {
            return Err(LaneError::Runtime(RuntimeError::Conflict));
        }
        let registered = self
            .register_member(
                principal,
                MemberRegisterRequest {
                    target: target.clone(),
                    display_name: command.display_name,
                    declared_role: command.declared_role,
                    transport: transport_wire(command.transport),
                },
            )
            .await?;
        let request_id = registered.progress.request_id;
        let _commit = self
            .confirm_member(
                principal,
                MemberConfirmRequest {
                    target: target.clone(),
                    request_id,
                    recipients: vec![],
                },
            )
            .await?;
        let _activation = self
            .activate_member(
                principal,
                MemberActivateRequest {
                    target: target.clone(),
                    request_id,
                },
            )
            .await?;
        Ok(())
    }
}

async fn query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentRequestParticipationQueryRequest>,
) -> Result<Json<AgentRequestParticipationQueryResponse>, LaneError> {
    validate_operation(&path, body.operation_id)?;
    if body.session_id.is_some_and(|id| id.is_nil()) {
        return Err(LaneError::InvalidIdentity);
    }
    let operation_id = body.operation_id;
    let session = state
        .catalog_job(move |runtime| {
            if let Some(session_id) = body.session_id {
                runtime.session(session_id, principal).map(Some)
            } else {
                runtime.session_open_readback(
                    agent_request_session_open_request_id(operation_id),
                    principal,
                )
            }
        })
        .await?;
    let mut response = AgentRequestParticipationQueryResponse {
        operation_id,
        session: session.map(AgentSessionResponse::from),
        target: None,
        membership: None,
    };
    let Some(session) = &response.session else {
        return Ok(Json(response));
    };
    if session.state != "open" {
        return Ok(Json(response));
    }
    let session_id = session.session_id;
    let linked = state
        .catalog_job(move |runtime| {
            runtime.agent_request_participation_linked(operation_id, session_id, principal)
        })
        .await?;
    if !linked {
        return Ok(Json(response));
    }
    let target = state
        .catalog_job(move |runtime| {
            let target = runtime.agent_request_participation_target(operation_id)?;
            let binding =
                runtime.profile_session_binding(target.profile_id, session_id, principal)?;
            Ok(binding
                .filter(|binding| binding.request_id == operation_id)
                .map(|_| target))
        })
        .await?;
    let Some(target) = target else {
        return Ok(Json(response));
    };
    let scope = state.participation_reference(&target, false).await?;
    let member_target = MemberTarget {
        scope: scope.clone(),
        activity_id: target.activity_id,
        session_id,
    };
    let request_id = heterarchy_orchestration_runtime::membership::member_registration_request_id(
        scope.workspace_id,
        target.activity_id,
        session_id,
    );
    let membership = state
        .query_member(
            principal,
            MemberRegistrationQueryRequest {
                target: member_target,
                request_id,
            },
        )
        .await?;
    let request = state
        .catalog_job(move |runtime| {
            runtime
                .agent_request_in_workspace(target.workspace_id, target.request_id)?
                .ok_or(RuntimeError::NotFound)
        })
        .await?;
    response.target = Some(AgentRequestParticipationTargetWire {
        scope,
        activity_id: target.activity_id,
        agent_request_id: target.request_id,
        agent_request_state: request.state.to_owned(),
        agent_request_revision: request.revision,
        profile_id: target.profile_id,
        runtime_session_id: target.runtime_session_id,
    });
    response.membership = Some(membership);
    Ok(Json(response))
}

fn partial_stage_error(error: impl Into<LaneError>) -> LaneError {
    match error.into() {
        LaneError::Capacity
        | LaneError::WorkspaceScopeUnavailable
        | LaneError::CoordinatorUnavailable
        | LaneError::ClockUnavailable => LaneError::SourceBindingUnconfirmed,
        LaneError::Runtime(error) => {
            let (status, response) = crate::app_support::runtime_error_response(&error);
            if status == axum::http::StatusCode::SERVICE_UNAVAILABLE && response.retryable {
                LaneError::SourceBindingUnconfirmed
            } else {
                LaneError::Runtime(error)
            }
        }
        other => other,
    }
}

fn transport_wire(transport: AgentTransportKind) -> MemberTransportWire {
    match transport {
        AgentTransportKind::Retired => MemberTransportWire::Mcp,
        AgentTransportKind::Stdio => MemberTransportWire::Stdio,
        AgentTransportKind::StreamableHttp => MemberTransportWire::StreamableHttp,
        AgentTransportKind::Other => MemberTransportWire::Other,
    }
}
