//! Workspace membership commands over Runtime and the configured Forge source.

use crate::{
    ApiError, BackendState, CoordinatorAuthority, CoordinatorAuthorityError,
    app_support::{LaneError, validate_workspace_reference},
    forge_membership::MembershipSourceReference,
    wire::membership::{
        MemberActivateRequest, MemberActivateResponse, MemberActivationWire, MemberCommitWire,
        MemberConfirmRequest, MemberConfirmResponse, MemberPhaseWire, MemberProgressWire,
        MemberRegisterRequest, MemberRegisterResponse, MemberRegistrationQueryRequest,
        MemberRegistrationQueryResponse, MemberTarget, MemberTransportWire,
    },
};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{AgentTransportKind, AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_operations::{AgentBindingActivationRequest, RegistrationPhase};
use heterarchy_orchestration_runtime::{
    RuntimeError,
    membership::{
        MemberActivation, MemberCommit, MemberProgress, MemberRegisterCommand, MembershipTarget,
        member_registration_request_id,
    },
};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/register",
            post(register),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/confirm",
            post(confirm),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/activate",
            post(activate),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/query",
            post(query),
        )
}

fn validate(
    path: &str,
    target: &MemberTarget,
    request: Option<Uuid>,
) -> Result<MembershipTarget, LaneError> {
    validate_workspace_reference(&target.scope, Some(target.session_id))?;
    if crate::canonical_uuid(path)? != target.scope.workspace_id || target.activity_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    if request.is_some_and(|request| {
        request
            != member_registration_request_id(
                target.scope.workspace_id,
                target.activity_id,
                target.session_id,
            )
    }) {
        return Err(LaneError::InvalidIdentity);
    }
    MembershipTarget::new(
        target.scope.workspace_id,
        target.activity_id,
        target.session_id,
    )
    .map_err(Into::into)
}

pub(crate) fn source_error(error: CoordinatorAuthorityError) -> ApiError {
    match error {
        CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
        CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
        CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
    }
}

pub(super) fn source_effect_error(error: CoordinatorAuthorityError) -> ApiError {
    match error {
        CoordinatorAuthorityError::Unavailable => ApiError::SourceBindingUnconfirmed,
        other => source_error(other),
    }
}

impl BackendState {
    pub(super) async fn member_source_slot(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        self.forge_query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::Unavailable)
    }

    pub(super) async fn member_authorize(
        &self,
        target: &MemberTarget,
        principal: AuthenticatedPrincipal,
        activity: bool,
    ) -> Result<MembershipSourceReference, ApiError> {
        let selected = target.clone();
        let context = self
            .catalog_job(move |runtime| {
                runtime.membership_source_context(
                    selected.scope.workspace_id,
                    selected.session_id,
                    principal,
                )
            })
            .await?;
        self.workspace_scope_proof(target.scope.clone()).await?;
        if activity {
            self.activity_scope_proof(target.scope.clone(), target.activity_id)
                .await?;
        }
        let selected = target.clone();
        self.catalog_job(move |runtime| {
            runtime.require_open_session(selected.session_id, principal)
        })
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

    pub(super) async fn member_refresh(
        &self,
        target: &MemberTarget,
        principal: AuthenticatedPrincipal,
        expected: MembershipSourceReference,
    ) -> Result<(), ApiError> {
        let current = self.member_authorize(target, principal, true).await?;
        if current != expected {
            return Err(ApiError::Runtime(RuntimeError::Conflict));
        }
        Ok(())
    }

    pub(crate) async fn member_admit(
        &self,
        target: &MemberTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<(CoordinatorAuthority, MembershipSourceReference), ApiError> {
        let _reference = self.member_authorize(target, principal, true).await?;
        self.runtime
            .ensure_broker_owner()
            .await
            .map_err(RuntimeError::BrokerOwner)
            .map_err(ApiError::Runtime)?;
        let reference = self.member_authorize(target, principal, true).await?;
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::WorkspaceScopeUnavailable)?;
        authority
            .membership_delegation(
                target.scope.clone(),
                reference,
                self.member_source_slot().await?,
            )
            .await
            .map_err(source_error)?;
        Ok((authority, reference))
    }
}

async fn register(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<MemberRegisterRequest>,
) -> Result<Json<MemberRegisterResponse>, LaneError> {
    let _target = validate(&path, &body.target, None)?;
    state.register_member(principal, body).await.map(Json)
}

async fn confirm(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<MemberConfirmRequest>,
) -> Result<Json<MemberConfirmResponse>, LaneError> {
    let _target = validate(&path, &body.target, Some(body.request_id))?;
    state.confirm_member(principal, body).await.map(Json)
}

async fn activate(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<MemberActivateRequest>,
) -> Result<Json<MemberActivateResponse>, LaneError> {
    let _target = validate(&path, &body.target, Some(body.request_id))?;
    state.activate_member(principal, body).await.map(Json)
}

async fn query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<MemberRegistrationQueryRequest>,
) -> Result<Json<MemberRegistrationQueryResponse>, LaneError> {
    let _target = validate(&path, &body.target, Some(body.request_id))?;
    state.query_member(principal, body).await.map(Json)
}

impl BackendState {
    pub(crate) async fn register_member(
        &self,
        principal: AuthenticatedPrincipal,
        body: MemberRegisterRequest,
    ) -> Result<MemberRegisterResponse, LaneError> {
        let target = validate(
            &body.target.scope.workspace_id.to_string(),
            &body.target,
            None,
        )?;
        let selected = body.target.clone();
        let owner = self.clone();
        let progress = self
            .run_owned(async move {
                let (authority, reference) = owner.member_admit(&selected, principal).await?;
                let command = MemberRegisterCommand {
                    target,
                    principal,
                    display_name: body.display_name,
                    declared_role: body.declared_role,
                    transport: transport(body.transport),
                    migration_id: reference.migration_id(),
                    now_ms: 0,
                };
                let mut progress = owner
                    .catalog_job(move |runtime| {
                        let mut command = command;
                        command.now_ms = crate::current_timestamp_millis()
                            .map_err(|_| RuntimeError::InvalidInput)?;
                        runtime.prepare_member_registration(command)
                    })
                    .await?;
                if progress.phase() == RegistrationPhase::Pending {
                    let request = progress.request().binding().clone();
                    // Read the exact source receipt before an explicit resume. Absence
                    // is an observation, never an automatic retry grant.
                    let observed = authority
                        .membership_reservation(
                            selected.scope.clone(),
                            reference,
                            request.clone(),
                            owner.member_source_slot().await?,
                        )
                        .await
                        .map_err(source_effect_error)?;
                    let reservation = if let Some(receipt) = observed {
                        receipt
                    } else {
                        owner
                            .member_refresh(&selected, principal, reference)
                            .await?;
                        authority
                            .membership_reserve(
                                selected.scope.clone(),
                                reference,
                                request,
                                now()?,
                                owner.member_source_slot().await?,
                            )
                            .await
                            .map_err(source_effect_error)?
                    };
                    owner
                        .member_refresh(&selected, principal, reference)
                        .await?;
                    progress = owner
                        .catalog_job(move |runtime| {
                            runtime.record_member_reservation_observation(
                                target,
                                principal,
                                &reservation,
                            )
                        })
                        .await?;
                }
                Ok(progress)
            })
            .await?;
        Ok(MemberRegisterResponse {
            target: body.target,
            progress: progress_wire(&progress),
        })
    }

    pub(crate) async fn confirm_member(
        &self,
        principal: AuthenticatedPrincipal,
        body: MemberConfirmRequest,
    ) -> Result<MemberConfirmResponse, LaneError> {
        let target = validate(
            &body.target.scope.workspace_id.to_string(),
            &body.target,
            Some(body.request_id),
        )?;
        if body.recipients.len() > 64 || body.recipients.iter().any(Uuid::is_nil) {
            return Err(LaneError::InvalidIdentity);
        }
        let mut unique = body.recipients.clone();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() != body.recipients.len() {
            return Err(LaneError::InvalidIdentity);
        }
        let selected = body.target.clone();
        let request = body.request_id;
        let owner = self.clone();
        let commit = self
            .run_owned(async move {
                let _source = owner.member_admit(&selected, principal).await?;
                owner
                    .catalog_job(move |runtime| {
                        runtime.confirm_member_registration(
                            target,
                            principal,
                            request,
                            &unique,
                            crate::current_timestamp_millis()
                                .map_err(|_| RuntimeError::InvalidInput)?,
                        )
                    })
                    .await
            })
            .await?;
        Ok(MemberConfirmResponse {
            target: body.target,
            commit: commit_wire(&commit),
        })
    }

    pub(crate) async fn activate_member(
        &self,
        principal: AuthenticatedPrincipal,
        body: MemberActivateRequest,
    ) -> Result<MemberActivateResponse, LaneError> {
        let target = validate(
            &body.target.scope.workspace_id.to_string(),
            &body.target,
            Some(body.request_id),
        )?;
        let selected = body.target.clone();
        let request_id = body.request_id;
        let owner = self.clone();
        let activation = self
            .run_owned(async move {
                let (authority, reference) = owner.member_admit(&selected, principal).await?;
                let plan = owner
                    .catalog_job(move |runtime| {
                        runtime.member_activation_plan(target, principal, request_id)
                    })
                    .await?;
                if let Some(activation) = plan.activation() {
                    return Ok(activation.clone());
                }
                let request = plan.progress().request();
                let binding = request.binding().clone();
                if binding.session_id().into_uuid() != target.session_id
                    || binding.principal() != principal
                {
                    return Err(ApiError::Runtime(RuntimeError::Forbidden));
                }
                let activation_request = AgentBindingActivationRequest::new(
                    request_id,
                    plan.commit().commit_digest(),
                    request.display_name().clone(),
                    request.declared_role().clone(),
                    request.transport(),
                )
                .map_err(|_| ApiError::InvalidIdentity)?;
                let existing = authority
                    .membership_activation(
                        selected.scope.clone(),
                        reference,
                        binding.clone(),
                        owner.member_source_slot().await?,
                    )
                    .await
                    .map_err(source_effect_error)?;
                let observed = if let Some(receipt) = existing {
                    receipt
                } else {
                    owner
                        .member_refresh(&selected, principal, reference)
                        .await?;
                    authority
                        .membership_activate(
                            selected.scope.clone(),
                            reference,
                            activation_request,
                            binding,
                            now()?,
                            owner.member_source_slot().await?,
                        )
                        .await
                        .map_err(source_effect_error)?
                };
                owner
                    .member_refresh(&selected, principal, reference)
                    .await?;
                let receipt = owner
                    .catalog_job(move |runtime| {
                        runtime.record_member_activation_observation(target, principal, &observed)
                    })
                    .await?;
                owner.require_snapshot_resync();
                Ok(receipt)
            })
            .await?;
        Ok(MemberActivateResponse {
            target: body.target,
            activation: activation_wire(&activation),
        })
    }

    pub(crate) async fn query_member(
        &self,
        principal: AuthenticatedPrincipal,
        body: MemberRegistrationQueryRequest,
    ) -> Result<MemberRegistrationQueryResponse, LaneError> {
        let target = validate(
            &body.target.scope.workspace_id.to_string(),
            &body.target,
            Some(body.request_id),
        )?;
        let _reference = self
            .member_authorize(&body.target, principal, false)
            .await?;
        let request_id = body.request_id;
        let readback = self
            .catalog_job(move |runtime| {
                runtime.member_registration_readback(target, principal, request_id)
            })
            .await?;
        Ok(MemberRegistrationQueryResponse {
            target: body.target,
            request_id,
            progress: readback.progress.as_ref().map(progress_wire),
            commit: readback.commit.as_ref().map(commit_wire),
            activation: readback.activation.as_ref().map(activation_wire),
        })
    }
}

fn transport(value: MemberTransportWire) -> AgentTransportKind {
    match value {
        MemberTransportWire::Mcp => AgentTransportKind::Retired,
        MemberTransportWire::Stdio => AgentTransportKind::Stdio,
        MemberTransportWire::StreamableHttp => AgentTransportKind::StreamableHttp,
        MemberTransportWire::Other => AgentTransportKind::Other,
    }
}
fn phase(value: RegistrationPhase) -> MemberPhaseWire {
    match value {
        RegistrationPhase::Pending => MemberPhaseWire::Pending,
        RegistrationPhase::Reserved => MemberPhaseWire::Reserved,
        RegistrationPhase::MembershipCommitted => MemberPhaseWire::MembershipCommitted,
        RegistrationPhase::Ready => MemberPhaseWire::Ready,
    }
}
fn now() -> Result<TimestampMillis, ApiError> {
    TimestampMillis::new(crate::current_timestamp_millis().map_err(|_| ApiError::Unavailable)?)
        .map_err(|_| ApiError::Unavailable)
}
fn progress_wire(value: &MemberProgress) -> MemberProgressWire {
    let request = value.request().binding();
    MemberProgressWire {
        workspace_id: value.workspace_id().into_uuid(),
        activity_id: request.activity_id().into_uuid(),
        session_id: request.session_id().into_uuid(),
        agent_id: request.agent_id().into_uuid(),
        request_id: request.request_id(),
        participant_id: value.participant_id(),
        migration_id: value.migration_id(),
        phase: phase(value.phase()),
        reserved: value.reserved_at().is_some(),
        reserved_at_ms: value.reserved_at().map(TimestampMillis::get),
    }
}
fn commit_wire(value: &MemberCommit) -> MemberCommitWire {
    MemberCommitWire {
        request_id: value.request_id(),
        participant_id: value.participant_id(),
        commit_digest: hex(&value.commit_digest()),
        joined_event_id: value.joined_event_id(),
        committed_at_ms: value.committed_at().get(),
    }
}
pub(crate) fn activation_wire(value: &MemberActivation) -> MemberActivationWire {
    MemberActivationWire {
        request_id: value.request_id(),
        participant_id: value.participant_id(),
        agent_id: value.binding().agent_id().into_uuid(),
        membership_digest: hex(&value.membership_digest()),
        activated_at_ms: value.activated_at().get(),
        phase: MemberPhaseWire::Ready,
    }
}
fn hex(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}
