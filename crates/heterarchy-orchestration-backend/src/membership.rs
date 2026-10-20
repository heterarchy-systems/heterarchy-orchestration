//! Workspace membership commands over Runtime and the configured Forge source.

use crate::{
    ApiError, BackendState, CoordinatorAuthority, CoordinatorAuthorityError,
    app_support::{LaneError, validate_workspace_reference},
    forge_membership::MembershipSourceReference,
    wire::membership::{
        DurableRequestIdPolicyWire, LegacyEnvelopeRequestIdPolicyWire, MemberActivateRequest,
        MemberActivateResponse, MemberActivationWire, MemberBindingRejectRequest,
        MemberBindingRejectResponse, MemberBindingRejectionQueryRequest,
        MemberBindingRejectionQueryResponse, MemberBindingRejectionReasonWire,
        MemberBindingRejectionReconcileRequest, MemberBindingRejectionReconcileResponse,
        MemberBindingRejectionWire, MemberCommitWire, MemberConfirmRequest, MemberConfirmResponse,
        MemberPhaseWire, MemberPrincipalWire, MemberProgressWire, MemberRegisterRequest,
        MemberRegisterResponse, MemberRegistrationQueryRequest, MemberRegistrationQueryResponse,
        MemberTarget, MemberTransportWire, SourceRequestIdPolicyWire,
    },
};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{AgentTransportKind, AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_operations::{
    AgentBindingActivationRequest, AgentBindingRejectionReason, BindingRejectionAttemptDisposition,
    BindingRejectionAttemptRequest, BindingRejectionSourceRequestPolicy,
    LegacyBindingRejectionEnvelopePolicy, RegistrationBindingRejectionReceipt, RegistrationPhase,
    RegistrationRequest, ResolveBindingRejectionAttempt,
};
use heterarchy_orchestration_runtime::{
    RuntimeError,
    membership::{
        MemberActivation, MemberBindingRejectionPlan, MemberCommit, MemberProgress,
        MemberRegisterCommand, MembershipTarget, member_registration_request_id,
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
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/binding-rejection/query",
            post(binding_rejection_query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/binding-rejection/settle",
            post(binding_rejection_settle),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/members/binding-rejection/reconcile-pre-admission-no-effect",
            post(binding_rejection_reconcile),
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

    async fn prepare_binding_rejection_source_attempt(
        &self,
        selected: &MemberTarget,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        plan: &MemberBindingRejectionPlan,
        registration: &RegistrationRequest,
    ) -> Result<
        (
            CoordinatorAuthority,
            MembershipSourceReference,
            BindingRejectionAttemptRequest,
            BindingRejectionAttemptDisposition,
        ),
        ApiError,
    > {
        if let Some(attempt) = plan.attempt().cloned() {
            self.workspace_scope_proof(selected.scope.clone()).await?;
            let authority = self
                .coordinator_authority
                .clone()
                .ok_or(ApiError::WorkspaceScopeUnavailable)?;
            let reference = MembershipSourceReference::new(
                attempt.issuer_id(),
                attempt.destination_id(),
                attempt.migration_id(),
                attempt.freeze_revision(),
                attempt.manifest_digest(),
            )
            .map_err(|_| ApiError::WorkspaceScopeUnavailable)?;
            let consume_attempt = attempt.clone();
            let disposition = self
                .catalog_job(move |runtime| {
                    runtime.consume_member_binding_rejection_resolution(
                        target,
                        principal,
                        consume_attempt.request_id(),
                        now().map_err(|_| RuntimeError::InvalidInput)?,
                    )
                })
                .await?;
            return Ok((authority, reference, attempt, disposition));
        }

        let (authority, reference) = self.member_admit(selected, principal).await?;
        let attempt = BindingRejectionAttemptRequest::for_registration(
            plan.progress().workspace_id(),
            reference.issuer_id(),
            reference.destination_id(),
            reference.migration_id(),
            reference.freeze_revision(),
            reference.manifest_digest(),
            registration,
            plan.commit().commit_digest(),
            now()?,
        )
        .map_err(RuntimeError::from)
        .map_err(ApiError::Runtime)?;
        let stored_attempt = attempt.clone();
        let disposition = self
            .catalog_job(move |runtime| {
                runtime.prepare_member_binding_rejection_attempt(target, principal, &stored_attempt)
            })
            .await?;
        Ok((authority, reference, attempt, disposition))
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

async fn binding_rejection_query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<MemberBindingRejectionQueryRequest>,
) -> Result<Json<MemberBindingRejectionQueryResponse>, LaneError> {
    let _target = validate(&path, &body.target, Some(body.request_id))?;
    state
        .query_member_binding_rejection(principal, body)
        .await
        .map(Json)
}

async fn binding_rejection_settle(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<MemberBindingRejectRequest>,
) -> Result<Json<MemberBindingRejectResponse>, LaneError> {
    let _target = validate(&path, &body.target, Some(body.request_id))?;
    state.reject_member_binding(principal, body).await.map(Json)
}

async fn binding_rejection_reconcile(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<MemberBindingRejectionReconcileRequest>,
) -> Result<Json<MemberBindingRejectionReconcileResponse>, LaneError> {
    let _target = validate(&path, &body.target, Some(body.request_id))?;
    state
        .reconcile_member_binding_rejection_attempt(principal, body)
        .await
        .map(Json)
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

    pub(crate) async fn reject_member_binding(
        &self,
        principal: AuthenticatedPrincipal,
        body: MemberBindingRejectRequest,
    ) -> Result<MemberBindingRejectResponse, LaneError> {
        let target = validate(
            &body.target.scope.workspace_id.to_string(),
            &body.target,
            Some(body.request_id),
        )?;
        let selected = body.target.clone();
        let request_id = body.request_id;
        let owner = self.clone();
        let rejection = self
            .run_owned(async move {
                owner
                    .catalog_job(move |runtime| runtime.require_operator(principal))
                    .await?;
                let plan = owner
                    .catalog_job(move |runtime| {
                        runtime.member_binding_rejection_plan(target, principal, request_id)
                    })
                    .await?;
                if let Some(rejection) = plan.rejection() {
                    return Ok(rejection.clone());
                }
                let registration = plan.progress().request().clone();
                let binding = registration.binding().clone();
                let activation = AgentBindingActivationRequest::new(
                    request_id,
                    plan.commit().commit_digest(),
                    registration.display_name().clone(),
                    registration.declared_role().clone(),
                    registration.transport(),
                )
                .map_err(|_| ApiError::InvalidIdentity)?;

                let (authority, reference, attempt, disposition) = owner
                    .prepare_binding_rejection_source_attempt(
                        &selected,
                        target,
                        principal,
                        &plan,
                        &registration,
                    )
                    .await?;

                let readback_only = matches!(
                    &disposition,
                    BindingRejectionAttemptDisposition::ReadbackRequired
                );
                if let BindingRejectionAttemptDisposition::Recorded(receipt) = disposition {
                    return Ok(*receipt);
                }
                let existing = authority
                    .membership_rejection(
                        selected.scope.clone(),
                        reference,
                        binding.clone(),
                        attempt.request_digest(),
                        owner.member_source_slot().await?,
                    )
                    .await
                    .map_err(source_effect_error)?;
                let observed = if let Some(receipt) = existing {
                    receipt
                } else {
                    if readback_only {
                        return Err(ApiError::SourceBindingUnconfirmed);
                    }
                    owner
                        .member_refresh(&selected, principal, reference)
                        .await?;
                    authority
                        .membership_reject(
                            selected.scope.clone(),
                            reference,
                            binding,
                            activation,
                            attempt.attempted_at(),
                            owner.member_source_slot().await?,
                        )
                        .await
                        .map_err(source_effect_error)?
                };
                let receipt = owner
                    .catalog_job(move |runtime| {
                        runtime.record_member_binding_rejection_observation(
                            target, principal, &observed,
                        )
                    })
                    .await?;
                Ok(receipt)
            })
            .await?;
        Ok(MemberBindingRejectResponse {
            target: body.target,
            rejection: binding_rejection_wire(&rejection),
        })
    }

    pub(crate) async fn reconcile_member_binding_rejection_attempt(
        &self,
        principal: AuthenticatedPrincipal,
        body: MemberBindingRejectionReconcileRequest,
    ) -> Result<MemberBindingRejectionReconcileResponse, LaneError> {
        let target = validate(
            &body.target.scope.workspace_id.to_string(),
            &body.target,
            Some(body.request_id),
        )?;
        let expected_attempt = digest32(&body.expected_attempt_request_digest)?;
        let expected_artifact = digest32(&body.expected_legacy_artifact_digest)?;
        let selected = body.target.clone();
        let request_id = body.request_id;
        let resolution_request_id = body.resolution_request_id;
        let owner = self.clone();
        let resolution = self
            .run_owned(async move {
                owner
                    .catalog_job(move |runtime| runtime.require_operator(principal))
                    .await?;
                let plan = owner
                    .catalog_job(move |runtime| {
                        runtime.member_binding_rejection_plan(target, principal, request_id)
                    })
                    .await?;
                let attempt = plan.attempt().cloned().ok_or(ApiError::NotFound)?;
                if plan.rejection().is_some() || attempt.request_digest() != expected_attempt {
                    return Err(ApiError::Runtime(RuntimeError::Conflict));
                }
                owner.workspace_scope_proof(selected.scope.clone()).await?;
                let authority = owner
                    .coordinator_authority
                    .clone()
                    .ok_or(ApiError::WorkspaceScopeUnavailable)?;
                let reference = MembershipSourceReference::new(
                    attempt.issuer_id(),
                    attempt.destination_id(),
                    attempt.migration_id(),
                    attempt.freeze_revision(),
                    attempt.manifest_digest(),
                )
                .map_err(|_| ApiError::WorkspaceScopeUnavailable)?;
                let binding = plan.progress().request().binding().clone();
                let found = authority
                    .membership_rejection(
                        selected.scope.clone(),
                        reference,
                        binding,
                        attempt.request_digest(),
                        owner.member_source_slot().await?,
                    )
                    .await
                    .map_err(source_effect_error)?;
                if found.is_some() {
                    return Err(ApiError::Runtime(RuntimeError::Conflict));
                }
                let observed_at = now()?;
                let proof = ResolveBindingRejectionAttempt {
                    request_id,
                    resolution_request_id,
                    expected_attempt_request_digest: expected_attempt,
                    expected_legacy_client_artifact_digest: expected_artifact,
                    source_get_observed_at: observed_at,
                    operator_principal: principal,
                    resolved_at: observed_at,
                };
                owner
                    .catalog_job(move |runtime| {
                        runtime.resolve_member_binding_rejection_attempt(target, principal, &proof)
                    })
                    .await
            })
            .await?;
        if resolution.durable_uuid_version() != 8 {
            return Err(ApiError::Runtime(RuntimeError::Conflict).into());
        }
        Ok(MemberBindingRejectionReconcileResponse {
            target: body.target,
            request_id,
            resolution_request_id: resolution.resolution_request_id(),
            attempt_request_digest: hex(&resolution.attempt_request_digest()),
            legacy_artifact_digest: hex(&resolution.legacy_client_artifact_digest()),
            legacy_envelope_request_id_policy: match resolution.envelope_policy() {
                LegacyBindingRejectionEnvelopePolicy::GeneratedUuidV7 => {
                    LegacyEnvelopeRequestIdPolicyWire::GeneratedUuidV7
                }
            },
            durable_request_id_policy: DurableRequestIdPolicyWire::PersistedUuidV8,
            source_request_id_policy: match resolution.source_request_policy() {
                BindingRejectionSourceRequestPolicy::BodyEqualsEnvelope => {
                    SourceRequestIdPolicyWire::BodyEqualsEnvelope
                }
            },
            source_get_observed_at_ms: resolution.source_get_observed_at().get(),
            resolved_at_ms: resolution.resolved_at().get(),
            consumed: resolution.consumed_at().is_some(),
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

    pub(crate) async fn query_member_binding_rejection(
        &self,
        principal: AuthenticatedPrincipal,
        body: MemberBindingRejectionQueryRequest,
    ) -> Result<MemberBindingRejectionQueryResponse, LaneError> {
        let target = validate(
            &body.target.scope.workspace_id.to_string(),
            &body.target,
            Some(body.request_id),
        )?;
        let request_id = body.request_id;
        let rejection = self
            .catalog_job(move |runtime| {
                runtime.member_binding_rejection_readback(target, principal, request_id)
            })
            .await?;
        Ok(MemberBindingRejectionQueryResponse {
            target: body.target,
            request_id,
            rejection: rejection.as_ref().map(binding_rejection_wire),
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

fn binding_rejection_wire(
    value: &RegistrationBindingRejectionReceipt,
) -> MemberBindingRejectionWire {
    MemberBindingRejectionWire {
        request_id: value.request_id(),
        workspace_id: value.workspace_id().into_uuid(),
        participant_id: value.participant_id(),
        activity_id: value.binding().activity_id().into_uuid(),
        agent_id: value.binding().agent_id().into_uuid(),
        session_id: value.binding().session_id().into_uuid(),
        principal: principal_wire(value.binding().principal()),
        migration_id: value.migration_id(),
        destination_id: value.destination_id(),
        expected_freeze_revision: value.expected_freeze_revision(),
        manifest_digest: hex(&value.manifest_digest()),
        issuer_id: value.issuer_id(),
        workspace_root_fingerprint: hex16(value.workspace_root_fingerprint().as_bytes()),
        binding_digest: hex(&value.binding_digest()),
        delegation_digest: hex(&value.delegation_digest()),
        membership_digest: hex(&value.membership_digest()),
        display_name: value.display_name().as_str().to_owned(),
        declared_role: value.declared_role().as_str().to_owned(),
        transport: transport_wire(value.transport()),
        reason: match value.reason() {
            AgentBindingRejectionReason::PrincipalTransportMismatch => {
                MemberBindingRejectionReasonWire::PrincipalTransportMismatch
            }
        },
        reserved_at_ms: value.reserved_at().get(),
        requested_at_ms: value.binding().requested_at().get(),
        observed_at_ms: value.observed_at().get(),
        rejected_at_ms: value.rejected_at().get(),
        request_digest: hex(&value.request_digest()),
        receipt_digest: hex(&value.receipt_digest()),
    }
}

fn principal_wire(value: AuthenticatedPrincipal) -> MemberPrincipalWire {
    match value {
        AuthenticatedPrincipal::TrustedStdio => MemberPrincipalWire::TrustedStdio,
        AuthenticatedPrincipal::DirectToken(value) => MemberPrincipalWire::DirectToken {
            token_id: value.into_uuid(),
        },
        AuthenticatedPrincipal::OAuthGrant(value) => MemberPrincipalWire::OAuthGrant {
            grant_id: value.into_uuid(),
        },
    }
}

fn transport_wire(value: AgentTransportKind) -> MemberTransportWire {
    match value {
        AgentTransportKind::Retired => MemberTransportWire::Mcp,
        AgentTransportKind::Stdio => MemberTransportWire::Stdio,
        AgentTransportKind::StreamableHttp => MemberTransportWire::StreamableHttp,
        AgentTransportKind::Other => MemberTransportWire::Other,
    }
}

fn hex16(bytes: &[u8; 16]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(32);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}
fn digest32(value: &str) -> Result<[u8; 32], LaneError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(LaneError::InvalidIdentity);
    }
    let mut digest = [0_u8; 32];
    for (index, slot) in digest.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| LaneError::InvalidIdentity)?;
    }
    if digest == [0; 32] {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(digest)
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
