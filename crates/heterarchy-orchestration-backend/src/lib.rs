//! Axum transport for the Orchestration platform backend.
//!
//! The backend is intentionally a thin adapter over the protocol-neutral
//! Orchestration runtime. It does not own Forge repository execution authority.

pub mod app_support;
pub mod approval;
pub mod auth;
pub mod binding_rejection_trust;
mod catalog_execution;
pub mod client;
mod connections;
pub mod coordination;
pub mod coordinator_authority;
mod correlation;
mod departure;
pub mod effect_fact_http;
pub mod forge_commands;
pub mod forge_membership;
pub mod forge_read_api;
pub mod forge_source;
pub mod forge_source_config;
mod handoff;
mod node_assignment_admission;
pub mod node_chat;
pub mod node_team;
mod operations_import;
use app_support::runtime_error_response;
pub use coordinator_authority::{CoordinatorAuthority, CoordinatorAuthorityError};
mod event_routes;
mod ingress_limits;
pub mod internal_session;
pub mod leadership;
pub mod member_list;
mod membership;
mod participation;
mod profile_config;
mod profile_deletion;
mod profile_sessions;
mod provider_worker;
pub mod provisioning;
pub mod realtime;
pub mod session_process;
mod sessions;
pub mod teams;
pub mod token_cli;
pub mod wire;
pub mod work_credit;
mod workspace_chat;
mod workspace_project;

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::realtime::{RealtimeEvent, RealtimeEventKind, RealtimeHub};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use forge_domain::{AuthenticatedPrincipal, ForgeBackendId, OrchestrationIssuerId};
use heterarchy_orchestration_auth::{
    McpTokenBootstrapMode, McpTokenClient, McpTokenRuntime, McpTokenRuntimeError,
};
use heterarchy_orchestration_operations::{
    ConfigureAdmissionIssuerCommand, MAX_ADMISSION_TTL_MS, MIN_ADMISSION_TTL_MS,
    TrustedLegacyBindingRejectionDeployment,
};
use heterarchy_orchestration_runtime::{
    AgentProfileCreateCommand, AgentProfileCreateSummary, AgentRequestSummary,
    ApprovalPolicySummary, OrchestrationRuntime, RuntimeError, TeamPage, TeamSummary,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;
use wire::{AgentProfileCreateRequest, AgentProfileCreateResponse, AgentProfileResponse};

use catalog_execution::{CatalogExecution, CatalogLane};
const DEFAULT_TEAM_LIMIT: u32 = 64;

/// Select existing domain permission checks for a source-fenced mutation.
#[derive(Clone, Copy)]
enum ScopedMutationAuthority {
    Operator,
    WorkspaceMember,
}

impl ScopedMutationAuthority {
    fn authorize(
        self,
        runtime: &OrchestrationRuntime,
        workspace: Uuid,
        session: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<(), RuntimeError> {
        if matches!(self, Self::Operator) {
            runtime.require_operator(principal)?;
        }
        runtime.require_open_session(session, principal)?;
        if matches!(self, Self::WorkspaceMember) {
            runtime.require_workspace_read(workspace, Some(session), principal)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct BackendConfig {
    listen: SocketAddr,
    data_root: std::path::PathBuf,
    token_authority: TokenAuthorityBootstrap,
    operator_policy: heterarchy_orchestration_runtime::access::OperatorPolicy,
    coordinator_authority: Option<coordinator_authority::CoordinatorAuthority>,
    admission_issuer: Option<AdmissionIssuerBootstrap>,
    trusted_binding_rejection_legacy_deployments: Vec<TrustedLegacyBindingRejectionDeployment>,
}

/// Explicit host identity used to initialize the Forge admission issuer.
///
/// The values are opaque identities configured by the host that runs Forge;
/// this type never derives or reads them from bearer credentials or imports.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionIssuerBootstrap {
    issuer_id: OrchestrationIssuerId,
    backend_id: ForgeBackendId,
    admission_ttl_ms: i64,
}

impl AdmissionIssuerBootstrap {
    /// Validate one complete startup issuer configuration.
    ///
    /// # Errors
    /// Rejects nil identities and TTL values outside the durable admission
    /// envelope bounds.
    pub fn new(
        issuer_id: OrchestrationIssuerId,
        backend_id: ForgeBackendId,
        admission_ttl_ms: i64,
    ) -> Result<Self, BackendError> {
        if issuer_id.into_uuid().is_nil()
            || backend_id.into_uuid().is_nil()
            || !(MIN_ADMISSION_TTL_MS..=MAX_ADMISSION_TTL_MS).contains(&admission_ttl_ms)
        {
            return Err(BackendError::InvalidAdmissionIssuer);
        }
        Ok(Self {
            issuer_id,
            backend_id,
            admission_ttl_ms,
        })
    }

    pub const fn issuer_id(self) -> OrchestrationIssuerId {
        self.issuer_id
    }

    pub const fn backend_id(self) -> ForgeBackendId {
        self.backend_id
    }

    #[must_use]
    pub const fn admission_ttl_ms(self) -> i64 {
        self.admission_ttl_ms
    }
}

/// How the backend obtains the bearer token authority.
///
/// There is no "disabled" mode: an `/internal` surface without
/// authentication is never served. Callers either share the app-global
/// authority (the preserved Keychain pepper and token catalog identities)
/// or inject an already-started runtime (embedded composition and tests).
pub enum TokenAuthorityBootstrap {
    /// Start the app-global authority under the canonical bootstrap policy.
    AppGlobal(McpTokenBootstrapMode),
    /// Use a caller-owned, already-started token runtime.
    Runtime(McpTokenRuntime),
}

impl std::fmt::Debug for TokenAuthorityBootstrap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AppGlobal(mode) => formatter.debug_tuple("AppGlobal").field(mode).finish(),
            Self::Runtime(..) => formatter
                .debug_tuple("Runtime")
                .field(&"owned-worker")
                .finish(),
        }
    }
}

impl BackendConfig {
    /// Create one local-only backend configuration.
    ///
    /// The default token authority is the app-global authority requiring an
    /// already-initialized catalog; use [`Self::with_token_authority`] to
    /// change the bootstrap policy or inject a runtime.
    ///
    /// # Errors
    /// Rejects non-loopback listeners because authenticated remote ingress is a
    /// separate capability and is not part of this cutover slice.
    pub fn new(listen: SocketAddr, data_root: std::path::PathBuf) -> Result<Self, BackendError> {
        if !listen.ip().is_loopback() {
            return Err(BackendError::NonLoopbackListener);
        }
        Ok(Self {
            listen,
            data_root,
            token_authority: TokenAuthorityBootstrap::AppGlobal(
                McpTokenBootstrapMode::ExistingAuthority,
            ),
            operator_policy: heterarchy_orchestration_runtime::access::OperatorPolicy::default(),
            coordinator_authority: None,
            admission_issuer: None,
            trusted_binding_rejection_legacy_deployments: Vec::new(),
        })
    }

    /// Override how the bearer token authority is obtained.
    #[must_use]
    pub fn with_token_authority(mut self, token_authority: TokenAuthorityBootstrap) -> Self {
        self.token_authority = token_authority;
        self
    }

    /// Inject trusted host grants without creating another credential catalog.
    #[must_use]
    pub fn with_operator_policy(
        mut self,
        policy: heterarchy_orchestration_runtime::access::OperatorPolicy,
    ) -> Self {
        self.operator_policy = policy;
        self
    }

    /// Supply an existing exact Forge owner proof port. References select a
    /// source scope; authority is revalidated for every submitted intent.
    #[must_use]
    pub fn with_coordinator_authority(
        mut self,
        authority: coordinator_authority::CoordinatorAuthority,
    ) -> Self {
        self.coordinator_authority = Some(authority);
        self
    }

    /// Configure the explicit Forge admission issuer used during startup.
    #[must_use]
    pub fn with_admission_issuer(mut self, issuer: AdmissionIssuerBootstrap) -> Self {
        self.admission_issuer = Some(issuer);
        self
    }

    #[must_use]
    pub fn with_trusted_binding_rejection_legacy_deployments(
        mut self,
        deployments: Vec<TrustedLegacyBindingRejectionDeployment>,
    ) -> Self {
        self.trusted_binding_rejection_legacy_deployments = deployments;
        self
    }

    #[must_use]
    pub const fn admission_issuer(&self) -> Option<AdmissionIssuerBootstrap> {
        self.admission_issuer
    }

    #[must_use]
    pub const fn listen(&self) -> SocketAddr {
        self.listen
    }

    #[must_use]
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }
}

fn start_authorities(bootstrap: TokenAuthorityBootstrap) -> Result<McpTokenRuntime, BackendError> {
    match bootstrap {
        TokenAuthorityBootstrap::AppGlobal(mode) => {
            let paths = heterarchy_orchestration_auth::TokenAuthorityPaths::discover()
                .map_err(BackendError::TokenPaths)?;
            let tokens =
                McpTokenRuntime::start(&paths, mode).map_err(BackendError::TokenAuthority)?;
            Ok(tokens)
        }
        TokenAuthorityBootstrap::Runtime(tokens) => Ok(tokens),
    }
}

#[derive(Debug, Clone)]
struct BackendState {
    runtime: Arc<OrchestrationRuntime>,
    catalog_execution: CatalogExecution,
    realtime: Arc<RealtimeHub>,
    coordinator_authority: Option<coordinator_authority::CoordinatorAuthority>,
    forge_query_slots: Arc<Semaphore>,
}

impl BackendState {
    #[cfg(test)]
    fn new(runtime: OrchestrationRuntime, realtime: Arc<RealtimeHub>) -> Self {
        Self::new_shared(Arc::new(runtime), realtime)
    }

    fn new_shared(runtime: Arc<OrchestrationRuntime>, realtime: Arc<RealtimeHub>) -> Self {
        Self {
            runtime,
            catalog_execution: CatalogExecution::new(),
            realtime,
            coordinator_authority: None,
            forge_query_slots: Arc::new(Semaphore::new(4)),
        }
    }

    /// Runtime accessor for realtime projections wired into lane handlers.
    #[must_use]
    pub(crate) fn realtime(&self) -> RealtimeHub {
        self.realtime.as_ref().clone()
    }

    /// Publish one committed state change to the realtime observation
    /// surface. Publish happens strictly after the durable commit returned
    /// success; a rejected publish is logged and never fails the request
    /// (the canonical state is already durable).
    pub(crate) fn publish_state_changed(
        &self,
        kind: RealtimeEventKind,
        resource_id: Uuid,
        resource_revision: u64,
        workspace_id: Option<Uuid>,
    ) {
        let occurred_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or_default();
        if let Err(error) = self.realtime.publish(RealtimeEvent {
            kind,
            resource_id,
            resource_revision,
            workspace_id,
            occurred_at_ms,
        }) {
            warn!(?error, %resource_id, "realtime publish rejected after commit");
        }
    }

    /// Refresh observers after committed changes with incomplete version coverage.
    /// Canonical state remains committed even if its observation owner fails.
    pub(crate) fn require_snapshot_resync(&self) {
        if let Err(error) = self.realtime.require_snapshot_resync() {
            warn!(?error, "realtime resync rejected after commit");
        }
    }

    fn profile_creation_job(
        &self,
        command: AgentProfileCreateCommand,
    ) -> impl FnOnce(&OrchestrationRuntime) -> Result<AgentProfileCreateSummary, RuntimeError>
    + Send
    + 'static {
        let publisher = self.clone();
        move |runtime| {
            let outcome = runtime.create_agent_profile(command)?;
            if outcome.created {
                publisher.publish_state_changed(
                    RealtimeEventKind::AgentProfileStateChanged,
                    outcome.profile.profile_id,
                    u64::try_from(outcome.profile.revision).unwrap_or_default(),
                    None,
                );
            }
            Ok(outcome)
        }
    }

    async fn run_catalog<T, F>(&self, operation: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.run_owned(self.catalog_job(operation)).await
    }

    fn catalog_job<T, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = Result<T, ApiError>> + Send + 'static
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.catalog_job_in(CatalogLane::Ordinary, operation)
    }

    fn catalog_job_in<T, F>(
        &self,
        lane: CatalogLane,
        operation: F,
    ) -> impl Future<Output = Result<T, ApiError>> + Send + 'static
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        let runtime = Arc::clone(&self.runtime);
        let execution = self.catalog_execution.clone();
        async move {
            let blocking = execution.execution_slot(lane).await?;
            tokio::task::spawn_blocking(move || {
                let _blocking = blocking;
                operation(runtime.as_ref())
            })
            .await
            .map_err(|_| ApiError::Unavailable)
            .and_then(|result| result.map_err(ApiError::Runtime))
        }
    }

    async fn run_mutation<T, F>(&self, operation: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.run_mutation_in(CatalogLane::Ordinary, operation).await
    }

    async fn run_control_mutation<T, F>(&self, operation: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.run_mutation_in(CatalogLane::Control, operation).await
    }

    async fn run_mutation_in<T, F>(&self, lane: CatalogLane, operation: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        let runtime = Arc::clone(&self.runtime);
        let job = self.catalog_job_in(lane, operation);
        self.run_owned_in(lane, async move {
            runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            job.await
        })
        .await
    }

    async fn run_session_mutation<T, F>(
        &self,
        session_id: Uuid,
        principal: forge_domain::AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.run_session_mutation_in_scope(None, session_id, principal, operation)
            .await
    }

    async fn run_workspace_session_mutation<T, F>(
        &self,
        workspace_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.run_session_mutation_in_scope(Some(workspace_id), session_id, principal, operation)
            .await
    }

    async fn run_session_mutation_in_scope<T, F>(
        &self,
        workspace_id: Option<Uuid>,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        let runtime = Arc::clone(&self.runtime);
        let preflight = self.catalog_job(move |runtime| {
            runtime.require_open_session(session_id, principal)?;
            if let Some(workspace) = workspace_id {
                runtime.require_workspace_read(workspace, Some(session_id), principal)?;
            }
            Ok(())
        });
        let job = self.catalog_job(move |runtime| {
            // A session may close while Broker authority is being checked.
            runtime.require_open_session(session_id, principal)?;
            if let Some(workspace) = workspace_id {
                runtime.require_workspace_read(workspace, Some(session_id), principal)?;
            }
            operation(runtime)
        });
        self.run_owned(async move {
            preflight.await?;
            runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            job.await
        })
        .await
    }

    async fn run_agent_request_mutation<T, F>(
        &self,
        scope: Option<crate::wire::WorkspaceQueryScope>,
        command: heterarchy_orchestration_runtime::provisioning::AgentRequestSubmit,
        principal: AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(
                &OrchestrationRuntime,
                heterarchy_orchestration_runtime::provisioning::AgentRequestSubmit,
            ) -> Result<T, RuntimeError>
            + Send
            + 'static,
    {
        let state = self.clone();
        self.run_owned(async move {
            state
                .catalog_job(move |runtime| {
                    runtime.require_open_session(command.requester_session_id, principal)
                })
                .await?;
            // No SQL slot/guard may span this external proof: Forge can call
            // back into Orchestration's canonical Session authority.
            if let Some(scope) = &scope {
                state.workspace_scope_proof(scope.clone()).await?;
            }
            let _validated_before_admission =
                state.coordinator_proof(command.clone(), principal).await?;
            let mut command = command;
            if scope.is_some() {
                // Match native: creation time follows initial Source proof
                // and precedes Broker admission.
                command.created_at_ms = current_timestamp_millis()?;
            }
            state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            if let Some(scope) = &scope {
                state.workspace_scope_proof(scope.clone()).await?;
            }
            let proof = state.coordinator_proof(command.clone(), principal).await?;
            command.requester_agent_id = proof.agent_id().into_uuid();
            command.requester_session_id = proof.session_id().into_uuid();
            command.coordinator_revision = u64::try_from(proof.expected_revision().get())
                .map_err(|_| ApiError::CoordinatorUnavailable)?;
            state
                .catalog_job(move |runtime| {
                    runtime.require_open_session(command.requester_session_id, principal)?;
                    operation(runtime, command)
                })
                .await
        })
        .await
    }

    async fn coordinator_proof(
        &self,
        command: heterarchy_orchestration_runtime::provisioning::AgentRequestSubmit,
        principal: AuthenticatedPrincipal,
    ) -> Result<forge_domain::ActivityCoordinatorActor, ApiError> {
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::CoordinatorUnavailable)?;
        let permit = self
            .forge_query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::Unavailable)?;
        authority
            .check_coordinator(command, principal, permit)
            .await
            .map_err(|error| match error {
                coordinator_authority::CoordinatorAuthorityError::Denied => {
                    ApiError::Runtime(RuntimeError::Forbidden)
                }
                coordinator_authority::CoordinatorAuthorityError::Stale => {
                    ApiError::Runtime(RuntimeError::Conflict)
                }
                coordinator_authority::CoordinatorAuthorityError::Unavailable => {
                    ApiError::CoordinatorUnavailable
                }
            })
    }

    async fn run_operator_mutation<T, F>(
        &self,
        principal: forge_domain::AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.runtime
            .require_operator(principal)
            .map_err(ApiError::Runtime)?;
        self.run_mutation(operation).await
    }

    async fn run_operator_control_mutation<T, F>(
        &self,
        principal: forge_domain::AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.runtime
            .require_operator(principal)
            .map_err(ApiError::Runtime)?;
        self.run_control_mutation(operation).await
    }

    /// Run a scoped source query without trusting caller routing fields or
    /// cached references as permission. The complete bridge is host-tracked.
    async fn run_workspace_reference_query<T, F>(
        &self,
        scope: wire::leadership::WorkspaceQueryScope,
        session_id: Option<Uuid>,
        principal: AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime, i64) -> Result<T, RuntimeError> + Send + 'static,
    {
        let state = self.clone();
        self.run_owned(async move {
            let workspace_id = scope.workspace_id;
            state
                .catalog_job(move |runtime| {
                    runtime.require_workspace_read(workspace_id, session_id, principal)
                })
                .await?;
            state.workspace_scope_proof(scope).await?;
            let observed_at = current_timestamp_millis()?;
            state
                .catalog_job(move |runtime| {
                    runtime.require_workspace_read(workspace_id, session_id, principal)?;
                    operation(runtime, observed_at)
                })
                .await
        })
        .await
    }

    async fn activity_scope_proof(
        &self,
        scope: wire::WorkspaceQueryScope,
        activity: Uuid,
    ) -> Result<(), ApiError> {
        use crate::{CoordinatorAuthorityError, RuntimeError};
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::WorkspaceScopeUnavailable)?;
        let permit = self
            .forge_query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::Unavailable)?;
        authority
            .check_activity(scope, activity, permit)
            .await
            .map_err(|error| match error {
                CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
                CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
                CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
            })
    }

    async fn workspace_scope_proof(
        &self,
        scope: wire::WorkspaceQueryScope,
    ) -> Result<(), ApiError> {
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::WorkspaceScopeUnavailable)?;
        let permit = self
            .forge_query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::Unavailable)?;
        authority
            .check_workspace(scope, permit)
            .await
            .map_err(|error| match error {
                CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
                CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
                CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
            })
    }

    async fn workspace_candidate_proof(
        &self,
        scope: wire::WorkspaceQueryScope,
    ) -> Result<forge_control::ResolvedWorkspaceScope, ApiError> {
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::WorkspaceScopeUnavailable)?;
        let permit = self
            .forge_query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::Unavailable)?;
        authority
            .check_workspace_candidate(scope, permit)
            .await
            .map_err(|error| match error {
                CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
                CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
                CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
            })
    }

    /// Authorize the same scoped operator and owned logical Session again at
    /// commit admission. Source and Broker waits retain no catalog guard.
    async fn run_scoped_operator_mutation<T, F>(
        &self,
        scope: wire::WorkspaceQueryScope,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime, i64) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.run_workspace_reference_mutation(
            scope,
            session_id,
            principal,
            ScopedMutationAuthority::Operator,
            CatalogLane::Ordinary,
            operation,
        )
        .await
    }

    async fn run_workspace_reference_mutation<T, F>(
        &self,
        scope: wire::WorkspaceQueryScope,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        authority: ScopedMutationAuthority,
        lane: CatalogLane,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime, i64) -> Result<T, RuntimeError> + Send + 'static,
    {
        let state = self.clone();
        let workspace = scope.workspace_id;
        self.run_owned_in(lane, async move {
            state
                .catalog_job_in(lane, move |runtime| {
                    authority.authorize(runtime, workspace, session_id, principal)
                })
                .await?;
            state.workspace_scope_proof(scope.clone()).await?;
            state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            state.workspace_scope_proof(scope).await?;
            let now = current_timestamp_millis()?;
            state
                .catalog_job_in(lane, move |runtime| {
                    authority.authorize(runtime, workspace, session_id, principal)?;
                    operation(runtime, now)
                })
                .await
        })
        .await
    }

    async fn run_workspace_query<T, F>(
        &self,
        workspace_id: Uuid,
        session_id: Option<Uuid>,
        principal: forge_domain::AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        self.run_catalog(move |runtime| {
            runtime.require_workspace_read(workspace_id, session_id, principal)?;
            operation(runtime)
        })
        .await
    }

    async fn run_resource_query<T, F>(
        &self,
        workspace_id: Option<Uuid>,
        session_id: Option<Uuid>,
        principal: forge_domain::AuthenticatedPrincipal,
        operation: F,
    ) -> Result<Option<T>, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<Option<(Uuid, T)>, RuntimeError>
            + Send
            + 'static,
    {
        self.run_catalog(move |runtime| {
            match workspace_id {
                Some(workspace) => {
                    runtime.require_workspace_read(workspace, session_id, principal)?;
                }
                None => runtime.require_operator(principal)?,
            }
            Ok(operation(runtime)?.and_then(|(scope, value)| {
                if workspace_id.is_some_and(|expected| expected != scope) {
                    None
                } else {
                    Some(value)
                }
            }))
        })
        .await
    }

    /// Track the whole admitted operation, including queue and Broker waits.
    async fn run_owned<T, F>(&self, operation: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, ApiError>> + Send + 'static,
    {
        self.run_owned_in(CatalogLane::Ordinary, operation).await
    }

    async fn run_owned_in<T, F>(&self, lane: CatalogLane, operation: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, ApiError>> + Send + 'static,
    {
        let receiver = self.catalog_execution.spawn(lane, operation)?;
        receiver.await.map_err(|_| ApiError::Unavailable)?
    }

    fn stop_admission(&self) {
        self.catalog_execution.stop_admission();
        self.realtime.shutdown();
    }

    async fn drain(&self) {
        self.catalog_execution.drain().await;
    }
}

/// Start the standalone Orchestration backend and serve until cancellation.
///
/// # Errors
/// Returns runtime bootstrap, listener, or server failures.
pub async fn serve(
    config: BackendConfig,
    cancellation: CancellationToken,
) -> Result<(), BackendError> {
    let listen = config.listen();
    let data_root = config.data_root().to_path_buf();
    let admission_issuer = config.admission_issuer;
    let token_runtime = start_authorities(config.token_authority)?;
    let mut runtime =
        OrchestrationRuntime::open(&data_root)?.with_operator_policy(config.operator_policy);
    if !config
        .trusted_binding_rejection_legacy_deployments
        .is_empty()
    {
        runtime = runtime.with_trusted_binding_rejection_legacy_deployments(
            config.trusted_binding_rejection_legacy_deployments,
        );
    }
    if let Some(issuer) = admission_issuer {
        let now_ms = startup_timestamp_millis()?;
        runtime
            .ensure_admission_issuer(ConfigureAdmissionIssuerCommand {
                issuer: issuer.issuer_id(),
                backend_id: issuer.backend_id(),
                admission_ttl_ms: issuer.admission_ttl_ms(),
                now_ms,
            })
            .map(|_| ())?;
    }
    let result = serve_with_runtime(
        Arc::new(runtime),
        listen,
        token_runtime.client(),
        config.coordinator_authority,
        cancellation,
    )
    .await;
    // Worker owners outlive every admitted authentication/catalog operation.
    let _owned_token_worker = token_runtime;
    result
}

/// Serve one host's existing Runtime without reacquiring its root or replacing
/// its immutable operator policy. The host owns the supplied authority workers
/// and must stop/join all transports before shutting those workers down.
/// Shared native ingress remains a legacy path until its backend parity is proven.
///
/// # Errors
/// Rejects non-loopback listeners and returns bind/server or owned-job failures.
pub async fn serve_with_runtime(
    runtime: Arc<OrchestrationRuntime>,
    listen: SocketAddr,
    token_client: McpTokenClient,
    coordinator_authority: Option<coordinator_authority::CoordinatorAuthority>,
    cancellation: CancellationToken,
) -> Result<(), BackendError> {
    if !listen.ip().is_loopback() {
        return Err(BackendError::NonLoopbackListener);
    }
    let destination_id = runtime.destination_id();
    let mut state = BackendState::new_shared(runtime, Arc::new(RealtimeHub::new()));
    state.coordinator_authority = coordinator_authority;
    let enrollments = state
        .catalog_job(OrchestrationRuntime::workspace_project_enrollments)
        .await
        .map_err(map_startup_catalog_error)?;
    if let Some(authority) = &state.coordinator_authority {
        authority
            .hydrate_workspace_project_enrollments(&enrollments)
            .map_err(|_| BackendError::WorkspaceProjectScopeProjection)?;
    }
    let authorizer = auth::BearerAuthorizer::new(token_client);
    let router = router(state.clone(), authorizer.clone());
    let listener = TcpListener::bind(listen).await?;
    let address = listener.local_addr()?;
    info!(
        %address,
        %destination_id,
        "heterarchy orchestration backend ready"
    );

    let shutdown_state = state.clone();
    let shutdown_auth = authorizer.clone();
    let shutdown = async move {
        cancellation.cancelled_owned().await;
        shutdown_state.stop_admission();
        shutdown_auth.stop_admission();
    };
    let serve_result = axum::serve(connections::BoundedListener::new(listener), router)
        .with_graceful_shutdown(shutdown)
        .await;
    state.stop_admission();
    state.drain().await;
    authorizer.stop_admission();
    authorizer
        .drain()
        .await
        .map_err(|_| BackendError::AuthJobFailed)?;
    serve_result.map_err(BackendError::Serve)
}

fn router(state: BackendState, authorizer: auth::BearerAuthorizer) -> Router {
    // Only the `/internal` surface sits behind the bearer layer; `/health`
    // and `/ready` stay open for supervision. `route_layer` covers exactly
    // the routes registered on the protected router above it.
    let protected = Router::new()
        .merge(operations_import::routes())
        .route("/internal/v1/workspaces/{workspace_id}/teams", get(teams))
        .route(
            "/internal/v1/agent-requests/{request_id}",
            get(agent_request),
        )
        .route("/internal/v1/events", get(event_routes::global))
        .route(
            "/internal/v1/workspaces/{workspace_id}/events",
            get(event_routes::workspace),
        )
        .route(
            "/internal/v1/agent-profiles",
            post(agent_profile_create).get(agent_profiles_list),
        )
        .route(
            "/internal/v1/agent-profiles/{profile_id}",
            get(agent_profile_get),
        )
        .merge(profile_config::routes())
        .merge(profile_sessions::routes())
        .merge(profile_deletion::routes())
        .merge(approval::routes())
        .merge(leadership::routes())
        .merge(provisioning::routes())
        .merge(provider_worker::routes())
        .merge(teams::routes())
        .merge(participation::routes())
        .merge(membership::routes())
        .merge(member_list::routes())
        .merge(departure::routes())
        .merge(coordination::routes())
        .merge(node_assignment_admission::routes())
        .merge(node_team::routes())
        .merge(node_chat::routes())
        .merge(handoff::routes())
        .merge(sessions::routes())
        .merge(work_credit::routes())
        .merge(workspace_chat::routes())
        .merge(workspace_project::routes())
        .merge(forge_read_api::routes())
        .route_layer(axum::middleware::from_fn_with_state(
            ingress_limits::RequestAdmission::new(),
            ingress_limits::middleware,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            authorizer,
            auth::require_bearer,
        ));
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .merge(protected)
        .layer(axum::middleware::from_fn(correlation::middleware))
        .with_state(state)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    service: &'static str,
    status: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "heterarchy-orchestration-backend",
        status: "alive",
    })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadinessResponse {
    service: &'static str,
    status: &'static str,
    readiness_scope: &'static str,
    forge_source: &'static str,
    coordinator_actor_proof: &'static str,
}

async fn ready(State(state): State<BackendState>) -> Result<Json<ReadinessResponse>, ApiError> {
    state.run_catalog(OrchestrationRuntime::probe).await?;
    let authority = state.coordinator_authority.as_ref();
    Ok(Json(ReadinessResponse {
        service: "heterarchy-orchestration-backend",
        status: "ready",
        readiness_scope: "local_runtime",
        forge_source: if authority
            .and_then(CoordinatorAuthority::http_source)
            .is_some()
        {
            "configured_not_probed"
        } else {
            "unconfigured"
        },
        coordinator_actor_proof: if authority.is_some() {
            "supported_not_probed"
        } else {
            "unavailable"
        },
    }))
}

async fn approval_policy(
    State(state): State<BackendState>,
    AxumPath(workspace_id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<ApprovalPolicyResponse>, ApiError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace_id = canonical_uuid(&workspace_id)?;
    let policy = state
        .run_workspace_query(workspace_id, session, principal, move |runtime| {
            runtime.approval_policy_summary(workspace_id)
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(policy.into()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TeamQuery {
    limit: Option<u32>,
}

async fn teams(
    State(state): State<BackendState>,
    AxumPath(workspace_id): AxumPath<String>,
    Query(query): Query<TeamQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<TeamPageResponse>, ApiError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace_id = canonical_uuid(&workspace_id)?;
    let limit = query.limit.unwrap_or(DEFAULT_TEAM_LIMIT);
    let page = state
        .run_workspace_query(workspace_id, session, principal, move |runtime| {
            runtime.teams(workspace_id, limit)
        })
        .await?;
    Ok(Json(page.into()))
}

async fn agent_request(
    State(state): State<BackendState>,
    AxumPath(request_id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<AgentRequestResponse>, ApiError> {
    let request_id = canonical_uuid(&request_id)?;
    let workspace = crate::app_support::workspace_selector(&headers)?;
    let session = crate::app_support::session_selector(&headers)?;
    let request = state
        .run_resource_query(workspace, session, principal, move |runtime| {
            runtime
                .agent_request(request_id)?
                .map(|request| {
                    let scope = Uuid::parse_str(&request.workspace_id).map_err(|_| {
                        RuntimeError::persistence(
                            heterarchy_orchestration_operations::OperationsError::Corrupt(
                                "request Workspace identity",
                            ),
                        )
                    })?;
                    Ok((scope, request))
                })
                .transpose()
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(request.into()))
}

async fn agent_profile_create(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<AgentProfileCreateRequest>,
) -> Result<(StatusCode, Json<AgentProfileCreateResponse>), ApiError> {
    let request_id = canonical_uuid(&request.request_id)?;
    let created_at_ms = current_timestamp_millis()?;
    let job = state.profile_creation_job(AgentProfileCreateCommand {
        request_id,
        stable_name: request.stable_name,
        display_name: request.display_name,
        preferred_model: request.preferred_model,
        preferred_reasoning: request.preferred_reasoning,
        base_capabilities: request.base_capabilities,
        created_at_ms,
    });
    let outcome = state.run_operator_mutation(principal, job).await?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(outcome.into())))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProfileListQuery {
    limit: Option<u32>,
}

async fn agent_profiles_list(
    State(state): State<BackendState>,
    Query(query): Query<ProfileListQuery>,
) -> Result<Json<Vec<AgentProfileResponse>>, ApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_PROFILE_LIST_LIMIT);
    let profiles = state
        .run_catalog(move |runtime| runtime.agent_profiles(limit))
        .await?;
    Ok(Json(
        profiles
            .iter()
            .map(|summary| crate::AgentProfileResponse::from(summary.clone()))
            .collect(),
    ))
}

const DEFAULT_PROFILE_LIST_LIMIT: u32 = 64;

async fn agent_profile_get(
    State(state): State<BackendState>,
    AxumPath(profile_id): AxumPath<String>,
) -> Result<Json<AgentProfileResponse>, ApiError> {
    let profile_id = canonical_uuid(&profile_id)?;
    let profile = state
        .run_catalog(move |runtime| runtime.agent_profile(profile_id))
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(profile.into()))
}

fn canonical_uuid(value: &str) -> Result<Uuid, ApiError> {
    let parsed = Uuid::parse_str(value).map_err(|_| ApiError::InvalidIdentity)?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(parsed)
}

fn current_timestamp_millis() -> Result<i64, ApiError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ApiError::Unavailable)?;
    i64::try_from(elapsed.as_millis()).map_err(|_| ApiError::Unavailable)
}

fn startup_timestamp_millis() -> Result<i64, BackendError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BackendError::StartupClock)?;
    i64::try_from(elapsed.as_millis()).map_err(|_| BackendError::StartupClock)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApprovalPolicyResponse {
    workspace_id: Uuid,
    enabled: bool,
    revision: u64,
    auto_scope_count: usize,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl From<ApprovalPolicySummary> for ApprovalPolicyResponse {
    fn from(value: ApprovalPolicySummary) -> Self {
        Self {
            workspace_id: value.workspace_id,
            enabled: value.enabled,
            revision: value.revision,
            auto_scope_count: value.auto_scope_count,
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TeamResponse {
    team_id: Uuid,
    workspace_id: Uuid,
    name: String,
    purpose: String,
    state: &'static str,
    revision: u64,
    created_at_ms: i64,
    updated_at_ms: i64,
    formed_at_ms: Option<i64>,
}

impl From<TeamSummary> for TeamResponse {
    fn from(value: TeamSummary) -> Self {
        Self {
            team_id: value.team_id,
            workspace_id: value.workspace_id,
            name: value.name,
            purpose: value.purpose,
            state: value.state,
            revision: value.revision,
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
            formed_at_ms: value.formed_at_ms,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TeamPageResponse {
    items: Vec<TeamResponse>,
    truncated: bool,
}

impl From<TeamPage> for TeamPageResponse {
    fn from(value: TeamPage) -> Self {
        Self {
            items: value.items.into_iter().map(TeamResponse::from).collect(),
            truncated: value.truncated,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentRequestResponse {
    request_id: Uuid,
    workspace_id: String,
    activity_id: String,
    objective: String,
    capability: String,
    requested_count: u8,
    reason: String,
    state: &'static str,
    created_at_ms: i64,
    revision: u64,
}

impl From<AgentRequestSummary> for AgentRequestResponse {
    fn from(value: AgentRequestSummary) -> Self {
        Self {
            request_id: value.request_id,
            workspace_id: value.workspace_id,
            activity_id: value.activity_id,
            objective: value.objective,
            capability: value.capability,
            requested_count: value.requested_count,
            reason: value.reason,
            state: value.state,
            created_at_ms: value.created_at_ms,
            revision: value.revision,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorResponse {
    code: &'static str,
    message: &'static str,
    retryable: bool,
}

#[derive(Debug)]
enum ApiError {
    InvalidIdentity,
    NotFound,
    Capacity,
    Unavailable,
    CoordinatorUnavailable,
    WorkspaceScopeUnavailable,
    SourceBindingUnconfirmed,
    Runtime(RuntimeError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = match &self {
            Self::InvalidIdentity => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    code: "INVALID_IDENTITY",
                    message: "The supplied identity is invalid.",
                    retryable: false,
                },
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                ErrorResponse {
                    code: "NOT_FOUND",
                    message: "The requested Orchestration resource was not found.",
                    retryable: false,
                },
            ),
            Self::Capacity => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse {
                    code: "ORCHESTRATION_CAPACITY",
                    message: "The Orchestration backend has reached its bounded work capacity.",
                    retryable: true,
                },
            ),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse {
                    code: "ORCHESTRATION_UNAVAILABLE",
                    message: "The Orchestration runtime is temporarily unavailable.",
                    retryable: true,
                },
            ),
            Self::CoordinatorUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse {
                    code: "COORDINATOR_AUTHORITY_UNAVAILABLE",
                    message: "The exact Forge Coordinator authority is unavailable; no AgentRequest was admitted.",
                    retryable: true,
                },
            ),
            Self::WorkspaceScopeUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse {
                    code: "WORKSPACE_SCOPE_UNAVAILABLE",
                    message: "The exact Forge Workspace scope authority is unavailable.",
                    retryable: true,
                },
            ),
            Self::SourceBindingUnconfirmed => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse {
                    code: "COMMITTED_BUT_UNCONFIRMED",
                    message: "Read the exact registration and source receipt before resuming.",
                    retryable: false,
                },
            ),
            Self::Runtime(error) => runtime_error_response(error),
        };
        if status.is_server_error() {
            warn!(error = ?self, "orchestration backend request failed");
        }
        (status, Json(body)).into_response()
    }
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("an owned authentication query failed to join")]
    AuthJobFailed,
    #[error("remote Orchestration listener is not enabled")]
    NonLoopbackListener,
    #[error("token authority paths discovery failed")]
    TokenPaths(#[source] heterarchy_orchestration_auth::TokenPathError),
    #[error("bearer token authority bootstrap failed")]
    TokenAuthority(#[source] McpTokenRuntimeError),
    #[error("admission issuer startup configuration is invalid")]
    InvalidAdmissionIssuer,
    #[error("Orchestration startup clock is unavailable")]
    StartupClock,
    #[error("Orchestration Workspace Project enrollment catalog could not be loaded")]
    WorkspaceProjectScopeCatalog,
    #[error("Orchestration Workspace Project enrollment projection is invalid")]
    WorkspaceProjectScopeProjection,
    #[error("Orchestration runtime bootstrap failed")]
    Runtime(#[from] RuntimeError),
    #[error("Orchestration listener failed")]
    Io(#[from] std::io::Error),
    #[error("Orchestration server failed")]
    Serve(std::io::Error),
}

fn map_startup_catalog_error(error: ApiError) -> BackendError {
    match error {
        ApiError::Runtime(error) => BackendError::Runtime(error),
        ApiError::WorkspaceScopeUnavailable | ApiError::Unavailable => {
            BackendError::WorkspaceProjectScopeCatalog
        }
        ApiError::InvalidIdentity
        | ApiError::NotFound
        | ApiError::Capacity
        | ApiError::CoordinatorUnavailable
        | ApiError::SourceBindingUnconfirmed => BackendError::WorkspaceProjectScopeCatalog,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog_execution::CATALOG_ADMISSION_CAPACITY;
    use forge_domain::{ForgeBackendId, OrchestrationIssuerId};
    use heterarchy_orchestration_auth::{
        McpTokenCatalog, McpTokenRuntime, McpTokenService, McpTokenWorker,
    };
    use heterarchy_orchestration_runtime::RuntimeError;

    fn test_token_runtime(
        label: &str,
    ) -> Result<(McpTokenRuntime, std::path::PathBuf), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "backend-admission-issuer-token-{label}-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root)?;
        let catalog = McpTokenCatalog::open(root.join("mcp-tokens.sqlite3"))?;
        let (client, worker) = McpTokenWorker::start(McpTokenService::new([0x5a; 32], catalog))?;
        Ok((McpTokenRuntime::from_parts(client, worker), root))
    }

    async fn serve_once(
        root: &std::path::Path,
        issuer: AdmissionIssuerBootstrap,
        token_runtime: McpTokenRuntime,
    ) -> Result<(), BackendError> {
        let config = BackendConfig::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            root.to_path_buf(),
        )?
        .with_admission_issuer(issuer)
        .with_token_authority(TokenAuthorityBootstrap::Runtime(token_runtime));
        let cancellation = CancellationToken::new();
        let server = tokio::spawn(serve(config, cancellation.clone()));
        tokio::task::yield_now().await;
        cancellation.cancel();
        server.await.map_err(|_| BackendError::AuthJobFailed)??;
        Ok(())
    }

    fn stored_issuer(
        root: &std::path::Path,
    ) -> Result<heterarchy_orchestration_operations::AdmissionIssuerConfig, RuntimeError> {
        let catalog = OrchestrationRuntime::open_existing_operations_read_only(root)?;
        catalog
            .admission_issuer_config()?
            .ok_or(RuntimeError::Conflict)
    }

    #[test]
    fn backend_config_defaults_to_no_admission_issuer_bootstrap() -> Result<(), BackendError> {
        let config = BackendConfig::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            std::env::temp_dir(),
        )?;
        assert!(config.admission_issuer().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn configured_startup_replays_exact_binding_and_releases_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("backend-admission-issuer-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let issuer = AdmissionIssuerBootstrap::new(
            OrchestrationIssuerId::from_uuid(Uuid::from_u128(0xA101)),
            ForgeBackendId::from_uuid(Uuid::from_u128(0xA102)),
            300_000,
        )?;

        let (token, token_root) = test_token_runtime("first")?;
        serve_once(&root, issuer, token).await?;
        std::fs::remove_dir_all(token_root)?;
        let first = stored_issuer(&root)?;

        let (token, token_root) = test_token_runtime("replay")?;
        serve_once(&root, issuer, token).await?;
        std::fs::remove_dir_all(token_root)?;
        let replay = stored_issuer(&root)?;
        assert_eq!(replay, first);
        assert_eq!(replay.issuer(), issuer.issuer_id());
        assert_eq!(replay.backend_id(), issuer.backend_id());
        assert_eq!(replay.admission_ttl_ms(), issuer.admission_ttl_ms());

        let reopened = OrchestrationRuntime::open(&root)?;
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn conflicting_startup_fails_closed_and_releases_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "backend-admission-issuer-conflict-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root)?;
        let first = AdmissionIssuerBootstrap::new(
            OrchestrationIssuerId::from_uuid(Uuid::from_u128(0xA201)),
            ForgeBackendId::from_uuid(Uuid::from_u128(0xA202)),
            300_000,
        )?;
        let second = AdmissionIssuerBootstrap::new(
            OrchestrationIssuerId::from_uuid(Uuid::from_u128(0xA203)),
            ForgeBackendId::from_uuid(Uuid::from_u128(0xA204)),
            300_000,
        )?;

        let (token, token_root) = test_token_runtime("conflict-first")?;
        serve_once(&root, first, token).await?;
        std::fs::remove_dir_all(token_root)?;
        let original = stored_issuer(&root)?;

        let (token, token_root) = test_token_runtime("conflict-second")?;
        let result = serve_once(&root, second, token).await;
        std::fs::remove_dir_all(token_root)?;
        assert!(matches!(
            result,
            Err(BackendError::Runtime(RuntimeError::Conflict))
        ));
        assert_eq!(stored_issuer(&root)?, original);

        let reopened = OrchestrationRuntime::open(&root)?;
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn readiness_without_source_reports_local_runtime_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("readiness-scope-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let state = BackendState::new(runtime, Arc::new(RealtimeHub::new()));
        let Json(response) = ready(State(state.clone()))
            .await
            .map_err(|error| format!("readiness probe failed: {error:?}"))?;
        assert_eq!(response.status, "ready");
        assert_eq!(response.readiness_scope, "local_runtime");
        assert_eq!(response.forge_source, "unconfigured");
        assert_eq!(response.coordinator_actor_proof, "unavailable");
        state.catalog_execution.stop_admission();
        state.catalog_execution.drain().await;
        drop(state);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn owned_session_and_body_ids_cannot_substitute_for_coordinator_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        use heterarchy_orchestration_runtime::provisioning::AgentRequestSubmit;
        let root = std::env::temp_dir().join(format!("coordinator-authority-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let principal = AuthenticatedPrincipal::DirectToken(
            forge_domain::DirectTokenId::from_uuid(Uuid::now_v7()),
        );
        let session = runtime
            .open_session(Uuid::now_v7(), principal, 1)?
            .session
            .session_id;
        let state = BackendState::new(runtime, Arc::new(RealtimeHub::new()));
        let request_id = Uuid::now_v7();
        let command = AgentRequestSubmit {
            workspace_id: Uuid::now_v7(),
            activity_id: Uuid::now_v7(),
            requester_agent_id: Uuid::now_v7(),
            requester_session_id: session,
            coordinator_revision: 1,
            request_id,
            objective: "forged coordinator".to_owned(),
            capability: "chat".to_owned(),
            count: 1,
            effect_scope: heterarchy_orchestration_operations::AgentRequestEffectScope::SourceWrite,
            independence: heterarchy_orchestration_operations::AgentRequestIndependence::None,
            reason: "an owned Session is not Coordinator authority".to_owned(),
            created_at_ms: 2,
        };
        let result = state
            .run_agent_request_mutation(None, command, principal, move |runtime, command| {
                runtime
                    .submit_agent_request(command)
                    .map_err(RuntimeError::from)
            })
            .await;
        assert!(matches!(result, Err(ApiError::CoordinatorUnavailable)));
        assert_eq!(state.runtime.agent_request(request_id)?, None);
        assert_eq!(state.runtime.agent_request_resolution(request_id)?, None);
        assert!(state.runtime.agent_profiles(64)?.is_empty());
        assert!(!root.join("broker-command-session.json").exists());
        state.stop_admission();
        state.drain().await;
        drop(state);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn scoped_query_without_forge_authority_never_runs_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("leadership-scope-unavailable-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let principal = AuthenticatedPrincipal::DirectToken(
            forge_domain::DirectTokenId::from_uuid(Uuid::now_v7()),
        );
        let runtime = OrchestrationRuntime::open(&root)?.with_operator_policy(
            heterarchy_orchestration_runtime::access::OperatorPolicy::new(vec![principal])?,
        );
        let state = BackendState::new(runtime, Arc::new(RealtimeHub::new()));
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let projection_ran = ran.clone();
        let result = state
            .run_workspace_reference_query(
                wire::leadership::WorkspaceQueryScope {
                    workspace_id: Uuid::now_v7(),
                    location_id: Uuid::now_v7(),
                    location_revision: 1,
                    workspace_revision: 1,
                    locator: "explicit source reference".to_owned(),
                },
                None,
                principal,
                move |_, _| {
                    projection_ran.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                },
            )
            .await;
        assert!(matches!(result, Err(ApiError::WorkspaceScopeUnavailable)));
        assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(state.realtime.snapshot().head_sequence, 0);
        state.stop_admission();
        state.drain().await;
        drop(state);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn owned_session_cannot_submit_approval_into_an_unjoined_workspace()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("approval-scope-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let principal = AuthenticatedPrincipal::DirectToken(
            forge_domain::DirectTokenId::from_uuid(Uuid::now_v7()),
        );
        let session = runtime
            .open_session(Uuid::now_v7(), principal, 1)?
            .session
            .session_id;
        let state = BackendState::new(runtime, Arc::new(RealtimeHub::new()));
        let workspace = Uuid::now_v7();
        let command = heterarchy_orchestration_operations::SubmitApprovalCommand {
            request_id: forge_domain::ApprovalRequestId::from_uuid(Uuid::now_v7()),
            workspace_id: forge_domain::WorkspaceId::from_uuid(workspace),
            action: forge_domain::ExecutionActionClass::DirectStructured,
            request_digest: [4; 32],
            principal,
            session_id: session,
            requested_permissions: forge_domain::ExecutionPermissions::new(true, false, false),
            ttl_ms: 1000,
            now_ms: 2,
        };
        let result = state
            .run_workspace_session_mutation(workspace, session, principal, move |runtime| {
                runtime.submit_approval(command)
            })
            .await;
        assert!(matches!(
            result,
            Err(ApiError::Runtime(RuntimeError::Forbidden))
        ));
        assert!(
            state
                .runtime
                .approval_requests(heterarchy_orchestration_operations::ApprovalRequestQuery {
                    workspace_id: forge_domain::WorkspaceId::from_uuid(workspace),
                    state: None,
                    limit: 64
                })?
                .is_empty()
        );
        assert_eq!(state.realtime.snapshot().head_sequence, 0);
        assert!(!root.join("broker-command-session.json").exists());
        state.stop_admission();
        state.drain().await;
        drop(state);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn catalog_control_admission_survives_ordinary_saturation()
    -> Result<(), Box<dyn std::error::Error>> {
        let execution = CatalogExecution::new();
        let release = CancellationToken::new();
        for _ in 0..CATALOG_ADMISSION_CAPACITY {
            let release = release.clone();
            let _observer = execution
                .spawn(CatalogLane::Ordinary, async move {
                    release.cancelled().await;
                    Ok(())
                })
                .map_err(|_| "ordinary capacity closed early")?;
        }
        let control = execution.spawn(CatalogLane::Control, async { Ok(()) });
        release.cancel();
        execution.stop_admission();
        execution.drain().await;
        let receiver = control.map_err(|_| "ordinary backlog rejected reserved control work")?;
        receiver.await?.map_err(|_| "control work failed")?;
        Ok(())
    }

    #[test]
    fn catalog_admission_rejects_work_above_the_bounded_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let execution = CatalogExecution::new();
        let mut permits = Vec::with_capacity(CATALOG_ADMISSION_CAPACITY);
        for _ in 0..CATALOG_ADMISSION_CAPACITY {
            let Ok(permit) = execution.try_admit(CatalogLane::Ordinary) else {
                return Err("catalog admission closed before its configured capacity".into());
            };
            permits.push(permit);
        }

        assert!(matches!(
            execution.try_admit(CatalogLane::Ordinary),
            Err(ApiError::Capacity)
        ));
        drop(permits);
        Ok(())
    }

    #[tokio::test]
    async fn profile_creation_replay_does_not_publish_a_second_change()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("profile-events-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let state = BackendState::new(
            OrchestrationRuntime::open(&root)?,
            Arc::new(RealtimeHub::new()),
        );
        let nonce = Uuid::now_v7();
        let command = || AgentProfileCreateCommand {
            request_id: nonce,
            stable_name: "profile-event-replay".to_owned(),
            display_name: "Profile event".to_owned(),
            preferred_model: None,
            preferred_reasoning: None,
            base_capabilities: Vec::new(),
            created_at_ms: 1,
        };
        let created = state
            .run_catalog(state.profile_creation_job(command()))
            .await
            .map_err(|_| "create failed")?;
        let replayed = state
            .run_catalog(state.profile_creation_job(command()))
            .await
            .map_err(|_| "replay failed")?;
        assert!(created.created);
        assert!(!replayed.created);
        assert_eq!(created.profile.profile_id, replayed.profile.profile_id);
        assert_eq!(state.realtime.snapshot().head_sequence, 1);
        state.stop_admission();
        state.drain().await;
        drop(state);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn catalog_admission_rejects_new_work_after_shutdown_starts() {
        let execution = CatalogExecution::new();
        execution.stop_admission();

        assert!(matches!(
            execution.try_admit(CatalogLane::Ordinary),
            Err(ApiError::Unavailable)
        ));
        assert!(matches!(
            execution.try_admit(CatalogLane::Control),
            Err(ApiError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn admitted_catalog_work_is_tracked_while_queued_and_drained_after_disconnect()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("catalog-drain-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let state = BackendState::new(
            OrchestrationRuntime::open(&root)?,
            Arc::new(RealtimeHub::new()),
        );
        let blocked = state
            .catalog_execution
            .execution_slot(CatalogLane::Ordinary)
            .await
            .map_err(|_| "slot closed")?;
        let caller_state = state.clone();
        let command = AgentProfileCreateCommand {
            request_id: Uuid::now_v7(),
            stable_name: "disconnected-profile".to_owned(),
            display_name: "Disconnected Profile".to_owned(),
            preferred_model: None,
            preferred_reasoning: None,
            base_capabilities: Vec::new(),
            created_at_ms: 1,
        };
        let caller = tokio::spawn(async move {
            caller_state
                .run_catalog(caller_state.profile_creation_job(command))
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while state.catalog_execution.admitted_slots.available_permits()
                == CATALOG_ADMISSION_CAPACITY
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(state.catalog_execution.tasks.len(), 1);
        caller.abort();
        assert!(caller.await.is_err());
        state.stop_admission();
        let drain_state = state.clone();
        let drain = tokio::spawn(async move { drain_state.drain().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        drop(blocked);
        tokio::time::timeout(std::time::Duration::from_secs(2), drain).await??;
        assert_eq!(state.runtime.agent_profiles(64)?.len(), 1);
        assert_eq!(
            state.realtime.snapshot().head_sequence,
            1,
            "a disconnected observer lost the committed Profile event"
        );
        assert!(state.catalog_execution.tasks.is_empty());
        drop(state);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
