//! Bounded Forge-backed Coordinator authority for Orchestration mutations.
//!
//! The host selects one control or HTTP transport and bounded Workspace route
//! references. This adapter opens no Forge catalog. Fresh source observations
//! and internal actor resolution remain separate from Orchestration caller
//! authorization; an HTTP observation never manufactures a Forge actor.

mod departure;
pub(crate) use departure::HandoffTargetSelector;
mod home_transfer;
mod membership;
#[cfg_attr(not(test), allow(dead_code))]
mod swarm_bridge;
pub(crate) use home_transfer::ConfiguredHomeTransferSource;
#[allow(unused_imports)]
pub(crate) use swarm_bridge::{
    ForgeAutonomyOutboxPageInput, ForgeSwarmCaptureInput, ForgeSwarmConvergeBridgeOutcome,
    ForgeSwarmConvergeInput, ForgeSwarmHybridMaterializeBridgeOutcome,
    ForgeSwarmHybridMaterializeInput, ForgeSwarmProvisionInput, ForgeSwarmPurgeInput,
    ForgeSwarmSettleBridgeOutcome, ForgeSwarmSettleInput, ForgeSwarmWorkspaceProvisioned,
};

use forge_backend::{ForgeHttpClientError, ForgeHttpServiceCode};
use forge_control::{
    AgentOperationsExportRequest, AgentOperationsFreezeReadRequest, ControlClientError,
    CoordinatorActorRequest, ForgeControlClient,
};
use forge_domain::{ActivityCoordinatorActor, ActivityId, AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_operations::{
    AgentOperationsExportCursor, AgentOperationsExportPage, AgentOperationsFreeze,
    MAX_WORKSPACE_AUTHORITY_SCOPES,
};
use heterarchy_orchestration_runtime::provisioning::AgentRequestSubmit;
use thiserror::Error;

use forge_control::{ControlErrorCode, ResolvedWorkspaceScope};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CoordinatorAuthorityError {
    #[error("Forge Coordinator authority denied the request")]
    Denied,
    #[error("Forge Coordinator authority observed stale scope or revision")]
    Stale,
    #[error("Forge Coordinator authority is unavailable")]
    Unavailable,
}

/// Host-composed Forge Coordinator authority.
///
/// The stored scopes are trusted host references used to select the exact
/// Workspace request sent to Forge. They are not permission caches or bearer
/// credentials; every resolution is delegated to the supplied Forge control
/// client.
#[derive(Clone)]
pub struct CoordinatorAuthority {
    transport: SourceTransport,
    scopes: Vec<ResolvedWorkspaceScope>,
    scope_registry: Option<
        heterarchy_orchestration_runtime::CatalogHandle<
            heterarchy_orchestration_operations::OperationsCatalog,
        >,
    >,
    effect_facts: Option<forge_backend::ForgeHttpClient>,
    home_transfer_source: Option<ConfiguredHomeTransferSource>,
}

impl std::fmt::Debug for CoordinatorAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoordinatorAuthority")
            .field("transport", &self.transport)
            .field("scopes", &self.scopes)
            .field("scope_registry_attached", &self.scope_registry.is_some())
            .field("effect_facts", &self.effect_facts)
            .field("home_transfer_source", &self.home_transfer_source)
            .finish()
    }
}

#[derive(Debug, Clone)]
enum SourceTransport {
    Control { client: ForgeControlClient },
    Http(crate::forge_source::ForgeSourceHttpClient),
}

impl CoordinatorAuthority {
    /// Resolve the exact current Coordinator and node observation from one Source snapshot lane.
    ///
    /// The caller separately proves the Workspace before invoking this method.
    /// No Orchestration catalog guard or Broker authority is held while Source
    /// is queried.
    pub(crate) async fn node_assignment_admission_observation(
        &self,
        scope: crate::wire::WorkspaceQueryScope,
        activity_id: uuid::Uuid,
        target_agent_id: uuid::Uuid,
        principal: AuthenticatedPrincipal,
        node_scope: heterarchy_orchestration_operations::NodeTeamBindingScope,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<
        (
            ActivityCoordinatorActor,
            crate::forge_source::node_assignment::NodeAssignmentObservation,
            crate::forge_source::node_assignment::NodeAssignmentTargetAgentObservation,
        ),
        CoordinatorAuthorityError,
    > {
        if activity_id.is_nil()
            || target_agent_id.is_nil()
            || node_scope.workspace_id().into_uuid() != scope.workspace_id
            || node_scope.activity_id().into_uuid() != activity_id
        {
            return Err(CoordinatorAuthorityError::Stale);
        }
        let requested = heterarchy_orchestration_operations::DagNodeEvidenceScope::new(
            node_scope.goal_id(),
            node_scope.dag_revision(),
            node_scope.node_id(),
        );
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let trusted = self.trusted_scope(&scope)?;
                let actor = client
                    .coordinator_actor(&trusted, activity_id, principal)
                    .await
                    .map_err(map_source_error)?
                    .ok_or(CoordinatorAuthorityError::Denied)?;
                let observation = client
                    .node_assignment(&trusted, requested)
                    .await
                    .map_err(map_source_error)?
                    .ok_or(CoordinatorAuthorityError::Stale)?;
                let target_agent = client
                    .node_assignment_target_agent(&trusted, activity_id, target_agent_id)
                    .await
                    .map_err(map_source_error)?
                    .ok_or(CoordinatorAuthorityError::Stale)?;
                Ok((actor, observation, target_agent))
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let actor = authority
                        .control()?
                        .activity_coordinator_actor(CoordinatorActorRequest {
                            scope: resolved.clone(),
                            activity_id: ActivityId::from_uuid(activity_id),
                            principal,
                        })
                        .map_err(map_control_error)?
                        .ok_or(CoordinatorAuthorityError::Denied)?;
                    let snapshot = authority
                        .control()?
                        .dag_node_assignment_snapshot(resolved.clone(), requested)
                        .map_err(map_control_error)?
                        .ok_or(CoordinatorAuthorityError::Stale)?;
                    let context = authority
                        .control()?
                        .agent_get_context(forge_control::AgentContextRequest {
                            scope: resolved,
                            activity_id: ActivityId::from_uuid(activity_id),
                            agent_id: forge_domain::AgentInstanceId::from_uuid(target_agent_id),
                            now: TimestampMillis::new(
                                crate::current_timestamp_millis()
                                    .map_err(|_| CoordinatorAuthorityError::Unavailable)?,
                            )
                            .map_err(|_| CoordinatorAuthorityError::Unavailable)?,
                            focus_event_limit: forge_domain::AgentListLimit::DEFAULT,
                            work_update_limit: forge_domain::AgentListLimit::DEFAULT,
                        })
                        .map_err(map_control_error)?
                        .ok_or(CoordinatorAuthorityError::Stale)?;
                    let target_agent = crate::forge_source::node_assignment::
                        NodeAssignmentTargetAgentObservation::from_agent(
                            context.attachment().agent(),
                        );
                    Ok((
                        actor,
                        crate::forge_source::node_assignment::node_assignment_observation_from_snapshot(
                            &snapshot,
                        ),
                        target_agent,
                    ))
                })
                .await
            }
        }
    }

    /// Build one bounded authority from an existing Forge control client and
    /// host-resolved Workspace references.
    ///
    /// # Errors
    /// Rejects more than 64 scopes, nil Workspace identities, or duplicate
    /// Workspace identities.
    pub fn new(
        control: ForgeControlClient,
        scopes: Vec<ResolvedWorkspaceScope>,
    ) -> Result<Self, CoordinatorAuthorityError> {
        validate_scopes(&scopes)?;
        Ok(Self {
            transport: SourceTransport::Control { client: control },
            scopes,
            scope_registry: None,
            effect_facts: None,
            home_transfer_source: None,
        })
    }

    /// Select only the configured Forge HTTP source for fresh scoped observations.
    /// This opens no Forge store and never falls back to a control client.
    ///
    /// # Errors
    /// Rejects invalid, duplicate or oversized host scope references.
    pub fn from_http(
        client: crate::forge_source::ForgeSourceHttpClient,
        scopes: Vec<ResolvedWorkspaceScope>,
    ) -> Result<Self, CoordinatorAuthorityError> {
        validate_scopes(&scopes)?;
        Ok(Self {
            transport: SourceTransport::Http(client),
            scopes,
            scope_registry: None,
            effect_facts: None,
            home_transfer_source: None,
        })
    }

    /// Attach the durable `Workspace` scope registry for fail-closed scope resolution.
    #[must_use]
    pub fn with_scope_registry(
        mut self,
        registry: heterarchy_orchestration_runtime::CatalogHandle<
            heterarchy_orchestration_operations::OperationsCatalog,
        >,
    ) -> Self {
        self.scope_registry = Some(registry);
        self
    }

    /// Convert configured host `Workspace` references into durable registry seed scopes.
    ///
    /// # Errors
    /// Returns [`CoordinatorAuthorityError::Unavailable`] if a configured scope
    /// fails domain bounds.
    pub fn configured_project_scopes(
        &self,
    ) -> Result<
        Vec<heterarchy_orchestration_operations::WorkspaceProjectScope>,
        CoordinatorAuthorityError,
    > {
        self.scopes
            .iter()
            .map(|scope| {
                heterarchy_orchestration_operations::WorkspaceProjectScope::new(
                    scope.workspace_id().into_uuid(),
                    scope.locator().location_id().into_uuid(),
                    scope.locator().expected_revision().get(),
                    scope.workspace_revision().get(),
                    scope.locator().encode(),
                )
                .map_err(|_| CoordinatorAuthorityError::Unavailable)
            })
            .collect()
    }

    fn control(&self) -> Result<&ForgeControlClient, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Control { client, .. } => Ok(client),
            SourceTransport::Http(_) => Err(CoordinatorAuthorityError::Unavailable),
        }
    }

    /// Attach the existing destination/backend-pinned HTTP effect observation client.
    /// No alternate transport is selected when this client fails.
    #[must_use]
    pub fn with_effect_facts(mut self, client: forge_backend::ForgeHttpClient) -> Self {
        self.effect_facts = Some(client);
        self
    }

    /// Select the exact configured source lane for Mission evidence operations.
    /// This is a host reference, not a fresh observation or mutation authority;
    /// callers retain their normal admission and actor checks around source I/O.
    pub(crate) fn mission_evidence_source(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
    ) -> Result<(ResolvedWorkspaceScope, forge_backend::ForgeHttpClient), CoordinatorAuthorityError>
    {
        if !matches!(&self.transport, SourceTransport::Http(_)) {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        let trusted = self.trusted_scope(request)?;
        let client = self
            .effect_facts
            .as_ref()
            .ok_or(CoordinatorAuthorityError::Unavailable)?;
        Ok((trusted, client.clone()))
    }

    /// The configured HTTP source client for bounded read-only source
    /// queries. Control-backed hosts return `None`: source reads stay
    /// fail-closed instead of switching transports.
    #[must_use]
    pub(crate) fn http_source(&self) -> Option<&crate::forge_source::ForgeSourceHttpClient> {
        match &self.transport {
            SourceTransport::Http(client) => Some(client),
            SourceTransport::Control { .. } => None,
        }
    }

    pub(crate) async fn coordination_effect_page(
        &self,
        scope: crate::wire::WorkspaceQueryScope,
        cursor: heterarchy_orchestration_operations::EffectFactConsumerProgress,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<
        forge_backend::ops::effect_facts::wire::EffectFactOutboxResponse,
        CoordinatorAuthorityError,
    > {
        let _permit = permit;
        let trusted = self.trusted_scope(&scope)?;
        if cursor.workspace_id() != trusted.workspace_id() {
            return Err(CoordinatorAuthorityError::Stale);
        }
        let client = self
            .effect_facts
            .as_ref()
            .ok_or(CoordinatorAuthorityError::Unavailable)?;
        client
            .effect_fact_outbox(&forge_control::EffectFactOutboxReadRequest {
                scope: trusted.clone(),
                migration_id: cursor.migration_id(),
                destination_id: cursor.destination_id(),
                after: cursor.last_sequence(),
                limit: 64,
            })
            .await
            .map_err(|_| CoordinatorAuthorityError::Unavailable)
    }

    /// Read one exact Forge operations freeze through the configured pinned
    /// HTTP effect client. Control-backed authorities fail closed here because
    /// this migration transport is an HTTP-only source boundary.
    pub(crate) async fn agent_operations_freeze_read(
        &self,
        scope: crate::wire::WorkspaceQueryScope,
        migration_id: uuid::Uuid,
        destination_id: uuid::Uuid,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Option<AgentOperationsFreeze>, CoordinatorAuthorityError> {
        let _permit = permit;
        let trusted = self.trusted_scope(&scope)?;
        if !matches!(&self.transport, SourceTransport::Http(_)) {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        let client = self
            .effect_facts
            .as_ref()
            .ok_or(CoordinatorAuthorityError::Unavailable)?;
        if client.destination_id() != destination_id {
            return Err(CoordinatorAuthorityError::Stale);
        }
        client
            .agent_operations_freeze_read(&AgentOperationsFreezeReadRequest {
                scope: trusted.clone(),
                migration_id,
                destination_id,
            })
            .await
            .map_err(map_operations_source_error)
    }

    /// Read one exact Forge operations export page through the configured
    /// pinned HTTP effect client. Control-backed authorities fail closed.
    pub(crate) async fn agent_operations_export(
        &self,
        scope: crate::wire::WorkspaceQueryScope,
        migration_id: uuid::Uuid,
        destination_id: uuid::Uuid,
        cursor: AgentOperationsExportCursor,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<AgentOperationsExportPage, CoordinatorAuthorityError> {
        let _permit = permit;
        let trusted = self.trusted_scope(&scope)?;
        if !matches!(&self.transport, SourceTransport::Http(_)) {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        let client = self
            .effect_facts
            .as_ref()
            .ok_or(CoordinatorAuthorityError::Unavailable)?;
        if client.destination_id() != destination_id {
            return Err(CoordinatorAuthorityError::Stale);
        }
        client
            .agent_operations_export(&AgentOperationsExportRequest {
                scope: trusted.clone(),
                migration_id,
                destination_id,
                cursor,
            })
            .await
            .map_err(map_operations_source_error)
    }

    fn trusted_scope(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
    ) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
        self.resolve_scope_with_generation(request, None)
            .map(|(scope, _)| scope)
    }

    fn resolve_scope_with_generation(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
        expected_generation: Option<u64>,
    ) -> Result<(ResolvedWorkspaceScope, u64), CoordinatorAuthorityError> {
        if let Some(registry) = &self.scope_registry {
            return resolve_registered_scope(registry, request, expected_generation);
        }
        let expected = self.trusted_workspace_scope(request.workspace_id)?;
        let locator = expected.locator();
        if locator.location_id().into_uuid() != request.location_id
            || locator.expected_revision().get() != request.location_revision
            || expected.workspace_revision().get() != request.workspace_revision
            || locator.encode() != request.locator
            || expected_generation.is_some_and(|generation| generation != 1)
        {
            return Err(CoordinatorAuthorityError::Stale);
        }
        Ok((expected, 1))
    }

    fn trusted_workspace_scope(
        &self,
        workspace_id: uuid::Uuid,
    ) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
        if let Some(registry) = &self.scope_registry {
            let active = {
                let catalog = registry
                    .lock()
                    .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
                catalog
                    .list_active_workspace_scopes()
                    .map_err(|_| CoordinatorAuthorityError::Unavailable)?
            };
            let record = active
                .into_iter()
                .find(|entry| entry.scope().workspace_id() == workspace_id)
                .ok_or(CoordinatorAuthorityError::Denied)?;
            return resolved_scope_from_project_scope(record.scope());
        }
        if self.scopes.is_empty() {
            return Err(CoordinatorAuthorityError::Denied);
        }
        self.scopes
            .iter()
            .find(|scope| scope.workspace_id().into_uuid() == workspace_id)
            .cloned()
            .ok_or(CoordinatorAuthorityError::Denied)
    }

    /// Validate one caller-supplied scope against the exact configured or
    /// registered `Workspace` reference without contacting Forge.
    pub(crate) fn validate_scope(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
    ) -> Result<(), CoordinatorAuthorityError> {
        self.trusted_scope(request).map(|_| ())
    }

    /// Validate one caller-supplied scope and optional monotonic registry
    /// generation fence without contacting Forge, returning the active generation.
    ///
    /// # Errors
    /// Returns [`CoordinatorAuthorityError::Denied`] when no active scope matches,
    /// [`CoordinatorAuthorityError::Stale`] on scope or generation fence mismatch,
    /// and [`CoordinatorAuthorityError::Unavailable`] on catalog failure.
    pub fn validate_scope_with_generation(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
        expected_generation: Option<u64>,
    ) -> Result<u64, CoordinatorAuthorityError> {
        self.resolve_scope_with_generation(request, expected_generation)
            .map(|(_, generation)| generation)
    }

    pub(crate) async fn coordinator_observation(
        &self,
        scope: crate::wire::WorkspaceQueryScope,
        activity_id: uuid::Uuid,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Option<forge_domain::ActivityCoordinatorBinding>, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let trusted = self.trusted_scope(&scope)?;
                client
                    .coordinator(&trusted, activity_id)
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    authority
                        .control()?
                        .activity_coordinator_get(forge_control::CoordinatorGetRequest {
                            scope: resolved,
                            activity_id: forge_domain::ActivityId::from_uuid(activity_id),
                        })
                        .map_err(map_control_error)
                })
                .await
            }
        }
    }

    pub(crate) async fn node_assignment_observation(
        &self,
        scope: crate::wire::WorkspaceQueryScope,
        node_scope: heterarchy_orchestration_operations::NodeTeamBindingScope,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<
        Option<crate::forge_source::node_assignment::NodeAssignmentObservation>,
        CoordinatorAuthorityError,
    > {
        if node_scope.workspace_id().into_uuid() != scope.workspace_id {
            return Err(CoordinatorAuthorityError::Stale);
        }
        let requested = heterarchy_orchestration_operations::DagNodeEvidenceScope::new(
            node_scope.goal_id(),
            node_scope.dag_revision(),
            node_scope.node_id(),
        );
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let trusted = self.trusted_scope(&scope)?;
                client
                    .node_assignment(&trusted, requested)
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let snapshot = authority.control()?.dag_node_assignment_snapshot(resolved, requested)
                        .map_err(map_control_error)?;
                    Ok(snapshot.as_ref().map(crate::forge_source::node_assignment::node_assignment_observation_from_snapshot))
                }).await
            }
        }
    }

    /// Keep the admitted permit with the actual control job through observer cancellation.
    pub(crate) async fn check_workspace(
        &self,
        request: crate::wire::WorkspaceQueryScope,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let trusted = self.trusted_scope(&request)?;
                client
                    .resolve_workspace(&trusted)
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || authority.resolve_workspace(&request)).await
            }
        }
    }

    pub(crate) async fn check_activity(
        &self,
        request: crate::wire::WorkspaceQueryScope,
        activity: uuid::Uuid,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), CoordinatorAuthorityError> {
        if activity.is_nil() {
            return Err(CoordinatorAuthorityError::Denied);
        }
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let expected = self.trusted_scope(&request)?;
                client
                    .resolve_workspace(&expected)
                    .await
                    .map_err(map_source_error)?;
                match client
                    .activity(&expected, activity)
                    .await
                    .map_err(map_source_error)?
                {
                    Some(observation)
                        if observation.state() == forge_domain::ActivityState::Open =>
                    {
                        Ok(())
                    }
                    _ => Err(CoordinatorAuthorityError::Denied),
                }
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    authority.authorize_activity(&request, activity)
                })
                .await
            }
        }
    }

    pub(crate) async fn check_coordinator(
        &self,
        command: AgentRequestSubmit,
        principal: AuthenticatedPrincipal,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<ActivityCoordinatorActor, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let scope = self.trusted_workspace_scope(command.workspace_id)?;
                let actor = client
                    .coordinator_actor(&scope, command.activity_id, principal)
                    .await
                    .map_err(map_source_error)?
                    .ok_or(CoordinatorAuthorityError::Denied)?;
                validate_actor(&command, principal, actor)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || authority.resolve(&command, principal)).await
            }
        }
    }

    /// Revalidate one exact host-routed Workspace reference through Forge.
    /// This proves source identity/fences only; Runtime separately authorizes
    /// the caller's logical Workspace membership before and after this query.
    ///
    /// # Errors
    /// Rejects unconfigured Workspace routes, changed caller references,
    /// stale durable Location/Workspace revisions, and unavailable Forge.
    pub fn resolve_workspace(
        &self,
        request: &crate::wire::leadership::WorkspaceQueryScope,
    ) -> Result<(), CoordinatorAuthorityError> {
        self.fresh_workspace(request).map(|_| ())
    }

    fn fresh_workspace(
        &self,
        request: &crate::wire::leadership::WorkspaceQueryScope,
    ) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
        let expected = self.trusted_scope(request)?;
        let locator = expected.locator();
        let resolved = self
            .control()?
            .resolve_workspace(
                locator.location_id(),
                locator.expected_revision(),
                locator.clone(),
            )
            .map_err(map_control_error)?;
        if resolved.workspace_id().into_uuid() != request.workspace_id
            || resolved.workspace_revision().get() != request.workspace_revision
        {
            return Err(CoordinatorAuthorityError::Stale);
        }
        Ok(resolved)
    }

    /// Prove the exact Activity remains Open through the existing Forge owner.
    ///
    /// # Errors
    /// Rejects nil identities, stale source routing, closed/missing Activities,
    /// or unavailable control authority.
    pub fn authorize_activity(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
        activity_id: uuid::Uuid,
    ) -> Result<(), CoordinatorAuthorityError> {
        if activity_id.is_nil() {
            return Err(CoordinatorAuthorityError::Denied);
        }
        let scope = self.fresh_workspace(request)?;
        let snapshot = self
            .control()?
            .activity_read(forge_control::ActivityReadRequest {
                scope,
                activity_id: ActivityId::from_uuid(activity_id),
            })
            .map_err(map_control_error)?;
        match snapshot.activity() {
            Some(activity) if activity.state() == forge_domain::ActivityState::Open => Ok(()),
            _ => Err(CoordinatorAuthorityError::Denied),
        }
    }

    /// Resolve the exact Forge Coordinator actor for one `AgentRequest` submit.
    ///
    /// The scope is selected by Workspace identity only; Forge then verifies
    /// the Activity, Session, Agent and principal against durable state.
    ///
    /// # Errors
    /// Returns `Denied` for an unconfigured/unregistered `Workspace` or
    /// missing/foreign actor identity, `Unavailable` for transport failure, and
    /// `Stale` for scope or Coordinator revision fences.
    pub fn resolve(
        &self,
        command: &AgentRequestSubmit,
        principal: AuthenticatedPrincipal,
    ) -> Result<ActivityCoordinatorActor, CoordinatorAuthorityError> {
        let scope = self.trusted_workspace_scope(command.workspace_id)?;
        let activity_id = ActivityId::from_uuid(command.activity_id);
        let actor = self
            .control()?
            .activity_coordinator_actor(CoordinatorActorRequest {
                scope,
                activity_id,
                principal,
            })
            .map_err(map_control_error)?
            .ok_or(CoordinatorAuthorityError::Denied)?;
        validate_actor(command, principal, actor)
    }
}

fn parse_candidate_scope(
    request: &crate::wire::WorkspaceQueryScope,
) -> Option<(
    heterarchy_orchestration_operations::WorkspaceProjectScope,
    forge_control::WorkspaceLocator,
)> {
    let candidate = heterarchy_orchestration_operations::WorkspaceProjectScope::new(
        request.workspace_id,
        request.location_id,
        request.location_revision,
        request.workspace_revision,
        request.locator.clone(),
    )
    .ok()?;
    let locator = forge_control::WorkspaceLocator::decode(candidate.locator()).ok()?;
    if locator.location_id().into_uuid() != candidate.location_id()
        || locator.expected_revision().get() != candidate.location_revision()
    {
        return None;
    }
    Some((candidate, locator))
}

fn resolved_scope_from_project_scope(
    scope: &heterarchy_orchestration_operations::WorkspaceProjectScope,
) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
    let workspace_revision = forge_domain::WorkspaceRevision::new(scope.workspace_revision())
        .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
    let locator = forge_control::WorkspaceLocator::decode(scope.locator())
        .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
    if locator.location_id().into_uuid() != scope.location_id()
        || locator.expected_revision().get() != scope.location_revision()
    {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    Ok(ResolvedWorkspaceScope::from_observation(
        forge_domain::WorkspaceId::from_uuid(scope.workspace_id()),
        workspace_revision,
        locator,
    ))
}

fn resolve_registered_scope(
    registry: &heterarchy_orchestration_runtime::CatalogHandle<
        heterarchy_orchestration_operations::OperationsCatalog,
    >,
    request: &crate::wire::WorkspaceQueryScope,
    expected_generation: Option<u64>,
) -> Result<(ResolvedWorkspaceScope, u64), CoordinatorAuthorityError> {
    use heterarchy_orchestration_operations::ScopeResolutionError;

    let Some((candidate, locator)) = parse_candidate_scope(request) else {
        let active = {
            let catalog = registry
                .lock()
                .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
            catalog
                .list_active_workspace_scopes()
                .map_err(|_| CoordinatorAuthorityError::Unavailable)?
        };
        if active
            .iter()
            .any(|record| record.scope().workspace_id() == request.workspace_id)
        {
            return Err(CoordinatorAuthorityError::Stale);
        }
        return Err(CoordinatorAuthorityError::Denied);
    };

    let resolution = {
        let catalog = registry
            .lock()
            .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
        catalog
            .resolve_registered_workspace_scope(&candidate, expected_generation)
            .map_err(|_| CoordinatorAuthorityError::Unavailable)?
    };
    let record = match resolution {
        Ok(record) => record,
        Err(ScopeResolutionError::EmptyRegistry | ScopeResolutionError::Unregistered { .. }) => {
            return Err(CoordinatorAuthorityError::Denied);
        }
        Err(
            ScopeResolutionError::StaleScope { .. }
            | ScopeResolutionError::GenerationFenceMismatch { .. },
        ) => {
            return Err(CoordinatorAuthorityError::Stale);
        }
    };
    let workspace_revision =
        forge_domain::WorkspaceRevision::new(record.scope().workspace_revision())
            .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
    let resolved = ResolvedWorkspaceScope::from_observation(
        forge_domain::WorkspaceId::from_uuid(record.scope().workspace_id()),
        workspace_revision,
        locator,
    );
    Ok((resolved, record.generation()))
}

fn validate_actor(
    command: &AgentRequestSubmit,
    principal: AuthenticatedPrincipal,
    actor: ActivityCoordinatorActor,
) -> Result<ActivityCoordinatorActor, CoordinatorAuthorityError> {
    if actor.workspace_id().into_uuid() != command.workspace_id
        || actor.activity_id().into_uuid() != command.activity_id
        || actor.agent_id().into_uuid() != command.requester_agent_id
        || actor.session_id().into_uuid() != command.requester_session_id
        || actor.principal() != principal
    {
        return Err(CoordinatorAuthorityError::Denied);
    }
    let expected_revision = i64::try_from(command.coordinator_revision)
        .map_err(|_| CoordinatorAuthorityError::Stale)?;
    if actor.expected_revision().get() != expected_revision {
        return Err(CoordinatorAuthorityError::Stale);
    }
    Ok(actor)
}

fn validate_scopes(scopes: &[ResolvedWorkspaceScope]) -> Result<(), CoordinatorAuthorityError> {
    if scopes.len() > MAX_WORKSPACE_AUTHORITY_SCOPES {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    let mut workspace_ids = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let workspace = scope.workspace_id().into_uuid();
        if workspace.is_nil()
            || scope.locator().location_id().into_uuid().is_nil()
            || workspace_ids.contains(&workspace)
        {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        workspace_ids.push(workspace);
    }
    Ok(())
}

async fn control_job<T, F>(
    permit: tokio::sync::OwnedSemaphorePermit,
    operation: F,
) -> Result<T, CoordinatorAuthorityError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, CoordinatorAuthorityError> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    })
    .await
    .map_err(|_| CoordinatorAuthorityError::Unavailable)?
}

fn map_source_error(error: crate::forge_source::ForgeSourceError) -> CoordinatorAuthorityError {
    use crate::forge_source::ForgeSourceError;
    match error {
        ForgeSourceError::Stale => CoordinatorAuthorityError::Stale,
        ForgeSourceError::Denied => CoordinatorAuthorityError::Denied,
        ForgeSourceError::InvalidScope
        | ForgeSourceError::NotFound
        | ForgeSourceError::Unavailable
        | ForgeSourceError::Protocol => CoordinatorAuthorityError::Unavailable,
    }
}

/// Preserve deterministic source rejections for the import boundary. A
/// released/stale migration is a conflict that must stop the caller, while
/// transport, capacity, protocol and unknown service failures remain source
/// unavailability and retain their retry/readback policy at the HTTP edge.
fn map_operations_source_error(error: ForgeHttpClientError) -> CoordinatorAuthorityError {
    match error {
        ForgeHttpClientError::Service { status, code } => match code {
            ForgeHttpServiceCode::WorkspaceConfinementDenied
            | ForgeHttpServiceCode::WorkspaceScopeOverlap => CoordinatorAuthorityError::Denied,
            ForgeHttpServiceCode::InvalidInput
            | ForgeHttpServiceCode::InvalidPathKind
            | ForgeHttpServiceCode::StaleRevision
            | ForgeHttpServiceCode::SourceConflict
            | ForgeHttpServiceCode::PlanningConflict
            | ForgeHttpServiceCode::LeaseConflict
            | ForgeHttpServiceCode::ResyncRequired => CoordinatorAuthorityError::Stale,
            // Forge's current HTTP envelope uses the generic
            // `FORGE_UNAVAILABLE` code for the control-owner
            // `AgentConflict` family, but still emits a 409. Preserve that
            // determinate source conflict instead of turning it into a
            // retryable scope outage. A server-side 5xx remains unavailable.
            ForgeHttpServiceCode::ForgeUnavailable if status < 500 => {
                CoordinatorAuthorityError::Stale
            }
            ForgeHttpServiceCode::CapacityExceeded
            | ForgeHttpServiceCode::UnknownOutcome
            | ForgeHttpServiceCode::AuthFailed
            | ForgeHttpServiceCode::ForgeUnavailable
            | ForgeHttpServiceCode::Unknown => CoordinatorAuthorityError::Unavailable,
        },
        // The pinned client performs this check before I/O. If its identity
        // no longer matches the caller's destination, treat the observation
        // as stale rather than advertising a retryable source outage.
        ForgeHttpClientError::DestinationMismatch => CoordinatorAuthorityError::Stale,
        ForgeHttpClientError::Capacity
        | ForgeHttpClientError::Transport
        | ForgeHttpClientError::Timeout
        | ForgeHttpClientError::Request
        | ForgeHttpClientError::RequestTooLarge
        | ForgeHttpClientError::ResponseHeadersTooLarge
        | ForgeHttpClientError::ResponseTooLarge
        | ForgeHttpClientError::Authentication
        | ForgeHttpClientError::Protocol
        | ForgeHttpClientError::InvalidRequest => CoordinatorAuthorityError::Unavailable,
    }
}

fn map_control_error(error: ControlClientError) -> CoordinatorAuthorityError {
    match error {
        ControlClientError::QueueFull
        | ControlClientError::WorkerClosed
        | ControlClientError::WorkerShuttingDown
        | ControlClientError::ResponseDropped => CoordinatorAuthorityError::Unavailable,
        ControlClientError::Service(error) => {
            map_control_service(error.code(), error.view().outcome())
        }
    }
}

fn map_control_service(
    code: ControlErrorCode,
    outcome: forge_control::ControlOperationOutcome,
) -> CoordinatorAuthorityError {
    if matches!(
        outcome,
        forge_control::ControlOperationOutcome::Unknown
            | forge_control::ControlOperationOutcome::CommittedButUnconfirmed
    ) {
        return CoordinatorAuthorityError::Unavailable;
    }
    match code {
        ControlErrorCode::WorkspaceRevisionConflict
        | ControlErrorCode::WorkspaceRootConflict
        | ControlErrorCode::WorkspaceIdentityConflict
        | ControlErrorCode::AgentConflict
        | ControlErrorCode::CoordinatorConflict => CoordinatorAuthorityError::Stale,
        ControlErrorCode::ActivityNotFound
        | ControlErrorCode::ActivityConflict
        | ControlErrorCode::AgentSessionNotFound
        | ControlErrorCode::AgentSessionClosed
        | ControlErrorCode::AgentSessionConflict
        | ControlErrorCode::AgentSessionUnresolved => CoordinatorAuthorityError::Denied,
        _ => CoordinatorAuthorityError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncertain_control_rejections_never_become_known_denials() {
        for code in [
            ControlErrorCode::AgentConflict,
            ControlErrorCode::CoordinatorConflict,
            ControlErrorCode::AgentSessionClosed,
            ControlErrorCode::WorkspaceRevisionConflict,
        ] {
            for outcome in [
                forge_control::ControlOperationOutcome::Unknown,
                forge_control::ControlOperationOutcome::CommittedButUnconfirmed,
            ] {
                assert_eq!(
                    map_control_service(code, outcome),
                    CoordinatorAuthorityError::Unavailable
                );
            }
        }
        assert_eq!(
            map_control_service(
                ControlErrorCode::AgentConflict,
                forge_control::ControlOperationOutcome::NotAdmitted
            ),
            CoordinatorAuthorityError::Stale
        );
    }

    #[test]
    fn import_source_rejections_preserve_determinate_classes() {
        for code in [
            ForgeHttpServiceCode::StaleRevision,
            ForgeHttpServiceCode::SourceConflict,
            ForgeHttpServiceCode::ResyncRequired,
        ] {
            assert_eq!(
                map_operations_source_error(ForgeHttpClientError::Service { status: 409, code }),
                CoordinatorAuthorityError::Stale
            );
        }
        assert_eq!(
            map_operations_source_error(ForgeHttpClientError::Service {
                status: 403,
                code: ForgeHttpServiceCode::WorkspaceConfinementDenied,
            }),
            CoordinatorAuthorityError::Denied
        );
        assert_eq!(
            map_operations_source_error(ForgeHttpClientError::Service {
                status: 409,
                code: ForgeHttpServiceCode::ForgeUnavailable,
            }),
            CoordinatorAuthorityError::Stale
        );
        assert_eq!(
            map_operations_source_error(ForgeHttpClientError::Service {
                status: 503,
                code: ForgeHttpServiceCode::ForgeUnavailable,
            }),
            CoordinatorAuthorityError::Unavailable
        );
        assert_eq!(
            map_operations_source_error(ForgeHttpClientError::DestinationMismatch),
            CoordinatorAuthorityError::Stale
        );
    }

    #[tokio::test]
    async fn cancelled_control_observer_keeps_capacity_until_the_job_finishes()
    -> Result<(), Box<dyn std::error::Error>> {
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await?;
        let (started, observing) = tokio::sync::oneshot::channel();
        let (release, waiting) = std::sync::mpsc::channel();
        let observer = tokio::spawn(control_job(permit, move || {
            let _started = started.send(());
            waiting
                .recv()
                .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
            Ok(())
        }));
        tokio::time::timeout(std::time::Duration::from_secs(1), observing).await??;
        observer.abort();
        assert!(observer.await.is_err());
        assert_eq!(slots.available_permits(), 0);
        assert!(slots.clone().try_acquire_owned().is_err());
        release.send(())?;
        let recovered = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            slots.clone().acquire_owned(),
        )
        .await??;
        drop(recovered);
        assert_eq!(slots.available_permits(), 1);
        Ok(())
    }

    struct TempDir(std::path::PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct RegistryFixture {
        runtime: heterarchy_orchestration_runtime::OrchestrationRuntime,
        _dir: TempDir,
    }

    impl RegistryFixture {
        fn open() -> Result<Self, Box<dyn std::error::Error>> {
            let path = std::env::temp_dir().join(format!(
                "coordinator-authority-registry-{}",
                uuid::Uuid::now_v7()
            ));
            std::fs::create_dir_all(&path)?;
            let dir = TempDir(path);
            let runtime = heterarchy_orchestration_runtime::OrchestrationRuntime::open(&dir.0)?;
            Ok(Self { runtime, _dir: dir })
        }
    }

    fn test_http_authority(
        scopes: Vec<ResolvedWorkspaceScope>,
    ) -> Result<CoordinatorAuthority, Box<dyn std::error::Error>> {
        let commands = crate::forge_commands::ForgeCommandHttpClient::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
            "test-bearer-secret",
            std::time::Duration::from_secs(1),
        )?;
        let source = crate::forge_source::ForgeSourceHttpClient::new(commands);
        Ok(CoordinatorAuthority::from_http(source, scopes)?)
    }

    fn sample_scope(
        workspace_id: uuid::Uuid,
        location_id: uuid::Uuid,
        location_revision: i64,
        workspace_revision: i64,
    ) -> Result<
        (ResolvedWorkspaceScope, crate::wire::WorkspaceQueryScope),
        Box<dyn std::error::Error>,
    > {
        let workspace_rev = forge_domain::WorkspaceRevision::new(workspace_revision)?;
        let location_rev = forge_domain::LocationRevision::new(location_revision)?;
        let locator = forge_control::WorkspaceLocator::new(
            forge_domain::LocationId::from_uuid(location_id),
            location_rev,
            std::path::Path::new("."),
        )?;
        let query = crate::wire::WorkspaceQueryScope {
            workspace_id,
            location_id,
            location_revision,
            workspace_revision,
            locator: locator.encode(),
        };
        let resolved = ResolvedWorkspaceScope::from_observation(
            forge_domain::WorkspaceId::from_uuid(workspace_id),
            workspace_rev,
            locator,
        );
        Ok((resolved, query))
    }

    #[test]
    fn http_authority_with_empty_scopes_fails_closed_with_or_without_registry()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, query) = sample_scope(uuid::Uuid::from_u128(11), uuid::Uuid::from_u128(21), 1, 1)?;
        let standalone = test_http_authority(Vec::new())?;
        assert!(standalone.configured_project_scopes()?.is_empty());
        assert_eq!(
            standalone.validate_scope(&query),
            Err(CoordinatorAuthorityError::Denied)
        );
        assert_eq!(
            standalone.trusted_workspace_scope(query.workspace_id),
            Err(CoordinatorAuthorityError::Denied)
        );

        let fixture = RegistryFixture::open()?;
        let attached = test_http_authority(Vec::new())?
            .with_scope_registry(fixture.runtime.operations_catalog());
        assert_eq!(
            attached.validate_scope(&query),
            Err(CoordinatorAuthorityError::Denied)
        );
        assert_eq!(
            attached.trusted_workspace_scope(query.workspace_id),
            Err(CoordinatorAuthorityError::Denied)
        );
        Ok(())
    }

    #[test]
    fn scope_registry_enforces_active_membership_and_revision_fences()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = RegistryFixture::open()?;
        let catalog_handle = fixture.runtime.operations_catalog();
        let (scope_a, query_a) =
            sample_scope(uuid::Uuid::from_u128(101), uuid::Uuid::from_u128(201), 1, 1)?;
        let (_, query_b) =
            sample_scope(uuid::Uuid::from_u128(102), uuid::Uuid::from_u128(202), 1, 1)?;

        let authority =
            test_http_authority(vec![scope_a.clone()])?.with_scope_registry(catalog_handle.clone());
        assert_eq!(
            authority.validate_scope(&query_a),
            Err(CoordinatorAuthorityError::Denied)
        );

        let initial_scopes = authority.configured_project_scopes()?;
        {
            let mut catalog = catalog_handle
                .lock()
                .map_err(|_| "operations catalog mutex poisoned")?;
            let seeded = catalog.seed_workspace_scopes_if_absent(&initial_scopes, 10)?;
            assert_eq!(seeded.len(), 1);
            assert_eq!(seeded[0].generation(), 1);
        }

        assert_eq!(authority.validate_scope(&query_a), Ok(()));
        assert_eq!(
            authority.trusted_workspace_scope(query_a.workspace_id)?,
            scope_a
        );
        assert_eq!(
            authority.validate_scope(&query_b),
            Err(CoordinatorAuthorityError::Denied)
        );
        assert_eq!(
            authority.trusted_workspace_scope(query_b.workspace_id),
            Err(CoordinatorAuthorityError::Denied)
        );

        let mut stale_workspace = query_a.clone();
        stale_workspace.workspace_revision = 2;
        assert_eq!(
            authority.validate_scope(&stale_workspace),
            Err(CoordinatorAuthorityError::Stale)
        );
        let mut stale_location = query_a.clone();
        stale_location.location_revision = 2;
        assert_eq!(
            authority.validate_scope(&stale_location),
            Err(CoordinatorAuthorityError::Stale)
        );
        let mut invalid_active_locator = query_a;
        invalid_active_locator.locator = "invalid-locator".to_owned();
        assert_eq!(
            authority.validate_scope(&invalid_active_locator),
            Err(CoordinatorAuthorityError::Stale)
        );
        let mut invalid_unregistered_locator = query_b;
        invalid_unregistered_locator.locator = "invalid-locator".to_owned();
        assert_eq!(
            authority.validate_scope(&invalid_unregistered_locator),
            Err(CoordinatorAuthorityError::Denied)
        );
        Ok(())
    }

    #[test]
    fn scope_registry_unregistration_and_reregistration_advance_generation_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        use heterarchy_orchestration_operations::{
            ScopeRegistrationCommand, ScopeUnregisterCommand, WorkspaceProjectScope,
        };

        let fixture = RegistryFixture::open()?;
        let catalog_handle = fixture.runtime.operations_catalog();
        let (scope_a, query_a) =
            sample_scope(uuid::Uuid::from_u128(103), uuid::Uuid::from_u128(203), 1, 1)?;
        let authority =
            test_http_authority(vec![scope_a])?.with_scope_registry(catalog_handle.clone());
        let initial_scopes = authority.configured_project_scopes()?;
        {
            let mut catalog = catalog_handle
                .lock()
                .map_err(|_| "operations catalog mutex poisoned")?;
            catalog.seed_workspace_scopes_if_absent(&initial_scopes, 10)?;
        }
        assert_eq!(
            authority.validate_scope_with_generation(&query_a, Some(1)),
            Ok(1)
        );

        {
            let mut catalog = catalog_handle
                .lock()
                .map_err(|_| "operations catalog mutex poisoned")?;
            let unregistered = catalog.unregister_workspace_scope(&ScopeUnregisterCommand::new(
                uuid::Uuid::now_v7(),
                forge_domain::WorkspaceId::from_uuid(query_a.workspace_id),
                1,
                20,
            )?)?;
            assert_eq!(unregistered.generation(), 2);
        }
        assert_eq!(
            authority.validate_scope(&query_a),
            Err(CoordinatorAuthorityError::Denied)
        );
        assert_eq!(
            authority.trusted_workspace_scope(query_a.workspace_id),
            Err(CoordinatorAuthorityError::Denied)
        );
        assert_eq!(
            authority.validate_scope_with_generation(&query_a, Some(1)),
            Err(CoordinatorAuthorityError::Denied)
        );

        let (_, query_a_v2) = sample_scope(query_a.workspace_id, query_a.location_id, 1, 2)?;
        {
            let mut catalog = catalog_handle
                .lock()
                .map_err(|_| "operations catalog mutex poisoned")?;
            let reregistered = catalog.register_workspace_scope(&ScopeRegistrationCommand::new(
                uuid::Uuid::now_v7(),
                2,
                WorkspaceProjectScope::new(
                    query_a_v2.workspace_id,
                    query_a_v2.location_id,
                    query_a_v2.location_revision,
                    query_a_v2.workspace_revision,
                    query_a_v2.locator.clone(),
                )?,
                30,
            )?)?;
            assert_eq!(reregistered.generation(), 3);
        }
        assert_eq!(
            authority.validate_scope_with_generation(&query_a_v2, Some(1)),
            Err(CoordinatorAuthorityError::Stale)
        );
        assert_eq!(
            authority.validate_scope_with_generation(&query_a_v2, Some(2)),
            Err(CoordinatorAuthorityError::Stale)
        );
        assert_eq!(
            authority.validate_scope_with_generation(&query_a_v2, Some(3)),
            Ok(3)
        );
        assert_eq!(
            authority.validate_scope_with_generation(&query_a_v2, None),
            Ok(3)
        );
        Ok(())
    }
}
