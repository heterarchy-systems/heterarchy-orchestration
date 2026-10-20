//! Bounded Forge-backed Coordinator authority for Orchestration mutations.
//!
//! The host selects one control or HTTP transport and bounded Workspace route
//! references. This adapter opens no Forge catalog. Fresh source observations
//! and internal actor resolution remain separate from Orchestration caller
//! authorization; an HTTP observation never manufactures a Forge actor.

mod departure;
pub(crate) use departure::HandoffTargetSelector;
mod membership;

use forge_backend::{ForgeHttpClientError, ForgeHttpServiceCode};
use forge_control::{
    AgentOperationsExportRequest, AgentOperationsFreezeReadRequest, ControlClientError,
    CoordinatorActorRequest, FileActivityScope, ForgeControlClient, WorkspaceLocator,
};
use forge_domain::{
    ActivityCoordinatorActor, ActivityId, AuthenticatedPrincipal, RuntimeInstanceId,
    TimestampMillis, WorkspaceId, WorkspaceRevision,
};
use heterarchy_orchestration_operations::{
    AgentOperationsExportCursor, AgentOperationsExportPage, AgentOperationsFreeze,
    MAX_WORKSPACE_AUTHORITY_SCOPES, WorkspaceProjectBindingRevision, WorkspaceProjectEnrollment,
    WorkspaceProjectScope,
};
use heterarchy_orchestration_runtime::provisioning::AgentRequestSubmit;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};
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
#[derive(Debug, Clone)]
pub struct CoordinatorAuthority {
    transport: SourceTransport,
    scopes: Vec<ResolvedWorkspaceScope>,
    enrolled_scopes: Arc<RwLock<WorkspaceProjectScopeProjection>>,
    effect_facts: Option<forge_backend::ForgeHttpClient>,
}

#[derive(Debug, Default)]
struct WorkspaceProjectScopeProjection {
    enrolled: BTreeMap<uuid::Uuid, EnrolledWorkspaceScope>,
    uncertain: BTreeSet<uuid::Uuid>,
}

#[derive(Debug, Clone)]
struct EnrolledWorkspaceScope {
    scope: ResolvedWorkspaceScope,
    binding_revision: WorkspaceProjectBindingRevision,
}

#[derive(Debug, Clone)]
enum SourceTransport {
    Control {
        client: ForgeControlClient,
        runtime_instance_id: RuntimeInstanceId,
    },
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
            transport: SourceTransport::Control {
                client: control,
                runtime_instance_id: RuntimeInstanceId::from_uuid(uuid::Uuid::now_v7()),
            },
            scopes,
            enrolled_scopes: Arc::new(RwLock::new(WorkspaceProjectScopeProjection::default())),
            effect_facts: None,
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
            enrolled_scopes: Arc::new(RwLock::new(WorkspaceProjectScopeProjection::default())),
            effect_facts: None,
        })
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

    pub(crate) fn static_workspace_ids(&self) -> Vec<uuid::Uuid> {
        self.scopes
            .iter()
            .map(|scope| scope.workspace_id().into_uuid())
            .collect()
    }

    /// Load the durable enrollment projection before serving requests. Static
    /// scopes remain the compatibility baseline only where no dynamic row exists.
    pub(crate) fn hydrate_workspace_project_enrollments(
        &self,
        enrollments: &[WorkspaceProjectEnrollment],
    ) -> Result<(), CoordinatorAuthorityError> {
        let mut dynamic = BTreeMap::new();
        for enrollment in enrollments {
            let scope = resolved_workspace_scope(enrollment.scope())?;
            let workspace_id = scope.workspace_id().into_uuid();
            if dynamic
                .insert(
                    workspace_id,
                    EnrolledWorkspaceScope {
                        scope,
                        binding_revision: enrollment.binding_revision(),
                    },
                )
                .is_some()
            {
                return Err(CoordinatorAuthorityError::Unavailable);
            }
        }
        let mut workspace_ids = self
            .scopes
            .iter()
            .map(|scope| scope.workspace_id().into_uuid())
            .collect::<std::collections::BTreeSet<_>>();
        workspace_ids.extend(dynamic.keys().copied());
        if workspace_ids.len() > MAX_WORKSPACE_AUTHORITY_SCOPES {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        *self
            .enrolled_scopes
            .write()
            .map_err(|_| CoordinatorAuthorityError::Unavailable)? =
            WorkspaceProjectScopeProjection {
                enrolled: dynamic,
                uncertain: BTreeSet::new(),
            };
        Ok(())
    }

    /// Publish one just-committed enrollment. Older or duplicate publication
    /// cannot replace a newer binding revision in a clone-shared projection.
    pub(crate) fn publish_workspace_project_enrollment(
        &self,
        scope: &ResolvedWorkspaceScope,
        binding_revision: WorkspaceProjectBindingRevision,
    ) -> bool {
        let Ok(mut projection) = self.enrolled_scopes.write() else {
            return false;
        };
        let workspace_id = scope.workspace_id().into_uuid();
        if let Some(current) = projection.enrolled.get(&workspace_id)
            && current.binding_revision >= binding_revision
        {
            let current_matches =
                current.binding_revision == binding_revision && current.scope == *scope;
            if current_matches {
                projection.uncertain.remove(&workspace_id);
            }
            return current_matches;
        }
        let mut workspace_ids = self
            .scopes
            .iter()
            .map(|scope| scope.workspace_id().into_uuid())
            .collect::<std::collections::BTreeSet<_>>();
        workspace_ids.extend(projection.enrolled.keys().copied());
        if !workspace_ids.contains(&workspace_id)
            && workspace_ids.len() >= MAX_WORKSPACE_AUTHORITY_SCOPES
        {
            return false;
        }
        projection.enrolled.insert(
            workspace_id,
            EnrolledWorkspaceScope {
                scope: scope.clone(),
                binding_revision,
            },
        );
        projection.uncertain.remove(&workspace_id);
        true
    }

    pub(crate) fn mark_workspace_project_projection_unavailable(&self, workspace_id: uuid::Uuid) {
        if let Ok(mut projection) = self.enrolled_scopes.write() {
            projection.uncertain.insert(workspace_id);
        }
    }

    /// Read the current authorization snapshot for one already-proven Forge scope.
    pub(crate) fn workspace_authorized(
        &self,
        scope: &ResolvedWorkspaceScope,
    ) -> Result<bool, CoordinatorAuthorityError> {
        let workspace_id = scope.workspace_id().into_uuid();
        let projection = self
            .enrolled_scopes
            .read()
            .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
        if projection.uncertain.contains(&workspace_id) {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        if let Some(enrollment) = projection.enrolled.get(&workspace_id) {
            return Ok(enrollment.scope == *scope);
        }
        Ok(self.scopes.iter().any(|configured| configured == scope))
    }

    /// Select an exact host-configured reference for a persisted Workspace ID.
    /// This selection is not a fresh source observation or execution authority.
    pub(crate) fn workspace_reference(
        &self,
        workspace_id: uuid::Uuid,
    ) -> Result<crate::wire::WorkspaceQueryScope, CoordinatorAuthorityError> {
        let scope = self.trusted_workspace_scope(workspace_id)?;
        Ok(workspace_scope_wire(&scope))
    }

    pub(crate) fn workspace_reference_for_operator_readback(
        &self,
        workspace_id: uuid::Uuid,
        candidate: &crate::wire::WorkspaceQueryScope,
    ) -> Result<crate::wire::WorkspaceQueryScope, CoordinatorAuthorityError> {
        if candidate.workspace_id != workspace_id {
            return Err(CoordinatorAuthorityError::Stale);
        }
        let projection = self
            .enrolled_scopes
            .read()
            .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
        if projection.uncertain.contains(&workspace_id) {
            drop(projection);
            return Ok(candidate.clone());
        }
        if let Some(enrollment) = projection.enrolled.get(&workspace_id) {
            return Ok(workspace_scope_wire(&enrollment.scope));
        }
        drop(projection);
        if let Some(scope) = self
            .scopes
            .iter()
            .find(|scope| scope.workspace_id().into_uuid() == workspace_id)
        {
            return Ok(workspace_scope_wire(scope));
        }
        Ok(candidate.clone())
    }

    fn trusted_scope(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
    ) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
        let expected = self.trusted_workspace_scope(request.workspace_id)?;
        let locator = expected.locator();
        if locator.location_id().into_uuid() != request.location_id
            || locator.expected_revision().get() != request.location_revision
            || expected.workspace_revision().get() != request.workspace_revision
            || locator.encode() != request.locator
        {
            return Err(CoordinatorAuthorityError::Stale);
        }
        Ok(expected)
    }

    fn trusted_workspace_scope(
        &self,
        workspace_id: uuid::Uuid,
    ) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
        let projection = self
            .enrolled_scopes
            .read()
            .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
        if projection.uncertain.contains(&workspace_id) {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        if let Some(enrollment) = projection.enrolled.get(&workspace_id) {
            return Ok(enrollment.scope.clone());
        }
        self.scopes
            .iter()
            .find(|scope| scope.workspace_id().into_uuid() == workspace_id)
            .cloned()
            .ok_or(CoordinatorAuthorityError::Unavailable)
    }

    /// Validate one caller-supplied scope against the exact host-configured
    /// reference without contacting Forge.
    pub(crate) fn validate_scope(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
    ) -> Result<(), CoordinatorAuthorityError> {
        self.trusted_scope(request).map(|_| ())
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

    /// Prove one exact current Forge Workspace candidate without requiring a
    /// prior static or dynamic enrollment row. This grants no work permission.
    pub(crate) async fn check_workspace_candidate(
        &self,
        request: crate::wire::WorkspaceQueryScope,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
        let expected = workspace_scope_from_wire(&request)?;
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let locator = expected.locator();
                let relative_root = locator
                    .relative_root()
                    .to_str()
                    .ok_or(CoordinatorAuthorityError::Stale)?;
                let resolution = client
                    .resolve_workspace_identity(
                        locator.location_id(),
                        locator.expected_revision(),
                        relative_root,
                    )
                    .await
                    .map_err(map_source_error)?;
                if resolution.workspace_id() != request.workspace_id
                    || resolution.workspace_revision() != request.workspace_revision
                {
                    return Err(CoordinatorAuthorityError::Stale);
                }
                Ok(expected)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let locator = expected.locator();
                    let resolved = authority
                        .control()?
                        .resolve_workspace(
                            locator.location_id(),
                            locator.expected_revision(),
                            locator.clone(),
                        )
                        .map_err(map_control_error)?;
                    if resolved != expected {
                        return Err(CoordinatorAuthorityError::Stale);
                    }
                    Ok(resolved)
                })
                .await
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
    /// The server-created process identity is required scope metadata, not
    /// Workspace authority; Forge supplies the fresh observation clock.
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
        let runtime_instance_id = match &self.transport {
            SourceTransport::Control {
                runtime_instance_id,
                ..
            } => *runtime_instance_id,
            SourceTransport::Http(_) => return Err(CoordinatorAuthorityError::Unavailable),
        };
        self.control()?
            .authorize_activity(
                scope,
                FileActivityScope {
                    activity_id: ActivityId::from_uuid(activity_id),
                    runtime_instance_id,
                    observed_at: TimestampMillis::new(
                        crate::current_timestamp_millis()
                            .map_err(|_| CoordinatorAuthorityError::Unavailable)?,
                    )
                    .map_err(|_| CoordinatorAuthorityError::Unavailable)?,
                },
            )
            .map_err(map_control_error)
    }

    /// Resolve the exact Forge Coordinator actor for one `AgentRequest` submit.
    ///
    /// The scope is selected by Workspace identity only; Forge then verifies
    /// the Activity, Session, Agent and principal against durable state.
    ///
    /// # Errors
    /// Returns `Unavailable` for an unconfigured/unknown Workspace or transport
    /// failure, `Denied` for missing/foreign actor identity, and `Stale` for
    /// scope or Coordinator revision fences.
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

fn workspace_scope_wire(scope: &ResolvedWorkspaceScope) -> crate::wire::WorkspaceQueryScope {
    crate::wire::WorkspaceQueryScope {
        workspace_id: scope.workspace_id().into_uuid(),
        workspace_revision: scope.workspace_revision().get(),
        location_id: scope.locator().location_id().into_uuid(),
        location_revision: scope.locator().expected_revision().get(),
        locator: scope.locator().encode(),
    }
}

fn resolved_workspace_scope(
    scope: &WorkspaceProjectScope,
) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
    if scope.workspace_id().is_nil()
        || scope.location_id().is_nil()
        || scope.location_revision() <= 0
        || scope.workspace_revision() <= 0
    {
        return Err(CoordinatorAuthorityError::Stale);
    }
    let locator =
        WorkspaceLocator::decode(scope.locator()).map_err(|_| CoordinatorAuthorityError::Stale)?;
    if locator.encode() != scope.locator()
        || locator.location_id().into_uuid() != scope.location_id()
        || locator.expected_revision().get() != scope.location_revision()
    {
        return Err(CoordinatorAuthorityError::Stale);
    }
    let workspace_revision = WorkspaceRevision::new(scope.workspace_revision())
        .map_err(|_| CoordinatorAuthorityError::Stale)?;
    Ok(ResolvedWorkspaceScope::from_observation(
        WorkspaceId::from_uuid(scope.workspace_id()),
        workspace_revision,
        locator,
    ))
}

fn workspace_scope_from_wire(
    request: &crate::wire::WorkspaceQueryScope,
) -> Result<ResolvedWorkspaceScope, CoordinatorAuthorityError> {
    let scope =
        WorkspaceProjectScope::try_from(request).map_err(|_| CoordinatorAuthorityError::Stale)?;
    resolved_workspace_scope(&scope)
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
    use forge_control::WorkspaceLocator;
    use forge_domain::{LocationId, LocationRevision, WorkspaceId, WorkspaceRevision};
    use std::path::Path;

    fn workspace_scope(
        workspace_id: u128,
        location_id: u128,
        workspace_revision: i64,
        relative_root: &str,
    ) -> Result<ResolvedWorkspaceScope, Box<dyn std::error::Error>> {
        Ok(ResolvedWorkspaceScope::from_observation(
            WorkspaceId::from_uuid(uuid::Uuid::from_u128(workspace_id)),
            WorkspaceRevision::new(workspace_revision)?,
            WorkspaceLocator::new(
                LocationId::from_uuid(uuid::Uuid::from_u128(location_id)),
                LocationRevision::new(1)?,
                Path::new(relative_root),
            )?,
        ))
    }

    fn authority(
        static_scope: ResolvedWorkspaceScope,
    ) -> Result<CoordinatorAuthority, Box<dyn std::error::Error>> {
        let commands = crate::forge_commands::ForgeCommandHttpClient::new(
            "127.0.0.1:55221".parse()?,
            "projection-test-secret",
            std::time::Duration::from_secs(1),
        )?;
        Ok(CoordinatorAuthority::from_http(
            crate::forge_source::ForgeSourceHttpClient::new(commands),
            vec![static_scope],
        )?)
    }

    #[test]
    fn enrollment_projection_is_shared_ordered_and_never_falls_back_when_uncertain()
    -> Result<(), Box<dyn std::error::Error>> {
        let static_scope = workspace_scope(0xA201, 0xA202, 1, "static")?;
        let first_enrollment = workspace_scope(0xA201, 0xA202, 2, "enrolled-v1")?;
        let current_enrollment = workspace_scope(0xA201, 0xA202, 3, "enrolled-v2")?;
        let authority = authority(static_scope.clone())?;
        authority.hydrate_workspace_project_enrollments(&[])?;
        assert!(authority.workspace_authorized(&static_scope)?);

        let cloned_authority = authority.clone();
        assert!(cloned_authority.publish_workspace_project_enrollment(
            &first_enrollment,
            WorkspaceProjectBindingRevision::new(1)?,
        ));
        assert!(authority.workspace_authorized(&first_enrollment)?);
        assert!(!authority.workspace_authorized(&static_scope)?);

        assert!(cloned_authority.publish_workspace_project_enrollment(
            &current_enrollment,
            WorkspaceProjectBindingRevision::new(2)?,
        ));
        assert!(!authority.workspace_authorized(&first_enrollment)?);
        assert!(authority.workspace_authorized(&current_enrollment)?);
        assert!(!authority.publish_workspace_project_enrollment(
            &first_enrollment,
            WorkspaceProjectBindingRevision::new(1)?,
        ));
        assert_eq!(
            authority.workspace_reference(current_enrollment.workspace_id().into_uuid())?,
            workspace_scope_wire(&current_enrollment)
        );

        authority.mark_workspace_project_projection_unavailable(
            current_enrollment.workspace_id().into_uuid(),
        );
        assert_eq!(
            authority.workspace_authorized(&static_scope),
            Err(CoordinatorAuthorityError::Unavailable)
        );
        assert_eq!(
            authority.workspace_reference(static_scope.workspace_id().into_uuid()),
            Err(CoordinatorAuthorityError::Unavailable)
        );
        Ok(())
    }

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
}
