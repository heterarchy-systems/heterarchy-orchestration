//! Exact departure observations through the configured Forge source transport.

use forge_control::{AgentDepartureReconcileReadRequest, AgentDepartureReconcileRequest};
use heterarchy_orchestration_operations::{
    AgentDepartureReconciliationRequest, DepartureObservation,
};
use tokio::sync::OwnedSemaphorePermit;

use super::{
    CoordinatorAuthority, CoordinatorAuthorityError, SourceTransport, control_job,
    map_control_error, map_source_error,
};
use crate::{
    forge_membership::{ForgeMembershipHttpClient, MembershipSourceReference},
    wire::WorkspaceQueryScope,
};

impl CoordinatorAuthority {
    pub(crate) async fn departure_by_identity(
        &self,
        scope: WorkspaceQueryScope,
        reference: MembershipSourceReference,
        request_id: uuid::Uuid,
        session_id: forge_domain::AgentSessionId,
        principal: forge_domain::AuthenticatedPrincipal,
        permit: OwnedSemaphorePermit,
    ) -> Result<Option<DepartureObservation>, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let trusted = self.trusted_scope(&scope)?;
                ForgeMembershipHttpClient::new(client.commands().clone())
                    .departure_by_identity(&trusted, &reference, request_id, session_id, principal)
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let receipt = authority
                        .control()?
                        .agent_departure_reconciliation(AgentDepartureReconcileReadRequest {
                            scope: resolved,
                            request_id,
                            session_id,
                            principal,
                        })
                        .map_err(map_control_error)?;
                    receipt
                        .as_ref()
                        .map(DepartureObservation::from)
                        .map(|receipt| validate_reference(receipt, reference))
                        .transpose()
                })
                .await
            }
        }
    }

    pub(crate) async fn handoff_target_principal(
        &self,
        scope: WorkspaceQueryScope,
        selector: HandoffTargetSelector,
        permit: OwnedSemaphorePermit,
    ) -> Result<forge_domain::AuthenticatedPrincipal, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(_) => Err(CoordinatorAuthorityError::Unavailable),
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let context = authority
                        .control()?
                        .agent_get_context(forge_control::AgentContextRequest {
                            scope: resolved,
                            activity_id: selector.activity_id,
                            agent_id: selector.agent_id,
                            now: forge_domain::TimestampMillis::new(
                                crate::current_timestamp_millis()
                                    .map_err(|_| CoordinatorAuthorityError::Unavailable)?,
                            )
                            .map_err(|_| CoordinatorAuthorityError::Unavailable)?,
                            focus_event_limit: forge_domain::AgentListLimit::DEFAULT,
                            work_update_limit: forge_domain::AgentListLimit::DEFAULT,
                        })
                        .map_err(map_control_error)?
                        .ok_or(CoordinatorAuthorityError::Denied)?;
                    let agent = context.attachment().agent();
                    if agent.id() != selector.agent_id
                        || agent.activity_id() != selector.activity_id
                        || agent.session_id() != Some(selector.session_id)
                        || agent.revision() != selector.revision
                        || !agent.attachment_state().is_attached()
                    {
                        return Err(CoordinatorAuthorityError::Stale);
                    }
                    Ok(agent.principal())
                })
                .await
            }
        }
    }

    pub(crate) async fn departure_read(
        &self,
        scope: WorkspaceQueryScope,
        reference: MembershipSourceReference,
        request: AgentDepartureReconciliationRequest,
        permit: OwnedSemaphorePermit,
    ) -> Result<Option<DepartureObservation>, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let trusted = self.trusted_scope(&scope)?;
                ForgeMembershipHttpClient::new(client.commands().clone())
                    .departure_reconciliation(&trusted, &reference, &request)
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let receipt = authority
                        .control()?
                        .agent_departure_reconciliation(AgentDepartureReconcileReadRequest {
                            scope: resolved,
                            request_id: request.request_id(),
                            session_id: request.session_id(),
                            principal: request.principal(),
                        })
                        .map_err(map_control_error)?;
                    receipt
                        .as_ref()
                        .map(DepartureObservation::from)
                        .map(|observation| validate_reference(observation, reference))
                        .transpose()
                })
                .await
            }
        }
    }

    pub(crate) async fn departure_reconcile(
        &self,
        scope: WorkspaceQueryScope,
        reference: MembershipSourceReference,
        request: AgentDepartureReconciliationRequest,
        permit: OwnedSemaphorePermit,
    ) -> Result<DepartureObservation, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let trusted = self.trusted_scope(&scope)?;
                ForgeMembershipHttpClient::new(client.commands().clone())
                    .reconcile_departure(&trusted, &reference, &request)
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let scope = authority.fresh_workspace(&scope)?;
                    let receipt = authority
                        .control()?
                        .reconcile_agent_departure(AgentDepartureReconcileRequest {
                            scope,
                            reconciliation: request,
                        })
                        .map_err(map_control_error)?;
                    validate_reference(DepartureObservation::from(&receipt), reference)
                })
                .await
            }
        }
    }
}

pub(crate) struct HandoffTargetSelector {
    pub activity_id: forge_domain::ActivityId,
    pub agent_id: forge_domain::AgentInstanceId,
    pub session_id: forge_domain::AgentSessionId,
    pub revision: forge_domain::AgentInstanceRevision,
}

fn validate_reference(
    observation: DepartureObservation,
    reference: MembershipSourceReference,
) -> Result<DepartureObservation, CoordinatorAuthorityError> {
    if observation.destination_id() != reference.destination_id()
        || observation.migration_id() != reference.migration_id()
    {
        return Err(CoordinatorAuthorityError::Stale);
    }
    Ok(observation)
}
