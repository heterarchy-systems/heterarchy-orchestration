//! Binding witness transport under the already configured source authority.

use super::{
    CoordinatorAuthority, CoordinatorAuthorityError, SourceTransport, control_job,
    map_control_error, map_source_error,
};
use crate::forge_membership::{ForgeMembershipHttpClient, MembershipSourceReference};
use crate::wire::WorkspaceQueryScope;
use forge_control::{
    AgentBindingActivateRequest, AgentBindingActivationReadRequest,
    AgentBindingReservationReadRequest, AgentBindingReserveRequest,
    AgentOperationsDelegationReadRequest,
};
use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    AgentBindingActivationRequest, AgentBindingReservationRequest, AgentOperationsDelegation,
    BindingActivationObservation, BindingReservationObservation,
};
use tokio::sync::OwnedSemaphorePermit;

impl CoordinatorAuthority {
    /// Verify the exact delegation selected by the existing Operations import.
    pub(crate) async fn membership_delegation(
        &self,
        scope: WorkspaceQueryScope,
        reference: MembershipSourceReference,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                let expected = self.trusted_scope(&scope)?;
                let _delegation = ForgeMembershipHttpClient::new(client.commands().clone())
                    .delegation(expected, &reference)
                    .await
                    .map_err(map_source_error)?
                    .ok_or(CoordinatorAuthorityError::Unavailable)?;
                Ok(())
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    authority.exact_delegation(resolved, reference).map(|_| ())
                })
                .await
            }
        }
    }

    pub(crate) async fn membership_reservation(
        &self,
        scope: WorkspaceQueryScope,
        reference: MembershipSourceReference,
        request: AgentBindingReservationRequest,
        permit: OwnedSemaphorePermit,
    ) -> Result<Option<BindingReservationObservation>, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                ForgeMembershipHttpClient::new(client.commands().clone())
                    .reservation(self.trusted_scope(&scope)?, &reference, &request)
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let receipt = authority
                        .control()?
                        .agent_binding_reservation(AgentBindingReservationReadRequest {
                            scope: resolved,
                            request_id: request.request_id(),
                            session_id: request.session_id(),
                            principal: request.principal(),
                        })
                        .map_err(map_control_error)?;
                    receipt
                        .as_ref()
                        .map(BindingReservationObservation::from_native)
                        .transpose()
                        .map_err(|_| CoordinatorAuthorityError::Unavailable)
                })
                .await
            }
        }
    }

    pub(crate) async fn membership_reserve(
        &self,
        scope: WorkspaceQueryScope,
        reference: MembershipSourceReference,
        request: AgentBindingReservationRequest,
        observed_at: TimestampMillis,
        permit: OwnedSemaphorePermit,
    ) -> Result<BindingReservationObservation, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                ForgeMembershipHttpClient::new(client.commands().clone())
                    .reserve(
                        self.trusted_scope(&scope)?,
                        &reference,
                        &request,
                        observed_at.get(),
                    )
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let delegation = authority.exact_delegation(resolved.clone(), reference)?;
                    let receipt = authority
                        .control()?
                        .reserve_agent_binding(AgentBindingReserveRequest {
                            scope: resolved,
                            freeze: delegation.freeze().clone(),
                            reservation: request,
                            observed_at,
                        })
                        .map_err(map_control_error)?;
                    BindingReservationObservation::from_native(&receipt)
                        .map_err(|_| CoordinatorAuthorityError::Unavailable)
                })
                .await
            }
        }
    }

    pub(crate) async fn membership_activation(
        &self,
        scope: WorkspaceQueryScope,
        reference: MembershipSourceReference,
        request: AgentBindingReservationRequest,
        permit: OwnedSemaphorePermit,
    ) -> Result<Option<BindingActivationObservation>, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                ForgeMembershipHttpClient::new(client.commands().clone())
                    .activation(self.trusted_scope(&scope)?, &reference, &request)
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let receipt = authority
                        .control()?
                        .agent_binding_activation(AgentBindingActivationReadRequest {
                            scope: resolved,
                            request_id: request.request_id(),
                            session_id: request.session_id(),
                            principal: request.principal(),
                        })
                        .map_err(map_control_error)?;
                    receipt
                        .as_ref()
                        .map(BindingActivationObservation::from_native)
                        .transpose()
                        .map_err(|_| CoordinatorAuthorityError::Unavailable)
                })
                .await
            }
        }
    }

    pub(crate) async fn membership_activate(
        &self,
        scope: WorkspaceQueryScope,
        reference: MembershipSourceReference,
        activation: AgentBindingActivationRequest,
        binding: AgentBindingReservationRequest,
        observed_at: TimestampMillis,
        permit: OwnedSemaphorePermit,
    ) -> Result<BindingActivationObservation, CoordinatorAuthorityError> {
        let session_id = binding.session_id();
        let principal = binding.principal();
        match &self.transport {
            SourceTransport::Http(client) => {
                let _permit = permit;
                ForgeMembershipHttpClient::new(client.commands().clone())
                    .activate(
                        self.trusted_scope(&scope)?,
                        &reference,
                        &activation,
                        session_id.into_uuid(),
                        principal,
                        observed_at.get(),
                    )
                    .await
                    .map_err(map_source_error)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let resolved = authority.fresh_workspace(&scope)?;
                    let delegation = authority.exact_delegation(resolved.clone(), reference)?;
                    let receipt = authority
                        .control()?
                        .activate_agent_binding(AgentBindingActivateRequest {
                            scope: resolved,
                            delegation,
                            activation,
                            session_id,
                            principal,
                            observed_at,
                        })
                        .map_err(map_control_error)?;
                    BindingActivationObservation::from_native(&receipt)
                        .map_err(|_| CoordinatorAuthorityError::Unavailable)
                })
                .await
            }
        }
    }

    fn exact_delegation(
        &self,
        scope: forge_control::ResolvedWorkspaceScope,
        reference: MembershipSourceReference,
    ) -> Result<AgentOperationsDelegation, CoordinatorAuthorityError> {
        let delegation = self
            .control()?
            .agent_operations_delegation(AgentOperationsDelegationReadRequest { scope })
            .map_err(map_control_error)?
            .ok_or(CoordinatorAuthorityError::Unavailable)?;
        if delegation.destination_id() != reference.destination_id()
            || delegation.migration_id() != reference.migration_id()
            || delegation.freeze().revision() != reference.freeze_revision()
            || delegation.manifest_digest() != reference.manifest_digest()
        {
            return Err(CoordinatorAuthorityError::Stale);
        }
        Ok(delegation)
    }
}
