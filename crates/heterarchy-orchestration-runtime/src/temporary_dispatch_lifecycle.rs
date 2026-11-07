//! Runtime boundary for consent and inbound-seat reservation after admission.
//!
//! These Operations facts never authorize Forge source work or execution, and
//! no reservation is released by expiry alone.

use std::time::{SystemTime, UNIX_EPOCH};

use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    OperationalActor, OperationsError,
    temporary_dispatch::{
        TemporaryDispatchAdmissionBinding, TemporaryDispatchConsentCommand,
        TemporaryDispatchConsentKind, TemporaryDispatchConsentReceipt,
        TemporaryDispatchConsentWithdrawalCommand, TemporaryDispatchConsentWithdrawalReceipt,
        TemporaryDispatchReservationReceipt, TemporaryDispatchReserveCommand,
    },
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError, participation::ParticipationTarget};

/// Current Workspace/Activity/Session selector for one consent or reservation action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporaryDispatchLifecycleActor {
    /// Workspace containing the admitted request.
    pub workspace_id: Uuid,
    /// Exact immutable admission request identity.
    pub request_id: Uuid,
    /// Current source Activity of the consentor or reservation actor.
    pub activity_id: Uuid,
    /// Open Session owned by the current request principal.
    pub session_id: Uuid,
}

/// One exact consent statement mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporaryDispatchConsentSpec {
    /// Current actor and request selectors.
    pub actor: TemporaryDispatchLifecycleActor,
    /// Unique immutable consent receipt ID.
    pub receipt_id: Uuid,
    /// Which of the current request authorities is consenting.
    pub kind: TemporaryDispatchConsentKind,
    /// Current consent-head revision expected by the caller.
    pub expected_revision: u64,
}

/// One exact consent receipt query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporaryDispatchConsentReadSpec {
    /// Current authenticated author and request selectors.
    pub actor: TemporaryDispatchLifecycleActor,
    /// Exact immutable receipt to resolve.
    pub receipt_id: Uuid,
    /// Consent authority whose binding is rechecked before readback.
    pub kind: TemporaryDispatchConsentKind,
}

/// One exact withdrawal of a named current consent receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporaryDispatchConsentWithdrawalSpec {
    /// Current authenticated author and request selectors.
    pub actor: TemporaryDispatchLifecycleActor,
    /// Unique immutable withdrawal receipt ID.
    pub receipt_id: Uuid,
    /// Exact consent receipt being withdrawn.
    pub consent_receipt_id: Uuid,
    /// Consent authority whose current role must remain valid.
    pub kind: TemporaryDispatchConsentKind,
    /// Current consent-head revision expected by the caller.
    pub expected_revision: u64,
}

/// One exact withdrawal receipt query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporaryDispatchConsentWithdrawalReadSpec {
    /// Current authenticated author and request selectors.
    pub actor: TemporaryDispatchLifecycleActor,
    /// Exact immutable withdrawal receipt.
    pub receipt_id: Uuid,
    /// Exact consent receipt withdrawn by this event.
    pub consent_receipt_id: Uuid,
    /// Consent authority used to revalidate the current request binding.
    pub kind: TemporaryDispatchConsentKind,
}

/// One initial Operations inbound Team reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporaryDispatchReservationSpec {
    /// Current Workspace Orchestrator and request selectors.
    pub actor: TemporaryDispatchLifecycleActor,
    /// Initial reservation revision. Operations currently accepts zero only.
    pub expected_revision: u64,
}

impl OrchestrationRuntime {
    /// Record one exact consent under its authenticated current Actor Session.
    ///
    /// Operations first authorizes the request binding for this consent kind,
    /// then repeats that role and authority validation in the write's own
    /// IMMEDIATE transaction. The read and write are not one cross-authority
    /// atomic snapshot.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, non-current consent authority, invalid
    /// request or revision, clock failure, or uncertain commit.
    pub fn record_temporary_dispatch_consent(
        &self,
        spec: TemporaryDispatchConsentSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TemporaryDispatchConsentReceipt, RuntimeError> {
        let (actor, participation) = current_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let mut catalog = self.catalog()?;
        let binding = authorized_consent_binding(&mut catalog, spec.actor, spec.kind, actor)?;
        let command = TemporaryDispatchConsentCommand::new(
            spec.receipt_id,
            binding,
            spec.kind,
            spec.expected_revision,
            actor,
        )
        .map_err(RuntimeError::persistence)?;
        catalog
            .record_temporary_dispatch_consent(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact consent receipt as its authenticated logical author.
    /// This query does not claim the consent still applies after authority changes.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, non-current consent authority, invalid
    /// identity, or corrupt durable data.
    pub fn read_temporary_dispatch_consent(
        &self,
        spec: TemporaryDispatchConsentReadSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TemporaryDispatchConsentReceipt>, RuntimeError> {
        let (actor, participation) = current_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let mut catalog = self.catalog()?;
        let Some(binding) = catalog
            .temporary_dispatch_consent_readback_binding(
                WorkspaceId::from_uuid(spec.actor.workspace_id),
                spec.actor.request_id,
                spec.receipt_id,
                spec.kind,
                actor,
            )
            .map_err(RuntimeError::persistence)?
        else {
            return Ok(None);
        };
        catalog
            .read_temporary_dispatch_consent(&binding, spec.receipt_id, actor)
            .map_err(RuntimeError::persistence)
    }

    /// Withdraw one consent event while its authority is current.
    ///
    /// This only appends consent history. It does not stop already admitted
    /// source work or release an inbound seat.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, changed role, stale consent/revision,
    /// invalid request, clock failure, or uncertain commit.
    pub fn withdraw_temporary_dispatch_consent(
        &self,
        spec: TemporaryDispatchConsentWithdrawalSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TemporaryDispatchConsentWithdrawalReceipt, RuntimeError> {
        let (actor, participation) = current_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let mut catalog = self.catalog()?;
        let binding = authorized_consent_binding(&mut catalog, spec.actor, spec.kind, actor)?;
        let command = TemporaryDispatchConsentWithdrawalCommand::new(
            spec.receipt_id,
            spec.consent_receipt_id,
            binding,
            spec.expected_revision,
            actor,
        )
        .map_err(RuntimeError::persistence)?;
        catalog
            .withdraw_temporary_dispatch_consent(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact consent-withdrawal event as its authenticated author.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, non-current consent authority, invalid
    /// identity, or corrupt durable data.
    pub fn read_temporary_dispatch_consent_withdrawal(
        &self,
        spec: TemporaryDispatchConsentWithdrawalReadSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TemporaryDispatchConsentWithdrawalReceipt>, RuntimeError> {
        let (actor, participation) = current_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let mut catalog = self.catalog()?;
        let Some(binding) = catalog
            .temporary_dispatch_consent_withdrawal_readback_binding(
                WorkspaceId::from_uuid(spec.actor.workspace_id),
                spec.actor.request_id,
                spec.receipt_id,
                spec.consent_receipt_id,
                spec.kind,
                actor,
            )
            .map_err(RuntimeError::persistence)?
        else {
            return Ok(None);
        };
        catalog
            .read_temporary_dispatch_consent_withdrawal(
                &binding,
                spec.receipt_id,
                spec.consent_receipt_id,
                actor,
            )
            .map_err(RuntimeError::persistence)
    }

    /// Reserve the admitted target's inbound Team seat.
    ///
    /// All three current consents, role epochs, home membership and capacity
    /// are checked by the Operations owner in one IMMEDIATE write. This records
    /// only a reservation, never source preparation or execution authority.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, non-current Workspace Orchestrator,
    /// missing or stale consent, leadership/member changes, quota/capacity,
    /// clock failures, or uncertain commit.
    pub fn reserve_temporary_dispatch(
        &self,
        spec: TemporaryDispatchReservationSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TemporaryDispatchReservationReceipt, RuntimeError> {
        let (actor, participation) = current_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let mut catalog = self.catalog()?;
        let binding = authorized_consent_binding(
            &mut catalog,
            spec.actor,
            TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval,
            actor,
        )?;
        let command = TemporaryDispatchReserveCommand::new(binding, spec.expected_revision, actor)
            .map_err(RuntimeError::persistence)?;
        catalog
            .reserve_temporary_dispatch(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read an inbound seat reservation as the original requesting Leader or
    /// current Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, actor outside those two roles, or
    /// corrupt/mismatched durable state.
    pub fn read_temporary_dispatch_reservation(
        &self,
        actor_spec: TemporaryDispatchLifecycleActor,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TemporaryDispatchReservationReceipt>, RuntimeError> {
        let (actor, participation) = current_actor(actor_spec)?;
        self.require_participation(participation, principal)?;
        let mut catalog = self.catalog()?;
        let binding = catalog
            .temporary_dispatch_reservation_binding(
                WorkspaceId::from_uuid(actor_spec.workspace_id),
                actor_spec.request_id,
                actor,
            )
            .map_err(RuntimeError::persistence)?;
        catalog
            .read_temporary_dispatch_reservation(&binding, actor)
            .map_err(RuntimeError::persistence)
    }
}

fn authorized_consent_binding(
    catalog: &mut heterarchy_orchestration_operations::OperationsCatalog,
    actor_spec: TemporaryDispatchLifecycleActor,
    kind: TemporaryDispatchConsentKind,
    actor: OperationalActor,
) -> Result<TemporaryDispatchAdmissionBinding, RuntimeError> {
    catalog
        .temporary_dispatch_consent_binding(
            WorkspaceId::from_uuid(actor_spec.workspace_id),
            actor_spec.request_id,
            kind,
            actor,
        )
        .map_err(RuntimeError::persistence)
}

fn current_actor(
    target: TemporaryDispatchLifecycleActor,
) -> Result<(OperationalActor, ParticipationTarget), RuntimeError> {
    if target.request_id.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    crate::teams::participant_actor(
        WorkspaceId::from_uuid(target.workspace_id),
        target.activity_id,
        target.session_id,
    )
}

fn trusted_server_time() -> Result<forge_domain::TimestampMillis, OperationsError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OperationsError::Bounds("temporary dispatch trusted clock"))?;
    let millis = i64::try_from(elapsed.as_millis())
        .map_err(|_| OperationsError::Bounds("temporary dispatch trusted clock"))?;
    forge_domain::TimestampMillis::new(millis)
        .map_err(|_| OperationsError::Bounds("temporary dispatch trusted clock"))
}
