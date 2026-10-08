//! Checked, read-only source observations for binding registration.
//!
//! These values preserve the binding identity returned by Forge without
//! reconstructing Forge's opaque reservation or activation receipts. They are
//! data for Operations to validate and journal, not Forge effect capabilities.

use crate::OperationsError;
use forge_domain::{
    AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentTransportKind, TimestampMillis,
    WorkspaceId,
};
use forge_persistence::{
    AgentBindingActivation, AgentBindingReservation, AgentBindingReservationRequest,
};
use uuid::Uuid;

/// Read-only source observation of one durable Forge binding reservation.
///
/// The request remains the exact typed identity used by Forge. Construction
/// checks the source tuple before Operations uses this
/// observation as registration evidence.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingReservationObservation {
    migration_id: Uuid,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    freeze_revision: u64,
    request: AgentBindingReservationRequest,
    reserved_at: TimestampMillis,
}

impl BindingReservationObservation {
    /// Construct a checked source readback using the exact typed identities.
    ///
    /// # Errors
    /// Rejects nil source identities or a zero freeze revision.
    pub fn new(
        migration_id: Uuid,
        destination_id: Uuid,
        workspace_id: WorkspaceId,
        freeze_revision: u64,
        request: AgentBindingReservationRequest,
        reserved_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if migration_id.is_nil() || destination_id.is_nil() || workspace_id.into_uuid().is_nil() {
            return Err(OperationsError::Conflict(
                "reservation observation identity",
            ));
        }
        if freeze_revision == 0 {
            return Err(OperationsError::Conflict(
                "reservation observation freeze revision",
            ));
        }
        Ok(Self {
            migration_id,
            destination_id,
            workspace_id,
            freeze_revision,
            request,
            reserved_at,
        })
    }

    /// Preserve the values of an original Forge reservation readback.
    ///
    /// # Errors
    /// Rejects invalid source identity or freeze metadata.
    pub fn from_native(reservation: &AgentBindingReservation) -> Result<Self, OperationsError> {
        Self::new(
            reservation.migration_id(),
            reservation.destination_id(),
            reservation.workspace_id(),
            reservation.freeze_revision(),
            reservation.request().clone(),
            reservation.reserved_at(),
        )
    }

    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration_id
    }

    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.destination_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn freeze_revision(&self) -> u64 {
        self.freeze_revision
    }

    pub const fn request(&self) -> &AgentBindingReservationRequest {
        &self.request
    }

    #[must_use]
    pub const fn reserved_at(&self) -> TimestampMillis {
        self.reserved_at
    }
}

/// Read-only source observation of one Forge binding activation readback.
///
/// Its nested reservation is checked independently. This type carries no
/// Forge actor, mutation port, or reconstructed native receipt.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingActivationObservation {
    reservation: BindingReservationObservation,
    membership_digest: [u8; 32],
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
    activated_at: TimestampMillis,
}

impl BindingActivationObservation {
    /// Construct a checked activation readback from typed source values.
    ///
    /// # Errors
    /// Rejects the zero digest required to be nonzero by Forge's activation
    /// contract, or an activation timestamp earlier than its reservation.
    pub fn new(
        reservation: BindingReservationObservation,
        membership_digest: [u8; 32],
        display_name: AgentDisplayName,
        declared_role: AgentDeclaredRole,
        transport: AgentTransportKind,
        activated_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if membership_digest == [0; 32] {
            return Err(OperationsError::Conflict("activation observation digest"));
        }
        if activated_at < reservation.reserved_at() {
            return Err(OperationsError::Conflict(
                "activation observation timestamp",
            ));
        }
        Ok(Self {
            reservation,
            membership_digest,
            display_name,
            declared_role,
            transport,
            activated_at,
        })
    }

    /// Preserve the values of an original Forge activation readback.
    ///
    /// # Errors
    /// Rejects invalid source metadata, digest or activation timestamp.
    pub fn from_native(activation: &AgentBindingActivation) -> Result<Self, OperationsError> {
        Self::new(
            BindingReservationObservation::from_native(activation.reservation())?,
            activation.membership_digest(),
            activation.display_name().clone(),
            activation.declared_role().clone(),
            activation.transport(),
            activation.activated_at(),
        )
    }

    pub const fn reservation(&self) -> &BindingReservationObservation {
        &self.reservation
    }

    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.reservation.migration_id()
    }

    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.reservation.destination_id()
    }

    #[must_use]
    pub const fn freeze_revision(&self) -> u64 {
        self.reservation.freeze_revision()
    }

    pub const fn request(&self) -> &AgentBindingReservationRequest {
        self.reservation.request()
    }

    #[must_use]
    pub const fn reserved_at(&self) -> TimestampMillis {
        self.reservation.reserved_at()
    }

    #[must_use]
    pub const fn membership_digest(&self) -> [u8; 32] {
        self.membership_digest
    }

    #[must_use]
    pub const fn display_name(&self) -> &AgentDisplayName {
        &self.display_name
    }

    #[must_use]
    pub const fn declared_role(&self) -> &AgentDeclaredRole {
        &self.declared_role
    }

    #[must_use]
    pub const fn transport(&self) -> AgentTransportKind {
        self.transport
    }

    #[must_use]
    pub const fn activated_at(&self) -> TimestampMillis {
        self.activated_at
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request().request_id()
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.request().agent_id()
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.reservation.workspace_id()
    }
}
