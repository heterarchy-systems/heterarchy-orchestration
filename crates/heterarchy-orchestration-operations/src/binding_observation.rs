//! Checked, read-only source observations for binding registration.
//!
//! These values preserve the binding identity returned by Forge without
//! reconstructing Forge's opaque reservation or activation receipts. They are
//! data for Operations to validate and journal, not Forge effect capabilities.

use crate::OperationsError;
use forge_domain::{
    ActivityId, AgentBindingRejectionReason, AgentBindingRejectionReceipt, AgentDeclaredRole,
    AgentDisplayName, AgentInstanceId, AgentSessionId, AgentTransportKind, AuthenticatedPrincipal,
    TimestampMillis, WorkspaceId, WorkspaceRootFingerprint,
};
use forge_persistence::{
    AgentBindingActivation, AgentBindingReservation, AgentBindingReservationRequest,
};
use sha2::{Digest, Sha256};
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

/// Read-only source observation of one durable Forge binding rejection.
///
/// Every source fence is retained, but this value carries no cancellation,
/// activation, or successor grant.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingRejectionObservation {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    migration_id: Uuid,
    destination_id: Uuid,
    expected_freeze_revision: u64,
    manifest_digest: [u8; 32],
    issuer_id: Uuid,
    workspace_root_fingerprint: WorkspaceRootFingerprint,
    binding_digest: [u8; 32],
    delegation_digest: [u8; 32],
    membership_digest: [u8; 32],
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
    reason: AgentBindingRejectionReason,
    reserved_at: TimestampMillis,
    requested_at: TimestampMillis,
    observed_at: TimestampMillis,
    rejected_at: TimestampMillis,
    request_digest: [u8; 32],
    receipt_digest: [u8; 32],
}

impl BindingRejectionObservation {
    /// Construct one checked terminal source observation.
    ///
    /// # Errors
    /// Rejects invalid identities, zero fences, or timestamp regression.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: Uuid,
        workspace_id: WorkspaceId,
        activity_id: ActivityId,
        agent_id: AgentInstanceId,
        session_id: AgentSessionId,
        principal: AuthenticatedPrincipal,
        migration_id: Uuid,
        destination_id: Uuid,
        expected_freeze_revision: u64,
        manifest_digest: [u8; 32],
        issuer_id: Uuid,
        workspace_root_fingerprint: WorkspaceRootFingerprint,
        binding_digest: [u8; 32],
        delegation_digest: [u8; 32],
        membership_digest: [u8; 32],
        display_name: AgentDisplayName,
        declared_role: AgentDeclaredRole,
        transport: AgentTransportKind,
        reason: AgentBindingRejectionReason,
        reserved_at: TimestampMillis,
        requested_at: TimestampMillis,
        observed_at: TimestampMillis,
        rejected_at: TimestampMillis,
        request_digest: [u8; 32],
        receipt_digest: [u8; 32],
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil()
            || workspace_id.into_uuid().is_nil()
            || activity_id.into_uuid().is_nil()
            || agent_id.into_uuid().is_nil()
            || session_id.into_uuid().is_nil()
            || migration_id.is_nil()
            || destination_id.is_nil()
            || issuer_id.is_nil()
        {
            return Err(OperationsError::Conflict("binding rejection identity"));
        }
        if expected_freeze_revision == 0
            || manifest_digest == [0; 32]
            || binding_digest == [0; 32]
            || delegation_digest == [0; 32]
            || membership_digest == [0; 32]
            || request_digest == [0; 32]
            || receipt_digest == [0; 32]
            || *workspace_root_fingerprint.as_bytes() == [0; 16]
            || reserved_at < requested_at
            || observed_at < reserved_at
            || observed_at < requested_at
            || rejected_at < observed_at
        {
            return Err(OperationsError::Conflict("binding rejection fence"));
        }
        let value = Self {
            request_id,
            workspace_id,
            activity_id,
            agent_id,
            session_id,
            principal,
            migration_id,
            destination_id,
            expected_freeze_revision,
            manifest_digest,
            issuer_id,
            workspace_root_fingerprint,
            binding_digest,
            delegation_digest,
            membership_digest,
            display_name,
            declared_role,
            transport,
            reason,
            reserved_at,
            requested_at,
            observed_at,
            rejected_at,
            request_digest,
            receipt_digest,
        };
        if rejection_receipt_digest(&value) != receipt_digest {
            return Err(OperationsError::Conflict(
                "binding rejection receipt digest",
            ));
        }
        Ok(value)
    }

    /// Preserve the exact immutable Source receipt as a non-capability observation.
    ///
    /// # Errors
    /// Rejects malformed identities, fences, or timestamp ordering.
    pub fn from_native(receipt: &AgentBindingRejectionReceipt) -> Result<Self, OperationsError> {
        let request = receipt.request();
        Self::new(
            request.request_id(),
            request.workspace_id(),
            request.activity_id(),
            request.agent_id(),
            request.session_id(),
            request.principal(),
            request.migration_id(),
            request.destination_id(),
            request.expected_freeze_revision(),
            request.manifest_digest(),
            request.issuer_id(),
            request.workspace_root_fingerprint(),
            request.binding_digest(),
            request.delegation_digest(),
            request.membership_digest(),
            request.display_name().clone(),
            request.declared_role().clone(),
            request.transport(),
            request.reason(),
            request.reserved_at(),
            request.requested_at(),
            request.observed_at(),
            receipt.rejected_at(),
            receipt.request_digest(),
            receipt.receipt_digest(),
        )
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }
    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }
    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
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
    pub const fn expected_freeze_revision(&self) -> u64 {
        self.expected_freeze_revision
    }
    #[must_use]
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    #[must_use]
    pub const fn issuer_id(&self) -> Uuid {
        self.issuer_id
    }
    #[must_use]
    pub const fn workspace_root_fingerprint(&self) -> WorkspaceRootFingerprint {
        self.workspace_root_fingerprint
    }
    #[must_use]
    pub const fn binding_digest(&self) -> [u8; 32] {
        self.binding_digest
    }
    #[must_use]
    pub const fn delegation_digest(&self) -> [u8; 32] {
        self.delegation_digest
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

    pub const fn reason(&self) -> AgentBindingRejectionReason {
        self.reason
    }

    #[must_use]
    pub const fn reserved_at(&self) -> TimestampMillis {
        self.reserved_at
    }

    #[must_use]
    pub const fn requested_at(&self) -> TimestampMillis {
        self.requested_at
    }

    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }

    #[must_use]
    pub const fn rejected_at(&self) -> TimestampMillis {
        self.rejected_at
    }

    #[must_use]
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    #[must_use]
    pub const fn receipt_digest(&self) -> [u8; 32] {
        self.receipt_digest
    }
}

fn rejection_receipt_digest(value: &BindingRejectionObservation) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"forge.agent.binding.rejection.receipt.v1\0");
    digest.update(value.request_digest());
    digest.update(value.request_id().as_bytes());
    digest.update(value.workspace_id().into_uuid().as_bytes());
    digest.update(value.activity_id().into_uuid().as_bytes());
    digest.update(value.agent_id().into_uuid().as_bytes());
    digest.update(value.session_id().into_uuid().as_bytes());
    match value.principal() {
        AuthenticatedPrincipal::TrustedStdio => digest.update([0]),
        AuthenticatedPrincipal::DirectToken(id) => {
            digest.update([1]);
            digest.update(id.into_uuid().as_bytes());
        }
        AuthenticatedPrincipal::OAuthGrant(id) => {
            digest.update([2]);
            digest.update(id.into_uuid().as_bytes());
        }
    }
    digest.update(value.migration_id().as_bytes());
    digest.update(value.destination_id().as_bytes());
    digest.update(value.expected_freeze_revision().to_le_bytes());
    digest.update(value.manifest_digest());
    digest.update(value.issuer_id().as_bytes());
    digest.update(value.workspace_root_fingerprint().as_bytes());
    digest.update(value.binding_digest());
    digest.update(value.delegation_digest());
    digest.update(value.membership_digest());
    hash_text(&mut digest, value.display_name().as_str());
    hash_text(&mut digest, value.declared_role().as_str());
    digest.update([match value.transport() {
        AgentTransportKind::Retired => 0,
        AgentTransportKind::Stdio => 1,
        AgentTransportKind::StreamableHttp => 2,
        AgentTransportKind::Other => 3,
    }]);
    digest.update(value.reserved_at().get().to_le_bytes());
    digest.update(value.requested_at().get().to_le_bytes());
    digest.update(value.observed_at().get().to_le_bytes());
    digest.update(value.rejected_at().get().to_le_bytes());
    digest.finalize().into()
}

fn hash_text(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value.as_bytes());
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
