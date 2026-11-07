//! Checked Session-owner records for Home acceptance holds.

use forge_domain::{
    AgentSessionId, AgentSessionRevision, AuthenticatedPrincipal, HomeHandoffTakeRequest,
    OrchestrationIssuerId, TimestampMillis,
};
use forge_persistence::{HomeHandoffAcceptanceOutcomeKind, HomeHandoffAcceptanceOutcomeRecord};
use uuid::Uuid;

use super::HomeAcceptanceSessionError as Error;

#[cfg(test)]
mod tests;

const DIGEST_BYTES: usize = 32;

/// Immutable intent recorded atomically with the first Session hold.
///
/// This value is checked data, not an authentication token. The caller supplies
/// the authenticated principal and configured issuer separately when acquiring.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceSessionIntent {
    request: HomeHandoffTakeRequest,
    issuer: OrchestrationIssuerId,
    expected_session_revision: AgentSessionRevision,
}

impl HomeAcceptanceSessionIntent {
    /// Build immutable identity data for a future Session hold.
    ///
    /// # Errors
    /// Returns [`Error::InvalidIntent`] when the configured issuer is nil.
    pub fn new(
        request: HomeHandoffTakeRequest,
        issuer: OrchestrationIssuerId,
        expected_session_revision: AgentSessionRevision,
    ) -> Result<Self, Error> {
        if issuer.into_uuid().is_nil() {
            return Err(Error::InvalidIntent("issuer"));
        }
        Ok(Self {
            request,
            issuer,
            expected_session_revision,
        })
    }

    pub const fn request(self) -> HomeHandoffTakeRequest {
        self.request
    }

    #[must_use]
    pub const fn acceptance_id(self) -> Uuid {
        self.request.acceptance_id()
    }

    pub const fn issuer(self) -> OrchestrationIssuerId {
        self.issuer
    }

    pub const fn session_id(self) -> AgentSessionId {
        self.request.recipient().session_id()
    }

    pub const fn principal(self) -> AuthenticatedPrincipal {
        self.request.recipient().principal()
    }

    pub const fn expected_session_revision(self) -> AgentSessionRevision {
        self.expected_session_revision
    }

    #[must_use]
    pub fn canonical_request_snapshot(self) -> Vec<u8> {
        self.request.canonical_snapshot()
    }

    #[must_use]
    pub const fn request_digest(self) -> [u8; DIGEST_BYTES] {
        *self.request.request_digest().as_bytes()
    }
}

/// Original Session hold evidence that Operations binds into its admission.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceSessionHoldReference {
    acceptance_id: Uuid,
    request_digest: [u8; DIGEST_BYTES],
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    session_revision: AgentSessionRevision,
    acquired_at: TimestampMillis,
}

impl HomeAcceptanceSessionHoldReference {
    pub(super) const fn new(
        intent: &HomeAcceptanceSessionIntent,
        acquired_at: TimestampMillis,
    ) -> Self {
        Self {
            acceptance_id: intent.acceptance_id(),
            request_digest: intent.request_digest(),
            session_id: intent.session_id(),
            principal: intent.principal(),
            session_revision: intent.expected_session_revision(),
            acquired_at,
        }
    }

    #[must_use]
    pub const fn acceptance_id(self) -> Uuid {
        self.acceptance_id
    }

    #[must_use]
    pub const fn request_digest(self) -> [u8; DIGEST_BYTES] {
        self.request_digest
    }

    pub const fn session_id(self) -> AgentSessionId {
        self.session_id
    }

    pub const fn principal(self) -> AuthenticatedPrincipal {
        self.principal
    }

    pub const fn session_revision(self) -> AgentSessionRevision {
        self.session_revision
    }

    #[must_use]
    pub const fn acquired_at(self) -> TimestampMillis {
        self.acquired_at
    }
}

/// Exact Operations admission receipt reference carried by a Source grant.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceOperationsAdmission {
    acceptance_id: Uuid,
    receipt_id: Uuid,
    receipt_digest: [u8; DIGEST_BYTES],
    session_hold: HomeAcceptanceSessionHoldReference,
}

impl HomeAcceptanceOperationsAdmission {
    /// Validate a Source grant's Operations receipt reference against its Session hold.
    ///
    /// # Errors
    /// Returns [`Error::InvalidIntent`] for nil receipt fields or when the
    /// admission identity differs from the held acceptance.
    pub fn new(
        acceptance_id: Uuid,
        receipt_id: Uuid,
        receipt_digest: [u8; DIGEST_BYTES],
        session_hold: HomeAcceptanceSessionHoldReference,
    ) -> Result<Self, Error> {
        if acceptance_id.is_nil()
            || receipt_id.is_nil()
            || receipt_digest == [0; DIGEST_BYTES]
            || session_hold.acceptance_id() != acceptance_id
        {
            return Err(Error::InvalidIntent("Operations admission reference"));
        }
        Ok(Self {
            acceptance_id,
            receipt_id,
            receipt_digest,
            session_hold,
        })
    }

    #[must_use]
    pub const fn acceptance_id(self) -> Uuid {
        self.acceptance_id
    }

    #[must_use]
    pub const fn receipt_id(self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn receipt_digest(self) -> [u8; DIGEST_BYTES] {
        self.receipt_digest
    }

    pub const fn session_hold(self) -> HomeAcceptanceSessionHoldReference {
        self.session_hold
    }
}

/// Operations' absorbing resolution receipt consumed when settling a Session
/// hold. Its fields are checked data, not proof that Operations wrote the receipt.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceOperationsResolution {
    acceptance_id: Uuid,
    outcome: HomeHandoffAcceptanceOutcomeKind,
    resolution_digest: [u8; DIGEST_BYTES],
    /// Source payload digest recorded by this Operations resolution.
    source_digest: [u8; DIGEST_BYTES],
    admission: Option<HomeAcceptanceOperationsAdmission>,
}

impl HomeAcceptanceOperationsResolution {
    /// Check an Operations resolution reference against the supplied Source terminal.
    ///
    /// # Errors
    /// Returns [`Error::InvalidResolution`] when the Source identity, payload
    /// digest, outcome, or Applied grant admission does not match the supplied
    /// Operations evidence.
    pub fn new(
        source: &HomeHandoffAcceptanceOutcomeRecord,
        resolution_digest: [u8; DIGEST_BYTES],
        source_digest: [u8; DIGEST_BYTES],
        admission: Option<HomeAcceptanceOperationsAdmission>,
    ) -> Result<Self, Error> {
        let outcome = source.outcome_kind();
        if source.acceptance_id().is_nil() || resolution_digest == [0; DIGEST_BYTES] {
            return Err(Error::InvalidResolution(
                "Source and Operations receipt pins",
            ));
        }
        validate_source_digest(source.payload_digest(), &source_digest)?;
        match (source.outcome_kind(), admission) {
            (HomeHandoffAcceptanceOutcomeKind::Applied, Some(admission)) => {
                let grant = source
                    .applied()
                    .ok_or(Error::InvalidResolution("Applied receipt missing grant"))?
                    .grant();
                if admission.acceptance_id() != source.acceptance_id()
                    || admission.receipt_id() != grant.acceptance_receipt_id()
                    || admission.receipt_digest() != *grant.acceptance_receipt_digest().as_bytes()
                {
                    return Err(Error::InvalidResolution("Applied Operations receipt"));
                }
            }
            (HomeHandoffAcceptanceOutcomeKind::Applied, None) => {
                return Err(Error::InvalidResolution("Applied admission missing"));
            }
            (HomeHandoffAcceptanceOutcomeKind::Aborted, Some(admission))
                if admission.acceptance_id() != source.acceptance_id() =>
            {
                return Err(Error::InvalidResolution(
                    "aborted Operations receipt identity",
                ));
            }
            (HomeHandoffAcceptanceOutcomeKind::Aborted, _) => {}
        }
        Ok(Self {
            acceptance_id: source.acceptance_id(),
            outcome,
            resolution_digest,
            source_digest,
            admission,
        })
    }

    #[must_use]
    pub const fn acceptance_id(self) -> Uuid {
        self.acceptance_id
    }

    pub const fn outcome(self) -> HomeHandoffAcceptanceOutcomeKind {
        self.outcome
    }

    #[must_use]
    pub const fn resolution_digest(self) -> [u8; DIGEST_BYTES] {
        self.resolution_digest
    }

    /// Source payload digest recorded by the durable Operations resolution.
    /// It remains distinct from the resolution row's own digest.
    #[must_use]
    pub const fn source_digest(self) -> [u8; DIGEST_BYTES] {
        self.source_digest
    }

    #[must_use]
    pub const fn admission(self) -> Option<HomeAcceptanceOperationsAdmission> {
        self.admission
    }

    pub(super) fn from_stored(
        acceptance_id: Uuid,
        outcome: HomeHandoffAcceptanceOutcomeKind,
        resolution_digest: [u8; DIGEST_BYTES],
        source_digest: [u8; DIGEST_BYTES],
        admission: Option<HomeAcceptanceOperationsAdmission>,
    ) -> Result<Self, Error> {
        if acceptance_id.is_nil()
            || resolution_digest == [0; DIGEST_BYTES]
            || source_digest == [0; DIGEST_BYTES]
            || admission.is_some_and(|value| value.acceptance_id() != acceptance_id)
            || (matches!(outcome, HomeHandoffAcceptanceOutcomeKind::Applied) && admission.is_none())
        {
            return Err(Error::InvalidResolution("stored Operations reference"));
        }
        Ok(Self {
            acceptance_id,
            outcome,
            resolution_digest,
            source_digest,
            admission,
        })
    }
}

/// Durable Session catalog state for one immutable acceptance identity.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// Boxing this payload would remove `Copy` from the established public snapshot API.
#[allow(clippy::large_enum_variant)]
pub enum HomeAcceptanceSessionState {
    Held {
        acquired_at: TimestampMillis,
    },
    Resolved {
        resolution: HomeAcceptanceSessionResolution,
        held_at: Option<TimestampMillis>,
    },
}

/// Immutable intent plus its current absorbing Session-owner state.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceSessionRecord {
    intent: HomeAcceptanceSessionIntent,
    state: HomeAcceptanceSessionState,
}

impl HomeAcceptanceSessionRecord {
    pub(super) const fn held(
        intent: &HomeAcceptanceSessionIntent,
        acquired_at: TimestampMillis,
    ) -> Self {
        Self {
            intent: *intent,
            state: HomeAcceptanceSessionState::Held { acquired_at },
        }
    }

    pub(super) const fn resolved(
        intent: &HomeAcceptanceSessionIntent,
        resolution: &HomeAcceptanceSessionResolution,
        held_at: Option<TimestampMillis>,
    ) -> Self {
        Self {
            intent: *intent,
            state: HomeAcceptanceSessionState::Resolved {
                resolution: *resolution,
                held_at,
            },
        }
    }

    pub const fn intent(&self) -> HomeAcceptanceSessionIntent {
        self.intent
    }

    pub const fn state(&self) -> HomeAcceptanceSessionState {
        self.state
    }

    /// Return the original acquisition evidence only while this row is Held.
    /// A resolved row remains historical evidence and is no longer a live pin.
    #[must_use]
    pub const fn held_reference(&self) -> Option<HomeAcceptanceSessionHoldReference> {
        match self.state {
            HomeAcceptanceSessionState::Held { acquired_at } => Some(
                HomeAcceptanceSessionHoldReference::new(&self.intent, acquired_at),
            ),
            HomeAcceptanceSessionState::Resolved { .. } => None,
        }
    }
}

/// Absorbing Source and Operations outcome captured by this Session catalog.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceSessionResolution {
    outcome: HomeHandoffAcceptanceOutcomeKind,
    source_digest: [u8; DIGEST_BYTES],
    source_at: TimestampMillis,
    operations: HomeAcceptanceOperationsResolution,
    resolved_at: TimestampMillis,
}

impl HomeAcceptanceSessionResolution {
    pub(super) fn from_receipts(
        intent: &HomeAcceptanceSessionIntent,
        source: &HomeHandoffAcceptanceOutcomeRecord,
        operations: HomeAcceptanceOperationsResolution,
        held_at: Option<TimestampMillis>,
        resolved_at: TimestampMillis,
    ) -> Result<Self, Error> {
        if source.request() != intent.request()
            || source.issuer() != intent.issuer()
            || source.acceptance_id() != intent.acceptance_id()
            || operations.acceptance_id() != intent.acceptance_id()
            || operations.outcome() != source.outcome_kind()
        {
            return Err(Error::InvalidResolution("acceptance identity"));
        }
        validate_source_digest(source.payload_digest(), &operations.source_digest())?;
        if held_at.is_some_and(|acquired_at| resolved_at < acquired_at) {
            return Err(Error::InvalidResolution("local Session resolution time"));
        }
        if let Some(admission) = operations.admission() {
            let reference = admission.session_hold();
            if reference.acceptance_id() != intent.acceptance_id()
                || reference.request_digest() != intent.request_digest()
                || reference.session_id() != intent.session_id()
                || reference.principal() != intent.principal()
                || reference.session_revision() != intent.expected_session_revision()
                || held_at != Some(reference.acquired_at())
            {
                return Err(Error::InvalidResolution(
                    "Operations Session hold reference",
                ));
            }
        }
        if source.outcome_kind() == HomeHandoffAcceptanceOutcomeKind::Applied && held_at.is_none() {
            return Err(Error::InvalidResolution("Applied without held Session"));
        }
        let source_digest = *source.payload_digest();
        if source_digest == [0; DIGEST_BYTES] {
            return Err(Error::InvalidResolution("Source terminal digest"));
        }
        Ok(Self {
            outcome: source.outcome_kind(),
            source_digest,
            source_at: source.source_at(),
            operations,
            resolved_at,
        })
    }

    pub const fn outcome(self) -> HomeHandoffAcceptanceOutcomeKind {
        self.outcome
    }

    #[must_use]
    pub const fn source_digest(self) -> [u8; DIGEST_BYTES] {
        self.source_digest
    }

    #[must_use]
    pub const fn source_at(self) -> TimestampMillis {
        self.source_at
    }

    pub const fn operations(self) -> HomeAcceptanceOperationsResolution {
        self.operations
    }

    #[must_use]
    pub const fn resolved_at(self) -> TimestampMillis {
        self.resolved_at
    }

    pub(super) fn from_stored_parts(
        intent: &HomeAcceptanceSessionIntent,
        parts: &StoredSessionResolutionParts,
    ) -> Result<Self, Error> {
        let StoredSessionResolutionParts {
            outcome,
            source_digest,
            source_at,
            operations_digest,
            admission,
            held_at,
            resolved_at,
        } = *parts;
        if source_digest == [0; DIGEST_BYTES]
            || resolved_at < held_at.unwrap_or(resolved_at)
            || (matches!(outcome, HomeHandoffAcceptanceOutcomeKind::Applied) && held_at.is_none())
        {
            return Err(Error::Corrupt("resolved Session outcome pins"));
        }
        if let Some(admission) = admission {
            let reference = admission.session_hold();
            if reference.acceptance_id() != intent.acceptance_id()
                || reference.request_digest() != intent.request_digest()
                || reference.session_id() != intent.session_id()
                || reference.principal() != intent.principal()
                || reference.session_revision() != intent.expected_session_revision()
                || held_at != Some(reference.acquired_at())
            {
                return Err(Error::Corrupt("resolved Operations Session reference"));
            }
        }
        // The immutable Session row stores this validated Source digest once.
        // The Operations digest commits it and was checked against the actual
        // Source record before the transition was written.
        let operations = HomeAcceptanceOperationsResolution::from_stored(
            intent.acceptance_id(),
            outcome,
            operations_digest,
            source_digest,
            admission,
        )?;
        Ok(Self {
            outcome,
            source_digest,
            source_at,
            operations,
            resolved_at,
        })
    }
}

/// Validated persistence fields used to reconstruct one durable resolution.
#[derive(Debug, Clone, Copy)]
pub(super) struct StoredSessionResolutionParts {
    pub(super) outcome: HomeHandoffAcceptanceOutcomeKind,
    pub(super) source_digest: [u8; DIGEST_BYTES],
    pub(super) source_at: TimestampMillis,
    pub(super) operations_digest: [u8; DIGEST_BYTES],
    pub(super) admission: Option<HomeAcceptanceOperationsAdmission>,
    pub(super) held_at: Option<TimestampMillis>,
    pub(super) resolved_at: TimestampMillis,
}

/// Operations must bind the Source terminal it actually resolved; the caller
/// cannot substitute a digest by echoing the Source object into a DTO.
fn validate_source_digest(
    source_payload_digest: &[u8; DIGEST_BYTES],
    operations_source_digest: &[u8; DIGEST_BYTES],
) -> Result<(), Error> {
    if source_payload_digest == &[0; DIGEST_BYTES]
        || operations_source_digest == &[0; DIGEST_BYTES]
        || source_payload_digest != operations_source_digest
    {
        return Err(Error::InvalidResolution(
            "Operations Source terminal digest",
        ));
    }
    Ok(())
}
