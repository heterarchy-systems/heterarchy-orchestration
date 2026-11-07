//! Checked Profile-owner records for one Home acceptance.

use forge_domain::{
    AgentSessionRevision, HomeHandoffTakeRequest, HomeHandoffTakeRequestDigest,
    OrchestrationIssuerId, TimestampMillis,
};
use forge_persistence::{HomeHandoffAcceptanceOutcomeKind, HomeHandoffAcceptanceOutcomeRecord};
use uuid::Uuid;

use super::HomeAcceptanceProfileError as Error;
use crate::profile_catalog::{AgentProfileId, AgentProfileRevision};

const DIGEST_BYTES: usize = 32;

/// Exact Operations admission reference carried through Source Apply.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceProfileOperationsAdmission {
    acceptance_id: Uuid,
    receipt_id: Uuid,
    receipt_digest: [u8; DIGEST_BYTES],
}

impl HomeAcceptanceProfileOperationsAdmission {
    /// Construct a checked reference to an Operations admission receipt.
    ///
    /// # Errors
    /// Returns an error when the acceptance or receipt ID is nil, or the digest
    /// is zero-filled.
    pub fn new(
        acceptance_id: Uuid,
        receipt_id: Uuid,
        receipt_digest: [u8; DIGEST_BYTES],
    ) -> Result<Self, Error> {
        if acceptance_id.is_nil() || receipt_id.is_nil() || receipt_digest == [0; DIGEST_BYTES] {
            return Err(Error::Invalid("Operations admission reference"));
        }
        Ok(Self {
            acceptance_id,
            receipt_id,
            receipt_digest,
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
}

/// Profile facts that Operations pinned before it admitted this exact take.
/// These fields are observations, not an authorization token.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceProfilePinReference {
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    logical_session_request_id: Uuid,
    linked_profile_revision: AgentProfileRevision,
    linked_session_revision: AgentSessionRevision,
    acquired_at: TimestampMillis,
}

impl HomeAcceptanceProfilePinReference {
    #[allow(clippy::too_many_arguments)]
    /// Construct a checked reference to an acquired recipient Profile pin.
    ///
    /// # Errors
    /// Returns an error when an identity is nil or the link's Profile revision
    /// is newer than the acquired Profile revision.
    pub fn new(
        profile_id: AgentProfileId,
        profile_revision: AgentProfileRevision,
        logical_session_request_id: Uuid,
        linked_profile_revision: AgentProfileRevision,
        linked_session_revision: AgentSessionRevision,
        acquired_at: TimestampMillis,
    ) -> Result<Self, Error> {
        if profile_id.into_uuid().is_nil()
            || logical_session_request_id.is_nil()
            || linked_profile_revision > profile_revision
        {
            return Err(Error::Invalid("Profile pin reference"));
        }
        Ok(Self {
            profile_id,
            profile_revision,
            logical_session_request_id,
            linked_profile_revision,
            linked_session_revision,
            acquired_at,
        })
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(self) -> AgentProfileRevision {
        self.profile_revision
    }

    #[must_use]
    pub const fn logical_session_request_id(self) -> Uuid {
        self.logical_session_request_id
    }

    #[must_use]
    pub const fn linked_profile_revision(self) -> AgentProfileRevision {
        self.linked_profile_revision
    }

    pub const fn linked_session_revision(self) -> AgentSessionRevision {
        self.linked_session_revision
    }

    #[must_use]
    pub const fn acquired_at(self) -> TimestampMillis {
        self.acquired_at
    }
}

/// Data read from Operations after its acceptance became absorbing.
///
/// Construction and these fields do not authenticate the Operations owner.
/// Runtime must read this receipt from the configured Operations catalog and
/// bind it to the exact request before using this value to settle a Profile pin.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceProfileOperationsResolution {
    acceptance_id: Uuid,
    issuer: OrchestrationIssuerId,
    resolution_id: Uuid,
    request_digest: HomeHandoffTakeRequestDigest,
    outcome: HomeHandoffAcceptanceOutcomeKind,
    source_payload_digest: [u8; DIGEST_BYTES],
    resolution_digest: [u8; DIGEST_BYTES],
    admission: Option<HomeAcceptanceProfileOperationsAdmission>,
    profile_pin: Option<HomeAcceptanceProfilePinReference>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct HomeAcceptanceProfileOperationsResolutionParts {
    pub(super) acceptance_id: Uuid,
    pub(super) issuer: OrchestrationIssuerId,
    pub(super) request_digest: HomeHandoffTakeRequestDigest,
    pub(super) outcome: HomeHandoffAcceptanceOutcomeKind,
    pub(super) source_payload_digest: [u8; DIGEST_BYTES],
    pub(super) resolution_id: Uuid,
    pub(super) resolution_digest: [u8; DIGEST_BYTES],
    pub(super) admission: Option<HomeAcceptanceProfileOperationsAdmission>,
    pub(super) profile_pin: Option<HomeAcceptanceProfilePinReference>,
}

impl HomeAcceptanceProfileOperationsResolution {
    #[allow(clippy::too_many_arguments)]
    /// Construct a checked view of an absorbing Operations owner resolution.
    ///
    /// # Errors
    /// Returns an error when the Source, request, issuer, digest, admission, or
    /// Profile-pin references are inconsistent.
    pub fn new(
        source: &HomeHandoffAcceptanceOutcomeRecord,
        acceptance_id: Uuid,
        issuer: OrchestrationIssuerId,
        resolution_id: Uuid,
        request_digest: HomeHandoffTakeRequestDigest,
        outcome: HomeHandoffAcceptanceOutcomeKind,
        source_payload_digest: [u8; DIGEST_BYTES],
        resolution_digest: [u8; DIGEST_BYTES],
        admission: Option<HomeAcceptanceProfileOperationsAdmission>,
        profile_pin: Option<HomeAcceptanceProfilePinReference>,
    ) -> Result<Self, Error> {
        if acceptance_id.is_nil()
            || acceptance_id != source.acceptance_id()
            || issuer.into_uuid().is_nil()
            || issuer != source.issuer()
            || resolution_id.is_nil()
            || request_digest != source.request().request_digest()
            || outcome != source.outcome_kind()
            || source_payload_digest == [0; DIGEST_BYTES]
            || source_payload_digest != *source.payload_digest()
            || resolution_digest == [0; DIGEST_BYTES]
        {
            return Err(Error::Invalid("Operations resolution reference"));
        }
        if let Some(admission) = admission
            && admission.acceptance_id() != acceptance_id
        {
            return Err(Error::Invalid("Operations admission acceptance ID"));
        }
        if outcome == HomeHandoffAcceptanceOutcomeKind::Applied
            && (admission.is_none() || profile_pin.is_none())
        {
            return Err(Error::Invalid(
                "Applied resolution requires admission and Profile pin references",
            ));
        }
        if admission.is_some() && profile_pin.is_none() {
            return Err(Error::Invalid(
                "Operations admission must retain the acquired Profile pin",
            ));
        }
        Ok(Self {
            acceptance_id,
            issuer,
            resolution_id,
            request_digest,
            outcome,
            source_payload_digest,
            resolution_digest,
            admission,
            profile_pin,
        })
    }

    #[must_use]
    pub const fn acceptance_id(self) -> Uuid {
        self.acceptance_id
    }

    pub const fn issuer(self) -> OrchestrationIssuerId {
        self.issuer
    }

    #[must_use]
    pub const fn resolution_id(self) -> Uuid {
        self.resolution_id
    }

    pub const fn request_digest(self) -> HomeHandoffTakeRequestDigest {
        self.request_digest
    }

    pub const fn outcome(self) -> HomeHandoffAcceptanceOutcomeKind {
        self.outcome
    }

    #[must_use]
    pub const fn source_payload_digest(self) -> [u8; DIGEST_BYTES] {
        self.source_payload_digest
    }

    #[must_use]
    pub const fn resolution_digest(self) -> [u8; DIGEST_BYTES] {
        self.resolution_digest
    }

    #[must_use]
    pub const fn admission(self) -> Option<HomeAcceptanceProfileOperationsAdmission> {
        self.admission
    }

    #[must_use]
    pub const fn profile_pin(self) -> Option<HomeAcceptanceProfilePinReference> {
        self.profile_pin
    }

    pub(super) fn matches_source(self, source: &HomeHandoffAcceptanceOutcomeRecord) -> bool {
        let owner_resolution_matches = match (source.outcome_kind(), self.admission) {
            (HomeHandoffAcceptanceOutcomeKind::Applied, Some(admission)) => {
                source.applied().is_some_and(|applied| {
                    admission.acceptance_id() == source.acceptance_id()
                        && admission.receipt_id() == applied.grant().acceptance_receipt_id()
                        && admission.receipt_digest()
                            == *applied.grant().acceptance_receipt_digest().as_bytes()
                        && self.profile_pin.is_some_and(|reference| {
                            reference.profile_id().into_uuid()
                                == applied.grant().recipient_profile_id()
                        })
                })
            }
            (HomeHandoffAcceptanceOutcomeKind::Applied, None) => false,
            (HomeHandoffAcceptanceOutcomeKind::Aborted, admission) => admission
                .is_none_or(|admission| admission.acceptance_id() == source.acceptance_id()),
        };
        self.request_digest == source.request().request_digest()
            && self.acceptance_id == source.acceptance_id()
            && self.issuer == source.issuer()
            && self.outcome == source.outcome_kind()
            && self.source_payload_digest == *source.payload_digest()
            && owner_resolution_matches
    }

    pub(super) fn from_persistence_parts(
        parts: &HomeAcceptanceProfileOperationsResolutionParts,
    ) -> Result<Self, Error> {
        let HomeAcceptanceProfileOperationsResolutionParts {
            acceptance_id,
            issuer,
            request_digest,
            outcome,
            source_payload_digest,
            resolution_id,
            resolution_digest,
            admission,
            profile_pin,
        } = *parts;
        if acceptance_id.is_nil()
            || issuer.into_uuid().is_nil()
            || resolution_id.is_nil()
            || source_payload_digest == [0; DIGEST_BYTES]
            || resolution_digest == [0; DIGEST_BYTES]
            || admission.is_some_and(|value| value.acceptance_id() != acceptance_id)
            || (outcome == HomeHandoffAcceptanceOutcomeKind::Applied
                && (admission.is_none() || profile_pin.is_none()))
            || (admission.is_some() && profile_pin.is_none())
        {
            return Err(Error::Corrupt("Operations resolution pins"));
        }
        Ok(Self {
            acceptance_id,
            issuer,
            resolution_id,
            request_digest,
            outcome,
            source_payload_digest,
            resolution_digest,
            admission,
            profile_pin,
        })
    }
}

/// One immutable Home acceptance Profile pin and, when settled, its owner evidence.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeAcceptanceProfilePinRecord {
    request: HomeHandoffTakeRequest,
    issuer: OrchestrationIssuerId,
    state: HomeAcceptanceProfileState,
    profile_pin: Option<HomeAcceptanceProfilePinReference>,
    source_payload_digest: Option<[u8; DIGEST_BYTES]>,
    source_outcome: Option<HomeHandoffAcceptanceOutcomeKind>,
    source_at: Option<TimestampMillis>,
    operations_resolution: Option<HomeAcceptanceProfileOperationsResolution>,
    created_at: TimestampMillis,
    resolved_at: Option<TimestampMillis>,
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeAcceptanceProfileState {
    Held,
    ResolvedApplied,
    ResolvedAborted,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct HomeAcceptanceProfilePinRecordParts {
    pub(super) request: HomeHandoffTakeRequest,
    pub(super) issuer: OrchestrationIssuerId,
    pub(super) state: HomeAcceptanceProfileState,
    pub(super) profile_pin: Option<HomeAcceptanceProfilePinReference>,
    pub(super) source_payload_digest: Option<[u8; DIGEST_BYTES]>,
    pub(super) source_outcome: Option<HomeHandoffAcceptanceOutcomeKind>,
    pub(super) source_at: Option<TimestampMillis>,
    pub(super) operations_resolution: Option<HomeAcceptanceProfileOperationsResolution>,
    pub(super) created_at: TimestampMillis,
    pub(super) resolved_at: Option<TimestampMillis>,
}

impl HomeAcceptanceProfilePinRecord {
    pub const fn request(&self) -> HomeHandoffTakeRequest {
        self.request
    }

    pub const fn issuer(&self) -> OrchestrationIssuerId {
        self.issuer
    }

    #[must_use]
    pub const fn acceptance_id(&self) -> Uuid {
        self.request.acceptance_id()
    }

    pub const fn state(&self) -> HomeAcceptanceProfileState {
        self.state
    }

    #[must_use]
    pub const fn profile_pin(&self) -> Option<HomeAcceptanceProfilePinReference> {
        self.profile_pin
    }

    #[must_use]
    pub const fn source_payload_digest(&self) -> Option<[u8; DIGEST_BYTES]> {
        self.source_payload_digest
    }

    #[must_use]
    pub const fn source_outcome(&self) -> Option<HomeHandoffAcceptanceOutcomeKind> {
        self.source_outcome
    }

    #[must_use]
    pub const fn source_at(&self) -> Option<TimestampMillis> {
        self.source_at
    }

    #[must_use]
    pub const fn operations_resolution(&self) -> Option<HomeAcceptanceProfileOperationsResolution> {
        self.operations_resolution
    }

    #[must_use]
    pub const fn resolved_at(&self) -> Option<TimestampMillis> {
        self.resolved_at
    }

    #[must_use]
    pub const fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    pub(super) fn from_parts(parts: &HomeAcceptanceProfilePinRecordParts) -> Result<Self, Error> {
        let HomeAcceptanceProfilePinRecordParts {
            request,
            issuer,
            state,
            profile_pin,
            source_payload_digest,
            source_outcome,
            source_at,
            operations_resolution,
            created_at,
            resolved_at,
        } = *parts;
        if issuer.into_uuid().is_nil()
            || match state {
                HomeAcceptanceProfileState::Held => {
                    profile_pin.is_none()
                        || source_payload_digest.is_some()
                        || source_outcome.is_some()
                        || source_at.is_some()
                        || operations_resolution.is_some()
                        || resolved_at.is_some()
                        || profile_pin.is_none_or(|pin| pin.acquired_at() != created_at)
                }
                HomeAcceptanceProfileState::ResolvedApplied => {
                    profile_pin.is_none()
                        || source_payload_digest.is_none()
                        || source_outcome != Some(HomeHandoffAcceptanceOutcomeKind::Applied)
                        || source_at.is_none()
                        || operations_resolution.is_none()
                        || resolved_at.is_none_or(|at| at < created_at)
                }
                HomeAcceptanceProfileState::ResolvedAborted => {
                    source_payload_digest.is_none()
                        || source_outcome != Some(HomeHandoffAcceptanceOutcomeKind::Aborted)
                        || source_at.is_none()
                        || operations_resolution.is_none()
                        || resolved_at.is_none_or(|at| at < created_at)
                }
            }
        {
            return Err(Error::Invalid("stored Home Profile pin state"));
        }
        if source_payload_digest.is_some_and(|digest| digest == [0; DIGEST_BYTES]) {
            return Err(Error::Invalid("stored Source digest"));
        }
        if let Some(operations) = operations_resolution
            && (operations.acceptance_id() != request.acceptance_id()
                || operations.issuer() != issuer
                || operations.request_digest() != request.request_digest()
                || operations.outcome()
                    != source_outcome
                        .ok_or(Error::Invalid("stored operations outcome is missing"))?
                || operations.source_payload_digest()
                    != source_payload_digest
                        .ok_or(Error::Invalid("stored operations Source digest is missing"))?)
        {
            return Err(Error::Invalid("stored Operations resolution pins"));
        }
        Ok(Self {
            request,
            issuer,
            state,
            profile_pin,
            source_payload_digest,
            source_outcome,
            source_at,
            operations_resolution,
            created_at,
            resolved_at,
        })
    }
}

/// Result of acquire: the durable immutable record and whether this inserted it.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeAcceptanceProfileAcquireOutcome {
    Held {
        record: HomeAcceptanceProfilePinRecord,
        created: bool,
    },
    Resolved(HomeAcceptanceProfilePinRecord),
}

/// Bounded Profile-owner page of unresolved pins.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeAcceptanceProfilePendingPage {
    pub(super) records: Vec<HomeAcceptanceProfilePinRecord>,
    pub(super) next_after: Option<Uuid>,
}

impl HomeAcceptanceProfilePendingPage {
    pub fn records(&self) -> &[HomeAcceptanceProfilePinRecord] {
        &self.records
    }

    #[must_use]
    pub const fn next_after(&self) -> Option<Uuid> {
        self.next_after
    }
}

/// Canonical Home request plus the Profile-owner observation used only while
/// attempting acquisition. The request key does not change when the observed
/// Profile revision changes; a resolved absent Aborted row stays absorbing.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceProfileIntent {
    request: HomeHandoffTakeRequest,
    issuer: OrchestrationIssuerId,
    observed_profile: Option<(AgentProfileId, AgentProfileRevision)>,
}

impl HomeAcceptanceProfileIntent {
    /// Construct a checked request plus the observation required for acquisition.
    ///
    /// # Errors
    /// Returns an error when the issuer or an observed Profile identity is nil.
    pub fn new(
        request: HomeHandoffTakeRequest,
        issuer: OrchestrationIssuerId,
        observed_profile: Option<(AgentProfileId, AgentProfileRevision)>,
    ) -> Result<Self, Error> {
        if issuer.into_uuid().is_nil()
            || observed_profile.is_some_and(|(profile_id, _)| profile_id.into_uuid().is_nil())
        {
            return Err(Error::Invalid("Profile intent identity"));
        }
        Ok(Self {
            request,
            issuer,
            observed_profile,
        })
    }

    pub const fn request(self) -> HomeHandoffTakeRequest {
        self.request
    }

    pub const fn issuer(self) -> OrchestrationIssuerId {
        self.issuer
    }

    #[must_use]
    pub const fn observed_profile(self) -> Option<(AgentProfileId, AgentProfileRevision)> {
        self.observed_profile
    }

    #[must_use]
    pub const fn acceptance_id(self) -> Uuid {
        self.request.acceptance_id()
    }

    pub const fn request_digest(self) -> HomeHandoffTakeRequestDigest {
        self.request.request_digest()
    }

    #[must_use]
    pub fn canonical_request_snapshot(self) -> Vec<u8> {
        self.request.canonical_snapshot()
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HomeAcceptanceProfilePin {
    pub(crate) profile_id: AgentProfileId,
    pub(crate) profile_revision: AgentProfileRevision,
    pub(crate) logical_session_request_id: Uuid,
    pub(crate) linked_profile_revision: AgentProfileRevision,
    pub(crate) linked_session_revision: AgentSessionRevision,
    pub(crate) acquired_at: TimestampMillis,
}

impl HomeAcceptanceProfilePin {
    pub(crate) const fn reference(self) -> HomeAcceptanceProfilePinReference {
        HomeAcceptanceProfilePinReference {
            profile_id: self.profile_id,
            profile_revision: self.profile_revision,
            logical_session_request_id: self.logical_session_request_id,
            linked_profile_revision: self.linked_profile_revision,
            linked_session_revision: self.linked_session_revision,
            acquired_at: self.acquired_at,
        }
    }
}
