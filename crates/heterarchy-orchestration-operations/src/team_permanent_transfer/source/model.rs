//! Checked dispatch inputs and immutable Source-result projections.

use std::net::SocketAddr;

use forge_domain::{
    ForgeBackendId, HomeTransferSourceAction, HomeTransferSourceBinding, HomeTransferSourceGrant,
    HomeTransferSourceRequest, HomeTransferSourceRequestDigest, HomeTransferSourceScope,
    HomeTransferSourceSubject, HomeTransferSourceWitness, OrchestrationIssuerId, TimestampMillis,
    WorkspaceId, WorkspaceRevision,
};
use forge_persistence::HomeTransferSourceRecord;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::super::{ActorSnapshot, TeamPermanentTransferPreparationReceipt};
use super::schema::{MAX_DISPATCH_PAYLOAD_BYTES, MAX_SOURCE_REQUEST_ENVELOPE_BYTES};
use crate::{OperationalActor, OperationsError, WorkspaceParticipationId, WorkspaceProjectScope};

/// Maximum body size accepted from the Backend's completed Source preflight.
pub const MAX_TEAM_PERMANENT_TRANSFER_SOURCE_BODY_BYTES: usize = 64 * 1024;

/// Digest of the checked business dispatch envelope, distinct from the FHTR
/// request digest and the whole persisted attempt payload digest.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TeamPermanentTransferSourceEnvelopeDigest([u8; 32]);

impl TeamPermanentTransferSourceEnvelopeDigest {
    pub(super) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// The validated Backend preflight identity; no bearer or raw HTTP body is stored.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferSourceWireIdentity {
    transport_request_id: Uuid,
    body_sha256: [u8; 32],
    body_byte_count: u32,
}

impl TeamPermanentTransferSourceWireIdentity {
    /// # Errors
    /// Rejects nil transport identity, empty/oversized bodies, and an empty digest.
    pub fn new(
        transport_request_id: Uuid,
        body_sha256: [u8; 32],
        body_byte_count: usize,
    ) -> Result<Self, OperationsError> {
        if transport_request_id.is_nil()
            || body_sha256 == [0; 32]
            || body_byte_count == 0
            || body_byte_count > MAX_TEAM_PERMANENT_TRANSFER_SOURCE_BODY_BYTES
        {
            return Err(OperationsError::Bounds(
                "permanent transfer Source HTTP body identity",
            ));
        }
        let body_byte_count = u32::try_from(body_byte_count)
            .map_err(|_| OperationsError::Bounds("permanent transfer Source HTTP body length"))?;
        Ok(Self {
            transport_request_id,
            body_sha256,
            body_byte_count,
        })
    }

    #[must_use]
    pub const fn transport_request_id(self) -> Uuid {
        self.transport_request_id
    }

    #[must_use]
    pub const fn body_sha256(self) -> [u8; 32] {
        self.body_sha256
    }

    #[must_use]
    pub const fn body_byte_count(self) -> u32 {
        self.body_byte_count
    }
}

/// Exact first Source preparation attempt. It contains no credential and does
/// not itself call Source; only a newly committed Permit may leave the owner.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferSourceDispatchCommand {
    pub(super) workspace_id: WorkspaceId,
    pub(super) preparation_id: Uuid,
    pub(super) transfer_id: Uuid,
    pub(super) generation: u64,
    pub(super) expected_workspace_leadership_revision: i64,
    pub(super) request_id: Uuid,
    pub(super) actor: OperationalActor,
    pub(super) source_route: SocketAddr,
    pub(super) source_scope: WorkspaceProjectScope,
    pub(super) backend_id: ForgeBackendId,
    pub(super) issuer_id: OrchestrationIssuerId,
    pub(super) service_destination_id: Uuid,
    pub(super) source_request: HomeTransferSourceRequest,
    pub(super) envelope_digest: TeamPermanentTransferSourceEnvelopeDigest,
    pub(super) grant: HomeTransferSourceGrant,
    pub(super) wire_identity: TeamPermanentTransferSourceWireIdentity,
}

impl TeamPermanentTransferSourceDispatchCommand {
    /// Construct one complete, pinned source attempt.
    ///
    /// The Source request is supplied by Backend preflight and independently
    /// re-derived from the immutable preparation by the Operations owner.
    /// `wire_identity` records the pre-encoded HTTP body digest and length; it
    /// carries no body bytes or bearer credential.
    ///
    /// # Errors
    /// Rejects nil identities, non-loopback Source routes, mismatched checked
    /// Workspace scope, and grant/service/request mismatches.
    #[allow(
        clippy::too_many_arguments,
        reason = "one constructor checks the full immutable Source dispatch context"
    )]
    pub fn new(
        workspace_id: WorkspaceId,
        preparation_id: Uuid,
        transfer_id: Uuid,
        generation: u64,
        expected_workspace_leadership_revision: i64,
        request_id: Uuid,
        actor: OperationalActor,
        source_route: SocketAddr,
        source_scope: WorkspaceProjectScope,
        backend_id: ForgeBackendId,
        issuer_id: OrchestrationIssuerId,
        service_destination_id: Uuid,
        source_request: HomeTransferSourceRequest,
        grant: HomeTransferSourceGrant,
        wire_identity: TeamPermanentTransferSourceWireIdentity,
    ) -> Result<Self, OperationsError> {
        if workspace_id.into_uuid().is_nil()
            || preparation_id.is_nil()
            || transfer_id.is_nil()
            || generation == 0
            || i64::try_from(generation).is_err()
            || expected_workspace_leadership_revision < 1
            || request_id.is_nil()
            || backend_id.into_uuid().is_nil()
            || issuer_id.into_uuid().is_nil()
            || service_destination_id.is_nil()
            || !source_route.ip().is_loopback()
            || source_route.port() == 0
            || source_scope.workspace_id() != workspace_id.into_uuid()
            || !super::super::actor_input_is_valid(actor)
        {
            return Err(OperationsError::Bounds(
                "permanent transfer Source dispatch identity",
            ));
        }
        if source_request.witness().preparation_id() != preparation_id
            || source_request.scope().workspace_id() != workspace_id
            || source_request.scope().workspace_revision().get()
                != source_scope.workspace_revision()
            || source_request.scope().backend_id() != backend_id
            || source_request.scope().destination_id() != service_destination_id
            || source_request.subject().transfer_id() != transfer_id
            || source_request.subject().transfer_generation() != generation
            || grant.issuer() != issuer_id
            || grant.action() != HomeTransferSourceAction::Prepare
            || grant.request_digest() != source_request.request_digest()
        {
            return Err(OperationsError::Conflict(
                "permanent transfer Source dispatch request pins",
            ));
        }
        let source_envelope = encode_source_request(&source_request)?;
        let envelope_digest = business_envelope_digest(
            source_route,
            &source_scope,
            backend_id,
            issuer_id,
            service_destination_id,
            &source_envelope,
        )?;
        Ok(Self {
            workspace_id,
            preparation_id,
            transfer_id,
            generation,
            expected_workspace_leadership_revision,
            request_id,
            actor,
            source_route,
            source_scope,
            backend_id,
            issuer_id,
            service_destination_id,
            source_request,
            envelope_digest,
            grant,
            wire_identity,
        })
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn preparation_id(&self) -> Uuid {
        self.preparation_id
    }
    #[must_use]
    pub const fn transfer_id(&self) -> Uuid {
        self.transfer_id
    }
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    #[must_use]
    pub const fn expected_workspace_leadership_revision(&self) -> i64 {
        self.expected_workspace_leadership_revision
    }
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn source_route(&self) -> SocketAddr {
        self.source_route
    }
    #[must_use]
    pub const fn source_scope(&self) -> &WorkspaceProjectScope {
        &self.source_scope
    }
    pub const fn backend_id(&self) -> ForgeBackendId {
        self.backend_id
    }
    pub const fn issuer_id(&self) -> OrchestrationIssuerId {
        self.issuer_id
    }
    #[must_use]
    pub const fn service_destination_id(&self) -> Uuid {
        self.service_destination_id
    }
    pub const fn source_request(&self) -> &HomeTransferSourceRequest {
        &self.source_request
    }
    pub const fn envelope_digest(&self) -> TeamPermanentTransferSourceEnvelopeDigest {
        self.envelope_digest
    }
    pub const fn grant(&self) -> HomeTransferSourceGrant {
        self.grant
    }
    pub const fn wire_identity(&self) -> TeamPermanentTransferSourceWireIdentity {
        self.wire_identity
    }
}

/// Newly committed dispatch eligible for one initial Source send.
/// Replays and readbacks cannot construct this permit. It is a one-shot
/// control-flow result, not persisted or authenticated Source authority.
#[must_use]
#[derive(Debug, PartialEq, Eq)]
pub struct TeamPermanentTransferSourceDispatchPermit {
    record: Box<TeamPermanentTransferSourceDispatchReceipt>,
}

impl TeamPermanentTransferSourceDispatchPermit {
    pub(super) fn new(record: TeamPermanentTransferSourceDispatchReceipt) -> Self {
        Self {
            record: Box::new(record),
        }
    }

    pub fn record(&self) -> &TeamPermanentTransferSourceDispatchReceipt {
        self.record.as_ref()
    }
}

/// The immutable first attempt already exists. It intentionally carries no
/// route, Source grant, wire identity, or send authority.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferSourceDispatchReplay {
    preparation_id: Uuid,
    request_id: Uuid,
    recorded_at: TimestampMillis,
    envelope_digest: TeamPermanentTransferSourceEnvelopeDigest,
}

impl TeamPermanentTransferSourceDispatchReplay {
    pub(super) const fn new(
        preparation_id: Uuid,
        request_id: Uuid,
        recorded_at: TimestampMillis,
        envelope_digest: TeamPermanentTransferSourceEnvelopeDigest,
    ) -> Self {
        Self {
            preparation_id,
            request_id,
            recorded_at,
            envelope_digest,
        }
    }

    #[must_use]
    pub const fn preparation_id(self) -> Uuid {
        self.preparation_id
    }
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn recorded_at(self) -> TimestampMillis {
        self.recorded_at
    }
    pub const fn envelope_digest(self) -> TeamPermanentTransferSourceEnvelopeDigest {
        self.envelope_digest
    }
}

/// First durable admission or an exact original attempt that cannot be resent.
#[must_use]
#[derive(Debug, PartialEq, Eq)]
pub enum TeamPermanentTransferSourceDispatchOutcome {
    Created(TeamPermanentTransferSourceDispatchPermit),
    Existing(TeamPermanentTransferSourceDispatchReplay),
}

/// Immutable first-dispatch snapshot, suitable for authorized readback.
/// Stored route, request, grant and wire identities are historical evidence;
/// they do not authenticate Source or recreate a Created send permit.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferSourceDispatchReceipt {
    pub(super) workspace_id: WorkspaceId,
    pub(super) preparation_id: Uuid,
    pub(super) transfer_id: Uuid,
    pub(super) generation: u64,
    pub(super) intent_digest: [u8; 32],
    pub(super) preparation_digest: [u8; 32],
    pub(super) expected_workspace_leadership_revision: i64,
    pub(super) request_id: Uuid,
    pub(super) actor: ActorSnapshot,
    pub(super) source_route: SocketAddr,
    pub(super) source_scope: WorkspaceProjectScope,
    pub(super) backend_id: ForgeBackendId,
    pub(super) issuer_id: OrchestrationIssuerId,
    pub(super) service_destination_id: Uuid,
    pub(super) source_request: HomeTransferSourceRequest,
    pub(super) source_request_envelope: Vec<u8>,
    pub(super) envelope_digest: TeamPermanentTransferSourceEnvelopeDigest,
    pub(super) grant: HomeTransferSourceGrant,
    pub(super) wire_identity: TeamPermanentTransferSourceWireIdentity,
    pub(super) recorded_at: TimestampMillis,
    pub(super) payload_digest: [u8; 32],
}

impl TeamPermanentTransferSourceDispatchReceipt {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn preparation_id(&self) -> Uuid {
        self.preparation_id
    }
    #[must_use]
    pub const fn transfer_id(&self) -> Uuid {
        self.transfer_id
    }
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    #[must_use]
    pub const fn intent_digest(&self) -> [u8; 32] {
        self.intent_digest
    }
    #[must_use]
    pub const fn preparation_digest(&self) -> [u8; 32] {
        self.preparation_digest
    }
    #[must_use]
    pub const fn expected_workspace_leadership_revision(&self) -> i64 {
        self.expected_workspace_leadership_revision
    }
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn actor(&self) -> &ActorSnapshot {
        &self.actor
    }
    #[must_use]
    pub const fn source_route(&self) -> SocketAddr {
        self.source_route
    }
    #[must_use]
    pub const fn source_scope(&self) -> &WorkspaceProjectScope {
        &self.source_scope
    }
    pub const fn backend_id(&self) -> ForgeBackendId {
        self.backend_id
    }
    pub const fn issuer_id(&self) -> OrchestrationIssuerId {
        self.issuer_id
    }
    #[must_use]
    pub const fn service_destination_id(&self) -> Uuid {
        self.service_destination_id
    }
    pub const fn source_request(&self) -> &HomeTransferSourceRequest {
        &self.source_request
    }
    pub const fn envelope_digest(&self) -> TeamPermanentTransferSourceEnvelopeDigest {
        self.envelope_digest
    }
    #[must_use]
    pub fn source_request_envelope(&self) -> &[u8] {
        &self.source_request_envelope
    }
    pub const fn grant(&self) -> HomeTransferSourceGrant {
        self.grant
    }
    pub const fn wire_identity(&self) -> TeamPermanentTransferSourceWireIdentity {
        self.wire_identity
    }
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }
    #[must_use]
    pub const fn payload_digest(&self) -> [u8; 32] {
        self.payload_digest
    }

    fn validate_storage_witness(&self, raw_source_route: &str) -> Result<(), OperationsError> {
        let expected_envelope_digest = business_envelope_digest(
            self.source_route,
            &self.source_scope,
            self.backend_id,
            self.issuer_id,
            self.service_destination_id,
            &self.source_request_envelope,
        )?;
        if self.workspace_id.into_uuid().is_nil()
            || self.preparation_id.is_nil()
            || self.transfer_id.is_nil()
            || self.generation == 0
            || i64::try_from(self.generation).is_err()
            || self.expected_workspace_leadership_revision < 1
            || self.request_id.is_nil()
            || self.backend_id.into_uuid().is_nil()
            || self.issuer_id.into_uuid().is_nil()
            || self.service_destination_id.is_nil()
            || !self.source_route.ip().is_loopback()
            || self.source_route.port() == 0
            || self.source_route.to_string() != raw_source_route
            || self.envelope_digest != expected_envelope_digest
            || self.source_scope.workspace_id() != self.workspace_id.into_uuid()
            || self.source_request.witness().preparation_id() != self.preparation_id
            || self.source_request.witness().intent_digest() != &self.intent_digest
            || self.source_request.witness().preparation_digest() != &self.preparation_digest
            || self.source_request.scope().workspace_id() != self.workspace_id
            || self.source_request.scope().workspace_revision().get()
                != self.source_scope.workspace_revision()
            || self.source_request.scope().backend_id() != self.backend_id
            || self.source_request.scope().destination_id() != self.service_destination_id
            || self.source_request.subject().transfer_id() != self.transfer_id
            || self.source_request.subject().transfer_generation() != self.generation
            || self.grant.issuer() != self.issuer_id
            || self.grant.action() != HomeTransferSourceAction::Prepare
            || self.grant.request_digest() != self.source_request.request_digest()
            || self.recorded_at < self.grant.issued_at()
            || self.recorded_at >= self.grant.expires_at()
        {
            return Err(OperationsError::Corrupt(
                "permanent transfer Source dispatch identity",
            ));
        }
        self.grant
            .validate_for(
                self.issuer_id,
                HomeTransferSourceAction::Prepare,
                self.source_request.request_digest(),
                self.recorded_at,
            )
            .map_err(|_| OperationsError::Corrupt("permanent transfer Source grant window"))
    }
}

/// Caller-supplied Source snapshot after Backend authenticated the pinned
/// loopback peer and checked the immutable dispatch correlation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferSourceReceiptCommand {
    pub(super) workspace_id: WorkspaceId,
    pub(super) preparation_id: Uuid,
    pub(super) expected_envelope_digest: TeamPermanentTransferSourceEnvelopeDigest,
    pub(super) recorded_at: TimestampMillis,
    pub(super) source_record: HomeTransferSourceRecord,
}

impl TeamPermanentTransferSourceReceiptCommand {
    /// Construct one bounded historical Source fact ingestion request.
    ///
    /// This checks internal input shape only. The Backend must authenticate
    /// Source before creating the command; this constructor is not proof of peer trust.
    ///
    /// # Errors
    /// Rejects nil Workspace/preparation identities and an empty envelope digest.
    pub fn new(
        workspace_id: WorkspaceId,
        preparation_id: Uuid,
        expected_envelope_digest: TeamPermanentTransferSourceEnvelopeDigest,
        recorded_at: TimestampMillis,
        source_record: HomeTransferSourceRecord,
    ) -> Result<Self, OperationsError> {
        if workspace_id.into_uuid().is_nil()
            || preparation_id.is_nil()
            || *expected_envelope_digest.as_bytes() == [0; 32]
        {
            return Err(OperationsError::Bounds(
                "permanent transfer Source receipt identity",
            ));
        }
        Ok(Self {
            workspace_id,
            preparation_id,
            expected_envelope_digest,
            recorded_at,
            source_record,
        })
    }
}

/// Original immutable Source outcome plus its exact local snapshot.
/// Snapshot consistency is checked on write/read; the value itself does not
/// carry or recreate Backend peer-authentication evidence.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferSourceReceiptRecord {
    pub(super) workspace_id: WorkspaceId,
    pub(super) preparation_id: Uuid,
    pub(super) envelope_digest: TeamPermanentTransferSourceEnvelopeDigest,
    pub(super) record: HomeTransferSourceRecord,
    pub(super) snapshot_bytes: Vec<u8>,
    pub(super) snapshot_sha256: [u8; 32],
    pub(super) recorded_at: TimestampMillis,
    pub(super) payload_digest: [u8; 32],
    pub(super) replayed: bool,
}

impl TeamPermanentTransferSourceReceiptRecord {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn preparation_id(&self) -> Uuid {
        self.preparation_id
    }
    pub const fn envelope_digest(&self) -> TeamPermanentTransferSourceEnvelopeDigest {
        self.envelope_digest
    }
    pub const fn source_record(&self) -> &HomeTransferSourceRecord {
        &self.record
    }
    #[must_use]
    pub fn snapshot_bytes(&self) -> &[u8] {
        &self.snapshot_bytes
    }
    #[must_use]
    pub const fn snapshot_sha256(&self) -> [u8; 32] {
        self.snapshot_sha256
    }
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }
    #[must_use]
    pub const fn payload_digest(&self) -> [u8; 32] {
        self.payload_digest
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// First receipt persistence or the exact immutable original already stored.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamPermanentTransferSourceReceiptOutcome {
    Recorded(TeamPermanentTransferSourceReceiptRecord),
    Existing(TeamPermanentTransferSourceReceiptRecord),
}

/// Coherent authorized observation of one dispatch and optional original result.
/// This is audit/readback data only and cannot be converted into a send permit.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferSourceDispatchReadback {
    dispatch: TeamPermanentTransferSourceDispatchReceipt,
    original: Option<TeamPermanentTransferSourceReceiptRecord>,
}

impl TeamPermanentTransferSourceDispatchReadback {
    pub(super) const fn new(
        dispatch: TeamPermanentTransferSourceDispatchReceipt,
        original: Option<TeamPermanentTransferSourceReceiptRecord>,
    ) -> Self {
        Self { dispatch, original }
    }
    pub const fn dispatch(&self) -> &TeamPermanentTransferSourceDispatchReceipt {
        &self.dispatch
    }
    #[must_use]
    pub const fn original_source_record(
        &self,
    ) -> Option<&TeamPermanentTransferSourceReceiptRecord> {
        self.original.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredDispatchPayload {
    pub(super) version: u8,
    pub(super) workspace_id: [u8; 16],
    pub(super) preparation_id: [u8; 16],
    pub(super) transfer_id: [u8; 16],
    pub(super) generation: u64,
    pub(super) intent_digest: [u8; 32],
    pub(super) preparation_digest: [u8; 32],
    pub(super) expected_workspace_leadership_revision: i64,
    pub(super) request_id: [u8; 16],
    pub(super) actor: StoredActor,
    pub(super) source_route: String,
    pub(super) source_scope: StoredWorkspaceProjectScope,
    pub(super) backend_id: [u8; 16],
    pub(super) issuer_id: [u8; 16],
    pub(super) service_destination_id: [u8; 16],
    pub(super) envelope_digest: [u8; 32],
    pub(super) grant: StoredGrant,
    pub(super) wire_identity: StoredWireIdentity,
    pub(super) recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredActor {
    pub(super) activity_id: [u8; 16],
    pub(super) agent_id: [u8; 16],
    pub(super) session_id: [u8; 16],
    pub(super) participation_id: [u8; 16],
    pub(super) profile_id: [u8; 16],
    pub(super) participant_id: [u8; 16],
    pub(super) display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredWorkspaceProjectScope {
    pub(super) workspace_id: [u8; 16],
    pub(super) location_id: [u8; 16],
    pub(super) location_revision: i64,
    pub(super) workspace_revision: i64,
    pub(super) locator: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredGrant {
    pub(super) issuer_id: [u8; 16],
    pub(super) action: u8,
    pub(super) request_digest: [u8; 32],
    pub(super) issued_at_ms: i64,
    pub(super) expires_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredWireIdentity {
    pub(super) transport_request_id: [u8; 16],
    pub(super) body_sha256: [u8; 32],
    pub(super) body_byte_count: u32,
}

pub(super) fn dispatch_payload(
    command: &TeamPermanentTransferSourceDispatchCommand,
    preparation: &TeamPermanentTransferPreparationReceipt,
    actor: &ActorSnapshot,
    recorded_at: TimestampMillis,
) -> StoredDispatchPayload {
    StoredDispatchPayload {
        version: 1,
        workspace_id: command.workspace_id.into_uuid().into_bytes(),
        preparation_id: command.preparation_id.into_bytes(),
        transfer_id: command.transfer_id.into_bytes(),
        generation: command.generation,
        intent_digest: preparation.intent_digest(),
        preparation_digest: preparation.digest(),
        expected_workspace_leadership_revision: command.expected_workspace_leadership_revision,
        request_id: command.request_id.into_bytes(),
        actor: StoredActor::from(actor),
        source_route: command.source_route.to_string(),
        source_scope: StoredWorkspaceProjectScope::from(&command.source_scope),
        backend_id: command.backend_id.into_uuid().into_bytes(),
        issuer_id: command.issuer_id.into_uuid().into_bytes(),
        service_destination_id: command.service_destination_id.into_bytes(),
        envelope_digest: *command.envelope_digest.as_bytes(),
        grant: StoredGrant::from(command.grant),
        wire_identity: StoredWireIdentity::from(command.wire_identity),
        recorded_at_ms: recorded_at.get(),
    }
}

impl From<&ActorSnapshot> for StoredActor {
    fn from(actor: &ActorSnapshot) -> Self {
        Self {
            activity_id: actor.activity_id.into_uuid().into_bytes(),
            agent_id: actor.agent_id.into_uuid().into_bytes(),
            session_id: actor.session_id.into_uuid().into_bytes(),
            participation_id: actor.participation_id.into_uuid().into_bytes(),
            profile_id: actor.profile_id.into_bytes(),
            participant_id: actor.participant_id.into_bytes(),
            display_name: actor.display_name.clone(),
        }
    }
}

impl StoredActor {
    pub(super) fn into_snapshot(self) -> Result<ActorSnapshot, OperationsError> {
        let ids = [
            self.activity_id,
            self.agent_id,
            self.session_id,
            self.participation_id,
            self.profile_id,
            self.participant_id,
        ];
        if ids.iter().any(|value| Uuid::from_bytes(*value).is_nil()) {
            return Err(OperationsError::Corrupt(
                "permanent transfer source actor identity",
            ));
        }
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_bytes(
            self.participation_id,
        ))
        .map_err(|_| OperationsError::Corrupt("permanent transfer source actor participation"))?;
        let display_name = super::super::validate_display_name(self.display_name)?;
        Ok(ActorSnapshot {
            activity_id: forge_domain::ActivityId::from_uuid(Uuid::from_bytes(self.activity_id)),
            agent_id: forge_domain::AgentInstanceId::from_uuid(Uuid::from_bytes(self.agent_id)),
            session_id: forge_domain::AgentSessionId::from_uuid(Uuid::from_bytes(self.session_id)),
            participation_id,
            profile_id: Uuid::from_bytes(self.profile_id),
            participant_id: Uuid::from_bytes(self.participant_id),
            display_name,
        })
    }
}

impl From<&WorkspaceProjectScope> for StoredWorkspaceProjectScope {
    fn from(scope: &WorkspaceProjectScope) -> Self {
        Self {
            workspace_id: scope.workspace_id().into_bytes(),
            location_id: scope.location_id().into_bytes(),
            location_revision: scope.location_revision(),
            workspace_revision: scope.workspace_revision(),
            locator: scope.locator().to_owned(),
        }
    }
}

impl StoredWorkspaceProjectScope {
    pub(super) fn into_scope(self) -> Result<WorkspaceProjectScope, OperationsError> {
        WorkspaceProjectScope::new(
            Uuid::from_bytes(self.workspace_id),
            Uuid::from_bytes(self.location_id),
            self.location_revision,
            self.workspace_revision,
            self.locator,
        )
        .map_err(|_| OperationsError::Corrupt("permanent transfer source Workspace scope"))
    }
}

impl From<HomeTransferSourceGrant> for StoredGrant {
    fn from(grant: HomeTransferSourceGrant) -> Self {
        let action = match grant.action() {
            HomeTransferSourceAction::Prepare => 0,
            HomeTransferSourceAction::AbortUnstarted => 1,
        };
        Self {
            issuer_id: grant.issuer().into_uuid().into_bytes(),
            action,
            request_digest: *grant.request_digest().as_bytes(),
            issued_at_ms: grant.issued_at().get(),
            expires_at_ms: grant.expires_at().get(),
        }
    }
}

impl StoredGrant {
    pub(super) fn into_grant(self) -> Result<HomeTransferSourceGrant, OperationsError> {
        let action = match self.action {
            0 => HomeTransferSourceAction::Prepare,
            _ => {
                return Err(OperationsError::Corrupt("permanent transfer Source action"));
            }
        };
        HomeTransferSourceGrant::new(
            OrchestrationIssuerId::from_uuid(Uuid::from_bytes(self.issuer_id)),
            action,
            HomeTransferSourceRequestDigest::from_bytes(self.request_digest),
            TimestampMillis::new(self.issued_at_ms)
                .map_err(|_| OperationsError::Corrupt("permanent transfer Source grant issue"))?,
            TimestampMillis::new(self.expires_at_ms)
                .map_err(|_| OperationsError::Corrupt("permanent transfer Source grant expiry"))?,
        )
        .map_err(|_| OperationsError::Corrupt("permanent transfer Source grant"))
    }
}

impl From<TeamPermanentTransferSourceWireIdentity> for StoredWireIdentity {
    fn from(identity: TeamPermanentTransferSourceWireIdentity) -> Self {
        Self {
            transport_request_id: identity.transport_request_id.into_bytes(),
            body_sha256: identity.body_sha256,
            body_byte_count: identity.body_byte_count,
        }
    }
}

impl StoredWireIdentity {
    pub(super) fn into_identity(
        self,
    ) -> Result<TeamPermanentTransferSourceWireIdentity, OperationsError> {
        TeamPermanentTransferSourceWireIdentity::new(
            Uuid::from_bytes(self.transport_request_id),
            self.body_sha256,
            usize::try_from(self.body_byte_count)
                .map_err(|_| OperationsError::Corrupt("permanent transfer Source body length"))?,
        )
        .map_err(|_| OperationsError::Corrupt("permanent transfer Source body identity"))
    }
}

pub(super) fn encode_payload(value: &StoredDispatchPayload) -> Result<Vec<u8>, OperationsError> {
    serde_json::to_vec(value).map_err(OperationsError::from)
}

pub(super) fn encode_source_request(
    request: &HomeTransferSourceRequest,
) -> Result<Vec<u8>, OperationsError> {
    let bytes = forge_persistence::encode_home_transfer_source_request(request)
        .map_err(|_| OperationsError::Corrupt("permanent transfer Source request encode"))?;
    if bytes.is_empty() || bytes.len() > MAX_SOURCE_REQUEST_ENVELOPE_BYTES {
        return Err(OperationsError::Bounds(
            "permanent transfer Source request envelope",
        ));
    }
    Ok(bytes)
}

fn business_envelope_digest(
    source_route: SocketAddr,
    source_scope: &WorkspaceProjectScope,
    backend_id: ForgeBackendId,
    issuer_id: OrchestrationIssuerId,
    service_destination_id: Uuid,
    source_request_envelope: &[u8],
) -> Result<TeamPermanentTransferSourceEnvelopeDigest, OperationsError> {
    let mut digest = Sha256::new();
    digest
        .update(b"heterarchy.orchestration.permanent-team-transfer.source-business-envelope.v1\0");
    let route = source_route.to_string();
    update_framed(&mut digest, route.as_bytes())?;
    digest.update(issuer_id.into_uuid().as_bytes());
    digest.update(source_scope.workspace_id().as_bytes());
    digest.update(source_scope.location_id().as_bytes());
    digest.update(source_scope.location_revision().to_be_bytes());
    digest.update(source_scope.workspace_revision().to_be_bytes());
    update_framed(&mut digest, source_scope.locator().as_bytes())?;
    digest.update(backend_id.into_uuid().as_bytes());
    digest.update(service_destination_id.as_bytes());
    update_framed(&mut digest, source_request_envelope)?;
    Ok(TeamPermanentTransferSourceEnvelopeDigest::from_bytes(
        digest.finalize().into(),
    ))
}

fn update_framed(digest: &mut Sha256, bytes: &[u8]) -> Result<(), OperationsError> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| OperationsError::Bounds("permanent transfer Source envelope field"))?;
    digest.update(length.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

pub(super) fn decode_payload(bytes: &[u8]) -> Result<StoredDispatchPayload, OperationsError> {
    if bytes.is_empty() || bytes.len() > MAX_DISPATCH_PAYLOAD_BYTES {
        return Err(OperationsError::Bounds(
            "permanent transfer Source dispatch payload",
        ));
    }
    let value: StoredDispatchPayload = serde_json::from_slice(bytes)?;
    if encode_payload(&value)?.as_slice() != bytes {
        return Err(OperationsError::Corrupt(
            "permanent transfer Source dispatch canonical payload",
        ));
    }
    Ok(value)
}

pub(super) fn payload_digest(payload: &[u8], source_envelope: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.permanent-team-transfer.source-dispatch.v1\0");
    digest.update(
        u64::try_from(payload.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    digest.update(payload);
    digest.update(
        u64::try_from(source_envelope.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    digest.update(source_envelope);
    digest.finalize().into()
}

pub(super) fn receipt_payload_digest(
    snapshot_bytes: &[u8],
    envelope_digest: [u8; 32],
    recorded_at: TimestampMillis,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.permanent-team-transfer.source-receipt.v1\0");
    digest.update(envelope_digest);
    digest.update(recorded_at.get().to_be_bytes());
    digest.update(
        u64::try_from(snapshot_bytes.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    digest.update(snapshot_bytes);
    digest.finalize().into()
}

pub(super) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn record_from_storage(
    payload: StoredDispatchPayload,
    source_envelope: Vec<u8>,
    expected_payload_digest: [u8; 32],
) -> Result<TeamPermanentTransferSourceDispatchReceipt, OperationsError> {
    if payload.version != 1
        || payload_digest(&encode_payload(&payload)?, &source_envelope) != expected_payload_digest
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer Source dispatch payload digest",
        ));
    }
    if source_envelope.is_empty() || source_envelope.len() > MAX_SOURCE_REQUEST_ENVELOPE_BYTES {
        return Err(OperationsError::Bounds(
            "permanent transfer Source request envelope",
        ));
    }
    let source_request =
        forge_persistence::decode_home_transfer_source_request(&source_envelope)
            .map_err(|_| OperationsError::Corrupt("permanent transfer Source request envelope"))?;
    let source_scope = payload.source_scope.into_scope()?;
    let grant = payload.grant.into_grant()?;
    let wire_identity = payload.wire_identity.into_identity()?;
    let actor = payload.actor.into_snapshot()?;
    let raw_source_route = payload.source_route;
    let source_route = raw_source_route
        .parse::<SocketAddr>()
        .map_err(|_| OperationsError::Corrupt("permanent transfer Source route"))?;
    let recorded_at = TimestampMillis::new(payload.recorded_at_ms)
        .map_err(|_| OperationsError::Corrupt("permanent transfer Source dispatch time"))?;
    let record = TeamPermanentTransferSourceDispatchReceipt {
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(payload.workspace_id)),
        preparation_id: Uuid::from_bytes(payload.preparation_id),
        transfer_id: Uuid::from_bytes(payload.transfer_id),
        generation: payload.generation,
        intent_digest: payload.intent_digest,
        preparation_digest: payload.preparation_digest,
        expected_workspace_leadership_revision: payload.expected_workspace_leadership_revision,
        request_id: Uuid::from_bytes(payload.request_id),
        actor,
        source_route,
        source_scope,
        backend_id: ForgeBackendId::from_uuid(Uuid::from_bytes(payload.backend_id)),
        issuer_id: OrchestrationIssuerId::from_uuid(Uuid::from_bytes(payload.issuer_id)),
        service_destination_id: Uuid::from_bytes(payload.service_destination_id),
        source_request,
        source_request_envelope: source_envelope,
        envelope_digest: TeamPermanentTransferSourceEnvelopeDigest::from_bytes(
            payload.envelope_digest,
        ),
        grant,
        wire_identity,
        recorded_at,
        payload_digest: expected_payload_digest,
    };
    record.validate_storage_witness(&raw_source_route)?;
    Ok(record)
}

/// Derive the canonical Source request from one checked preparation and the
/// exact Workspace/Backend service pins that Backend validated in preflight.
///
/// # Errors
/// Returns a conflict when the checked Workspace/service scope differs from
/// the preparation's Operations destination, or corruption when the immutable
/// preparation cannot form a valid Source request.
pub fn derive_team_permanent_transfer_source_request(
    preparation: &TeamPermanentTransferPreparationReceipt,
    source_scope: &WorkspaceProjectScope,
    backend_id: ForgeBackendId,
    service_destination_id: Uuid,
) -> Result<HomeTransferSourceRequest, OperationsError> {
    if source_scope.workspace_id() != preparation.workspace_id().into_uuid()
        || backend_id.into_uuid().is_nil()
        || service_destination_id.is_nil()
        || service_destination_id != preparation.operations_destination_id()
    {
        return Err(OperationsError::Conflict(
            "permanent transfer Source Workspace/service scope",
        ));
    }
    let workspace_revision = WorkspaceRevision::new(source_scope.workspace_revision())
        .map_err(|_| OperationsError::Conflict("permanent transfer Source Workspace revision"))?;
    let scope = HomeTransferSourceScope::new(
        preparation.workspace_id(),
        workspace_revision,
        backend_id,
        service_destination_id,
    )
    .map_err(|_| OperationsError::Conflict("permanent transfer Source scope"))?;
    let subject = HomeTransferSourceSubject::new(
        preparation.transfer_id(),
        preparation.generation(),
        preparation.target_profile_id(),
        preparation.target_participation_id().into_uuid(),
        preparation.source_team_id(),
        preparation.destination_team_id(),
        preparation.source_membership_revision(),
        preparation.participation_revision(),
    )
    .map_err(|_| OperationsError::Corrupt("permanent transfer preparation subject"))?;
    let witness = HomeTransferSourceWitness::new(
        preparation.preparation_id(),
        preparation.intent_digest(),
        preparation.digest(),
    )
    .map_err(|_| OperationsError::Corrupt("permanent transfer preparation witness"))?;
    let sessions = preparation.sessions().collect::<Vec<_>>();
    let bindings = preparation
        .bindings()
        .map(|binding| {
            HomeTransferSourceBinding::new(
                binding.activity_id(),
                binding.agent_id(),
                binding.session_id(),
                binding
                    .principal()
                    .map_err(|_| OperationsError::Corrupt("preparation source principal"))?,
            )
            .map_err(|_| OperationsError::Corrupt("preparation source binding"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    HomeTransferSourceRequest::new(witness, scope, subject, sessions, bindings)
        .map_err(|_| OperationsError::Corrupt("permanent transfer preparation Source request"))
}
