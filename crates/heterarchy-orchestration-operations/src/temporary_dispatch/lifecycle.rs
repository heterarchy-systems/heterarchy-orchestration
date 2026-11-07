//! Authenticated consent receipts and the atomic inbound Team reservation.
//!
//! This module owns the Operations-side durable boundary only. A RESERVED row
//! occupies a Team seat; it does not activate temporary dispatch work or grant
//! Forge execution authority.

mod consent;
mod reservation;
mod schema;

#[cfg(test)]
mod tests;

pub(crate) use schema::{
    create_schema as create_lifecycle_schema, schema_objects as lifecycle_schema_objects,
};

use forge_domain::{TimestampMillis, WorkspaceId};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::OperationsError;

use super::{
    OperationalActor, TemporaryDispatchActorProvenance, TemporaryDispatchRequestRecord,
    TemporaryDispatchRequestedScope,
};

const INTENT_DIGEST_DOMAIN: &[u8] = b"heterarchy.orchestration.temporary_dispatch.intent.v1\0";

/// Immutable admission payload used by later consent and reservation writes.
///
/// Construct this only from the Operations admission receipt. The digest binds
/// the immutable request intent, original requester, target, home and receiving
/// Teams, scope, end time, and admission leadership fence. It is an intent
/// digest rather than a digest of every quota-receipt field: admission time and
/// quota lock remain on the existing immutable receipt, and this digest is not
/// source Work evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchAdmissionBinding {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
    leadership_revision: u64,
    actor: TemporaryDispatchActorProvenance,
    target_home_team_id: Uuid,
    target_participant_id: Uuid,
    requested_scope: TemporaryDispatchRequestedScope,
    requested_end_at: TimestampMillis,
    intent_digest: [u8; 32],
}

impl TemporaryDispatchAdmissionBinding {
    /// Build the exact lifecycle binding from a durable admission receipt.
    #[must_use]
    pub fn from_record(record: &TemporaryDispatchRequestRecord) -> Self {
        Self {
            request_id: record.request_id(),
            workspace_id: record.workspace_id(),
            requesting_team_id: record.requesting_team_id(),
            leadership_revision: record.leadership_revision(),
            actor: record.actor().clone(),
            target_home_team_id: record.target_home_team_id(),
            target_participant_id: record.target_participant_id(),
            requested_scope: record.requested_scope().clone(),
            requested_end_at: record.requested_end_at(),
            intent_digest: admission_digest(record),
        }
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
    pub const fn requesting_team_id(&self) -> Uuid {
        self.requesting_team_id
    }

    #[must_use]
    pub const fn leadership_revision(&self) -> u64 {
        self.leadership_revision
    }

    #[must_use]
    pub const fn actor(&self) -> &TemporaryDispatchActorProvenance {
        &self.actor
    }

    #[must_use]
    pub const fn target_home_team_id(&self) -> Uuid {
        self.target_home_team_id
    }

    #[must_use]
    pub const fn target_participant_id(&self) -> Uuid {
        self.target_participant_id
    }

    #[must_use]
    pub fn requested_scope(&self) -> &TemporaryDispatchRequestedScope {
        &self.requested_scope
    }

    #[must_use]
    pub const fn requested_end_at(&self) -> TimestampMillis {
        self.requested_end_at
    }

    #[must_use]
    pub const fn intent_digest(&self) -> [u8; 32] {
        self.intent_digest
    }

    pub(super) fn matches_record(&self, record: &TemporaryDispatchRequestRecord) -> bool {
        self.request_id == record.request_id()
            && self.workspace_id == record.workspace_id()
            && self.requesting_team_id == record.requesting_team_id()
            && self.leadership_revision == record.leadership_revision()
            && same_actor(&self.actor, record.actor())
            && self.target_home_team_id == record.target_home_team_id()
            && self.target_participant_id == record.target_participant_id()
            && self.requested_scope == *record.requested_scope()
            && self.requested_end_at == record.requested_end_at()
            && self.intent_digest == admission_digest(record)
    }
}

/// The three independent consents required before inbound reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporaryDispatchConsentKind {
    /// The target Agent accepts the exact dispatch request.
    TargetAcceptance,
    /// The target's current home-Team Leader consents.
    HomeLeaderConsent,
    /// The current Workspace Orchestrator approves.
    WorkspaceOrchestratorApproval,
}

impl TemporaryDispatchConsentKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::TargetAcceptance => "TARGET_ACCEPTANCE",
            Self::HomeLeaderConsent => "HOME_LEADER_CONSENT",
            Self::WorkspaceOrchestratorApproval => "WORKSPACE_ORCHESTRATOR_APPROVAL",
        }
    }

    pub(super) fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "TARGET_ACCEPTANCE" => Ok(Self::TargetAcceptance),
            "HOME_LEADER_CONSENT" => Ok(Self::HomeLeaderConsent),
            "WORKSPACE_ORCHESTRATOR_APPROVAL" => Ok(Self::WorkspaceOrchestratorApproval),
            _ => Err(OperationsError::Corrupt("temporary dispatch consent kind")),
        }
    }
}

/// One idempotent consent write bound to an immutable admission receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchConsentCommand {
    pub(super) receipt_id: Uuid,
    pub(super) binding: TemporaryDispatchAdmissionBinding,
    pub(super) kind: TemporaryDispatchConsentKind,
    pub(super) expected_revision: u64,
    pub(super) actor: OperationalActor,
}

impl TemporaryDispatchConsentCommand {
    /// Construct one exact consent receipt request.
    ///
    /// `expected_revision` is zero before the first consent mutation for this
    /// request and kind, then the current owner revision read back by the caller.
    ///
    /// # Errors
    /// Rejects nil request/receipt identities, non-participant actors, or an
    /// invalid expected revision.
    pub fn new(
        receipt_id: Uuid,
        binding: TemporaryDispatchAdmissionBinding,
        kind: TemporaryDispatchConsentKind,
        expected_revision: u64,
        actor: OperationalActor,
    ) -> Result<Self, OperationsError> {
        validate_command_identity(receipt_id, &binding, expected_revision, actor)?;
        if receipt_id == binding.request_id {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent receipt identity",
            ));
        }
        Ok(Self {
            receipt_id,
            binding,
            kind,
            expected_revision,
            actor,
        })
    }

    #[must_use]
    pub const fn receipt_id(&self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.binding.request_id
    }

    #[must_use]
    pub const fn kind(&self) -> TemporaryDispatchConsentKind {
        self.kind
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }
}

/// Immutable actor and authority snapshot for one accepted consent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchConsentReceipt {
    pub(super) receipt_id: Uuid,
    pub(super) binding: TemporaryDispatchAdmissionBinding,
    pub(super) kind: TemporaryDispatchConsentKind,
    pub(super) revision: u64,
    pub(super) actor: TemporaryDispatchActorProvenance,
    pub(super) authority_revision: u64,
    pub(super) recorded_at: TimestampMillis,
    pub(super) replayed: bool,
}

impl TemporaryDispatchConsentReceipt {
    #[must_use]
    pub const fn receipt_id(&self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.binding.request_id
    }

    #[must_use]
    pub const fn kind(&self) -> TemporaryDispatchConsentKind {
        self.kind
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn actor(&self) -> &TemporaryDispatchActorProvenance {
        &self.actor
    }

    #[must_use]
    pub const fn authority_revision(&self) -> u64 {
        self.authority_revision
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    #[must_use]
    pub const fn intent_digest(&self) -> [u8; 32] {
        self.binding.intent_digest
    }
}

/// One idempotent withdrawal of the exact consent receipt named by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchConsentWithdrawalCommand {
    pub(super) receipt_id: Uuid,
    pub(super) consent_receipt_id: Uuid,
    pub(super) binding: TemporaryDispatchAdmissionBinding,
    pub(super) expected_revision: u64,
    pub(super) actor: OperationalActor,
}

impl TemporaryDispatchConsentWithdrawalCommand {
    /// Construct one exact consent withdrawal request.
    ///
    /// # Errors
    /// Rejects nil identities, non-participant actors, or an invalid expected
    /// consent revision.
    pub fn new(
        receipt_id: Uuid,
        consent_receipt_id: Uuid,
        binding: TemporaryDispatchAdmissionBinding,
        expected_revision: u64,
        actor: OperationalActor,
    ) -> Result<Self, OperationsError> {
        validate_command_identity(receipt_id, &binding, expected_revision, actor)?;
        if consent_receipt_id.is_nil()
            || receipt_id == binding.request_id
            || receipt_id == consent_receipt_id
        {
            return Err(OperationsError::Conflict(
                "temporary dispatch withdrawal identity",
            ));
        }
        Ok(Self {
            receipt_id,
            consent_receipt_id,
            binding,
            expected_revision,
            actor,
        })
    }

    #[must_use]
    pub const fn receipt_id(&self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn consent_receipt_id(&self) -> Uuid {
        self.consent_receipt_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.binding.request_id
    }
}

/// Immutable evidence that one authenticated actor withdrew its consent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchConsentWithdrawalReceipt {
    pub(super) receipt_id: Uuid,
    pub(super) consent_receipt_id: Uuid,
    pub(super) binding: TemporaryDispatchAdmissionBinding,
    pub(super) kind: TemporaryDispatchConsentKind,
    pub(super) revision: u64,
    pub(super) actor: TemporaryDispatchActorProvenance,
    pub(super) authority_revision: u64,
    pub(super) recorded_at: TimestampMillis,
    pub(super) replayed: bool,
}

impl TemporaryDispatchConsentWithdrawalReceipt {
    #[must_use]
    pub const fn receipt_id(&self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn consent_receipt_id(&self) -> Uuid {
        self.consent_receipt_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.binding.request_id
    }

    #[must_use]
    pub const fn kind(&self) -> TemporaryDispatchConsentKind {
        self.kind
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn actor(&self) -> &TemporaryDispatchActorProvenance {
        &self.actor
    }

    #[must_use]
    pub const fn authority_revision(&self) -> u64 {
        self.authority_revision
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Current Operations-owned reservation state for one admitted request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporaryDispatchReservationState {
    /// A same-subject overlap across receiving Teams needs an explicit product
    /// decision. The request is retained but it occupies no Team seat.
    PendingConflict,
    /// The inbound Team seat is held; no source work or execution is authorized.
    Reserved,
}

impl TemporaryDispatchReservationState {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::PendingConflict => "PENDING_CONFLICT",
            Self::Reserved => "RESERVED",
        }
    }

    pub(super) fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "PENDING_CONFLICT" => Ok(Self::PendingConflict),
            "RESERVED" => Ok(Self::Reserved),
            _ => Err(OperationsError::ResyncRequired(
                "temporary dispatch reservation lifecycle state",
            )),
        }
    }
}

/// One idempotent attempt to reserve the admitted target in its receiving Team.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchReserveCommand {
    pub(super) binding: TemporaryDispatchAdmissionBinding,
    pub(super) expected_revision: u64,
    pub(super) actor: OperationalActor,
}

impl TemporaryDispatchReserveCommand {
    /// Construct the initial reservation CAS. This slice accepts revision zero
    /// only; a durable pending-conflict result remains held for explicit review.
    ///
    /// # Errors
    /// Rejects invalid request identity, non-participant actors, or an invalid
    /// expected revision.
    pub fn new(
        binding: TemporaryDispatchAdmissionBinding,
        expected_revision: u64,
        actor: OperationalActor,
    ) -> Result<Self, OperationsError> {
        validate_command_identity(binding.request_id, &binding, expected_revision, actor)?;
        if expected_revision != 0 {
            return Err(OperationsError::Conflict(
                "temporary dispatch reservation revision",
            ));
        }
        Ok(Self {
            binding,
            expected_revision,
            actor,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.binding.request_id
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }
}

/// Durable receipt for either a held inbound Team seat or a pending conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchReservationReceipt {
    pub(super) binding: TemporaryDispatchAdmissionBinding,
    pub(super) state: TemporaryDispatchReservationState,
    pub(super) revision: u64,
    pub(super) target_participation_id: Uuid,
    pub(super) target_profile_id: Uuid,
    pub(super) target_membership_revision: u64,
    pub(super) requesting_leadership_revision: u64,
    pub(super) home_leadership_revision: u64,
    pub(super) workspace_leadership_revision: u64,
    pub(super) target_consent_receipt_id: Uuid,
    pub(super) home_leader_consent_receipt_id: Uuid,
    pub(super) orchestrator_consent_receipt_id: Uuid,
    pub(super) actor: TemporaryDispatchActorProvenance,
    pub(super) recorded_at: TimestampMillis,
    pub(super) replayed: bool,
}

impl TemporaryDispatchReservationReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.binding.request_id
    }

    #[must_use]
    pub const fn state(&self) -> TemporaryDispatchReservationState {
        self.state
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn target_participation_id(&self) -> Uuid {
        self.target_participation_id
    }

    #[must_use]
    pub const fn target_profile_id(&self) -> Uuid {
        self.target_profile_id
    }

    #[must_use]
    pub const fn target_membership_revision(&self) -> u64 {
        self.target_membership_revision
    }

    #[must_use]
    pub const fn requesting_leadership_revision(&self) -> u64 {
        self.requesting_leadership_revision
    }

    #[must_use]
    pub const fn home_leadership_revision(&self) -> u64 {
        self.home_leadership_revision
    }

    #[must_use]
    pub const fn workspace_leadership_revision(&self) -> u64 {
        self.workspace_leadership_revision
    }

    #[must_use]
    pub const fn target_consent_receipt_id(&self) -> Uuid {
        self.target_consent_receipt_id
    }

    #[must_use]
    pub const fn home_leader_consent_receipt_id(&self) -> Uuid {
        self.home_leader_consent_receipt_id
    }

    #[must_use]
    pub const fn orchestrator_consent_receipt_id(&self) -> Uuid {
        self.orchestrator_consent_receipt_id
    }

    #[must_use]
    pub const fn actor(&self) -> &TemporaryDispatchActorProvenance {
        &self.actor
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    #[must_use]
    pub const fn intent_digest(&self) -> [u8; 32] {
        self.binding.intent_digest
    }
}

pub(super) fn same_actor(
    left: &TemporaryDispatchActorProvenance,
    right: &TemporaryDispatchActorProvenance,
) -> bool {
    left.activity_id() == right.activity_id()
        && left.agent_id() == right.agent_id()
        && left.session_id() == right.session_id()
        && left.participation_id() == right.participation_id()
        && left.profile_id() == right.profile_id()
        && left.participant_id() == right.participant_id()
}

pub(super) fn actor_input_is_valid(actor: OperationalActor) -> bool {
    match actor {
        OperationalActor::LocalOperator => false,
        OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => {
            !activity_id.into_uuid().is_nil()
                && !agent_id.into_uuid().is_nil()
                && !session_id.into_uuid().is_nil()
        }
    }
}

pub(super) fn validate_command_identity(
    receipt_id: Uuid,
    binding: &TemporaryDispatchAdmissionBinding,
    expected_revision: u64,
    actor: OperationalActor,
) -> Result<(), OperationsError> {
    if receipt_id.is_nil() || binding.request_id.is_nil() || !actor_input_is_valid(actor) {
        return Err(OperationsError::Conflict(
            "temporary dispatch lifecycle identity",
        ));
    }
    if expected_revision > i64::MAX as u64 {
        return Err(OperationsError::Bounds(
            "temporary dispatch lifecycle revision",
        ));
    }
    Ok(())
}

fn admission_digest(record: &TemporaryDispatchRequestRecord) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(INTENT_DIGEST_DOMAIN);
    digest.update(record.request_id().as_bytes());
    digest.update(record.workspace_id().into_uuid().as_bytes());
    digest.update(record.requesting_team_id().as_bytes());
    digest.update(record.leadership_revision().to_be_bytes());
    digest.update(record.actor().activity_id().into_uuid().as_bytes());
    digest.update(record.actor().agent_id().into_uuid().as_bytes());
    digest.update(record.actor().session_id().into_uuid().as_bytes());
    digest.update(record.actor().participation_id().into_uuid().as_bytes());
    digest.update(record.actor().profile_id().as_bytes());
    digest.update(record.actor().participant_id().as_bytes());
    digest.update(record.target_home_team_id().as_bytes());
    digest.update(record.target_participant_id().as_bytes());
    let scope = record.requested_scope().as_str().as_bytes();
    digest.update((scope.len() as u64).to_be_bytes());
    digest.update(scope);
    digest.update(record.requested_end_at().get().to_be_bytes());
    digest.finalize().into()
}
