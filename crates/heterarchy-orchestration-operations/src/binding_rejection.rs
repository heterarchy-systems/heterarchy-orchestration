//! Durable terminal observation of a Forge binding rejection.
//!
//! Rejection settles only the exact source reservation. It preserves the
//! logical membership commit and never creates Ready/effect authority.

use crate::{
    OperationsCatalog, OperationsError,
    binding_observation::BindingRejectionObservation,
    membership,
    registration::{self, RegistrationPhase},
};
use forge_domain::{
    AgentBindingRejectionReason, AgentDeclaredRole, AgentDisplayName, AgentTransportKind,
    AuthenticatedPrincipal, TimestampMillis, WorkspaceId, WorkspaceRootFingerprint,
};
use forge_persistence::AgentBindingReservationRequest;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const REJECTIONS_SQL: &str = "CREATE TABLE ops_binding_rejections (request_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_logical_bindings(request_id) CHECK(length(request_id)=16 AND request_id != zeroblob(16)), issuer_id BLOB NOT NULL CHECK(length(issuer_id)=16 AND issuer_id != zeroblob(16)), workspace_root_fingerprint BLOB NOT NULL CHECK(length(workspace_root_fingerprint)=16), binding_digest BLOB NOT NULL CHECK(length(binding_digest)=32 AND binding_digest != zeroblob(32)), delegation_digest BLOB NOT NULL CHECK(length(delegation_digest)=32 AND delegation_digest != zeroblob(32)), reason INTEGER NOT NULL CHECK(reason=0), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), rejected_at_ms INTEGER NOT NULL CHECK(rejected_at_ms>=observed_at_ms), request_digest BLOB NOT NULL CHECK(length(request_digest)=32 AND request_digest != zeroblob(32)), receipt_digest BLOB NOT NULL CHECK(length(receipt_digest)=32 AND receipt_digest != zeroblob(32)), FOREIGN KEY(request_id) REFERENCES ops_binding_rejection_attempts(request_id))";
const ATTEMPTS_SQL: &str = "CREATE TABLE ops_binding_rejection_attempts (request_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_logical_bindings(request_id) CHECK(length(request_id)=16 AND request_id != zeroblob(16)), issuer_id BLOB NOT NULL CHECK(length(issuer_id)=16 AND issuer_id != zeroblob(16)), destination_id BLOB NOT NULL CHECK(length(destination_id)=16 AND destination_id != zeroblob(16)), migration_id BLOB NOT NULL CHECK(length(migration_id)=16 AND migration_id != zeroblob(16)), freeze_revision INTEGER NOT NULL CHECK(freeze_revision>=1), manifest_digest BLOB NOT NULL CHECK(length(manifest_digest)=32 AND manifest_digest != zeroblob(32)), registration_digest BLOB NOT NULL CHECK(length(registration_digest)=32 AND registration_digest != zeroblob(32)), request_digest BLOB NOT NULL CHECK(length(request_digest)=32 AND request_digest != zeroblob(32)), membership_digest BLOB NOT NULL CHECK(length(membership_digest)=32 AND membership_digest != zeroblob(32)), attempted_at_ms INTEGER NOT NULL CHECK(attempted_at_ms>=0))";
const RESOLUTIONS_SQL: &str = "CREATE TABLE ops_binding_rejection_attempt_resolutions (request_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_binding_rejection_attempts(request_id), resolution_request_id BLOB NOT NULL UNIQUE CHECK(length(resolution_request_id)=16 AND resolution_request_id != zeroblob(16)), attempt_request_digest BLOB NOT NULL CHECK(length(attempt_request_digest)=32 AND attempt_request_digest != zeroblob(32)), legacy_client_artifact_digest BLOB NOT NULL CHECK(length(legacy_client_artifact_digest)=32 AND legacy_client_artifact_digest != zeroblob(32)), envelope_policy INTEGER NOT NULL CHECK(envelope_policy=0), durable_uuid_version INTEGER NOT NULL CHECK(durable_uuid_version=8), source_contract INTEGER NOT NULL CHECK(source_contract=0), source_get_observed_at_ms INTEGER NOT NULL CHECK(source_get_observed_at_ms>=0), operator_principal_kind INTEGER NOT NULL CHECK(operator_principal_kind IN(0,1,2)), operator_principal_id BLOB CHECK(operator_principal_id IS NULL OR length(operator_principal_id)=16), resolved_at_ms INTEGER NOT NULL CHECK(resolved_at_ms>=source_get_observed_at_ms), consume_request_id BLOB UNIQUE CHECK(consume_request_id IS NULL OR (length(consume_request_id)=16 AND consume_request_id != zeroblob(16))), consumed_at_ms INTEGER CHECK(consumed_at_ms IS NULL OR consumed_at_ms>=resolved_at_ms), CHECK((operator_principal_kind=0 AND operator_principal_id IS NULL) OR (operator_principal_kind IN(1,2) AND operator_principal_id IS NOT NULL)), CHECK((consume_request_id IS NULL AND consumed_at_ms IS NULL) OR (consume_request_id IS NOT NULL AND consumed_at_ms IS NOT NULL)))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    let mut objects = schema_objects_v44();
    objects.push((
        "table",
        "ops_binding_rejection_attempt_resolutions",
        RESOLUTIONS_SQL,
    ));
    objects
}

pub(crate) fn schema_objects_v44() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_binding_rejection_attempts", ATTEMPTS_SQL),
        ("table", "ops_binding_rejections", REJECTIONS_SQL),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(ATTEMPTS_SQL)?;
    connection.execute_batch(REJECTIONS_SQL)?;
    connection.execute_batch(RESOLUTIONS_SQL)?;
    Ok(())
}

pub(crate) fn create_resolution_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(RESOLUTIONS_SQL)?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(
        "DROP TABLE IF EXISTS ops_binding_rejection_attempt_resolutions; DROP TABLE ops_binding_rejections; DROP TABLE ops_binding_rejection_attempts;",
    )?;
    Ok(())
}

/// Exact O-owned immutable input frozen before the first Source mutation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingRejectionAttemptRequest {
    request_id: Uuid,
    issuer_id: Uuid,
    destination_id: Uuid,
    migration_id: Uuid,
    freeze_revision: u64,
    manifest_digest: [u8; 32],
    registration_digest: [u8; 32],
    request_digest: [u8; 32],
    membership_digest: [u8; 32],
    attempted_at: TimestampMillis,
}

impl BindingRejectionAttemptRequest {
    /// Construct one checked immutable attempt identity.
    ///
    /// # Errors
    /// Rejects nil identities, a zero revision, or zero evidence digests.
    #[allow(clippy::too_many_arguments)]
    pub fn for_registration(
        workspace_id: WorkspaceId,
        issuer_id: Uuid,
        destination_id: Uuid,
        migration_id: Uuid,
        freeze_revision: u64,
        manifest_digest: [u8; 32],
        registration: &registration::RegistrationRequest,
        membership_digest: [u8; 32],
        attempted_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        let request_id = registration.binding().request_id();
        let registration_digest = registration.digest()?;
        let request_digest = rejection_intent_digest(
            workspace_id,
            issuer_id,
            destination_id,
            migration_id,
            freeze_revision,
            manifest_digest,
            registration,
            membership_digest,
            attempted_at,
        );
        Self::checked(
            request_id,
            issuer_id,
            destination_id,
            migration_id,
            freeze_revision,
            manifest_digest,
            registration_digest,
            request_digest,
            membership_digest,
            attempted_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn checked(
        request_id: Uuid,
        issuer_id: Uuid,
        destination_id: Uuid,
        migration_id: Uuid,
        freeze_revision: u64,
        manifest_digest: [u8; 32],
        registration_digest: [u8; 32],
        request_digest: [u8; 32],
        membership_digest: [u8; 32],
        attempted_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil()
            || issuer_id.is_nil()
            || destination_id.is_nil()
            || migration_id.is_nil()
            || freeze_revision == 0
            || manifest_digest == [0; 32]
            || registration_digest == [0; 32]
            || request_digest == [0; 32]
            || membership_digest == [0; 32]
        {
            return Err(OperationsError::Conflict(
                "binding rejection attempt identity",
            ));
        }
        Ok(Self {
            request_id,
            issuer_id,
            destination_id,
            migration_id,
            freeze_revision,
            manifest_digest,
            registration_digest,
            request_digest,
            membership_digest,
            attempted_at,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn issuer_id(&self) -> Uuid {
        self.issuer_id
    }
    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.destination_id
    }
    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn freeze_revision(&self) -> u64 {
        self.freeze_revision
    }
    #[must_use]
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    #[must_use]
    pub const fn registration_digest(&self) -> [u8; 32] {
        self.registration_digest
    }
    #[must_use]
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    #[must_use]
    pub const fn membership_digest(&self) -> [u8; 32] {
        self.membership_digest
    }
    #[must_use]
    pub const fn attempted_at(&self) -> TimestampMillis {
        self.attempted_at
    }
}

#[allow(clippy::too_many_arguments)]
fn rejection_intent_digest(
    workspace_id: WorkspaceId,
    issuer_id: Uuid,
    destination_id: Uuid,
    migration_id: Uuid,
    freeze_revision: u64,
    manifest_digest: [u8; 32],
    registration: &registration::RegistrationRequest,
    membership_digest: [u8; 32],
    observed_at: TimestampMillis,
) -> [u8; 32] {
    let binding = registration.binding();
    let mut digest = Sha256::new();
    digest.update(b"forge.agent.binding.rejection.intent.v1\0");
    digest.update(binding.request_id().as_bytes());
    digest.update(workspace_id.into_uuid().as_bytes());
    digest.update(binding.activity_id().into_uuid().as_bytes());
    digest.update(binding.agent_id().into_uuid().as_bytes());
    digest.update(binding.session_id().into_uuid().as_bytes());
    match binding.principal() {
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
    digest.update(migration_id.as_bytes());
    digest.update(destination_id.as_bytes());
    digest.update(freeze_revision.to_le_bytes());
    digest.update(manifest_digest);
    digest.update(issuer_id.as_bytes());
    digest.update(membership_digest);
    for text in [
        registration.display_name().as_str(),
        registration.declared_role().as_str(),
    ] {
        digest.update((text.len() as u64).to_le_bytes());
        digest.update(text.as_bytes());
    }
    digest.update([match registration.transport() {
        AgentTransportKind::Retired => 0,
        AgentTransportKind::Stdio => 1,
        AgentTransportKind::StreamableHttp => 2,
        AgentTransportKind::Other => 3,
    }]);
    digest.update([0]);
    digest.update(binding.requested_at().get().to_le_bytes());
    digest.update(observed_at.get().to_le_bytes());
    digest.finalize().into()
}

fn source_receipt_digest(receipt: &RegistrationBindingRejectionReceipt) -> [u8; 32] {
    let binding = receipt.binding();
    let mut digest = Sha256::new();
    digest.update(b"forge.agent.binding.rejection.receipt.v1\0");
    digest.update(receipt.request_digest());
    digest.update(receipt.request_id().as_bytes());
    digest.update(receipt.workspace_id().into_uuid().as_bytes());
    digest.update(binding.activity_id().into_uuid().as_bytes());
    digest.update(binding.agent_id().into_uuid().as_bytes());
    digest.update(binding.session_id().into_uuid().as_bytes());
    match binding.principal() {
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
    digest.update(receipt.migration_id().as_bytes());
    digest.update(receipt.destination_id().as_bytes());
    digest.update(receipt.expected_freeze_revision().to_le_bytes());
    digest.update(receipt.manifest_digest());
    digest.update(receipt.issuer_id().as_bytes());
    digest.update(receipt.workspace_root_fingerprint().as_bytes());
    digest.update(receipt.binding_digest());
    digest.update(receipt.delegation_digest());
    digest.update(receipt.membership_digest());
    for text in [
        receipt.display_name().as_str(),
        receipt.declared_role().as_str(),
    ] {
        digest.update((text.len() as u64).to_le_bytes());
        digest.update(text.as_bytes());
    }
    digest.update([match receipt.transport() {
        AgentTransportKind::Retired => 0,
        AgentTransportKind::Stdio => 1,
        AgentTransportKind::StreamableHttp => 2,
        AgentTransportKind::Other => 3,
    }]);
    digest.update(receipt.reserved_at().get().to_le_bytes());
    digest.update(binding.requested_at().get().to_le_bytes());
    digest.update(receipt.observed_at().get().to_le_bytes());
    digest.update(receipt.rejected_at().get().to_le_bytes());
    digest.finalize().into()
}

/// Durable dispatch policy after freezing an exact Source mutation intent.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingRejectionAttemptDisposition {
    /// This exact intent was newly journalled and may be dispatched once.
    DispatchAllowed,
    /// An earlier dispatch may have committed; only exact Source readback is allowed.
    ReadbackRequired,
    /// Terminal Source evidence is already durably observed by O.
    Recorded(Box<RegistrationBindingRejectionReceipt>),
}

/// Frozen behavior of the legacy client artifact that created the uncertain
/// attempt. Only this closed policy is eligible for the one-shot repair.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyBindingRejectionEnvelopePolicy {
    GeneratedUuidV7,
}

/// Frozen identity rule used by the corrected Source command client.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingRejectionSourceRequestPolicy {
    BodyEqualsEnvelope,
}

/// Host-authenticated deployment evidence for one legacy client artifact.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrustedLegacyBindingRejectionDeployment {
    artifact_digest: [u8; 32],
    envelope_policy: LegacyBindingRejectionEnvelopePolicy,
    durable_uuid_version: u8,
    source_request_policy: BindingRejectionSourceRequestPolicy,
}

impl TrustedLegacyBindingRejectionDeployment {
    /// Construct one closed deployment proof read from trusted local config.
    ///
    /// # Errors
    /// Rejects zero artifact evidence or a durable identity policy other than
    /// `UUIDv8`, which is the only historical mismatch covered by this repair.
    pub fn new(
        artifact_digest: [u8; 32],
        envelope_policy: LegacyBindingRejectionEnvelopePolicy,
        durable_uuid_version: u8,
        source_request_policy: BindingRejectionSourceRequestPolicy,
    ) -> Result<Self, OperationsError> {
        if artifact_digest == [0; 32] || durable_uuid_version != 8 {
            return Err(OperationsError::Conflict(
                "binding rejection deployment proof",
            ));
        }
        Ok(Self {
            artifact_digest,
            envelope_policy,
            durable_uuid_version,
            source_request_policy,
        })
    }

    #[must_use]
    pub const fn artifact_digest(self) -> [u8; 32] {
        self.artifact_digest
    }

    pub const fn envelope_policy(self) -> LegacyBindingRejectionEnvelopePolicy {
        self.envelope_policy
    }

    #[must_use]
    pub const fn durable_uuid_version(self) -> u8 {
        self.durable_uuid_version
    }

    pub const fn source_request_policy(self) -> BindingRejectionSourceRequestPolicy {
        self.source_request_policy
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingRejectionAttemptResolution {
    request_id: Uuid,
    resolution_request_id: Uuid,
    attempt_request_digest: [u8; 32],
    legacy_client_artifact_digest: [u8; 32],
    envelope_policy: LegacyBindingRejectionEnvelopePolicy,
    durable_uuid_version: u8,
    source_request_policy: BindingRejectionSourceRequestPolicy,
    source_get_observed_at: TimestampMillis,
    operator_principal: AuthenticatedPrincipal,
    resolved_at: TimestampMillis,
    consume_request_id: Option<Uuid>,
    consumed_at: Option<TimestampMillis>,
}

impl BindingRejectionAttemptResolution {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn resolution_request_id(&self) -> Uuid {
        self.resolution_request_id
    }
    #[must_use]
    pub const fn attempt_request_digest(&self) -> [u8; 32] {
        self.attempt_request_digest
    }
    #[must_use]
    pub const fn legacy_client_artifact_digest(&self) -> [u8; 32] {
        self.legacy_client_artifact_digest
    }
    pub const fn envelope_policy(&self) -> LegacyBindingRejectionEnvelopePolicy {
        self.envelope_policy
    }
    #[must_use]
    pub const fn durable_uuid_version(&self) -> u8 {
        self.durable_uuid_version
    }
    pub const fn source_request_policy(&self) -> BindingRejectionSourceRequestPolicy {
        self.source_request_policy
    }
    #[must_use]
    pub const fn source_get_observed_at(&self) -> TimestampMillis {
        self.source_get_observed_at
    }
    pub const fn operator_principal(&self) -> AuthenticatedPrincipal {
        self.operator_principal
    }
    #[must_use]
    pub const fn resolved_at(&self) -> TimestampMillis {
        self.resolved_at
    }
    #[must_use]
    pub const fn consume_request_id(&self) -> Option<Uuid> {
        self.consume_request_id
    }
    #[must_use]
    pub const fn consumed_at(&self) -> Option<TimestampMillis> {
        self.consumed_at
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveBindingRejectionAttempt {
    pub request_id: Uuid,
    pub resolution_request_id: Uuid,
    pub expected_attempt_request_digest: [u8; 32],
    pub expected_legacy_client_artifact_digest: [u8; 32],
    pub source_get_observed_at: TimestampMillis,
    pub operator_principal: AuthenticatedPrincipal,
    pub resolved_at: TimestampMillis,
}

pub(crate) fn has_rejection(
    connection: &Connection,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_binding_rejections WHERE request_id=?1)",
        params![request_id.as_bytes()],
        |row| row.get(0),
    )?)
}

/// Immutable Orchestration readback of one terminal Source rejection.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationBindingRejectionReceipt {
    request_id: Uuid,
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    expected_freeze_revision: u64,
    manifest_digest: [u8; 32],
    participant_id: Uuid,
    binding: AgentBindingReservationRequest,
    membership_digest: [u8; 32],
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
    issuer_id: Uuid,
    workspace_root_fingerprint: WorkspaceRootFingerprint,
    binding_digest: [u8; 32],
    delegation_digest: [u8; 32],
    reason: AgentBindingRejectionReason,
    reserved_at: TimestampMillis,
    observed_at: TimestampMillis,
    rejected_at: TimestampMillis,
    request_digest: [u8; 32],
    receipt_digest: [u8; 32],
}

impl RegistrationBindingRejectionReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
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
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }
    pub const fn binding(&self) -> &AgentBindingReservationRequest {
        &self.binding
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
    pub const fn reason(&self) -> AgentBindingRejectionReason {
        self.reason
    }
    #[must_use]
    pub const fn reserved_at(&self) -> TimestampMillis {
        self.reserved_at
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

impl OperationsCatalog {
    /// Persist or replay an exact operator-approved resolution of one legacy
    /// uncertain attempt.
    ///
    /// # Errors
    /// Rejects untrusted deployment evidence, changed attempt identity,
    /// conflicting replay, terminal rejection evidence, or storage failure.
    pub fn resolve_registration_binding_rejection_attempt(
        &mut self,
        request: &ResolveBindingRejectionAttempt,
        trusted_deployment: TrustedLegacyBindingRejectionDeployment,
    ) -> Result<BindingRejectionAttemptResolution, OperationsError> {
        if request.request_id.is_nil()
            || request.resolution_request_id.is_nil()
            || request.request_id.get_version_num() != 8
            || request.expected_attempt_request_digest == [0; 32]
            || request.expected_legacy_client_artifact_digest
                != trusted_deployment.artifact_digest()
            || request.request_id.get_version_num()
                != usize::from(trusted_deployment.durable_uuid_version())
            || request.resolved_at < request.source_get_observed_at
        {
            return Err(OperationsError::Conflict(
                "binding rejection resolution identity",
            ));
        }
        let validated_attempt = self
            .registration_binding_rejection_attempt(request.request_id)?
            .ok_or(OperationsError::Conflict(
                "binding rejection attempt missing",
            ))?;
        if validated_attempt.request_digest() != request.expected_attempt_request_digest {
            return Err(OperationsError::Conflict(
                "binding rejection resolution attempt",
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if load_rejection(&transaction, request.request_id)?.is_some() {
            return Err(OperationsError::Conflict(
                "binding rejection already recorded",
            ));
        }
        if let Some(existing) = load_resolution(&transaction, request.request_id)? {
            if existing.resolution_request_id() != request.resolution_request_id
                || existing.attempt_request_digest() != request.expected_attempt_request_digest
                || existing.legacy_client_artifact_digest()
                    != request.expected_legacy_client_artifact_digest
                || existing.envelope_policy() != trusted_deployment.envelope_policy()
                || existing.durable_uuid_version() != trusted_deployment.durable_uuid_version()
                || existing.source_request_policy() != trusted_deployment.source_request_policy()
                || existing.source_get_observed_at() != request.source_get_observed_at
                || existing.operator_principal() != request.operator_principal
                || existing.resolved_at() != request.resolved_at
            {
                return Err(OperationsError::Conflict(
                    "binding rejection resolution replay",
                ));
            }
            commit_rejection(transaction, request.request_id)?;
            return Ok(existing);
        }
        let (principal_kind, principal_id) =
            registration::encode_principal(request.operator_principal);
        transaction.execute(
            "INSERT INTO ops_binding_rejection_attempt_resolutions(request_id,resolution_request_id,attempt_request_digest,legacy_client_artifact_digest,envelope_policy,durable_uuid_version,source_contract,source_get_observed_at_ms,operator_principal_kind,operator_principal_id,resolved_at_ms,consume_request_id,consumed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,NULL,NULL)",
            params![request.request_id.as_bytes(),request.resolution_request_id.as_bytes(),request.expected_attempt_request_digest.as_slice(),request.expected_legacy_client_artifact_digest.as_slice(),encode_envelope_policy(trusted_deployment.envelope_policy()),i64::from(trusted_deployment.durable_uuid_version()),encode_source_request_policy(trusted_deployment.source_request_policy()),request.source_get_observed_at.get(),principal_kind,principal_id.as_deref(),request.resolved_at.get()],
        )?;
        let result = load_resolution(&transaction, request.request_id)?.ok_or(
            OperationsError::Corrupt("binding rejection resolution insert"),
        )?;
        commit_rejection(transaction, request.request_id)?;
        Ok(result)
    }

    /// Consume one validated resolution exactly once before Source dispatch.
    ///
    /// # Errors
    /// Rejects corrupt attempt/resolution evidence, a foreign operator or
    /// deployment, timestamp regression, repeated consumption, or storage
    /// failure.
    pub fn consume_registration_binding_rejection_resolution(
        &mut self,
        request_id: Uuid,
        operator_principal: AuthenticatedPrincipal,
        trusted_deployment: TrustedLegacyBindingRejectionDeployment,
        consumed_at: TimestampMillis,
    ) -> Result<BindingRejectionAttemptDisposition, OperationsError> {
        if request_id.is_nil() {
            return Err(OperationsError::Conflict(
                "binding rejection consume identity",
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(raw) = load_rejection(&transaction, request_id)? {
            let receipt = assemble_receipt(
                rejection_identity(&transaction, self.destination, request_id)?,
                raw,
            )?;
            commit_rejection(transaction, request_id)?;
            return Ok(BindingRejectionAttemptDisposition::Recorded(Box::new(
                receipt,
            )));
        }
        let Some(resolution) = validated_resolution(&transaction, self.destination, request_id)?
        else {
            commit_rejection(transaction, request_id)?;
            return Ok(BindingRejectionAttemptDisposition::ReadbackRequired);
        };
        if resolution.operator_principal() != operator_principal
            || resolution.legacy_client_artifact_digest() != trusted_deployment.artifact_digest()
            || resolution.envelope_policy() != trusted_deployment.envelope_policy()
            || resolution.durable_uuid_version() != trusted_deployment.durable_uuid_version()
            || resolution.source_request_policy() != trusted_deployment.source_request_policy()
        {
            return Err(OperationsError::Corrupt(
                "binding rejection resolution authority",
            ));
        }
        if resolution.consumed_at().is_some() {
            commit_rejection(transaction, request_id)?;
            return Ok(BindingRejectionAttemptDisposition::ReadbackRequired);
        }
        if consumed_at < resolution.resolved_at() {
            return Err(OperationsError::Conflict(
                "binding rejection consume timestamp",
            ));
        }
        let changed = transaction.execute(
            "UPDATE ops_binding_rejection_attempt_resolutions SET consume_request_id=?1,consumed_at_ms=?2 WHERE request_id=?3 AND consume_request_id IS NULL AND consumed_at_ms IS NULL",
            params![resolution.resolution_request_id().as_bytes(), consumed_at.get(), request_id.as_bytes()],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict(
                "binding rejection resolution consumed",
            ));
        }
        commit_rejection(transaction, request_id)?;
        Ok(BindingRejectionAttemptDisposition::DispatchAllowed)
    }

    /// Read and validate one immutable reconciliation proof without granting
    /// Source dispatch authority.
    ///
    /// # Errors
    /// Returns corrupt attempt, registration, membership, resolution, or
    /// storage evidence.
    pub fn registration_binding_rejection_attempt_resolution(
        &self,
        request_id: Uuid,
    ) -> Result<Option<BindingRejectionAttemptResolution>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = validated_resolution(&transaction, self.destination, request_id);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Read the first durable outbound intent without granting redispatch.
    ///
    /// # Errors
    /// Returns corrupt registration/commit/attempt evidence or storage failure.
    pub fn registration_binding_rejection_attempt(
        &self,
        request_id: Uuid,
    ) -> Result<Option<BindingRejectionAttemptRequest>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = (|| {
            let Some(attempt) = load_attempt(&transaction, request_id)? else {
                return Ok(None);
            };
            let identity = rejection_identity(&transaction, self.destination, request_id)?;
            let registration = registration::committed_progress_at(&transaction, request_id)?
                .ok_or(OperationsError::Corrupt(
                    "binding rejection registration missing",
                ))?;
            if attempt.destination_id() != identity.destination_id()
                || attempt.migration_id() != identity.migration_id()
                || attempt.freeze_revision() != identity.expected_freeze_revision()
                || attempt.manifest_digest() != identity.manifest_digest()
                || attempt.registration_digest() != registration.request().digest()?
                || attempt.request_digest()
                    != rejection_intent_digest(
                        identity.workspace_id(),
                        attempt.issuer_id(),
                        attempt.destination_id(),
                        attempt.migration_id(),
                        attempt.freeze_revision(),
                        attempt.manifest_digest(),
                        registration.request(),
                        attempt.membership_digest(),
                        attempt.attempted_at(),
                    )
                || attempt.membership_digest() != identity.membership_digest()
            {
                return Err(OperationsError::Corrupt(
                    "binding rejection attempt evidence",
                ));
            }
            Ok(Some(attempt))
        })();
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Freeze one exact Source mutation intent before any external I/O.
    ///
    /// Exact replay never grants another dispatch. Absence on a later Source
    /// read also does not change `ReadbackRequired` into dispatch authority.
    ///
    /// # Errors
    /// Rejects changed intent, missing registration/commit evidence,
    /// activation conflicts, or uncertain local commit.
    pub fn prepare_registration_binding_rejection_attempt(
        &mut self,
        request: &BindingRejectionAttemptRequest,
    ) -> Result<BindingRejectionAttemptDisposition, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let identity = rejection_identity(&transaction, self.destination, request.request_id())?;
        let import = crate::catalog::load_progress(&transaction, identity.migration_id())?
            .ok_or(OperationsError::Corrupt("binding rejection import missing"))?;
        if import.phase() != crate::ImportPhase::Verified {
            return Err(OperationsError::Conflict(
                "binding rejection import not verified",
            ));
        }
        let stored_registration =
            registration::committed_progress_at(&transaction, request.request_id())?
                .ok_or(OperationsError::Conflict("registration request missing"))?;
        if request.destination_id() != identity.destination_id()
            || request.migration_id() != identity.migration_id()
            || request.freeze_revision() != identity.expected_freeze_revision()
            || request.manifest_digest() != identity.manifest_digest()
            || request.registration_digest() != stored_registration.request().digest()?
            || request.request_digest()
                != rejection_intent_digest(
                    identity.workspace_id(),
                    request.issuer_id(),
                    request.destination_id(),
                    request.migration_id(),
                    request.freeze_revision(),
                    request.manifest_digest(),
                    stored_registration.request(),
                    request.membership_digest(),
                    request.attempted_at(),
                )
            || request.membership_digest() != identity.membership_digest()
        {
            return Err(OperationsError::Conflict(
                "binding rejection attempt source identity",
            ));
        }
        if crate::activation::has_activation(&transaction, request.request_id())? {
            return Err(OperationsError::Conflict(
                "binding activation already recorded",
            ));
        }
        let existing_attempt = load_attempt(&transaction, request.request_id())?;
        if let Some(raw) = load_rejection(&transaction, request.request_id())? {
            if !existing_attempt
                .as_ref()
                .is_some_and(|stored| same_attempt_identity(stored, request))
            {
                return Err(OperationsError::Corrupt(
                    "binding rejection attempt missing or changed",
                ));
            }
            let receipt = assemble_receipt(identity, raw)?;
            commit_rejection(transaction, request.request_id())?;
            return Ok(BindingRejectionAttemptDisposition::Recorded(Box::new(
                receipt,
            )));
        }
        if let Some(existing) = existing_attempt {
            if !same_attempt_identity(&existing, request) {
                return Err(OperationsError::Conflict(
                    "binding rejection attempt replay",
                ));
            }
            commit_rejection(transaction, request.request_id())?;
            return Ok(BindingRejectionAttemptDisposition::ReadbackRequired);
        }
        transaction.execute(
            "INSERT INTO ops_binding_rejection_attempts(request_id,issuer_id,destination_id,migration_id,freeze_revision,manifest_digest,registration_digest,request_digest,membership_digest,attempted_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                request.request_id().as_bytes(),
                request.issuer_id().as_bytes(),
                request.destination_id().as_bytes(),
                request.migration_id().as_bytes(),
                i64::try_from(request.freeze_revision())
                    .map_err(|_| OperationsError::Bounds("binding rejection freeze revision"))?,
                request.manifest_digest().as_slice(),
                request.registration_digest().as_slice(),
                request.request_digest().as_slice(),
                request.membership_digest().as_slice(),
                request.attempted_at().get(),
            ],
        )?;
        let stored = load_attempt(&transaction, request.request_id())?
            .ok_or(OperationsError::Corrupt("binding rejection attempt insert"))?;
        if stored != *request {
            return Err(OperationsError::Corrupt(
                "binding rejection attempt identity",
            ));
        }
        commit_rejection(transaction, request.request_id())?;
        Ok(BindingRejectionAttemptDisposition::DispatchAllowed)
    }

    /// Journal one exact authenticated Source rejection observation.
    ///
    /// # Errors
    /// Rejects missing/uncommitted registrations, changed source identity,
    /// activation/rejection conflicts, replay disagreement, or uncertain commit.
    pub fn record_registration_binding_rejection_observation(
        &mut self,
        rejection: &BindingRejectionObservation,
    ) -> Result<RegistrationBindingRejectionReceipt, OperationsError> {
        let request_id = rejection.request_id();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempt = load_attempt(&transaction, request_id)?.ok_or(OperationsError::Conflict(
            "binding rejection attempt missing",
        ))?;
        if attempt.issuer_id() != rejection.issuer_id()
            || attempt.destination_id() != rejection.destination_id()
            || attempt.migration_id() != rejection.migration_id()
            || attempt.freeze_revision() != rejection.expected_freeze_revision()
            || attempt.manifest_digest() != rejection.manifest_digest()
            || attempt.request_digest() != rejection.request_digest()
            || attempt.membership_digest() != rejection.membership_digest()
        {
            return Err(OperationsError::Conflict(
                "binding rejection attempt observation",
            ));
        }
        let receipt = validate_source(&transaction, self.destination, rejection)?;
        if crate::activation::has_activation(&transaction, request_id)? {
            return Err(OperationsError::Conflict(
                "binding activation already recorded",
            ));
        }
        if let Some(raw) = load_rejection(&transaction, request_id)? {
            let stored = assemble_receipt(receipt, raw)?;
            if !same_rejection(&stored, rejection) {
                return Err(OperationsError::Conflict("binding rejection replay"));
            }
            commit_rejection(transaction, request_id)?;
            return Ok(stored);
        }
        transaction.execute(
            "INSERT INTO ops_binding_rejections(request_id,issuer_id,workspace_root_fingerprint,binding_digest,delegation_digest,reason,observed_at_ms,rejected_at_ms,request_digest,receipt_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                request_id.as_bytes(),
                rejection.issuer_id().as_bytes(),
                rejection.workspace_root_fingerprint().as_bytes(),
                rejection.binding_digest().as_slice(),
                rejection.delegation_digest().as_slice(),
                encode_reason(rejection.reason()),
                rejection.observed_at().get(),
                rejection.rejected_at().get(),
                rejection.request_digest().as_slice(),
                rejection.receipt_digest().as_slice(),
            ],
        )?;
        let raw = load_rejection(&transaction, request_id)?.ok_or(OperationsError::Corrupt(
            "binding rejection insert readback",
        ))?;
        let stored = assemble_receipt(receipt, raw)?;
        if !same_rejection(&stored, rejection) {
            return Err(OperationsError::Corrupt(
                "binding rejection insert identity",
            ));
        }
        commit_rejection(transaction, request_id)?;
        Ok(stored)
    }

    /// Read one immutable terminal rejection without granting current authority.
    ///
    /// # Errors
    /// Returns storage failures or corrupt registration/commit/rejection rows.
    pub fn registration_binding_rejection(
        &self,
        request_id: Uuid,
    ) -> Result<Option<RegistrationBindingRejectionReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = (|| {
            let Some(raw) = load_rejection(&transaction, request_id)? else {
                return Ok(None);
            };
            if crate::activation::has_activation(&transaction, request_id)? {
                return Err(OperationsError::Corrupt(
                    "binding activation and rejection coexist",
                ));
            }
            let attempt = load_attempt(&transaction, request_id)?.ok_or(
                OperationsError::Corrupt("binding rejection attempt missing"),
            )?;
            let receipt = rejection_identity(&transaction, self.destination, request_id)?;
            let receipt = assemble_receipt(receipt, raw)?;
            validate_stored_attempt(&transaction, &attempt, &receipt)?;
            Ok(Some(receipt))
        })();
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}

fn same_attempt_identity(
    left: &BindingRejectionAttemptRequest,
    right: &BindingRejectionAttemptRequest,
) -> bool {
    left.request_id() == right.request_id()
        && left.issuer_id() == right.issuer_id()
        && left.destination_id() == right.destination_id()
        && left.migration_id() == right.migration_id()
        && left.freeze_revision() == right.freeze_revision()
        && left.manifest_digest() == right.manifest_digest()
        && left.registration_digest() == right.registration_digest()
        && left.request_digest() == right.request_digest()
        && left.membership_digest() == right.membership_digest()
}

fn validate_stored_attempt(
    connection: &Connection,
    attempt: &BindingRejectionAttemptRequest,
    receipt: &RegistrationBindingRejectionReceipt,
) -> Result<(), OperationsError> {
    let registration = registration::committed_progress_at(connection, receipt.request_id())?
        .ok_or(OperationsError::Corrupt(
            "binding rejection registration missing",
        ))?;
    let expected_request_digest = rejection_intent_digest(
        receipt.workspace_id(),
        attempt.issuer_id(),
        attempt.destination_id(),
        attempt.migration_id(),
        attempt.freeze_revision(),
        attempt.manifest_digest(),
        registration.request(),
        attempt.membership_digest(),
        attempt.attempted_at(),
    );
    if attempt.request_id() != receipt.request_id()
        || attempt.issuer_id() != receipt.issuer_id()
        || attempt.destination_id() != receipt.destination_id()
        || attempt.migration_id() != receipt.migration_id()
        || attempt.freeze_revision() != receipt.expected_freeze_revision()
        || attempt.manifest_digest() != receipt.manifest_digest()
        || attempt.registration_digest() != registration.request().digest()?
        || attempt.request_digest() != expected_request_digest
        || receipt.request_digest() != expected_request_digest
        || attempt.membership_digest() != receipt.membership_digest()
    {
        return Err(OperationsError::Corrupt(
            "binding rejection attempt evidence",
        ));
    }
    if source_receipt_digest(receipt) != receipt.receipt_digest() {
        return Err(OperationsError::Corrupt("binding rejection receipt digest"));
    }
    Ok(())
}

fn validate_source(
    connection: &Connection,
    destination: Uuid,
    rejection: &BindingRejectionObservation,
) -> Result<RegistrationBindingRejectionReceipt, OperationsError> {
    let receipt = rejection_identity(connection, destination, rejection.request_id())?;
    let binding = receipt.binding();
    if rejection.migration_id() != receipt.migration_id()
        || rejection.destination_id() != receipt.destination_id()
        || rejection.workspace_id() != receipt.workspace_id()
        || rejection.activity_id() != binding.activity_id()
        || rejection.agent_id() != binding.agent_id()
        || rejection.session_id() != binding.session_id()
        || rejection.principal() != binding.principal()
        || rejection.requested_at() != binding.requested_at()
        || rejection.reserved_at() != receipt.reserved_at()
        || rejection.membership_digest() != receipt.membership_digest()
        || rejection.display_name() != receipt.display_name()
        || rejection.declared_role() != receipt.declared_role()
        || rejection.transport() != receipt.transport()
        || rejection.reason() != AgentBindingRejectionReason::PrincipalTransportMismatch
    {
        return Err(OperationsError::Conflict(
            "binding rejection source identity",
        ));
    }
    let import = crate::catalog::load_progress(connection, receipt.migration_id())?
        .ok_or(OperationsError::Corrupt("binding rejection import missing"))?;
    if import.phase() != crate::ImportPhase::Verified
        || rejection.expected_freeze_revision() != import.source_revision()
        || rejection.manifest_digest() != import.digest()
    {
        return Err(OperationsError::Conflict("binding rejection source freeze"));
    }
    Ok(receipt)
}

fn rejection_identity(
    connection: &Connection,
    destination: Uuid,
    request_id: Uuid,
) -> Result<RegistrationBindingRejectionReceipt, OperationsError> {
    let progress = registration::committed_progress_at(connection, request_id)?
        .ok_or(OperationsError::Conflict("registration request missing"))?;
    if progress.phase() != RegistrationPhase::MembershipCommitted {
        return Err(OperationsError::Conflict(
            "registration not membership committed",
        ));
    }
    let membership = membership::commit_at(connection, destination, request_id)?
        .ok_or(OperationsError::Conflict("membership commit missing"))?;
    let import = crate::catalog::load_progress(connection, membership.migration_id())?
        .ok_or(OperationsError::Corrupt("binding rejection import missing"))?;
    Ok(RegistrationBindingRejectionReceipt {
        request_id,
        migration_id: membership.migration_id(),
        workspace_id: membership.workspace_id(),
        destination_id: destination,
        expected_freeze_revision: import.source_revision(),
        manifest_digest: import.digest(),
        participant_id: membership.participant_id(),
        binding: membership.binding().clone(),
        membership_digest: membership.commit_digest(),
        display_name: progress.request().display_name().clone(),
        declared_role: progress.request().declared_role().clone(),
        transport: progress.request().transport(),
        issuer_id: Uuid::nil(),
        workspace_root_fingerprint: WorkspaceRootFingerprint::from_bytes([0; 16]),
        binding_digest: [0; 32],
        delegation_digest: [0; 32],
        reason: AgentBindingRejectionReason::PrincipalTransportMismatch,
        reserved_at: membership.reserved_at(),
        observed_at: membership.committed_at(),
        rejected_at: membership.committed_at(),
        request_digest: [0; 32],
        receipt_digest: [0; 32],
    })
}

fn load_attempt(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<BindingRejectionAttemptRequest>, OperationsError> {
    connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(issuer_id,1,17),substr(destination_id,1,17),substr(migration_id,1,17),freeze_revision,substr(manifest_digest,1,33),substr(registration_digest,1,33),substr(request_digest,1,33),substr(membership_digest,1,33),attempted_at_ms FROM ops_binding_rejection_attempts WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, i64>(9)?,
                ))
            },
        )
        .optional()?
        .map(
            |(
                request_id,
                issuer_id,
                destination_id,
                migration_id,
                freeze_revision,
                manifest_digest,
                registration_digest,
                request_digest,
                membership_digest,
                attempted_at,
            )| {
                BindingRejectionAttemptRequest::checked(
                    Uuid::from_bytes(crate::wire::id(request_id)?),
                    Uuid::from_bytes(crate::wire::id(issuer_id)?),
                    Uuid::from_bytes(crate::wire::id(destination_id)?),
                    Uuid::from_bytes(crate::wire::id(migration_id)?),
                    u64::try_from(freeze_revision).map_err(|_| {
                        OperationsError::Corrupt("binding rejection attempt freeze revision")
                    })?,
                    manifest_digest.try_into().map_err(|_| {
                        OperationsError::Corrupt("binding rejection attempt manifest digest")
                    })?,
                    registration_digest.try_into().map_err(|_| {
                        OperationsError::Corrupt("binding rejection attempt registration digest")
                    })?,
                    request_digest.try_into().map_err(|_| {
                        OperationsError::Corrupt("binding rejection attempt request digest")
                    })?,
                    membership_digest.try_into().map_err(|_| {
                        OperationsError::Corrupt("binding rejection attempt membership digest")
                    })?,
                    TimestampMillis::new(attempted_at).map_err(|_| {
                        OperationsError::Corrupt("binding rejection attempt timestamp")
                    })?,
                )
                .map_err(|_| OperationsError::Corrupt("binding rejection attempt identity"))
            },
        )
        .transpose()
}

fn load_resolution(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<BindingRejectionAttemptResolution>, OperationsError> {
    connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(resolution_request_id,1,17),substr(attempt_request_digest,1,33),substr(legacy_client_artifact_digest,1,33),envelope_policy,durable_uuid_version,source_contract,source_get_observed_at_ms,operator_principal_kind,substr(operator_principal_id,1,17),resolved_at_ms,substr(consume_request_id,1,17),consumed_at_ms FROM ops_binding_rejection_attempt_resolutions WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,Vec<u8>>(3)?,row.get::<_,i64>(4)?,row.get::<_,i64>(5)?,row.get::<_,i64>(6)?,row.get::<_,i64>(7)?,row.get::<_,i64>(8)?,row.get::<_,Option<Vec<u8>>>(9)?,row.get::<_,i64>(10)?,row.get::<_,Option<Vec<u8>>>(11)?,row.get::<_,Option<i64>>(12)?)),
        )
        .optional()?
        .map(|(request,resolution,digest,artifact,policy,version,contract,get_at,principal_kind,principal_id,resolved_at,consume,consumed_at)| {
            let envelope_policy = decode_envelope_policy(policy)?;
            let durable_uuid_version = u8::try_from(version)
                .map_err(|_| OperationsError::Corrupt("binding rejection resolution policy"))?;
            if durable_uuid_version != 8 {
                return Err(OperationsError::Corrupt("binding rejection resolution policy"));
            }
            let source_request_policy = decode_source_request_policy(contract)?;
            let digest: [u8;32] = digest.try_into().map_err(|_| OperationsError::Corrupt("binding rejection resolution digest"))?;
            let artifact: [u8;32] = artifact.try_into().map_err(|_| OperationsError::Corrupt("binding rejection resolution artifact"))?;
            if digest == [0;32] || artifact == [0;32] { return Err(OperationsError::Corrupt("binding rejection resolution digest")); }
            let request_id = Uuid::from_bytes(crate::wire::id(request)?);
            let resolution_request_id = Uuid::from_bytes(crate::wire::id(resolution)?);
            let source_get_observed_at = TimestampMillis::new(get_at).map_err(|_| OperationsError::Corrupt("binding rejection resolution timestamp"))?;
            let resolved_at = TimestampMillis::new(resolved_at).map_err(|_| OperationsError::Corrupt("binding rejection resolution timestamp"))?;
            let consume_request_id = consume.map(|value| crate::wire::id(value).map(Uuid::from_bytes)).transpose()?;
            let consumed_at = consumed_at.map(|value| TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt("binding rejection resolution timestamp"))).transpose()?;
            if request_id.is_nil()
                || resolution_request_id.is_nil()
                || resolved_at < source_get_observed_at
                || consume_request_id.is_some() != consumed_at.is_some()
                || consume_request_id.is_some_and(|value| value.is_nil())
                || consume_request_id.is_some_and(|value| value != resolution_request_id)
                || consumed_at.is_some_and(|value| value < resolved_at)
            {
                return Err(OperationsError::Corrupt("binding rejection resolution identity"));
            }
            Ok(BindingRejectionAttemptResolution {
                request_id,
                resolution_request_id,
                attempt_request_digest: digest,
                legacy_client_artifact_digest: artifact,
                envelope_policy,
                durable_uuid_version,
                source_request_policy,
                source_get_observed_at,
                operator_principal: registration::decode_principal_fields(principal_kind, principal_id.as_deref())?,
                resolved_at,
                consume_request_id,
                consumed_at,
            })
        })
        .transpose()
}

fn validated_resolution(
    connection: &Connection,
    destination: Uuid,
    request_id: Uuid,
) -> Result<Option<BindingRejectionAttemptResolution>, OperationsError> {
    let Some(resolution) = load_resolution(connection, request_id)? else {
        return Ok(None);
    };
    let attempt = load_attempt(connection, request_id)?.ok_or(OperationsError::Corrupt(
        "binding rejection resolution attempt missing",
    ))?;
    let identity = rejection_identity(connection, destination, request_id)?;
    let registration = registration::committed_progress_at(connection, request_id)?.ok_or(
        OperationsError::Corrupt("binding rejection registration missing"),
    )?;
    if resolution.request_id() != request_id
        || resolution.attempt_request_digest() != attempt.request_digest()
        || resolution.operator_principal() != registration.request().binding().principal()
        || resolution.source_get_observed_at() < attempt.attempted_at()
        || attempt.destination_id() != identity.destination_id()
        || attempt.migration_id() != identity.migration_id()
        || attempt.freeze_revision() != identity.expected_freeze_revision()
        || attempt.manifest_digest() != identity.manifest_digest()
        || attempt.registration_digest() != registration.request().digest()?
        || attempt.membership_digest() != identity.membership_digest()
        || attempt.request_digest()
            != rejection_intent_digest(
                identity.workspace_id(),
                attempt.issuer_id(),
                attempt.destination_id(),
                attempt.migration_id(),
                attempt.freeze_revision(),
                attempt.manifest_digest(),
                registration.request(),
                attempt.membership_digest(),
                attempt.attempted_at(),
            )
    {
        return Err(OperationsError::Corrupt(
            "binding rejection resolution evidence",
        ));
    }
    Ok(Some(resolution))
}

const fn encode_envelope_policy(policy: LegacyBindingRejectionEnvelopePolicy) -> i64 {
    match policy {
        LegacyBindingRejectionEnvelopePolicy::GeneratedUuidV7 => 0,
    }
}

fn decode_envelope_policy(
    value: i64,
) -> Result<LegacyBindingRejectionEnvelopePolicy, OperationsError> {
    match value {
        0 => Ok(LegacyBindingRejectionEnvelopePolicy::GeneratedUuidV7),
        _ => Err(OperationsError::Corrupt(
            "binding rejection resolution envelope policy",
        )),
    }
}

const fn encode_source_request_policy(policy: BindingRejectionSourceRequestPolicy) -> i64 {
    match policy {
        BindingRejectionSourceRequestPolicy::BodyEqualsEnvelope => 0,
    }
}

fn decode_source_request_policy(
    value: i64,
) -> Result<BindingRejectionSourceRequestPolicy, OperationsError> {
    match value {
        0 => Ok(BindingRejectionSourceRequestPolicy::BodyEqualsEnvelope),
        _ => Err(OperationsError::Corrupt(
            "binding rejection resolution source policy",
        )),
    }
}

#[derive(Debug, Clone, Copy)]
struct RawRejection {
    issuer_id: Uuid,
    workspace_root_fingerprint: WorkspaceRootFingerprint,
    binding_digest: [u8; 32],
    delegation_digest: [u8; 32],
    reason: AgentBindingRejectionReason,
    observed_at: TimestampMillis,
    rejected_at: TimestampMillis,
    request_digest: [u8; 32],
    receipt_digest: [u8; 32],
}

fn load_rejection(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawRejection>, OperationsError> {
    connection
        .query_row(
            "SELECT substr(issuer_id,1,17),substr(workspace_root_fingerprint,1,17),substr(binding_digest,1,33),substr(delegation_digest,1,33),reason,observed_at_ms,rejected_at_ms,substr(request_digest,1,33),substr(receipt_digest,1,33) FROM ops_binding_rejections WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                ))
            },
        )
        .optional()?
        .map(
            |(
                issuer_id,
                root,
                binding_digest,
                delegation_digest,
                reason,
                observed_at,
                rejected_at,
                request_digest,
                receipt_digest,
            )| {
                Ok(RawRejection {
                    issuer_id: Uuid::from_bytes(crate::wire::id(issuer_id)?),
                    workspace_root_fingerprint: WorkspaceRootFingerprint::from_bytes(
                        root.try_into().map_err(|_| {
                            OperationsError::Corrupt("binding rejection root fingerprint")
                        })?,
                    ),
                    binding_digest: binding_digest.try_into().map_err(|_| {
                        OperationsError::Corrupt("binding rejection binding digest")
                    })?,
                    delegation_digest: delegation_digest.try_into().map_err(|_| {
                        OperationsError::Corrupt("binding rejection delegation digest")
                    })?,
                    reason: decode_reason(reason)?,
                    observed_at: TimestampMillis::new(observed_at).map_err(|_| {
                        OperationsError::Corrupt("binding rejection observed timestamp")
                    })?,
                    rejected_at: TimestampMillis::new(rejected_at)
                        .map_err(|_| OperationsError::Corrupt("binding rejection timestamp"))?,
                    request_digest: request_digest.try_into().map_err(|_| {
                        OperationsError::Corrupt("binding rejection request digest")
                    })?,
                    receipt_digest: receipt_digest.try_into().map_err(|_| {
                        OperationsError::Corrupt("binding rejection receipt digest")
                    })?,
                })
            },
        )
        .transpose()
}

fn assemble_receipt(
    mut receipt: RegistrationBindingRejectionReceipt,
    raw: RawRejection,
) -> Result<RegistrationBindingRejectionReceipt, OperationsError> {
    if raw.issuer_id.is_nil()
        || raw.binding_digest == [0; 32]
        || raw.delegation_digest == [0; 32]
        || raw.request_digest == [0; 32]
        || raw.receipt_digest == [0; 32]
        || raw.rejected_at < raw.observed_at
        || raw.observed_at < receipt.rejected_at
    {
        return Err(OperationsError::Corrupt("binding rejection evidence"));
    }
    receipt.issuer_id = raw.issuer_id;
    receipt.workspace_root_fingerprint = raw.workspace_root_fingerprint;
    receipt.binding_digest = raw.binding_digest;
    receipt.delegation_digest = raw.delegation_digest;
    receipt.reason = raw.reason;
    receipt.observed_at = raw.observed_at;
    receipt.rejected_at = raw.rejected_at;
    receipt.request_digest = raw.request_digest;
    receipt.receipt_digest = raw.receipt_digest;
    Ok(receipt)
}

fn same_rejection(
    receipt: &RegistrationBindingRejectionReceipt,
    rejection: &BindingRejectionObservation,
) -> bool {
    receipt.request_id() == rejection.request_id()
        && receipt.membership_digest() == rejection.membership_digest()
        && receipt.display_name() == rejection.display_name()
        && receipt.declared_role() == rejection.declared_role()
        && receipt.transport() == rejection.transport()
        && receipt.issuer_id() == rejection.issuer_id()
        && receipt.workspace_root_fingerprint() == rejection.workspace_root_fingerprint()
        && receipt.binding_digest() == rejection.binding_digest()
        && receipt.delegation_digest() == rejection.delegation_digest()
        && receipt.reason() == rejection.reason()
        && receipt.observed_at() == rejection.observed_at()
        && receipt.rejected_at() == rejection.rejected_at()
        && receipt.request_digest() == rejection.request_digest()
        && receipt.receipt_digest() == rejection.receipt_digest()
}

const fn encode_reason(reason: AgentBindingRejectionReason) -> i64 {
    match reason {
        AgentBindingRejectionReason::PrincipalTransportMismatch => 0,
    }
}

fn decode_reason(value: i64) -> Result<AgentBindingRejectionReason, OperationsError> {
    match value {
        0 => Ok(AgentBindingRejectionReason::PrincipalTransportMismatch),
        _ => Err(OperationsError::Corrupt("binding rejection reason")),
    }
}

fn commit_rejection(transaction: Transaction<'_>, request_id: Uuid) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::BindingRejectionCommitUnconfirmed { request_id, source })
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal};

    use super::*;
    use crate::RegistrationRequest;

    #[test]
    fn caller_intent_digest_matches_frozen_source_vector() -> Result<(), Box<dyn std::error::Error>>
    {
        let registration = RegistrationRequest::new(
            Uuid::from_u128(10),
            AgentBindingReservationRequest::new(
                Uuid::from_u128(1),
                ActivityId::from_uuid(Uuid::from_u128(3)),
                AgentInstanceId::from_uuid(Uuid::from_u128(4)),
                AgentSessionId::from_uuid(Uuid::from_u128(5)),
                AuthenticatedPrincipal::TrustedStdio,
                TimestampMillis::new(20)?,
            )?,
            AgentDisplayName::new("worker".to_owned())?,
            AgentDeclaredRole::new("operator".to_owned())?,
            AgentTransportKind::Other,
        )?;
        let attempt = BindingRejectionAttemptRequest::for_registration(
            WorkspaceId::from_uuid(Uuid::from_u128(2)),
            Uuid::from_u128(9),
            Uuid::from_u128(7),
            Uuid::from_u128(6),
            8,
            [0x11; 32],
            &registration,
            [0x55; 32],
            TimestampMillis::new(40)?,
        )?;
        let mut actual = String::with_capacity(64);
        for byte in attempt.request_digest() {
            write!(&mut actual, "{byte:02x}")?;
        }
        assert_eq!(
            actual,
            "1832e23b3d2acab13bda3fc25cfce47e3799fb99704626cc355560523e90e788"
        );
        Ok(())
    }
}
