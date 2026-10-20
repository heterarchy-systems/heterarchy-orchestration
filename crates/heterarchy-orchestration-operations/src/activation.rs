//! Ready witness for one Orchestration membership binding.
//!
//! A Ready state is derived only from a real Forge activation receipt whose
//! reservation, source freeze, membership digest, and public creation
//! metadata match the committed Orchestration membership. This module never
//! accepts a caller-provided committed flag and never activates a Forge Agent.

use crate::{
    OperationsCatalog, OperationsError,
    binding_observation::BindingActivationObservation,
    effect_binding,
    membership::{self, MembershipCommitReceipt},
    registration::{self, RegistrationPhase, RegistrationProgress},
    wire,
};
use forge_domain::{
    AgentDeclaredRole, AgentDisplayName, AgentTransportKind, TimestampMillis, WorkspaceId,
};
use forge_persistence::AgentBindingActivation;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

const ACTIVATIONS_SQL: &str = "CREATE TABLE ops_binding_activations (request_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_logical_bindings(request_id) CHECK(length(request_id)=16 AND request_id != zeroblob(16)), membership_digest BLOB NOT NULL CHECK(length(membership_digest)=32 AND membership_digest != zeroblob(32)), display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128), declared_role TEXT NOT NULL CHECK(length(CAST(declared_role AS BLOB)) BETWEEN 1 AND 128), transport_kind INTEGER NOT NULL CHECK(transport_kind IN(0,1,2,3)), activated_at_ms INTEGER NOT NULL CHECK(activated_at_ms>=0))";

/// Durable activation witness owned by the Orchestration destination.
pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![("table", "ops_binding_activations", ACTIVATIONS_SQL)]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(crate) fn request_ready_witness(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<bool, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(b.commit_digest,1,33),substr(a.membership_digest,1,33),substr(CAST(a.display_name AS BLOB),1,129),substr(CAST(a.declared_role AS BLOB),1,129),a.transport_kind,i.phase,substr(CAST(m.display_name AS BLOB),1,129),substr(CAST(m.declared_role AS BLOB),1,129),m.transport_kind FROM ops_binding_activations a JOIN ops_logical_bindings b ON b.request_id=a.request_id JOIN ops_registration_requests r ON r.request_id=b.request_id JOIN ops_logical_members m ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id JOIN ops_pending_participants p ON p.workspace_id=b.workspace_id AND p.participant_id=b.participant_id JOIN ops_imports i ON i.migration_id=b.migration_id AND i.workspace_id=b.workspace_id WHERE a.request_id=?1 AND b.workspace_id=?2 AND b.participant_id=?3 AND r.phase=1 AND r.migration_id=b.migration_id AND r.workspace_id=b.workspace_id AND r.destination_id=b.destination_id AND r.activity_id=b.activity_id AND r.session_id=b.session_id AND r.participant_id=b.participant_id AND r.agent_id=b.agent_id AND b.freeze_revision=i.source_revision AND b.source_reserved_at_ms=r.reserved_at_ms AND b.destination_id=i.destination_id AND m.session_id=b.session_id AND p.session_id=b.session_id AND m.principal_kind=p.principal_kind AND m.principal_id IS p.principal_id AND m.display_name=p.display_name AND m.declared_role=p.declared_role AND m.transport_kind=p.transport_kind LIMIT 1",
            params![request_id.as_bytes(), workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?,row.get::<_, Vec<u8>>(1)?,row.get::<_, Vec<u8>>(2)?,row.get::<_, Vec<u8>>(3)?,row.get::<_, i64>(4)?,row.get::<_, i64>(5)?,row.get::<_, Vec<u8>>(6)?,row.get::<_, Vec<u8>>(7)?,row.get::<_, i64>(8)?)),
        )
        .optional()?;
    let Some((
        binding_digest,
        activation_digest,
        activation_display,
        activation_role,
        activation_transport,
        import_phase,
        member_display,
        member_role,
        member_transport,
    )) = raw
    else {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_binding_activations WHERE request_id=?1)",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )?;
        if exists {
            return Err(OperationsError::Corrupt("activation witness source tuple"));
        }
        return Ok(false);
    };
    if import_phase != 2 {
        return Ok(false);
    }
    let binding_digest: [u8; 32] = binding_digest
        .try_into()
        .map_err(|_| OperationsError::Corrupt("activation binding digest"))?;
    let activation_digest: [u8; 32] = activation_digest
        .try_into()
        .map_err(|_| OperationsError::Corrupt("activation receipt digest"))?;
    let activation_display = String::from_utf8(activation_display)
        .map_err(|_| OperationsError::Corrupt("activation display"))?;
    let activation_role = String::from_utf8(activation_role)
        .map_err(|_| OperationsError::Corrupt("activation role"))?;
    let member_display = String::from_utf8(member_display)
        .map_err(|_| OperationsError::Corrupt("logical member display"))?;
    let member_role = String::from_utf8(member_role)
        .map_err(|_| OperationsError::Corrupt("logical member role"))?;
    let activation_transport = decode_transport(activation_transport)?;
    let member_transport = decode_transport(member_transport)?;
    if binding_digest == [0; 32]
        || activation_digest == [0; 32]
        || binding_digest != activation_digest
        || activation_display != member_display
        || activation_role != member_role
        || activation_transport != member_transport
    {
        return Err(OperationsError::Corrupt("activation witness identity"));
    }
    registration::validate_ready_request(connection, request_id, workspace_id, participant_id)?;
    Ok(true)
}

pub(crate) fn has_activation(
    connection: &Connection,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_binding_activations WHERE request_id=?1)",
        params![request_id.as_bytes()],
        |row| row.get(0),
    )?)
}

/// Immutable Orchestration readback of a real Forge activation witness.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationActivationReceipt {
    request_id: Uuid,
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    participant_id: Uuid,
    binding: forge_persistence::AgentBindingReservationRequest,
    membership_digest: [u8; 32],
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
    activated_at: TimestampMillis,
}

impl RegistrationActivationReceipt {
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
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    pub const fn binding(&self) -> &forge_persistence::AgentBindingReservationRequest {
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
    pub const fn activated_at(&self) -> TimestampMillis {
        self.activated_at
    }
}

impl OperationsCatalog {
    /// Record a real Forge activation receipt and derive Ready for the exact
    /// `MembershipCommitted` registration. The source binding remains Forge's
    /// authority; this method only journals its trusted readback.
    ///
    /// # Errors
    /// Rejects an unverified or aborted import, source/reservation mismatches,
    /// changed metadata or digest, and uncertain commits.
    pub fn record_registration_activation(
        &mut self,
        activation: &AgentBindingActivation,
    ) -> Result<RegistrationActivationReceipt, OperationsError> {
        let observation = BindingActivationObservation::from_native(activation)?;
        self.record_registration_activation_observation(&observation)
    }

    /// Record one checked readback of Forge's activation for the exact
    /// `MembershipCommitted` registration and derive Ready. The source binding
    /// remains Forge's authority; this method only journals its readback.
    ///
    /// # Errors
    /// Rejects an unverified or aborted import, source/reservation mismatches,
    /// changed metadata or digest, and uncertain commits.
    pub fn record_registration_activation_observation(
        &mut self,
        activation: &BindingActivationObservation,
    ) -> Result<RegistrationActivationReceipt, OperationsError> {
        let request_id = activation.request_id();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if crate::binding_rejection::has_rejection(&transaction, request_id)? {
            return Err(OperationsError::Conflict(
                "binding rejection already recorded",
            ));
        }
        let Some(progress) = registration::committed_progress_at(&transaction, request_id)? else {
            return Err(OperationsError::Conflict("registration request missing"));
        };
        let import =
            registration::verified_import(&transaction, progress.migration_id(), self.destination)?;
        let membership = membership::commit_at(&transaction, self.destination, request_id)?
            .ok_or(OperationsError::Conflict("membership commit missing"))?;
        validate_activation_source(
            &progress,
            &membership,
            &import,
            activation,
            self.destination,
        )?;

        if let Some(raw) = load_activation(&transaction, request_id)? {
            let receipt = read_activation_row(&progress, &membership, self.destination, &raw)?;
            if !same_activation(&receipt, activation) {
                return Err(OperationsError::Conflict("activation replay"));
            }
            let binding = membership.binding();
            // The immutable receipt survives departure and Session replacement;
            // replay must never recreate authority that a later event retired.
            let current = effect_binding::exact(
                &transaction,
                receipt.workspace_id(),
                binding.activity_id(),
                binding.agent_id(),
                binding.session_id(),
            )?
            .is_some_and(|value| value.participant_id() == receipt.participant_id())
                && activation_member_is_current(&transaction, &membership)?;
            commit_activation(transaction, request_id)?;
            if current {
                let _bootstrapped = self.bootstrap_deferred_after_activation(&receipt)?;
            }
            return Ok(receipt);
        }
        if !activation_member_is_current(&transaction, &membership)? {
            return Err(OperationsError::Conflict(
                "activation current member Session",
            ));
        }
        transaction.execute(
            "INSERT INTO ops_binding_activations(request_id,membership_digest,display_name,declared_role,transport_kind,activated_at_ms) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                request_id.as_bytes(),
                activation.membership_digest().as_slice(),
                activation.display_name().as_str(),
                activation.declared_role().as_str(),
                encode_transport(activation.transport()),
                activation.activated_at().get(),
            ],
        )?;
        let raw = load_activation(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt("activation insert readback"))?;
        let receipt = read_activation_row(&progress, &membership, self.destination, &raw)?;
        if !same_activation(&receipt, activation) {
            return Err(OperationsError::Corrupt("activation insert identity"));
        }
        let binding = membership.binding();
        let _current = effect_binding::record_registration(
            &transaction,
            receipt.workspace_id(),
            receipt.participant_id(),
            binding.activity_id(),
            binding.session_id(),
            binding.agent_id(),
            request_id,
        )?;
        commit_activation(transaction, request_id)?;
        let _bootstrapped = self.bootstrap_deferred_after_activation(&receipt)?;
        Ok(receipt)
    }

    /// Read one immutable activation witness. It does not reauthorize the
    /// source and remains an audit read after an import is aborted.
    ///
    /// # Errors
    /// Returns storage failures or corrupt activation/membership rows.
    pub fn registration_activation(
        &self,
        request_id: Uuid,
    ) -> Result<Option<RegistrationActivationReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = (|| {
            let Some(progress) = registration::committed_progress_at(&transaction, request_id)?
            else {
                return Ok(None);
            };
            let Some(membership) =
                membership::commit_at(&transaction, self.destination, request_id)?
            else {
                return Ok(None);
            };
            let Some(raw) = load_activation(&transaction, request_id)? else {
                return Ok(None);
            };
            read_activation_row(&progress, &membership, self.destination, &raw).map(Some)
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

fn activation_member_is_current(
    connection: &Connection,
    membership: &MembershipCommitReceipt,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2 AND session_id=?3)",
        params![
            membership.workspace_id().into_uuid().as_bytes(),
            membership.participant_id().as_bytes(),
            membership.binding().session_id().into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?)
}

#[derive(Debug)]
struct RawActivation {
    request_id: Uuid,
    membership_digest: [u8; 32],
    display_name: String,
    declared_role: String,
    transport: AgentTransportKind,
    activated_at: TimestampMillis,
}

fn load_activation(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawActivation>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(membership_digest,1,33),substr(CAST(display_name AS BLOB),1,129),substr(CAST(declared_role AS BLOB),1,129),transport_kind,activated_at_ms FROM ops_binding_activations WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, Vec<u8>>(2)?, row.get::<_, Vec<u8>>(3)?, row.get::<_, i64>(4)?, row.get::<_, i64>(5)?)),
        )
        .optional()?;
    raw.map(
        |(request, digest, display, role, transport, activated_at)| {
            let digest: [u8; 32] = digest
                .try_into()
                .map_err(|_| OperationsError::Corrupt("activation digest"))?;
            if digest == [0; 32] {
                return Err(OperationsError::Corrupt("activation digest"));
            }
            let display = String::from_utf8(display)
                .map_err(|_| OperationsError::Corrupt("activation display"))?;
            let role =
                String::from_utf8(role).map_err(|_| OperationsError::Corrupt("activation role"))?;
            Ok(RawActivation {
                request_id: Uuid::from_bytes(wire::id(request)?),
                membership_digest: digest,
                display_name: display,
                declared_role: role,
                transport: decode_transport(transport)?,
                activated_at: TimestampMillis::new(activated_at)
                    .map_err(|_| OperationsError::Corrupt("activation timestamp"))?,
            })
        },
    )
    .transpose()
}

fn validate_activation_source(
    progress: &RegistrationProgress,
    membership: &MembershipCommitReceipt,
    import: &crate::ImportProgress,
    activation: &BindingActivationObservation,
    destination: Uuid,
) -> Result<(), OperationsError> {
    let reservation = activation.reservation();
    let binding = progress.request().binding();
    if progress.phase() != RegistrationPhase::MembershipCommitted
        && progress.phase() != RegistrationPhase::Ready
    {
        return Err(OperationsError::Conflict(
            "registration not membership committed",
        ));
    }
    if import.phase() != crate::ImportPhase::Verified
        || import.workspace_id() != progress.workspace_id()
        || import.destination_id() != destination
        || reservation.migration_id() != progress.migration_id()
        || reservation.destination_id() != destination
        || reservation.workspace_id() != progress.workspace_id()
        || reservation.freeze_revision() != import.source_revision()
        || reservation.freeze_revision() != membership.freeze_revision()
        || reservation.reserved_at() != membership.reserved_at()
        || reservation.request() != binding
        || activation.membership_digest() != membership.commit_digest()
        || activation.display_name() != progress.request().display_name()
        || activation.declared_role() != progress.request().declared_role()
        || activation.transport() != progress.request().transport()
    {
        return Err(OperationsError::Conflict("activation source identity"));
    }
    Ok(())
}

fn read_activation_row(
    progress: &RegistrationProgress,
    membership: &MembershipCommitReceipt,
    destination: Uuid,
    raw: &RawActivation,
) -> Result<RegistrationActivationReceipt, OperationsError> {
    if raw.request_id != membership.request_id()
        || raw.membership_digest != membership.commit_digest()
        || raw.display_name != progress.request().display_name().as_str()
        || raw.declared_role != progress.request().declared_role().as_str()
        || raw.transport != progress.request().transport()
    {
        return Err(OperationsError::Corrupt("activation identity"));
    }
    Ok(RegistrationActivationReceipt {
        request_id: raw.request_id,
        migration_id: membership.migration_id(),
        workspace_id: membership.workspace_id(),
        destination_id: destination,
        participant_id: membership.participant_id(),
        binding: membership.binding().clone(),
        membership_digest: raw.membership_digest,
        display_name: progress.request().display_name().clone(),
        declared_role: progress.request().declared_role().clone(),
        transport: raw.transport,
        activated_at: raw.activated_at,
    })
}

fn same_activation(
    receipt: &RegistrationActivationReceipt,
    activation: &BindingActivationObservation,
) -> bool {
    receipt.request_id() == activation.request_id()
        && receipt.membership_digest() == activation.membership_digest()
        && receipt.display_name() == activation.display_name()
        && receipt.declared_role() == activation.declared_role()
        && receipt.transport() == activation.transport()
        && receipt.activated_at() == activation.activated_at()
}

fn encode_transport(value: AgentTransportKind) -> i64 {
    match value {
        AgentTransportKind::Retired => 0,
        AgentTransportKind::Stdio => 1,
        AgentTransportKind::StreamableHttp => 2,
        AgentTransportKind::Other => 3,
    }
}

fn decode_transport(value: i64) -> Result<AgentTransportKind, OperationsError> {
    match value {
        0 => Ok(AgentTransportKind::Retired),
        1 => Ok(AgentTransportKind::Stdio),
        2 => Ok(AgentTransportKind::StreamableHttp),
        3 => Ok(AgentTransportKind::Other),
        _ => Err(OperationsError::Corrupt("activation transport")),
    }
}

fn commit_activation(
    transaction: Transaction<'_>,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::ActivationCommitUnconfirmed { request_id, source })
}
