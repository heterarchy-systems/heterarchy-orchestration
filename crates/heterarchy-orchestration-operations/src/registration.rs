//! Pending logical participation and Forge binding-reservation journal.
//!
//! This module deliberately stops at the `Pending`/`Reserved` boundary.  A
//! reservation is an effect-plane identity receipt; it is not an active
//! participant, a membership claim, a presence row, or a `JOINED` event.

mod wire;

use crate::binding_observation::BindingReservationObservation;
use crate::{
    ImportPhase, OperationsCatalog, OperationsError, catalog::load_progress, wire as import_wire,
};
use forge_domain::{
    ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentSessionId,
    AgentTransportKind, AuthenticatedPrincipal, TimestampMillis, WorkspaceId,
};
use forge_persistence::{AgentBindingReservation, AgentBindingReservationRequest};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

const MAX_REQUEST_WIRE_BYTES: usize = 4096;

const PENDING_PARTICIPANTS_SQL: &str = "CREATE TABLE ops_pending_participants (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), principal_kind INTEGER NOT NULL CHECK(principal_kind IN(0,1,2)), principal_id BLOB CHECK(principal_id IS NULL OR length(principal_id)=16), display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128), declared_role TEXT NOT NULL CHECK(length(CAST(declared_role AS BLOB)) BETWEEN 1 AND 128), transport_kind INTEGER NOT NULL CHECK(transport_kind IN(0,1,2,3)), PRIMARY KEY(workspace_id,participant_id), UNIQUE(workspace_id,session_id), UNIQUE(workspace_id,participant_id,session_id), CHECK((principal_kind=0 AND principal_id IS NULL) OR (principal_kind IN(1,2) AND principal_id IS NOT NULL)))";
const REGISTRATION_REQUESTS_SQL: &str = "CREATE TABLE ops_registration_requests (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), migration_id BLOB NOT NULL CHECK(length(migration_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), destination_id BLOB NOT NULL CHECK(length(destination_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), session_id BLOB NOT NULL CHECK(length(session_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16), agent_id BLOB NOT NULL CHECK(length(agent_id)=16), request_wire BLOB NOT NULL CHECK(length(request_wire) BETWEEN 1 AND 4096), phase INTEGER NOT NULL CHECK(phase IN(0,1)), reserved_at_ms INTEGER CHECK(reserved_at_ms IS NULL OR reserved_at_ms>=0), UNIQUE(workspace_id,agent_id), UNIQUE(migration_id,activity_id,session_id), FOREIGN KEY(migration_id) REFERENCES ops_imports(migration_id), FOREIGN KEY(destination_id) REFERENCES ops_catalog_identity(destination_id), FOREIGN KEY(workspace_id,participant_id,session_id) REFERENCES ops_pending_participants(workspace_id,participant_id,session_id), CHECK((phase=0 AND reserved_at_ms IS NULL) OR (phase=1 AND reserved_at_ms IS NOT NULL)))";

/// The durable objects owned by the registration journal.
pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_pending_participants",
            PENDING_PARTICIPANTS_SQL,
        ),
        (
            "table",
            "ops_registration_requests",
            REGISTRATION_REQUESTS_SQL,
        ),
    ]
}

/// Create the pending registration tables in a caller-owned transaction.
pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// A registration request is the immutable Orchestration-side proposal.  Its
/// Forge binding request remains a separate typed value and no pending data is
/// represented as a `RegisteredAgentInstance`.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationRequest {
    proposed_participant_id: Uuid,
    binding: AgentBindingReservationRequest,
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
}

impl RegistrationRequest {
    /// Construct one checked, immutable registration proposal.
    ///
    /// # Errors
    /// Returns a conflict for a nil proposed participant or if the bounded
    /// canonical wire form cannot be represented within 4096 bytes.
    pub fn new(
        proposed_participant_id: Uuid,
        binding: AgentBindingReservationRequest,
        display_name: AgentDisplayName,
        declared_role: AgentDeclaredRole,
        transport: AgentTransportKind,
    ) -> Result<Self, OperationsError> {
        if proposed_participant_id.is_nil() {
            return Err(OperationsError::Conflict(
                "registration participant identity",
            ));
        }
        let value = Self {
            proposed_participant_id,
            binding,
            display_name,
            declared_role,
            transport,
        };
        if wire::encode(&value)?.len() > MAX_REQUEST_WIRE_BYTES {
            return Err(OperationsError::Bounds("registration request"));
        }
        Ok(value)
    }

    #[must_use]
    pub const fn proposed_participant_id(&self) -> Uuid {
        self.proposed_participant_id
    }

    pub const fn binding(&self) -> &AgentBindingReservationRequest {
        &self.binding
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

    pub(crate) fn encode(&self) -> Result<Vec<u8>, OperationsError> {
        wire::encode(self)
    }

    pub(crate) fn decode(value: &[u8]) -> Result<Self, OperationsError> {
        wire::decode(value)
    }
}

pub(crate) fn encode_principal(value: AuthenticatedPrincipal) -> (i64, Option<Vec<u8>>) {
    wire::encode_principal(value)
}

pub(crate) fn decode_principal_fields(
    kind: i64,
    value: Option<&[u8]>,
) -> Result<AuthenticatedPrincipal, OperationsError> {
    wire::decode_principal_fields(kind, value)
}

pub(crate) fn encode_transport(value: AgentTransportKind) -> i64 {
    wire::encode_transport(value)
}

pub(crate) fn decode_transport(value: i64) -> Result<AgentTransportKind, OperationsError> {
    wire::decode_transport(value)
}

/// Registration is intentionally closed before activation. `Reserved` records
/// Forge's exact identity reservation; `MembershipCommitted` records the
/// Orchestration-side logical membership transaction and `Ready` is derived
/// only from a real Forge activation witness.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationPhase {
    Pending,
    Reserved,
    MembershipCommitted,
    Ready,
}

impl RegistrationPhase {
    fn encode(self) -> i64 {
        match self {
            Self::Pending => 0,
            Self::Reserved => 1,
            Self::MembershipCommitted => 2,
            Self::Ready => 3,
        }
    }

    fn decode(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Reserved),
            2 => Ok(Self::MembershipCommitted),
            3 => Ok(Self::Ready),
            _ => Err(OperationsError::Corrupt("registration phase")),
        }
    }
}

/// Durable readback of one pending, Forge-reserved, or logically committed
/// registration proposal. Ready is derived only from a real Forge activation
/// witness.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationProgress {
    workspace: WorkspaceId,
    migration: Uuid,
    participant: Uuid,
    request: RegistrationRequest,
    phase: RegistrationPhase,
    reserved_at: Option<TimestampMillis>,
}

impl RegistrationProgress {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace
    }

    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant
    }

    /// Explicit name for the canonical logical participant identity.
    #[must_use]
    pub const fn canonical_participant_id(&self) -> Uuid {
        self.participant_id()
    }

    pub const fn request(&self) -> &RegistrationRequest {
        &self.request
    }

    pub const fn phase(&self) -> RegistrationPhase {
        self.phase
    }

    #[must_use]
    pub const fn reserved_at(&self) -> Option<TimestampMillis> {
        self.reserved_at
    }
}

impl OperationsCatalog {
    /// Persist a pending registration proposal after an import is verified.
    ///
    /// The operation only writes the pending logical participant and request
    /// journal.  It never writes imported effect snapshots, presence, inbox,
    /// events, or Forge source references.
    ///
    /// # Errors
    /// Rejects imports that are not still `Verified`, changed request replays,
    /// duplicate binding identities, and uncertain commits.
    pub fn prepare_registration(
        &mut self,
        migration: Uuid,
        request: &RegistrationRequest,
    ) -> Result<RegistrationProgress, OperationsError> {
        self.prepare_registration_with_profile(migration, request, None)
    }

    /// Admit or exactly replay a request with a host-resolved immutable Profile link.
    /// The link is persisted before the caller can dispatch a Forge reservation.
    ///
    /// # Errors
    /// Rejects foreign Sessions, changed Profile evidence, request conflicts and storage failures.
    pub fn prepare_profile_registration(
        &mut self,
        migration: Uuid,
        request: &RegistrationRequest,
        profile_link: heterarchy_orchestration_sessions::AgentProfileLogicalSessionLink,
    ) -> Result<RegistrationProgress, OperationsError> {
        self.prepare_registration_with_profile(migration, request, Some(profile_link))
    }

    fn prepare_registration_with_profile(
        &mut self,
        migration: Uuid,
        request: &RegistrationRequest,
        profile_link: Option<heterarchy_orchestration_sessions::AgentProfileLogicalSessionLink>,
    ) -> Result<RegistrationProgress, OperationsError> {
        let encoded = request.encode()?;
        let request_id = request.binding().request_id();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let import = verified_import(&transaction, migration, self.destination)?;

        if let Some(existing) = load_row(&transaction, request_id)? {
            if existing.migration != migration
                || existing.workspace != import.workspace
                || existing.destination != self.destination
                || existing.wire != encoded
            {
                return Err(OperationsError::Conflict("registration request replay"));
            }
            if let Some(link) = profile_link {
                crate::registration_profile::record(&transaction, import.workspace, request, link)?;
            }
            let result = read_row(&transaction, &existing)?;
            commit_registration(transaction, request_id)?;
            return Ok(result);
        }

        let workspace = import.workspace;
        let binding = request.binding();
        crate::leadership::work_roster::ensure_identity_admission_open(
            &transaction,
            workspace,
            binding.session_id(),
            profile_link
                .map(heterarchy_orchestration_sessions::AgentProfileLogicalSessionLink::profile_id),
        )?;
        ensure_agent_identity_available(&transaction, workspace, binding.agent_id())?;
        ensure_activity_session_available(
            &transaction,
            migration,
            binding.activity_id(),
            binding.session_id(),
        )?;

        let participant = if let Some(existing) =
            load_pending_identity(&transaction, workspace, binding.session_id())?
        {
            if existing.principal != binding.principal()
                || existing.display_name != request.display_name().as_str()
                || existing.declared_role != request.declared_role().as_str()
                || existing.transport != request.transport()
            {
                return Err(OperationsError::Conflict("pending participant identity"));
            }
            if existing.participant != request.proposed_participant_id()
                && pending_participant_exists(
                    &transaction,
                    workspace,
                    request.proposed_participant_id(),
                )?
            {
                return Err(OperationsError::Conflict("pending participant identity"));
            }
            existing.participant
        } else {
            if pending_participant_exists(
                &transaction,
                workspace,
                request.proposed_participant_id(),
            )? {
                return Err(OperationsError::Conflict("pending participant identity"));
            }
            insert_pending_identity(&transaction, workspace, request)?;
            request.proposed_participant_id()
        };

        transaction.execute(
            "INSERT INTO ops_registration_requests(request_id,migration_id,workspace_id,destination_id,activity_id,session_id,participant_id,agent_id,request_wire,phase,reserved_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,NULL)",
            params![
                request_id.as_bytes(),
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                self.destination.as_bytes(),
                binding.activity_id().into_uuid().as_bytes(),
                binding.session_id().into_uuid().as_bytes(),
                participant.as_bytes(),
                binding.agent_id().into_uuid().as_bytes(),
                encoded.as_slice(),
                RegistrationPhase::Pending.encode(),
            ],
        )?;
        if let Some(link) = profile_link {
            crate::registration_profile::record(&transaction, workspace, request, link)?;
        }
        let result = load_row(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt("registration insert"))
            .and_then(|row| read_row(&transaction, &row))?;
        commit_registration(transaction, request_id)?;
        Ok(result)
    }

    /// Record Forge's exact durable reservation receipt.
    ///
    /// This remains an internal composition boundary: it does not activate a
    /// participant or emit any operational event. The receipt is checked
    /// against the destination's still-verified import and exact source-freeze
    /// revision. It cannot by itself establish current source delegation or
    /// detect a later source release; activation must re-read that authority.
    ///
    /// # Errors
    /// Rejects mismatched reservations, late receipts, changed replays, and
    /// uncertain commits.
    pub fn record_registration_reservation(
        &mut self,
        reservation: &AgentBindingReservation,
    ) -> Result<RegistrationProgress, OperationsError> {
        let observation = BindingReservationObservation::from_native(reservation)?;
        self.record_registration_reservation_observation(&observation)
    }

    /// Record one checked readback of Forge's durable binding reservation.
    ///
    /// This remains an internal composition boundary: it does not activate a
    /// participant or emit any operational event. The observation is checked
    /// against the destination's verified import and exact source-freeze
    /// revision by the same transaction used for a native Forge receipt.
    ///
    /// # Errors
    /// Rejects mismatched reservations, late receipts, changed replays, and
    /// uncertain commits.
    pub fn record_registration_reservation_observation(
        &mut self,
        reservation: &BindingReservationObservation,
    ) -> Result<RegistrationProgress, OperationsError> {
        let request_id = reservation.request().request_id();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let import = verified_import(&transaction, reservation.migration_id(), self.destination)?;
        if import.workspace != reservation.workspace_id()
            || import.destination != reservation.destination_id()
            || import.source_revision != reservation.freeze_revision()
        {
            return Err(OperationsError::Conflict("reservation source identity"));
        }
        let Some(existing) = load_row(&transaction, request_id)? else {
            return Err(OperationsError::Conflict("registration request missing"));
        };
        if existing.migration != reservation.migration_id()
            || existing.workspace != reservation.workspace_id()
            || existing.destination != reservation.destination_id()
            || existing.activity != reservation.request().activity_id()
            || existing.session != reservation.request().session_id()
            || existing.agent != reservation.request().agent_id()
        {
            return Err(OperationsError::Conflict("reservation request identity"));
        }
        let existing_progress = read_row(&transaction, &existing)?;
        if existing_progress.request().binding() != reservation.request() {
            return Err(OperationsError::Conflict("reservation request payload"));
        }
        let reserved_at = reservation.reserved_at();
        if existing.phase == RegistrationPhase::Reserved {
            if existing.reserved_at != Some(reserved_at.get()) {
                return Err(OperationsError::Conflict("reservation replay"));
            }
            let result = read_row(&transaction, &existing)?;
            commit_registration(transaction, request_id)?;
            return Ok(result);
        }
        if existing.phase != RegistrationPhase::Pending {
            return Err(OperationsError::Corrupt("registration phase"));
        }
        let changed = transaction.execute(
            "UPDATE ops_registration_requests SET phase=?1,reserved_at_ms=?2 WHERE request_id=?3 AND phase=?4 AND reserved_at_ms IS NULL",
            params![
                RegistrationPhase::Reserved.encode(),
                reserved_at.get(),
                request_id.as_bytes(),
                RegistrationPhase::Pending.encode(),
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("registration reservation CAS"));
        }
        let result = load_row(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt(
                "registration reservation readback",
            ))
            .and_then(|row| read_row(&transaction, &row))?;
        commit_registration(transaction, request_id)?;
        Ok(result)
    }

    /// Read one durable pending/reserved registration proposal.
    ///
    /// Aborted migration rows remain readable as audit history.  Mutation APIs
    /// reject them before replay or readback can be used to advance state.
    ///
    /// # Errors
    /// Returns storage failures or a corrupt persisted registration payload.
    pub fn registration_progress(
        &self,
        request_id: Uuid,
    ) -> Result<Option<RegistrationProgress>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = progress_at(&transaction, request_id);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Read the immutable registration journey, including a replaced Session.
    ///
    /// `Ready` here records the original checked activation. It does not grant
    /// current participation; admission still uses `registration_progress`
    /// and the current binding/presence owners.
    ///
    /// # Errors
    /// Returns storage failures or corrupt historical registration/activation evidence.
    pub fn registration_audit_progress(
        &self,
        request_id: Uuid,
    ) -> Result<Option<RegistrationProgress>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let progress = committed_progress_at(&transaction, request_id)?;
        crate::catalog::commit_read(transaction, self.destination)?;
        let Some(mut progress) = progress else {
            return self.registration_progress(request_id);
        };
        if self.registration_activation(request_id)?.is_some() {
            progress.phase = RegistrationPhase::Ready;
        }
        Ok(Some(progress))
    }
}

pub(crate) fn progress_at(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RegistrationProgress>, OperationsError> {
    load_row(connection, request_id)?
        .map(|row| read_row(connection, &row))
        .transpose()
}

/// Read immutable committed registration evidence without deriving live Ready
/// authority from the participant's replaceable current Session.
pub(crate) fn committed_progress_at(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RegistrationProgress>, OperationsError> {
    let Some(raw) = load_row(connection, request_id)? else {
        return Ok(None);
    };
    if !crate::membership::is_committed(connection, request_id)? {
        return Ok(None);
    }
    if raw.phase == RegistrationPhase::Pending {
        return Err(OperationsError::Corrupt("membership phase"));
    }
    let request = read_request(connection, &raw)?;
    assemble_progress(&raw, request, RegistrationPhase::MembershipCommitted).map(Some)
}

pub(crate) fn session_is_ready(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    session_id: AgentSessionId,
    participant_id: Uuid,
) -> Result<bool, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(request_id,1,17)
         FROM ops_registration_requests
         WHERE workspace_id=?1 AND activity_id=?2 AND session_id=?3 AND participant_id=?4
         ORDER BY request_id
         LIMIT 2",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        activity_id.into_uuid().as_bytes(),
        session_id.into_uuid().as_bytes(),
        participant_id.as_bytes(),
    ])?;
    let Some(row) = rows.next()? else {
        return Ok(false);
    };
    let request_id = Uuid::from_bytes(import_wire::id(row.get::<_, Vec<u8>>(0)?)?);
    if rows.next()?.is_some() {
        return Err(OperationsError::Corrupt(
            "duplicate registration for Session participation",
        ));
    }
    let progress = progress_at(connection, request_id)?.ok_or(OperationsError::Corrupt(
        "registration disappeared during readiness read",
    ))?;
    if progress.workspace_id() != workspace_id
        || progress.participant_id() != participant_id
        || progress.request().binding().activity_id() != activity_id
        || progress.request().binding().session_id() != session_id
    {
        return Err(OperationsError::Corrupt(
            "registration readiness scope mismatch",
        ));
    }
    Ok(progress.phase() == RegistrationPhase::Ready)
}

#[derive(Debug)]
struct RawRegistration {
    request_id: Uuid,
    migration: Uuid,
    workspace: WorkspaceId,
    destination: Uuid,
    activity: ActivityId,
    session: AgentSessionId,
    participant: Uuid,
    agent: AgentInstanceId,
    wire: Vec<u8>,
    phase: RegistrationPhase,
    reserved_at: Option<i64>,
}

#[derive(Debug)]
struct RawPendingIdentity {
    participant: Uuid,
    principal: AuthenticatedPrincipal,
    display_name: String,
    declared_role: String,
    transport: AgentTransportKind,
}

pub(crate) fn verified_import(
    connection: &Connection,
    migration: Uuid,
    destination: Uuid,
) -> Result<crate::ImportProgress, OperationsError> {
    let import = load_progress(connection, migration)?
        .ok_or(OperationsError::Conflict("import not prepared"))?;
    if import.destination != destination {
        return Err(OperationsError::Conflict("catalog destination"));
    }
    match import.phase {
        ImportPhase::Verified => Ok(import),
        ImportPhase::Aborted => Err(OperationsError::Conflict("import aborted")),
        ImportPhase::Importing | ImportPhase::Imported => {
            Err(OperationsError::Conflict("import not verified"))
        }
    }
}

fn load_row(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawRegistration>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(migration_id,1,17),substr(workspace_id,1,17),substr(destination_id,1,17),substr(activity_id,1,17),substr(session_id,1,17),substr(participant_id,1,17),substr(agent_id,1,17),substr(request_wire,1,4097),phase,reserved_at_ms FROM ops_registration_requests WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(
            request,
            migration,
            workspace,
            destination,
            activity,
            session,
            participant,
            agent,
            request_wire,
            phase,
            reserved_at,
        )| {
            if reserved_at.is_some_and(|value| value < 0) {
                return Err(OperationsError::Corrupt("registration reserved timestamp"));
            }
            if reserved_at.is_none() && phase == 1 {
                return Err(OperationsError::Corrupt("registration reserved timestamp"));
            }
            if phase == 0 && reserved_at.is_some() {
                return Err(OperationsError::Corrupt("registration pending timestamp"));
            }
            if request_wire.is_empty() || request_wire.len() > MAX_REQUEST_WIRE_BYTES {
                return Err(OperationsError::Corrupt("registration wire bounds"));
            }
            Ok(RawRegistration {
                request_id: Uuid::from_bytes(import_wire::id(request)?),
                migration: Uuid::from_bytes(import_wire::id(migration)?),
                workspace: WorkspaceId::from_uuid(Uuid::from_bytes(import_wire::id(workspace)?)),
                destination: Uuid::from_bytes(import_wire::id(destination)?),
                activity: ActivityId::from_uuid(Uuid::from_bytes(import_wire::id(activity)?)),
                session: AgentSessionId::from_uuid(Uuid::from_bytes(import_wire::id(session)?)),
                participant: Uuid::from_bytes(import_wire::id(participant)?),
                agent: AgentInstanceId::from_uuid(Uuid::from_bytes(import_wire::id(agent)?)),
                wire: request_wire,
                phase: RegistrationPhase::decode(phase)?,
                reserved_at,
            })
        },
    )
    .transpose()
}

fn read_request(
    connection: &Connection,
    raw: &RawRegistration,
) -> Result<RegistrationRequest, OperationsError> {
    let request = RegistrationRequest::decode(&raw.wire)?;
    if request.binding().request_id() != raw.request_id
        || request.binding().activity_id() != raw.activity
        || request.binding().session_id() != raw.session
        || request.binding().agent_id() != raw.agent
    {
        return Err(OperationsError::Corrupt("registration wire identity"));
    }
    let identity = load_pending_identity(connection, raw.workspace, raw.session)?.ok_or(
        OperationsError::Corrupt("missing canonical pending participant"),
    )?;
    if identity.participant != raw.participant
        || identity.principal != request.binding().principal()
        || identity.display_name != request.display_name().as_str()
        || identity.declared_role != request.declared_role().as_str()
        || identity.transport != request.transport()
    {
        return Err(OperationsError::Corrupt("registration canonical metadata"));
    }
    Ok(request)
}

/// Check canonical request data without deriving readiness (which would recurse).
pub(crate) fn validate_ready_request(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    let raw = load_row(connection, request_id)?
        .ok_or(OperationsError::Corrupt("activation registration missing"))?;
    if raw.workspace != workspace_id || raw.participant != participant_id {
        return Err(OperationsError::Corrupt("activation registration scope"));
    }
    let _request = read_request(connection, &raw)?;
    Ok(())
}

fn read_row(
    connection: &Connection,
    raw: &RawRegistration,
) -> Result<RegistrationProgress, OperationsError> {
    let request = read_request(connection, raw)?;
    let phase = match raw.phase {
        RegistrationPhase::Pending => {
            if crate::membership::is_committed(connection, raw.request_id)? {
                return Err(OperationsError::Corrupt("membership phase"));
            }
            RegistrationPhase::Pending
        }
        RegistrationPhase::Reserved => {
            if crate::membership::is_committed(connection, raw.request_id)? {
                let import_verified = crate::catalog::load_progress(connection, raw.migration)?
                    .is_some_and(|progress| progress.phase() == ImportPhase::Verified);
                if import_verified
                    && crate::activation::request_ready_witness(
                        connection,
                        raw.request_id,
                        raw.workspace,
                        raw.participant,
                    )?
                {
                    RegistrationPhase::Ready
                } else {
                    RegistrationPhase::MembershipCommitted
                }
            } else {
                RegistrationPhase::Reserved
            }
        }
        RegistrationPhase::MembershipCommitted => {
            if !crate::membership::is_committed(connection, raw.request_id)? {
                return Err(OperationsError::Corrupt("membership commit missing"));
            }
            let import_verified = crate::catalog::load_progress(connection, raw.migration)?
                .is_some_and(|progress| progress.phase() == ImportPhase::Verified);
            if import_verified
                && crate::activation::request_ready_witness(
                    connection,
                    raw.request_id,
                    raw.workspace,
                    raw.participant,
                )?
            {
                RegistrationPhase::Ready
            } else {
                RegistrationPhase::MembershipCommitted
            }
        }
        RegistrationPhase::Ready => {
            let import_verified = crate::catalog::load_progress(connection, raw.migration)?
                .is_some_and(|progress| progress.phase() == ImportPhase::Verified);
            if !import_verified
                || !crate::membership::is_committed(connection, raw.request_id)?
                || !crate::activation::request_ready_witness(
                    connection,
                    raw.request_id,
                    raw.workspace,
                    raw.participant,
                )?
            {
                return Err(OperationsError::Corrupt("ready witness missing"));
            }
            RegistrationPhase::Ready
        }
    };
    assemble_progress(raw, request, phase)
}

fn assemble_progress(
    raw: &RawRegistration,
    request: RegistrationRequest,
    phase: RegistrationPhase,
) -> Result<RegistrationProgress, OperationsError> {
    let reserved_at = raw
        .reserved_at
        .map(|value| {
            TimestampMillis::new(value)
                .map_err(|_| OperationsError::Corrupt("registration timestamp"))
        })
        .transpose()?;
    Ok(RegistrationProgress {
        workspace: raw.workspace,
        migration: raw.migration,
        participant: raw.participant,
        request,
        phase,
        reserved_at,
    })
}

fn load_pending_identity(
    connection: &Connection,
    workspace: WorkspaceId,
    session: AgentSessionId,
) -> Result<Option<RawPendingIdentity>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(participant_id,1,17),principal_kind,substr(principal_id,1,17),substr(CAST(display_name AS BLOB),1,129),substr(CAST(declared_role AS BLOB),1,129),transport_kind FROM ops_pending_participants WHERE workspace_id=?1 AND session_id=?2",
            params![workspace.into_uuid().as_bytes(), session.into_uuid().as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(participant, principal_kind, principal_id, display_name, declared_role, transport)| {
            let principal = wire::decode_principal_fields(principal_kind, principal_id.as_deref())?;
            let display_name = String::from_utf8(display_name)
                .map_err(|_| OperationsError::Corrupt("pending display name"))?;
            let declared_role = String::from_utf8(declared_role)
                .map_err(|_| OperationsError::Corrupt("pending declared role"))?;
            Ok(RawPendingIdentity {
                participant: Uuid::from_bytes(import_wire::id(participant)?),
                principal,
                display_name,
                declared_role,
                transport: wire::decode_transport(transport)?,
            })
        },
    )
    .transpose()
}

fn pending_participant_exists(
    connection: &Connection,
    workspace: WorkspaceId,
    participant: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_pending_participants WHERE workspace_id=?1 AND participant_id=?2)",
        params![workspace.into_uuid().as_bytes(), participant.as_bytes()],
        |row| row.get(0),
    )?)
}

fn ensure_agent_identity_available(
    connection: &Connection,
    workspace: WorkspaceId,
    agent: AgentInstanceId,
) -> Result<(), OperationsError> {
    let occupied: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_members WHERE workspace_id=?1 AND agent_id=?2 UNION ALL SELECT 1 FROM ops_registration_requests WHERE workspace_id=?1 AND agent_id=?2)",
        params![workspace.into_uuid().as_bytes(), agent.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if occupied {
        return Err(OperationsError::Conflict("registration Agent identity"));
    }
    Ok(())
}

fn ensure_activity_session_available(
    connection: &Connection,
    migration: Uuid,
    activity: ActivityId,
    session: AgentSessionId,
) -> Result<(), OperationsError> {
    let occupied: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_registration_requests WHERE migration_id=?1 AND activity_id=?2 AND session_id=?3)",
        params![migration.as_bytes(), activity.into_uuid().as_bytes(), session.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if occupied {
        return Err(OperationsError::Conflict("registration activity/session"));
    }
    Ok(())
}

fn insert_pending_identity(
    transaction: &Transaction<'_>,
    workspace: WorkspaceId,
    request: &RegistrationRequest,
) -> Result<(), OperationsError> {
    let (principal_kind, principal_id) = wire::encode_principal(request.binding().principal());
    transaction.execute(
        "INSERT INTO ops_pending_participants(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            workspace.into_uuid().as_bytes(),
            request.proposed_participant_id().as_bytes(),
            request.binding().session_id().into_uuid().as_bytes(),
            principal_kind,
            principal_id.as_deref(),
            request.display_name().as_str(),
            request.declared_role().as_str(),
            wire::encode_transport(request.transport()),
        ],
    )?;
    Ok(())
}

fn commit_registration(
    transaction: Transaction<'_>,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::RegistrationCommitUnconfirmed { request_id, source })
}
