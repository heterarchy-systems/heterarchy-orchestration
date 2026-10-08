//! Atomic logical membership commit after a Forge binding reservation.
//!
//! This journal owns the Orchestration-side logical participant, binding
//! association, JOINED event, and recipient receipts.  It deliberately does
//! not create or mutate a Forge `Agent` binding and it exposes no Ready or
//! activation state.

use crate::{
    OperationsCatalog, OperationsError,
    registration::{self, RegistrationPhase, RegistrationProgress, RegistrationRequest},
    wire,
};
use forge_domain::{
    ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentSessionId,
    AgentTransportKind, AuthenticatedPrincipal, TimestampMillis, WorkspaceId,
};
use forge_persistence::AgentBindingReservationRequest;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_RECIPIENTS: usize = 64;
const MAX_VISIBLE_RESULTS: usize = 64;
const COMMIT_DIGEST_DOMAIN: &[u8] = b"heterarchy-orchestration-membership-commit-v1";
const JOINED_EVENT_DOMAIN: &[u8] = b"heterarchy-orchestration-membership-joined-v1";

const LOGICAL_MEMBERS_SQL: &str = "CREATE TABLE ops_logical_members (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), principal_kind INTEGER NOT NULL CHECK(principal_kind IN(0,1,2)), principal_id BLOB CHECK(principal_id IS NULL OR length(principal_id)=16), display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128), declared_role TEXT NOT NULL CHECK(length(CAST(declared_role AS BLOB)) BETWEEN 1 AND 128), transport_kind INTEGER NOT NULL CHECK(transport_kind IN(0,1,2,3)), membership_revision INTEGER NOT NULL CHECK(membership_revision=1), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), PRIMARY KEY(workspace_id,participant_id), UNIQUE(workspace_id,session_id), CHECK((principal_kind=0 AND principal_id IS NULL) OR (principal_kind IN(1,2) AND principal_id IS NOT NULL)))";
const LOGICAL_EVENTS_SQL: &str = "CREATE TABLE ops_logical_events (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), event_id BLOB NOT NULL CHECK(length(event_id)=16 AND event_id != zeroblob(16)), participant_id BLOB NOT NULL CHECK(length(participant_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), event_kind INTEGER NOT NULL CHECK(event_kind=0), membership_revision INTEGER NOT NULL CHECK(membership_revision=1), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), PRIMARY KEY(workspace_id,event_id), UNIQUE(workspace_id,participant_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id))";
const LOGICAL_BINDINGS_SQL: &str = "CREATE TABLE ops_logical_bindings (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), migration_id BLOB NOT NULL CHECK(length(migration_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), destination_id BLOB NOT NULL CHECK(length(destination_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), session_id BLOB NOT NULL CHECK(length(session_id)=16), agent_id BLOB NOT NULL CHECK(length(agent_id)=16), freeze_revision INTEGER NOT NULL CHECK(freeze_revision>=1), requested_at_ms INTEGER NOT NULL CHECK(requested_at_ms>=0), source_reserved_at_ms INTEGER NOT NULL CHECK(source_reserved_at_ms>=0), commit_digest BLOB NOT NULL CHECK(length(commit_digest)=32 AND commit_digest != zeroblob(32)), committed_at_ms INTEGER NOT NULL CHECK(committed_at_ms>=0), joined_event_id BLOB NOT NULL CHECK(length(joined_event_id)=16), FOREIGN KEY(request_id) REFERENCES ops_registration_requests(request_id), FOREIGN KEY(migration_id) REFERENCES ops_imports(migration_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id), FOREIGN KEY(workspace_id,joined_event_id) REFERENCES ops_logical_events(workspace_id,event_id), UNIQUE(workspace_id,agent_id), UNIQUE(migration_id,activity_id,session_id), UNIQUE(workspace_id,participant_id,activity_id))";
const LOGICAL_RECEIPTS_SQL: &str = "CREATE TABLE ops_logical_receipts (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), recipient_participant_id BLOB NOT NULL CHECK(length(recipient_participant_id)=16), event_id BLOB NOT NULL CHECK(length(event_id)=16), receipt_revision INTEGER NOT NULL CHECK(receipt_revision=1), committed_at_ms INTEGER NOT NULL CHECK(committed_at_ms>=0), PRIMARY KEY(workspace_id,recipient_participant_id,event_id), FOREIGN KEY(workspace_id,recipient_participant_id) REFERENCES ops_logical_members(workspace_id,participant_id), FOREIGN KEY(workspace_id,event_id) REFERENCES ops_logical_events(workspace_id,event_id))";

/// Durable objects owned by the logical membership commit boundary.
pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_logical_members", LOGICAL_MEMBERS_SQL),
        ("table", "ops_logical_events", LOGICAL_EVENTS_SQL),
        ("table", "ops_logical_bindings", LOGICAL_BINDINGS_SQL),
        ("table", "ops_logical_receipts", LOGICAL_RECEIPTS_SQL),
    ]
}

/// Create the logical membership journal in a caller-owned transaction.
pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Whether a registration has an immutable logical membership commit receipt.
pub(crate) fn is_committed(
    connection: &Connection,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_logical_bindings WHERE request_id=?1)",
        params![request_id.as_bytes()],
        |row| row.get(0),
    )?)
}

/// Immutable readback of the logical membership commit receipt.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipCommitReceipt {
    request_id: Uuid,
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    participant_id: Uuid,
    binding: AgentBindingReservationRequest,
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
    freeze_revision: u64,
    source_reserved_at: TimestampMillis,
    commit_digest: [u8; 32],
    committed_at: TimestampMillis,
    joined_event_id: Uuid,
}

impl MembershipCommitReceipt {
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

    #[must_use]
    pub const fn freeze_revision(&self) -> u64 {
        self.freeze_revision
    }

    #[must_use]
    pub const fn reserved_at(&self) -> TimestampMillis {
        self.source_reserved_at
    }

    #[must_use]
    pub const fn commit_digest(&self) -> [u8; 32] {
        self.commit_digest
    }

    #[must_use]
    pub const fn committed_at(&self) -> TimestampMillis {
        self.committed_at
    }

    #[must_use]
    pub const fn joined_event_id(&self) -> Uuid {
        self.joined_event_id
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleLogicalMember {
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
    membership_revision: u64,
}

impl VisibleLogicalMember {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }
    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }
    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
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
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibleLogicalEvent {
    workspace_id: WorkspaceId,
    event_id: Uuid,
    participant_id: Uuid,
    activity_id: ActivityId,
    membership_revision: u64,
    observed_at: TimestampMillis,
}

impl VisibleLogicalEvent {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }
    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibleLogicalReceipt {
    workspace_id: WorkspaceId,
    recipient_participant_id: Uuid,
    event_id: Uuid,
    receipt_revision: u64,
    committed_at: TimestampMillis,
}

impl VisibleLogicalReceipt {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn recipient_participant_id(&self) -> Uuid {
        self.recipient_participant_id
    }
    #[must_use]
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }
    #[must_use]
    pub const fn receipt_revision(&self) -> u64 {
        self.receipt_revision
    }
    #[must_use]
    pub const fn committed_at(&self) -> TimestampMillis {
        self.committed_at
    }
}

impl OperationsCatalog {
    /// Atomically commit one Reserved registration into logical membership,
    /// its one deterministic JOINED event, recipient receipts, and the
    /// per-binding commit receipt. This does not create a Forge Agent binding.
    ///
    /// # Errors
    /// Rejects an unverified or aborted import, a non-Reserved registration,
    /// malformed recipients, changed retries, canonical metadata corruption,
    /// and uncertain commits.
    pub fn commit_registration_membership(
        &mut self,
        request_id: Uuid,
        recipients: &[Uuid],
        observed_at: TimestampMillis,
    ) -> Result<MembershipCommitReceipt, OperationsError> {
        validate_recipients(recipients)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(progress) = registration::progress_at(&transaction, request_id)? else {
            return Err(OperationsError::Conflict("registration request missing"));
        };
        let import =
            registration::verified_import(&transaction, progress.migration_id(), self.destination)?;
        if import.workspace_id() != progress.workspace_id()
            || import.destination_id() != self.destination
            || progress.reserved_at().is_none()
        {
            return Err(OperationsError::Conflict("membership reservation identity"));
        }

        let digest = commit_digest(
            progress.request(),
            progress.participant_id(),
            import.source_revision(),
            recipients,
        )?;
        if matches!(
            progress.phase(),
            RegistrationPhase::MembershipCommitted | RegistrationPhase::Ready
        ) {
            let raw = load_commit(&transaction, request_id)?
                .ok_or(OperationsError::Corrupt("membership commit missing"))?;
            if raw.freeze_revision != import.source_revision() {
                return Err(OperationsError::Corrupt("membership freeze revision"));
            }
            if raw.commit_digest != digest {
                return Err(OperationsError::Conflict("membership commit replay"));
            }
            let receipt = read_commit_row(&transaction, self.destination, &progress, &raw)?;
            commit_membership(transaction, request_id)?;
            return Ok(receipt);
        }
        if progress.phase() != RegistrationPhase::Reserved {
            return Err(OperationsError::Conflict("registration not reserved"));
        }
        if load_commit(&transaction, request_id)?.is_some() {
            return Err(OperationsError::Corrupt("membership phase"));
        }

        ensure_logical_member(&transaction, &progress, observed_at)?;
        let joined_event_id = joined_event_id(progress.workspace_id(), progress.participant_id());
        let joined_existing = ensure_joined_event(
            &transaction,
            progress.workspace_id(),
            progress.participant_id(),
            progress.request().binding().activity_id(),
            joined_event_id,
            observed_at,
        )?;
        ensure_recipients_committed(&transaction, progress.workspace_id(), recipients)?;
        if !joined_existing {
            insert_receipts(
                &transaction,
                progress.workspace_id(),
                joined_event_id,
                recipients,
                observed_at,
            )?;
        }

        let binding = progress.request().binding();
        transaction.execute(
            "INSERT INTO ops_logical_bindings(request_id,migration_id,workspace_id,destination_id,participant_id,activity_id,session_id,agent_id,freeze_revision,requested_at_ms,source_reserved_at_ms,commit_digest,committed_at_ms,joined_event_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![
                request_id.as_bytes(),
                progress.migration_id().as_bytes(),
                progress.workspace_id().into_uuid().as_bytes(),
                self.destination.as_bytes(),
                progress.participant_id().as_bytes(),
                binding.activity_id().into_uuid().as_bytes(),
                binding.session_id().into_uuid().as_bytes(),
                binding.agent_id().into_uuid().as_bytes(),
                crate::catalog::signed(import.source_revision())?,
                binding.requested_at().get(),
                progress.reserved_at().ok_or(OperationsError::Corrupt("membership reservation timestamp"))?.get(),
                digest.as_slice(),
                observed_at.get(),
                joined_event_id.as_bytes(),
            ],
        )?;
        let raw = load_commit(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt("membership commit readback"))?;
        let receipt = read_commit_row(&transaction, self.destination, &progress, &raw)?;
        commit_membership(transaction, request_id)?;
        Ok(receipt)
    }

    /// Read one immutable logical membership commit receipt.
    ///
    /// # Errors
    /// Returns storage errors or corrupt registration/commit rows.
    pub fn membership_commit(
        &self,
        request_id: Uuid,
    ) -> Result<Option<MembershipCommitReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = commit_at(&transaction, self.destination, request_id);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}

pub(crate) fn commit_at(
    connection: &Connection,
    destination: Uuid,
    request_id: Uuid,
) -> Result<Option<MembershipCommitReceipt>, OperationsError> {
    let Some(progress) = registration::committed_progress_at(connection, request_id)? else {
        return Ok(None);
    };
    let Some(raw) = load_commit(connection, request_id)? else {
        return Ok(None);
    };
    let import = crate::catalog::load_progress(connection, progress.migration_id())?
        .ok_or(OperationsError::Corrupt("membership import missing"))?;
    if raw.freeze_revision != import.source_revision() {
        return Err(OperationsError::Corrupt("membership freeze revision"));
    }
    read_commit_row(connection, destination, &progress, &raw).map(Some)
}

impl OperationsCatalog {
    /// Read at most 64 logical members whose own binding has a checked Forge
    /// activation witness in a still-verified import.
    ///
    /// # Errors
    /// Returns bounds, storage, or corrupt-witness errors.
    pub fn visible_logical_members(
        &self,
        workspace_id: WorkspaceId,
        limit: u32,
    ) -> Result<Vec<VisibleLogicalMember>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = visible_logical_members_at(&transaction, workspace_id, limit);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Read at most 64 logically visible JOINED events, filtered by a checked
    /// activation witness and verified import.
    ///
    /// # Errors
    /// Returns bounds, storage, or corrupt-witness errors.
    pub fn visible_logical_events(
        &self,
        workspace_id: WorkspaceId,
        limit: u32,
    ) -> Result<Vec<VisibleLogicalEvent>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = visible_logical_events_at(&transaction, workspace_id, limit);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Read at most 64 logically visible recipient receipts, filtered by the
    /// same activation witness as the corresponding JOINED event.
    ///
    /// # Errors
    /// Returns bounds, storage, or corrupt-witness errors.
    pub fn visible_logical_receipts(
        &self,
        workspace_id: WorkspaceId,
        limit: u32,
    ) -> Result<Vec<VisibleLogicalReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = visible_logical_receipts_at(&transaction, workspace_id, limit);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}

fn visible_logical_members_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    limit: u32,
) -> Result<Vec<VisibleLogicalMember>, OperationsError> {
    let (limit, limit_usize) = visible_limit(limit)?;
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement = connection.prepare(
            "SELECT DISTINCT substr(m.workspace_id,1,17),substr(m.participant_id,1,17),substr(b.session_id,1,17),substr(m.session_id,1,17),m.principal_kind,substr(m.principal_id,1,17),substr(CAST(m.display_name AS BLOB),1,129),substr(CAST(m.declared_role AS BLOB),1,129),m.transport_kind,m.membership_revision FROM ops_current_effect_bindings b JOIN ops_logical_members m ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id WHERE b.workspace_id=?1 ORDER BY m.participant_id, (b.session_id != m.session_id) DESC LIMIT ?2",
        )?;
    let mut rows = statement.query(params![workspace_id.into_uuid().as_bytes(), limit])?;
    let mut visible = Vec::new();
    while let Some(row) = rows.next()? {
        let participant = Uuid::from_bytes(crate::wire::id(row.get(1)?)?);
        let binding_session =
            AgentSessionId::from_uuid(Uuid::from_bytes(crate::wire::id(row.get(2)?)?));
        let member_session =
            AgentSessionId::from_uuid(Uuid::from_bytes(crate::wire::id(row.get(3)?)?));
        if binding_session != member_session {
            return Err(OperationsError::Corrupt("current effect binding session"));
        }
        visible.push(VisibleLogicalMember {
            workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(crate::wire::id(row.get(0)?)?)),
            participant_id: participant,
            session_id: member_session,
            principal: registration::decode_principal_fields(
                row.get(4)?,
                row.get::<_, Option<Vec<u8>>>(5)?.as_deref(),
            )?,
            display_name: AgentDisplayName::new(
                String::from_utf8(row.get(6)?)
                    .map_err(|_| OperationsError::Corrupt("visible member display"))?,
            )
            .map_err(|_| OperationsError::Corrupt("visible member display"))?,
            declared_role: AgentDeclaredRole::new(
                String::from_utf8(row.get(7)?)
                    .map_err(|_| OperationsError::Corrupt("visible member role"))?,
            )
            .map_err(|_| OperationsError::Corrupt("visible member role"))?,
            transport: registration::decode_transport(row.get(8)?)?,
            membership_revision: u64::try_from(row.get::<_, i64>(9)?)
                .map_err(|_| OperationsError::Corrupt("visible member revision"))?,
        });
        if visible.len() == limit_usize {
            break;
        }
    }
    Ok(visible)
}

fn visible_logical_events_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    limit: u32,
) -> Result<Vec<VisibleLogicalEvent>, OperationsError> {
    let (limit, limit_usize) = visible_limit(limit)?;
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement = connection.prepare(
            "SELECT substr(e.workspace_id,1,17),substr(e.event_id,1,17),substr(e.participant_id,1,17),substr(e.activity_id,1,17),e.membership_revision,e.observed_at_ms FROM ops_logical_events e JOIN ops_current_effect_bindings b ON b.workspace_id=e.workspace_id AND b.participant_id=e.participant_id AND b.activity_id=e.activity_id WHERE e.workspace_id=?1 ORDER BY e.event_id LIMIT ?2",
        )?;
    let mut rows = statement.query(params![workspace_id.into_uuid().as_bytes(), limit])?;
    let mut visible = Vec::new();
    while let Some(row) = rows.next()? {
        let event_id = Uuid::from_bytes(crate::wire::id(row.get(1)?)?);
        let participant = Uuid::from_bytes(crate::wire::id(row.get(2)?)?);
        visible.push(VisibleLogicalEvent {
            workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(crate::wire::id(row.get(0)?)?)),
            event_id,
            participant_id: participant,
            activity_id: ActivityId::from_uuid(Uuid::from_bytes(crate::wire::id(row.get(3)?)?)),
            membership_revision: u64::try_from(row.get::<_, i64>(4)?)
                .map_err(|_| OperationsError::Corrupt("visible event revision"))?,
            observed_at: TimestampMillis::new(row.get(5)?)
                .map_err(|_| OperationsError::Corrupt("visible event timestamp"))?,
        });
        if visible.len() == limit_usize {
            break;
        }
    }
    Ok(visible)
}

fn visible_logical_receipts_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    limit: u32,
) -> Result<Vec<VisibleLogicalReceipt>, OperationsError> {
    let (limit, limit_usize) = visible_limit(limit)?;
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement = connection.prepare(
            "SELECT substr(r.workspace_id,1,17),substr(r.recipient_participant_id,1,17),substr(r.event_id,1,17),r.receipt_revision,r.committed_at_ms FROM ops_logical_receipts r JOIN ops_logical_events e ON e.workspace_id=r.workspace_id AND e.event_id=r.event_id JOIN ops_current_effect_bindings b ON b.workspace_id=e.workspace_id AND b.participant_id=e.participant_id AND b.activity_id=e.activity_id WHERE r.workspace_id=?1 ORDER BY r.event_id,r.recipient_participant_id LIMIT ?2",
        )?;
    let mut rows = statement.query(params![workspace_id.into_uuid().as_bytes(), limit])?;
    let mut visible = Vec::new();
    while let Some(row) = rows.next()? {
        let event_id = Uuid::from_bytes(crate::wire::id(row.get(2)?)?);
        visible.push(VisibleLogicalReceipt {
            workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(crate::wire::id(row.get(0)?)?)),
            recipient_participant_id: Uuid::from_bytes(crate::wire::id(row.get(1)?)?),
            event_id,
            receipt_revision: u64::try_from(row.get::<_, i64>(3)?)
                .map_err(|_| OperationsError::Corrupt("visible receipt revision"))?,
            committed_at: TimestampMillis::new(row.get(4)?)
                .map_err(|_| OperationsError::Corrupt("visible receipt timestamp"))?,
        });
        if visible.len() == limit_usize {
            break;
        }
    }
    Ok(visible)
}

fn visible_limit(limit: u32) -> Result<(i64, usize), OperationsError> {
    let limit_usize =
        usize::try_from(limit).map_err(|_| OperationsError::Bounds("visible logical results"))?;
    if limit_usize > MAX_VISIBLE_RESULTS {
        return Err(OperationsError::Bounds("visible logical results"));
    }
    Ok((i64::from(limit), limit_usize))
}

#[derive(Debug)]
struct RawCommit {
    request_id: Uuid,
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    participant_id: Uuid,
    activity_id: forge_domain::ActivityId,
    session_id: AgentSessionId,
    agent_id: AgentInstanceId,
    freeze_revision: u64,
    requested_at: TimestampMillis,
    source_reserved_at: TimestampMillis,
    commit_digest: [u8; 32],
    committed_at: TimestampMillis,
    joined_event_id: Uuid,
}

fn load_commit(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawCommit>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(migration_id,1,17),substr(workspace_id,1,17),substr(destination_id,1,17),substr(participant_id,1,17),substr(activity_id,1,17),substr(session_id,1,17),substr(agent_id,1,17),freeze_revision,requested_at_ms,source_reserved_at_ms,substr(commit_digest,1,33),committed_at_ms,substr(joined_event_id,1,17) FROM ops_logical_bindings WHERE request_id=?1",
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
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, Vec<u8>>(11)?,
                    row.get::<_, i64>(12)?,
                    row.get::<_, Vec<u8>>(13)?,
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
            participant,
            activity,
            session,
            agent,
            freeze,
            requested,
            source_reserved,
            digest,
            committed,
            event,
        )| {
            let digest: [u8; 32] = digest
                .try_into()
                .map_err(|_| OperationsError::Corrupt("membership commit digest"))?;
            Ok(RawCommit {
                request_id: Uuid::from_bytes(wire::id(request)?),
                migration_id: Uuid::from_bytes(wire::id(migration)?),
                workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
                destination_id: Uuid::from_bytes(wire::id(destination)?),
                participant_id: Uuid::from_bytes(wire::id(participant)?),
                activity_id: forge_domain::ActivityId::from_uuid(Uuid::from_bytes(wire::id(
                    activity,
                )?)),
                session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(session)?)),
                agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(agent)?)),
                freeze_revision: u64::try_from(freeze)
                    .map_err(|_| OperationsError::Corrupt("membership freeze revision"))?,
                requested_at: TimestampMillis::new(requested)
                    .map_err(|_| OperationsError::Corrupt("membership requested timestamp"))?,
                source_reserved_at: TimestampMillis::new(source_reserved)
                    .map_err(|_| OperationsError::Corrupt("membership reserved timestamp"))?,
                commit_digest: digest,
                committed_at: TimestampMillis::new(committed)
                    .map_err(|_| OperationsError::Corrupt("membership committed timestamp"))?,
                joined_event_id: Uuid::from_bytes(wire::id(event)?),
            })
        },
    )
    .transpose()
}

fn read_commit_row(
    connection: &Connection,
    destination: Uuid,
    progress: &RegistrationProgress,
    raw: &RawCommit,
) -> Result<MembershipCommitReceipt, OperationsError> {
    let binding = progress.request().binding();
    if raw.request_id != binding.request_id()
        || raw.migration_id != progress.migration_id()
        || raw.workspace_id != progress.workspace_id()
        || raw.destination_id != destination
        || raw.participant_id != progress.participant_id()
        || raw.activity_id != binding.activity_id()
        || raw.session_id != binding.session_id()
        || raw.agent_id != binding.agent_id()
        || raw.freeze_revision == 0
        || raw.requested_at != binding.requested_at()
        || Some(raw.source_reserved_at) != progress.reserved_at()
        || raw.joined_event_id != joined_event_id(raw.workspace_id, raw.participant_id)
        || !is_committed(connection, raw.request_id)?
    {
        return Err(OperationsError::Corrupt("membership commit identity"));
    }
    Ok(MembershipCommitReceipt {
        request_id: raw.request_id,
        migration_id: raw.migration_id,
        workspace_id: raw.workspace_id,
        destination_id: raw.destination_id,
        participant_id: raw.participant_id,
        binding: binding.clone(),
        display_name: progress.request().display_name().clone(),
        declared_role: progress.request().declared_role().clone(),
        transport: progress.request().transport(),
        freeze_revision: raw.freeze_revision,
        source_reserved_at: raw.source_reserved_at,
        commit_digest: raw.commit_digest,
        committed_at: raw.committed_at,
        joined_event_id: raw.joined_event_id,
    })
}

pub(crate) fn validate_recipients(recipients: &[Uuid]) -> Result<(), OperationsError> {
    if recipients.len() > MAX_RECIPIENTS {
        return Err(OperationsError::Bounds("membership recipients"));
    }
    if recipients.iter().any(Uuid::is_nil) {
        return Err(OperationsError::Conflict("membership recipient identity"));
    }
    if recipients.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(OperationsError::Conflict("membership recipients order"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalMemberIdentity {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) participant_id: Uuid,
    pub(crate) session_id: AgentSessionId,
    pub(crate) principal: AuthenticatedPrincipal,
    pub(crate) display_name: AgentDisplayName,
    pub(crate) declared_role: AgentDeclaredRole,
    pub(crate) transport: AgentTransportKind,
}

fn ensure_logical_member(
    transaction: &Transaction<'_>,
    progress: &RegistrationProgress,
    observed_at: TimestampMillis,
) -> Result<(), OperationsError> {
    let request = progress.request();
    let binding = request.binding();
    ensure_logical_member_identity(
        transaction,
        &LogicalMemberIdentity {
            workspace_id: progress.workspace_id(),
            participant_id: progress.participant_id(),
            session_id: binding.session_id(),
            principal: binding.principal(),
            display_name: request.display_name().clone(),
            declared_role: request.declared_role().clone(),
            transport: request.transport(),
        },
        observed_at,
    )
}

pub(crate) fn ensure_logical_member_identity(
    transaction: &Transaction<'_>,
    identity: &LogicalMemberIdentity,
    observed_at: TimestampMillis,
) -> Result<(), OperationsError> {
    let existing = transaction
        .query_row(
            "SELECT substr(participant_id,1,17),principal_kind,substr(principal_id,1,17),substr(CAST(display_name AS BLOB),1,129),substr(CAST(declared_role AS BLOB),1,129),transport_kind,membership_revision FROM ops_logical_members WHERE workspace_id=?1 AND session_id=?2",
            params![identity.workspace_id.into_uuid().as_bytes(), identity.session_id.into_uuid().as_bytes()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<Vec<u8>>>(2)?, row.get::<_, Vec<u8>>(3)?, row.get::<_, Vec<u8>>(4)?, row.get::<_, i64>(5)?, row.get::<_, i64>(6)?)),
        )
        .optional()?;
    if let Some((participant, kind, principal_id, display, role, transport, revision)) = existing {
        if revision != 1
            || Uuid::from_bytes(wire::id(participant)?) != identity.participant_id
            || registration::decode_principal_fields(kind, principal_id.as_deref())?
                != identity.principal
            || String::from_utf8(display)
                .map_err(|_| OperationsError::Corrupt("logical member display"))?
                != identity.display_name.as_str()
            || String::from_utf8(role)
                .map_err(|_| OperationsError::Corrupt("logical member role"))?
                != identity.declared_role.as_str()
            || registration::decode_transport(transport)? != identity.transport
        {
            return Err(OperationsError::Conflict("logical member identity"));
        }
        return Ok(());
    }
    if transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2)",
        params![identity.workspace_id.into_uuid().as_bytes(), identity.participant_id.as_bytes()],
        |row| row.get(0),
    )? {
        return Err(OperationsError::Conflict("logical member identity"));
    }
    let (principal_kind, principal_id) = registration::encode_principal(identity.principal);
    transaction.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,1,?9)",
        params![
            identity.workspace_id.into_uuid().as_bytes(),
            identity.participant_id.as_bytes(),
            identity.session_id.into_uuid().as_bytes(),
            principal_kind,
            principal_id.as_deref(),
            identity.display_name.as_str(),
            identity.declared_role.as_str(),
            registration::encode_transport(identity.transport),
            observed_at.get(),
        ],
    )?;
    Ok(())
}

pub(crate) fn ensure_joined_event(
    transaction: &Transaction<'_>,
    workspace: WorkspaceId,
    participant: Uuid,
    activity: forge_domain::ActivityId,
    event_id: Uuid,
    observed_at: TimestampMillis,
) -> Result<bool, OperationsError> {
    let existing: Option<(Vec<u8>, Vec<u8>, i64, i64)> = transaction
        .query_row(
            "SELECT substr(event_id,1,17),substr(participant_id,1,17),event_kind,membership_revision FROM ops_logical_events WHERE workspace_id=?1 AND participant_id=?2",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    if let Some((stored_event, stored_participant, kind, revision)) = existing {
        if Uuid::from_bytes(wire::id(stored_event)?) != event_id
            || Uuid::from_bytes(wire::id(stored_participant)?) != participant
            || kind != 0
            || revision != 1
        {
            return Err(OperationsError::Corrupt("logical joined event"));
        }
        return Ok(true);
    }
    transaction.execute(
        "INSERT INTO ops_logical_events(workspace_id,event_id,participant_id,activity_id,event_kind,membership_revision,observed_at_ms) VALUES(?1,?2,?3,?4,0,1,?5)",
        params![workspace.into_uuid().as_bytes(), event_id.as_bytes(), participant.as_bytes(), activity.into_uuid().as_bytes(), observed_at.get()],
    )?;
    Ok(false)
}

pub(crate) fn ensure_recipients_committed(
    transaction: &Transaction<'_>,
    workspace: WorkspaceId,
    recipients: &[Uuid],
) -> Result<(), OperationsError> {
    for recipient in recipients {
        let present: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2)",
            params![workspace.into_uuid().as_bytes(), recipient.as_bytes()],
            |row| row.get(0),
        )?;
        if !present {
            return Err(OperationsError::Conflict("membership recipient"));
        }
    }
    Ok(())
}

pub(crate) fn insert_receipts(
    transaction: &Transaction<'_>,
    workspace: WorkspaceId,
    event_id: Uuid,
    recipients: &[Uuid],
    committed_at: TimestampMillis,
) -> Result<(), OperationsError> {
    for recipient in recipients {
        transaction.execute(
            "INSERT INTO ops_logical_receipts(workspace_id,recipient_participant_id,event_id,receipt_revision,committed_at_ms) VALUES(?1,?2,?3,1,?4) ON CONFLICT(workspace_id,recipient_participant_id,event_id) DO NOTHING",
            params![workspace.into_uuid().as_bytes(), recipient.as_bytes(), event_id.as_bytes(), committed_at.get()],
        )?;
    }
    Ok(())
}

pub(crate) fn joined_event_id(workspace: WorkspaceId, participant: Uuid) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(JOINED_EVENT_DOMAIN);
    digest.update((16_u32).to_be_bytes());
    digest.update(workspace.into_uuid().as_bytes());
    digest.update((16_u32).to_be_bytes());
    digest.update(participant.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

fn commit_digest(
    request: &RegistrationRequest,
    participant: Uuid,
    freeze_revision: u64,
    recipients: &[Uuid],
) -> Result<[u8; 32], OperationsError> {
    let request_wire = request.encode()?;
    let mut digest = Sha256::new();
    digest.update(COMMIT_DIGEST_DOMAIN);
    digest.update(
        u32::try_from(request_wire.len())
            .map_err(|_| OperationsError::Bounds("membership digest request"))?
            .to_be_bytes(),
    );
    digest.update(request_wire);
    digest.update((16_u32).to_be_bytes());
    digest.update(participant.as_bytes());
    digest.update(freeze_revision.to_be_bytes());
    frame_text(&mut digest, request.display_name().as_str())?;
    frame_text(&mut digest, request.declared_role().as_str())?;
    let transport = u8::try_from(registration::encode_transport(request.transport()))
        .map_err(|_| OperationsError::Corrupt("membership transport"))?;
    digest.update([transport]);
    digest.update(
        u32::try_from(recipients.len())
            .map_err(|_| OperationsError::Bounds("membership digest recipients"))?
            .to_be_bytes(),
    );
    for recipient in recipients {
        digest.update(recipient.as_bytes());
    }
    Ok(digest.finalize().into())
}

fn frame_text(digest: &mut Sha256, value: &str) -> Result<(), OperationsError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| OperationsError::Bounds("membership digest text"))?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn commit_membership(
    transaction: Transaction<'_>,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::MembershipCommitUnconfirmed { request_id, source })
}
