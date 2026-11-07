//! Transactional first-attempt Source dispatch and immutable Source readback.

use forge_domain::{HomeTransferSourceAction, TimestampMillis, WorkspaceId};
use forge_persistence::HomeTransferSourceRecord;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use super::super::{
    ActorSnapshot, authorize_current_orchestrator, consent_reader_has_orchestrator_access,
    load_intent_in_workspace, preparation,
};
use super::model::{
    self, TeamPermanentTransferSourceDispatchCommand as DispatchCommand,
    TeamPermanentTransferSourceDispatchOutcome as DispatchOutcome,
    TeamPermanentTransferSourceDispatchPermit as DispatchPermit,
    TeamPermanentTransferSourceDispatchReadback as DispatchReadback,
    TeamPermanentTransferSourceDispatchReceipt as DispatchReceipt,
    TeamPermanentTransferSourceDispatchReplay as DispatchReplay,
    TeamPermanentTransferSourceReceiptCommand as SourceReceiptCommand,
    TeamPermanentTransferSourceReceiptOutcome as SourceReceiptOutcome,
    TeamPermanentTransferSourceReceiptRecord as SourceReceiptRecord,
};
use super::schema;
use crate::{
    OperationalActor, OperationsCatalog, OperationsError,
    TeamPermanentTransferSourceDispatchCommand, TeamPermanentTransferSourceDispatchOutcome,
    TeamPermanentTransferSourceDispatchReadback, TeamPermanentTransferSourceReceiptCommand,
    TeamPermanentTransferSourceReceiptOutcome, teams,
};

/// Recorded preparation and its original confirmed Source result, observed in
/// one caller-owned Operations transaction. This does not grant fresh admission.
#[must_use]
#[derive(Debug)]
pub(crate) struct HomeSourcePreparationContext {
    preparation: crate::TeamPermanentTransferPreparationReceipt,
    source: SourceReceiptRecord,
}

impl HomeSourcePreparationContext {
    pub(crate) const fn preparation(&self) -> &crate::TeamPermanentTransferPreparationReceipt {
        &self.preparation
    }

    pub(crate) const fn source(&self) -> &SourceReceiptRecord {
        &self.source
    }
}

/// Internal composition seam: reuse the existing checked preparation, dispatch
/// and original-receipt readers without opening a nested transaction or I/O.
pub(crate) fn load_home_preparation_for_handoff_in_transaction(
    connection: &Connection,
    workspace_id: WorkspaceId,
    preparation_id: Uuid,
    catalog_destination: Uuid,
) -> Result<Option<HomeSourcePreparationContext>, OperationsError> {
    if workspace_id.into_uuid().is_nil() || preparation_id.is_nil() || catalog_destination.is_nil()
    {
        return Err(OperationsError::Bounds("Home acceptance Source identity"));
    }
    teams::ensure_verified_workspace(connection, workspace_id)?;
    let dispatch = load_dispatch_by_preparation(
        connection,
        workspace_id,
        preparation_id,
        catalog_destination,
    )?;
    let Some(dispatch) = dispatch else {
        let orphan: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_permanent_transfer_source_receipts
             WHERE workspace_id=?1 AND preparation_id=?2)",
            params![
                workspace_id.into_uuid().as_bytes(),
                preparation_id.as_bytes()
            ],
            |row| row.get(0),
        )?;
        if orphan {
            return Err(OperationsError::Corrupt("Source receipt has no dispatch"));
        }
        return Ok(None);
    };
    let preparation = preparation::load_in_transaction(connection, workspace_id, preparation_id)?
        .ok_or(OperationsError::Corrupt(
        "Home acceptance preparation is missing",
    ))?;
    if preparation.operations_destination_id() != catalog_destination {
        return Err(OperationsError::Corrupt(
            "Home acceptance preparation destination",
        ));
    }
    let Some(source) = load_source_receipt(connection, &dispatch)? else {
        return Ok(None);
    };
    if source.source_record().state() != forge_persistence::HomeTransferSourceState::Prepared {
        return Err(OperationsError::Conflict(
            "Home acceptance Source is not Prepared",
        ));
    }
    Ok(Some(HomeSourcePreparationContext {
        preparation,
        source,
    }))
}

impl OperationsCatalog {
    /// Commit the unique first Source attempt for one immutable preparation.
    ///
    /// A `Created` result is the only value that may be handed to the Source
    /// transport. An exact retry returns `Existing` without its transport ID,
    /// grant, route or wire identity, so replay never authorizes a resend.
    ///
    /// # Errors
    /// Rejects stale leadership/consents/generation, a changed Source request,
    /// invalid route or grant, a competing first attempt, and uncertain commit.
    pub fn commit_team_permanent_transfer_source_dispatch(
        &mut self,
        command: &TeamPermanentTransferSourceDispatchCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamPermanentTransferSourceDispatchOutcome, OperationsError> {
        commit_source_dispatch(self, command, trusted_now)
    }

    /// Read one immutable Source attempt and original Source result for the
    /// preparation author, attempt author, or current Workspace Orchestrator.
    /// This read never contacts Source and never grants send authority.
    ///
    /// # Errors
    /// Rejects malformed actor identities, corrupt stored snapshots, and I/O.
    pub fn read_team_permanent_transfer_source_dispatch(
        &self,
        workspace_id: WorkspaceId,
        preparation_id: Uuid,
        actor_input: OperationalActor,
    ) -> Result<Option<TeamPermanentTransferSourceDispatchReadback>, OperationsError> {
        read_source_dispatch(self, workspace_id, preparation_id, actor_input)
    }

    /// Persist a Source result returned by the trusted Backend after it
    /// authenticated the pinned loopback peer. The public snapshot constructor
    /// proves internal consistency, not Source authentication; that trust check
    /// belongs to Backend before this owner is called.
    ///
    /// Receipt ingestion revalidates the immutable attempt and Source request,
    /// but does not require current leader/consent authority: it records the
    /// historical Source fact after network I/O.
    ///
    /// # Errors
    /// Rejects a foreign dispatch, changed Source snapshot, stale local clock,
    /// malformed FHTS bytes, and uncertain commit.
    pub fn record_team_permanent_transfer_source_receipt(
        &mut self,
        command: &TeamPermanentTransferSourceReceiptCommand,
    ) -> Result<TeamPermanentTransferSourceReceiptOutcome, OperationsError> {
        record_source_receipt(self, command)
    }
}

fn commit_source_dispatch(
    catalog: &mut OperationsCatalog,
    command: &DispatchCommand,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<DispatchOutcome, OperationsError> {
    let catalog_destination = catalog.destination_id();
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
    let context = load_dispatch_context(&transaction, command, catalog_destination)?;

    if let Some(existing) = load_dispatch_by_preparation(
        &transaction,
        command.workspace_id,
        command.preparation_id,
        catalog_destination,
    )? {
        if same_replay_context(&existing, command, &context.actor) {
            let replay = DispatchReplay::new(
                existing.preparation_id(),
                existing.request_id(),
                existing.recorded_at(),
                existing.envelope_digest(),
            );
            crate::catalog::commit_read(transaction, catalog_destination)?;
            return Ok(DispatchOutcome::Existing(replay));
        }
        return Err(OperationsError::Conflict(
            "permanent transfer Source first attempt already exists; read original",
        ));
    }
    let now = validate_new_dispatch_admission(
        &transaction,
        command,
        &context.actor,
        &context.preparation,
        trusted_now,
        catalog_destination,
    )?;
    let receipt =
        persist_source_dispatch(&transaction, command, &context, now, catalog_destination)?;
    transaction
        .commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id: command.workspace_id,
            request_id: command.request_id,
            source,
        })?;
    Ok(DispatchOutcome::Created(DispatchPermit::new(receipt)))
}

fn validate_new_dispatch_admission(
    transaction: &Transaction<'_>,
    command: &DispatchCommand,
    actor: &ActorSnapshot,
    preparation: &crate::TeamPermanentTransferPreparationReceipt,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    catalog_destination: Uuid,
) -> Result<TimestampMillis, OperationsError> {
    if load_dispatch_by_request(
        transaction,
        command.workspace_id,
        command.request_id,
        catalog_destination,
    )?
    .is_some()
    {
        return Err(OperationsError::Conflict(
            "permanent transfer Source mutation request identity",
        ));
    }
    let current_orchestrator_revision =
        authorize_current_orchestrator(transaction, command.workspace_id, actor)?;
    if current_orchestrator_revision != command.expected_workspace_leadership_revision {
        return Err(OperationsError::Conflict(
            "permanent transfer Source Orchestrator revision",
        ));
    }
    let intent = load_intent_in_workspace(transaction, command.workspace_id, command.transfer_id)?
        .ok_or(OperationsError::Corrupt(
            "permanent transfer Source preparation intent",
        ))?;
    let source_high_water =
        preparation::admission::validate_current_source(transaction, preparation, &intent)?;

    // Sample only after the current authority and Source consent checks.
    let now = trusted_now()?;
    let high_water = source_high_water.max(preparation.recorded_at().get());
    if now.get() < high_water {
        return Err(OperationsError::Conflict(
            "permanent transfer Source dispatch time regression",
        ));
    }
    Ok(now)
}

struct SourceDispatchContext {
    actor: ActorSnapshot,
    preparation: crate::TeamPermanentTransferPreparationReceipt,
    source_request: forge_domain::HomeTransferSourceRequest,
    source_envelope: Vec<u8>,
}

fn load_dispatch_context(
    transaction: &Transaction<'_>,
    command: &DispatchCommand,
    catalog_destination: Uuid,
) -> Result<SourceDispatchContext, OperationsError> {
    let actor = ActorSnapshot::resolve(transaction, command.workspace_id, command.actor)?;
    let preparation = preparation::load_in_transaction(
        transaction,
        command.workspace_id,
        command.preparation_id,
    )?
    .ok_or(OperationsError::Conflict(
        "permanent transfer Source preparation missing",
    ))?;
    validate_preparation_context(command, &preparation, catalog_destination)?;
    let source_request = model::derive_team_permanent_transfer_source_request(
        &preparation,
        &command.source_scope,
        command.backend_id,
        command.service_destination_id,
    )?;
    if source_request != command.source_request {
        return Err(OperationsError::Conflict(
            "permanent transfer Source request differs from preparation",
        ));
    }
    let source_envelope = model::encode_source_request(&source_request)?;
    Ok(SourceDispatchContext {
        actor,
        preparation,
        source_request,
        source_envelope,
    })
}

fn persist_source_dispatch(
    transaction: &Transaction<'_>,
    command: &DispatchCommand,
    context: &SourceDispatchContext,
    recorded_at: TimestampMillis,
    catalog_destination: Uuid,
) -> Result<DispatchReceipt, OperationsError> {
    command
        .grant
        .validate_for(
            command.issuer_id,
            HomeTransferSourceAction::Prepare,
            context.source_request.request_digest(),
            recorded_at,
        )
        .map_err(|_| OperationsError::Conflict("permanent transfer Source grant"))?;
    let payload =
        model::dispatch_payload(command, &context.preparation, &context.actor, recorded_at);
    let payload_bytes = model::encode_payload(&payload)?;
    if payload_bytes.len() > schema::MAX_DISPATCH_PAYLOAD_BYTES {
        return Err(OperationsError::Bounds(
            "permanent transfer Source dispatch payload",
        ));
    }
    let payload_digest = model::payload_digest(&payload_bytes, &context.source_envelope);
    insert_dispatch(
        transaction,
        command,
        &context.actor,
        &context.source_envelope,
        &payload_bytes,
        payload_digest,
        recorded_at,
    )?;
    let receipt = load_dispatch_by_preparation(
        transaction,
        command.workspace_id,
        command.preparation_id,
        catalog_destination,
    )?
    .ok_or(OperationsError::Corrupt(
        "permanent transfer Source dispatch insert readback",
    ))?;
    if !same_created_attempt(
        &receipt,
        command,
        &context.actor,
        payload_digest,
        recorded_at,
    ) {
        return Err(OperationsError::Corrupt(
            "permanent transfer Source dispatch insert identity",
        ));
    }
    Ok(receipt)
}

fn validate_preparation_context(
    command: &DispatchCommand,
    preparation: &crate::TeamPermanentTransferPreparationReceipt,
    catalog_destination: Uuid,
) -> Result<(), OperationsError> {
    if command.service_destination_id != catalog_destination
        || command.workspace_id != preparation.workspace_id()
        || command.preparation_id != preparation.preparation_id()
        || command.transfer_id != preparation.transfer_id()
        || command.generation != preparation.generation()
        || command.source_scope.workspace_id() != preparation.workspace_id().into_uuid()
        || command.source_request.witness().preparation_id() != preparation.preparation_id()
        || command.source_request.witness().intent_digest() != &preparation.intent_digest()
        || command.source_request.witness().preparation_digest() != &preparation.digest()
    {
        return Err(OperationsError::Conflict(
            "permanent transfer Source preparation pins",
        ));
    }
    Ok(())
}

fn same_replay_context(
    existing: &DispatchReceipt,
    command: &DispatchCommand,
    actor: &ActorSnapshot,
) -> bool {
    existing.request_id() == command.request_id
        && existing.workspace_id() == command.workspace_id
        && existing.preparation_id() == command.preparation_id
        && existing.transfer_id() == command.transfer_id
        && existing.generation() == command.generation
        && existing.expected_workspace_leadership_revision()
            == command.expected_workspace_leadership_revision
        && existing.actor() == actor
        && existing.source_route() == command.source_route
        && existing.source_scope() == &command.source_scope
        && existing.backend_id() == command.backend_id
        && existing.issuer_id() == command.issuer_id
        && existing.service_destination_id() == command.service_destination_id
        && existing.source_request() == &command.source_request
        && existing.envelope_digest() == command.envelope_digest()
        && existing.grant().issuer() == command.grant.issuer()
        && existing.grant().action() == command.grant.action()
        && existing.grant().request_digest() == command.grant.request_digest()
}

fn same_created_attempt(
    existing: &DispatchReceipt,
    command: &DispatchCommand,
    actor: &ActorSnapshot,
    payload_digest: [u8; 32],
    recorded_at: TimestampMillis,
) -> bool {
    same_replay_context(existing, command, actor)
        && existing.grant().issued_at() == command.grant.issued_at()
        && existing.grant().expires_at() == command.grant.expires_at()
        && existing.wire_identity() == command.wire_identity
        && existing.payload_digest() == payload_digest
        && existing.recorded_at() == recorded_at
}

fn insert_dispatch(
    transaction: &Transaction<'_>,
    command: &DispatchCommand,
    actor: &ActorSnapshot,
    source_envelope: &[u8],
    payload: &[u8],
    payload_digest: [u8; 32],
    recorded_at: TimestampMillis,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_team_permanent_transfer_source_dispatches(
             workspace_id,preparation_id,transfer_id,generation,mutation_request_id,
             actor_participation_id,actor_profile_id,transport_request_id,envelope_digest,
             body_sha256,body_byte_count,source_request_envelope,payload,payload_digest,
             recorded_at_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            command.workspace_id.into_uuid().as_bytes(),
            command.preparation_id.as_bytes(),
            command.transfer_id.as_bytes(),
            signed(command.generation)?,
            command.request_id.as_bytes(),
            actor.participation_id.into_uuid().as_bytes(),
            actor.profile_id.as_bytes(),
            command.wire_identity.transport_request_id().as_bytes(),
            command.envelope_digest().as_bytes().as_slice(),
            command.wire_identity.body_sha256().as_slice(),
            i64::from(command.wire_identity.body_byte_count()),
            source_envelope,
            payload,
            payload_digest.as_slice(),
            recorded_at.get(),
        ],
    )?;
    Ok(())
}

#[derive(Debug)]
struct RawDispatch {
    workspace_id: Vec<u8>,
    preparation_id: Vec<u8>,
    transfer_id: Vec<u8>,
    generation: i64,
    request_id: Vec<u8>,
    actor_participation: Vec<u8>,
    actor_profile: Vec<u8>,
    transport_request_id: Vec<u8>,
    envelope_digest: Vec<u8>,
    body_sha256: Vec<u8>,
    body_byte_count: i64,
    source_envelope: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    payload_digest: Vec<u8>,
    recorded_at_ms: i64,
}

fn load_dispatch_by_preparation(
    connection: &Connection,
    workspace_id: WorkspaceId,
    preparation_id: Uuid,
    catalog_destination: Uuid,
) -> Result<Option<DispatchReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),substr(preparation_id,1,17),
                    substr(transfer_id,1,17),generation,substr(mutation_request_id,1,17),
                    substr(actor_participation_id,1,17),substr(actor_profile_id,1,17),
                    substr(transport_request_id,1,17),substr(envelope_digest,1,33),
                    substr(body_sha256,1,33),body_byte_count,
                    CASE WHEN length(source_request_envelope)<=131072 THEN source_request_envelope END,
                    CASE WHEN length(payload)<=1048576 THEN payload END,
                    substr(payload_digest,1,33),recorded_at_ms
             FROM ops_team_permanent_transfer_source_dispatches
             WHERE workspace_id=?1 AND preparation_id=?2",
            params![workspace_id.into_uuid().as_bytes(), preparation_id.as_bytes()],
            |row| {
                Ok(RawDispatch {
                    workspace_id: row.get(0)?,
                    preparation_id: row.get(1)?,
                    transfer_id: row.get(2)?,
                    generation: row.get(3)?,
                    request_id: row.get(4)?,
                    actor_participation: row.get(5)?,
                    actor_profile: row.get(6)?,
                    transport_request_id: row.get(7)?,
                    envelope_digest: row.get(8)?,
                    body_sha256: row.get(9)?,
                    body_byte_count: row.get(10)?,
                    source_envelope: row.get(11)?,
                    payload: row.get(12)?,
                    payload_digest: row.get(13)?,
                    recorded_at_ms: row.get(14)?,
                })
            },
        )
        .optional()?;
    let Some(record) = raw.map(decode_dispatch).transpose()? else {
        return Ok(None);
    };
    validate_dispatch_preparation(connection, &record, catalog_destination)?;
    Ok(Some(record))
}

fn load_dispatch_by_request(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    catalog_destination: Uuid,
) -> Result<Option<DispatchReceipt>, OperationsError> {
    let preparation: Option<Vec<u8>> = connection
        .query_row(
            "SELECT substr(preparation_id,1,17)
             FROM ops_team_permanent_transfer_source_dispatches
             WHERE workspace_id=?1 AND mutation_request_id=?2 LIMIT 2",
            params![workspace_id.into_uuid().as_bytes(), request_id.as_bytes()],
            |row| row.get(0),
        )
        .optional()?;
    preparation
        .map(|bytes| {
            let preparation_id = decode_uuid(&bytes, "source dispatch preparation id")?;
            load_dispatch_by_preparation(
                connection,
                workspace_id,
                preparation_id,
                catalog_destination,
            )?
            .ok_or(OperationsError::Corrupt("source dispatch request index"))
        })
        .transpose()
}

fn decode_dispatch(raw: RawDispatch) -> Result<DispatchReceipt, OperationsError> {
    let source_envelope = raw
        .source_envelope
        .ok_or(OperationsError::Bounds("Source request envelope"))?;
    let payload = raw
        .payload
        .ok_or(OperationsError::Bounds("Source dispatch payload"))?;
    let payload_digest: [u8; 32] = raw
        .payload_digest
        .as_slice()
        .try_into()
        .map_err(|_| OperationsError::Corrupt("Source dispatch payload digest"))?;
    let record = model::record_from_storage(
        model::decode_payload(&payload)?,
        source_envelope,
        payload_digest,
    )?;
    if raw.workspace_id != record.workspace_id().into_uuid().as_bytes()
        || raw.preparation_id != record.preparation_id().as_bytes()
        || raw.transfer_id != record.transfer_id().as_bytes()
        || u64::try_from(raw.generation).ok() != Some(record.generation())
        || raw.request_id != record.request_id().as_bytes()
        || raw.actor_participation != record.actor().participation_id.into_uuid().as_bytes()
        || raw.actor_profile != record.actor().profile_id.as_bytes()
        || raw.transport_request_id != record.wire_identity().transport_request_id().as_bytes()
        || raw.envelope_digest.as_slice() != record.envelope_digest().as_bytes()
        || raw.body_sha256.as_slice() != record.wire_identity().body_sha256()
        || u32::try_from(raw.body_byte_count).ok() != Some(record.wire_identity().body_byte_count())
        || raw.recorded_at_ms != record.recorded_at().get()
    {
        return Err(OperationsError::Corrupt("Source dispatch index mismatch"));
    }
    Ok(record)
}

fn decode_uuid(bytes: &[u8], field: &'static str) -> Result<Uuid, OperationsError> {
    Uuid::from_slice(bytes).map_err(|_| OperationsError::Corrupt(field))
}

fn signed(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("permanent transfer Source counter"))
}

fn validate_dispatch_preparation(
    connection: &Connection,
    dispatch: &DispatchReceipt,
    catalog_destination: Uuid,
) -> Result<(), OperationsError> {
    let preparation = preparation::load_in_transaction(
        connection,
        dispatch.workspace_id(),
        dispatch.preparation_id(),
    )?
    .ok_or(OperationsError::Corrupt(
        "permanent transfer Source preparation missing",
    ))?;
    if dispatch.transfer_id() != preparation.transfer_id()
        || dispatch.generation() != preparation.generation()
        || dispatch.intent_digest() != preparation.intent_digest()
        || dispatch.preparation_digest() != preparation.digest()
        || dispatch.service_destination_id() != catalog_destination
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer Source dispatch preparation identity",
        ));
    }
    let derived = model::derive_team_permanent_transfer_source_request(
        &preparation,
        dispatch.source_scope(),
        dispatch.backend_id(),
        dispatch.service_destination_id(),
    )
    .map_err(|_| OperationsError::Corrupt("permanent transfer Source request derivation"))?;
    if derived != *dispatch.source_request() {
        return Err(OperationsError::Corrupt(
            "permanent transfer Source request differs from preparation",
        ));
    }
    Ok(())
}

fn read_source_dispatch(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    preparation_id: Uuid,
    actor_input: OperationalActor,
) -> Result<Option<DispatchReadback>, OperationsError> {
    if workspace_id.into_uuid().is_nil()
        || preparation_id.is_nil()
        || !super::super::actor_input_is_valid(actor_input)
    {
        return Err(OperationsError::Bounds(
            "permanent transfer Source dispatch read identity",
        ));
    }
    let transaction = catalog.connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, workspace_id)?;
    let actor = ActorSnapshot::resolve(&transaction, workspace_id, actor_input)?;
    let Some(preparation) =
        preparation::load_in_transaction(&transaction, workspace_id, preparation_id)?
    else {
        transaction.commit()?;
        return Ok(None);
    };
    let Some(dispatch) = load_dispatch_by_preparation(
        &transaction,
        workspace_id,
        preparation_id,
        catalog.destination_id(),
    )?
    else {
        transaction.commit()?;
        return Ok(None);
    };
    let is_preparation_author = actor.participation_id() == preparation.actor_participation_id()
        && actor.profile_id() == preparation.actor_profile_id();
    let is_attempt_author = actor.participation_id() == dispatch.actor().participation_id()
        && actor.profile_id() == dispatch.actor().profile_id();
    if !is_preparation_author
        && !is_attempt_author
        && !consent_reader_has_orchestrator_access(authorize_current_orchestrator(
            &transaction,
            workspace_id,
            &actor,
        ))?
    {
        transaction.commit()?;
        return Ok(None);
    }
    let original = load_source_receipt(&transaction, &dispatch)?;
    transaction.commit()?;
    Ok(Some(DispatchReadback::new(dispatch, original)))
}

fn record_source_receipt(
    catalog: &mut OperationsCatalog,
    command: &SourceReceiptCommand,
) -> Result<SourceReceiptOutcome, OperationsError> {
    let catalog_destination = catalog.destination_id();
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
    let dispatch = load_dispatch_by_preparation(
        &transaction,
        command.workspace_id,
        command.preparation_id,
        catalog_destination,
    )?
    .ok_or(OperationsError::Conflict(
        "permanent transfer Source dispatch missing",
    ))?;
    let snapshot = command
        .source_record
        .to_snapshot_bytes()
        .map_err(|_| OperationsError::Corrupt("permanent transfer Source snapshot"))?;
    if command.expected_envelope_digest != dispatch.envelope_digest()
        || command.recorded_at < dispatch.recorded_at()
        || command.source_record.request() != dispatch.source_request()
        || command.source_record.issuer() != dispatch.issuer_id()
        || snapshot.is_empty()
        || snapshot.len() > schema::MAX_SOURCE_SNAPSHOT_BYTES
    {
        return Err(OperationsError::Conflict(
            "permanent transfer Source receipt dispatch pins",
        ));
    }
    let checked = HomeTransferSourceRecord::from_snapshot_bytes(&snapshot)
        .map_err(|_| OperationsError::Corrupt("permanent transfer Source snapshot"))?;
    if checked != command.source_record {
        return Err(OperationsError::Corrupt(
            "permanent transfer Source snapshot round trip",
        ));
    }
    let expected_outcome_digest: [u8; 32] = checked.outcome_digest().to_owned();
    let expected_snapshot_sha256 = model::sha256(&snapshot);
    let expected_payload_digest = model::receipt_payload_digest(
        &snapshot,
        *command.expected_envelope_digest.as_bytes(),
        command.recorded_at,
    );

    if let Some(mut original) = load_source_receipt(&transaction, &dispatch)? {
        if original.envelope_digest() != command.expected_envelope_digest
            || original.source_record() != &checked
            || original.snapshot_bytes() != snapshot.as_slice()
        {
            return Err(OperationsError::Conflict(
                "permanent transfer Source original receipt is immutable",
            ));
        }
        original.replayed = true;
        transaction.commit()?;
        return Ok(SourceReceiptOutcome::Existing(original));
    }

    transaction.execute(
        "INSERT INTO ops_team_permanent_transfer_source_receipts(
             workspace_id,preparation_id,envelope_digest,source_outcome_digest,
             snapshot_payload,snapshot_sha256,local_recorded_at_ms,payload_digest
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            command.workspace_id.into_uuid().as_bytes(),
            command.preparation_id.as_bytes(),
            command.expected_envelope_digest.as_bytes().as_slice(),
            expected_outcome_digest.as_slice(),
            snapshot,
            expected_snapshot_sha256.as_slice(),
            command.recorded_at.get(),
            expected_payload_digest.as_slice(),
        ],
    )?;
    let mut original = load_source_receipt(&transaction, &dispatch)?.ok_or(
        OperationsError::Corrupt("permanent transfer Source receipt insert readback"),
    )?;
    if original.envelope_digest() != command.expected_envelope_digest
        || original.source_record() != &checked
        || original.snapshot_sha256() != expected_snapshot_sha256
        || original.recorded_at() != command.recorded_at
        || original.payload_digest() != expected_payload_digest
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer Source receipt insert identity",
        ));
    }
    transaction
        .commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id: command.workspace_id,
            request_id: dispatch.request_id(),
            source,
        })?;
    original.replayed = false;
    Ok(SourceReceiptOutcome::Recorded(original))
}

#[derive(Debug)]
struct RawSourceReceipt {
    workspace_id: Vec<u8>,
    preparation_id: Vec<u8>,
    envelope_digest: Vec<u8>,
    source_outcome_digest: Vec<u8>,
    snapshot_payload: Option<Vec<u8>>,
    snapshot_sha256: Vec<u8>,
    local_recorded_at_ms: i64,
    payload_digest: Vec<u8>,
}

fn load_source_receipt(
    connection: &Connection,
    dispatch: &DispatchReceipt,
) -> Result<Option<SourceReceiptRecord>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),substr(preparation_id,1,17),
                    substr(envelope_digest,1,33),substr(source_outcome_digest,1,33),
                    CASE WHEN length(snapshot_payload)<=1048576 THEN snapshot_payload END,
                    substr(snapshot_sha256,1,33),local_recorded_at_ms,
                    substr(payload_digest,1,33)
             FROM ops_team_permanent_transfer_source_receipts
             WHERE workspace_id=?1 AND preparation_id=?2",
            params![
                dispatch.workspace_id().into_uuid().as_bytes(),
                dispatch.preparation_id().as_bytes(),
            ],
            |row| {
                Ok(RawSourceReceipt {
                    workspace_id: row.get(0)?,
                    preparation_id: row.get(1)?,
                    envelope_digest: row.get(2)?,
                    source_outcome_digest: row.get(3)?,
                    snapshot_payload: row.get(4)?,
                    snapshot_sha256: row.get(5)?,
                    local_recorded_at_ms: row.get(6)?,
                    payload_digest: row.get(7)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| decode_source_receipt(raw, dispatch))
        .transpose()
}

fn decode_source_receipt(
    raw: RawSourceReceipt,
    dispatch: &DispatchReceipt,
) -> Result<SourceReceiptRecord, OperationsError> {
    let snapshot_bytes = raw.snapshot_payload.ok_or(OperationsError::Bounds(
        "permanent transfer Source snapshot",
    ))?;
    if snapshot_bytes.is_empty() || snapshot_bytes.len() > schema::MAX_SOURCE_SNAPSHOT_BYTES {
        return Err(OperationsError::Bounds(
            "permanent transfer Source snapshot",
        ));
    }
    let envelope_digest: [u8; 32] = raw
        .envelope_digest
        .as_slice()
        .try_into()
        .map_err(|_| OperationsError::Corrupt("Source receipt envelope digest"))?;
    let source_outcome_digest: [u8; 32] = raw
        .source_outcome_digest
        .as_slice()
        .try_into()
        .map_err(|_| OperationsError::Corrupt("Source receipt outcome digest"))?;
    let snapshot_sha256: [u8; 32] = raw
        .snapshot_sha256
        .as_slice()
        .try_into()
        .map_err(|_| OperationsError::Corrupt("Source receipt snapshot digest"))?;
    let payload_digest: [u8; 32] = raw
        .payload_digest
        .as_slice()
        .try_into()
        .map_err(|_| OperationsError::Corrupt("Source receipt payload digest"))?;
    let recorded_at = TimestampMillis::new(raw.local_recorded_at_ms)
        .map_err(|_| OperationsError::Corrupt("Source receipt local time"))?;
    let source_record = HomeTransferSourceRecord::from_snapshot_bytes(&snapshot_bytes)
        .map_err(|_| OperationsError::Corrupt("permanent transfer Source snapshot"))?;
    if raw.workspace_id != dispatch.workspace_id().into_uuid().as_bytes()
        || raw.preparation_id != dispatch.preparation_id().as_bytes()
        || envelope_digest != *dispatch.envelope_digest().as_bytes()
        || source_record.request() != dispatch.source_request()
        || source_record.issuer() != dispatch.issuer_id()
        || source_outcome_digest != *source_record.outcome_digest()
        || model::sha256(&snapshot_bytes) != snapshot_sha256
        || model::receipt_payload_digest(&snapshot_bytes, envelope_digest, recorded_at)
            != payload_digest
        || source_record.to_snapshot_bytes().as_deref().ok() != Some(snapshot_bytes.as_slice())
        || recorded_at < dispatch.recorded_at()
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer Source receipt identity or digest",
        ));
    }
    Ok(SourceReceiptRecord {
        workspace_id: dispatch.workspace_id(),
        preparation_id: dispatch.preparation_id(),
        envelope_digest: dispatch.envelope_digest(),
        record: source_record,
        snapshot_bytes,
        snapshot_sha256,
        recorded_at,
        payload_digest,
        replayed: false,
    })
}
