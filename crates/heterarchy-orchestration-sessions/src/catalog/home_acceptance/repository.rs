//! Bounded SQLite reads and compare-and-set writes for Session acceptance pins.

use forge_domain::{
    AgentSessionState, AuthenticatedPrincipal, OrchestrationIssuerId, TimestampMillis,
};
use forge_persistence::{HomeHandoffAcceptanceOutcomeKind, HomeHandoffAcceptanceOutcomeRecord};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use super::model::{
    HomeAcceptanceOperationsAdmission, HomeAcceptanceOperationsResolution,
    HomeAcceptanceSessionHoldReference, HomeAcceptanceSessionIntent, HomeAcceptanceSessionRecord,
    HomeAcceptanceSessionResolution, HomeAcceptanceSessionState,
};
use super::{
    HomeAcceptanceSessionAcquireOutcome, HomeAcceptanceSessionError as Error,
    HomeAcceptanceSessionPendingPage, schema,
};
use crate::catalog::{
    self, AgentSessionCatalog, AgentSessionCatalogError, decode_principal, decode_session_id,
    encode_principal,
};

#[cfg(test)]
mod tests;

const DIGEST_BYTES: usize = 32;
const REQUEST_SNAPSHOT_MAX_BYTES: usize = 320;
const SESSION_SCHEMA_VERSION_V1: i64 = 1;

#[derive(Debug)]
struct RawPin {
    acceptance_id: Vec<u8>,
    acceptance_id_length: i64,
    issuer_id: Vec<u8>,
    issuer_id_length: i64,
    session_id: Vec<u8>,
    session_id_length: i64,
    held_session_id: Option<Vec<u8>>,
    held_session_id_length: Option<i64>,
    principal_kind: i64,
    principal_id: Vec<u8>,
    principal_id_length: i64,
    session_revision: i64,
    request_digest: Vec<u8>,
    request_digest_length: i64,
    request_snapshot: Vec<u8>,
    request_snapshot_length: i64,
    state: i64,
    created_at_ms: i64,
    held_at_ms: Option<i64>,
    source_outcome_kind: Option<i64>,
    source_payload_digest: Option<Vec<u8>>,
    source_payload_digest_length: Option<i64>,
    source_observed_at_ms: Option<i64>,
    operations_resolution_digest: Option<Vec<u8>>,
    operations_resolution_digest_length: Option<i64>,
    operations_admission_id: Option<Vec<u8>>,
    operations_admission_id_length: Option<i64>,
    operations_admission_digest: Option<Vec<u8>>,
    operations_admission_digest_length: Option<i64>,
    operations_held_at_ms: Option<i64>,
    resolved_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
enum CatalogVersion {
    LegacyV1,
    CurrentV2,
}

pub(super) fn acquire<C>(
    owner: &mut AgentSessionCatalog,
    intent: &HomeAcceptanceSessionIntent,
    authenticated_principal: AuthenticatedPrincipal,
    configured_issuer: OrchestrationIssuerId,
    sample_clock: C,
) -> Result<HomeAcceptanceSessionAcquireOutcome, Error>
where
    C: FnOnce() -> Result<TimestampMillis, Error>,
{
    if intent.issuer() != configured_issuer || intent.principal() != authenticated_principal {
        return Err(Error::Conflict);
    }
    validate_current(&owner.connection)?;
    let transaction = owner
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    require_current(&transaction)?;

    if let Some(existing) = load_record(&transaction, intent.acceptance_id())? {
        ensure_same_intent(&existing.intent(), intent)?;
        let outcome = match existing.state() {
            HomeAcceptanceSessionState::Held { .. } => HomeAcceptanceSessionAcquireOutcome::Held {
                record: existing,
                created: false,
            },
            HomeAcceptanceSessionState::Resolved { .. } => {
                HomeAcceptanceSessionAcquireOutcome::Resolved(existing)
            }
        };
        transaction.commit()?;
        return Ok(outcome);
    }

    let session = catalog::load_session(&transaction, intent.session_id())
        .map_err(super::catalog_error)?
        .ok_or_else(|| {
            session_error(AgentSessionCatalogError::SessionMissing(
                intent.session_id(),
            ))
        })?;
    if session.principal() != authenticated_principal {
        return Err(session_error(AgentSessionCatalogError::ForeignPrincipal(
            intent.session_id(),
        )));
    }
    if session.state() != AgentSessionState::Open {
        return Err(session_error(AgentSessionCatalogError::Transition(
            forge_domain::AgentSessionTransitionError::AlreadyClosed,
        )));
    }
    if session.revision() != intent.expected_session_revision() {
        return Err(session_error(AgentSessionCatalogError::RevisionConflict(
            intent.session_id(),
        )));
    }

    let acquired_at = sample_clock()?;
    if acquired_at < session.updated_at() {
        return Err(Error::Clock);
    }
    let record = HomeAcceptanceSessionRecord::held(intent, acquired_at);
    insert_held(&transaction, &record)?;
    let readback = load_record(&transaction, intent.acceptance_id())?.ok_or(Error::Corrupt(
        "new Session hold disappeared inside transaction",
    ))?;
    if readback != record {
        return Err(Error::Corrupt(
            "new Session hold differs from same-transaction readback",
        ));
    }
    transaction
        .commit()
        .map_err(|source| Error::CommitUnconfirmed {
            acceptance_id: intent.acceptance_id(),
            source: Box::new(source),
        })?;
    Ok(HomeAcceptanceSessionAcquireOutcome::Held {
        record: readback,
        created: true,
    })
}

pub(super) fn resolve<C>(
    owner: &mut AgentSessionCatalog,
    intent: &HomeAcceptanceSessionIntent,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    operations: HomeAcceptanceOperationsResolution,
    sample_clock: C,
) -> Result<HomeAcceptanceSessionRecord, Error>
where
    C: FnOnce() -> Result<TimestampMillis, Error>,
{
    validate_current(&owner.connection)?;
    let transaction = owner
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    require_current(&transaction)?;
    if let Some(record) = load_record(&transaction, intent.acceptance_id())? {
        ensure_same_intent(&record.intent(), intent)?;
        if let HomeAcceptanceSessionState::Resolved {
            resolution: stored, ..
        } = record.state()
        {
            let expected = HomeAcceptanceSessionResolution::from_receipts(
                intent,
                source,
                operations,
                record_held_at(&record),
                stored.resolved_at(),
            )?;
            if stored != expected {
                return Err(Error::Conflict);
            }
            transaction.commit()?;
            return Ok(record);
        }

        let held_at = record_held_at(&record);
        let resolved_at = sample_clock()?;
        let resolution = HomeAcceptanceSessionResolution::from_receipts(
            intent,
            source,
            operations,
            held_at,
            resolved_at,
        )?;
        let terminal = HomeAcceptanceSessionRecord::resolved(intent, &resolution, held_at);
        update_resolution(&transaction, &terminal, held_at)?;
        let readback = load_record(&transaction, intent.acceptance_id())?
            .ok_or(Error::Corrupt("resolved Session hold disappeared"))?;
        if readback != terminal {
            return Err(Error::Corrupt(
                "resolved Session hold differs from readback",
            ));
        }
        transaction
            .commit()
            .map_err(|source| Error::CommitUnconfirmed {
                acceptance_id: intent.acceptance_id(),
                source: Box::new(source),
            })?;
        return Ok(readback);
    }

    // An absent row is a genuine abort only after Source and Operations have
    // written their absorbing terminal records for this exact intent.
    if source.outcome_kind() != HomeHandoffAcceptanceOutcomeKind::Aborted
        || source.acceptance_id() != intent.acceptance_id()
        || source.request() != intent.request()
        || source.issuer() != intent.issuer()
        || operations.outcome() != HomeHandoffAcceptanceOutcomeKind::Aborted
        || operations.acceptance_id() != intent.acceptance_id()
        || operations.admission().is_some()
    {
        return Err(Error::Conflict);
    }
    let resolved_at = sample_clock()?;
    let resolution = HomeAcceptanceSessionResolution::from_receipts(
        intent,
        source,
        operations,
        None,
        resolved_at,
    )?;
    let terminal = HomeAcceptanceSessionRecord::resolved(intent, &resolution, None);
    insert_resolved_absent(&transaction, &terminal)?;
    let readback = load_record(&transaction, intent.acceptance_id())?
        .ok_or(Error::Corrupt("Aborted absent Session fence disappeared"))?;
    if readback != terminal {
        return Err(Error::Corrupt(
            "Aborted absent Session differs from readback",
        ));
    }
    transaction
        .commit()
        .map_err(|source| Error::CommitUnconfirmed {
            acceptance_id: intent.acceptance_id(),
            source: Box::new(source),
        })?;
    Ok(readback)
}

pub(super) fn get(
    owner: &AgentSessionCatalog,
    acceptance_id: Uuid,
    configured_issuer: OrchestrationIssuerId,
    principal: Option<AuthenticatedPrincipal>,
) -> Result<Option<HomeAcceptanceSessionRecord>, Error> {
    if acceptance_id.is_nil() || configured_issuer.into_uuid().is_nil() {
        return Err(Error::InvalidIntent("acceptance selector"));
    }
    let transaction = owner.connection.unchecked_transaction()?;
    let version = validate_owner(&transaction)?;
    let record = match version {
        CatalogVersion::LegacyV1 => None,
        CatalogVersion::CurrentV2 => load_record(&transaction, acceptance_id)?,
    };
    if let Some(record) = record {
        if record.intent().issuer() != configured_issuer {
            return Err(Error::Conflict);
        }
        if principal.is_some_and(|value| value != record.intent().principal()) {
            return Err(session_error(AgentSessionCatalogError::ForeignPrincipal(
                record.intent().session_id(),
            )));
        }
        transaction.commit()?;
        return Ok(Some(record));
    }
    transaction.commit()?;
    Ok(None)
}

pub(super) fn pending(
    owner: &AgentSessionCatalog,
    configured_issuer: OrchestrationIssuerId,
    after: Option<Uuid>,
    limit: u16,
) -> Result<HomeAcceptanceSessionPendingPage, Error> {
    if configured_issuer.into_uuid().is_nil() {
        return Err(Error::InvalidIntent("configured issuer"));
    }
    if !(1..=super::MAX_PENDING_PAGE_SIZE).contains(&limit) {
        return Err(Error::PageSize);
    }
    if after.is_some_and(|value| value.is_nil()) {
        return Err(Error::InvalidIntent("pending page cursor"));
    }
    let transaction = owner.connection.unchecked_transaction()?;
    if matches!(validate_owner(&transaction)?, CatalogVersion::LegacyV1) {
        transaction.commit()?;
        return Ok(HomeAcceptanceSessionPendingPage {
            records: Vec::new(),
            next_after: None,
        });
    }
    let mut statement = transaction.prepare(
        "SELECT acceptance_id FROM home_acceptance_session_pins
         WHERE issuer_id=?1 AND state=0 AND (?2 IS NULL OR acceptance_id>?2)
         ORDER BY acceptance_id LIMIT ?3",
    )?;
    let ids = statement
        .query_map(
            params![
                configured_issuer.into_uuid().as_bytes(),
                after.map(Uuid::into_bytes),
                i64::from(limit) + 1,
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    let has_more = ids.len() > usize::from(limit);
    let mut records = Vec::with_capacity(usize::from(limit).min(ids.len()));
    for raw_id in ids.iter().take(usize::from(limit)) {
        let id = decode_uuid(raw_id, "pending acceptance ID")?;
        let record = load_record(&transaction, id)?
            .ok_or(Error::Corrupt("pending acceptance disappeared"))?;
        if record.intent().issuer() != configured_issuer
            || !matches!(record.state(), HomeAcceptanceSessionState::Held { .. })
        {
            return Err(Error::Corrupt("pending Session hold selection"));
        }
        records.push(record);
    }
    let next_after = if has_more {
        records.last().map(|record| record.intent().acceptance_id())
    } else {
        None
    };
    transaction.commit()?;
    Ok(HomeAcceptanceSessionPendingPage {
        records,
        next_after,
    })
}

pub(super) fn ensure_unpinned(
    transaction: &Transaction<'_>,
    session_id: forge_domain::AgentSessionId,
) -> Result<(), AgentSessionCatalogError> {
    schema::validate_schema(transaction)
        .map_err(|error| AgentSessionCatalogError::HomeAcceptance(Box::new(error)))?;
    let active_id: Option<Vec<u8>> = transaction
        .query_row(
            "SELECT substr(acceptance_id,1,17)
             FROM home_acceptance_session_pins
             WHERE session_id=?1 AND state=0 ORDER BY acceptance_id LIMIT 1",
            params![session_id.into_uuid().as_bytes()],
            |row| row.get(0),
        )
        .optional()?;
    if active_id.is_some() {
        return Err(AgentSessionCatalogError::HomeAcceptancePending(session_id));
    }
    Ok(())
}

fn validate_current(connection: &Connection) -> Result<(), Error> {
    match validate_owner(connection)? {
        CatalogVersion::CurrentV2 => Ok(()),
        CatalogVersion::LegacyV1 => Err(Error::Catalog(Box::new(
            AgentSessionCatalogError::Ownership {
                application_id: super::super::AGENT_SESSION_APPLICATION_ID,
                user_version: SESSION_SCHEMA_VERSION_V1,
                object_count: 2,
            },
        ))),
    }
}

fn require_current(connection: &Connection) -> Result<(), Error> {
    validate_current(connection)
}

fn validate_owner(connection: &Connection) -> Result<CatalogVersion, Error> {
    let (application_id, user_version, object_count) =
        catalog::catalog_ownership(connection).map_err(catalog_error)?;
    if application_id != super::super::AGENT_SESSION_APPLICATION_ID {
        return Err(Error::Catalog(Box::new(
            AgentSessionCatalogError::Ownership {
                application_id,
                user_version,
                object_count,
            },
        )));
    }
    match user_version {
        SESSION_SCHEMA_VERSION_V1 => {
            catalog::validate_legacy_schema(connection).map_err(catalog_error)?;
            Ok(CatalogVersion::LegacyV1)
        }
        super::super::AGENT_SESSION_SCHEMA_VERSION => {
            catalog::validate_schema(connection).map_err(catalog_error)?;
            Ok(CatalogVersion::CurrentV2)
        }
        _ => Err(Error::Catalog(Box::new(
            AgentSessionCatalogError::Ownership {
                application_id,
                user_version,
                object_count,
            },
        ))),
    }
}

fn load_record(
    connection: &Connection,
    acceptance_id: Uuid,
) -> Result<Option<HomeAcceptanceSessionRecord>, Error> {
    if acceptance_id.is_nil() {
        return Err(Error::InvalidIntent("acceptance identity"));
    }
    load_pin_row(connection, acceptance_id)?
        .map(|raw| decode_record(connection, &raw))
        .transpose()
}

fn decode_record(
    connection: &Connection,
    raw: &RawPin,
) -> Result<HomeAcceptanceSessionRecord, Error> {
    let acceptance_id = decode_uuid(&raw.acceptance_id, "acceptance_id")?;
    if raw.acceptance_id_length != 16
        || acceptance_id.is_nil()
        || raw.issuer_id_length != 16
        || raw.session_id_length != 16
        || raw.principal_id_length != 16
        || raw.request_digest_length != i64::try_from(raw.request_digest.len()).unwrap_or(i64::MAX)
        || raw.request_snapshot_length
            != i64::try_from(raw.request_snapshot.len()).unwrap_or(i64::MAX)
        || raw.request_snapshot.len() > REQUEST_SNAPSHOT_MAX_BYTES
    {
        return Err(Error::Corrupt("acceptance identity/snapshot lengths"));
    }
    match (&raw.held_session_id, raw.held_session_id_length) {
        (None, None) => {}
        (Some(bytes), Some(16)) if bytes.len() == 16 => {}
        _ => return Err(Error::Corrupt("held Session identity length")),
    }
    let issuer = OrchestrationIssuerId::from_uuid(decode_uuid(&raw.issuer_id, "issuer_id")?);
    let session_id = decode_session_id(&raw.session_id, "session_id")?;
    let principal = decode_principal(raw.principal_kind, Some(raw.principal_id.clone()))
        .map_err(catalog_error)?;
    let request =
        forge_domain::HomeHandoffTakeRequest::from_canonical_snapshot(&raw.request_snapshot)?;
    let request_digest = fixed_digest(
        &raw.request_digest,
        raw.request_digest_length,
        "request_digest",
    )?;
    if request.acceptance_id() != acceptance_id
        || request.recipient().session_id() != session_id
        || request.recipient().principal() != principal
        || request_digest != *request.request_digest().as_bytes()
    {
        return Err(Error::Corrupt("acceptance request selectors/digest"));
    }
    let session_revision = forge_domain::AgentSessionRevision::new(raw.session_revision)
        .map_err(|_| Error::Corrupt("Session revision"))?;
    let intent = HomeAcceptanceSessionIntent::new(request, issuer, session_revision)
        .map_err(|_| Error::Corrupt("Session intent"))?;
    let created_at = timestamp(raw.created_at_ms, "created_at_ms")?;

    match raw.state {
        0 => decode_held_record(connection, &intent, created_at, raw),
        1 | 2 => decode_resolved_record(&intent, created_at, raw),
        _ => Err(Error::Corrupt("Session pin state")),
    }
}

fn decode_held_record(
    connection: &Connection,
    intent: &HomeAcceptanceSessionIntent,
    created_at: TimestampMillis,
    raw: &RawPin,
) -> Result<HomeAcceptanceSessionRecord, Error> {
    if raw.state != 0
        || raw.held_session_id.as_deref() != Some(intent.session_id().into_uuid().as_bytes())
        || raw.held_at_ms != Some(created_at.get())
        || raw.source_outcome_kind.is_some()
        || raw.source_payload_digest.is_some()
        || raw.source_observed_at_ms.is_some()
        || raw.operations_resolution_digest.is_some()
        || raw.operations_admission_id.is_some()
        || raw.operations_admission_digest.is_some()
        || raw.operations_held_at_ms.is_some()
        || raw.resolved_at_ms.is_some()
    {
        return Err(Error::Corrupt("Held Session pin fields"));
    }
    let session = catalog::load_session(connection, intent.session_id())?
        .ok_or(Error::Corrupt("Held Session row is missing"))?;
    if session.state() != AgentSessionState::Open
        || session.revision() != intent.expected_session_revision()
        || session.principal() != intent.principal()
        || raw
            .held_at_ms
            .is_some_and(|held_at| held_at < session.created_at().get())
    {
        return Err(Error::Corrupt(
            "Held Session no longer matches its immutable pin",
        ));
    }
    Ok(HomeAcceptanceSessionRecord::held(intent, created_at))
}

fn decode_resolved_record(
    intent: &HomeAcceptanceSessionIntent,
    created_at: TimestampMillis,
    raw: &RawPin,
) -> Result<HomeAcceptanceSessionRecord, Error> {
    let source_kind = match raw.source_outcome_kind {
        Some(1) => HomeHandoffAcceptanceOutcomeKind::Applied,
        Some(2) => HomeHandoffAcceptanceOutcomeKind::Aborted,
        _ => return Err(Error::Corrupt("resolved Source outcome")),
    };
    let source_payload_digest = optional_fixed_digest(
        raw.source_payload_digest.as_deref(),
        raw.source_payload_digest_length,
        "Source payload digest",
    )?
    .ok_or(Error::Corrupt("resolved Source payload digest is missing"))?;
    let source_at = timestamp(
        raw.source_observed_at_ms
            .ok_or(Error::Corrupt("resolved Source time is missing"))?,
        "source_observed_at_ms",
    )?;
    let operations_digest = optional_fixed_digest(
        raw.operations_resolution_digest.as_deref(),
        raw.operations_resolution_digest_length,
        "Operations resolution digest",
    )?
    .ok_or(Error::Corrupt("Operations resolution digest is missing"))?;
    let held_at = raw
        .held_at_ms
        .map(|value| timestamp(value, "held_at_ms"))
        .transpose()?;
    let held_session = raw
        .held_session_id
        .as_deref()
        .map(|value| decode_session_id(value, "held_session_id"))
        .transpose()?;
    if held_session.is_some_and(|session_id| session_id != intent.session_id())
        || held_session.is_some() != held_at.is_some()
        || held_at.is_some_and(|value| value != created_at)
    {
        return Err(Error::Corrupt("resolved local acquisition identity"));
    }
    let admission = decode_admission(raw, intent)?;
    if source_kind == HomeHandoffAcceptanceOutcomeKind::Applied
        && (held_at.is_none() || admission.is_none())
    {
        return Err(Error::Corrupt("Applied resolution has no admitted Session"));
    }
    if raw.state == 1 && source_kind != HomeHandoffAcceptanceOutcomeKind::Applied
        || raw.state == 2 && source_kind != HomeHandoffAcceptanceOutcomeKind::Aborted
    {
        return Err(Error::Corrupt("Session and Source terminal states differ"));
    }
    let resolved_at = timestamp(
        raw.resolved_at_ms
            .ok_or(Error::Corrupt("local resolution time is missing"))?,
        "resolved_at_ms",
    )?;
    if resolved_at < created_at {
        return Err(Error::Corrupt("local Session resolution predates creation"));
    }
    let resolution = HomeAcceptanceSessionResolution::from_stored_parts(
        intent,
        &super::model::StoredSessionResolutionParts {
            outcome: source_kind,
            source_digest: source_payload_digest,
            source_at,
            operations_digest,
            admission,
            held_at,
            resolved_at,
        },
    )?;
    Ok(HomeAcceptanceSessionRecord::resolved(
        intent,
        &resolution,
        held_at,
    ))
}

fn insert_held(
    transaction: &Transaction<'_>,
    record: &HomeAcceptanceSessionRecord,
) -> Result<(), Error> {
    let intent = record.intent();
    let HomeAcceptanceSessionState::Held { acquired_at } = record.state() else {
        return Err(Error::Corrupt("new Session row is not Held"));
    };
    let (principal_kind, principal_id) = encode_principal(intent.principal());
    let acceptance_id = intent.acceptance_id().into_bytes();
    let issuer_id = intent.issuer().into_uuid().into_bytes();
    let session_id = intent.session_id().into_uuid().into_bytes();
    let request_digest = intent.request_digest();
    let snapshot = intent.canonical_request_snapshot();
    let changed = transaction.execute(
        "INSERT INTO home_acceptance_session_pins(
             acceptance_id,issuer_id,session_id,held_session_id,
             principal_kind,principal_id,session_revision,request_digest,request_snapshot,
             state,created_at_ms,held_at_ms,source_outcome_kind,source_payload_digest,
             source_observed_at_ms,operations_resolution_digest,operations_admission_id,
             operations_admission_digest,operations_held_at_ms,resolved_at_ms
         ) VALUES(?1,?2,?3,?3,?4,?5,?6,?7,?8,0,?9,?9,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
        params![
            acceptance_id.as_slice(),
            issuer_id.as_slice(),
            session_id.as_slice(),
            principal_kind,
            principal_id.as_slice(),
            intent.expected_session_revision().get(),
            request_digest.as_slice(),
            snapshot.as_slice(),
            acquired_at.get(),
        ],
    )?;
    if changed != 1 {
        return Err(Error::Corrupt("Session hold insert count"));
    }
    Ok(())
}

fn insert_resolved_absent(
    transaction: &Transaction<'_>,
    record: &HomeAcceptanceSessionRecord,
) -> Result<(), Error> {
    let intent = record.intent();
    let HomeAcceptanceSessionState::Resolved { resolution, .. } = record.state() else {
        return Err(Error::Corrupt("absent Session resolution is not terminal"));
    };
    if resolution.outcome() != HomeHandoffAcceptanceOutcomeKind::Aborted {
        return Err(Error::Conflict);
    }
    let (principal_kind, principal_id) = encode_principal(intent.principal());
    let (state, source_digest, source_at, operations) = resolution_values(&resolution);
    if state != 2 {
        return Err(Error::Corrupt("absent Session row is not Aborted"));
    }
    let acceptance_id = intent.acceptance_id().into_bytes();
    let issuer_id = intent.issuer().into_uuid().into_bytes();
    let session_id = intent.session_id().into_uuid().into_bytes();
    let request_digest = intent.request_digest();
    let snapshot = intent.canonical_request_snapshot();
    let operations_digest = operations.resolution_digest();
    let changed = transaction.execute(
        "INSERT INTO home_acceptance_session_pins(
             acceptance_id,issuer_id,session_id,held_session_id,
             principal_kind,principal_id,session_revision,request_digest,request_snapshot,
             state,created_at_ms,held_at_ms,source_outcome_kind,source_payload_digest,
             source_observed_at_ms,operations_resolution_digest,operations_admission_id,
             operations_admission_digest,operations_held_at_ms,resolved_at_ms
         ) VALUES(?1,?2,?3,NULL,?4,?5,?6,?7,?8,2,?9,NULL,2,?10,?11,?12,?13,?14,?15,?16)",
        params![
            acceptance_id.as_slice(),
            issuer_id.as_slice(),
            session_id.as_slice(),
            principal_kind,
            principal_id.as_slice(),
            intent.expected_session_revision().get(),
            request_digest.as_slice(),
            snapshot.as_slice(),
            resolution.resolved_at().get(),
            source_digest.as_slice(),
            source_at.get(),
            operations_digest.as_slice(),
            Option::<&[u8]>::None,
            Option::<&[u8]>::None,
            Option::<i64>::None,
            resolution.resolved_at().get(),
        ],
    )?;
    if changed != 1 {
        return Err(Error::Corrupt("absent Session resolution insert count"));
    }
    Ok(())
}

fn update_resolution(
    transaction: &Transaction<'_>,
    record: &HomeAcceptanceSessionRecord,
    held_at: Option<TimestampMillis>,
) -> Result<(), Error> {
    let intent = record.intent();
    let HomeAcceptanceSessionState::Resolved { resolution, .. } = record.state() else {
        return Err(Error::Corrupt("Session resolution is not terminal"));
    };
    let held_at = held_at.ok_or(Error::Corrupt("updating an absent acquisition"))?;
    let (state, source_digest, source_at, operations) = resolution_values(&resolution);
    let source_outcome = match state {
        1 => 1_i64,
        2 => 2_i64,
        _ => return Err(Error::Corrupt("Session terminal encoding")),
    };
    let ops_digest = operations.resolution_digest();
    let admission = operations.admission();
    let admission_id = admission.map(|value| value.receipt_id().into_bytes());
    let admission_digest = admission.map(HomeAcceptanceOperationsAdmission::receipt_digest);
    let operations_held_at = admission.map(|value| value.session_hold().acquired_at().get());
    if admission.is_some_and(|value| value.session_hold().acquired_at() != held_at) {
        return Err(Error::Conflict);
    }
    let changed = transaction.execute(
        "UPDATE home_acceptance_session_pins SET
             state=?1, source_outcome_kind=?2, source_payload_digest=?3,
             source_observed_at_ms=?4, operations_resolution_digest=?5,
             operations_admission_id=?6, operations_admission_digest=?7,
             operations_held_at_ms=?8, resolved_at_ms=?9
         WHERE acceptance_id=?10 AND state=0 AND request_digest=?11",
        params![
            state,
            source_outcome,
            source_digest.as_slice(),
            source_at.get(),
            ops_digest.as_slice(),
            admission_id.as_ref().map(<[u8; 16]>::as_slice),
            admission_digest
                .as_ref()
                .map(<[u8; DIGEST_BYTES]>::as_slice),
            operations_held_at,
            resolution.resolved_at().get(),
            intent.acceptance_id().as_bytes(),
            intent.request_digest().as_slice(),
        ],
    )?;
    if changed != 1 {
        return Err(Error::Conflict);
    }
    Ok(())
}

fn resolution_values(
    resolution: &HomeAcceptanceSessionResolution,
) -> (
    i64,
    [u8; DIGEST_BYTES],
    TimestampMillis,
    HomeAcceptanceOperationsResolution,
) {
    let state = match resolution.outcome() {
        HomeHandoffAcceptanceOutcomeKind::Applied => 1,
        HomeHandoffAcceptanceOutcomeKind::Aborted => 2,
    };
    (
        state,
        resolution.source_digest(),
        resolution.source_at(),
        resolution.operations(),
    )
}

fn record_held_at(record: &HomeAcceptanceSessionRecord) -> Option<TimestampMillis> {
    match record.state() {
        HomeAcceptanceSessionState::Held { acquired_at } => Some(acquired_at),
        HomeAcceptanceSessionState::Resolved { held_at, .. } => held_at,
    }
}

fn ensure_same_intent(
    actual: &HomeAcceptanceSessionIntent,
    expected: &HomeAcceptanceSessionIntent,
) -> Result<(), Error> {
    if actual == expected {
        Ok(())
    } else {
        Err(Error::Conflict)
    }
}

fn decode_admission(
    raw: &RawPin,
    intent: &HomeAcceptanceSessionIntent,
) -> Result<Option<HomeAcceptanceOperationsAdmission>, Error> {
    match (
        raw.operations_admission_id.as_deref(),
        raw.operations_admission_id_length,
        raw.operations_admission_digest.as_deref(),
        raw.operations_admission_digest_length,
        raw.operations_held_at_ms,
    ) {
        (None, None, None, None, None) => Ok(None),
        (Some(id), Some(16), Some(digest), Some(32), Some(held_at))
            if id.len() == 16 && digest.len() == DIGEST_BYTES =>
        {
            let held_at = timestamp(held_at, "operations_held_at_ms")?;
            let hold = HomeAcceptanceSessionHoldReference::new(intent, held_at);
            let receipt_id = decode_uuid(id, "Operations admission ID")?;
            let receipt_digest = fixed_digest(digest, 32, "Operations admission digest")?;
            HomeAcceptanceOperationsAdmission::new(
                intent.acceptance_id(),
                receipt_id,
                receipt_digest,
                hold,
            )
            .map(Some)
        }
        _ => Err(Error::Corrupt("Operations admission reference fields")),
    }
}

fn fixed_digest(
    bytes: &[u8],
    actual_length: i64,
    field: &'static str,
) -> Result<[u8; DIGEST_BYTES], Error> {
    if bytes.len() != DIGEST_BYTES || actual_length != 32 {
        return Err(Error::Corrupt(field));
    }
    bytes.try_into().map_err(|_| Error::Corrupt(field))
}

fn optional_fixed_digest(
    bytes: Option<&[u8]>,
    actual_length: Option<i64>,
    field: &'static str,
) -> Result<Option<[u8; DIGEST_BYTES]>, Error> {
    match (bytes, actual_length) {
        (None, None) => Ok(None),
        (Some(bytes), Some(actual_length)) => fixed_digest(bytes, actual_length, field).map(Some),
        _ => Err(Error::Corrupt(field)),
    }
}

fn timestamp(value: i64, field: &'static str) -> Result<TimestampMillis, Error> {
    TimestampMillis::new(value).map_err(|_| Error::Corrupt(field))
}

fn decode_uuid(bytes: &[u8], field: &'static str) -> Result<Uuid, Error> {
    if bytes.len() != 16 {
        return Err(Error::Corrupt(field));
    }
    Uuid::from_slice(bytes).map_err(|_| Error::Corrupt(field))
}

fn load_pin_row(connection: &Connection, acceptance_id: Uuid) -> Result<Option<RawPin>, Error> {
    let raw = connection
        .query_row(
            "SELECT substr(acceptance_id,1,17),length(acceptance_id),
                    substr(issuer_id,1,17),length(issuer_id),
                    substr(session_id,1,17),length(session_id),
                    substr(held_session_id,1,17),length(held_session_id),
                    principal_kind,substr(principal_id,1,17),length(principal_id),
                    session_revision,substr(request_digest,1,33),length(request_digest),
                    substr(request_snapshot,1,321),length(request_snapshot),state,
                    created_at_ms,held_at_ms,source_outcome_kind,
                    substr(source_payload_digest,1,33),length(source_payload_digest),
                    source_observed_at_ms,
                    substr(operations_resolution_digest,1,33),length(operations_resolution_digest),
                    substr(operations_admission_id,1,17),length(operations_admission_id),
                    substr(operations_admission_digest,1,33),length(operations_admission_digest),
                    operations_held_at_ms,resolved_at_ms
             FROM home_acceptance_session_pins WHERE acceptance_id=?1",
            params![acceptance_id.as_bytes()],
            |row| {
                Ok(RawPin {
                    acceptance_id: row.get(0)?,
                    acceptance_id_length: row.get(1)?,
                    issuer_id: row.get(2)?,
                    issuer_id_length: row.get(3)?,
                    session_id: row.get(4)?,
                    session_id_length: row.get(5)?,
                    held_session_id: row.get(6)?,
                    held_session_id_length: row.get(7)?,
                    principal_kind: row.get(8)?,
                    principal_id: row.get(9)?,
                    principal_id_length: row.get(10)?,
                    session_revision: row.get(11)?,
                    request_digest: row.get(12)?,
                    request_digest_length: row.get(13)?,
                    request_snapshot: row.get(14)?,
                    request_snapshot_length: row.get(15)?,
                    state: row.get(16)?,
                    created_at_ms: row.get(17)?,
                    held_at_ms: row.get(18)?,
                    source_outcome_kind: row.get(19)?,
                    source_payload_digest: row.get(20)?,
                    source_payload_digest_length: row.get(21)?,
                    source_observed_at_ms: row.get(22)?,
                    operations_resolution_digest: row.get(23)?,
                    operations_resolution_digest_length: row.get(24)?,
                    operations_admission_id: row.get(25)?,
                    operations_admission_id_length: row.get(26)?,
                    operations_admission_digest: row.get(27)?,
                    operations_admission_digest_length: row.get(28)?,
                    operations_held_at_ms: row.get(29)?,
                    resolved_at_ms: row.get(30)?,
                })
            },
        )
        .optional()?;
    Ok(raw)
}

fn catalog_error(error: AgentSessionCatalogError) -> Error {
    Error::Catalog(Box::new(error))
}

fn session_error(error: AgentSessionCatalogError) -> Error {
    Error::Session(Box::new(error))
}
