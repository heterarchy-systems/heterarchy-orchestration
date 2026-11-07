//! Transaction-scoped Profile pin acquisition, replay, and settlement.

use forge_domain::{HomeHandoffTakeRequest, TimestampMillis};
use forge_persistence::{HomeHandoffAcceptanceOutcomeKind, HomeHandoffAcceptanceOutcomeRecord};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use uuid::Uuid;

use super::HomeAcceptanceProfileError as Error;
use super::model::{
    HomeAcceptanceProfileAcquireOutcome, HomeAcceptanceProfileIntent,
    HomeAcceptanceProfileOperationsAdmission, HomeAcceptanceProfileOperationsResolution,
    HomeAcceptanceProfileOperationsResolutionParts, HomeAcceptanceProfilePendingPage,
    HomeAcceptanceProfilePin, HomeAcceptanceProfilePinRecord, HomeAcceptanceProfilePinRecordParts,
    HomeAcceptanceProfilePinReference, HomeAcceptanceProfileState,
};
use super::schema;
use crate::HomeAcceptanceSessionHoldReference;
use crate::profile_catalog::{
    AgentProfileCatalog, AgentProfileId, AgentProfileLifecycleState, AgentProfileRevision,
    load_profile, logical_session_link_by_request,
};

const DIGEST_BYTES: usize = 32;
const MAX_REQUEST_BYTES: usize = 320;
const MAX_PAGE_SIZE: u16 = 64;

#[derive(Debug)]
struct RawPin {
    acceptance_id: Vec<u8>,
    acceptance_id_len: i64,
    issuer_id: Vec<u8>,
    issuer_id_len: i64,
    request_digest: Vec<u8>,
    request_digest_len: i64,
    request_snapshot: Vec<u8>,
    request_snapshot_len: i64,
    state: i64,
    profile_id: Option<Vec<u8>>,
    profile_id_len: Option<i64>,
    profile_revision: Option<i64>,
    link_request_id: Option<Vec<u8>>,
    link_request_id_len: Option<i64>,
    link_profile_revision: Option<i64>,
    link_session_revision: Option<i64>,
    acquired_at: Option<i64>,
    source_outcome: Option<i64>,
    source_payload_digest: Option<Vec<u8>>,
    source_payload_digest_len: Option<i64>,
    source_at: Option<i64>,
    operations_resolution_id: Option<Vec<u8>>,
    operations_resolution_id_len: Option<i64>,
    operations_resolution_digest: Option<Vec<u8>>,
    operations_resolution_digest_len: Option<i64>,
    operations_admission_id: Option<Vec<u8>>,
    operations_admission_id_len: Option<i64>,
    operations_admission_digest: Option<Vec<u8>>,
    operations_admission_digest_len: Option<i64>,
    operations_profile_pin_present: Option<i64>,
    created_at: i64,
    resolved_at: Option<i64>,
}

#[derive(Clone, Copy)]
struct DecodedPinContext {
    acceptance_id: Uuid,
    issuer: forge_domain::OrchestrationIssuerId,
    request_digest: forge_domain::HomeHandoffTakeRequestDigest,
    profile_pin: Option<HomeAcceptanceProfilePinReference>,
    source_payload_digest: Option<[u8; DIGEST_BYTES]>,
    source_outcome: Option<HomeHandoffAcceptanceOutcomeKind>,
}

#[derive(Clone, Copy)]
struct DecodedOperationsPin {
    resolution_id: Option<Uuid>,
    resolution_digest: Option<[u8; DIGEST_BYTES]>,
    admission_id: Option<Uuid>,
    admission_digest: Option<[u8; DIGEST_BYTES]>,
    profile_pin_present: Option<bool>,
}

struct EncodedProfilePin {
    profile_id: Option<Vec<u8>>,
    profile_revision: Option<i64>,
    logical_session_request_id: Option<Vec<u8>>,
    linked_profile_revision: Option<i64>,
    linked_session_revision: Option<i64>,
    acquired_at: Option<i64>,
}

pub(super) fn acquire<C>(
    catalog: &mut AgentProfileCatalog,
    intent: &HomeAcceptanceProfileIntent,
    session_hold: HomeAcceptanceSessionHoldReference,
    sample_clock: C,
) -> Result<HomeAcceptanceProfileAcquireOutcome, Error>
where
    C: FnOnce() -> Result<TimestampMillis, Error>,
{
    if session_hold.acceptance_id() != intent.request().acceptance_id()
        || session_hold.request_digest() != *intent.request().request_digest().as_bytes()
        || session_hold.session_id() != intent.request().recipient().session_id()
        || session_hold.principal() != intent.request().recipient().principal()
    {
        return Err(Error::Conflict);
    }
    schema::validate_schema(&catalog.connection)?;
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(existing) = load_pin(&transaction, intent.request().acceptance_id())? {
        if !same_core_intent(&existing, intent) {
            return Err(Error::Conflict);
        }
        match existing.state() {
            HomeAcceptanceProfileState::Held => {
                let (profile_id, profile_revision) =
                    intent.observed_profile().ok_or(Error::Conflict)?;
                let Some(pin) = existing.profile_pin() else {
                    return Err(Error::Conflict);
                };
                if pin.profile_id() != profile_id
                    || pin.profile_revision() != profile_revision
                    || pin.linked_session_revision() > session_hold.session_revision()
                {
                    return Err(Error::Conflict);
                }
                validate_record_references(&transaction, &existing)?;
                transaction.commit()?;
                return Ok(HomeAcceptanceProfileAcquireOutcome::Held {
                    record: existing,
                    created: false,
                });
            }
            HomeAcceptanceProfileState::ResolvedApplied
            | HomeAcceptanceProfileState::ResolvedAborted => {
                transaction.commit()?;
                return Ok(HomeAcceptanceProfileAcquireOutcome::Resolved(existing));
            }
        }
    }

    let (profile_id, expected_profile_revision) = intent
        .observed_profile()
        .ok_or(Error::Invalid("Profile acquisition observation"))?;
    let profile = load_profile(&transaction, profile_id)?.ok_or(Error::Conflict)?;
    super::ensure_pinned_profile_revision(&profile, expected_profile_revision)?;
    let link = super::validate_profile_link(
        &transaction,
        &intent.request(),
        &profile,
        session_hold.session_revision(),
    )?;
    let source_now = sample_clock()?;
    let expected_at_least = profile.updated_at().max(link.linked_at());
    if source_now < expected_at_least {
        return Err(Error::Clock {
            profile_id: profile.id(),
            expected_at_least,
            observed: source_now,
        });
    }
    let pin = HomeAcceptanceProfilePin {
        profile_id: profile.id(),
        profile_revision: profile.revision(),
        logical_session_request_id: link.request_id(),
        linked_profile_revision: link.profile_revision(),
        linked_session_revision: link.session_revision(),
        acquired_at: source_now,
    };
    let record = held_record(intent, pin)?;
    insert_pin(&transaction, &record)?;
    transaction
        .commit()
        .map_err(|source| Error::CommitUnconfirmed {
            acceptance_id: intent.acceptance_id(),
            source: Box::new(source),
        })?;
    let record = readback_after_commit(&catalog.connection, intent)?;
    Ok(classify_acquire_readback(record))
}

fn classify_acquire_readback(
    record: HomeAcceptanceProfilePinRecord,
) -> HomeAcceptanceProfileAcquireOutcome {
    match record.state() {
        HomeAcceptanceProfileState::Held => HomeAcceptanceProfileAcquireOutcome::Held {
            record,
            created: true,
        },
        HomeAcceptanceProfileState::ResolvedApplied
        | HomeAcceptanceProfileState::ResolvedAborted => {
            HomeAcceptanceProfileAcquireOutcome::Resolved(record)
        }
    }
}

pub(super) fn get(
    connection: &Connection,
    intent: &HomeAcceptanceProfileIntent,
) -> Result<Option<HomeAcceptanceProfilePinRecord>, Error> {
    schema::validate_schema(connection)?;
    let transaction = connection.unchecked_transaction()?;
    let record = load_pin(&transaction, intent.acceptance_id())?;
    if let Some(record) = &record {
        if !same_core_intent(record, intent) {
            return Err(Error::Conflict);
        }
        validate_record_references(&transaction, record)?;
    }
    transaction.commit()?;
    Ok(record)
}

pub(super) fn pending(
    connection: &Connection,
    issuer: forge_domain::OrchestrationIssuerId,
    after_acceptance_id: Option<Uuid>,
    limit: u16,
) -> Result<HomeAcceptanceProfilePendingPage, Error> {
    if issuer.into_uuid().is_nil() || limit == 0 || limit > MAX_PAGE_SIZE {
        return Err(Error::PageLimit);
    }
    if after_acceptance_id.is_some_and(|id| id.is_nil()) {
        return Err(Error::Invalid("pending Profile pin cursor"));
    }
    schema::validate_schema(connection)?;
    let transaction = connection.unchecked_transaction()?;
    let page_limit = usize::from(limit);
    let sql_limit = i64::try_from(page_limit + 1).map_err(|_| Error::PageLimit)?;
    let raw_ids = {
        let mut statement = transaction.prepare(
            "SELECT substr(acceptance_id,1,17),length(acceptance_id)
             FROM agent_profile_home_acceptance_pins
             WHERE issuer_id=?1 AND state=1 AND (?2 IS NULL OR acceptance_id>?2)
             ORDER BY acceptance_id LIMIT ?3",
        )?;
        statement
            .query_map(
                params![
                    issuer.into_uuid().as_bytes(),
                    after_acceptance_id.map(|id| id.into_bytes().to_vec()),
                    sql_limit,
                ],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?
    };
    let has_more = raw_ids.len() > page_limit;
    let mut records = Vec::with_capacity(raw_ids.len().min(MAX_PAGE_SIZE.into()));
    for (id_bytes, id_length) in raw_ids.iter().take(page_limit) {
        let acceptance_id = checked_uuid(id_bytes, *id_length, "pending acceptance ID")?;
        let record = load_pin(&transaction, acceptance_id)?
            .ok_or(Error::Corrupt("Profile pin vanished in recovery snapshot"))?;
        if record.issuer() != issuer || record.state() != HomeAcceptanceProfileState::Held {
            return Err(Error::Corrupt("Profile pin recovery selector"));
        }
        validate_record_references(&transaction, &record)?;
        records.push(record);
    }
    let next_after = has_more
        .then(|| {
            records
                .last()
                .map(HomeAcceptanceProfilePinRecord::acceptance_id)
        })
        .flatten();
    transaction.commit()?;
    Ok(HomeAcceptanceProfilePendingPage {
        records,
        next_after,
    })
}

fn same_core_intent(
    record: &HomeAcceptanceProfilePinRecord,
    intent: &HomeAcceptanceProfileIntent,
) -> bool {
    record.request() == intent.request() && record.issuer() == intent.issuer()
}

fn held_record(
    intent: &HomeAcceptanceProfileIntent,
    pin: HomeAcceptanceProfilePin,
) -> Result<HomeAcceptanceProfilePinRecord, Error> {
    HomeAcceptanceProfilePinRecord::from_parts(&HomeAcceptanceProfilePinRecordParts {
        request: intent.request(),
        issuer: intent.issuer(),
        state: HomeAcceptanceProfileState::Held,
        profile_pin: Some(pin.reference()),
        source_payload_digest: None,
        source_outcome: None,
        source_at: None,
        operations_resolution: None,
        created_at: pin.acquired_at,
        resolved_at: None,
    })
}

fn resolved_record(
    intent: &HomeAcceptanceProfileIntent,
    existing: Option<&HomeAcceptanceProfilePinRecord>,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    operations: &HomeAcceptanceProfileOperationsResolution,
    resolved_at: TimestampMillis,
) -> Result<HomeAcceptanceProfilePinRecord, Error> {
    let state = match source.outcome_kind() {
        HomeHandoffAcceptanceOutcomeKind::Applied => HomeAcceptanceProfileState::ResolvedApplied,
        HomeHandoffAcceptanceOutcomeKind::Aborted => HomeAcceptanceProfileState::ResolvedAborted,
    };
    let profile_pin = existing.and_then(HomeAcceptanceProfilePinRecord::profile_pin);
    if source.outcome_kind() == HomeHandoffAcceptanceOutcomeKind::Applied && profile_pin.is_none() {
        return Err(Error::Corrupt(
            "Applied Source outcome requires an acquired Profile pin",
        ));
    }
    if let Some(operation_pin) = operations.profile_pin()
        && profile_pin != Some(operation_pin)
    {
        return Err(Error::Corrupt(
            "Operations Profile pin differs from acquired pin",
        ));
    }
    if operations.admission().is_some() && profile_pin.is_none() {
        return Err(Error::Corrupt(
            "Operations admission has no acquired Profile pin",
        ));
    }
    let created_at = existing.map_or(resolved_at, HomeAcceptanceProfilePinRecord::created_at);
    HomeAcceptanceProfilePinRecord::from_parts(&HomeAcceptanceProfilePinRecordParts {
        request: intent.request(),
        issuer: intent.issuer(),
        state,
        profile_pin,
        source_payload_digest: Some(*source.payload_digest()),
        source_outcome: Some(source.outcome_kind()),
        source_at: Some(source.source_at()),
        operations_resolution: Some(*operations),
        created_at,
        resolved_at: Some(resolved_at),
    })
}

fn settlement_matches(
    existing: &HomeAcceptanceProfilePinRecord,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    operations: &HomeAcceptanceProfileOperationsResolution,
) -> bool {
    existing.source_payload_digest() == Some(*source.payload_digest())
        && existing.source_outcome() == Some(source.outcome_kind())
        && existing.operations_resolution() == Some(*operations)
}

fn validate_operations_pin_reference(
    record: &HomeAcceptanceProfilePinRecord,
    operations: &HomeAcceptanceProfileOperationsResolution,
) -> Result<(), Error> {
    if operations
        .profile_pin()
        .is_some_and(|reference| record.profile_pin() != Some(reference))
    {
        return Err(Error::Corrupt("Operations Profile pin reference"));
    }
    if operations
        .admission()
        .is_some_and(|admission| admission.acceptance_id() != record.acceptance_id())
    {
        return Err(Error::Corrupt("Operations admission reference"));
    }
    Ok(())
}

fn insert_pin(
    transaction: &rusqlite::Transaction<'_>,
    record: &HomeAcceptanceProfilePinRecord,
) -> Result<(), Error> {
    let snapshot = record.request().canonical_snapshot();
    if snapshot.is_empty() || snapshot.len() > MAX_REQUEST_BYTES {
        return Err(Error::Corrupt("Profile pin take request bound"));
    }
    let pin = record.profile_pin();
    let encoded_pin = encode_profile_pin(pin);
    let (source_outcome, source_digest, source_at) = encode_source(record);
    let operations = record.operations_resolution();
    let resolution_id = operations.map(HomeAcceptanceProfileOperationsResolution::resolution_id);
    let resolution_digest =
        operations.map(HomeAcceptanceProfileOperationsResolution::resolution_digest);
    let admission_id = operations
        .and_then(HomeAcceptanceProfileOperationsResolution::admission)
        .map(HomeAcceptanceProfileOperationsAdmission::receipt_id);
    let admission_digest = operations
        .and_then(HomeAcceptanceProfileOperationsResolution::admission)
        .map(HomeAcceptanceProfileOperationsAdmission::receipt_digest);
    let operations_profile_pin_present = operations
        .map(HomeAcceptanceProfileOperationsResolution::profile_pin)
        .map(|pin| i64::from(pin.is_some()));
    let resolved_at = record.resolved_at().map(TimestampMillis::get);
    let changed = transaction.execute(
        "INSERT INTO agent_profile_home_acceptance_pins(
            acceptance_id,issuer_id,request_digest,request_snapshot,state,
            profile_id,profile_revision,logical_session_request_id,
            linked_profile_revision,linked_session_revision,acquired_at_ms,
            source_outcome,source_payload_digest,source_observed_at_ms,
            operations_resolution_id,operations_resolution_digest,
            operations_admission_id,operations_admission_digest,
            operations_profile_pin_present,created_at_ms,resolved_at_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        params![
            record.acceptance_id().as_bytes(),
            record.issuer().into_uuid().as_bytes(),
            record.request().request_digest().as_bytes(),
            snapshot,
            encode_profile_state(record.state()),
            encoded_pin.profile_id.as_deref(),
            encoded_pin.profile_revision,
            encoded_pin.logical_session_request_id.as_deref(),
            encoded_pin.linked_profile_revision,
            encoded_pin.linked_session_revision,
            encoded_pin.acquired_at,
            source_outcome,
            source_digest.as_deref(),
            source_at,
            resolution_id.map(|id| id.as_bytes().to_vec()),
            resolution_digest.map(|digest| digest.to_vec()),
            admission_id.map(|id| id.as_bytes().to_vec()),
            admission_digest.map(|digest| digest.to_vec()),
            operations_profile_pin_present,
            record.created_at().get(),
            resolved_at,
        ],
    )?;
    if changed != 1 {
        return Err(Error::Corrupt("Profile pin insert row count"));
    }
    if load_pin(transaction, record.acceptance_id())?.as_ref() != Some(record) {
        return Err(Error::Corrupt("Profile pin insert canonical readback"));
    }
    Ok(())
}

fn load_pin(
    connection: &Connection,
    acceptance_id: Uuid,
) -> Result<Option<HomeAcceptanceProfilePinRecord>, Error> {
    if acceptance_id.is_nil() {
        return Err(Error::Invalid("acceptance identity"));
    }
    let raw = connection
        .query_row(
            "SELECT substr(acceptance_id,1,17),length(acceptance_id),
                    substr(issuer_id,1,17),length(issuer_id),
                    substr(request_digest,1,33),length(request_digest),
                    substr(request_snapshot,1,321),length(request_snapshot),state,
                    substr(profile_id,1,17),length(profile_id),profile_revision,
                    substr(logical_session_request_id,1,17),length(logical_session_request_id),
                    linked_profile_revision,linked_session_revision,acquired_at_ms,
                    source_outcome,substr(source_payload_digest,1,33),length(source_payload_digest),
                    source_observed_at_ms,substr(operations_resolution_id,1,17),
                    length(operations_resolution_id),substr(operations_resolution_digest,1,33),
                    length(operations_resolution_digest),substr(operations_admission_id,1,17),
                    length(operations_admission_id),substr(operations_admission_digest,1,33),
                    length(operations_admission_digest),operations_profile_pin_present,
                    created_at_ms,resolved_at_ms
             FROM agent_profile_home_acceptance_pins WHERE acceptance_id=?1",
            [acceptance_id.as_bytes()],
            |row| {
                Ok(RawPin {
                    acceptance_id: row.get(0)?,
                    acceptance_id_len: row.get(1)?,
                    issuer_id: row.get(2)?,
                    issuer_id_len: row.get(3)?,
                    request_digest: row.get(4)?,
                    request_digest_len: row.get(5)?,
                    request_snapshot: row.get(6)?,
                    request_snapshot_len: row.get(7)?,
                    state: row.get(8)?,
                    profile_id: row.get(9)?,
                    profile_id_len: row.get(10)?,
                    profile_revision: row.get(11)?,
                    link_request_id: row.get(12)?,
                    link_request_id_len: row.get(13)?,
                    link_profile_revision: row.get(14)?,
                    link_session_revision: row.get(15)?,
                    acquired_at: row.get(16)?,
                    source_outcome: row.get(17)?,
                    source_payload_digest: row.get(18)?,
                    source_payload_digest_len: row.get(19)?,
                    source_at: row.get(20)?,
                    operations_resolution_id: row.get(21)?,
                    operations_resolution_id_len: row.get(22)?,
                    operations_resolution_digest: row.get(23)?,
                    operations_resolution_digest_len: row.get(24)?,
                    operations_admission_id: row.get(25)?,
                    operations_admission_id_len: row.get(26)?,
                    operations_admission_digest: row.get(27)?,
                    operations_admission_digest_len: row.get(28)?,
                    operations_profile_pin_present: row.get(29)?,
                    created_at: row.get(30)?,
                    resolved_at: row.get(31)?,
                })
            },
        )
        .optional()?;
    raw.as_ref().map(decode_pin).transpose()
}

fn decode_pin(raw: &RawPin) -> Result<HomeAcceptanceProfilePinRecord, Error> {
    let acceptance_id = checked_uuid(&raw.acceptance_id, raw.acceptance_id_len, "acceptance_id")?;
    let issuer = forge_domain::OrchestrationIssuerId::from_uuid(checked_uuid(
        &raw.issuer_id,
        raw.issuer_id_len,
        "issuer_id",
    )?);
    let stored_digest = checked_digest(
        &raw.request_digest,
        raw.request_digest_len,
        "request_digest",
    )?;
    let snapshot_length = usize::try_from(raw.request_snapshot_len)
        .map_err(|_| Error::Corrupt("Profile pin request snapshot length"))?;
    if snapshot_length == 0
        || snapshot_length > MAX_REQUEST_BYTES
        || raw.request_snapshot.len() != snapshot_length
    {
        return Err(Error::Corrupt("Profile pin request snapshot length"));
    }
    let request = HomeHandoffTakeRequest::from_canonical_snapshot(&raw.request_snapshot)
        .map_err(|_| Error::Corrupt("Profile pin request snapshot"))?;
    if request.acceptance_id() != acceptance_id
        || request.request_digest().as_bytes() != &stored_digest
    {
        return Err(Error::Corrupt("Profile pin request identity"));
    }
    let profile_pin = decode_profile_pin(raw)?;
    let source_payload_digest = decode_optional_digest(
        raw.source_payload_digest.as_deref(),
        raw.source_payload_digest_len,
        "source_payload_digest",
    )?;
    let source_outcome = raw.source_outcome.map(decode_source_outcome).transpose()?;
    let source_at = raw.source_at.map(profile_timestamp).transpose()?;
    let operations_context = DecodedPinContext {
        acceptance_id,
        issuer,
        request_digest: request.request_digest(),
        profile_pin,
        source_payload_digest,
        source_outcome,
    };
    let operations_resolution =
        decode_operations_resolution(&operations_context, decode_operations_pin(raw)?)?;
    HomeAcceptanceProfilePinRecord::from_parts(&HomeAcceptanceProfilePinRecordParts {
        request,
        issuer,
        state: decode_profile_state(raw.state)?,
        profile_pin,
        source_payload_digest,
        source_outcome,
        source_at,
        operations_resolution,
        created_at: profile_timestamp(raw.created_at)?,
        resolved_at: raw.resolved_at.map(profile_timestamp).transpose()?,
    })
    .map_err(|_| Error::Corrupt("Profile pin record"))
}

fn decode_operations_pin(raw: &RawPin) -> Result<DecodedOperationsPin, Error> {
    let resolution_id = decode_optional_uuid(
        raw.operations_resolution_id.as_deref(),
        raw.operations_resolution_id_len,
        "operations_resolution_id",
    )?;
    let resolution_digest = decode_optional_digest(
        raw.operations_resolution_digest.as_deref(),
        raw.operations_resolution_digest_len,
        "operations_resolution_digest",
    )?;
    let admission_id = decode_optional_uuid(
        raw.operations_admission_id.as_deref(),
        raw.operations_admission_id_len,
        "operations_admission_id",
    )?;
    let admission_digest = decode_optional_digest(
        raw.operations_admission_digest.as_deref(),
        raw.operations_admission_digest_len,
        "operations_admission_digest",
    )?;
    let profile_pin_present = match raw.operations_profile_pin_present {
        None => None,
        Some(0) => Some(false),
        Some(1) => Some(true),
        Some(_) => return Err(Error::Corrupt("Operations Profile pin presence")),
    };
    Ok(DecodedOperationsPin {
        resolution_id,
        resolution_digest,
        admission_id,
        admission_digest,
        profile_pin_present,
    })
}

fn decode_operations_resolution(
    context: &DecodedPinContext,
    stored: DecodedOperationsPin,
) -> Result<Option<HomeAcceptanceProfileOperationsResolution>, Error> {
    let DecodedPinContext {
        acceptance_id,
        issuer,
        request_digest,
        profile_pin,
        source_payload_digest,
        source_outcome,
    } = *context;
    let DecodedOperationsPin {
        resolution_id,
        resolution_digest,
        admission_id,
        admission_digest,
        profile_pin_present,
    } = stored;
    let resolution = match (
        resolution_id,
        resolution_digest,
        admission_id,
        admission_digest,
        source_outcome,
        source_payload_digest,
        profile_pin_present,
    ) {
        (None, None, None, None, None, None, None) => None,
        (
            Some(resolution_id),
            Some(resolution_digest),
            admission_id,
            admission_digest,
            Some(outcome),
            Some(source_payload_digest),
            Some(profile_pin_present),
        ) => {
            let admission = match (admission_id, admission_digest) {
                (None, None) => None,
                (Some(id), Some(digest)) => Some(
                    HomeAcceptanceProfileOperationsAdmission::new(acceptance_id, id, digest)
                        .map_err(|_| Error::Corrupt("Profile pin Operations admission"))?,
                ),
                _ => return Err(Error::Corrupt("Profile pin Operations admission pair")),
            };
            let operations_profile_pin = if profile_pin_present {
                Some(profile_pin.ok_or(Error::Corrupt(
                    "Operations Profile pin presence has no acquired Profile pin",
                ))?)
            } else {
                None
            };
            let parts = HomeAcceptanceProfileOperationsResolutionParts {
                acceptance_id,
                issuer,
                request_digest,
                outcome,
                source_payload_digest,
                resolution_id,
                resolution_digest,
                admission,
                profile_pin: operations_profile_pin,
            };
            Some(
                HomeAcceptanceProfileOperationsResolution::from_persistence_parts(&parts)
                    .map_err(|_| Error::Corrupt("Profile pin Operations resolution"))?,
            )
        }
        _ => return Err(Error::Corrupt("Profile pin Operations resolution fields")),
    };
    Ok(resolution)
}

fn decode_profile_pin(raw: &RawPin) -> Result<Option<HomeAcceptanceProfilePinReference>, Error> {
    match (
        raw.profile_id.as_ref(),
        raw.profile_id_len,
        raw.profile_revision,
        raw.link_request_id.as_ref(),
        raw.link_request_id_len,
        raw.link_profile_revision,
        raw.link_session_revision,
        raw.acquired_at,
    ) {
        (None, None, None, None, None, None, None, None) => Ok(None),
        (
            Some(profile_id),
            Some(profile_id_length),
            Some(profile_revision),
            Some(link_request_id),
            Some(link_request_id_length),
            Some(link_profile_revision),
            Some(link_session_revision),
            Some(acquired_at),
        ) => {
            let profile_id = AgentProfileId::from_uuid(checked_uuid(
                profile_id,
                profile_id_length,
                "profile_id",
            )?)
            .map_err(|_| Error::Corrupt("Profile pin Profile ID"))?;
            let profile_revision = AgentProfileRevision::new(profile_revision)
                .map_err(|_| Error::Corrupt("Profile pin Profile revision"))?;
            let link_request_id = checked_uuid(
                link_request_id,
                link_request_id_length,
                "logical_session_request_id",
            )?;
            let link_profile_revision = AgentProfileRevision::new(link_profile_revision)
                .map_err(|_| Error::Corrupt("Profile pin link Profile revision"))?;
            let link_session_revision =
                forge_domain::AgentSessionRevision::new(link_session_revision)
                    .map_err(|_| Error::Corrupt("Profile pin link Session revision"))?;
            let acquired_at = profile_timestamp(acquired_at)?;
            Ok(Some(
                HomeAcceptanceProfilePinReference::new(
                    profile_id,
                    profile_revision,
                    link_request_id,
                    link_profile_revision,
                    link_session_revision,
                    acquired_at,
                )
                .map_err(|_| Error::Corrupt("Profile pin reference"))?,
            ))
        }
        _ => Err(Error::Corrupt("Profile pin observation fields")),
    }
}

fn validate_record_references(
    connection: &Connection,
    record: &HomeAcceptanceProfilePinRecord,
) -> Result<(), Error> {
    let Some(pin) = record.profile_pin() else {
        if matches!(
            record.state(),
            HomeAcceptanceProfileState::Held | HomeAcceptanceProfileState::ResolvedApplied
        ) {
            return Err(Error::Corrupt(
                "Profile pin is missing its Profile reference",
            ));
        }
        return Ok(());
    };
    let profile = load_profile(connection, pin.profile_id())?
        .ok_or(Error::Corrupt("Profile pin Profile row is missing"))?;
    let link = logical_session_link_by_request(connection, pin.logical_session_request_id())?
        .ok_or(Error::Corrupt(
            "Profile pin logical Session link is missing",
        ))?;
    if link.profile_id() != pin.profile_id()
        || link.session_id() != record.request().recipient().session_id()
        || link.profile_revision() != pin.linked_profile_revision()
        || link.session_revision() != pin.linked_session_revision()
        || link.profile_revision() > pin.profile_revision()
        || profile.revision() < pin.profile_revision()
    {
        return Err(Error::Corrupt(
            "Profile pin logical-link or revision reference",
        ));
    }
    if record.state() == HomeAcceptanceProfileState::Held
        && (profile.lifecycle_state() != AgentProfileLifecycleState::Active
            || profile.revision() != pin.profile_revision())
    {
        return Err(Error::Corrupt(
            "held Profile pin no longer matches current authority",
        ));
    }
    Ok(())
}

fn profile_timestamp(value: i64) -> Result<TimestampMillis, Error> {
    TimestampMillis::new(value).map_err(|_| Error::Corrupt("Profile pin timestamp"))
}

fn decode_optional_uuid(
    bytes: Option<&[u8]>,
    length: Option<i64>,
    field: &'static str,
) -> Result<Option<Uuid>, Error> {
    match (bytes, length) {
        (None, None) => Ok(None),
        (Some(bytes), Some(length)) => checked_uuid(bytes, length, field).map(Some),
        _ => Err(Error::Corrupt(field)),
    }
}

fn decode_optional_digest(
    bytes: Option<&[u8]>,
    length: Option<i64>,
    field: &'static str,
) -> Result<Option<[u8; DIGEST_BYTES]>, Error> {
    match (bytes, length) {
        (None, None) => Ok(None),
        (Some(bytes), Some(length)) => checked_digest(bytes, length, field).map(Some),
        _ => Err(Error::Corrupt(field)),
    }
}

fn checked_uuid(bytes: &[u8], length: i64, field: &'static str) -> Result<Uuid, Error> {
    if length != 16 || bytes.len() != 16 {
        return Err(Error::Corrupt(field));
    }
    let id = Uuid::from_slice(bytes).map_err(|_| Error::Corrupt(field))?;
    if id.is_nil() {
        return Err(Error::Corrupt(field));
    }
    Ok(id)
}

fn checked_digest(
    bytes: &[u8],
    length: i64,
    field: &'static str,
) -> Result<[u8; DIGEST_BYTES], Error> {
    let expected_length = i64::try_from(DIGEST_BYTES).map_err(|_| Error::Corrupt(field))?;
    if length != expected_length || bytes.len() != DIGEST_BYTES {
        return Err(Error::Corrupt(field));
    }
    let digest: [u8; DIGEST_BYTES] = bytes.try_into().map_err(|_| Error::Corrupt(field))?;
    if digest == [0; DIGEST_BYTES] {
        return Err(Error::Corrupt(field));
    }
    Ok(digest)
}

fn update_pin(
    transaction: &rusqlite::Transaction<'_>,
    acceptance_id: Uuid,
    record: &HomeAcceptanceProfilePinRecord,
    expected_state: HomeAcceptanceProfileState,
) -> Result<(), Error> {
    let (source_outcome, source_digest, source_at) = encode_source(record);
    let operations = record.operations_resolution();
    let resolution_id = operations.map(HomeAcceptanceProfileOperationsResolution::resolution_id);
    let resolution_digest =
        operations.map(HomeAcceptanceProfileOperationsResolution::resolution_digest);
    let admission_id = operations
        .and_then(HomeAcceptanceProfileOperationsResolution::admission)
        .map(HomeAcceptanceProfileOperationsAdmission::receipt_id);
    let admission_digest = operations
        .and_then(HomeAcceptanceProfileOperationsResolution::admission)
        .map(HomeAcceptanceProfileOperationsAdmission::receipt_digest);
    let operations_profile_pin_present = operations
        .map(HomeAcceptanceProfileOperationsResolution::profile_pin)
        .map(|pin| i64::from(pin.is_some()));
    let changed = transaction.execute(
        "UPDATE agent_profile_home_acceptance_pins
         SET state=?1,source_outcome=?2,source_payload_digest=?3,source_observed_at_ms=?4,
             operations_resolution_id=?5,operations_resolution_digest=?6,
             operations_admission_id=?7,operations_admission_digest=?8,
             operations_profile_pin_present=?9,resolved_at_ms=?10
         WHERE acceptance_id=?11 AND state=?12",
        params![
            encode_profile_state(record.state()),
            source_outcome,
            source_digest.as_deref(),
            source_at,
            resolution_id.map(|id| id.as_bytes().to_vec()),
            resolution_digest.map(|digest| digest.to_vec()),
            admission_id.map(|id| id.as_bytes().to_vec()),
            admission_digest.map(|digest| digest.to_vec()),
            operations_profile_pin_present,
            record.resolved_at().map(TimestampMillis::get),
            acceptance_id.as_bytes(),
            encode_profile_state(expected_state),
        ],
    )?;
    if changed != 1 {
        return Err(Error::Conflict);
    }
    if load_pin(transaction, acceptance_id)?.as_ref() != Some(record) {
        return Err(Error::Corrupt("Profile pin resolution canonical readback"));
    }
    Ok(())
}

fn encode_profile_pin(pin: Option<HomeAcceptanceProfilePinReference>) -> EncodedProfilePin {
    match pin {
        None => EncodedProfilePin {
            profile_id: None,
            profile_revision: None,
            logical_session_request_id: None,
            linked_profile_revision: None,
            linked_session_revision: None,
            acquired_at: None,
        },
        Some(pin) => EncodedProfilePin {
            profile_id: Some(pin.profile_id().into_uuid().as_bytes().to_vec()),
            profile_revision: Some(pin.profile_revision().get()),
            logical_session_request_id: Some(pin.logical_session_request_id().as_bytes().to_vec()),
            linked_profile_revision: Some(pin.linked_profile_revision().get()),
            linked_session_revision: Some(pin.linked_session_revision().get()),
            acquired_at: Some(pin.acquired_at().get()),
        },
    }
}

fn encode_source(
    record: &HomeAcceptanceProfilePinRecord,
) -> (Option<i64>, Option<Vec<u8>>, Option<i64>) {
    (
        record.source_outcome().map(encode_source_outcome),
        record.source_payload_digest().map(|digest| digest.to_vec()),
        record.source_at().map(TimestampMillis::get),
    )
}

fn encode_profile_state(state: HomeAcceptanceProfileState) -> i64 {
    match state {
        HomeAcceptanceProfileState::Held => 1,
        HomeAcceptanceProfileState::ResolvedApplied => 2,
        HomeAcceptanceProfileState::ResolvedAborted => 3,
    }
}

fn decode_profile_state(value: i64) -> Result<HomeAcceptanceProfileState, Error> {
    match value {
        1 => Ok(HomeAcceptanceProfileState::Held),
        2 => Ok(HomeAcceptanceProfileState::ResolvedApplied),
        3 => Ok(HomeAcceptanceProfileState::ResolvedAborted),
        _ => Err(Error::Corrupt("Profile pin state")),
    }
}

fn encode_source_outcome(outcome: HomeHandoffAcceptanceOutcomeKind) -> i64 {
    match outcome {
        HomeHandoffAcceptanceOutcomeKind::Applied => 1,
        HomeHandoffAcceptanceOutcomeKind::Aborted => 2,
    }
}

fn decode_source_outcome(value: i64) -> Result<HomeHandoffAcceptanceOutcomeKind, Error> {
    match value {
        1 => Ok(HomeHandoffAcceptanceOutcomeKind::Applied),
        2 => Ok(HomeHandoffAcceptanceOutcomeKind::Aborted),
        _ => Err(Error::Corrupt("Source terminal outcome")),
    }
}

pub(super) fn resolve<C>(
    catalog: &mut AgentProfileCatalog,
    intent: &HomeAcceptanceProfileIntent,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    operations: &HomeAcceptanceProfileOperationsResolution,
    sample_clock: C,
) -> Result<HomeAcceptanceProfilePinRecord, Error>
where
    C: FnOnce() -> Result<TimestampMillis, Error>,
{
    if source.request() != intent.request()
        || source.issuer() != intent.issuer()
        || !operations.matches_source(source)
    {
        return Err(Error::Conflict);
    }
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let existing = load_pin(&transaction, intent.acceptance_id())?;
    if let Some(record) = &existing {
        if !same_core_intent(record, intent) {
            return Err(Error::Conflict);
        }
        if record.state() != HomeAcceptanceProfileState::Held {
            return if settlement_matches(record, source, operations) {
                transaction.commit()?;
                Ok(record.clone())
            } else {
                Err(Error::Conflict)
            };
        }
        validate_record_references(&transaction, record)?;
        validate_operations_pin_reference(record, operations)?;
    } else if source.outcome_kind() == HomeHandoffAcceptanceOutcomeKind::Applied {
        return Err(Error::Corrupt(
            "Applied Source acceptance has no Profile hold",
        ));
    } else if operations.profile_pin().is_some() {
        return Err(Error::Corrupt(
            "Operations references a missing Profile hold",
        ));
    }

    let resolved_at = sample_clock()?;
    let record = resolved_record(intent, existing.as_ref(), source, operations, resolved_at)?;
    if let Some(existing) = existing.as_ref() {
        update_pin(
            &transaction,
            intent.acceptance_id(),
            &record,
            existing.state(),
        )?;
    } else {
        insert_pin(&transaction, &record)?;
    }
    transaction
        .commit()
        .map_err(|source| Error::CommitUnconfirmed {
            acceptance_id: intent.acceptance_id(),
            source: Box::new(source),
        })?;
    readback_after_commit(&catalog.connection, intent)
}

fn readback_after_commit(
    connection: &Connection,
    intent: &HomeAcceptanceProfileIntent,
) -> Result<HomeAcceptanceProfilePinRecord, Error> {
    get_exact(connection, intent)
        .and_then(|record| record.ok_or(Error::Corrupt("committed Profile pin is missing")))
        .map_err(|source| Error::ReadbackRequired {
            acceptance_id: intent.acceptance_id(),
            source: Box::new(source.into()),
        })
}

fn get_exact(
    connection: &Connection,
    intent: &HomeAcceptanceProfileIntent,
) -> Result<Option<HomeAcceptanceProfilePinRecord>, Error> {
    get(connection, intent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{
        AgentAssignmentTarget, AgentInstanceId, AgentInstanceRevision, AgentSessionId,
        AuthenticatedPrincipal, DirectTokenId, ForgeBackendId, HomeHandoffProposalRequestDigest,
        HomeTransferSourceScope, OrchestrationIssuerId, WorkspaceId, WorkspaceRevision,
    };
    use std::error::Error;

    struct AbortedPinFixture {
        request: HomeHandoffTakeRequest,
        profile_id: AgentProfileId,
        profile_request_id: Uuid,
        session_id: AgentSessionId,
        profile_pin: HomeAcceptanceProfilePinReference,
        record: HomeAcceptanceProfilePinRecord,
    }

    fn take_request() -> Result<HomeHandoffTakeRequest, Box<dyn Error>> {
        let session_id = AgentSessionId::from_uuid(Uuid::from_u128(0xB104));
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xB105)));
        Ok(HomeHandoffTakeRequest::new(
            Uuid::from_u128(0xB101),
            Uuid::from_u128(0xB106),
            Uuid::from_u128(0xB107),
            1,
            HomeHandoffProposalRequestDigest::from_bytes([0xA1; DIGEST_BYTES])?,
            HomeTransferSourceScope::new(
                WorkspaceId::from_uuid(Uuid::from_u128(0xB108)),
                WorkspaceRevision::new(1)?,
                ForgeBackendId::from_uuid(Uuid::from_u128(0xB109)),
                Uuid::from_u128(0xB10A),
            )?,
            AgentAssignmentTarget::new(
                AgentInstanceId::from_uuid(Uuid::from_u128(0xB10B)),
                session_id,
                principal,
                AgentInstanceRevision::new(1)?,
            ),
        )?)
    }

    fn aborted_pin_fixture() -> Result<AbortedPinFixture, Box<dyn Error>> {
        let request = take_request()?;
        let acceptance_id = request.acceptance_id();
        let issuer = OrchestrationIssuerId::from_uuid(Uuid::from_u128(0xB10C));
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xB102))?;
        let profile_request_id = Uuid::from_u128(0xB103);
        let session_id = request.recipient().session_id();
        let profile_revision = AgentProfileRevision::new(1)?;
        let acquired_at = TimestampMillis::new(10)?;
        let profile_pin = HomeAcceptanceProfilePinReference::new(
            profile_id,
            profile_revision,
            Uuid::from_u128(0xB10D),
            profile_revision,
            forge_domain::AgentSessionRevision::new(1)?,
            acquired_at,
        )?;

        let source_payload_digest = [0xA2; DIGEST_BYTES];
        // Synthetic owner references validate this codec, not receipt authentication.
        let operations = HomeAcceptanceProfileOperationsResolution::from_persistence_parts(
            &HomeAcceptanceProfileOperationsResolutionParts {
                acceptance_id,
                issuer,
                request_digest: request.request_digest(),
                outcome: HomeHandoffAcceptanceOutcomeKind::Aborted,
                source_payload_digest,
                resolution_id: Uuid::from_u128(0xB10E),
                resolution_digest: [0xA3; DIGEST_BYTES],
                admission: None,
                profile_pin: None,
            },
        )?;
        let record =
            HomeAcceptanceProfilePinRecord::from_parts(&HomeAcceptanceProfilePinRecordParts {
                request,
                issuer,
                state: HomeAcceptanceProfileState::ResolvedAborted,
                profile_pin: Some(profile_pin),
                source_payload_digest: Some(source_payload_digest),
                source_outcome: Some(HomeHandoffAcceptanceOutcomeKind::Aborted),
                source_at: Some(TimestampMillis::new(25)?),
                operations_resolution: Some(operations),
                created_at: acquired_at,
                resolved_at: Some(TimestampMillis::new(30)?),
            })?;

        Ok(AbortedPinFixture {
            request,
            profile_id,
            profile_request_id,
            session_id,
            profile_pin,
            record,
        })
    }

    fn insert_fixture_owner_rows(
        connection: &Connection,
        fixture: &AbortedPinFixture,
    ) -> Result<(), rusqlite::Error> {
        connection.execute(
            "INSERT INTO agent_profiles(
                 profile_id,request_id,stable_name,display_name,provider_family,
                 lifecycle_state,creation_origin,created_at_ms,updated_at_ms,revision
             ) VALUES(?1,?2,'recipient','Recipient',1,1,1,1,1,1)",
            rusqlite::params![
                fixture.profile_id.into_uuid().as_bytes(),
                fixture.profile_request_id.as_bytes(),
            ],
        )?;
        connection.execute(
            "INSERT INTO agent_profile_logical_sessions(
                 request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms
             ) VALUES(?1,?2,1,?3,1,9)",
            rusqlite::params![
                fixture.profile_pin.logical_session_request_id().as_bytes(),
                fixture.profile_id.into_uuid().as_bytes(),
                fixture.session_id.into_uuid().as_bytes(),
            ],
        )?;
        Ok(())
    }

    #[test]
    fn aborted_resolution_preserves_absent_operations_profile_pin_and_acquire_readback()
    -> Result<(), Box<dyn Error>> {
        let mut connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", true)?;
        crate::profile_catalog::create_schema(&mut connection)?;
        let fixture = aborted_pin_fixture()?;
        insert_fixture_owner_rows(&connection, &fixture)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_pin(&transaction, &fixture.record)?;
        transaction.commit()?;

        let stored_operations_pin_presence: Option<i64> = connection.query_row(
            "SELECT operations_profile_pin_present
             FROM agent_profile_home_acceptance_pins WHERE acceptance_id=?1",
            [fixture.request.acceptance_id().as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(stored_operations_pin_presence, Some(0));
        let readback =
            load_pin(&connection, fixture.request.acceptance_id())?.ok_or("pin disappeared")?;
        assert_eq!(readback, fixture.record);
        assert_eq!(
            readback
                .operations_resolution()
                .ok_or("Operations resolution disappeared")?
                .profile_pin(),
            None,
        );
        assert!(matches!(
            classify_acquire_readback(readback),
            HomeAcceptanceProfileAcquireOutcome::Resolved(_)
        ));
        Ok(())
    }

    #[test]
    fn postcommit_readback_failure_retains_the_acceptance_identity() -> Result<(), Box<dyn Error>> {
        // Synthetic resolution data exercises the local postcommit error
        // boundary, not external Source/Operations receipt authenticity.
        let mut connection = Connection::open_in_memory()?;
        crate::profile_catalog::create_schema(&mut connection)?;
        let fixture = aborted_pin_fixture()?;
        insert_fixture_owner_rows(&connection, &fixture)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_pin(&transaction, &fixture.record)?;
        transaction.commit()?;
        let intent =
            HomeAcceptanceProfileIntent::new(fixture.request, fixture.record.issuer(), None)?;
        assert_eq!(readback_after_commit(&connection, &intent)?, fixture.record);
        connection.execute_batch("DROP TABLE agent_profile_home_acceptance_pins")?;
        assert!(matches!(
            readback_after_commit(&connection, &intent),
            Err(super::Error::ReadbackRequired { acceptance_id, .. })
                if acceptance_id == intent.acceptance_id()
        ));
        Ok(())
    }
}
