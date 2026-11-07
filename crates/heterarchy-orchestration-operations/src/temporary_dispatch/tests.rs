use std::{
    cell::Cell,
    error::Error,
    fs::OpenOptions,
    io::{Error as IoError, ErrorKind},
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use uuid::Uuid;

use super::{
    OperationsError, TemporaryDispatchActorProvenance, TemporaryDispatchAdmissionRequest,
    TemporaryDispatchAdmissionResult, TemporaryDispatchRequestRecord,
    TemporaryDispatchRequestedScope, quota, resolve_target_home_team, sample_clock, schema,
    validate_requesting_leader_authority,
};
use crate::{
    LeadershipScope, OperationalActor, OperationsCatalog, TeamCreateCommand, TeamRosterMemberInput,
    WorkspaceParticipationId,
};

const WORKSPACE: u128 = 0x100;
const REQUESTING_TEAM: u128 = 0x200;
const HOME_TEAM: u128 = 0x300;
const ACTOR_PARTICIPANT: u128 = 0x400;
const ACTOR_PROFILE: u128 = 0x500;
const ACTOR_PARTICIPATION: u128 = 0x600;
const TARGET_PARTICIPANT: u128 = 0x700;
const ACTOR_SESSION: u128 = 0x800;
const TARGET_PROFILE: u128 = 0x900;
const OWNER_DESTINATION: u128 = 0x9000;
const OWNER_RECEIVING_TEAM: u128 = 0xA100;
const OWNER_HOME_TEAM: u128 = 0xA200;
const WINDOW_MS: i64 = 60 * 60 * 1_000;
const OWNER_NOW_MS: i64 = 1_000_000;

static TEMPORARY_DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
struct OwnerParticipant {
    profile: Uuid,
    participation: Uuid,
    participant: Uuid,
    activity: Uuid,
    agent: Uuid,
    session: Uuid,
}

impl OwnerParticipant {
    fn actor(self) -> OperationalActor {
        OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(self.activity),
            agent_id: AgentInstanceId::from_uuid(self.agent),
            session_id: AgentSessionId::from_uuid(self.session),
        }
    }

    fn roster_member(self, role: &str) -> TeamRosterMemberInput {
        TeamRosterMemberInput {
            activity_id: ActivityId::from_uuid(self.activity),
            agent_id: AgentInstanceId::from_uuid(self.agent),
            session_id: AgentSessionId::from_uuid(self.session),
            role: role.to_owned(),
        }
    }
}

struct OwnerFixture {
    catalog: OperationsCatalog,
    workspace: WorkspaceId,
    receiving_team: Uuid,
    home_team: Uuid,
    leader: OwnerParticipant,
    receiving_peer: OwnerParticipant,
    target: OwnerParticipant,
    home_leader: OwnerParticipant,
}

#[derive(Debug, PartialEq, Eq)]
struct OwnerQuotaStateSnapshot {
    last_admitted_at_ms: i64,
    last_sequence: i64,
    recent_1_request_id: Vec<u8>,
    recent_1_sequence: i64,
    recent_1_admitted_at_ms: i64,
    recent_2_request_id: Option<Vec<u8>>,
    recent_2_sequence: Option<i64>,
    recent_2_admitted_at_ms: Option<i64>,
    recent_3_request_id: Option<Vec<u8>>,
    recent_3_sequence: Option<i64>,
    recent_3_admitted_at_ms: Option<i64>,
    locked_until_ms: Option<i64>,
    lock_request_id: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
struct OwnerMutationSnapshot {
    receipt_ids: Vec<Vec<u8>>,
    quota_state_count: i64,
    requesting_team_state: Option<OwnerQuotaStateSnapshot>,
}

struct OwnerContentionSummary {
    admitted_request_id: Uuid,
    retry_at: TimestampMillis,
}

#[test]
fn quota_window_is_open_at_lower_edge_and_closed_at_upper_edge() -> Result<(), Box<dyn Error>> {
    let mut connection = in_memory_catalog()?;
    let first = request(1, ACTOR_SESSION, "scope", 10_000_000)?;
    let second = request(2, ACTOR_SESSION, "scope", 10_000_000)?;
    let third = request(3, ACTOR_SESSION, "scope", 10_000_000)?;
    let fourth = request(4, ACTOR_SESSION, "scope", 10_000_000)?;

    admit(&mut connection, &first, 0)?;
    admit(&mut connection, &second, 100)?;
    let lower_edge = admit(&mut connection, &third, WINDOW_MS)?;
    assert_eq!(lower_edge.lock_until(), None);

    // The admission at exactly `now` remains in (now - 60m, now], making this
    // the third admission in the window and fixing its lock boundary.
    let upper_edge = admit(&mut connection, &fourth, WINDOW_MS)?;
    assert_eq!(upper_edge.admitted_at(), timestamp(WINDOW_MS)?);
    assert_eq!(upper_edge.lock_until(), Some(timestamp(WINDOW_MS * 2)?));
    Ok(())
}

#[test]
fn third_admission_lock_is_fixed_and_quota_rejections_do_not_extend_it()
-> Result<(), Box<dyn Error>> {
    let mut connection = in_memory_catalog()?;
    let base = 2_000_000;
    for (seed, offset) in [(1, 0), (2, 1), (3, 2)] {
        let command = request(seed, ACTOR_SESSION, "scope", 9_000_000)?;
        let record = admit(&mut connection, &command, base + offset)?;
        if seed == 3 {
            assert_eq!(record.lock_until(), Some(timestamp(base + 2 + WINDOW_MS)?));
        } else {
            assert_eq!(record.lock_until(), None);
        }
    }

    let lock_until = base + 2 + WINDOW_MS;
    let denied = request(4, ACTOR_SESSION, "scope", 9_000_000)?;
    let denied_record = attempt(&mut connection, &denied, lock_until - 1)?;
    assert_eq!(
        denied_record,
        TemporaryDispatchAdmissionResult::QuotaRejected {
            request_id: denied.request_id(),
            workspace_id: denied.workspace_id(),
            requesting_team_id: denied.requesting_team_id(),
            retry_at: timestamp(lock_until)?,
        }
    );
    let repeated = request(6, ACTOR_SESSION, "scope", 9_000_000)?;
    assert_eq!(
        attempt(&mut connection, &repeated, lock_until - 1)?,
        TemporaryDispatchAdmissionResult::QuotaRejected {
            request_id: repeated.request_id(),
            workspace_id: repeated.workspace_id(),
            requesting_team_id: repeated.requesting_team_id(),
            retry_at: timestamp(lock_until)?,
        }
    );

    let counted_before_unlock: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests WHERE workspace_id=?1 AND requesting_team_id=?2",
        params![id(WORKSPACE).as_bytes(), id(REQUESTING_TEAM).as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(counted_before_unlock, 3);

    let stored_lock: i64 = connection.query_row(
        "SELECT locked_until_ms FROM ops_temporary_dispatch_quota_state WHERE workspace_id=?1 AND requesting_team_id=?2",
        params![id(WORKSPACE).as_bytes(), id(REQUESTING_TEAM).as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(stored_lock, lock_until);
    let last_admitted: i64 = connection.query_row(
        "SELECT last_admitted_at_ms FROM ops_temporary_dispatch_quota_state WHERE workspace_id=?1 AND requesting_team_id=?2",
        params![id(WORKSPACE).as_bytes(), id(REQUESTING_TEAM).as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(last_admitted, base + 2);
    let request_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests WHERE workspace_id=?1 AND requesting_team_id=?2",
        params![id(WORKSPACE).as_bytes(), id(REQUESTING_TEAM).as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(request_count, 3);

    // A rejection reserves no request ID, so the same request is evaluated
    // again after the fixed lock expires.
    assert!(matches!(
        attempt(&mut connection, &denied, lock_until)?,
        TemporaryDispatchAdmissionResult::Admitted(_)
    ));
    let admission_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests WHERE workspace_id=?1 AND requesting_team_id=?2",
        params![id(WORKSPACE).as_bytes(), id(REQUESTING_TEAM).as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(admission_count, 4);
    Ok(())
}

#[test]
fn concurrent_same_time_third_slot_admits_only_one_new_request() -> Result<(), Box<dyn Error>> {
    let database = TemporaryDatabase::new()?;
    let mut connection = Connection::open(&database.path)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    schema::create_schema(&connection)?;
    let base = 5_000_000;
    for (seed, at) in [(1, base - 2), (2, base - 1)] {
        let command = request(seed, ACTOR_SESSION, "scope", 9_000_000)?;
        admit(&mut connection, &command, at)?;
    }
    drop(connection);

    let barrier = Arc::new(Barrier::new(3));
    let workers = [6_u128, 7_u128].map(|request_seed| {
        let barrier = Arc::clone(&barrier);
        let path = database.path.clone();
        std::thread::spawn(move || {
            let opened_connection = Connection::open(path);
            barrier.wait();
            let mut connection = opened_connection.map_err(|error| error.to_string())?;
            connection
                .pragma_update(None, "foreign_keys", true)
                .map_err(|error| error.to_string())?;
            connection
                .busy_timeout(Duration::from_secs(5))
                .map_err(|error| error.to_string())?;
            let command = request(request_seed, ACTOR_SESSION, "scope", 9_000_000)
                .map_err(|error| error.to_string())?;
            let actor = provenance(ACTOR_SESSION).map_err(|error| error.to_string())?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| error.to_string())?;
            let outcome = quota::record_admission_attempt(
                &transaction,
                &command,
                &actor,
                id(HOME_TEAM),
                timestamp(base).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            match outcome {
                TemporaryDispatchAdmissionResult::Admitted(record) => {
                    transaction.commit().map_err(|error| error.to_string())?;
                    Ok::<_, String>(TemporaryDispatchAdmissionResult::Admitted(record))
                }
                rejected @ TemporaryDispatchAdmissionResult::QuotaRejected { .. } => {
                    transaction.rollback().map_err(|error| error.to_string())?;
                    Ok::<_, String>(rejected)
                }
            }
        })
    });
    barrier.wait();

    let outcomes = workers
        .into_iter()
        .map(|worker| {
            worker
                .join()
                .map_err(|_| IoError::other("quota admission worker panicked"))?
                .map_err(IoError::other)
        })
        .collect::<Result<Vec<_>, IoError>>()?;
    let admitted = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, TemporaryDispatchAdmissionResult::Admitted(_)))
        .count();
    let rejected = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                TemporaryDispatchAdmissionResult::QuotaRejected { .. }
            )
        })
        .count();
    assert_eq!((admitted, rejected), (1, 1));

    let connection = Connection::open(&database.path)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    let admission_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(admission_count, 3);
    Ok(())
}

#[test]
fn regressed_or_overflowing_clock_does_not_publish_an_admission() -> Result<(), Box<dyn Error>> {
    let mut connection = in_memory_catalog()?;
    let first = request(1, ACTOR_SESSION, "scope", 9_000_000)?;
    admit(&mut connection, &first, 5_000)?;

    let regressed = request(2, ACTOR_SESSION, "scope", 9_000_000)?;
    let error = attempt(&mut connection, &regressed, 4_999)
        .err()
        .ok_or_else(|| IoError::other("expected clock regression"))?;
    assert!(matches!(
        error,
        OperationsError::Conflict("temporary dispatch clock regression")
    ));

    let overflowing = request(3, ACTOR_SESSION, "scope", i64::MAX)?;
    let error = attempt(&mut connection, &overflowing, i64::MAX)
        .err()
        .ok_or_else(|| IoError::other("expected clock overflow"))?;
    assert!(matches!(
        error,
        OperationsError::Bounds("temporary dispatch clock range")
    ));

    let admission_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(admission_count, 1);
    Ok(())
}

#[test]
fn admission_sequence_overflow_fails_without_publishing_a_receipt() -> Result<(), Box<dyn Error>> {
    let mut connection = in_memory_catalog()?;
    let existing = request(1, ACTOR_SESSION, "scope", 9_000_000)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let actor = provenance(ACTOR_SESSION)?;
    quota::seed_receipt_sequence_for_test(
        &transaction,
        &existing,
        &actor,
        id(HOME_TEAM),
        i64::MAX,
        timestamp(5_000)?,
    )?;
    transaction.commit()?;

    let next = request(2, ACTOR_SESSION, "scope", 9_000_000)?;
    let error = attempt(&mut connection, &next, 5_001)
        .err()
        .ok_or_else(|| IoError::other("expected admission sequence overflow"))?;
    assert!(matches!(
        error,
        OperationsError::Bounds("temporary dispatch admission sequence")
    ));
    let request_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(request_count, 1);
    Ok(())
}

#[test]
fn missing_or_mismatched_fixed_lock_state_fails_closed() -> Result<(), Box<dyn Error>> {
    for corrupt_state in ["missing", "mismatched"] {
        let mut connection = in_memory_catalog()?;
        let base = 3_000_000;
        for (seed, offset) in [(1, 0), (2, 1), (3, 2)] {
            let command = request(seed, ACTOR_SESSION, "scope", 9_000_000)?;
            let record = admit(&mut connection, &command, base + offset)?;
            if seed == 3 {
                assert_eq!(record.lock_until(), Some(timestamp(base + 2 + WINDOW_MS)?));
            }
        }
        if corrupt_state == "missing" {
            connection.execute("DELETE FROM ops_temporary_dispatch_quota_state", [])?;
        } else {
            connection.execute(
                "UPDATE ops_temporary_dispatch_quota_state SET locked_until_ms=locked_until_ms+1",
                [],
            )?;
        }
        let next = request(4, ACTOR_SESSION, "scope", 9_000_000)?;
        let error = attempt(&mut connection, &next, base + 100)
            .err()
            .ok_or_else(|| IoError::other("expected corrupted quota state"))?;
        let expected = if corrupt_state == "missing" {
            "temporary dispatch quota state missing"
        } else {
            "temporary dispatch quota receipt anchors"
        };
        assert!(matches!(error, OperationsError::Corrupt(message) if message == expected));
        let request_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM ops_temporary_dispatch_requests",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(request_count, 3);
    }
    Ok(())
}

#[derive(Debug)]
struct ReceiptTailRows {
    newest_id: Vec<u8>,
    newest_sequence: i64,
    newest_at_ms: i64,
    previous_id: Option<Vec<u8>>,
    previous_sequence: Option<i64>,
    previous_at_ms: Option<i64>,
}

#[test]
fn same_time_receipt_tail_tracks_distinct_sequences() -> Result<(), Box<dyn Error>> {
    let mut connection = in_memory_catalog()?;
    let same_time = 6_000_000;
    let first = request(1, ACTOR_SESSION, "scope", 9_000_000)?;
    let second = request(2, ACTOR_SESSION, "scope", 9_000_000)?;
    admit(&mut connection, &first, same_time)?;
    admit(&mut connection, &second, same_time)?;

    let tail = connection.query_row(
        "SELECT recent_1_request_id,recent_1_sequence,recent_1_admitted_at_ms,recent_2_request_id,recent_2_sequence,recent_2_admitted_at_ms FROM ops_temporary_dispatch_quota_state WHERE workspace_id=?1 AND requesting_team_id=?2",
        params![id(WORKSPACE).as_bytes(), id(REQUESTING_TEAM).as_bytes()],
        |row| {
            Ok(ReceiptTailRows {
                newest_id: row.get(0)?,
                newest_sequence: row.get(1)?,
                newest_at_ms: row.get(2)?,
                previous_id: row.get(3)?,
                previous_sequence: row.get(4)?,
                previous_at_ms: row.get(5)?,
            })
        },
    )?;
    assert_eq!(Uuid::from_slice(&tail.newest_id)?, second.request_id());
    assert_eq!((tail.newest_sequence, tail.newest_at_ms), (2, same_time));
    let previous_id = tail
        .previous_id
        .as_deref()
        .ok_or_else(|| IoError::other("previous receipt anchor missing"))?;
    assert_eq!(Uuid::from_slice(previous_id)?, first.request_id());
    assert_eq!(
        (
            tail.previous_sequence
                .ok_or_else(|| IoError::other("previous receipt sequence missing"))?,
            tail.previous_at_ms
                .ok_or_else(|| IoError::other("previous receipt time missing"))?,
        ),
        (1, same_time)
    );
    Ok(())
}

#[test]
fn admitted_receipt_update_and_delete_are_immutable() -> Result<(), Box<dyn Error>> {
    let mut connection = in_memory_catalog()?;
    connection.pragma_update(None, "foreign_keys", false)?;
    let first = request(1, ACTOR_SESSION, "scope", 9_000_000)?;
    admit(&mut connection, &first, 6_000_000)?;

    assert!(connection
        .execute(
            "UPDATE ops_temporary_dispatch_requests SET requested_scope='changed' WHERE request_id=?1",
            params![first.request_id().as_bytes()],
        )
        .is_err());
    assert!(
        connection
            .execute(
                "DELETE FROM ops_temporary_dispatch_requests WHERE request_id=?1",
                params![first.request_id().as_bytes()],
            )
            .is_err()
    );
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn missing_same_time_tail_receipt_fails_audit_and_reopen() -> Result<(), Box<dyn Error>> {
    let database = TemporaryDatabase::new()?;
    let mut live_catalog = Connection::open(&database.path)?;
    live_catalog.pragma_update(None, "foreign_keys", true)?;
    schema::create_schema(&live_catalog)?;
    let same_time = 6_000_000;
    let first = request(1, ACTOR_SESSION, "scope", 9_000_000)?;
    let second = request(2, ACTOR_SESSION, "scope", 9_000_000)?;
    admit(&mut live_catalog, &first, same_time)?;
    admit(&mut live_catalog, &second, same_time)?;

    let corruptor = Connection::open(&database.path)?;
    corruptor.pragma_update(None, "foreign_keys", false)?;
    assert!(
        corruptor
            .execute(
                "DELETE FROM ops_temporary_dispatch_requests WHERE request_id=?1",
                params![first.request_id().as_bytes()],
            )
            .is_err()
    );
    corruptor.execute_batch(
        "DROP TRIGGER ops_temporary_dispatch_receipt_no_update;
         DROP TRIGGER ops_temporary_dispatch_receipt_no_delete;",
    )?;
    corruptor.execute(
        "DELETE FROM ops_temporary_dispatch_requests WHERE request_id=?1",
        params![first.request_id().as_bytes()],
    )?;
    schema::create_immutable_request_triggers(&corruptor)?;
    schema::validate_schema(&corruptor)?;
    drop(corruptor);

    schema::validate_schema(&live_catalog)?;
    let third = request(3, ACTOR_SESSION, "scope", 9_000_000)?;
    let live_error = attempt(&mut live_catalog, &third, same_time)
        .err()
        .ok_or_else(|| IoError::other("expected live receipt-tail corruption"))?;
    assert!(matches!(
        live_error,
        OperationsError::Corrupt("temporary dispatch quota receipt anchors")
    ));
    let receipt_count: i64 = live_catalog.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests WHERE workspace_id=?1 AND requesting_team_id=?2",
        params![id(WORKSPACE).as_bytes(), id(REQUESTING_TEAM).as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(receipt_count, 1);
    drop(live_catalog);

    let mut reopened = Connection::open(&database.path)?;
    reopened.pragma_update(None, "foreign_keys", true)?;
    schema::validate_schema(&reopened)?;
    let mut statement = reopened.prepare("PRAGMA foreign_key_check")?;
    let has_foreign_key_error = {
        let mut rows = statement.query([])?;
        rows.next()?.is_some()
    };
    drop(statement);
    assert!(has_foreign_key_error);
    let reopen_error = attempt(&mut reopened, &third, same_time)
        .err()
        .ok_or_else(|| IoError::other("expected reopened receipt-tail corruption"))?;
    assert!(matches!(
        reopen_error,
        OperationsError::Corrupt("temporary dispatch quota receipt anchors")
    ));
    Ok(())
}

#[test]
fn election_state_requires_current_authority_and_handover_state_is_rejected() {
    let leader = id(ACTOR_PARTICIPANT);
    let confirmed = Some((leader, 7));
    for state in ["ACTIVE", "STABILIZING", "ELECTING"] {
        assert!(validate_requesting_leader_authority(state, confirmed, leader, 7).is_ok());
    }
    assert!(validate_requesting_leader_authority("ELECTING", None, leader, 7).is_err());
    assert!(
        validate_requesting_leader_authority("AUTHORITY_PENDING", confirmed, leader, 7).is_err()
    );
    assert!(validate_requesting_leader_authority("HANDOFF_PENDING", confirmed, leader, 7).is_err());
}

#[test]
fn same_logical_actor_replay_survives_transport_rebind_without_rewriting_origin()
-> Result<(), Box<dyn Error>> {
    let original = request(1, ACTOR_SESSION, "bounded work intent", 8_000_000)?;
    let mut connection = in_memory_catalog()?;
    let record = admit(&mut connection, &original, 1_000)?;
    let rebound = request(1, 0xA02, "bounded work intent", 8_000_000)?;
    let rebound_actor = provenance(0xA02)?;

    assert!(record.matches_intent(&original, &provenance(ACTOR_SESSION)?));
    assert!(!record.matches_intent(&rebound, &rebound_actor));
    assert!(record.matches_readback_intent(&rebound, &rebound_actor));
    assert_eq!(record.actor().session_id(), session(ACTOR_SESSION));

    let changed_scope = request(1, 0xA02, "different work intent", 8_000_000)?;
    assert!(!record.matches_readback_intent(&changed_scope, &rebound_actor));
    let other_actor = TemporaryDispatchActorProvenance {
        participant_id: id(0xB01),
        ..rebound_actor
    };
    assert!(!record.matches_readback_intent(&rebound, &other_actor));
    Ok(())
}

#[test]
fn schema_helpers_create_and_validate_only_the_owned_objects() -> Result<(), Box<dyn Error>> {
    let connection = Connection::open_in_memory()?;
    schema::create_schema(&connection)?;
    schema::validate_schema(&connection)?;
    connection.execute_batch("DROP INDEX ops_temporary_dispatch_admission_window_idx")?;
    assert!(schema::validate_schema(&connection).is_err());
    Ok(())
}

#[test]
fn unavailable_clock_fails_closed() {
    assert!(matches!(
        sample_clock(|| {
            Err(OperationsError::Conflict(
                "temporary dispatch clock unavailable",
            ))
        }),
        Err(OperationsError::Conflict(
            "temporary dispatch clock unavailable"
        ))
    ));
}

#[test]
fn joined_target_without_ready_effect_binding_still_resolves_home_team()
-> Result<(), Box<dyn Error>> {
    let mut connection = home_identity_fixture()?;
    connection.pragma_update(None, "foreign_keys", true)?;
    schema::create_schema(&connection)?;
    add_home_member(
        &connection,
        TARGET_PARTICIPANT,
        0xA01,
        TARGET_PROFILE,
        HOME_TEAM,
    )?;
    let command = request(1, ACTOR_SESSION, "bounded request intent", 9_000_000)?;
    let actor = provenance(ACTOR_SESSION)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let home = resolve_target_home_team(
        &transaction,
        WorkspaceId::from_uuid(id(WORKSPACE)),
        id(REQUESTING_TEAM),
        id(TARGET_PARTICIPANT),
    )?;
    let outcome = quota::record_admission_attempt(
        &transaction,
        &command,
        &actor,
        home,
        timestamp(5_000_000)?,
    )?;
    transaction.commit()?;
    assert_eq!(home, id(HOME_TEAM));
    assert!(matches!(
        outcome,
        TemporaryDispatchAdmissionResult::Admitted(_)
    ));
    let admission_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(admission_count, 1);
    Ok(())
}

#[test]
fn legacy_profile_aliases_with_multiple_joined_homes_are_ambiguous() -> Result<(), Box<dyn Error>> {
    let connection = home_identity_fixture()?;
    add_home_member(
        &connection,
        TARGET_PARTICIPANT,
        0xA01,
        TARGET_PROFILE,
        HOME_TEAM,
    )?;
    add_participant_alias(&connection, 0xA10, 0xA01, TARGET_PROFILE)?;
    add_joined_membership(&connection, 0xA01, 0xA12)?;
    let transaction = connection.unchecked_transaction()?;
    let result = resolve_target_home_team(
        &transaction,
        WorkspaceId::from_uuid(id(WORKSPACE)),
        id(REQUESTING_TEAM),
        id(TARGET_PARTICIPANT),
    );
    let error = result
        .err()
        .ok_or_else(|| IoError::other("expected ambiguous home Team"))?;
    assert!(matches!(
        error,
        OperationsError::Conflict("temporary dispatch target has ambiguous home Team")
    ));
    transaction.rollback()?;
    Ok(())
}

#[test]
fn operations_catalog_admits_and_exactly_replays_without_target_binding()
-> Result<(), Box<dyn Error>> {
    let mut fixture = owner_fixture()?;
    // Keep the target's authoritative JOINED home membership but remove its
    // current effect binding. Availability and consent are later lifecycle checks.
    fixture.catalog.connection.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            fixture.target.participant.as_bytes(),
        ],
    )?;
    let request = owner_request(
        0xD001,
        fixture.workspace,
        fixture.receiving_team,
        fixture.leader.actor(),
        fixture.target.participant,
        1,
    )?;
    let clock_calls = Cell::new(0);
    let first = fixture.catalog.admit_temporary_dispatch(&request, || {
        clock_calls.set(clock_calls.get() + 1);
        timestamp(OWNER_NOW_MS)
    })?;
    let first_record = admitted_record(first)?;
    assert_eq!(first_record.request_id(), request.request_id());
    assert_eq!(first_record.workspace_id(), fixture.workspace);
    assert_eq!(first_record.requesting_team_id(), fixture.receiving_team);
    assert_eq!(first_record.target_home_team_id(), fixture.home_team);
    assert_eq!(
        first_record.target_participant_id(),
        fixture.target.participant
    );
    assert_eq!(
        first_record.actor().participant_id(),
        fixture.leader.participant
    );
    assert_eq!(
        first_record.actor().activity_id(),
        ActivityId::from_uuid(fixture.leader.activity)
    );
    assert_eq!(
        first_record.actor().agent_id(),
        AgentInstanceId::from_uuid(fixture.leader.agent)
    );
    assert_eq!(
        first_record.actor().session_id(),
        AgentSessionId::from_uuid(fixture.leader.session)
    );
    assert_eq!(
        first_record.actor().participation_id(),
        WorkspaceParticipationId::from_uuid(fixture.leader.participation)?
    );
    assert_eq!(first_record.actor().profile_id(), fixture.leader.profile);
    assert_eq!(
        first_record.requested_scope().as_str(),
        request.requested_scope().as_str()
    );
    assert_eq!(first_record.requested_end_at(), request.requested_end_at());
    assert_eq!(first_record.admitted_at(), timestamp(OWNER_NOW_MS)?);
    assert!(!first_record.replayed());
    assert_eq!(clock_calls.get(), 1);
    let after_first = owner_snapshot(
        &fixture.catalog.connection,
        fixture.workspace,
        fixture.receiving_team,
    )?;

    let replay = fixture.catalog.admit_temporary_dispatch(&request, || {
        clock_calls.set(clock_calls.get() + 1);
        timestamp(OWNER_NOW_MS + 1)
    })?;
    let replay_record = admitted_record(replay)?;
    assert!(replay_record.replayed());
    assert_eq!(replay_record.admitted_at(), timestamp(OWNER_NOW_MS)?);
    assert_eq!(clock_calls.get(), 1);
    assert_eq!(
        after_first,
        owner_snapshot(
            &fixture.catalog.connection,
            fixture.workspace,
            fixture.receiving_team,
        )?
    );
    Ok(())
}

#[test]
fn operations_catalog_conflicts_on_same_id_with_changed_payload_without_writes()
-> Result<(), Box<dyn Error>> {
    let mut fixture = owner_fixture()?;
    let request = owner_request(
        0xD005,
        fixture.workspace,
        fixture.receiving_team,
        fixture.leader.actor(),
        fixture.target.participant,
        1,
    )?;
    let _record = admitted_record(owner_admit_at(&mut fixture, &request, OWNER_NOW_MS)?)?;
    let before = owner_snapshot(
        &fixture.catalog.connection,
        fixture.workspace,
        fixture.receiving_team,
    )?;
    let conflicting_payload = TemporaryDispatchAdmissionRequest::new(
        request.request_id(),
        request.workspace_id(),
        request.requesting_team_id(),
        request.expected_leadership_revision(),
        request.actor(),
        request.target_participant_id(),
        TemporaryDispatchRequestedScope::new("different requested work intent".to_owned())?,
        request.requested_end_at(),
    )?;
    let clock_calls = Cell::new(0);
    let error = fixture
        .catalog
        .admit_temporary_dispatch(&conflicting_payload, || {
            clock_calls.set(clock_calls.get() + 1);
            timestamp(OWNER_NOW_MS + 2)
        })
        .err()
        .ok_or_else(|| IoError::other("expected same-ID changed-payload conflict"))?;
    assert!(matches!(
        error,
        OperationsError::Conflict("temporary dispatch request replay")
    ));
    assert_eq!(clock_calls.get(), 0);
    assert_eq!(
        before,
        owner_snapshot(
            &fixture.catalog.connection,
            fixture.workspace,
            fixture.receiving_team,
        )?
    );
    Ok(())
}

#[test]
fn operations_catalog_rejects_stale_foreign_and_unverified_requests_without_writes()
-> Result<(), Box<dyn Error>> {
    let mut fixture = owner_fixture()?;
    let stale = owner_request(
        0xD002,
        fixture.workspace,
        fixture.receiving_team,
        fixture.leader.actor(),
        fixture.target.participant,
        2,
    )?;
    assert_owner_rejection_is_read_only(
        &mut fixture,
        &stale,
        "requesting Team leadership changed",
    )?;

    let foreign_leader = owner_request(
        0xD003,
        fixture.workspace,
        fixture.receiving_team,
        fixture.home_leader.actor(),
        fixture.target.participant,
        1,
    )?;
    assert_owner_rejection_is_read_only(
        &mut fixture,
        &foreign_leader,
        "requesting Team leadership changed",
    )?;

    let unverified_workspace = owner_request(
        0xD004,
        WorkspaceId::from_uuid(id(0xDEAD)),
        fixture.receiving_team,
        fixture.leader.actor(),
        fixture.target.participant,
        1,
    )?;
    assert_owner_rejection_is_read_only(
        &mut fixture,
        &unverified_workspace,
        "team workspace import not verified",
    )?;
    Ok(())
}

#[test]
fn operations_catalog_quota_rejection_does_not_write_or_extend_lock() -> Result<(), Box<dyn Error>>
{
    let mut fixture = owner_fixture()?;
    for seed in 0xD100..0xD103 {
        let request = owner_request(
            seed,
            fixture.workspace,
            fixture.receiving_team,
            fixture.leader.actor(),
            fixture.target.participant,
            1,
        )?;
        let record = admitted_record(owner_admit_at(&mut fixture, &request, OWNER_NOW_MS)?)?;
        if seed == 0xD102 {
            assert_eq!(
                record.lock_until(),
                Some(timestamp(OWNER_NOW_MS + WINDOW_MS)?)
            );
        }
    }
    let before = owner_snapshot(
        &fixture.catalog.connection,
        fixture.workspace,
        fixture.receiving_team,
    )?;
    assert_eq!(before.receipt_ids.len(), 3);

    for seed in [0xD103, 0xD104] {
        let request = owner_request(
            seed,
            fixture.workspace,
            fixture.receiving_team,
            fixture.leader.actor(),
            fixture.target.participant,
            1,
        )?;
        match owner_admit_at(&mut fixture, &request, OWNER_NOW_MS)? {
            TemporaryDispatchAdmissionResult::QuotaRejected { retry_at, .. } => {
                assert_eq!(retry_at, timestamp(OWNER_NOW_MS + WINDOW_MS)?);
            }
            TemporaryDispatchAdmissionResult::Admitted(_) => {
                return Err(IoError::other("quota-limited owner request was admitted").into());
            }
        }
        assert_eq!(
            before,
            owner_snapshot(
                &fixture.catalog.connection,
                fixture.workspace,
                fixture.receiving_team,
            )?
        );
    }
    Ok(())
}

#[test]
fn operations_catalog_serializes_same_time_third_slot_contention() -> Result<(), Box<dyn Error>> {
    let database = TemporaryDatabase::new()?;
    std::fs::remove_file(&database.path)?;
    let mut fixture = owner_fixture_from_connection(crate::teams::tests::create_connection(
        Some(&database.path),
    )?)?;
    let now = OWNER_NOW_MS;
    for (seed, admitted_at) in [(0xD110, now - 2), (0xD111, now - 1)] {
        let request = owner_request(
            seed,
            fixture.workspace,
            fixture.receiving_team,
            fixture.leader.actor(),
            fixture.target.participant,
            1,
        )?;
        let _record = admitted_record(owner_admit_at(&mut fixture, &request, admitted_at)?)?;
    }
    let workspace = fixture.workspace;
    let requesting_team = fixture.receiving_team;
    let leader = fixture.leader;
    let target = fixture.target;
    drop(fixture);

    let mut connections = [
        crate::teams::tests::create_connection(Some(&database.path))?,
        crate::teams::tests::create_connection(Some(&database.path))?,
    ];
    for connection in &mut connections {
        connection.busy_timeout(Duration::from_secs(5))?;
    }
    let requests = [0xD112, 0xD113].map(|seed| {
        owner_request(
            seed,
            workspace,
            requesting_team,
            leader.actor(),
            target.participant,
            1,
        )
    });
    let requests = requests.into_iter().collect::<Result<Vec<_>, _>>()?;
    let barrier = Arc::new(Barrier::new(3));
    let workers = connections
        .into_iter()
        .zip(requests)
        .map(|(connection, request)| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let mut catalog = OperationsCatalog {
                    connection,
                    destination: id(OWNER_DESTINATION),
                };
                catalog
                    .admit_temporary_dispatch(&request, || timestamp(now))
                    .map_err(|error| error.to_string())
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let outcomes = workers
        .into_iter()
        .map(|worker| {
            worker
                .join()
                .map_err(|_| IoError::other("owner admission worker panicked"))?
                .map_err(IoError::other)
        })
        .collect::<Result<Vec<_>, IoError>>()?;
    let contention = owner_contention_outcomes(&outcomes)?;
    assert_eq!(contention.retry_at, timestamp(now + WINDOW_MS)?);

    let connection = Connection::open(&database.path)?;
    let snapshot = owner_snapshot(&connection, workspace, requesting_team)?;
    assert_eq!(snapshot.receipt_ids.len(), 3);
    let state = snapshot
        .requesting_team_state
        .ok_or_else(|| IoError::other("owner quota state missing after contention"))?;
    assert_eq!(state.last_sequence, 3);
    assert_eq!(state.locked_until_ms, Some(now + WINDOW_MS));
    assert_eq!(
        state.lock_request_id,
        Some(contention.admitted_request_id.as_bytes().to_vec())
    );
    Ok(())
}

#[test]
fn operations_catalog_allows_live_election_but_blocks_handover_incumbent()
-> Result<(), Box<dyn Error>> {
    let mut fixture = owner_fixture()?;
    let election_id = id(0xE100);
    seed_owner_election(&fixture, election_id)?;
    let authority = fixture
        .catalog
        .current_leadership_authority(LeadershipScope::team(
            fixture.workspace,
            fixture.receiving_team,
        ))?
        .ok_or_else(|| IoError::other("live election incumbent authority missing"))?;
    assert_eq!(authority.participant_id(), fixture.leader.participant);
    assert_eq!(authority.leadership_revision(), 1);

    let election_request = owner_request(
        0xD120,
        fixture.workspace,
        fixture.receiving_team,
        fixture.leader.actor(),
        fixture.target.participant,
        1,
    )?;
    let record = admitted_record(owner_admit_at(
        &mut fixture,
        &election_request,
        OWNER_NOW_MS,
    )?)?;
    assert_eq!(record.request_id(), election_request.request_id());

    close_owner_election_as_handover_pending(&fixture, election_id)?;
    let pending_authority = fixture
        .catalog
        .current_leadership_authority(LeadershipScope::team(
            fixture.workspace,
            fixture.receiving_team,
        ))?
        .ok_or_else(|| IoError::other("handover incumbent authority observation missing"))?;
    assert_eq!(
        pending_authority.participant_id(),
        fixture.leader.participant
    );
    assert_eq!(pending_authority.leadership_revision(), 2);
    let denied = owner_request(
        0xD121,
        fixture.workspace,
        fixture.receiving_team,
        fixture.leader.actor(),
        fixture.target.participant,
        2,
    )?;
    assert_owner_rejection_is_read_only(
        &mut fixture,
        &denied,
        "requesting Team Leader is not active for new requests",
    )?;
    Ok(())
}

fn owner_fixture() -> Result<OwnerFixture, Box<dyn Error>> {
    let connection = crate::teams::tests::create_connection(None)?;
    owner_fixture_from_connection(connection)
}

fn owner_fixture_from_connection(connection: Connection) -> Result<OwnerFixture, Box<dyn Error>> {
    let workspace = WorkspaceId::from_uuid(id(0x1000));
    let leader = owner_participant(0);
    let receiving_peer = owner_participant(0x100);
    let target = owner_participant(0x200);
    let home_leader = owner_participant(0x300);
    crate::election_subject::create_schema(&connection)?;
    schema::create_schema(&connection)?;
    crate::teams::tests::ensure_team_expansion_schema(&connection)?;
    crate::teams::tests::ensure_test_workspace_orchestrator(&connection)?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        workspace,
        home_leader.profile,
        home_leader.participation,
        home_leader.participant,
        home_leader.activity,
        home_leader.agent,
        home_leader.session,
        id(0xB300),
    )?;
    let mut catalog = OperationsCatalog {
        connection,
        destination: id(OWNER_DESTINATION),
    };
    create_owner_team(
        &mut catalog,
        workspace,
        OWNER_RECEIVING_TEAM,
        0xD001,
        "Receiving Team",
        &[leader, receiving_peer],
    )?;
    create_owner_team(
        &mut catalog,
        workspace,
        OWNER_HOME_TEAM,
        0xD002,
        "Home Team",
        &[target, home_leader],
    )?;

    // The current leadership SQL predicate prepares a join to the election
    // subject cutover table even when no election is active, so the fixture
    // creates that production-owned schema above. Synthetic native-test facts:
    // the Team helper seeds a Verified import/current identity bindings, while
    // these rows model confirmed Team leadership only. They are not Forge/
    // Broker confirmation evidence or proof of HTTP/Runtime authorization.
    seed_owner_leadership(&catalog.connection, workspace, OWNER_RECEIVING_TEAM, leader)?;
    seed_owner_leadership(&catalog.connection, workspace, OWNER_HOME_TEAM, home_leader)?;
    Ok(OwnerFixture {
        catalog,
        workspace,
        receiving_team: id(OWNER_RECEIVING_TEAM),
        home_team: id(OWNER_HOME_TEAM),
        leader,
        receiving_peer,
        target,
        home_leader,
    })
}

fn owner_participant(block: u128) -> OwnerParticipant {
    OwnerParticipant {
        profile: id(0x3000 + block),
        participation: id(0x4000 + block),
        participant: id(0x5000 + block),
        activity: id(0x6000 + block),
        agent: id(0x7000 + block),
        session: id(0x8000 + block),
    }
}

fn create_owner_team(
    catalog: &mut OperationsCatalog,
    workspace: WorkspaceId,
    team_id: u128,
    request_id: u128,
    name: &str,
    members: &[OwnerParticipant],
) -> Result<(), Box<dyn Error>> {
    let _record = catalog.create_team_with_capacity(
        TeamCreateCommand {
            request_id: id(request_id),
            team_id: id(team_id),
            workspace_id: workspace,
            actor: OperationalActor::Participant {
                activity_id: ActivityId::from_uuid(id(0x6000)),
                agent_id: AgentInstanceId::from_uuid(id(0x7000)),
                session_id: AgentSessionId::from_uuid(id(0x8000)),
            },
            expected_sender_leadership_revision: Some(1),
            name: name.to_owned(),
            purpose: "Synthetic native owner fixture".to_owned(),
            initial_members: members
                .iter()
                .map(|member| member.roster_member("member"))
                .collect(),
            observed_at: timestamp(100)?,
        },
        crate::team_expansion::TeamMemberCapacity::new(8)?,
    )?;
    Ok(())
}

fn seed_owner_leadership(
    connection: &Connection,
    workspace: WorkspaceId,
    team: u128,
    leader: OwnerParticipant,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,2,NULL,?2,'STABILIZING',?3,1,1200100,NULL,100)",
        params![
            workspace.into_uuid().as_bytes(),
            id(team).as_bytes(),
            leader.participant.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,1,'CONFIRMED',NULL,NULL,?3,1,100)",
        params![
            workspace.into_uuid().as_bytes(),
            id(team).as_bytes(),
            leader.participant.as_bytes(),
        ],
    )?;
    Ok(())
}

fn owner_request(
    request_seed: u128,
    workspace: WorkspaceId,
    requesting_team: Uuid,
    actor: OperationalActor,
    target: Uuid,
    expected_leadership_revision: u64,
) -> Result<TemporaryDispatchAdmissionRequest, OperationsError> {
    TemporaryDispatchAdmissionRequest::new(
        id(request_seed),
        workspace,
        requesting_team,
        expected_leadership_revision,
        actor,
        target,
        TemporaryDispatchRequestedScope::new("bounded requested work intent".to_owned())?,
        timestamp(10_000_000)?,
    )
}

fn owner_admit_at(
    fixture: &mut OwnerFixture,
    request: &TemporaryDispatchAdmissionRequest,
    now_ms: i64,
) -> Result<TemporaryDispatchAdmissionResult, OperationsError> {
    fixture
        .catalog
        .admit_temporary_dispatch(request, || timestamp(now_ms))
}

fn admitted_record(
    result: TemporaryDispatchAdmissionResult,
) -> Result<Box<TemporaryDispatchRequestRecord>, IoError> {
    match result {
        TemporaryDispatchAdmissionResult::Admitted(record) => Ok(record),
        TemporaryDispatchAdmissionResult::QuotaRejected { .. } => {
            Err(IoError::other("owner fixture expected an admitted request"))
        }
    }
}

fn owner_contention_outcomes(
    outcomes: &[TemporaryDispatchAdmissionResult],
) -> Result<OwnerContentionSummary, IoError> {
    let mut admitted_request_id = None;
    let mut retry_at = None;
    for outcome in outcomes {
        match outcome {
            TemporaryDispatchAdmissionResult::Admitted(record) if admitted_request_id.is_none() => {
                admitted_request_id = Some(record.request_id());
            }
            TemporaryDispatchAdmissionResult::QuotaRejected {
                retry_at: value, ..
            } if retry_at.is_none() => {
                retry_at = Some(*value);
            }
            _ => {
                return Err(IoError::other(
                    "contention did not produce one admission and one rejection",
                ));
            }
        }
    }
    Ok(OwnerContentionSummary {
        admitted_request_id: admitted_request_id
            .ok_or_else(|| IoError::other("contention admission missing"))?,
        retry_at: retry_at.ok_or_else(|| IoError::other("contention rejection missing"))?,
    })
}

fn assert_owner_rejection_is_read_only(
    fixture: &mut OwnerFixture,
    request: &TemporaryDispatchAdmissionRequest,
    expected_conflict: &'static str,
) -> Result<(), Box<dyn Error>> {
    let before = owner_snapshot(
        &fixture.catalog.connection,
        fixture.workspace,
        fixture.receiving_team,
    )?;
    let clock_calls = Cell::new(0);
    let error = fixture
        .catalog
        .admit_temporary_dispatch(request, || {
            clock_calls.set(clock_calls.get() + 1);
            timestamp(5_000_000)
        })
        .err()
        .ok_or_else(|| IoError::other("expected owner admission rejection"))?;
    assert!(matches!(
        error,
        OperationsError::Conflict(message) if message == expected_conflict
    ));
    assert_eq!(clock_calls.get(), 0);
    assert_eq!(
        before,
        owner_snapshot(
            &fixture.catalog.connection,
            fixture.workspace,
            fixture.receiving_team,
        )?
    );
    Ok(())
}

fn owner_snapshot(
    connection: &Connection,
    workspace: WorkspaceId,
    requesting_team: Uuid,
) -> Result<OwnerMutationSnapshot, rusqlite::Error> {
    let receipt_ids = {
        let mut statement = connection.prepare(
            "SELECT request_id FROM ops_temporary_dispatch_requests WHERE workspace_id=?1 AND requesting_team_id=?2 ORDER BY request_id",
        )?;
        statement
            .query_map(
                params![workspace.into_uuid().as_bytes(), requesting_team.as_bytes(),],
                |row| row.get::<_, Vec<u8>>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?
    };
    let quota_state_count = connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_quota_state",
        [],
        |row| row.get(0),
    )?;
    let requesting_team_state = connection
        .query_row(
            "SELECT last_admitted_at_ms,last_sequence,recent_1_request_id,recent_1_sequence,recent_1_admitted_at_ms,recent_2_request_id,recent_2_sequence,recent_2_admitted_at_ms,recent_3_request_id,recent_3_sequence,recent_3_admitted_at_ms,locked_until_ms,lock_request_id FROM ops_temporary_dispatch_quota_state WHERE workspace_id=?1 AND requesting_team_id=?2",
            params![
                workspace.into_uuid().as_bytes(),
                requesting_team.as_bytes(),
            ],
            |row| {
                Ok(OwnerQuotaStateSnapshot {
                    last_admitted_at_ms: row.get(0)?,
                    last_sequence: row.get(1)?,
                    recent_1_request_id: row.get(2)?,
                    recent_1_sequence: row.get(3)?,
                    recent_1_admitted_at_ms: row.get(4)?,
                    recent_2_request_id: row.get(5)?,
                    recent_2_sequence: row.get(6)?,
                    recent_2_admitted_at_ms: row.get(7)?,
                    recent_3_request_id: row.get(8)?,
                    recent_3_sequence: row.get(9)?,
                    recent_3_admitted_at_ms: row.get(10)?,
                    locked_until_ms: row.get(11)?,
                    lock_request_id: row.get(12)?,
                })
            },
        )
        .optional()?;
    Ok(OwnerMutationSnapshot {
        receipt_ids,
        quota_state_count,
        requesting_team_state,
    })
}

fn seed_owner_election(fixture: &OwnerFixture, election_id: Uuid) -> Result<(), Box<dyn Error>> {
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,2,NULL,?3,'RECALL','ELECTING',?4,?5,6000000,NULL,NULL,NULL,NULL,100,NULL)",
        params![
            election_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            fixture.receiving_team.as_bytes(),
            fixture.leader.participant.as_bytes(),
            fixture.receiving_peer.participant.as_bytes(),
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_election_subject_cutovers(election_id,workspace_id,scope_kind,activity_id,team_id,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) VALUES(?1,?2,2,NULL,?3,1,2,1,2,2,?4,100)",
        params![
            election_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            fixture.receiving_team.as_bytes(),
            [0x5A_u8; 32].as_slice(),
        ],
    )?;
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership SET state='ELECTING',current_election_id=?1 WHERE workspace_id=?2 AND scope_kind=2 AND team_id=?3",
        params![
            election_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            fixture.receiving_team.as_bytes(),
        ],
    )?;
    Ok(())
}

fn close_owner_election_as_handover_pending(
    fixture: &OwnerFixture,
    election_id: Uuid,
) -> Result<(), Box<dyn Error>> {
    let election_rows = fixture.catalog.connection.execute(
        "UPDATE ops_leadership_elections SET state='CLOSED_ELECTED',elected_participant_id=?1,tally_yes=2,tally_no=0,tally_abstain=0,closed_at_ms=200 WHERE election_id=?2 AND state='ELECTING'",
        params![fixture.receiving_peer.participant.as_bytes(), election_id.as_bytes()],
    )?;
    if election_rows != 1 {
        return Err(IoError::other("synthetic live election row missing").into());
    }
    let leadership_rows = fixture.catalog.connection.execute(
        "UPDATE ops_leadership SET state='AUTHORITY_PENDING',leadership_revision=2,updated_at_ms=200 WHERE workspace_id=?1 AND scope_kind=2 AND team_id=?2 AND current_election_id=?3",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            fixture.receiving_team.as_bytes(),
            election_id.as_bytes(),
        ],
    )?;
    if leadership_rows != 1 {
        return Err(IoError::other("synthetic pending leadership row missing").into());
    }
    Ok(())
}

fn in_memory_catalog() -> Result<Connection, Box<dyn Error>> {
    let connection = Connection::open_in_memory()?;
    connection.pragma_update(None, "foreign_keys", true)?;
    schema::create_schema(&connection)?;
    Ok(connection)
}

fn home_identity_fixture() -> Result<Connection, Box<dyn Error>> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE ops_workspace_participation_mappings(workspace_id BLOB,participant_id BLOB,participation_id BLOB,agent_profile_id BLOB);
         CREATE TABLE ops_workspace_participations(workspace_id BLOB,participation_id BLOB,agent_profile_id BLOB);
         CREATE TABLE ops_team_members(workspace_id BLOB,team_id BLOB,participation_id BLOB,state TEXT);
         CREATE TABLE ops_leadership(workspace_id BLOB,leader_participant_id BLOB);",
    )?;
    Ok(connection)
}

fn add_home_member(
    connection: &Connection,
    participant_seed: u128,
    participation_seed: u128,
    profile_seed: u128,
    team_seed: u128,
) -> Result<(), Box<dyn Error>> {
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(workspace_id,participant_id,participation_id,agent_profile_id) VALUES(?1,?2,?3,?4)",
        params![
            id(WORKSPACE).as_bytes(),
            id(participant_seed).as_bytes(),
            id(participation_seed).as_bytes(),
            id(profile_seed).as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,participation_id,agent_profile_id) VALUES(?1,?2,?3)",
        params![
            id(WORKSPACE).as_bytes(),
            id(participation_seed).as_bytes(),
            id(profile_seed).as_bytes(),
        ],
    )?;
    add_joined_membership(connection, participation_seed, team_seed)
}

fn add_participant_alias(
    connection: &Connection,
    participant_seed: u128,
    participation_seed: u128,
    profile_seed: u128,
) -> Result<(), Box<dyn Error>> {
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(workspace_id,participant_id,participation_id,agent_profile_id) VALUES(?1,?2,?3,?4)",
        params![
            id(WORKSPACE).as_bytes(),
            id(participant_seed).as_bytes(),
            id(participation_seed).as_bytes(),
            id(profile_seed).as_bytes(),
        ],
    )?;
    Ok(())
}

fn add_joined_membership(
    connection: &Connection,
    participation_seed: u128,
    team_seed: u128,
) -> Result<(), Box<dyn Error>> {
    connection.execute(
        "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state) VALUES(?1,?2,?3,'JOINED')",
        params![
            id(WORKSPACE).as_bytes(),
            id(team_seed).as_bytes(),
            id(participation_seed).as_bytes(),
        ],
    )?;
    Ok(())
}

fn admit(
    connection: &mut Connection,
    request: &TemporaryDispatchAdmissionRequest,
    now_ms: i64,
) -> Result<TemporaryDispatchRequestRecord, OperationsError> {
    match attempt(connection, request, now_ms)? {
        TemporaryDispatchAdmissionResult::Admitted(record) => Ok(*record),
        TemporaryDispatchAdmissionResult::QuotaRejected { .. } => Err(OperationsError::Conflict(
            "test expected admitted temporary dispatch request",
        )),
    }
}

fn attempt(
    connection: &mut Connection,
    request: &TemporaryDispatchAdmissionRequest,
    now_ms: i64,
) -> Result<TemporaryDispatchAdmissionResult, OperationsError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let actor = provenance_for_request(request)?;
    let outcome = quota::record_admission_attempt(
        &transaction,
        request,
        &actor,
        id(HOME_TEAM),
        timestamp(now_ms)?,
    )?;
    match outcome {
        TemporaryDispatchAdmissionResult::Admitted(record) => {
            transaction.commit()?;
            Ok(TemporaryDispatchAdmissionResult::Admitted(record))
        }
        rejected @ TemporaryDispatchAdmissionResult::QuotaRejected { .. } => {
            transaction.rollback()?;
            Ok(rejected)
        }
    }
}

fn request(
    seed: u128,
    session_seed: u128,
    scope: &str,
    end_at_ms: i64,
) -> Result<TemporaryDispatchAdmissionRequest, OperationsError> {
    let actor = OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(id(session_seed + 0x10)),
        agent_id: AgentInstanceId::from_uuid(id(session_seed + 0x20)),
        session_id: session(session_seed),
    };
    TemporaryDispatchAdmissionRequest::new(
        id(seed),
        WorkspaceId::from_uuid(id(WORKSPACE)),
        id(REQUESTING_TEAM),
        7,
        actor,
        id(TARGET_PARTICIPANT),
        TemporaryDispatchRequestedScope::new(scope.to_owned())?,
        timestamp(end_at_ms)?,
    )
}

fn provenance(session_seed: u128) -> Result<TemporaryDispatchActorProvenance, OperationsError> {
    Ok(TemporaryDispatchActorProvenance {
        activity_id: ActivityId::from_uuid(id(session_seed + 0x10)),
        agent_id: AgentInstanceId::from_uuid(id(session_seed + 0x20)),
        session_id: session(session_seed),
        participation_id: WorkspaceParticipationId::from_uuid(id(ACTOR_PARTICIPATION))?,
        profile_id: id(ACTOR_PROFILE),
        participant_id: id(ACTOR_PARTICIPANT),
        display_name: "Current Team Leader".to_owned(),
    })
}

fn provenance_for_request(
    request: &TemporaryDispatchAdmissionRequest,
) -> Result<TemporaryDispatchActorProvenance, OperationsError> {
    match request.actor() {
        OperationalActor::Participant { session_id, .. } => {
            provenance(session_id.into_uuid().as_u128())
        }
        OperationalActor::LocalOperator => Err(OperationsError::Conflict(
            "test request actor must be a participant",
        )),
    }
}

fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Bounds("test timestamp"))
}

fn session(value: u128) -> AgentSessionId {
    AgentSessionId::from_uuid(id(value))
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

struct TemporaryDatabase {
    path: PathBuf,
}

impl TemporaryDatabase {
    fn new() -> Result<Self, IoError> {
        for _ in 0..64 {
            let sequence = TEMPORARY_DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "temporary-dispatch-{}-{sequence}.sqlite",
                std::process::id(),
            ));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(_file) => return Ok(Self { path }),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(IoError::new(
            ErrorKind::AlreadyExists,
            "could not allocate a unique temporary dispatch database path",
        ))
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.path.as_os_str().to_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(path));
        }
    }
}
