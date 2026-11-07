use super::*;
use crate::{BindingActivationObservation, BindingReservationObservation, RegistrationRequest};
use forge_domain::{
    AgentDeclaredRole, AgentDisplayName, AgentTransportKind, AuthenticatedPrincipal,
};
use forge_persistence::AgentBindingReservationRequest;
use std::{
    error::Error,
    fs,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

const DESTINATION_ID: Uuid = Uuid::from_u128(0x9000);
const WORKSPACE_ID: Uuid = Uuid::from_u128(0xA510);
const WORKSPACE_ID_TWO: Uuid = Uuid::from_u128(0xA530);
const PROFILE_ID: Uuid = Uuid::from_u128(0xA511);
const PARTICIPATION_ID: Uuid = Uuid::from_u128(0xA512);
const PARTICIPANT_ID: Uuid = Uuid::from_u128(0xA513);
const ACTIVITY_ID: Uuid = Uuid::from_u128(0xA514);
const AGENT_ID: Uuid = Uuid::from_u128(0xA515);
const SESSION_ID: Uuid = Uuid::from_u128(0xA516);
const OTHER_PROFILE_ID: Uuid = Uuid::from_u128(0xA521);
const OTHER_PARTICIPATION_ID: Uuid = Uuid::from_u128(0xA522);
const OTHER_PARTICIPANT_ID: Uuid = Uuid::from_u128(0xA523);
const OTHER_ACTIVITY_ID: Uuid = Uuid::from_u128(0xA524);
const OTHER_AGENT_ID: Uuid = Uuid::from_u128(0xA525);
const OTHER_SESSION_ID: Uuid = Uuid::from_u128(0xA526);
const SECOND_PROFILE_ID: Uuid = Uuid::from_u128(0xA531);
const SECOND_PARTICIPATION_ID: Uuid = Uuid::from_u128(0xA532);
const SECOND_PARTICIPANT_ID: Uuid = Uuid::from_u128(0xA533);
const SECOND_ACTIVITY_ID: Uuid = Uuid::from_u128(0xA534);
const SECOND_AGENT_ID: Uuid = Uuid::from_u128(0xA535);
const SECOND_SESSION_ID: Uuid = Uuid::from_u128(0xA536);
static DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

type TestResult = Result<(), Box<dyn Error>>;

struct TemporaryDatabase {
    path: PathBuf,
}

impl TemporaryDatabase {
    fn new() -> Result<Self, IoError> {
        for _ in 0..64 {
            let sequence = DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "reviewer-participation-{}-{sequence}.sqlite",
                std::process::id(),
            ));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    drop(file);
                    fs::remove_file(&path)?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(IoError::new(
            ErrorKind::AlreadyExists,
            "could not allocate Reviewer participation fixture database",
        ))
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.path.as_os_str().to_os_string();
            path.push(suffix);
            let _ = fs::remove_file(PathBuf::from(path));
        }
    }
}

fn workspace() -> WorkspaceId {
    WorkspaceId::from_uuid(WORKSPACE_ID)
}

fn actor(activity_id: Uuid, agent_id: Uuid, session_id: Uuid) -> OperationalActor {
    OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(activity_id),
        agent_id: AgentInstanceId::from_uuid(agent_id),
        session_id: AgentSessionId::from_uuid(session_id),
    }
}

fn owner() -> OperationalActor {
    actor(ACTIVITY_ID, AGENT_ID, SESSION_ID)
}

fn other_owner() -> OperationalActor {
    actor(OTHER_ACTIVITY_ID, OTHER_AGENT_ID, OTHER_SESSION_ID)
}

fn second_workspace() -> WorkspaceId {
    WorkspaceId::from_uuid(WORKSPACE_ID_TWO)
}

fn command(
    request: u128,
    expected_revision: u64,
    statement: ReviewerParticipationStatement,
) -> Result<ReviewerParticipationCommand, OperationsError> {
    ReviewerParticipationCommand::new(
        workspace(),
        Uuid::from_u128(request),
        expected_revision,
        owner(),
        statement,
    )
}

fn fixture(path: Option<&Path>) -> Result<OperationsCatalog, Box<dyn Error>> {
    let path = path.map(Path::to_path_buf);
    let connection = crate::teams::tests::create_connection(path.as_ref())?;
    connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,phase) VALUES(?1,?2,?3,2)",
        rusqlite::params![
            Uuid::from_u128(0xA530).as_bytes(),
            WORKSPACE_ID.as_bytes(),
            DESTINATION_ID.as_bytes(),
        ],
    )?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        workspace(),
        PROFILE_ID,
        PARTICIPATION_ID,
        PARTICIPANT_ID,
        ACTIVITY_ID,
        AGENT_ID,
        SESSION_ID,
        Uuid::from_u128(0xA531),
    )?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        workspace(),
        OTHER_PROFILE_ID,
        OTHER_PARTICIPATION_ID,
        OTHER_PARTICIPANT_ID,
        OTHER_ACTIVITY_ID,
        OTHER_AGENT_ID,
        OTHER_SESSION_ID,
        Uuid::from_u128(0xA541),
    )?;
    let transaction = connection.unchecked_transaction()?;
    schema::create_schema(&transaction)?;
    transaction.commit()?;
    Ok(OperationsCatalog {
        connection,
        destination: DESTINATION_ID,
    })
}

#[test]
fn interest_and_exact_replay_preserve_original_actor_session() -> TestResult {
    let mut catalog = fixture(None)?;
    let initial = command(0xA550, 0, ReviewerParticipationStatement::Interested)?;
    let first = catalog.record_reviewer_participation(&initial)?;
    assert_eq!(first.revision(), 1);
    assert_eq!(
        first.statement(),
        ReviewerParticipationStatement::Interested
    );
    assert_eq!(first.session_id(), AgentSessionId::from_uuid(SESSION_ID));
    assert!(!first.replayed());

    let replay = catalog.record_reviewer_participation(&initial)?;
    assert!(replay.replayed());
    assert_eq!(
        replay.request_id(),
        first.request_id(),
        "exact retry must return the original receipt",
    );

    let page = catalog.reviewer_participation_history(workspace(), owner(), None, 8)?;
    assert_eq!(page.records().len(), 1);
    assert_eq!(page.records()[0], first);
    assert_eq!(page.records()[0].session_id(), first.session_id());
    Ok(())
}

#[test]
fn simulated_unknown_commit_recovers_exact_receipt_without_a_second_write() -> TestResult {
    let mut catalog = fixture(None)?;
    let command = command(0xA550, 0, ReviewerParticipationStatement::Interested)?;
    let original = catalog.record_reviewer_participation(&command)?;
    let actor = resolve_profile_actor(&catalog.connection, workspace(), owner())?;
    let rows_before = count_receipts(&catalog)?;
    let readbacks = std::cell::Cell::new(0_u8);

    // Simulates only the post-commit reconciliation branch. The receipt is a
    // real durable owner write and the exact request readback uses the real
    // Operations query. SQLite commit failure itself is not injected here.
    let recovered = finalize_commit(
        Err(rusqlite::Error::ExecuteReturnedResults),
        original.clone(),
        workspace(),
        original.request_id(),
        || {
            readbacks.set(readbacks.get() + 1);
            catalog.reviewer_participation_request(workspace(), original.request_id(), owner())
        },
        |receipt| receipt_matches_command(receipt, &command, &actor),
    )?;
    assert_eq!(readbacks.get(), 1);
    assert!(recovered.replayed());
    assert_eq!(recovered.request_id(), original.request_id());
    assert_eq!(recovered.session_id(), original.session_id());
    assert_eq!(count_receipts(&catalog)?, rows_before);
    Ok(())
}

#[test]
fn unavailable_or_missing_unknown_commit_readback_remains_unconfirmed_without_rewrite() -> TestResult
{
    let catalog = fixture(None)?;
    let command = command(0xA590, 0, ReviewerParticipationStatement::Interested)?;
    let actor = resolve_profile_actor(&catalog.connection, workspace(), owner())?;
    let fresh = receipt_from_actor(&command, &actor, 1, TimestampMillis::new(50)?);
    let rows_before = count_receipts(&catalog)?;

    for (request_id, readback) in [
        (command.request_id(), Ok(None)),
        (
            Uuid::from_u128(0xA591),
            Err(OperationsError::Conflict("simulated readback failure")),
        ),
    ] {
        let no_receipt = ReviewerParticipationReceipt {
            request_id,
            ..fresh.clone()
        };
        let result = finalize_commit(
            Err(rusqlite::Error::ExecuteReturnedResults),
            no_receipt,
            workspace(),
            request_id,
            || readback,
            |_| false,
        );
        let Err(error) = result else {
            return Err("unknown commit without exact readback was confirmed".into());
        };
        assert!(matches!(
            error,
            OperationsError::OperationCommitUnconfirmed {
                workspace_id,
                request_id: observed_request,
                ..
            } if workspace_id == workspace() && observed_request == request_id
        ));
    }
    assert_eq!(count_receipts(&catalog)?, rows_before);
    assert_eq!(count_heads(&catalog)?, 0);
    Ok(())
}

#[test]
fn concurrent_same_revision_interest_commands_have_one_durable_winner() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let mut catalog = fixture(Some(&database.path))?;
    let first_interest = catalog.record_reviewer_participation(&command(
        0xA550,
        0,
        ReviewerParticipationStatement::Interested,
    )?)?;
    let withdrawal = catalog.record_reviewer_participation(&command(
        0xA553,
        1,
        ReviewerParticipationStatement::WithdrawInterest {
            withdraws_request_id: first_interest.request_id(),
            withdraws_revision: first_interest.revision(),
        },
    )?)?;
    assert_eq!(withdrawal.revision(), 2);
    drop(catalog);

    // Both requests are valid against this exact WithdrawInterest snapshot.
    // Identity and verified Workspace rows are synthetic Operations fixtures.
    let first_connection = crate::teams::tests::create_connection(Some(&database.path))?;
    first_connection.busy_timeout(Duration::from_secs(5))?;
    let second_connection = crate::teams::tests::create_connection(Some(&database.path))?;
    second_connection.busy_timeout(Duration::from_secs(5))?;
    let first_catalog = OperationsCatalog {
        connection: first_connection,
        destination: DESTINATION_ID,
    };
    let second_catalog = OperationsCatalog {
        connection: second_connection,
        destination: DESTINATION_ID,
    };
    let first_request = Uuid::from_u128(0xA592);
    let second_request = Uuid::from_u128(0xA593);
    let first_command = command(
        first_request.as_u128(),
        2,
        ReviewerParticipationStatement::Interested,
    )?;
    let second_command = command(
        second_request.as_u128(),
        2,
        ReviewerParticipationStatement::Interested,
    )?;
    let barrier = Arc::new(Barrier::new(3));
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let first_worker = thread::spawn(move || {
        run_interested_contender(first_catalog, &first_barrier, &first_command)
    });
    let second_worker = thread::spawn(move || {
        run_interested_contender(second_catalog, &second_barrier, &second_command)
    });
    barrier.wait();

    let first_result = first_worker
        .join()
        .map_err(|_| IoError::other("Reviewer participation first writer panicked"))?;
    let second_result = second_worker
        .join()
        .map_err(|_| IoError::other("Reviewer participation second writer panicked"))?;
    let (winner, losing_request_id) = match (first_result, second_result) {
        (Ok(winner), Err(OperationsError::Conflict(_))) => (winner, second_request),
        (Err(OperationsError::Conflict(_)), Ok(winner)) => (winner, first_request),
        (Err(_), _) | (_, Err(_)) => {
            return Err("a contender failed for a reason other than the CAS conflict".into());
        }
        (Ok(_), Ok(_)) => return Err("both same-revision contenders committed".into()),
    };
    assert_eq!(winner.revision(), 3);
    assert_eq!(winner.expected_revision(), 2);
    assert_eq!(
        winner.statement(),
        ReviewerParticipationStatement::Interested
    );
    assert!(winner.request_id() == first_request || winner.request_id() == second_request);

    let reader = fixture_reopen(&database.path)?;
    assert_eq!(count_receipts(&reader)?, 3);
    assert_eq!(count_heads(&reader)?, 1);
    assert!(
        reader
            .reviewer_participation_request(workspace(), losing_request_id, owner())?
            .is_none()
    );
    let winner_readback = reader
        .reviewer_participation_request(workspace(), winner.request_id(), owner())?
        .ok_or("winning Reviewer interest receipt disappeared")?;
    assert_eq!(winner_readback, winner);
    let history = reader.reviewer_participation_history(workspace(), owner(), None, 8)?;
    assert_eq!(history.records().len(), 3);
    assert_eq!(
        history.records()[0].request_id(),
        first_interest.request_id()
    );
    assert_eq!(history.records()[1].request_id(), withdrawal.request_id());
    assert_eq!(history.records()[2].request_id(), winner.request_id());
    assert_eq!(history.records()[2].revision(), 3);
    Ok(())
}

fn run_interested_contender(
    mut catalog: OperationsCatalog,
    barrier: &Barrier,
    command: &ReviewerParticipationCommand,
) -> Result<ReviewerParticipationReceipt, OperationsError> {
    barrier.wait();
    catalog.record_reviewer_participation(command)
}

#[test]
fn withdrawal_must_name_the_exact_current_interest_and_preserves_both_receipts() -> TestResult {
    let mut catalog = fixture(None)?;
    let interest = catalog.record_reviewer_participation(&command(
        0xA550,
        0,
        ReviewerParticipationStatement::Interested,
    )?)?;
    let wrong_target = command(
        0xA551,
        1,
        ReviewerParticipationStatement::WithdrawInterest {
            withdraws_request_id: Uuid::from_u128(0xA552),
            withdraws_revision: 1,
        },
    )?;
    assert!(
        catalog
            .record_reviewer_participation(&wrong_target)
            .is_err()
    );
    assert_eq!(count_receipts(&catalog)?, 1);

    let withdrawal = catalog.record_reviewer_participation(&command(
        0xA553,
        1,
        ReviewerParticipationStatement::WithdrawInterest {
            withdraws_request_id: interest.request_id(),
            withdraws_revision: interest.revision(),
        },
    )?)?;
    assert_eq!(withdrawal.revision(), 2);
    assert_eq!(
        withdrawal.statement(),
        ReviewerParticipationStatement::WithdrawInterest {
            withdraws_request_id: interest.request_id(),
            withdraws_revision: interest.revision(),
        },
    );
    assert_eq!(count_receipts(&catalog)?, 2);
    let page = catalog.reviewer_participation_history(workspace(), owner(), None, 8)?;
    assert_eq!(page.records().len(), 2);
    assert_eq!(
        page.records()[0].statement(),
        ReviewerParticipationStatement::Interested
    );
    assert_eq!(page.records()[1].statement(), withdrawal.statement());
    Ok(())
}

#[test]
fn stale_request_reuse_and_other_participant_reads_fail_without_disclosure() -> TestResult {
    let mut catalog = fixture(None)?;
    let first = catalog.record_reviewer_participation(&command(
        0xA550,
        0,
        ReviewerParticipationStatement::Interested,
    )?)?;
    let reused = ReviewerParticipationCommand::new(
        workspace(),
        first.request_id(),
        1,
        owner(),
        ReviewerParticipationStatement::WithdrawInterest {
            withdraws_request_id: first.request_id(),
            withdraws_revision: 1,
        },
    )?;
    assert!(catalog.record_reviewer_participation(&reused).is_err());
    assert!(
        catalog
            .reviewer_participation_request(workspace(), first.request_id(), other_owner())?
            .is_none()
    );
    assert_eq!(count_receipts(&catalog)?, 1);
    Ok(())
}

#[test]
fn stale_revision_and_non_participant_authority_cannot_create_a_receipt() -> TestResult {
    let mut catalog = fixture(None)?;
    catalog.record_reviewer_participation(&command(
        0xA550,
        0,
        ReviewerParticipationStatement::Interested,
    )?)?;
    let stale = command(0xA554, 0, ReviewerParticipationStatement::Interested)?;
    assert!(catalog.record_reviewer_participation(&stale).is_err());
    assert!(
        ReviewerParticipationCommand::new(
            workspace(),
            Uuid::from_u128(0xA555),
            0,
            OperationalActor::LocalOperator,
            ReviewerParticipationStatement::Interested,
        )
        .is_err()
    );
    assert_eq!(count_receipts(&catalog)?, 1);
    Ok(())
}

#[test]
fn local_operator_cannot_create_interest_or_head() -> TestResult {
    let catalog = fixture(None)?;
    let command = ReviewerParticipationCommand::new(
        workspace(),
        Uuid::from_u128(0xA580),
        0,
        OperationalActor::LocalOperator,
        ReviewerParticipationStatement::Interested,
    );
    assert!(command.is_err());
    assert_eq!(count_receipts(&catalog)?, 0);
    assert_eq!(count_heads(&catalog)?, 0);
    Ok(())
}

#[test]
fn ready_native_participant_cannot_create_interest_or_head() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let mut catalog = full_fixture(&database.path)?;
    let migration_id = Uuid::from_u128(0xA587);
    let session_id = AgentSessionId::from_uuid(Uuid::from_u128(0xA582));
    let activity_id = ActivityId::from_uuid(Uuid::from_u128(0xA583));
    let agent_id = AgentInstanceId::from_uuid(Uuid::from_u128(0xA584));
    let request_id = Uuid::from_u128(0xA585);
    let request =
        native_registration_request(workspace(), request_id, activity_id, agent_id, session_id)?;
    let participant_id = request.proposed_participant_id();
    let _pending = catalog.prepare_registration(migration_id, &request)?;
    let reservation = BindingReservationObservation::new(
        migration_id,
        DESTINATION_ID,
        workspace(),
        1,
        request.binding().clone(),
        TimestampMillis::new(20)?,
    )?;
    let _reserved = catalog.record_registration_reservation_observation(&reservation)?;
    let membership =
        catalog.commit_registration_membership(request_id, &[], TimestampMillis::new(30)?)?;
    let activation = BindingActivationObservation::new(
        reservation,
        membership.commit_digest(),
        request.display_name().clone(),
        request.declared_role().clone(),
        request.transport(),
        TimestampMillis::new(40)?,
    )?;
    let _ready = catalog.record_registration_activation_observation(&activation)?;

    // The registration, reservation, membership and activation evidence above
    // use real Operations APIs. The typed source observations are fixture data,
    // not readbacks from a live Forge process.
    let native_actor = OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    };
    assert_eq!(
        participant_id,
        crate::native_session_participation_id(WORKSPACE_ID, session_id.into_uuid()),
    );
    let command = ReviewerParticipationCommand::new(
        workspace(),
        Uuid::from_u128(0xA586),
        0,
        native_actor,
        ReviewerParticipationStatement::Interested,
    )?;
    assert!(catalog.record_reviewer_participation(&command).is_err());
    assert_eq!(count_receipts(&catalog)?, 0);
    assert_eq!(count_heads(&catalog)?, 0);
    Ok(())
}

fn native_registration_request(
    workspace_id: WorkspaceId,
    request_id: Uuid,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
) -> Result<RegistrationRequest, Box<dyn Error>> {
    let binding = AgentBindingReservationRequest::new(
        request_id,
        activity_id,
        agent_id,
        session_id,
        AuthenticatedPrincipal::TrustedStdio,
        TimestampMillis::new(10)?,
    )?;
    let participant_id =
        crate::native_session_participation_id(workspace_id.into_uuid(), session_id.into_uuid());
    Ok(RegistrationRequest::new(
        participant_id,
        binding,
        AgentDisplayName::new("Native reviewer candidate".to_owned())?,
        AgentDeclaredRole::new("agent".to_owned())?,
        AgentTransportKind::StreamableHttp,
    )?)
}

fn full_fixture(path: &Path) -> Result<OperationsCatalog, Box<dyn Error>> {
    let catalog = OperationsCatalog::open(path, DESTINATION_ID)?;
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        rusqlite::params![
            Uuid::from_u128(0xA587).as_bytes(),
            WORKSPACE_ID.as_bytes(),
            DESTINATION_ID.as_bytes(),
            [0x57_u8; 32].as_slice(),
        ],
    )?;
    Ok(catalog)
}

fn count_heads(catalog: &OperationsCatalog) -> Result<i64, rusqlite::Error> {
    catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_reviewer_participation_heads",
        [],
        |row| row.get(0),
    )
}

#[test]
fn reused_request_id_in_another_workspace_conflicts_without_writing_or_disclosing() -> TestResult {
    let mut catalog = fixture(None)?;
    let first = catalog.record_reviewer_participation(&command(
        0xA550,
        0,
        ReviewerParticipationStatement::Interested,
    )?)?;
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,phase) VALUES(?1,?2,?3,2)",
        rusqlite::params![
            Uuid::from_u128(0xA570).as_bytes(),
            WORKSPACE_ID_TWO.as_bytes(),
            DESTINATION_ID.as_bytes(),
        ],
    )?;
    crate::teams::tests::seed_participant_identity(
        &catalog.connection,
        second_workspace(),
        SECOND_PROFILE_ID,
        SECOND_PARTICIPATION_ID,
        SECOND_PARTICIPANT_ID,
        SECOND_ACTIVITY_ID,
        SECOND_AGENT_ID,
        SECOND_SESSION_ID,
        Uuid::from_u128(0xA571),
    )?;
    let command = ReviewerParticipationCommand::new(
        second_workspace(),
        first.request_id(),
        0,
        actor(SECOND_ACTIVITY_ID, SECOND_AGENT_ID, SECOND_SESSION_ID),
        ReviewerParticipationStatement::Interested,
    )?;
    assert!(catalog.record_reviewer_participation(&command).is_err());
    assert!(
        catalog
            .reviewer_participation_request(
                second_workspace(),
                first.request_id(),
                actor(SECOND_ACTIVITY_ID, SECOND_AGENT_ID, SECOND_SESSION_ID),
            )?
            .is_none()
    );
    assert_eq!(count_receipts(&catalog)?, 1);
    let second_workspace_heads: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_reviewer_participation_heads WHERE workspace_id=?1",
        rusqlite::params![WORKSPACE_ID_TWO.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(second_workspace_heads, 0);
    Ok(())
}

#[test]
fn exact_history_readback_survives_connection_reopen() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let mut catalog = fixture(Some(&database.path))?;
    let first = catalog.record_reviewer_participation(&command(
        0xA550,
        0,
        ReviewerParticipationStatement::Interested,
    )?)?;
    let request_id = first.request_id();
    drop(catalog);

    let reopened = fixture_reopen(&database.path)?;
    let receipt = reopened
        .reviewer_participation_request(workspace(), request_id, owner())?
        .ok_or("exact participant-owned Reviewer history missing after reopen")?;
    assert_eq!(receipt.request_id(), first.request_id());
    assert_eq!(receipt.session_id(), first.session_id());
    assert_eq!(receipt.participation_id(), first.participation_id());
    Ok(())
}

#[test]
fn same_profile_participation_alias_can_read_history_but_cannot_replay_a_write() -> TestResult {
    let mut catalog = fixture(None)?;
    let first = catalog.record_reviewer_participation(&command(
        0xA550,
        0,
        ReviewerParticipationStatement::Interested,
    )?)?;
    seed_session_alias(&catalog, Uuid::from_u128(0xA560))?;
    let next_session = actor(
        Uuid::from_u128(0xA561),
        Uuid::from_u128(0xA562),
        Uuid::from_u128(0xA560),
    );

    let readback = catalog
        .reviewer_participation_request(workspace(), first.request_id(), next_session)?
        .ok_or("same participation history was not readable")?;
    assert_eq!(readback.participation_id(), first.participation_id());
    assert_eq!(readback.profile_id(), first.profile_id());
    assert_eq!(readback.session_id(), first.session_id());
    let changed_session_retry = ReviewerParticipationCommand::new(
        workspace(),
        first.request_id(),
        0,
        next_session,
        ReviewerParticipationStatement::Interested,
    )?;
    assert!(
        catalog
            .record_reviewer_participation(&changed_session_retry)
            .is_err()
    );
    Ok(())
}

fn fixture_reopen(path: &Path) -> Result<OperationsCatalog, Box<dyn Error>> {
    let connection = crate::teams::tests::create_connection(Some(&path.to_path_buf()))?;
    Ok(OperationsCatalog {
        connection,
        destination: DESTINATION_ID,
    })
}

fn seed_session_alias(catalog: &OperationsCatalog, session: Uuid) -> Result<(), rusqlite::Error> {
    let participant = Uuid::from_u128(0xA563);
    let request = Uuid::from_u128(0xA564);
    catalog.connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
        rusqlite::params![
            request.as_bytes(),
            WORKSPACE_ID.as_bytes(),
            PARTICIPATION_ID.as_bytes(),
            PROFILE_ID.as_bytes(),
            Uuid::from_u128(0xA565).as_bytes(),
            session.as_bytes(),
            participant.as_bytes(),
        ],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,display_name) VALUES(?1,?2,?3,'Participant reconnected')",
        rusqlite::params![WORKSPACE_ID.as_bytes(), participant.as_bytes(), session.as_bytes()],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
        rusqlite::params![
            WORKSPACE_ID.as_bytes(),
            participant.as_bytes(),
            Uuid::from_u128(0xA561).as_bytes(),
            session.as_bytes(),
            Uuid::from_u128(0xA562).as_bytes(),
            Uuid::from_u128(0xA566).as_bytes(),
        ],
    )?;
    Ok(())
}

fn count_receipts(catalog: &OperationsCatalog) -> Result<i64, rusqlite::Error> {
    catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_reviewer_participation_receipts",
        [],
        |row| row.get(0),
    )
}
