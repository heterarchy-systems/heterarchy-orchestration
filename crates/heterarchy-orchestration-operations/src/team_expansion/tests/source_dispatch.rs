use super::*;
use crate::{
    TeamPermanentTransferPreparationCommand, TeamPermanentTransferSourceDispatchCommand,
    TeamPermanentTransferSourceDispatchOutcome, TeamPermanentTransferSourceReceiptCommand,
    TeamPermanentTransferSourceReceiptOutcome, TeamPermanentTransferSourceWireIdentity,
    WorkspaceProjectScope,
};
use forge_domain::{
    ForgeBackendId, HomeTransferSourceAction, HomeTransferSourceGrant, OrchestrationIssuerId,
    TimestampMillis,
};
use forge_persistence::{
    HomeTransferSourceRecord, HomeTransferSourceRecordParts, HomeTransferSourceState,
};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs,
    io::Error as IoError,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};
use uuid::Uuid;

mod home_acceptance;

type SourceDispatchResult<T = ()> = Result<T, Box<dyn Error>>;

const SOURCE_BACKEND: Uuid = Uuid::from_u128(0xA100);
const SOURCE_ISSUER: Uuid = Uuid::from_u128(0xA101);
const SOURCE_LOCATION: Uuid = Uuid::from_u128(0xA102);
const SOURCE_PREPARATION: Uuid = Uuid::from_u128(0xA103);
const SOURCE_CONSENT: Uuid = Uuid::from_u128(0xA104);
const TARGET_ACCEPTANCE: Uuid = Uuid::from_u128(0xA105);

struct SourceDispatchFixture {
    transfer: PermanentTransferIntentFixture,
    preparation: crate::TeamPermanentTransferPreparationReceipt,
}

struct TemporaryDatabase(PathBuf);

impl TemporaryDatabase {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "heterarchy-source-dispatch-{}.sqlite",
            Uuid::now_v7()
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(self.0.with_extension("sqlite-wal"));
        let _ = fs::remove_file(self.0.with_extension("sqlite-shm"));
    }
}

fn prepare_source_dispatch_fixture(
    path: Option<&Path>,
) -> SourceDispatchResult<SourceDispatchFixture> {
    let mut transfer = match path {
        Some(path) => {
            prepare_permanent_transfer_intent_with_catalog(workspace_capacity_fixture_at(path)?)?
        }
        None => prepare_permanent_transfer_intent()?,
    };
    ensure_source_dispatch_test_schema(&transfer.catalog.connection)?;
    let source_consent = transfer.catalog.record_team_permanent_transfer_consent(
        &crate::TeamPermanentTransferConsentCommand::new(
            SOURCE_CONSENT,
            WORKSPACE,
            transfer.intent.transfer_id(),
            crate::TeamPermanentTransferConsentKind::SourceHomeLeader,
            seventh_participant(),
        )?,
        || timestamp(500),
    )?;
    let target_acceptance = transfer.catalog.record_team_permanent_transfer_consent(
        &crate::TeamPermanentTransferConsentCommand::new(
            TARGET_ACCEPTANCE,
            WORKSPACE,
            transfer.intent.transfer_id(),
            crate::TeamPermanentTransferConsentKind::TargetAgentAcceptance,
            sixth_participant(),
        )?,
        || timestamp(501),
    )?;
    let command = TeamPermanentTransferPreparationCommand::new(
        SOURCE_PREPARATION,
        WORKSPACE,
        transfer.intent.transfer_id(),
        1,
        1,
        source_consent.receipt_id(),
        target_acceptance.receipt_id(),
        orchestrator(),
    )?;
    let preparation = transfer
        .catalog
        .prepare_team_permanent_transfer(&command, || timestamp(502))?;
    ensure_source_schema(&transfer.catalog.connection)?;
    Ok(SourceDispatchFixture {
        transfer,
        preparation,
    })
}

fn ensure_source_dispatch_test_schema(connection: &Connection) -> SourceDispatchResult {
    for (kind, name, sql) in crate::membership::schema_objects() {
        ensure_schema_object(connection, kind, name, sql)?;
    }
    let has_member_history: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('ops_logical_members') WHERE name='membership_revision')",
        [],
        |row| row.get(0),
    )?;
    if !has_member_history {
        super::prepare_session_rebind_test_columns(connection)?;
    }
    ensure_schema_objects(connection, crate::session_replacement::schema_objects())?;
    ensure_schema_objects(
        connection,
        crate::replacement_registration::schema_objects(),
    )?;
    ensure_schema_objects(
        connection,
        crate::temporary_dispatch::lifecycle::lifecycle_schema_objects(),
    )?;
    ensure_schema_objects(
        connection,
        crate::team_permanent_transfer::preparation::schema::schema_objects()
            .iter()
            .copied(),
    )?;
    Ok(())
}

fn ensure_source_schema(connection: &Connection) -> SourceDispatchResult {
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_team_permanent_transfer_source_dispatches')",
        [],
        |row| row.get(0),
    )?;
    if exists {
        crate::team_permanent_transfer::source::schema::validate_schema(connection)?;
    } else {
        crate::team_permanent_transfer::source::schema::create_schema(connection)?;
    }
    Ok(())
}

fn ensure_schema_objects(
    connection: &Connection,
    objects: impl IntoIterator<Item = (&'static str, &'static str, &'static str)>,
) -> SourceDispatchResult {
    for (kind, name, sql) in objects {
        ensure_schema_object(connection, kind, name, sql)?;
    }
    Ok(())
}

fn ensure_schema_object(
    connection: &Connection,
    kind: &str,
    name: &str,
    sql: &str,
) -> SourceDispatchResult {
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type=?1 AND name=?2)",
        rusqlite::params![kind, name],
        |row| row.get(0),
    )?;
    if !exists {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

fn open_test_catalog(path: &Path) -> SourceDispatchResult<OperationsCatalog> {
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.busy_timeout(Duration::from_secs(2))?;
    Ok(OperationsCatalog {
        connection,
        destination: DESTINATION,
    })
}

fn source_scope(
    workspace: WorkspaceId,
    location_id: Uuid,
) -> Result<WorkspaceProjectScope, OperationsError> {
    WorkspaceProjectScope::new(
        workspace.into_uuid(),
        location_id,
        1,
        1,
        "file:///workspace/source-dispatch".to_owned(),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "each test supplies a complete immutable dispatch pin tuple"
)]
fn source_dispatch_command(
    preparation: &crate::TeamPermanentTransferPreparationReceipt,
    request_id: Uuid,
    route: SocketAddr,
    source_scope: WorkspaceProjectScope,
    issuer: Uuid,
    service_destination: Uuid,
    issued_at: i64,
    expires_at: i64,
    wire_request_id: Uuid,
    body_sha256: [u8; 32],
    body_byte_count: usize,
) -> Result<TeamPermanentTransferSourceDispatchCommand, OperationsError> {
    source_dispatch_command_for_authority(
        preparation,
        preparation.workspace_leadership_revision(),
        orchestrator(),
        request_id,
        route,
        source_scope,
        issuer,
        service_destination,
        issued_at,
        expires_at,
        wire_request_id,
        body_sha256,
        body_byte_count,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the regression varies only explicit current authority and its revision"
)]
fn source_dispatch_command_for_authority(
    preparation: &crate::TeamPermanentTransferPreparationReceipt,
    expected_workspace_leadership_revision: i64,
    actor: OperationalActor,
    request_id: Uuid,
    route: SocketAddr,
    source_scope: WorkspaceProjectScope,
    issuer: Uuid,
    service_destination: Uuid,
    issued_at: i64,
    expires_at: i64,
    wire_request_id: Uuid,
    body_sha256: [u8; 32],
    body_byte_count: usize,
) -> Result<TeamPermanentTransferSourceDispatchCommand, OperationsError> {
    let backend = ForgeBackendId::from_uuid(SOURCE_BACKEND);
    let source_request = crate::derive_team_permanent_transfer_source_request(
        preparation,
        &source_scope,
        backend,
        service_destination,
    )?;
    let grant = HomeTransferSourceGrant::new(
        OrchestrationIssuerId::from_uuid(issuer),
        HomeTransferSourceAction::Prepare,
        source_request.request_digest(),
        TimestampMillis::new(issued_at)
            .map_err(|_| OperationsError::Bounds("test Source grant issue time"))?,
        TimestampMillis::new(expires_at)
            .map_err(|_| OperationsError::Bounds("test Source grant expiry time"))?,
    )
    .map_err(|_| OperationsError::Conflict("test Source grant"))?;
    let wire_identity = TeamPermanentTransferSourceWireIdentity::new(
        wire_request_id,
        body_sha256,
        body_byte_count,
    )?;
    TeamPermanentTransferSourceDispatchCommand::new(
        preparation.workspace_id(),
        preparation.preparation_id(),
        preparation.transfer_id(),
        preparation.generation(),
        expected_workspace_leadership_revision,
        request_id,
        actor,
        route,
        source_scope,
        backend,
        OrchestrationIssuerId::from_uuid(issuer),
        service_destination,
        source_request,
        grant,
        wire_identity,
    )
}

fn default_dispatch_command(
    preparation: &crate::TeamPermanentTransferPreparationReceipt,
    request_id: Uuid,
) -> Result<TeamPermanentTransferSourceDispatchCommand, OperationsError> {
    source_dispatch_command(
        preparation,
        request_id,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51_020),
        source_scope(WORKSPACE, SOURCE_LOCATION)?,
        SOURCE_ISSUER,
        DESTINATION,
        510,
        600,
        Uuid::from_u128(0xA106),
        [0xA1; 32],
        128,
    )
}

fn count_dispatches(connection: &Connection, preparation_id: Uuid) -> SourceDispatchResult<i64> {
    Ok(connection.query_row(
        "SELECT count(*) FROM ops_team_permanent_transfer_source_dispatches WHERE workspace_id=?1 AND preparation_id=?2",
        rusqlite::params![WORKSPACE.into_uuid().as_bytes(), preparation_id.as_bytes()],
        |row| row.get(0),
    )?)
}

fn count_source_receipts(
    connection: &Connection,
    preparation_id: Uuid,
) -> SourceDispatchResult<i64> {
    Ok(connection.query_row(
        "SELECT count(*) FROM ops_team_permanent_transfer_source_receipts WHERE workspace_id=?1 AND preparation_id=?2",
        rusqlite::params![WORKSPACE.into_uuid().as_bytes(), preparation_id.as_bytes()],
        |row| row.get(0),
    )?)
}

fn preparation_payload(
    connection: &Connection,
    preparation_id: Uuid,
) -> SourceDispatchResult<Vec<u8>> {
    Ok(connection.query_row(
        "SELECT payload FROM ops_team_permanent_transfer_preparations WHERE workspace_id=?1 AND preparation_id=?2",
        rusqlite::params![WORKSPACE.into_uuid().as_bytes(), preparation_id.as_bytes()],
        |row| row.get(0),
    )?)
}

#[test]
fn source_dispatch_two_connections_commit_one_send_permit_and_read_after_restart()
-> SourceDispatchResult {
    let database = TemporaryDatabase::new();
    let mut fixture = prepare_source_dispatch_fixture(Some(database.path()))?;
    let command = default_dispatch_command(&fixture.preparation, Uuid::from_u128(0xA110))?;
    let mut peer_catalog = open_test_catalog(database.path())?;
    let start = Arc::new(Barrier::new(2));
    let peer_start = Arc::clone(&start);
    let peer_command = command.clone();
    let peer = thread::spawn(move || {
        peer_start.wait();
        peer_catalog
            .commit_team_permanent_transfer_source_dispatch(&peer_command, || timestamp(510))
    });
    start.wait();
    let local_result = fixture
        .transfer
        .catalog
        .commit_team_permanent_transfer_source_dispatch(&command, || timestamp(510))?;
    let peer_result = peer
        .join()
        .map_err(|_| IoError::other("source dispatch peer panicked"))??;
    let permit_count = usize::from(matches!(
        local_result,
        TeamPermanentTransferSourceDispatchOutcome::Created(_)
    )) + usize::from(matches!(
        peer_result,
        TeamPermanentTransferSourceDispatchOutcome::Created(_)
    ));
    assert_eq!(
        permit_count, 1,
        "only the committed first attempt has send authority"
    );
    assert_eq!(
        count_dispatches(
            &fixture.transfer.catalog.connection,
            command.preparation_id()
        )?,
        1
    );

    drop(fixture.transfer.catalog);
    let mut restarted = open_test_catalog(database.path())?;
    let readback = restarted
        .read_team_permanent_transfer_source_dispatch(
            WORKSPACE,
            command.preparation_id(),
            orchestrator(),
        )?
        .ok_or("source dispatch readback missing after reopening its database")?;
    let dispatch = readback.dispatch();
    assert_eq!(dispatch.request_id(), command.request_id());
    assert_eq!(dispatch.envelope_digest(), command.envelope_digest());
    assert_eq!(dispatch.grant(), command.grant());
    assert_eq!(dispatch.wire_identity(), command.wire_identity());
    assert_eq!(dispatch.source_request(), command.source_request());
    assert!(readback.original_source_record().is_none());
    let retry_after_readback =
        restarted.commit_team_permanent_transfer_source_dispatch(&command, || timestamp(520))?;
    assert!(matches!(
        retry_after_readback,
        TeamPermanentTransferSourceDispatchOutcome::Existing(_)
    ));
    assert_eq!(
        restarted.read_team_permanent_transfer_source_dispatch(
            WORKSPACE,
            command.preparation_id(),
            fifth_participant(),
        )?,
        None,
        "unrelated valid reader does not see dispatch"
    );
    Ok(())
}

#[test]
fn source_dispatch_replay_preserves_original_grant_and_wire_identity() -> SourceDispatchResult {
    let mut fixture = prepare_source_dispatch_fixture(None)?;
    let original = default_dispatch_command(&fixture.preparation, Uuid::from_u128(0xA120))?;
    let created = fixture
        .transfer
        .catalog
        .commit_team_permanent_transfer_source_dispatch(&original, || timestamp(510))?;
    assert!(matches!(
        created,
        TeamPermanentTransferSourceDispatchOutcome::Created(_)
    ));

    assert_exact_source_dispatch_replay_preserves_original(&mut fixture, &original)?;
    assert_changed_route_conflicts_with_original_dispatch(&mut fixture, &original)?;
    assert_changed_issuer_conflicts_with_original_dispatch(&mut fixture, &original)?;
    assert_changed_access_scope_conflicts_with_original_dispatch(&mut fixture, &original)?;
    assert_new_request_id_conflicts_with_existing_dispatch(&mut fixture, &original)?;
    assert_eq!(
        count_dispatches(
            &fixture.transfer.catalog.connection,
            original.preparation_id()
        )?,
        1
    );
    Ok(())
}

fn assert_exact_source_dispatch_replay_preserves_original(
    fixture: &mut SourceDispatchFixture,
    original: &TeamPermanentTransferSourceDispatchCommand,
) -> SourceDispatchResult {
    let proposed_retry = source_dispatch_command(
        &fixture.preparation,
        original.request_id(),
        original.source_route(),
        original.source_scope().clone(),
        SOURCE_ISSUER,
        DESTINATION,
        520,
        700,
        Uuid::from_u128(0xA121),
        [0xA2; 32],
        256,
    )?;
    assert_eq!(proposed_retry.envelope_digest(), original.envelope_digest());
    let sampled_clock = std::cell::Cell::new(false);
    let replay = fixture
        .transfer
        .catalog
        .commit_team_permanent_transfer_source_dispatch(&proposed_retry, || {
            sampled_clock.set(true);
            timestamp(520)
        })?;
    let TeamPermanentTransferSourceDispatchOutcome::Existing(replayed) = replay else {
        return Err("exact retry did not return the existing dispatch".into());
    };
    assert_eq!(replayed.envelope_digest(), original.envelope_digest());
    assert!(
        !sampled_clock.get(),
        "exact replay does not extend or refresh its grant"
    );
    let readback = fixture
        .transfer
        .catalog
        .read_team_permanent_transfer_source_dispatch(
            WORKSPACE,
            original.preparation_id(),
            orchestrator(),
        )?
        .ok_or("source dispatch readback missing")?;
    assert_eq!(readback.dispatch().grant(), original.grant());
    assert_eq!(
        readback.dispatch().wire_identity(),
        original.wire_identity()
    );
    assert_eq!(readback.dispatch().source_route(), original.source_route());
    Ok(())
}

fn assert_changed_route_conflicts_with_original_dispatch(
    fixture: &mut SourceDispatchFixture,
    original: &TeamPermanentTransferSourceDispatchCommand,
) -> SourceDispatchResult {
    let changed_route = source_dispatch_command(
        &fixture.preparation,
        original.request_id(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51_021),
        original.source_scope().clone(),
        SOURCE_ISSUER,
        DESTINATION,
        520,
        700,
        Uuid::from_u128(0xA124),
        [0xA4; 32],
        256,
    )?;
    assert!(matches!(
        fixture
            .transfer
            .catalog
            .commit_team_permanent_transfer_source_dispatch(&changed_route, || timestamp(520)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(
        changed_route.source_request().request_digest(),
        original.source_request().request_digest(),
    );
    assert_ne!(changed_route.envelope_digest(), original.envelope_digest());
    Ok(())
}

fn assert_changed_issuer_conflicts_with_original_dispatch(
    fixture: &mut SourceDispatchFixture,
    original: &TeamPermanentTransferSourceDispatchCommand,
) -> SourceDispatchResult {
    let changed_issuer = source_dispatch_command(
        &fixture.preparation,
        original.request_id(),
        original.source_route(),
        original.source_scope().clone(),
        Uuid::from_u128(0xA126),
        DESTINATION,
        520,
        700,
        Uuid::from_u128(0xA127),
        [0xA5; 32],
        256,
    )?;
    assert_eq!(
        changed_issuer.source_request().request_digest(),
        original.source_request().request_digest(),
    );
    assert_ne!(changed_issuer.envelope_digest(), original.envelope_digest());
    assert!(
        fixture
            .transfer
            .catalog
            .commit_team_permanent_transfer_source_dispatch(&changed_issuer, || timestamp(520))
            .is_err()
    );
    Ok(())
}

fn assert_changed_access_scope_conflicts_with_original_dispatch(
    fixture: &mut SourceDispatchFixture,
    original: &TeamPermanentTransferSourceDispatchCommand,
) -> SourceDispatchResult {
    let changed_scope = WorkspaceProjectScope::new(
        WORKSPACE.into_uuid(),
        Uuid::from_u128(0xA128),
        2,
        1,
        "file:///workspace/source-dispatch/alternate".to_owned(),
    )?;
    let changed_access_scope = source_dispatch_command(
        &fixture.preparation,
        original.request_id(),
        original.source_route(),
        changed_scope,
        SOURCE_ISSUER,
        DESTINATION,
        520,
        700,
        Uuid::from_u128(0xA129),
        [0xA6; 32],
        256,
    )?;
    assert_eq!(
        changed_access_scope.source_request().request_digest(),
        original.source_request().request_digest(),
    );
    assert_ne!(
        changed_access_scope.envelope_digest(),
        original.envelope_digest(),
    );
    assert!(
        fixture
            .transfer
            .catalog
            .commit_team_permanent_transfer_source_dispatch(&changed_access_scope, || timestamp(
                520
            ))
            .is_err()
    );
    Ok(())
}

fn assert_new_request_id_conflicts_with_existing_dispatch(
    fixture: &mut SourceDispatchFixture,
    original: &TeamPermanentTransferSourceDispatchCommand,
) -> SourceDispatchResult {
    let different_request_id = source_dispatch_command(
        &fixture.preparation,
        Uuid::from_u128(0xA122),
        original.source_route(),
        original.source_scope().clone(),
        SOURCE_ISSUER,
        DESTINATION,
        520,
        700,
        Uuid::from_u128(0xA123),
        [0xA3; 32],
        256,
    )?;
    assert!(matches!(
        fixture
            .transfer
            .catalog
            .commit_team_permanent_transfer_source_dispatch(&different_request_id, || timestamp(
                520
            )),
        Err(OperationsError::Conflict(_))
    ));
    Ok(())
}

#[test]
fn source_dispatch_rejects_source_leader_drift_before_first_attempt() -> SourceDispatchResult {
    let mut fixture = prepare_source_dispatch_fixture(None)?;
    replace_team_leader(
        &fixture.transfer.catalog.connection,
        fixture.transfer.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let command = default_dispatch_command(&fixture.preparation, Uuid::from_u128(0xA125))?;
    assert!(
        fixture
            .transfer
            .catalog
            .commit_team_permanent_transfer_source_dispatch(&command, || timestamp(510))
            .is_err()
    );
    assert_eq!(
        count_dispatches(
            &fixture.transfer.catalog.connection,
            command.preparation_id()
        )?,
        0
    );
    Ok(())
}

#[test]
fn current_orchestrator_can_dispatch_from_original_preparation_without_rewriting_it()
-> SourceDispatchResult {
    let mut fixture = prepare_source_dispatch_fixture(None)?;
    let original = SourceDispatchPreparationSnapshot::capture(&fixture)?;
    assert_eq!(original.receipt.actor(), orchestrator());
    assert_eq!(
        original.receipt.actor_participation_id().into_uuid(),
        ORCHESTRATOR_PARTICIPATION,
    );
    let original_command = default_dispatch_command(&original.receipt, Uuid::from_u128(0xA126))?;
    let original_source_request = original_command.source_request().clone();

    let current_workspace_revision =
        replace_workspace_orchestrator_for_source_dispatch(&mut fixture, &original)?;
    assert_original_orchestrator_cannot_dispatch_after_leadership_change(
        &mut fixture,
        &original_command,
    )?;
    assert_current_orchestrator_can_dispatch_from_original_preparation(
        &mut fixture,
        &original.receipt,
        &original_command,
        &original_source_request,
        current_workspace_revision,
    )?;
    assert_original_preparation_and_source_request_are_preserved(
        &fixture,
        &original,
        &original_source_request,
    )?;
    Ok(())
}

struct SourceDispatchPreparationSnapshot {
    receipt: crate::TeamPermanentTransferPreparationReceipt,
    payload: Vec<u8>,
}

impl SourceDispatchPreparationSnapshot {
    fn capture(fixture: &SourceDispatchFixture) -> SourceDispatchResult<Self> {
        let preparation_id = fixture.preparation.preparation_id();
        Ok(Self {
            receipt: fixture.preparation.clone(),
            payload: preparation_payload(&fixture.transfer.catalog.connection, preparation_id)?,
        })
    }
}

fn replace_workspace_orchestrator_for_source_dispatch(
    fixture: &mut SourceDispatchFixture,
    original: &SourceDispatchPreparationSnapshot,
) -> SourceDispatchResult<i64> {
    assert_ne!(MEMBER_FOUR_PARTICIPANT, MEMBER_SIX_PARTICIPANT);
    assert_ne!(MEMBER_FOUR_PARTICIPANT, MEMBER_SEVEN_PARTICIPANT);
    replace_workspace_orchestrator(
        &fixture.transfer.catalog.connection,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let current_workspace_revision: i64 = fixture.transfer.catalog.connection.query_row(
        "SELECT leadership_revision FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=0 AND activity_id IS NULL AND team_id IS NULL",
        rusqlite::params![WORKSPACE.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    assert_ne!(
        current_workspace_revision,
        original.receipt.workspace_leadership_revision()
    );
    Ok(current_workspace_revision)
}

fn assert_original_orchestrator_cannot_dispatch_after_leadership_change(
    fixture: &mut SourceDispatchFixture,
    original: &TeamPermanentTransferSourceDispatchCommand,
) -> SourceDispatchResult {
    assert!(matches!(
        fixture
            .transfer
            .catalog
            .commit_team_permanent_transfer_source_dispatch(original, || timestamp(510)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(
        count_dispatches(
            &fixture.transfer.catalog.connection,
            original.preparation_id()
        )?,
        0
    );
    Ok(())
}

fn assert_current_orchestrator_can_dispatch_from_original_preparation(
    fixture: &mut SourceDispatchFixture,
    original_preparation: &crate::TeamPermanentTransferPreparationReceipt,
    original_command: &TeamPermanentTransferSourceDispatchCommand,
    original_source_request: &forge_domain::HomeTransferSourceRequest,
    current_workspace_revision: i64,
) -> SourceDispatchResult {
    let current_orchestrator = source_dispatch_command_for_authority(
        original_preparation,
        current_workspace_revision,
        fourth_participant(),
        original_command.request_id(),
        original_command.source_route(),
        original_command.source_scope().clone(),
        SOURCE_ISSUER,
        DESTINATION,
        510,
        600,
        original_command.wire_identity().transport_request_id(),
        original_command.wire_identity().body_sha256(),
        usize::try_from(original_command.wire_identity().body_byte_count())?,
    )?;
    assert_eq!(
        current_orchestrator.source_request(),
        original_source_request,
    );
    let outcome = fixture
        .transfer
        .catalog
        .commit_team_permanent_transfer_source_dispatch(&current_orchestrator, || timestamp(510))?;
    let TeamPermanentTransferSourceDispatchOutcome::Created(permit) = outcome else {
        return Err("new current Orchestrator did not receive the first-attempt permit".into());
    };
    assert_eq!(
        permit.record().expected_workspace_leadership_revision(),
        current_workspace_revision,
    );
    assert_eq!(
        permit.record().actor().participant_id(),
        MEMBER_FOUR_PARTICIPANT
    );
    assert_eq!(
        count_dispatches(
            &fixture.transfer.catalog.connection,
            original_preparation.preparation_id()
        )?,
        1
    );
    Ok(())
}

fn assert_original_preparation_and_source_request_are_preserved(
    fixture: &SourceDispatchFixture,
    original: &SourceDispatchPreparationSnapshot,
    original_source_request: &forge_domain::HomeTransferSourceRequest,
) -> SourceDispatchResult {
    let preparation_id = original.receipt.preparation_id();
    let payload_after = preparation_payload(&fixture.transfer.catalog.connection, preparation_id)?;
    assert_eq!(payload_after, original.payload);
    assert_eq!(fixture.preparation, original.receipt);
    assert_eq!(
        fixture.preparation.workspace_leadership_revision(),
        original.receipt.workspace_leadership_revision(),
    );
    let prepared_readback = fixture
        .transfer
        .catalog
        .read_team_permanent_transfer_preparation(WORKSPACE, preparation_id, fourth_participant())?
        .ok_or("current Orchestrator could not read the original preparation")?;
    assert_eq!(prepared_readback, original.receipt);

    let source_readback = fixture
        .transfer
        .catalog
        .read_team_permanent_transfer_source_dispatch(
            WORKSPACE,
            preparation_id,
            fourth_participant(),
        )?
        .ok_or("current Orchestrator could not read the Source dispatch")?;
    let stored_request = source_readback.dispatch().source_request();
    assert_eq!(stored_request, original_source_request);
    assert_eq!(stored_request.witness().preparation_id(), preparation_id);
    assert_eq!(
        stored_request.witness().intent_digest(),
        &original.receipt.intent_digest(),
    );
    assert_eq!(
        stored_request.witness().preparation_digest(),
        &original.receipt.digest(),
    );
    Ok(())
}

#[test]
fn source_dispatch_expiry_and_invalid_scope_fail_before_journal_write() -> SourceDispatchResult {
    let mut fixture = prepare_source_dispatch_fixture(None)?;
    let expired = default_dispatch_command(&fixture.preparation, Uuid::from_u128(0xA130))?;
    let expired = source_dispatch_command(
        &fixture.preparation,
        expired.request_id(),
        expired.source_route(),
        expired.source_scope().clone(),
        SOURCE_ISSUER,
        DESTINATION,
        510,
        511,
        Uuid::from_u128(0xA131),
        [0xA4; 32],
        32,
    )?;
    assert!(
        fixture
            .transfer
            .catalog
            .commit_team_permanent_transfer_source_dispatch(&expired, || timestamp(511))
            .is_err()
    );
    assert_eq!(
        count_dispatches(
            &fixture.transfer.catalog.connection,
            fixture.preparation.preparation_id()
        )?,
        0
    );

    let scope = source_scope(WORKSPACE, SOURCE_LOCATION)?;
    let oversized_wire =
        TeamPermanentTransferSourceWireIdentity::new(Uuid::from_u128(0xA132), [0xA5; 32], 65_537);
    assert!(oversized_wire.is_err());
    assert!(
        source_dispatch_command(
            &fixture.preparation,
            Uuid::from_u128(0xA133),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51_020),
            scope.clone(),
            SOURCE_ISSUER,
            Uuid::from_u128(0xA134),
            510,
            600,
            Uuid::from_u128(0xA135),
            [0xA6; 32],
            32,
        )
        .is_err()
    );
    assert!(
        source_dispatch_command(
            &fixture.preparation,
            Uuid::from_u128(0xA136),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51_020),
            source_scope(
                WorkspaceId::from_uuid(Uuid::from_u128(0xA137)),
                SOURCE_LOCATION
            )?,
            SOURCE_ISSUER,
            DESTINATION,
            510,
            600,
            Uuid::from_u128(0xA138),
            [0xA7; 32],
            32,
        )
        .is_err()
    );
    assert_eq!(
        count_dispatches(
            &fixture.transfer.catalog.connection,
            fixture.preparation.preparation_id()
        )?,
        0
    );
    Ok(())
}

#[test]
fn source_receipt_is_immutable_and_ingestion_survives_leader_drift() -> SourceDispatchResult {
    let mut fixture = prepare_source_dispatch_fixture(None)?;
    let command = default_dispatch_command(&fixture.preparation, Uuid::from_u128(0xA140))?;
    let outcome = fixture
        .transfer
        .catalog
        .commit_team_permanent_transfer_source_dispatch(&command, || timestamp(510))?;
    assert!(matches!(
        outcome,
        TeamPermanentTransferSourceDispatchOutcome::Created(_)
    ));
    replace_team_leader(
        &fixture.transfer.catalog.connection,
        fixture.transfer.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    replace_workspace_orchestrator(
        &fixture.transfer.catalog.connection,
        MEMBER_FOUR_PARTICIPANT,
    )?;

    let record = aborted_source_record(command.source_request().clone(), SOURCE_ISSUER, 20)?;
    assert_wrong_source_receipt_pins_are_rejected(&mut fixture, &command, &record)?;
    record_first_source_receipt(&mut fixture, &command, &record)?;
    assert_source_receipt_retries_preserve_original(&mut fixture, &command, &record)?;
    assert_source_dispatch_readback_survives_leader_drift(&fixture, &command)?;
    Ok(())
}

fn assert_wrong_source_receipt_pins_are_rejected(
    fixture: &mut SourceDispatchFixture,
    command: &TeamPermanentTransferSourceDispatchCommand,
    record: &HomeTransferSourceRecord,
) -> SourceDispatchResult {
    let changed_route = source_dispatch_command(
        &fixture.preparation,
        Uuid::from_u128(0xA141),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51_022),
        command.source_scope().clone(),
        SOURCE_ISSUER,
        DESTINATION,
        510,
        600,
        Uuid::from_u128(0xA142),
        [0xA9; 32],
        128,
    )?;
    assert_eq!(
        changed_route.source_request().request_digest(),
        command.source_request().request_digest(),
    );
    assert_ne!(changed_route.envelope_digest(), command.envelope_digest());
    let wrong_digest = TeamPermanentTransferSourceReceiptCommand::new(
        WORKSPACE,
        command.preparation_id(),
        changed_route.envelope_digest(),
        TimestampMillis::new(511)?,
        record.clone(),
    )?;
    assert!(
        fixture
            .transfer
            .catalog
            .record_team_permanent_transfer_source_receipt(&wrong_digest)
            .is_err()
    );
    let wrong_issuer_record = aborted_source_record(
        command.source_request().clone(),
        Uuid::from_u128(0xA151),
        20,
    )?;
    let wrong_issuer = TeamPermanentTransferSourceReceiptCommand::new(
        WORKSPACE,
        command.preparation_id(),
        command.envelope_digest(),
        TimestampMillis::new(511)?,
        wrong_issuer_record,
    )?;
    assert!(
        fixture
            .transfer
            .catalog
            .record_team_permanent_transfer_source_receipt(&wrong_issuer)
            .is_err()
    );
    assert_eq!(
        count_source_receipts(
            &fixture.transfer.catalog.connection,
            command.preparation_id()
        )?,
        0
    );
    Ok(())
}

fn record_first_source_receipt(
    fixture: &mut SourceDispatchFixture,
    command: &TeamPermanentTransferSourceDispatchCommand,
    record: &HomeTransferSourceRecord,
) -> SourceDispatchResult {
    let receipt = TeamPermanentTransferSourceReceiptCommand::new(
        WORKSPACE,
        command.preparation_id(),
        command.envelope_digest(),
        TimestampMillis::new(511)?,
        record.clone(),
    )?;
    let first = fixture
        .transfer
        .catalog
        .record_team_permanent_transfer_source_receipt(&receipt)?;
    let TeamPermanentTransferSourceReceiptOutcome::Recorded(stored) = first else {
        return Err("first Source result was not recorded as a new original".into());
    };
    assert_eq!(stored.source_record(), record);
    assert_eq!(stored.recorded_at(), TimestampMillis::new(511)?);
    Ok(())
}

fn assert_source_receipt_retries_preserve_original(
    fixture: &mut SourceDispatchFixture,
    command: &TeamPermanentTransferSourceDispatchCommand,
    record: &HomeTransferSourceRecord,
) -> SourceDispatchResult {
    let later_retry = TeamPermanentTransferSourceReceiptCommand::new(
        WORKSPACE,
        command.preparation_id(),
        command.envelope_digest(),
        TimestampMillis::new(512)?,
        record.clone(),
    )?;
    let duplicate = fixture
        .transfer
        .catalog
        .record_team_permanent_transfer_source_receipt(&later_retry)?;
    let TeamPermanentTransferSourceReceiptOutcome::Existing(duplicate) = duplicate else {
        return Err("duplicate Source result did not preserve the original receipt".into());
    };
    assert!(duplicate.replayed());
    assert_eq!(duplicate.recorded_at(), TimestampMillis::new(511)?);
    let conflicting = aborted_source_record(command.source_request().clone(), SOURCE_ISSUER, 21)?;
    let conflict_command = TeamPermanentTransferSourceReceiptCommand::new(
        WORKSPACE,
        command.preparation_id(),
        command.envelope_digest(),
        TimestampMillis::new(513)?,
        conflicting,
    )?;
    assert!(matches!(
        fixture
            .transfer
            .catalog
            .record_team_permanent_transfer_source_receipt(&conflict_command),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(
        count_source_receipts(
            &fixture.transfer.catalog.connection,
            command.preparation_id()
        )?,
        1
    );
    let stale_local_time = TeamPermanentTransferSourceReceiptCommand::new(
        WORKSPACE,
        command.preparation_id(),
        command.envelope_digest(),
        TimestampMillis::new(509)?,
        record.clone(),
    )?;
    assert!(
        fixture
            .transfer
            .catalog
            .record_team_permanent_transfer_source_receipt(&stale_local_time)
            .is_err()
    );
    Ok(())
}

fn assert_source_dispatch_readback_survives_leader_drift(
    fixture: &SourceDispatchFixture,
    command: &TeamPermanentTransferSourceDispatchCommand,
) -> SourceDispatchResult {
    let readback = fixture
        .transfer
        .catalog
        .read_team_permanent_transfer_source_dispatch(
            WORKSPACE,
            command.preparation_id(),
            orchestrator(),
        )?
        .ok_or("Source dispatch readback missing after leader drift")?;
    let original = readback
        .original_source_record()
        .ok_or("Source original result missing after leader drift")?;
    assert_eq!(
        original.source_record().state(),
        HomeTransferSourceState::AbortedUnstarted
    );
    assert_eq!(original.recorded_at(), TimestampMillis::new(511)?);
    assert!(
        fixture
            .transfer
            .catalog
            .read_team_permanent_transfer_source_dispatch(
                WORKSPACE,
                command.preparation_id(),
                fourth_participant(),
            )?
            .is_some()
    );
    Ok(())
}

fn aborted_source_record(
    request: forge_domain::HomeTransferSourceRequest,
    issuer: Uuid,
    observed_at: i64,
) -> SourceDispatchResult<HomeTransferSourceRecord> {
    let mut parts = HomeTransferSourceRecordParts {
        request,
        issuer: OrchestrationIssuerId::from_uuid(issuer),
        state: HomeTransferSourceState::AbortedUnstarted,
        observed_at: TimestampMillis::new(observed_at)?,
        grant_issued_at: TimestampMillis::new(10)?,
        grant_expires_at: TimestampMillis::new(30)?,
        migration_id: Uuid::from_u128(0xA150),
        freeze_revision: 1,
        delegation_digest: [0xA8; 32],
        inventory_payload: None,
        inventory_digest: None,
        outcome_digest: [0; 32],
    };
    let mut digest = Sha256::new();
    digest.update(b"forge.home-transfer.source.outcome.v1\0");
    digest.update(parts.request.request_digest().as_bytes());
    digest.update(parts.issuer.into_uuid().as_bytes());
    digest.update(1_i64.to_be_bytes());
    digest.update(parts.observed_at.get().to_be_bytes());
    digest.update(parts.grant_issued_at.get().to_be_bytes());
    digest.update(parts.grant_expires_at.get().to_be_bytes());
    digest.update(parts.migration_id.as_bytes());
    digest.update(parts.freeze_revision.to_be_bytes());
    digest.update(parts.delegation_digest);
    digest.update([0]);
    parts.outcome_digest = digest.finalize().into();
    Ok(HomeTransferSourceRecord::from_parts(parts)?)
}
