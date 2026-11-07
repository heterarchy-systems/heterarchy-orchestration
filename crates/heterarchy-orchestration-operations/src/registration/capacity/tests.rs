use super::{ensure_admission_capacity, workspace_agent_state};
use crate::binding_observation::BindingReservationObservation;
use crate::catalog::OperationsCatalog;
use crate::{
    OperationsError, RegistrationPhase, RegistrationRequest, native_session_participation_id,
};
use forge_domain::{
    ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentSessionId,
    AgentTransportKind, AuthenticatedPrincipal, TimestampMillis, WorkspaceId,
};
use forge_persistence::AgentBindingReservationRequest;
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, TransactionBehavior, params};
use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;
use uuid::Uuid;

struct Fixture {
    root: PathBuf,
    database_path: PathBuf,
    destination: Uuid,
    workspace: WorkspaceId,
    migration: Uuid,
}

impl Fixture {
    fn new(seed: u128) -> Result<Self, Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-workspace-capacity-{}-{}",
            std::process::id(),
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let database_path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(seed);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(seed + 1));
        let migration = Uuid::from_u128(seed + 2);
        let catalog = OperationsCatalog::open(&database_path, destination)?;
        let digest = [0_u8; 32];
        catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
                digest.as_slice(),
            ],
        )?;
        drop(catalog);
        Ok(Self {
            root,
            database_path,
            destination,
            workspace,
            migration,
        })
    }

    fn open(&self) -> Result<OperationsCatalog, OperationsError> {
        OperationsCatalog::open(&self.database_path, self.destination)
    }

    fn cleanup(self) -> Result<(), Box<dyn Error>> {
        std::fs::remove_dir_all(self.root)?;
        Ok(())
    }
}

fn native_request(
    workspace: WorkspaceId,
    index: u128,
    display_name: &str,
    declared_role: &str,
) -> Result<RegistrationRequest, Box<dyn Error>> {
    let base = 0x00A0_0000 + index * 0x10;
    let request_id = Uuid::from_u128(base + 1);
    let activity_id = ActivityId::from_uuid(Uuid::from_u128(base + 2));
    let agent_id = AgentInstanceId::from_uuid(Uuid::from_u128(base + 3));
    let session_id = AgentSessionId::from_uuid(Uuid::from_u128(base + 4));
    let requested_at = TimestampMillis::new(100 + i64::try_from(index)?)?;
    let binding = AgentBindingReservationRequest::new(
        request_id,
        activity_id,
        agent_id,
        session_id,
        AuthenticatedPrincipal::TrustedStdio,
        requested_at,
    )?;
    let participant_id =
        native_session_participation_id(workspace.into_uuid(), session_id.into_uuid());
    Ok(RegistrationRequest::new(
        participant_id,
        binding,
        AgentDisplayName::new(display_name.to_owned())?,
        AgentDeclaredRole::new(declared_role.to_owned())?,
        AgentTransportKind::StreamableHttp,
    )?)
}

fn reserve_and_commit(
    catalog: &mut OperationsCatalog,
    fixture: &Fixture,
    request: &RegistrationRequest,
    reserved_at_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let reserved_at = TimestampMillis::new(reserved_at_ms)?;
    let reservation = BindingReservationObservation::new(
        fixture.migration,
        fixture.destination,
        fixture.workspace,
        1,
        request.binding().clone(),
        reserved_at,
    )?;
    let _reserved = catalog.record_registration_reservation_observation(&reservation)?;
    let _commit = catalog.commit_registration_membership(
        request.binding().request_id(),
        &[],
        TimestampMillis::new(reserved_at_ms + 1)?,
    )?;
    Ok(())
}

fn seed_legacy_pending_registration(
    catalog: &mut OperationsCatalog,
    fixture: &Fixture,
    request: &RegistrationRequest,
) -> Result<(), OperationsError> {
    let binding = request.binding();
    let request_id = binding.request_id();
    let encoded = request.encode()?;
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    super::super::insert_pending_identity(&transaction, fixture.workspace, request)?;
    transaction.execute(
        "INSERT INTO ops_registration_requests(request_id,migration_id,workspace_id,destination_id,activity_id,session_id,participant_id,agent_id,request_wire,phase,reserved_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,NULL)",
        params![
            request_id.as_bytes(),
            fixture.migration.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            fixture.destination.as_bytes(),
            binding.activity_id().into_uuid().as_bytes(),
            binding.session_id().into_uuid().as_bytes(),
            request.proposed_participant_id().as_bytes(),
            binding.agent_id().into_uuid().as_bytes(),
            encoded.as_slice(),
            RegistrationPhase::Pending.encode(),
        ],
    )?;
    super::super::commit_registration(transaction, request_id)
}

fn count_workspace_rows(
    connection: &Connection,
    table: &str,
    workspace: WorkspaceId,
) -> Result<i64, rusqlite::Error> {
    let query = format!("SELECT COUNT(*) FROM {table} WHERE workspace_id=?1");
    connection.query_row(&query, params![workspace.into_uuid().as_bytes()], |row| {
        row.get(0)
    })
}

fn insert_legacy_logical_member(
    connection: &Connection,
    workspace: WorkspaceId,
    participant_id: Uuid,
    session_id: Uuid,
    display_name: &str,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,10)",
        params![
            workspace.into_uuid().as_bytes(),
            participant_id.as_bytes(),
            session_id.as_bytes(),
            display_name,
        ],
    )?;
    Ok(())
}

fn insert_profile_anchor(
    connection: &Connection,
    workspace: WorkspaceId,
    profile_id: Uuid,
    participation_id: Uuid,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,10,1)",
        params![
            workspace.into_uuid().as_bytes(),
            profile_id.as_bytes(),
            participation_id.as_bytes(),
        ],
    )?;
    Ok(())
}

fn insert_profile_session_mapping(
    connection: &Connection,
    workspace: WorkspaceId,
    profile_id: Uuid,
    participation_id: Uuid,
    participant_id: Uuid,
    session_id: Uuid,
    seed: u128,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,20)",
        params![
            Uuid::from_u128(seed).as_bytes(),
            workspace.into_uuid().as_bytes(),
            participation_id.as_bytes(),
            profile_id.as_bytes(),
            Uuid::from_u128(seed + 1).as_bytes(),
            session_id.as_bytes(),
            participant_id.as_bytes(),
        ],
    )?;
    Ok(())
}

fn seed_orchestrator_and_ten_pending_agents(
    catalog: &mut OperationsCatalog,
    fixture: &Fixture,
) -> Result<(), Box<dyn Error>> {
    let orchestrator = native_request(
        fixture.workspace,
        0,
        "Workspace Orchestrator",
        "coordinator",
    )?;
    let _prepared = catalog.prepare_registration(fixture.migration, &orchestrator)?;
    reserve_and_commit(catalog, fixture, &orchestrator, 1_000)?;
    catalog.connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,1,NULL,NULL,1002)",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            orchestrator.proposed_participant_id().as_bytes(),
        ],
    )?;

    for index in 1_u128..=10 {
        let request = native_request(
            fixture.workspace,
            index,
            &format!("Workspace Agent {index}"),
            "worker",
        )?;
        let _pending = catalog.prepare_registration(fixture.migration, &request)?;
    }
    Ok(())
}

fn assert_registration_rejection_is_atomic(
    catalog: &mut OperationsCatalog,
    fixture: &Fixture,
    request: &RegistrationRequest,
) -> Result<(), Box<dyn Error>> {
    let pending_before = count_workspace_rows(
        &catalog.connection,
        "ops_pending_participants",
        fixture.workspace,
    )?;
    let requests_before = count_workspace_rows(
        &catalog.connection,
        "ops_registration_requests",
        fixture.workspace,
    )?;
    assert!(matches!(
        catalog.prepare_registration(fixture.migration, request),
        Err(OperationsError::Bounds("workspace logical Agent capacity"))
    ));
    assert_eq!(
        count_workspace_rows(
            &catalog.connection,
            "ops_pending_participants",
            fixture.workspace
        )?,
        pending_before
    );
    assert_eq!(
        count_workspace_rows(
            &catalog.connection,
            "ops_registration_requests",
            fixture.workspace
        )?,
        requests_before
    );
    assert!(
        catalog
            .registration_progress(request.binding().request_id())?
            .is_none()
    );
    Ok(())
}

#[test]
fn workspace_limit_counts_pending_and_committed_once_and_preserves_overcap_replays()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(0xD3000)?;
    let mut catalog = fixture.open()?;
    seed_orchestrator_and_ten_pending_agents(&mut catalog, &fixture)?;
    assert_eq!(
        count_workspace_rows(
            &catalog.connection,
            "ops_logical_members",
            fixture.workspace
        )?,
        1
    );
    assert_eq!(
        count_workspace_rows(
            &catalog.connection,
            "ops_pending_participants",
            fixture.workspace
        )?,
        11
    );
    assert_eq!(
        workspace_agent_state(&catalog.connection, fixture.workspace)?,
        11
    );

    let twelfth = native_request(fixture.workspace, 11, "Workspace Agent 11", "worker")?;
    let twelfth_progress = catalog.prepare_registration(fixture.migration, &twelfth)?;
    assert_eq!(
        workspace_agent_state(&catalog.connection, fixture.workspace)?,
        12
    );
    let replay = catalog.prepare_registration(fixture.migration, &twelfth)?;
    assert_eq!(replay, twelfth_progress);

    let thirteenth = native_request(fixture.workspace, 12, "Workspace Agent 12", "worker")?;
    assert_registration_rejection_is_atomic(&mut catalog, &fixture, &thirteenth)?;

    seed_legacy_pending_registration(&mut catalog, &fixture, &thirteenth)?;
    assert_eq!(
        workspace_agent_state(&catalog.connection, fixture.workspace)?,
        13
    );
    let legacy_progress = catalog
        .registration_progress(thirteenth.binding().request_id())?
        .ok_or_else(|| std::io::Error::other("legacy over-cap registration"))?;
    assert_eq!(
        catalog.prepare_registration(fixture.migration, &thirteenth)?,
        legacy_progress
    );

    let fourteenth = native_request(fixture.workspace, 13, "Workspace Agent 13", "worker")?;
    assert_registration_rejection_is_atomic(&mut catalog, &fixture, &fourteenth)?;
    drop(catalog);
    fixture.cleanup()?;
    Ok(())
}

#[test]
fn profile_sessions_share_one_slot_and_native_participants_keep_distinct_ids()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(0xD4000)?;
    let mut catalog = fixture.open()?;
    let profile_id = Uuid::from_u128(0xD4100);
    let participation_id = Uuid::from_u128(0xD4101);
    insert_profile_anchor(
        &catalog.connection,
        fixture.workspace,
        profile_id,
        participation_id,
    )?;
    let profile_members = [
        (Uuid::from_u128(0xD4200), Uuid::from_u128(0xD4300)),
        (Uuid::from_u128(0xD4201), Uuid::from_u128(0xD4301)),
    ];
    for (index, (participant, session)) in profile_members.into_iter().enumerate() {
        insert_legacy_logical_member(
            &catalog.connection,
            fixture.workspace,
            participant,
            session,
            "Profile-linked legacy Agent",
        )?;
        insert_profile_session_mapping(
            &catalog.connection,
            fixture.workspace,
            profile_id,
            participation_id,
            participant,
            session,
            0xD4400 + u128::try_from(index)? * 0x10,
        )?;
    }
    for index in 0_u128..11 {
        let session = Uuid::from_u128(0xD4500 + index);
        insert_legacy_logical_member(
            &catalog.connection,
            fixture.workspace,
            native_session_participation_id(fixture.workspace.into_uuid(), session),
            session,
            "Profileless legacy Agent",
        )?;
    }

    assert_eq!(
        workspace_agent_state(&catalog.connection, fixture.workspace)?,
        12
    );
    let profile_id = AgentProfileId::from_uuid(profile_id)?;
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_admission_capacity(
        &transaction,
        fixture.workspace,
        Some(profile_id),
        profile_members[0].0,
    )?;
    let new_session = Uuid::from_u128(0xD4600);
    assert!(matches!(
        ensure_admission_capacity(
            &transaction,
            fixture.workspace,
            None,
            native_session_participation_id(fixture.workspace.into_uuid(), new_session),
        ),
        Err(OperationsError::Bounds("workspace logical Agent capacity"))
    ));
    transaction.rollback()?;
    drop(catalog);
    fixture.cleanup()?;
    Ok(())
}

fn race_registration(
    mut catalog: OperationsCatalog,
    migration: Uuid,
    request: &RegistrationRequest,
    barrier: &Barrier,
) -> Result<bool, String> {
    let _wait_result = barrier.wait();
    match catalog.prepare_registration(migration, request) {
        Ok(_) => Ok(true),
        Err(OperationsError::Bounds("workspace logical Agent capacity")) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

#[test]
fn two_connections_reserve_the_last_workspace_agent_slot_once() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(0xD5000)?;
    let mut seeded = fixture.open()?;
    for index in 0_u128..11 {
        let request = native_request(
            fixture.workspace,
            index,
            &format!("Pending Workspace Agent {index}"),
            "worker",
        )?;
        let _pending = seeded.prepare_registration(fixture.migration, &request)?;
    }
    assert_eq!(
        workspace_agent_state(&seeded.connection, fixture.workspace)?,
        11
    );
    drop(seeded);

    let left_catalog = fixture.open()?;
    let right_catalog = fixture.open()?;
    left_catalog
        .connection
        .busy_timeout(Duration::from_secs(5))?;
    right_catalog
        .connection
        .busy_timeout(Duration::from_secs(5))?;
    let left_request = native_request(fixture.workspace, 20, "Racing Agent Left", "worker")?;
    let right_request = native_request(fixture.workspace, 21, "Racing Agent Right", "worker")?;
    let barrier = Arc::new(Barrier::new(2));
    let migration = fixture.migration;
    let left_worker = {
        let barrier = Arc::clone(&barrier);
        let request = left_request.clone();
        std::thread::spawn(move || race_registration(left_catalog, migration, &request, &barrier))
    };
    let right_worker = {
        let barrier = Arc::clone(&barrier);
        let request = right_request.clone();
        std::thread::spawn(move || race_registration(right_catalog, migration, &request, &barrier))
    };
    let left_result = left_worker.join();
    let right_result = right_worker.join();
    let left_created = left_result
        .map_err(|_| std::io::Error::other("left registration worker panicked"))?
        .map_err(std::io::Error::other)?;
    let right_created = right_result
        .map_err(|_| std::io::Error::other("right registration worker panicked"))?
        .map_err(std::io::Error::other)?;
    assert_ne!(left_created, right_created);

    let (winner, loser) = if left_created {
        (left_request, right_request)
    } else {
        (right_request, left_request)
    };
    let mut verification = fixture.open()?;
    assert_eq!(
        workspace_agent_state(&verification.connection, fixture.workspace)?,
        12
    );
    assert_eq!(
        count_workspace_rows(
            &verification.connection,
            "ops_pending_participants",
            fixture.workspace
        )?,
        12
    );
    assert_eq!(
        count_workspace_rows(
            &verification.connection,
            "ops_registration_requests",
            fixture.workspace
        )?,
        12
    );
    assert!(
        verification
            .registration_progress(loser.binding().request_id())?
            .is_none()
    );
    let replay = verification.prepare_registration(fixture.migration, &winner)?;
    assert_eq!(
        workspace_agent_state(&verification.connection, fixture.workspace)?,
        12
    );
    assert_eq!(
        count_workspace_rows(
            &verification.connection,
            "ops_registration_requests",
            fixture.workspace
        )?,
        12
    );
    assert_eq!(replay.request(), &winner);

    drop(verification);
    fixture.cleanup()?;
    Ok(())
}
