use super::*;
use crate::{
    TeamLeaveCommand, TeamPermanentTransferConsentCommand, TeamPermanentTransferConsentKind,
    TeamPermanentTransferIntentCommand, TeamPermanentTransferPreparationCommand,
    TeamPermanentTransferRenewalCommand,
};

type PreparationResult<T = ()> = Result<T, Box<dyn Error>>;

fn record_consent(
    fixture: &mut PermanentTransferIntentFixture,
    id: u128,
    kind: TeamPermanentTransferConsentKind,
    actor: OperationalActor,
    now: i64,
) -> PreparationResult<crate::TeamPermanentTransferConsentReceipt> {
    Ok(fixture.catalog.record_team_permanent_transfer_consent(
        &TeamPermanentTransferConsentCommand::new(
            Uuid::from_u128(id),
            WORKSPACE,
            fixture.intent.transfer_id(),
            kind,
            actor,
        )?,
        || timestamp(now),
    )?)
}

fn preparation_command(
    fixture: &PermanentTransferIntentFixture,
    preparation: u128,
    source_consent: Uuid,
    target_acceptance: Uuid,
) -> Result<TeamPermanentTransferPreparationCommand, OperationsError> {
    TeamPermanentTransferPreparationCommand::new(
        Uuid::from_u128(preparation),
        WORKSPACE,
        fixture.intent.transfer_id(),
        1,
        1,
        source_consent,
        target_acceptance,
        orchestrator(),
    )
}

fn add_preparation_owner_schemas(fixture: &mut PermanentTransferIntentFixture) -> TestResult {
    for (kind, name, sql) in crate::membership::schema_objects() {
        let exists: bool = fixture.catalog.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type=?1 AND name=?2)",
            params![kind, name],
            |row| row.get(0),
        )?;
        if !exists {
            fixture.catalog.connection.execute_batch(sql)?;
        }
    }
    let has_member_history: bool = fixture.catalog.connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('ops_logical_members') WHERE name='membership_revision')",
        [],
        |row| row.get(0),
    )?;
    if !has_member_history {
        prepare_session_rebind_test_columns(&fixture.catalog.connection)?;
    }
    let has_rebinds: bool = fixture.catalog.connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_workspace_participation_session_rebinds')",
        [],
        |row| row.get(0),
    )?;
    if !has_rebinds {
        crate::session_replacement::create_schema(&fixture.catalog.connection)?;
    }
    let has_replacements: bool = fixture.catalog.connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_workspace_participation_replacement_registrations')",
        [], |row| row.get(0),
    )?;
    if !has_replacements {
        crate::replacement_registration::create_schema(&fixture.catalog.connection)?;
    }
    let has_dispatch: bool = fixture.catalog.connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_temporary_dispatch_reservations')",
        [],
        |row| row.get(0),
    )?;
    if !has_dispatch {
        crate::temporary_dispatch::lifecycle::create_lifecycle_schema(&fixture.catalog.connection)?;
    }
    let has_preparation: bool = fixture.catalog.connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_team_permanent_transfer_preparations')",
        [],
        |row| row.get(0),
    )?;
    if !has_preparation {
        crate::team_permanent_transfer::preparation::schema::create_schema(
            &fixture.catalog.connection,
        )?;
    }
    Ok(())
}

fn both_consents(
    fixture: &mut PermanentTransferIntentFixture,
    seed: u128,
) -> PreparationResult<(Uuid, Uuid)> {
    let source = record_consent(
        fixture,
        seed,
        TeamPermanentTransferConsentKind::SourceHomeLeader,
        seventh_participant(),
        500,
    )?;
    let target = record_consent(
        fixture,
        seed + 1,
        TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        sixth_participant(),
        501,
    )?;
    Ok((source.receipt_id(), target.receipt_id()))
}

fn preparation_rows(
    fixture: &PermanentTransferIntentFixture,
) -> PreparationResult<(i64, i64, i64)> {
    Ok((
        count_rows(
            &fixture.catalog.connection,
            "ops_team_permanent_transfer_preparations",
        )?,
        count_rows(
            &fixture.catalog.connection,
            "ops_team_permanent_transfer_preparation_barriers",
        )?,
        count_rows(
            &fixture.catalog.connection,
            "ops_team_permanent_transfer_preparation_sessions",
        )?,
    ))
}

fn leave_target(
    fixture: &mut PermanentTransferIntentFixture,
    request_id: u128,
) -> Result<crate::TeamMutationRecord, OperationsError> {
    let team = fixture
        .catalog
        .team(WORKSPACE, fixture.source.source_team_id)?
        .ok_or(OperationsError::Conflict("source Team missing"))?;
    fixture.catalog.leave_team(TeamLeaveCommand {
        request_id: Uuid::from_u128(request_id),
        workspace_id: WORKSPACE,
        team_id: fixture.source.source_team_id,
        expected_team_revision: team.revision(),
        actor: sixth_participant(),
        observed_at: timestamp(510)?,
    })
}

fn seed_synthetic_author_profile_alias(
    fixture: &PermanentTransferIntentFixture,
) -> PreparationResult<OperationalActor> {
    let alias_actor = actor(
        Uuid::from_u128(0xFD80),
        Uuid::from_u128(0xFD81),
        Uuid::from_u128(0xFD82),
    );
    let OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    } = alias_actor
    else {
        return Err("synthetic Orchestrator alias requires a participant actor".into());
    };
    let connection = &fixture.catalog.connection;
    let alias_participant = Uuid::from_u128(0xFD86);
    // Model the legal W Profile/Participation alias with a separate actor; the
    // current Orchestrator's original participant and authority stay untouched.
    connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,display_name,principal_kind,principal_id,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,'Synthetic Orchestrator alias',0,NULL,'orchestrator',1,1,1)",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            alias_participant.as_bytes(),
            session_id.into_uuid().as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,2)",
        params![
            Uuid::from_u128(0xFD83).as_bytes(),
            WORKSPACE.into_uuid().as_bytes(),
            ORCHESTRATOR_PARTICIPATION.as_bytes(),
            ORCHESTRATOR_PROFILE.as_bytes(),
            Uuid::from_u128(0xFD84).as_bytes(),
            session_id.into_uuid().as_bytes(),
            alias_participant.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            alias_participant.as_bytes(),
            activity_id.into_uuid().as_bytes(),
            session_id.into_uuid().as_bytes(),
            agent_id.into_uuid().as_bytes(),
            Uuid::from_u128(0xFD85).as_bytes(),
        ],
    )?;
    Ok(alias_actor)
}

fn assert_preparation_receipt_and_history(
    fixture: &PermanentTransferIntentFixture,
    receipt: &crate::TeamPermanentTransferPreparationReceipt,
    source_consent: Uuid,
    target_acceptance: Uuid,
) -> TestResult {
    assert!(!receipt.replayed());
    assert_eq!(receipt.transfer_id(), fixture.intent.transfer_id());
    assert_eq!(receipt.generation(), 1);
    assert_eq!(receipt.workspace_leadership_revision(), 1);
    assert_eq!(receipt.source_leader_consent_id(), source_consent);
    assert_eq!(receipt.target_acceptance_id(), target_acceptance);
    assert_eq!(
        receipt.target_participation_id().into_uuid(),
        MEMBER_SIX_PARTICIPATION
    );
    assert_eq!(receipt.target_profile_id(), MEMBER_SIX_PROFILE);
    assert_eq!(receipt.sessions().count(), 2);
    assert_eq!(receipt.bindings().len(), 2);
    assert!(
        receipt
            .bindings()
            .all(|binding| !binding.participant_id().is_nil())
    );
    assert_eq!(preparation_rows(fixture)?, (1, 1, 2));

    let generation = fixture
        .catalog
        .read_team_permanent_transfer_generation(
            WORKSPACE,
            fixture.intent.transfer_id(),
            orchestrator(),
        )?
        .ok_or("prepared generation missing")?;
    assert_eq!(
        generation.generation().state(),
        crate::TeamPermanentTransferGenerationState::Preparing
    );
    assert_eq!(
        fixture.catalog.read_team_permanent_transfer_preparation(
            WORKSPACE,
            receipt.preparation_id(),
            orchestrator(),
        )?,
        Some(receipt.clone())
    );
    assert_eq!(
        fixture.catalog.read_team_permanent_transfer_preparation(
            WORKSPACE,
            receipt.preparation_id(),
            fifth_participant(),
        )?,
        None
    );
    Ok(())
}

fn assert_target_admission_is_fenced(
    fixture: &mut PermanentTransferIntentFixture,
    receipt: &crate::TeamPermanentTransferPreparationReceipt,
) {
    for session in receipt.sessions() {
        assert!(
            crate::team_permanent_transfer::preparation::ensure_session_open(
                &fixture.catalog.connection,
                WORKSPACE,
                session,
            )
            .is_err()
        );
    }
    assert!(
        crate::team_permanent_transfer::preparation::ensure_profile_open(
            &fixture.catalog.connection,
            WORKSPACE,
            MEMBER_SIX_PROFILE,
        )
        .is_err()
    );
    assert!(matches!(
        leave_target(fixture, 0xFD23),
        Err(OperationsError::Conflict(_))
    ));
}

fn assert_exact_replay_and_home_preservation(
    fixture: &mut PermanentTransferIntentFixture,
    command: &TeamPermanentTransferPreparationCommand,
    receipt: &crate::TeamPermanentTransferPreparationReceipt,
    source_consent: Uuid,
    target_acceptance: Uuid,
    members_before: (i64, i64),
    hold_before: usize,
) -> TestResult {
    let replay = fixture
        .catalog
        .prepare_team_permanent_transfer(command, || timestamp(1))?;
    assert!(replay.replayed());
    assert_eq!(replay.digest(), receipt.digest());
    for altered in [
        TeamPermanentTransferPreparationCommand::new(
            receipt.preparation_id(),
            WORKSPACE,
            receipt.transfer_id(),
            2,
            1,
            source_consent,
            target_acceptance,
            orchestrator(),
        )?,
        TeamPermanentTransferPreparationCommand::new(
            receipt.preparation_id(),
            WORKSPACE,
            receipt.transfer_id(),
            1,
            1,
            source_consent,
            Uuid::from_u128(0xFD24),
            orchestrator(),
        )?,
    ] {
        assert!(matches!(
            fixture
                .catalog
                .prepare_team_permanent_transfer(&altered, || timestamp(503)),
            Err(OperationsError::Conflict(_))
        ));
    }
    assert_eq!(preparation_rows(fixture)?, (1, 1, 2));
    assert_eq!(
        team_member_counts(
            &fixture.catalog.connection,
            WORKSPACE,
            fixture.source.source_team_id,
        )?,
        members_before
    );
    assert_eq!(
        super::super::workspace_hold::workspace_capacity_snapshot(
            &fixture.catalog.connection,
            WORKSPACE,
        )?
        .used(),
        hold_before
    );
    assert_eq!(
        count_rows(&fixture.catalog.connection, "ops_team_expansion_slot_uses")?,
        0
    );
    Ok(())
}

#[test]
fn preparation_pins_all_target_alias_sessions_and_blocks_home_leave() -> TestResult {
    let mut fixture = prepare_permanent_transfer_intent()?;
    add_preparation_owner_schemas(&mut fixture)?;
    let (source_consent, target_acceptance) = both_consents(&mut fixture, 0xFD20)?;
    let hold_before = super::super::workspace_hold::workspace_capacity_snapshot(
        &fixture.catalog.connection,
        WORKSPACE,
    )?
    .used();
    let members_before = team_member_counts(
        &fixture.catalog.connection,
        WORKSPACE,
        fixture.source.source_team_id,
    )?;
    let command = preparation_command(&fixture, 0xFD22, source_consent, target_acceptance)?;

    let receipt = fixture
        .catalog
        .prepare_team_permanent_transfer(&command, || timestamp(502))?;
    assert_preparation_receipt_and_history(&fixture, &receipt, source_consent, target_acceptance)?;
    assert_target_admission_is_fenced(&mut fixture, &receipt);
    assert_exact_replay_and_home_preservation(
        &mut fixture,
        &command,
        &receipt,
        source_consent,
        target_acceptance,
        members_before,
        hold_before,
    )?;
    Ok(())
}

#[test]
fn preparation_requires_both_original_consents_and_exact_generation() -> TestResult {
    let mut fixture = prepare_permanent_transfer_intent()?;
    add_preparation_owner_schemas(&mut fixture)?;
    let target = record_consent(
        &mut fixture,
        0xFD30,
        TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        sixth_participant(),
        501,
    )?;
    let missing_source = preparation_command(
        &fixture,
        0xFD32,
        Uuid::from_u128(0xFD31),
        target.receipt_id(),
    )?;
    assert!(matches!(
        fixture
            .catalog
            .prepare_team_permanent_transfer(&missing_source, || timestamp(502)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(preparation_rows(&fixture)?, (0, 0, 0));

    record_consent(
        &mut fixture,
        0xFD33,
        TeamPermanentTransferConsentKind::SourceHomeLeader,
        seventh_participant(),
        503,
    )?;
    let wrong_source_receipt = preparation_command(
        &fixture,
        0xFD34,
        Uuid::from_u128(0xFD35),
        target.receipt_id(),
    )?;
    assert!(matches!(
        fixture
            .catalog
            .prepare_team_permanent_transfer(&wrong_source_receipt, || timestamp(504)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(preparation_rows(&fixture)?, (0, 0, 0));

    let wrong_generation = TeamPermanentTransferPreparationCommand::new(
        Uuid::from_u128(0xFD36),
        WORKSPACE,
        fixture.intent.transfer_id(),
        2,
        1,
        Uuid::from_u128(0xFD33),
        target.receipt_id(),
        orchestrator(),
    )?;
    assert!(matches!(
        fixture
            .catalog
            .prepare_team_permanent_transfer(&wrong_generation, || timestamp(504)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(preparation_rows(&fixture)?, (0, 0, 0));
    Ok(())
}

#[test]
fn stale_leader_consent_leaves_the_intent_reusable_until_a_valid_prepare() -> TestResult {
    let mut fixture = prepare_permanent_transfer_intent()?;
    add_preparation_owner_schemas(&mut fixture)?;
    let (source, target) = both_consents(&mut fixture, 0xFD40)?;
    replace_team_leader(
        &fixture.catalog.connection,
        fixture.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let command = preparation_command(&fixture, 0xFD42, source, target)?;
    assert!(matches!(
        fixture
            .catalog
            .prepare_team_permanent_transfer(&command, || timestamp(600)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(preparation_rows(&fixture)?, (0, 0, 0));
    assert_eq!(
        fixture
            .catalog
            .read_team_permanent_transfer_generation(
                WORKSPACE,
                fixture.intent.transfer_id(),
                orchestrator(),
            )?
            .ok_or("root generation missing")?
            .generation()
            .state(),
        crate::TeamPermanentTransferGenerationState::IntentOnly,
    );
    let renewal = TeamPermanentTransferRenewalCommand::new(
        fixture.intent.transfer_id(),
        1,
        TeamPermanentTransferIntentCommand::new(
            Uuid::from_u128(0xFD43),
            crate::TeamPermanentTransferScope::new(
                WORKSPACE,
                fixture.source.source_team_id,
                SECOND_TEAM_ID,
                WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
            ),
            crate::TeamPermanentTransferDestinationSlot::new(
                fixture.intent.destination_request_id(),
                fixture.intent.destination_decision_id(),
                fixture.intent.destination_ordinal(),
            ),
            crate::TeamPermanentTransferRevisions::new(
                fixture.source.destination_a.team().revision(),
                1,
                1,
                1,
                2,
            ),
            orchestrator(),
        )?,
    )?;
    assert_eq!(
        fixture
            .catalog
            .renew_team_permanent_transfer(&renewal, || timestamp(601))?
            .generation()
            .number(),
        2,
    );
    assert_eq!(preparation_rows(&fixture)?, (0, 0, 0));
    Ok(())
}

#[test]
fn preparation_clock_failures_leave_no_barrier_and_same_id_replay_skips_clock() -> TestResult {
    let mut fixture = prepare_permanent_transfer_intent()?;
    add_preparation_owner_schemas(&mut fixture)?;
    let (source, target) = both_consents(&mut fixture, 0xFD50)?;
    let command = preparation_command(&fixture, 0xFD52, source, target)?;

    assert!(matches!(
        fixture
            .catalog
            .prepare_team_permanent_transfer(&command, || {
                Err(OperationsError::Conflict("synthetic trusted clock failure"))
            }),
        Err(OperationsError::Conflict("synthetic trusted clock failure"))
    ));
    assert_eq!(preparation_rows(&fixture)?, (0, 0, 0));

    assert!(matches!(
        fixture
            .catalog
            .prepare_team_permanent_transfer(&command, || timestamp(500)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(preparation_rows(&fixture)?, (0, 0, 0));

    let receipt = fixture
        .catalog
        .prepare_team_permanent_transfer(&command, || timestamp(502))?;
    let called = std::cell::Cell::new(false);
    let replay = fixture
        .catalog
        .prepare_team_permanent_transfer(&command, || {
            called.set(true);
            Err(OperationsError::Conflict("replay must not read clock"))
        })?;
    assert!(!called.get());
    assert!(replay.replayed());
    assert_eq!(replay.digest(), receipt.digest());
    assert_eq!(preparation_rows(&fixture)?, (1, 1, 2));
    Ok(())
}

#[test]
fn prepared_transfer_keeps_its_generation_and_original_consents_after_leader_change() -> TestResult
{
    let mut fixture = prepare_permanent_transfer_intent()?;
    add_preparation_owner_schemas(&mut fixture)?;
    let (source_consent, target_acceptance) = both_consents(&mut fixture, 0xFD60)?;
    let command = preparation_command(&fixture, 0xFD62, source_consent, target_acceptance)?;
    let preparation = fixture
        .catalog
        .prepare_team_permanent_transfer(&command, || timestamp(502))?;

    replace_team_leader(
        &fixture.catalog.connection,
        fixture.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let child = TeamPermanentTransferIntentCommand::new(
        Uuid::from_u128(0xFD63),
        crate::TeamPermanentTransferScope::new(
            WORKSPACE,
            fixture.source.source_team_id,
            SECOND_TEAM_ID,
            WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
        ),
        crate::TeamPermanentTransferDestinationSlot::new(
            fixture.intent.destination_request_id(),
            fixture.intent.destination_decision_id(),
            fixture.intent.destination_ordinal(),
        ),
        crate::TeamPermanentTransferRevisions::new(
            fixture.source.destination_a.team().revision(),
            1,
            1,
            1,
            2,
        ),
        orchestrator(),
    )?;
    let renewal = TeamPermanentTransferRenewalCommand::new(fixture.intent.transfer_id(), 1, child)?;
    assert!(matches!(
        fixture
            .catalog
            .renew_team_permanent_transfer(&renewal, || timestamp(601)),
        Err(OperationsError::Conflict(_))
    ));

    let snapshot = fixture
        .catalog
        .read_team_permanent_transfer_snapshot(
            WORKSPACE,
            fixture.intent.transfer_id(),
            orchestrator(),
        )?
        .ok_or("prepared transfer intent history missing")?;
    assert_eq!(snapshot.intent(), &fixture.intent);
    assert_eq!(
        snapshot
            .source_home_leader_consent()
            .map(crate::TeamPermanentTransferConsentReceipt::receipt_id,),
        Some(source_consent),
    );
    assert_eq!(
        snapshot
            .target_agent_acceptance()
            .map(crate::TeamPermanentTransferConsentReceipt::receipt_id,),
        Some(target_acceptance),
    );
    assert_eq!(
        fixture.catalog.read_team_permanent_transfer_preparation(
            WORKSPACE,
            preparation.preparation_id(),
            orchestrator(),
        )?,
        Some(preparation),
    );
    let original_generation = fixture
        .catalog
        .read_team_permanent_transfer_generation(
            WORKSPACE,
            fixture.intent.transfer_id(),
            orchestrator(),
        )?
        .ok_or("prepared root generation history missing")?;
    assert_eq!(original_generation.generation().number(), 1);
    assert_eq!(
        original_generation.generation().state(),
        crate::TeamPermanentTransferGenerationState::Preparing
    );
    assert_eq!(preparation_rows(&fixture)?, (1, 1, 2));
    Ok(())
}

#[test]
fn preparation_replay_pins_physical_author_while_current_author_can_read_history() -> TestResult {
    let mut fixture = prepare_permanent_transfer_intent()?;
    add_preparation_owner_schemas(&mut fixture)?;
    let (source, target) = both_consents(&mut fixture, 0xFD90)?;
    let command = preparation_command(&fixture, 0xFD92, source, target)?;
    let receipt = fixture
        .catalog
        .prepare_team_permanent_transfer(&command, || timestamp(502))?;
    let alias_actor = seed_synthetic_author_profile_alias(&fixture)?;
    let alias_replay = TeamPermanentTransferPreparationCommand::new(
        command.preparation_id(),
        command.workspace_id(),
        command.transfer_id(),
        command.expected_generation(),
        command.expected_workspace_leadership_revision(),
        command.source_leader_consent_id(),
        command.target_acceptance_id(),
        alias_actor,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .prepare_team_permanent_transfer(&alias_replay, || timestamp(503)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(preparation_rows(&fixture)?, (1, 1, 2));

    let readback = fixture
        .catalog
        .read_team_permanent_transfer_preparation(WORKSPACE, receipt.preparation_id(), alias_actor)?
        .ok_or("current Orchestrator alias could not read preparation history")?;
    assert_eq!(readback.actor(), command.actor());
    assert_eq!(
        readback.actor_participation_id(),
        receipt.actor_participation_id()
    );
    assert_eq!(readback.actor_profile_id(), receipt.actor_profile_id());
    assert_eq!(readback.digest(), receipt.digest());

    let original_replay = fixture
        .catalog
        .prepare_team_permanent_transfer(&command, || {
            Err(OperationsError::Conflict(
                "same-ID author replay must not need a new clock",
            ))
        })?;
    assert!(original_replay.replayed());
    assert_eq!(original_replay.actor(), command.actor());
    assert_eq!(original_replay.digest(), receipt.digest());
    assert_eq!(preparation_rows(&fixture)?, (1, 1, 2));
    Ok(())
}

#[test]
fn concurrent_preparations_for_one_transfer_commit_one_complete_barrier() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let mut fixture = prepare_permanent_transfer_intent_with_catalog(
        workspace_capacity_fixture_at(&database.path)?,
    )?;
    add_preparation_owner_schemas(&mut fixture)?;
    let (source, target) = both_consents(&mut fixture, 0xFD70)?;
    let first = preparation_command(&fixture, 0xFD72, source, target)?;
    let second = preparation_command(&fixture, 0xFD73, source, target)?;

    let open_writer = || -> PreparationResult<OperationsCatalog> {
        let connection = Connection::open(&database.path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        Ok(OperationsCatalog {
            connection,
            destination: DESTINATION,
        })
    };
    let first_catalog = open_writer()?;
    let second_catalog = open_writer()?;
    let barrier = Arc::new(Barrier::new(3));
    let (first_result, second_result) = std::thread::scope(|scope| {
        let first_barrier = Arc::clone(&barrier);
        let first_thread = scope.spawn(move || {
            let mut catalog = first_catalog;
            first_barrier.wait();
            catalog.prepare_team_permanent_transfer(&first, || timestamp(502))
        });
        let second_barrier = Arc::clone(&barrier);
        let second_thread = scope.spawn(move || {
            let mut catalog = second_catalog;
            second_barrier.wait();
            catalog.prepare_team_permanent_transfer(&second, || timestamp(503))
        });
        barrier.wait();
        (first_thread.join(), second_thread.join())
    });
    let first_result =
        first_result.map_err(|_| IoError::other("first preparation writer panicked"))?;
    let second_result =
        second_result.map_err(|_| IoError::other("second preparation writer panicked"))?;
    let first_won = first_result.is_ok();
    let second_won = second_result.is_ok();
    assert_ne!(
        first_won, second_won,
        "only one competing preparation may hold the target"
    );
    assert!(matches!(
        if first_won {
            second_result.as_ref()
        } else {
            first_result.as_ref()
        },
        Err(OperationsError::Conflict(_))
    ));

    assert_eq!(preparation_rows(&fixture)?, (1, 1, 2));
    let winner = if first_won {
        first_result?
    } else {
        second_result?
    };
    let durable = fixture
        .catalog
        .read_team_permanent_transfer_preparation(
            WORKSPACE,
            winner.preparation_id(),
            orchestrator(),
        )?
        .ok_or("winning preparation receipt was not durable")?;
    assert_eq!(durable.digest(), winner.digest());
    assert_eq!(durable.sessions().count(), 2);
    assert_eq!(durable.bindings().len(), 2);
    Ok(())
}

fn team_member_counts(
    connection: &rusqlite::Connection,
    workspace: WorkspaceId,
    team: Uuid,
) -> PreparationResult<(i64, i64)> {
    Ok(connection.query_row(
        "SELECT SUM(CASE WHEN state='JOINED' AND participation_id=?3 THEN 1 ELSE 0 END),COUNT(*) FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2",
        params![workspace.into_uuid().as_bytes(), team.as_bytes(), MEMBER_SIX_PARTICIPATION.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}
