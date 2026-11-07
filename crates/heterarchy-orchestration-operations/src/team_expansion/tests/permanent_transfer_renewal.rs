use super::*;
use crate::{
    TeamPermanentTransferConsentCommand, TeamPermanentTransferConsentKind,
    TeamPermanentTransferGenerationState, TeamPermanentTransferIntentCommand,
    TeamPermanentTransferRenewalCommand,
};

fn proposal(
    fixture: &PermanentTransferIntentFixture,
    identity: u128,
    leader_revision: i64,
    membership_revision: u64,
) -> Result<TeamPermanentTransferIntentCommand, OperationsError> {
    TeamPermanentTransferIntentCommand::new(
        Uuid::from_u128(identity),
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
            membership_revision,
            leader_revision,
        ),
        orchestrator(),
    )
}

fn renewal(
    fixture: &PermanentTransferIntentFixture,
    identity: u128,
) -> Result<TeamPermanentTransferRenewalCommand, OperationsError> {
    TeamPermanentTransferRenewalCommand::new(
        fixture.intent.transfer_id(),
        1,
        proposal(fixture, identity, 2, 1)?,
    )
}

fn record_consent(
    catalog: &mut OperationsCatalog,
    transfer: Uuid,
    receipt: u128,
    kind: TeamPermanentTransferConsentKind,
    actor: OperationalActor,
    now: i64,
) -> Result<crate::TeamPermanentTransferConsentReceipt, OperationsError> {
    catalog.record_team_permanent_transfer_consent(
        &TeamPermanentTransferConsentCommand::new(
            Uuid::from_u128(receipt),
            WORKSPACE,
            transfer,
            kind,
            actor,
        )?,
        || timestamp(now),
    )
}

#[test]
fn leader_change_creates_fresh_generation_and_preserves_original_consent_and_hold() -> TestResult {
    let mut f = prepare_permanent_transfer_intent()?;
    let old_consent = record_consent(
        &mut f.catalog,
        f.intent.transfer_id(),
        0xFC01,
        TeamPermanentTransferConsentKind::SourceHomeLeader,
        seventh_participant(),
        500,
    )?;
    let before_hold = super::super::workspace_hold::workspace_capacity_snapshot(
        &f.catalog.connection,
        WORKSPACE,
    )?
    .used();
    replace_team_leader(
        &f.catalog.connection,
        f.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let command = renewal(&f, 0xFC02)?;
    let updated = f
        .catalog
        .renew_team_permanent_transfer(&command, || timestamp(600))?;
    assert_eq!(updated.generation().number(), 2);
    assert_eq!(
        updated.generation().state(),
        TeamPermanentTransferGenerationState::IntentOnly
    );
    assert_eq!(
        updated.generation().predecessor_transfer_id(),
        Some(f.intent.transfer_id())
    );
    assert_eq!(
        updated.intent().source_leader_profile_id(),
        MEMBER_FOUR_PROFILE
    );
    assert_eq!(
        super::super::workspace_hold::workspace_capacity_snapshot(
            &f.catalog.connection,
            WORKSPACE
        )?
        .used(),
        before_hold
    );
    let old = f
        .catalog
        .read_team_permanent_transfer_generation(WORKSPACE, f.intent.transfer_id(), orchestrator())?
        .ok_or("old generation missing")?;
    assert_eq!(
        old.generation().state(),
        TeamPermanentTransferGenerationState::Superseded
    );
    assert_eq!(
        old.generation().head_transfer_id(),
        updated.intent().transfer_id()
    );
    assert!(
        f.catalog
            .propose_team_permanent_transfer(&f.proposal, || timestamp(1))?
            .replayed()
    );
    assert_eq!(
        f.catalog.read_team_permanent_transfer_consent(
            WORKSPACE,
            f.intent.transfer_id(),
            old_consent.receipt_id(),
            seventh_participant()
        )?,
        Some(old_consent)
    );
    assert!(
        record_consent(
            &mut f.catalog,
            f.intent.transfer_id(),
            0xFC01,
            TeamPermanentTransferConsentKind::SourceHomeLeader,
            seventh_participant(),
            1,
        )?
        .replayed()
    );
    assert!(matches!(
        record_consent(
            &mut f.catalog,
            f.intent.transfer_id(),
            0xFC03,
            TeamPermanentTransferConsentKind::TargetAgentAcceptance,
            sixth_participant(),
            610
        ),
        Err(OperationsError::Conflict(
            "permanent transfer generation is superseded"
        ))
    ));
    assert_new_generation_requires_consents_and_replays(&mut f, &updated, &command)
}

fn assert_new_generation_requires_consents_and_replays(
    f: &mut PermanentTransferIntentFixture,
    updated: &crate::TeamPermanentTransferGenerationSnapshot,
    command: &TeamPermanentTransferRenewalCommand,
) -> TestResult {
    let fresh = f
        .catalog
        .read_team_permanent_transfer_snapshot(
            WORKSPACE,
            updated.intent().transfer_id(),
            orchestrator(),
        )?
        .ok_or("new snapshot missing")?;
    assert_eq!(fresh.source_home_leader_consent(), None);
    assert_eq!(fresh.target_agent_acceptance(), None);
    record_consent(
        &mut f.catalog,
        updated.intent().transfer_id(),
        0xFC04,
        TeamPermanentTransferConsentKind::SourceHomeLeader,
        fourth_participant(),
        611,
    )?;
    record_consent(
        &mut f.catalog,
        updated.intent().transfer_id(),
        0xFC05,
        TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        sixth_participant(),
        612,
    )?;
    let replay = f
        .catalog
        .renew_team_permanent_transfer(command, || timestamp(1))?;
    assert!(replay.intent().replayed());
    assert_eq!(replay.generation(), updated.generation());
    let child_as_root = proposal(f, 0xFC02, 2, 1)?;
    assert!(matches!(
        f.catalog
            .propose_team_permanent_transfer(&child_as_root, || timestamp(620)),
        Err(OperationsError::Conflict(
            "permanent transfer root versus renewal"
        ))
    ));
    assert_eq!(
        count_rows(&f.catalog.connection, "ops_team_expansion_slot_assignments")?,
        0
    );
    assert_eq!(
        count_rows(&f.catalog.connection, "ops_team_expansion_slot_uses")?,
        0
    );
    Ok(())
}

#[test]
fn renewal_rejects_unchanged_leader_regressing_clock_and_changed_source_membership() -> TestResult {
    let mut f = prepare_permanent_transfer_intent()?;
    let unchanged = TeamPermanentTransferRenewalCommand::new(
        f.intent.transfer_id(),
        1,
        proposal(&f, 0xFC10, 1, 1)?,
    )?;
    assert!(matches!(
        f.catalog
            .renew_team_permanent_transfer(&unchanged, || timestamp(600)),
        Err(OperationsError::Conflict(
            "permanent transfer source Leader has not advanced"
        ))
    ));
    record_consent(
        &mut f.catalog,
        f.intent.transfer_id(),
        0xFC11,
        TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        sixth_participant(),
        700,
    )?;
    replace_team_leader(
        &f.catalog.connection,
        f.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let regressing = renewal(&f, 0xFC12)?;
    assert!(matches!(
        f.catalog
            .renew_team_permanent_transfer(&regressing, || timestamp(650)),
        Err(OperationsError::Conflict(
            "permanent transfer renewal time regression"
        ))
    ));
    f.catalog.connection.execute("UPDATE ops_team_members SET revision=2 WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3", params![WORKSPACE.into_uuid().as_bytes(), f.source.source_team_id.as_bytes(), MEMBER_SIX_PARTICIPATION.as_bytes()])?;
    let changed = TeamPermanentTransferRenewalCommand::new(
        f.intent.transfer_id(),
        1,
        proposal(&f, 0xFC13, 2, 2)?,
    )?;
    assert!(
        f.catalog
            .renew_team_permanent_transfer(&changed, || timestamp(800))
            .is_err()
    );
    assert_eq!(
        count_rows(&f.catalog.connection, "ops_team_permanent_transfer_intents")?,
        1
    );
    assert_eq!(
        count_rows(
            &f.catalog.connection,
            "ops_team_permanent_transfer_generation_ancestry"
        )?,
        1
    );
    Ok(())
}

#[test]
fn generation_readback_hides_unrelated_readers_and_root_cannot_replay_as_renewal() -> TestResult {
    let mut f = prepare_permanent_transfer_intent()?;
    for identity in [f.intent.transfer_id(), Uuid::from_u128(0xFC20)] {
        assert_eq!(
            f.catalog.read_team_permanent_transfer_generation(
                WORKSPACE,
                identity,
                fifth_participant()
            )?,
            None
        );
    }
    let changed_route =
        TeamPermanentTransferRenewalCommand::new(Uuid::from_u128(0xFC21), 1, f.proposal.clone())?;
    assert!(matches!(
        f.catalog
            .renew_team_permanent_transfer(&changed_route, || timestamp(600)),
        Err(OperationsError::Conflict(
            "permanent transfer renewal replay"
        ))
    ));
    assert_eq!(
        count_rows(
            &f.catalog.connection,
            "ops_team_permanent_transfer_generation_heads"
        )?,
        1
    );
    Ok(())
}

#[test]
fn two_sqlite_writers_renew_one_predecessor_exactly_once() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let f = prepare_permanent_transfer_intent_with_catalog(workspace_capacity_fixture_at(
        &database.path,
    )?)?;
    replace_team_leader(
        &f.catalog.connection,
        f.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let first = renewal(&f, 0xFC30)?;
    let second = renewal(&f, 0xFC31)?;
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for (command, now) in [(first, 600), (second, 601)] {
        let connection = create_connection(Some(&database.path))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let mut catalog = OperationsCatalog {
            connection,
            destination: DESTINATION,
        };
        let gate = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            gate.wait();
            catalog.renew_team_permanent_transfer(&command, || timestamp(now))
        }));
    }
    barrier.wait();
    let mut wins = 0;
    for handle in handles {
        match handle
            .join()
            .map_err(|_| IoError::other("renewal contender panicked"))?
        {
            Ok(value) => {
                assert_eq!(value.generation().number(), 2);
                wins += 1;
            }
            Err(OperationsError::Conflict("permanent transfer generation changed")) => {}
            other => return Err(format!("unexpected renewal race outcome: {other:?}").into()),
        }
    }
    assert_eq!(wins, 1);
    assert_eq!(
        count_rows(&f.catalog.connection, "ops_team_permanent_transfer_intents")?,
        2
    );
    assert_eq!(
        count_rows(
            &f.catalog.connection,
            "ops_team_permanent_transfer_generation_ancestry"
        )?,
        2
    );
    assert_eq!(
        count_rows(
            &f.catalog.connection,
            "ops_team_permanent_transfer_generation_heads"
        )?,
        1
    );
    Ok(())
}

#[test]
fn authorized_child_read_revalidates_predecessor_source_in_one_snapshot() -> TestResult {
    let mut f = prepare_permanent_transfer_intent()?;
    replace_team_leader(
        &f.catalog.connection,
        f.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let command = renewal(&f, 0xFC40)?;
    let child = f
        .catalog
        .renew_team_permanent_transfer(&command, || timestamp(600))?;
    f.catalog
        .connection
        .execute_batch("DROP TRIGGER ops_team_permanent_transfer_intents_no_update;")?;
    f.catalog.connection.execute("UPDATE ops_team_permanent_transfer_intents SET source_membership_revision=2 WHERE workspace_id=?1 AND transfer_id=?2", params![WORKSPACE.into_uuid().as_bytes(), f.intent.transfer_id().as_bytes()])?;
    assert_eq!(
        f.catalog.read_team_permanent_transfer_generation(
            WORKSPACE,
            child.intent().transfer_id(),
            fifth_participant()
        )?,
        None
    );
    assert!(matches!(
        f.catalog.read_team_permanent_transfer_generation(
            WORKSPACE,
            child.intent().transfer_id(),
            orchestrator()
        ),
        Err(OperationsError::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn later_generation_does_not_turn_exact_renewal_replay_into_fresh_authority() -> TestResult {
    let mut f = prepare_permanent_transfer_intent()?;
    replace_team_leader(
        &f.catalog.connection,
        f.source.source_team_id,
        MEMBER_FOUR_PARTICIPANT,
    )?;
    let first = renewal(&f, 0xFC50)?;
    let second = f
        .catalog
        .renew_team_permanent_transfer(&first, || timestamp(600))?;
    f.catalog.connection.execute(
        "UPDATE ops_leadership SET leader_participant_id=?1,leadership_revision=3,updated_at_ms=700 WHERE workspace_id=?2 AND scope_kind=2 AND team_id=?3 AND leadership_revision=2",
        params![MEMBER_SEVEN_PARTICIPANT.as_bytes(), WORKSPACE.into_uuid().as_bytes(), f.source.source_team_id.as_bytes()],
    )?;
    f.catalog.connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,3,'CONFIRMED',?3,3,700)",
        params![WORKSPACE.into_uuid().as_bytes(), f.source.source_team_id.as_bytes(), MEMBER_SEVEN_PARTICIPANT.as_bytes()],
    )?;
    let third = TeamPermanentTransferRenewalCommand::new(
        second.intent().transfer_id(),
        2,
        proposal(&f, 0xFC51, 3, 1)?,
    )?;
    let latest = f
        .catalog
        .renew_team_permanent_transfer(&third, || timestamp(800))?;
    let replay = f
        .catalog
        .renew_team_permanent_transfer(&first, || timestamp(0))?;
    assert!(replay.intent().replayed());
    assert_eq!(replay.generation().number(), 2);
    assert_eq!(
        replay.generation().state(),
        TeamPermanentTransferGenerationState::Superseded
    );
    assert_eq!(
        replay.generation().head_transfer_id(),
        latest.intent().transfer_id()
    );
    assert_eq!(replay.generation().head_generation(), 3);
    assert!(matches!(
        record_consent(
            &mut f.catalog,
            second.intent().transfer_id(),
            0xFC52,
            TeamPermanentTransferConsentKind::TargetAgentAcceptance,
            sixth_participant(),
            810
        ),
        Err(OperationsError::Conflict(
            "permanent transfer generation is superseded"
        ))
    ));
    Ok(())
}
