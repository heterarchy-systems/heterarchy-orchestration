use super::*;
use rusqlite::Connection;
use std::error::Error;

fn profile_actor() -> OperationalActor {
    OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(Uuid::from_u128(0xAA01)),
        agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xAA02)),
        session_id: AgentSessionId::from_uuid(Uuid::from_u128(0xAA03)),
    }
}

#[test]
fn intent_command_rejects_invalid_transfer_identity_and_local_operator()
-> Result<(), OperationsError> {
    let participation = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xAA08))?;
    let transfer_id = Uuid::from_u128(0xAA04);
    let scope = TeamPermanentTransferScope::new(
        WorkspaceId::from_uuid(Uuid::from_u128(0xAA05)),
        Uuid::from_u128(0xAA06),
        Uuid::from_u128(0xAA07),
        participation,
    );
    let slot = TeamPermanentTransferDestinationSlot::new(
        Uuid::from_u128(0xAA09),
        Uuid::from_u128(0xAA0A),
        0,
    );
    let revisions = TeamPermanentTransferRevisions::new(1, 1, 1, 1, 1);
    assert!(
        TeamPermanentTransferIntentCommand::new(
            Uuid::nil(),
            scope,
            slot,
            revisions,
            profile_actor(),
        )
        .is_err()
    );
    assert!(
        TeamPermanentTransferIntentCommand::new(
            transfer_id,
            scope,
            TeamPermanentTransferDestinationSlot::new(
                slot.request_id,
                slot.decision_id,
                MAX_ORDINAL + 1,
            ),
            revisions,
            profile_actor(),
        )
        .is_err()
    );
    assert!(
        TeamPermanentTransferIntentCommand::new(
            transfer_id,
            scope,
            slot,
            revisions,
            OperationalActor::LocalOperator,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn stable_target_identity_survives_session_and_participant_rebind() -> Result<(), OperationsError> {
    let original = ActorSnapshot {
        activity_id: ActivityId::from_uuid(Uuid::from_u128(0xAA11)),
        agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xAA12)),
        session_id: AgentSessionId::from_uuid(Uuid::from_u128(0xAA13)),
        participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xAA14))?,
        profile_id: Uuid::from_u128(0xAA15),
        participant_id: Uuid::from_u128(0xAA16),
        display_name: "target".to_owned(),
    };
    let rebound = ActorSnapshot {
        activity_id: ActivityId::from_uuid(Uuid::from_u128(0xAA21)),
        agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xAA22)),
        session_id: AgentSessionId::from_uuid(Uuid::from_u128(0xAA23)),
        participation_id: original.participation_id,
        profile_id: original.profile_id,
        participant_id: Uuid::from_u128(0xAA26),
        display_name: "target rebound".to_owned(),
    };
    assert!(original.same_logical_actor(&rebound));
    let foreign = ActorSnapshot {
        profile_id: Uuid::from_u128(0xAA35),
        ..rebound
    };
    assert!(!original.same_logical_actor(&foreign));
    Ok(())
}

#[test]
fn source_leader_authority_compares_the_stable_participation_not_participant_id()
-> Result<(), Box<dyn Error>> {
    let source_participation = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xAC01))?;
    let source_profile = Uuid::from_u128(0xAC02);
    let original_source_participant_id = Uuid::from_u128(0xAC03);
    let rebound_source_participant_id = Uuid::from_u128(0xAC04);
    assert_ne!(
        original_source_participant_id,
        source_participation.into_uuid(),
        "these are distinct identifier domains"
    );
    assert_ne!(
        original_source_participant_id,
        rebound_source_participant_id
    );
    assert!(source_leader_identity_matches(
        7,
        source_participation,
        source_profile,
        7,
        source_participation,
        source_profile,
    ));
    assert!(!source_leader_identity_matches(
        7,
        source_participation,
        source_profile,
        8,
        source_participation,
        source_profile,
    ));
    assert!(!source_leader_identity_matches(
        7,
        source_participation,
        source_profile,
        7,
        WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xAC05))?,
        source_profile,
    ));
    assert!(!source_leader_identity_matches(
        7,
        source_participation,
        source_profile,
        7,
        source_participation,
        Uuid::from_u128(0xAC06),
    ));
    Ok(())
}

#[test]
fn only_one_active_home_counts_and_retired_joined_history_is_ignored() -> Result<(), OperationsError>
{
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE ops_workspace_participations(workspace_id BLOB,participation_id BLOB,agent_profile_id BLOB);
         CREATE TABLE ops_teams(workspace_id BLOB,team_id BLOB,state TEXT);
         CREATE TABLE ops_team_members(workspace_id BLOB,team_id BLOB,participation_id BLOB,state TEXT,revision INTEGER);",
    )?;
    let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xAC11));
    let participation = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xAC12))?;
    let profile = Uuid::from_u128(0xAC13);
    let retired_team = Uuid::from_u128(0xAC14);
    let active_team = Uuid::from_u128(0xAC15);
    connection.execute(
        "INSERT INTO ops_workspace_participations VALUES(?1,?2,?3)",
        params![
            workspace.into_uuid().as_bytes(),
            participation.into_uuid().as_bytes(),
            profile.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_teams VALUES(?1,?2,'RETIRED'),(?1,?3,'ACTIVE')",
        params![
            workspace.into_uuid().as_bytes(),
            retired_team.as_bytes(),
            active_team.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_members VALUES(?1,?2,?3,'JOINED',3),(?1,?4,?3,'JOINED',7)",
        params![
            workspace.into_uuid().as_bytes(),
            retired_team.as_bytes(),
            participation.into_uuid().as_bytes(),
            active_team.as_bytes(),
        ],
    )?;
    assert_eq!(
        current_home_membership(&connection, workspace, active_team, participation)?,
        (profile, 7),
        "a retired historical JOINED row is not a second current Home"
    );
    connection.execute(
        "UPDATE ops_teams SET state='RETIRED' WHERE workspace_id=?1 AND team_id=?2",
        params![workspace.into_uuid().as_bytes(), active_team.as_bytes()],
    )?;
    assert!(current_home_membership(&connection, workspace, active_team, participation).is_err());
    let new_home = Uuid::from_u128(0xAC16);
    connection.execute(
        "INSERT INTO ops_teams VALUES(?1,?2,'ACTIVE')",
        params![workspace.into_uuid().as_bytes(), new_home.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_team_members VALUES(?1,?2,?3,'JOINED',8)",
        params![
            workspace.into_uuid().as_bytes(),
            new_home.as_bytes(),
            participation.into_uuid().as_bytes(),
        ],
    )?;
    assert_eq!(
        current_home_membership(&connection, workspace, new_home, participation)?,
        (profile, 8),
        "a leave/rejoin or Home move exposes the new membership revision"
    );
    Ok(())
}

#[test]
fn transfer_requires_exact_v3_source_and_current_home() -> Result<(), Box<dyn Error>> {
    let (connection, exact) = transfer_source_test_fixture()?;
    assert!(verify_workspace_existing_target_plan(&connection, exact, 1).is_ok());
    assert!(verify_v3_existing_home_source(&connection, exact).is_ok());
    assert!(
        verify_v3_existing_home_source(
            &connection,
            TransferSlotSource {
                source_membership_revision: 8,
                ..exact
            },
        )
        .is_err()
    );
    connection.execute(
        "UPDATE ops_teams SET state='RETIRED' WHERE workspace_id=?1 AND team_id=?2",
        params![
            exact.workspace_id.into_uuid().as_bytes(),
            exact.source_team_id.as_bytes()
        ],
    )?;
    assert!(verify_v3_existing_home_source(&connection, exact).is_err());
    Ok(())
}

fn transfer_source_test_fixture() -> Result<(Connection, TransferSlotSource), Box<dyn Error>> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE ops_teams(workspace_id BLOB,team_id BLOB,revision INTEGER,state TEXT,updated_at_ms INTEGER);
         CREATE TABLE ops_team_members(workspace_id BLOB,team_id BLOB,participation_id BLOB,state TEXT,revision INTEGER,joined_at_ms INTEGER);
         CREATE TABLE ops_workspace_participations(workspace_id BLOB,participation_id BLOB,agent_profile_id BLOB);
         CREATE TABLE ops_team_expansion_workspace_hold_manifests(workspace_id BLOB,team_id BLOB,decision_id BLOB,request_id BLOB,plan_version INTEGER,slot_count INTEGER);
         CREATE TABLE ops_team_expansion_workspace_hold_plans(workspace_id BLOB,team_id BLOB,decision_id BLOB,ordinal INTEGER,target_kind INTEGER,target_profile_id BLOB,target_participation_id BLOB);
         CREATE TABLE ops_team_expansion_v3_manifests(workspace_id BLOB,team_id BLOB,decision_id BLOB,request_id BLOB,slot_count INTEGER,transfer_count INTEGER,recorded_at_ms INTEGER);
         CREATE TABLE ops_team_expansion_cooldown_anchors(workspace_id BLOB,team_id BLOB,decision_id BLOB,request_id BLOB,anchor_at_ms INTEGER);
         CREATE TABLE ops_team_expansion_existing_home_transfers(workspace_id BLOB,destination_team_id BLOB,decision_id BLOB,ordinal INTEGER,target_profile_id BLOB,target_participation_id BLOB,source_team_id BLOB,source_membership_revision INTEGER);",
    )?;
    let source = TransferSlotSource {
        workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(0xAB01)),
        destination_team_id: Uuid::from_u128(0xAB02),
        decision_id: Uuid::from_u128(0xAB03),
        request_id: Uuid::from_u128(0xAB04),
        ordinal: 0,
        target_profile_id: Uuid::from_u128(0xAB05),
        target_participation_id: Uuid::from_u128(0xAB06),
        source_team_id: Uuid::from_u128(0xAB09),
        source_membership_revision: 7,
    };
    connection.execute(
        "INSERT INTO ops_teams VALUES(?1,?2,1,'ACTIVE',1),(?1,?3,1,'ACTIVE',41)",
        params![
            source.workspace_id.into_uuid().as_bytes(),
            source.destination_team_id.as_bytes(),
            source.source_team_id.as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_members VALUES(?1,?2,?3,'JOINED',7,42)",
        params![
            source.workspace_id.into_uuid().as_bytes(),
            source.source_team_id.as_bytes(),
            source.target_participation_id.as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participations VALUES(?1,?2,?3)",
        params![
            source.workspace_id.into_uuid().as_bytes(),
            source.target_participation_id.as_bytes(),
            source.target_profile_id.as_bytes()
        ],
    )?;
    seed_transfer_source_rows(&connection, source)?;
    Ok((connection, source))
}

fn seed_transfer_source_rows(
    connection: &Connection,
    source: TransferSlotSource,
) -> Result<(), rusqlite::Error> {
    let workspace_uuid = source.workspace_id.into_uuid();
    let common = params![
        workspace_uuid.as_bytes(),
        source.destination_team_id.as_bytes(),
        source.decision_id.as_bytes(),
        source.request_id.as_bytes(),
    ];
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_manifests VALUES(?1,?2,?3,?4,1,1)",
        common,
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_plans VALUES(?1,?2,?3,0,0,?4,?5)",
        params![
            source.workspace_id.into_uuid().as_bytes(),
            source.destination_team_id.as_bytes(),
            source.decision_id.as_bytes(),
            source.target_profile_id.as_bytes(),
            source.target_participation_id.as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_v3_manifests VALUES(?1,?2,?3,?4,1,1,43)",
        params![
            source.workspace_id.into_uuid().as_bytes(),
            source.destination_team_id.as_bytes(),
            source.decision_id.as_bytes(),
            source.request_id.as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_cooldown_anchors VALUES(?1,?2,?3,?4,44)",
        params![
            source.workspace_id.into_uuid().as_bytes(),
            source.destination_team_id.as_bytes(),
            source.decision_id.as_bytes(),
            source.request_id.as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_existing_home_transfers VALUES(?1,?2,?3,0,?4,?5,?6,7)",
        params![
            source.workspace_id.into_uuid().as_bytes(),
            source.destination_team_id.as_bytes(),
            source.decision_id.as_bytes(),
            source.target_profile_id.as_bytes(),
            source.target_participation_id.as_bytes(),
            source.source_team_id.as_bytes()
        ],
    )?;
    Ok(())
}

#[test]
fn consent_history_is_scoped_immutable_and_unique() -> Result<(), Box<dyn Error>> {
    let connection = Connection::open_in_memory()?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.execute_batch(
        "CREATE TABLE ops_teams(workspace_id BLOB NOT NULL,team_id BLOB NOT NULL,PRIMARY KEY(workspace_id,team_id));
         CREATE TABLE ops_workspace_participations(workspace_id BLOB NOT NULL,participation_id BLOB NOT NULL,PRIMARY KEY(workspace_id,participation_id));
         CREATE TABLE ops_team_expansion_decisions(workspace_id BLOB NOT NULL,team_id BLOB NOT NULL,decision_id BLOB NOT NULL,request_id BLOB NOT NULL,UNIQUE(workspace_id,team_id,decision_id,request_id));
         CREATE TABLE ops_team_expansion_slots(workspace_id BLOB NOT NULL,team_id BLOB NOT NULL,decision_id BLOB NOT NULL,ordinal INTEGER NOT NULL,UNIQUE(workspace_id,team_id,decision_id,ordinal));",
    )?;
    schema::create_schema(&connection)?;
    schema::validate_schema(&connection)?;
    let intent = test_intent()?;
    connection.execute(
        "INSERT INTO ops_teams VALUES(?1,?2),(?1,?3)",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.source_team_id.as_bytes(),
            intent.destination_team_id.as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participations VALUES(?1,?2),(?1,?3)",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.target_participation_id.into_uuid().as_bytes(),
            intent.source_leader_participation_id.into_uuid().as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_decisions VALUES(?1,?2,?3,?4)",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.destination_team_id.as_bytes(),
            intent.destination_decision_id.as_bytes(),
            intent.destination_request_id.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_slots VALUES(?1,?2,?3,0)",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.destination_team_id.as_bytes(),
            intent.destination_decision_id.as_bytes()
        ],
    )?;
    insert_intent_for_test(&connection, &intent)?;
    assert_eq!(
        load_intent_in_workspace(&connection, intent.workspace_id, intent.transfer_id)?,
        Some(intent.clone())
    );
    let consent = TeamPermanentTransferConsentReceipt {
        receipt_id: Uuid::from_u128(0xAA41),
        transfer_id: intent.transfer_id,
        kind: TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        authority_revision: 1,
        membership_revision: 1,
        actor: intent.workspace_actor.clone(),
        recorded_at: TimestampMillis::new(21)?,
        intent_digest: intent.intent_digest,
        replayed: false,
    };
    let transaction = connection.unchecked_transaction()?;
    insert_consent(&transaction, &consent, intent.workspace_id)?;
    transaction.commit()?;
    assert!(connection.execute(
        "UPDATE ops_team_permanent_transfer_consents SET recorded_at_ms=22 WHERE receipt_id=?1",
        params![consent.receipt_id.as_bytes()],
    ).is_err());
    let transaction = connection.unchecked_transaction()?;
    let duplicate = TeamPermanentTransferConsentReceipt {
        receipt_id: Uuid::from_u128(0xAA42),
        ..consent
    };
    assert!(insert_consent(&transaction, &duplicate, intent.workspace_id).is_err());
    transaction.rollback()?;
    Ok(())
}

fn insert_intent_for_test(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), OperationsError> {
    let transaction = connection.unchecked_transaction()?;
    insert_intent(&transaction, intent)?;
    transaction.commit()?;
    Ok(())
}

#[test]
fn consent_recorded_before_proposal_time_is_rejected() -> Result<(), Box<dyn Error>> {
    let intent = test_intent()?;
    assert!(ensure_consent_time_follows_intent(&intent, TimestampMillis::new(19)?,).is_err());
    assert!(ensure_consent_time_follows_intent(&intent, TimestampMillis::new(20)?,).is_ok());
    Ok(())
}

#[test]
fn consent_reader_hides_only_authorization_conflicts() {
    assert!(matches!(
        consent_reader_has_orchestrator_access(Ok(2)),
        Ok(true)
    ));
    assert!(matches!(
        consent_reader_has_orchestrator_access(Err(OperationsError::Conflict("not O"))),
        Ok(false)
    ));
    assert!(matches!(
        consent_reader_has_orchestrator_access(Err(OperationsError::Corrupt("leadership source"))),
        Err(OperationsError::Corrupt("leadership source"))
    ));
}

fn test_intent() -> Result<TeamPermanentTransferIntentReceipt, Box<dyn Error>> {
    let actor = ActorSnapshot {
        activity_id: ActivityId::from_uuid(Uuid::from_u128(0xAA31)),
        agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xAA32)),
        session_id: AgentSessionId::from_uuid(Uuid::from_u128(0xAA33)),
        participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xAA34))?,
        profile_id: Uuid::from_u128(0xAA35),
        participant_id: Uuid::from_u128(0xAA36),
        display_name: "Orchestrator".to_owned(),
    };
    let mut receipt = TeamPermanentTransferIntentReceipt {
        transfer_id: Uuid::from_u128(0xAA37),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(0xAA38)),
        source_team_id: Uuid::from_u128(0xAA39),
        destination_team_id: Uuid::from_u128(0xAA3A),
        target_participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xAA3B))?,
        target_profile_id: Uuid::from_u128(0xAA3C),
        source_membership_revision: 1,
        source_leadership_revision: 1,
        source_leader_participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_u128(
            0xAA3D,
        ))?,
        source_leader_profile_id: Uuid::from_u128(0xAA3E),
        destination_team_revision: 1,
        destination_capacity_revision: 1,
        destination_workspace_leadership_revision: 1,
        destination_request_id: Uuid::from_u128(0xAA3F),
        destination_decision_id: Uuid::from_u128(0xAA40),
        destination_ordinal: 0,
        workspace_actor: actor,
        recorded_at: TimestampMillis::new(20)?,
        intent_digest: [0; 32],
        replayed: false,
    };
    receipt.intent_digest = intent_digest_from_receipt(&receipt)?;
    Ok(receipt)
}

#[test]
fn permanent_transfer_v64_rows_migrate_and_ids_are_workspace_scoped() -> Result<(), Box<dyn Error>>
{
    let connection = Connection::open_in_memory()?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.execute_batch(
        "CREATE TABLE ops_teams(workspace_id BLOB NOT NULL,team_id BLOB NOT NULL,PRIMARY KEY(workspace_id,team_id));
         CREATE TABLE ops_workspace_participations(workspace_id BLOB NOT NULL,participation_id BLOB NOT NULL,PRIMARY KEY(workspace_id,participation_id));
         CREATE TABLE ops_team_expansion_decisions(workspace_id BLOB NOT NULL,team_id BLOB NOT NULL,decision_id BLOB NOT NULL,request_id BLOB NOT NULL,UNIQUE(workspace_id,team_id,decision_id,request_id));
         CREATE TABLE ops_team_expansion_slots(workspace_id BLOB NOT NULL,team_id BLOB NOT NULL,decision_id BLOB NOT NULL,ordinal INTEGER NOT NULL,UNIQUE(workspace_id,team_id,decision_id,ordinal));",
    )?;
    for &(_, _, sql) in schema::schema_objects_v64() {
        connection.execute_batch(sql)?;
    }
    let original = test_intent()?;
    seed_migration_parent_rows(&connection, &original)?;
    insert_intent_for_test(&connection, &original)?;
    let original_consent = TeamPermanentTransferConsentReceipt {
        receipt_id: Uuid::from_u128(0xAD01),
        transfer_id: original.transfer_id,
        kind: TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        authority_revision: 1,
        membership_revision: 1,
        actor: ActorSnapshot {
            participation_id: original.target_participation_id,
            profile_id: original.target_profile_id,
            ..original.workspace_actor.clone()
        },
        recorded_at: TimestampMillis::new(21)?,
        intent_digest: original.intent_digest,
        replayed: false,
    };
    let transaction = connection.unchecked_transaction()?;
    insert_consent(&transaction, &original_consent, original.workspace_id)?;
    transaction.commit()?;
    connection.pragma_update(None, "foreign_keys", false)?;
    let transaction = connection.unchecked_transaction()?;
    schema::migrate_v64_to_v65(&transaction)?;
    schema::migrate_v65_to_v66(&transaction)?;
    transaction.rollback()?;
    let original_schema: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE name='ops_team_permanent_transfer_intents'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(original_schema, schema::schema_objects_v64()[0].2);
    assert_eq!(
        load_intent_in_workspace(&connection, original.workspace_id, original.transfer_id)?,
        Some(original.clone())
    );
    let transaction = connection.unchecked_transaction()?;
    schema::migrate_v64_to_v65(&transaction)?;
    schema::migrate_v65_to_v66(&transaction)?;
    schema::validate_schema(&transaction)?;
    assert_eq!(
        load_intent_in_workspace(&transaction, original.workspace_id, original.transfer_id)?,
        Some(original.clone())
    );
    let violations: i64 =
        transaction.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    assert_eq!(violations, 0);
    transaction.commit()?;
    connection.pragma_update(None, "foreign_keys", true)?;
    let mut other = original.clone();
    other.workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0xAD02));
    other.intent_digest = intent_digest_from_receipt(&other)?;
    seed_migration_parent_rows(&connection, &other)?;
    insert_intent_for_test(&connection, &other)?;
    let transaction = connection.unchecked_transaction()?;
    assert!(insert_consent(&transaction, &original_consent, other.workspace_id).is_err());
    let other_consent = TeamPermanentTransferConsentReceipt {
        intent_digest: other.intent_digest,
        ..original_consent.clone()
    };
    insert_consent(&transaction, &other_consent, other.workspace_id)?;
    transaction.commit()?;
    assert_eq!(
        load_intent_in_workspace(&connection, other.workspace_id, other.transfer_id)?,
        Some(other)
    );
    assert_eq!(
        load_consent(
            &connection,
            original.workspace_id,
            original.transfer_id,
            original_consent.receipt_id
        )?,
        Some(original_consent)
    );
    Ok(())
}

fn seed_migration_parent_rows(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), rusqlite::Error> {
    let workspace = intent.workspace_id.into_uuid();
    connection.execute(
        "INSERT INTO ops_teams VALUES(?1,?2),(?1,?3)",
        params![
            workspace.as_bytes(),
            intent.source_team_id.as_bytes(),
            intent.destination_team_id.as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participations VALUES(?1,?2),(?1,?3)",
        params![
            workspace.as_bytes(),
            intent.target_participation_id.into_uuid().as_bytes(),
            intent.source_leader_participation_id.into_uuid().as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_decisions VALUES(?1,?2,?3,?4)",
        params![
            workspace.as_bytes(),
            intent.destination_team_id.as_bytes(),
            intent.destination_decision_id.as_bytes(),
            intent.destination_request_id.as_bytes()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_slots VALUES(?1,?2,?3,0)",
        params![
            workspace.as_bytes(),
            intent.destination_team_id.as_bytes(),
            intent.destination_decision_id.as_bytes()
        ],
    )?;
    Ok(())
}

#[test]
fn consent_actor_lookup_never_joins_another_workspace_intent() -> Result<(), Box<dyn Error>> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE ops_team_permanent_transfer_intents(workspace_id BLOB,transfer_id BLOB,intent_digest BLOB);
         CREATE TABLE ops_team_permanent_transfer_consents(workspace_id BLOB,transfer_id BLOB,receipt_id BLOB,intent_digest BLOB,actor_participation_id BLOB,actor_profile_id BLOB);",
    )?;
    let workspace_a = Uuid::from_u128(0xAD31);
    let workspace_b = Uuid::from_u128(0xAD32);
    let transfer = Uuid::from_u128(0xAD33);
    let receipt = Uuid::from_u128(0xAD34);
    let digest = [7_u8; 32];
    connection.execute(
        "INSERT INTO ops_team_permanent_transfer_intents VALUES(?1,?2,?3)",
        params![
            workspace_a.as_bytes(),
            transfer.as_bytes(),
            digest.as_slice()
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_team_permanent_transfer_consents VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            workspace_b.as_bytes(),
            transfer.as_bytes(),
            receipt.as_bytes(),
            digest.as_slice(),
            Uuid::from_u128(0xAD35).as_bytes(),
            Uuid::from_u128(0xAD36).as_bytes()
        ],
    )?;
    let workspace_b_id = WorkspaceId::from_uuid(workspace_b);
    assert_eq!(
        consent_actor_identity(&connection, workspace_b_id, transfer, receipt)?,
        None
    );
    connection.execute(
        "INSERT INTO ops_team_permanent_transfer_intents VALUES(?1,?2,?3)",
        params![
            workspace_b.as_bytes(),
            transfer.as_bytes(),
            digest.as_slice()
        ],
    )?;
    assert!(consent_actor_identity(&connection, workspace_b_id, transfer, receipt)?.is_some());
    Ok(())
}
