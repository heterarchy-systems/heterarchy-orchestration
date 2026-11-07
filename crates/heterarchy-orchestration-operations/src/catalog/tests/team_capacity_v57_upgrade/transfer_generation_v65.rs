use super::*;
use crate::{
    TeamCapacitySetCommand, TeamExpansionApprovalSlotPlanV3, TeamExpansionDecision,
    TeamExpansionDecisionCommand, TeamExpansionDecisionState, TeamExpansionDecisionV3Command,
    TeamExpansionRequestCommand, TeamExpansionWorkspaceTargetV3, TeamMemberCapacity,
    TeamPermanentTransferConsentCommand, TeamPermanentTransferConsentKind,
    TeamPermanentTransferDestinationSlot, TeamPermanentTransferGenerationSnapshot,
    TeamPermanentTransferGenerationState, TeamPermanentTransferIntentCommand,
    TeamPermanentTransferRevisions, TeamPermanentTransferScope,
    team_expansion::{TeamExpansionRevisions, TeamExpansionScope},
};
use rusqlite::TransactionBehavior;
use uuid::Uuid;

mod source_dispatch_v67;

struct ApprovedDestination {
    capacity: crate::TeamCapacityReceipt,
    request: crate::TeamExpansionRequestReceipt,
    decision_id: Uuid,
}

#[derive(Debug, PartialEq)]
struct TransferRows {
    intents: Vec<Vec<rusqlite::types::Value>>,
    consents: Vec<Vec<rusqlite::types::Value>>,
}

type RawGenerationAncestry = (Vec<u8>, Vec<u8>, Vec<u8>, i64, i64, Option<Vec<u8>>, i64);

#[test]
fn synthetic_v65_transfer_root_backfills_and_reopens_read_only() -> TestResult {
    let database = TestDatabase::new();
    let destination_id = id(DESTINATION_ID);
    let workspace_id = WorkspaceId::from_uuid(id(WORKSPACE_ID));
    let mut catalog = OperationsCatalog::open(&database.path, destination_id)?;
    let owners = seed_synthetic_v57_owner_facts(&mut catalog, workspace_id)?;
    let snapshot_before = create_transfer_with_two_consents(&mut catalog, workspace_id, &owners)?;
    let transfer_id = snapshot_before.intent().transfer_id();
    let intent = snapshot_before.intent();
    let rows_before = transfer_rows(&catalog.connection, workspace_id, transfer_id)?;
    assert_eq!(rows_before.intents.len(), 1);
    assert_eq!(rows_before.consents.len(), 2);
    assert_root_and_head(&catalog.connection, workspace_id, intent)?;
    let generation_before = read_generation_snapshot(
        &catalog,
        workspace_id,
        transfer_id,
        owners.orchestrator.actor(),
    )?;
    assert_intent_only_root(&generation_before, intent);

    reconstruct_synthetic_v65_fixture(&mut catalog, destination_id)?;
    assert_eq!(catalog_version(&catalog.connection, destination_id)?, 65);
    assert_eq!(
        transfer_rows(&catalog.connection, workspace_id, transfer_id)?,
        rows_before
    );
    drop(catalog);

    let mut upgraded = OperationsCatalog::open(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&upgraded.connection, destination_id)?,
        VERSION
    );
    validate_schema(&upgraded.connection)?;
    assert_no_foreign_key_violations(&upgraded.connection)?;
    assert_eq!(
        transfer_rows(&upgraded.connection, workspace_id, transfer_id)?,
        rows_before
    );
    assert_root_and_head(&upgraded.connection, workspace_id, intent)?;
    let upgraded_snapshot = upgraded
        .read_team_permanent_transfer_snapshot(
            workspace_id,
            transfer_id,
            owners.orchestrator.actor(),
        )?
        .ok_or(crate::OperationsError::Corrupt(
            "test transfer snapshot missing after migration",
        ))?;
    assert_eq!(upgraded_snapshot, snapshot_before);
    let upgraded_generation = read_generation_snapshot(
        &upgraded,
        workspace_id,
        transfer_id,
        owners.orchestrator.actor(),
    )?;
    assert_eq!(upgraded_generation, generation_before);
    drop(upgraded);

    let mut read_only = OperationsCatalog::open_existing_read_only(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&read_only.connection, destination_id)?,
        VERSION
    );
    validate_schema(&read_only.connection)?;
    assert_no_foreign_key_violations(&read_only.connection)?;
    assert_eq!(
        transfer_rows(&read_only.connection, workspace_id, transfer_id)?,
        rows_before
    );
    assert_root_and_head(&read_only.connection, workspace_id, intent)?;
    assert_eq!(
        read_only.read_team_permanent_transfer_snapshot(
            workspace_id,
            transfer_id,
            owners.orchestrator.actor(),
        )?,
        Some(snapshot_before)
    );
    assert_eq!(
        read_generation_snapshot(
            &read_only,
            workspace_id,
            transfer_id,
            owners.orchestrator.actor(),
        )?,
        generation_before
    );
    Ok(())
}

#[test]
fn synthetic_v66_catalog_adds_empty_preparation_schema_without_changing_transfer_history()
-> TestResult {
    let database = TestDatabase::new();
    let destination_id = id(DESTINATION_ID);
    let workspace_id = WorkspaceId::from_uuid(id(WORKSPACE_ID));
    let mut catalog = OperationsCatalog::open(&database.path, destination_id)?;
    let owners = seed_synthetic_v57_owner_facts(&mut catalog, workspace_id)?;
    let before = create_transfer_with_two_consents(&mut catalog, workspace_id, &owners)?;
    let transfer_id = before.intent().transfer_id();
    let rows_before = transfer_rows(&catalog.connection, workspace_id, transfer_id)?;
    let generation_before = read_generation_snapshot(
        &catalog,
        workspace_id,
        transfer_id,
        owners.orchestrator.actor(),
    )?;
    assert_root_and_head(&catalog.connection, workspace_id, before.intent())?;

    // Reconstruct the exact full v66 schema by removing only the additive,
    // empty v67 preparation objects. No v66 preparation provenance is invented.
    reconstruct_synthetic_v66_fixture(&mut catalog)?;
    assert_eq!(catalog_version(&catalog.connection, destination_id)?, 66);
    validate_schema_v66(&catalog.connection)?;
    assert_eq!(
        transfer_rows(&catalog.connection, workspace_id, transfer_id)?,
        rows_before
    );
    drop(catalog);

    let mut upgraded = OperationsCatalog::open(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&upgraded.connection, destination_id)?,
        VERSION
    );
    validate_schema(&upgraded.connection)?;
    assert_no_foreign_key_violations(&upgraded.connection)?;
    assert_preparation_schema_empty(&upgraded.connection)?;
    assert_eq!(
        transfer_rows(&upgraded.connection, workspace_id, transfer_id)?,
        rows_before
    );
    assert_root_and_head(&upgraded.connection, workspace_id, before.intent())?;
    assert_eq!(
        upgraded.read_team_permanent_transfer_snapshot(
            workspace_id,
            transfer_id,
            owners.orchestrator.actor(),
        )?,
        Some(before.clone())
    );
    assert_eq!(
        read_generation_snapshot(
            &upgraded,
            workspace_id,
            transfer_id,
            owners.orchestrator.actor(),
        )?,
        generation_before
    );
    drop(upgraded);

    let mut read_only = OperationsCatalog::open_existing_read_only(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&read_only.connection, destination_id)?,
        VERSION
    );
    validate_schema(&read_only.connection)?;
    assert_no_foreign_key_violations(&read_only.connection)?;
    assert_preparation_schema_empty(&read_only.connection)?;
    assert_eq!(
        transfer_rows(&read_only.connection, workspace_id, transfer_id)?,
        rows_before
    );
    assert_eq!(
        read_only.read_team_permanent_transfer_snapshot(
            workspace_id,
            transfer_id,
            owners.orchestrator.actor(),
        )?,
        Some(before)
    );
    assert_eq!(
        read_generation_snapshot(
            &read_only,
            workspace_id,
            transfer_id,
            owners.orchestrator.actor(),
        )?,
        generation_before
    );
    Ok(())
}

fn reconstruct_synthetic_v66_fixture(catalog: &mut OperationsCatalog) -> FixtureResult<()> {
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    source_dispatch_v67::drop_empty_source_schema_for_legacy_fixture(&transaction)?;
    assert_preparation_schema_empty(&transaction)?;
    for &(kind, name, _) in crate::team_permanent_transfer::preparation::schema::schema_objects()
        .iter()
        .rev()
    {
        let statement = match kind {
            "trigger" => format!("DROP TRIGGER {name}"),
            "index" => format!("DROP INDEX {name}"),
            "table" => format!("DROP TABLE {name}"),
            _ => {
                return Err(std::io::Error::other("unexpected preparation schema object").into());
            }
        };
        transaction.execute_batch(&statement)?;
    }
    transaction.pragma_update(None, "user_version", 66_i64)?;
    validate_schema_v66(&transaction)?;
    assert_no_foreign_key_violations(&transaction)?;
    transaction.commit()?;
    Ok(())
}

fn assert_preparation_schema_empty(connection: &Connection) -> FixtureResult<()> {
    let counts: (i64, i64, i64) = connection.query_row(
        "SELECT
             (SELECT COUNT(*) FROM ops_team_permanent_transfer_preparations),
             (SELECT COUNT(*) FROM ops_team_permanent_transfer_preparation_barriers),
             (SELECT COUNT(*) FROM ops_team_permanent_transfer_preparation_sessions)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(counts, (0, 0, 0));
    Ok(())
}

fn create_transfer_with_two_consents(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    owners: &LegacyOwners,
) -> FixtureResult<crate::TeamPermanentTransferSnapshot> {
    let destination = prepare_approved_destination(catalog, workspace_id, owners)?;
    let transfer_id = id(0xCC32);
    let target_participation =
        crate::WorkspaceParticipationId::from_uuid(owners.target.participation)?;
    let intent_command = TeamPermanentTransferIntentCommand::new(
        transfer_id,
        TeamPermanentTransferScope::new(
            workspace_id,
            id(HOME_TEAM_ID),
            id(REQUESTING_TEAM_ID),
            target_participation,
        ),
        TeamPermanentTransferDestinationSlot::new(
            destination.request.request_id(),
            destination.decision_id,
            0,
        ),
        TeamPermanentTransferRevisions::new(
            destination.capacity.result_team_revision(),
            destination.capacity.snapshot().revision(),
            1,
            1,
            1,
        ),
        owners.orchestrator.actor(),
    )?;
    let intent =
        catalog.propose_team_permanent_transfer(&intent_command, || timestamp(1_000_500))?;

    let source_consent_id = id(0xCC33);
    let source_consent = TeamPermanentTransferConsentCommand::new(
        source_consent_id,
        workspace_id,
        transfer_id,
        TeamPermanentTransferConsentKind::SourceHomeLeader,
        owners.home_leader.actor(),
    )?;
    catalog.record_team_permanent_transfer_consent(&source_consent, || timestamp(1_000_600))?;

    let target_acceptance_id = id(0xCC34);
    let target_acceptance = TeamPermanentTransferConsentCommand::new(
        target_acceptance_id,
        workspace_id,
        transfer_id,
        TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        owners.target.actor(),
    )?;
    catalog.record_team_permanent_transfer_consent(&target_acceptance, || timestamp(1_000_700))?;

    let snapshot = catalog
        .read_team_permanent_transfer_snapshot(
            workspace_id,
            transfer_id,
            owners.orchestrator.actor(),
        )?
        .ok_or(crate::OperationsError::Corrupt(
            "test transfer snapshot missing after consent",
        ))?;
    assert_eq!(snapshot.intent(), &intent);
    assert_eq!(
        snapshot
            .source_home_leader_consent()
            .map(crate::TeamPermanentTransferConsentReceipt::receipt_id),
        Some(source_consent_id)
    );
    assert_eq!(
        snapshot
            .source_home_leader_consent()
            .map(|consent| consent.recorded_at().get()),
        Some(1_000_600)
    );
    assert_eq!(
        snapshot
            .target_agent_acceptance()
            .map(crate::TeamPermanentTransferConsentReceipt::receipt_id),
        Some(target_acceptance_id)
    );
    assert_eq!(
        snapshot
            .target_agent_acceptance()
            .map(|consent| consent.recorded_at().get()),
        Some(1_000_700)
    );
    Ok(snapshot)
}

fn prepare_approved_destination(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    owners: &LegacyOwners,
) -> FixtureResult<ApprovedDestination> {
    let destination_team_id = id(REQUESTING_TEAM_ID);
    let capacity = catalog.set_team_capacity(
        &TeamCapacitySetCommand::new(
            id(0xCC30),
            TeamExpansionScope::new(workspace_id, destination_team_id),
            TeamExpansionRevisions::new(1, 0, 1),
            TeamMemberCapacity::new(4)?,
            owners.orchestrator.actor(),
        ),
        || timestamp(1_000_100),
    )?;
    let request = catalog.request_team_expansion(
        &TeamExpansionRequestCommand::new(
            id(0xCC31),
            TeamExpansionScope::new(workspace_id, destination_team_id),
            TeamExpansionRevisions::new(
                capacity.result_team_revision(),
                capacity.snapshot().revision(),
                1,
            ),
            1,
            "synthetic v65 generation migration slot".to_owned(),
            owners.requester.actor(),
        ),
        || timestamp(1_000_200),
    )?;
    let decision_id = id(0xCC35);
    let decision = TeamExpansionDecisionCommand::new(
        decision_id,
        request.request_id(),
        TeamExpansionScope::new(workspace_id, destination_team_id),
        TeamExpansionRevisions::new(
            request.expected_team_revision(),
            request.capacity_revision(),
            1,
        ),
        request.requester_leadership_revision(),
        TeamExpansionDecision::Approve { approved_count: 1 },
        owners.orchestrator.actor(),
    );
    let slot_plan = TeamExpansionApprovalSlotPlanV3::new(
        0,
        TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer {
            profile_id: owners.target.profile,
            participation_id: crate::WorkspaceParticipationId::from_uuid(
                owners.target.participation,
            )?,
            source_team_id: id(HOME_TEAM_ID),
            source_membership_revision: 1,
        },
    );
    let decision_v3 = TeamExpansionDecisionV3Command::new(decision, vec![slot_plan]);
    let pending = catalog.decide_team_expansion_v3(&decision_v3, || timestamp(1_000_300))?;
    assert_eq!(
        pending.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    let anchored = catalog.finalize_team_expansion_cooldown(
        workspace_id,
        request.request_id(),
        decision_id,
        1,
        owners.orchestrator.actor(),
        || timestamp(1_000_400),
    )?;
    assert_eq!(anchored.state(), TeamExpansionDecisionState::Approved);
    Ok(ApprovedDestination {
        capacity,
        request,
        decision_id,
    })
}

fn reconstruct_synthetic_v65_fixture(
    catalog: &mut OperationsCatalog,
    destination_id: Uuid,
) -> FixtureResult<()> {
    reconstruct_synthetic_v66_fixture(catalog)?;
    let transaction = catalog
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    for &(kind, name, _) in crate::team_permanent_transfer::generation::schema_objects()
        .iter()
        .rev()
    {
        let statement = match kind {
            "trigger" => format!("DROP TRIGGER {name}"),
            "table" => format!("DROP TABLE {name}"),
            _ => return Err(std::io::Error::other("unexpected generation schema object").into()),
        };
        transaction.execute_batch(&statement)?;
    }

    // This test-owned reconstruction creates the canonical v65 transfer table
    // shape over rows written by current owner APIs. It does not claim a v65
    // executable authored these rows or provide source authentication proof.
    crate::team_permanent_transfer::schema::migrate_v64_to_v65(&transaction)?;
    transaction.pragma_update(None, "user_version", 65_i64)?;
    assert_eq!(catalog_version(&transaction, destination_id)?, 65);
    validate_schema_v65(&transaction)?;
    assert_no_foreign_key_violations(&transaction)?;
    transaction.commit()?;
    Ok(())
}

fn transfer_rows(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> FixtureResult<TransferRows> {
    Ok(TransferRows {
        intents: selected_rows(
            connection,
            "SELECT * FROM ops_team_permanent_transfer_intents
             WHERE workspace_id=?1 AND transfer_id=?2 ORDER BY transfer_id",
            workspace_id,
            transfer_id,
        )?,
        consents: selected_rows(
            connection,
            "SELECT * FROM ops_team_permanent_transfer_consents
             WHERE workspace_id=?1 AND transfer_id=?2 ORDER BY receipt_id",
            workspace_id,
            transfer_id,
        )?,
    })
}

fn selected_rows(
    connection: &Connection,
    query: &str,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> FixtureResult<Vec<Vec<rusqlite::types::Value>>> {
    let mut statement = connection.prepare(query)?;
    let column_count = statement.column_count();
    let rows = statement
        .query_map(
            rusqlite::params![workspace_id.into_uuid().as_bytes(), transfer_id.as_bytes()],
            |row| {
                (0..column_count)
                    .map(|column| row.get::<_, rusqlite::types::Value>(column))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn assert_root_and_head(
    connection: &Connection,
    workspace_id: WorkspaceId,
    intent: &crate::TeamPermanentTransferIntentReceipt,
) -> FixtureResult<()> {
    let counts: (i64, i64, i64) = connection.query_row(
        "SELECT
             (SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_ancestry WHERE workspace_id=?1),
             (SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_heads WHERE workspace_id=?1),
             (SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_ancestry
              WHERE workspace_id=?1 AND (generation!=1 OR predecessor_transfer_id IS NOT NULL))",
        rusqlite::params![workspace_id.into_uuid().as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(counts, (1, 1, 0));

    let ancestry: RawGenerationAncestry = connection.query_row(
        "SELECT workspace_id,transfer_id,destination_decision_id,destination_ordinal,
                    generation,predecessor_transfer_id,recorded_at_ms
             FROM ops_team_permanent_transfer_generation_ancestry
             WHERE workspace_id=?1 AND transfer_id=?2",
        rusqlite::params![
            workspace_id.into_uuid().as_bytes(),
            intent.transfer_id().as_bytes(),
        ],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        },
    )?;
    assert_eq!(ancestry.0, workspace_id.into_uuid().as_bytes().to_vec());
    assert_eq!(ancestry.1, intent.transfer_id().as_bytes().to_vec());
    assert_eq!(
        ancestry.2,
        intent.destination_decision_id().as_bytes().to_vec()
    );
    assert_eq!(ancestry.3, i64::from(intent.destination_ordinal()));
    assert_eq!(ancestry.4, 1);
    assert_eq!(ancestry.5, None);
    assert_eq!(ancestry.6, intent.recorded_at().get());

    let head: (Vec<u8>, Vec<u8>, i64, Vec<u8>, i64) = connection.query_row(
        "SELECT workspace_id,destination_decision_id,destination_ordinal,transfer_id,generation
         FROM ops_team_permanent_transfer_generation_heads
         WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=?3",
        rusqlite::params![
            workspace_id.into_uuid().as_bytes(),
            intent.destination_decision_id().as_bytes(),
            i64::from(intent.destination_ordinal()),
        ],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    assert_eq!(head.0, workspace_id.into_uuid().as_bytes().to_vec());
    assert_eq!(head.1, intent.destination_decision_id().as_bytes().to_vec());
    assert_eq!(head.2, i64::from(intent.destination_ordinal()));
    assert_eq!(head.3, intent.transfer_id().as_bytes().to_vec());
    assert_eq!(head.4, 1);
    Ok(())
}

fn read_generation_snapshot(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    reader: crate::OperationalActor,
) -> FixtureResult<TeamPermanentTransferGenerationSnapshot> {
    Ok(catalog
        .read_team_permanent_transfer_generation(workspace_id, transfer_id, reader)?
        .ok_or(crate::OperationsError::Corrupt(
            "test permanent transfer generation snapshot missing",
        ))?)
}

fn assert_intent_only_root(
    snapshot: &TeamPermanentTransferGenerationSnapshot,
    intent: &crate::TeamPermanentTransferIntentReceipt,
) {
    let generation = snapshot.generation();
    assert_eq!(snapshot.intent(), intent);
    assert_eq!(generation.number(), 1);
    assert_eq!(generation.predecessor_transfer_id(), None);
    assert_eq!(
        generation.state(),
        TeamPermanentTransferGenerationState::IntentOnly
    );
    assert_eq!(generation.recorded_at(), intent.recorded_at());
    assert_eq!(generation.head_transfer_id(), intent.transfer_id());
    assert_eq!(generation.head_generation(), 1);
}
