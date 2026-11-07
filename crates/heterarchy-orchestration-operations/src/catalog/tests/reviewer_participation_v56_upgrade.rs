use super::super::*;
use crate::{
    OperationalActor,
    reviewer_qualification_rule::{
        ActualRuntimeFact, EvidenceOwnerNamespace, EvidenceSourceOwnerId, ExactRuntimeText,
        QualificationDomain, QualificationEvidenceRequirement, QualificationScope,
        ReviewerQualificationRuleChange, ReviewerQualificationRuleMutationCommand,
        ReviewerQualificationRulePolicy, RuntimeFactDigest, RuntimeFactPredicate,
    },
    temporary_dispatch::{
        TemporaryDispatchAdmissionBinding, TemporaryDispatchAdmissionRequest,
        TemporaryDispatchAdmissionResult, TemporaryDispatchConsentCommand,
        TemporaryDispatchConsentKind, TemporaryDispatchConsentReceipt,
        TemporaryDispatchRequestRecord, TemporaryDispatchRequestedScope,
        TemporaryDispatchReservationReceipt, TemporaryDispatchReservationState,
    },
};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, DirectTokenId,
    TimestampMillis,
};
use rusqlite::{Connection, params, types::Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::PathBuf,
};
use uuid::Uuid;

const DESTINATION_ID: u128 = 0xB001;
const WORKSPACE_ID: u128 = 0xB002;
const REQUESTING_TEAM_ID: u128 = 0xB003;
const HOME_TEAM_ID: u128 = 0xB004;
const OWNER_NOW_MS: i64 = 1_000_000;
const CONSENT_NOW_MS: i64 = 1_000_100;
const RESERVATION_NOW_MS: i64 = 1_000_200;

type TableRows = BTreeMap<String, Vec<Vec<Value>>>;
type TestResult = Result<(), Box<dyn Error>>;
type FixtureResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Copy)]
struct FixtureParticipant {
    profile: Uuid,
    participation: Uuid,
    participant: Uuid,
    activity: Uuid,
    agent: Uuid,
    session: Uuid,
}

impl FixtureParticipant {
    fn from_seed(seed: u128) -> Self {
        Self {
            profile: id(0x10_000 + seed),
            participation: id(0x20_000 + seed),
            participant: id(0x30_000 + seed),
            activity: id(0x40_000 + seed),
            agent: id(0x50_000 + seed),
            session: id(0x60_000 + seed),
        }
    }

    fn actor(self) -> OperationalActor {
        OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(self.activity),
            agent_id: AgentInstanceId::from_uuid(self.agent),
            session_id: AgentSessionId::from_uuid(self.session),
        }
    }
}

#[derive(Clone, Copy)]
struct DispatchOwnerFixture {
    requesting_leader: FixtureParticipant,
    target: FixtureParticipant,
    home_leader: FixtureParticipant,
    orchestrator: FixtureParticipant,
}

struct DispatchRecords {
    request: TemporaryDispatchAdmissionRequest,
    admission_readback: TemporaryDispatchRequestRecord,
    binding: TemporaryDispatchAdmissionBinding,
    target_consent: TemporaryDispatchConsentReceipt,
    home_consent: TemporaryDispatchConsentReceipt,
    orchestrator_consent: TemporaryDispatchConsentReceipt,
    reservation: TemporaryDispatchReservationReceipt,
    owners: DispatchOwnerFixture,
}

struct RuleRecord {
    rule_id: Uuid,
    request_id: Uuid,
    record: crate::reviewer_qualification_rule::ReviewerQualificationRuleGenerationRecord,
}

struct V56Records {
    dispatch: DispatchRecords,
    rule: RuleRecord,
}

struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "operations-reviewer-v56-upgrade-{}.sqlite",
                Uuid::now_v7()
            )),
        }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(self.path.with_extension("sqlite-shm"));
    }
}

#[test]
fn v56_upgrade_preserves_tf_and_rule_records_and_starts_reviewer_participation_empty() -> TestResult
{
    let database = TestDatabase::new();
    let destination_id = id(DESTINATION_ID);
    let workspace_id = WorkspaceId::from_uuid(id(WORKSPACE_ID));
    let mut catalog = OperationsCatalog::open(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&catalog.connection, destination_id)?,
        VERSION
    );
    validate_schema(&catalog.connection)?;
    let persisted = seed_nonempty_v56_rows(&mut catalog, workspace_id)?;
    let v56_objects = schema_v56_objects();
    let original_rows = rows_for_schema(&catalog.connection, &v56_objects)?;
    assert_nonempty_v56_fixture(&original_rows)?;

    let mut newer_objects = crate::reviewer_participation::schema_objects().to_vec();
    newer_objects.extend_from_slice(crate::team_expansion::schema_objects());
    newer_objects.extend_from_slice(crate::team_create_source::schema_objects());
    newer_objects.extend_from_slice(crate::team_expansion::workspace_hold_schema_objects());
    newer_objects.extend_from_slice(crate::team_expansion::legacy_slot_recovery_schema_objects());
    assert_schema_tables_empty(&catalog.connection, &newer_objects)?;
    assert!(
        catalog
            .team_capacity(workspace_id, id(REQUESTING_TEAM_ID))?
            .is_none()
    );
    assert!(
        catalog
            .team_capacity(workspace_id, id(HOME_TEAM_ID))?
            .is_none()
    );
    assert_no_foreign_key_violations(&catalog.connection)?;
    super::super::drop_empty_mission_schema_for_legacy_fixture(
        &catalog.connection,
        &newer_objects,
    )?;
    catalog.connection.pragma_update(None, "user_version", 56)?;
    validate_schema_v56(&catalog.connection)?;
    assert_eq!(catalog_version(&catalog.connection, destination_id)?, 56);
    assert_eq!(
        rows_for_schema(&catalog.connection, &v56_objects)?,
        original_rows
    );
    drop(catalog);

    let mut upgraded = OperationsCatalog::open(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&upgraded.connection, destination_id)?,
        VERSION
    );
    validate_schema(&upgraded.connection)?;
    assert_eq!(
        rows_for_schema(&upgraded.connection, &v56_objects)?,
        original_rows
    );
    assert_schema_tables_empty(&upgraded.connection, &newer_objects)?;
    assert!(
        upgraded
            .team_capacity(workspace_id, id(REQUESTING_TEAM_ID))?
            .is_none()
    );
    assert!(
        upgraded
            .team_capacity(workspace_id, id(HOME_TEAM_ID))?
            .is_none()
    );
    assert_no_foreign_key_violations(&upgraded.connection)?;
    assert_v56_readbacks(&mut upgraded, workspace_id, &persisted)?;
    drop(upgraded);

    let mut read_only = OperationsCatalog::open_existing_read_only(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&read_only.connection, destination_id)?,
        VERSION
    );
    validate_schema(&read_only.connection)?;
    assert_eq!(
        rows_for_schema(&read_only.connection, &v56_objects)?,
        original_rows
    );
    assert_schema_tables_empty(&read_only.connection, &newer_objects)?;
    assert!(
        read_only
            .team_capacity(workspace_id, id(REQUESTING_TEAM_ID))?
            .is_none()
    );
    assert!(
        read_only
            .team_capacity(workspace_id, id(HOME_TEAM_ID))?
            .is_none()
    );
    assert_no_foreign_key_violations(&read_only.connection)?;
    assert_v56_readbacks(&mut read_only, workspace_id, &persisted)?;
    Ok(())
}

fn seed_nonempty_v56_rows(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
) -> FixtureResult<V56Records> {
    let owners = seed_dispatch_owner(catalog, workspace_id)?;
    let dispatch = record_dispatch_lifecycle(catalog, workspace_id, &owners)?;
    let rule = record_qualification_rule(catalog, workspace_id, owners.requesting_leader)?;
    Ok(V56Records { dispatch, rule })
}

fn seed_dispatch_owner(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
) -> FixtureResult<DispatchOwnerFixture> {
    // Synthetic verified-import, logical identity and leadership owner facts.
    // The admission, consent, reservation and rule below use Operations APIs;
    // this fixture does not claim authenticated HTTP or Broker provenance.
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            id(0xB010).as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            catalog.destination_id().as_bytes(),
            [0x51_u8; 32].as_slice(),
        ],
    )?;

    let requesting_leader = FixtureParticipant::from_seed(1);
    let requesting_peer = FixtureParticipant::from_seed(2);
    let target = FixtureParticipant::from_seed(3);
    let home_leader = FixtureParticipant::from_seed(4);
    let orchestrator = FixtureParticipant::from_seed(5);
    for (participant, request_seed) in [
        (requesting_leader, 0xB020),
        (requesting_peer, 0xB021),
        (target, 0xB022),
        (home_leader, 0xB023),
        (orchestrator, 0xB024),
    ] {
        seed_native_participant(&catalog.connection, workspace_id, participant, request_seed)?;
    }
    seed_synthetic_legacy_team(
        catalog,
        workspace_id,
        id(REQUESTING_TEAM_ID),
        "Receiving Team",
        &[requesting_leader, requesting_peer],
    )?;
    seed_synthetic_legacy_team(
        catalog,
        workspace_id,
        id(HOME_TEAM_ID),
        "Home Team",
        &[target, home_leader],
    )?;

    seed_leadership(
        &catalog.connection,
        workspace_id,
        LeadershipFixtureScope::Team(id(REQUESTING_TEAM_ID)),
        requesting_leader,
    )?;
    seed_leadership(
        &catalog.connection,
        workspace_id,
        LeadershipFixtureScope::Team(id(HOME_TEAM_ID)),
        home_leader,
    )?;
    seed_leadership(
        &catalog.connection,
        workspace_id,
        LeadershipFixtureScope::Workspace,
        orchestrator,
    )?;
    Ok(DispatchOwnerFixture {
        requesting_leader,
        target,
        home_leader,
        orchestrator,
    })
}

fn seed_native_participant(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant: FixtureParticipant,
    request_seed: u128,
) -> TestResult {
    connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant.profile.as_bytes(),
            participant.participation.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Migration fixture participant','worker',1,1,1)",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant.participant.as_bytes(),
            participant.session.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
        params![
            id(request_seed).as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            participant.participation.as_bytes(),
            participant.profile.as_bytes(),
            id(request_seed + 1).as_bytes(),
            participant.session.as_bytes(),
            participant.participant.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant.participant.as_bytes(),
            participant.activity.as_bytes(),
            participant.session.as_bytes(),
            participant.agent.as_bytes(),
            id(request_seed + 2).as_bytes(),
        ],
    )?;
    Ok(())
}

fn seed_synthetic_legacy_team(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    name: &str,
    members: &[FixtureParticipant],
) -> TestResult {
    // Import-style SQL fixture: these pre-capacity Teams intentionally have
    // no schema-58 capacity event.
    let creator = members
        .first()
        .copied()
        .ok_or("legacy Team fixture requires a member")?;
    catalog.connection.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,?3,'synthetic schema migration fixture','ACTIVE',1,'synthetic:legacy-import','Synthetic fixture leader',?4,?5,?6,?7,100,100,100)",
        params![
            team_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            name,
            creator.participation.as_bytes(),
            creator.profile.as_bytes(),
            creator.participant.as_bytes(),
            creator.session.as_bytes(),
        ],
    )?;
    for member in members {
        catalog.connection.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,100,NULL,'member')",
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                member.participation.as_bytes(),
            ],
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum LeadershipFixtureScope {
    Workspace,
    Team(Uuid),
}

fn seed_leadership(
    connection: &Connection,
    workspace_id: WorkspaceId,
    scope: LeadershipFixtureScope,
    leader: FixtureParticipant,
) -> TestResult {
    let (scope_kind, team_id) = match scope {
        LeadershipFixtureScope::Workspace => (0_i64, None),
        LeadershipFixtureScope::Team(team_id) => (2_i64, Some(team_id)),
    };
    connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,?2,NULL,?3,'ACTIVE',?4,1,NULL,NULL,100)",
        params![
            workspace_id.into_uuid().as_bytes(),
            scope_kind,
            team_id.map(|id| id.as_bytes().to_vec()),
            leader.participant.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,?2,zeroblob(16),?3,1,'CONFIRMED',NULL,NULL,?4,1,100)",
        params![
            workspace_id.into_uuid().as_bytes(),
            scope_kind,
            team_id.map_or_else(|| vec![0_u8; 16], |id| id.as_bytes().to_vec()),
            leader.participant.as_bytes(),
        ],
    )?;
    Ok(())
}

fn record_dispatch_lifecycle(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    owners: &DispatchOwnerFixture,
) -> FixtureResult<DispatchRecords> {
    let request = TemporaryDispatchAdmissionRequest::new(
        id(0xB040),
        workspace_id,
        id(REQUESTING_TEAM_ID),
        1,
        owners.requesting_leader.actor(),
        owners.target.participant,
        TemporaryDispatchRequestedScope::new("v56 migration fixture intent".to_owned())?,
        timestamp(10_000_000)?,
    )?;
    let outcome = catalog.admit_temporary_dispatch(&request, || timestamp(OWNER_NOW_MS))?;
    let admission_created = match outcome {
        TemporaryDispatchAdmissionResult::Admitted(record) => *record,
        TemporaryDispatchAdmissionResult::QuotaRejected { .. } => {
            return Err("fixture temporary dispatch admission was quota-rejected".into());
        }
    };
    let binding = TemporaryDispatchAdmissionBinding::from_record(&admission_created);
    let admission_readback = catalog
        .read_temporary_dispatch_request(&request)?
        .ok_or("missing temporary dispatch admission readback")?;
    assert_eq!(admission_readback.request_id(), request.request_id());

    let target_written = record_consent(
        catalog,
        &binding,
        owners.target,
        TemporaryDispatchConsentKind::TargetAcceptance,
        id(0xB041),
    )?;
    let target_consent = read_consent(catalog, &binding, owners.target, &target_written)?;
    let home_written = record_consent(
        catalog,
        &binding,
        owners.home_leader,
        TemporaryDispatchConsentKind::HomeLeaderConsent,
        id(0xB042),
    )?;
    let home_consent = read_consent(catalog, &binding, owners.home_leader, &home_written)?;
    let orchestrator_written = record_consent(
        catalog,
        &binding,
        owners.orchestrator,
        TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval,
        id(0xB043),
    )?;
    let orchestrator_consent = read_consent(
        catalog,
        &binding,
        owners.orchestrator,
        &orchestrator_written,
    )?;
    // This is an explicitly synthetic v56 historical reservation. Current
    // reservation writes require a configured cap; an unknown-cap legacy Team
    // must remain unknown, so the test seeds the old receipt/state rows and
    // validates them through the current read API.
    seed_synthetic_legacy_reservation(
        &catalog.connection,
        &binding,
        &target_consent,
        &home_consent,
        &orchestrator_consent,
        timestamp(RESERVATION_NOW_MS)?,
    )?;
    let reservation = catalog
        .read_temporary_dispatch_reservation(&binding, owners.orchestrator.actor())?
        .ok_or("missing temporary dispatch reservation readback")?;
    assert_eq!(
        reservation.state(),
        TemporaryDispatchReservationState::Reserved
    );
    assert_eq!(reservation.request_id(), binding.request_id());

    Ok(DispatchRecords {
        request,
        admission_readback,
        binding,
        target_consent,
        home_consent,
        orchestrator_consent,
        reservation,
        owners: *owners,
    })
}

fn seed_synthetic_legacy_reservation(
    connection: &Connection,
    binding: &TemporaryDispatchAdmissionBinding,
    target_consent: &TemporaryDispatchConsentReceipt,
    home_consent: &TemporaryDispatchConsentReceipt,
    orchestrator_consent: &TemporaryDispatchConsentReceipt,
    recorded_at: TimestampMillis,
) -> TestResult {
    let actor = orchestrator_consent.actor();
    let target_identity = target_consent.actor();
    connection.execute(
        "INSERT INTO ops_temporary_dispatch_reservation_receipts(receipt_id,request_id,revision,state,intent_digest,target_participation_id,target_profile_id,target_membership_revision,requesting_leadership_revision,home_leadership_revision,workspace_leadership_revision,target_consent_receipt_id,home_leader_consent_receipt_id,orchestrator_consent_receipt_id,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms) VALUES(?1,?1,1,'RESERVED',?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
        params![
            binding.request_id().as_bytes(),
            binding.intent_digest().as_slice(),
            target_identity.participation_id().into_uuid().as_bytes(),
            target_identity.profile_id().as_bytes(),
            i64::try_from(target_consent.authority_revision())?,
            i64::try_from(binding.leadership_revision())?,
            i64::try_from(home_consent.authority_revision())?,
            i64::try_from(orchestrator_consent.authority_revision())?,
            target_consent.receipt_id().as_bytes(),
            home_consent.receipt_id().as_bytes(),
            orchestrator_consent.receipt_id().as_bytes(),
            actor.activity_id().into_uuid().as_bytes(),
            actor.agent_id().into_uuid().as_bytes(),
            actor.session_id().into_uuid().as_bytes(),
            actor.participation_id().into_uuid().as_bytes(),
            actor.profile_id().as_bytes(),
            actor.participant_id().as_bytes(),
            actor.display_name(),
            recorded_at.get(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_temporary_dispatch_reservations(request_id,workspace_id,receiving_team_id,home_team_id,target_participant_id,target_participation_id,target_profile_id,requested_scope,requested_end_at_ms,intent_digest,state,revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'RESERVED',1,?11)",
        params![
            binding.request_id().as_bytes(),
            binding.workspace_id().into_uuid().as_bytes(),
            binding.requesting_team_id().as_bytes(),
            binding.target_home_team_id().as_bytes(),
            binding.target_participant_id().as_bytes(),
            target_identity.participation_id().into_uuid().as_bytes(),
            target_identity.profile_id().as_bytes(),
            binding.requested_scope().as_str(),
            binding.requested_end_at().get(),
            binding.intent_digest().as_slice(),
            recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn record_consent(
    catalog: &mut OperationsCatalog,
    binding: &TemporaryDispatchAdmissionBinding,
    participant: FixtureParticipant,
    kind: TemporaryDispatchConsentKind,
    receipt_id: Uuid,
) -> FixtureResult<TemporaryDispatchConsentReceipt> {
    let command = TemporaryDispatchConsentCommand::new(
        receipt_id,
        binding.clone(),
        kind,
        0,
        participant.actor(),
    )?;
    Ok(catalog.record_temporary_dispatch_consent(&command, || timestamp(CONSENT_NOW_MS))?)
}

fn read_consent(
    catalog: &mut OperationsCatalog,
    binding: &TemporaryDispatchAdmissionBinding,
    participant: FixtureParticipant,
    receipt: &TemporaryDispatchConsentReceipt,
) -> FixtureResult<TemporaryDispatchConsentReceipt> {
    let readback = catalog
        .read_temporary_dispatch_consent(binding, receipt.receipt_id(), participant.actor())?
        .ok_or("missing temporary dispatch consent readback")?;
    assert_eq!(readback.kind(), receipt.kind());
    assert_eq!(readback.receipt_id(), receipt.receipt_id());
    Ok(readback)
}

fn record_qualification_rule(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    requester: FixtureParticipant,
) -> FixtureResult<RuleRecord> {
    let rule_id = id(0xB050);
    let request_id = id(0xB051);
    let requester_session = AgentSessionId::from_uuid(requester.session);
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(id(0xB052)));
    // The DirectToken and Session IDs are synthetic fixture identities, not
    // bearer credentials or proof of Runtime authentication and OperatorPolicy.
    let command = ReviewerQualificationRuleMutationCommand::new(
        workspace_id,
        rule_id,
        request_id,
        None,
        requester_session,
        principal,
        ReviewerQualificationRuleChange::Publish {
            policy: migration_fixture_policy()?,
        },
    )?;
    let _mutation = catalog.mutate_reviewer_qualification_rule(command)?;
    let record = catalog
        .reviewer_qualification_rule_request(workspace_id, rule_id, request_id)?
        .ok_or("missing qualification rule readback")?;
    assert_eq!(record.requester_session_id(), requester_session);
    assert_eq!(record.original_actor(), principal);
    Ok(RuleRecord {
        rule_id,
        request_id,
        record,
    })
}

fn migration_fixture_policy() -> FixtureResult<ReviewerQualificationRulePolicy> {
    let source_owner =
        EvidenceSourceOwnerId::new(EvidenceOwnerNamespace::RuntimeAdapter, id(0xB053))?;
    let fact_predicates = BTreeSet::from([
        RuntimeFactPredicate::exact_text(
            ActualRuntimeFact::Model,
            BTreeSet::from([ExactRuntimeText::new("v56-fixture-model".to_owned())?]),
        )?,
        RuntimeFactPredicate::exact_text(
            ActualRuntimeFact::ToolVersions,
            BTreeSet::from([ExactRuntimeText::new("v56-fixture-tools".to_owned())?]),
        )?,
        RuntimeFactPredicate::exact_digests(
            ActualRuntimeFact::EnvironmentFingerprint,
            BTreeSet::from([RuntimeFactDigest::new(
                ExactRuntimeText::new("fixture-digest".to_owned())?,
                vec![0x52; 32],
            )?]),
        )?,
    ]);
    Ok(ReviewerQualificationRulePolicy::new(
        QualificationDomain::new("migration-fixture-domain".to_owned())?,
        QualificationScope::new("workspace-review".to_owned())?,
        BTreeSet::from([QualificationEvidenceRequirement::ActualRuntimeProvenance {
            source_owner,
            fact_predicates,
        }]),
    )?)
}

fn assert_nonempty_v56_fixture(rows: &TableRows) -> TestResult {
    for (table, expected) in [
        ("ops_temporary_dispatch_requests", 1),
        ("ops_temporary_dispatch_consent_receipts", 3),
        ("ops_temporary_dispatch_current_consents", 3),
        ("ops_temporary_dispatch_reservation_receipts", 1),
        ("ops_temporary_dispatch_reservations", 1),
        ("ops_reviewer_qualification_rule_generations", 1),
    ] {
        assert_eq!(table_row_count(rows, table)?, expected, "table {table}");
    }
    Ok(())
}

fn assert_v56_readbacks(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    persisted: &V56Records,
) -> TestResult {
    let dispatch = &persisted.dispatch;
    assert_eq!(
        catalog.read_temporary_dispatch_request(&dispatch.request)?,
        Some(dispatch.admission_readback.clone())
    );
    for (receipt, participant) in [
        (&dispatch.target_consent, dispatch.owners.target),
        (&dispatch.home_consent, dispatch.owners.home_leader),
        (&dispatch.orchestrator_consent, dispatch.owners.orchestrator),
    ] {
        assert_eq!(
            catalog.read_temporary_dispatch_consent(
                &dispatch.binding,
                receipt.receipt_id(),
                participant.actor(),
            )?,
            Some(receipt.clone())
        );
    }
    assert_eq!(
        catalog.read_temporary_dispatch_reservation(
            &dispatch.binding,
            dispatch.owners.orchestrator.actor(),
        )?,
        Some(dispatch.reservation.clone())
    );
    assert_eq!(
        catalog.reviewer_qualification_rule_request(
            workspace_id,
            persisted.rule.rule_id,
            persisted.rule.request_id,
        )?,
        Some(persisted.rule.record.clone())
    );
    Ok(())
}

fn rows_for_schema(
    connection: &Connection,
    objects: &[(&'static str, &'static str, String)],
) -> FixtureResult<TableRows> {
    let mut tables = BTreeMap::new();
    for &(kind, name, _) in objects {
        if kind != "table" {
            continue;
        }
        let mut statement = connection.prepare(&format!("SELECT * FROM {name}"))?;
        let column_count = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..column_count)
                    .map(|column| row.get::<_, Value>(column))
                    .collect::<Result<Vec<_>, _>>()
            })?
            .collect::<Result<Vec<_>, _>>()?;
        tables.insert(name.to_owned(), rows);
    }
    Ok(tables)
}

fn table_row_count(rows: &TableRows, table: &str) -> Result<usize, std::io::Error> {
    rows.get(table)
        .map(Vec::len)
        .ok_or_else(|| std::io::Error::other(format!("missing snapshot table {table}")))
}

fn assert_schema_tables_empty(
    connection: &Connection,
    objects: &[(&'static str, &'static str, &'static str)],
) -> TestResult {
    for &(kind, name, _) in objects {
        if kind != "table" {
            continue;
        }
        let count: i64 =
            connection.query_row(&format!("SELECT COUNT(*) FROM {name}"), [], |row| {
                row.get(0)
            })?;
        assert_eq!(count, 0, "new reviewer participation table {name}");
    }
    Ok(())
}

fn assert_no_foreign_key_violations(connection: &Connection) -> TestResult {
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    let mut rows = statement.query([])?;
    assert!(
        rows.next()?.is_none(),
        "schema upgrade left an FK violation"
    );
    Ok(())
}

fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Bounds("migration fixture time"))
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
