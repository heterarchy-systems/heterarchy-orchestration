use super::super::*;
use crate::{
    OperationalActor,
    reviewer_participation::{
        ReviewerParticipationCommand, ReviewerParticipationReceipt, ReviewerParticipationStatement,
    },
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
    TimestampMillis, WorkspaceId,
};
use rusqlite::{Connection, params, types::Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::PathBuf,
};
use uuid::Uuid;

const DESTINATION_ID: u128 = 0xC001;
const WORKSPACE_ID: u128 = 0xC002;
const REQUESTING_TEAM_ID: u128 = 0xC003;
const HOME_TEAM_ID: u128 = 0xC004;
const ADMISSION_NOW_MS: i64 = 2_000_000;
const CONSENT_NOW_MS: i64 = 2_000_100;
const RESERVATION_NOW_MS: i64 = 2_000_200;

type TableRows = BTreeMap<String, Vec<Vec<Value>>>;
type TestResult = Result<(), Box<dyn Error>>;
type FixtureResult<T> = Result<T, Box<dyn Error>>;

mod transfer_generation_v65;

#[derive(Debug, Clone, Copy)]
struct Participant {
    profile: Uuid,
    participation: Uuid,
    logical_member: Uuid,
    activity: Uuid,
    agent: Uuid,
    session: Uuid,
}

impl Participant {
    fn from_seed(seed: u128) -> Self {
        Self {
            profile: id(0x10_000 + seed),
            participation: id(0x20_000 + seed),
            logical_member: id(0x30_000 + seed),
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

    fn display_name(self) -> String {
        format!("Synthetic participant {}", self.logical_member.as_u128())
    }
}

#[derive(Debug, Clone, Copy)]
struct LegacyOwners {
    requester: Participant,
    reviewer: Participant,
    target: Participant,
    home_leader: Participant,
    orchestrator: Participant,
}

struct DispatchRows {
    request: TemporaryDispatchAdmissionRequest,
    admission: TemporaryDispatchRequestRecord,
    binding: TemporaryDispatchAdmissionBinding,
    target: Participant,
    home_leader: Participant,
    orchestrator: Participant,
    target_consent: TemporaryDispatchConsentReceipt,
    home_consent: TemporaryDispatchConsentReceipt,
    orchestrator_consent: TemporaryDispatchConsentReceipt,
    reservation: TemporaryDispatchReservationReceipt,
}

struct RuleRow {
    rule_id: Uuid,
    request_id: Uuid,
    record: crate::reviewer_qualification_rule::ReviewerQualificationRuleGenerationRecord,
}

struct ParticipationRow {
    request_id: Uuid,
    actor: Participant,
    record: ReviewerParticipationReceipt,
}

struct V57Rows {
    dispatch: DispatchRows,
    rule: RuleRow,
    participation: ParticipationRow,
}

struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "operations-team-capacity-v57-upgrade-{}.sqlite",
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
fn v57_upgrade_preserves_team_governance_and_does_not_assign_legacy_capacity() -> TestResult {
    let database = TestDatabase::new();
    let destination_id = id(DESTINATION_ID);
    let workspace_id = WorkspaceId::from_uuid(id(WORKSPACE_ID));
    let mut catalog = OperationsCatalog::open(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&catalog.connection, destination_id)?,
        VERSION
    );
    validate_schema(&catalog.connection)?;

    let owners = seed_synthetic_v57_owner_facts(&mut catalog, workspace_id)?;
    let records = record_nonempty_v57_rows(&mut catalog, workspace_id, &owners)?;
    let v57_objects = schema_v57_objects();
    let original_rows = rows_for_schema(&catalog.connection, &v57_objects)?;
    assert_nonempty_v57_rows(&original_rows)?;
    let mut expansion_objects = crate::team_expansion::schema_objects().to_vec();
    expansion_objects.extend_from_slice(crate::team_create_source::schema_objects());
    expansion_objects.extend_from_slice(crate::team_expansion::workspace_hold_schema_objects());
    expansion_objects
        .extend_from_slice(crate::team_expansion::legacy_slot_recovery_schema_objects());
    assert_team_expansion_empty(&catalog.connection, &expansion_objects)?;
    assert_legacy_team_capacity_unset(&catalog, workspace_id)?;
    assert_no_foreign_key_violations(&catalog.connection)?;

    // The only removed objects are the empty schema-58 Team capacity and
    // expansion tables. The old Team rows and all schema-57 receipts remain.
    super::super::drop_empty_mission_schema_for_legacy_fixture(
        &catalog.connection,
        &expansion_objects,
    )?;
    catalog.connection.pragma_update(None, "user_version", 57)?;
    validate_schema_v57(&catalog.connection)?;
    assert_eq!(catalog_version(&catalog.connection, destination_id)?, 57);
    assert_eq!(
        rows_for_schema(&catalog.connection, &v57_objects)?,
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
        rows_for_schema(&upgraded.connection, &v57_objects)?,
        original_rows
    );
    assert_team_expansion_empty(&upgraded.connection, &expansion_objects)?;
    assert_legacy_team_capacity_unset(&upgraded, workspace_id)?;
    assert_no_foreign_key_violations(&upgraded.connection)?;
    assert_v57_readbacks(&mut upgraded, workspace_id, &records)?;
    drop(upgraded);

    let mut read_only = OperationsCatalog::open_existing_read_only(&database.path, destination_id)?;
    assert_eq!(
        catalog_version(&read_only.connection, destination_id)?,
        VERSION
    );
    validate_schema(&read_only.connection)?;
    assert_eq!(
        rows_for_schema(&read_only.connection, &v57_objects)?,
        original_rows
    );
    assert_team_expansion_empty(&read_only.connection, &expansion_objects)?;
    assert_legacy_team_capacity_unset(&read_only, workspace_id)?;
    assert_no_foreign_key_violations(&read_only.connection)?;
    assert_v57_readbacks(&mut read_only, workspace_id, &records)?;
    Ok(())
}

fn seed_synthetic_v57_owner_facts(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
) -> FixtureResult<LegacyOwners> {
    // These imported Workspace, Profile participation/effect-binding, Team,
    // roster, and leadership rows are explicitly synthetic v57 owner facts.
    // They do not assert Forge import, Broker leadership, or HTTP identity.
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            id(0xC010).as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            catalog.destination_id().as_bytes(),
            [0x61_u8; 32].as_slice(),
        ],
    )?;

    let requester = Participant::from_seed(1);
    let reviewer = Participant::from_seed(2);
    let target = Participant::from_seed(3);
    let home_leader = Participant::from_seed(4);
    let orchestrator = Participant::from_seed(5);
    for (participant, seed) in [
        (requester, 0xC020),
        (reviewer, 0xC021),
        (target, 0xC022),
        (home_leader, 0xC023),
        (orchestrator, 0xC024),
    ] {
        seed_participant_identity(&catalog.connection, workspace_id, participant, seed)?;
    }
    seed_team(
        &catalog.connection,
        workspace_id,
        id(REQUESTING_TEAM_ID),
        "Receiving Team",
        requester,
        &[requester, reviewer],
    )?;
    seed_team(
        &catalog.connection,
        workspace_id,
        id(HOME_TEAM_ID),
        "Home Team",
        home_leader,
        &[target, home_leader],
    )?;
    seed_team_leadership(
        &catalog.connection,
        workspace_id,
        Some(id(REQUESTING_TEAM_ID)),
        requester,
    )?;
    seed_team_leadership(
        &catalog.connection,
        workspace_id,
        Some(id(HOME_TEAM_ID)),
        home_leader,
    )?;
    seed_team_leadership(&catalog.connection, workspace_id, None, orchestrator)?;
    Ok(LegacyOwners {
        requester,
        reviewer,
        target,
        home_leader,
        orchestrator,
    })
}

fn seed_participant_identity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant: Participant,
    request_seed: u128,
) -> FixtureResult<()> {
    connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant.profile.as_bytes(),
            participant.participation.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,1)",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant.logical_member.as_bytes(),
            participant.session.as_bytes(),
            participant.display_name(),
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
            participant.logical_member.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant.logical_member.as_bytes(),
            participant.activity.as_bytes(),
            participant.session.as_bytes(),
            participant.agent.as_bytes(),
            id(request_seed + 2).as_bytes(),
        ],
    )?;
    Ok(())
}

fn seed_team(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    name: &str,
    creator: Participant,
    members: &[Participant],
) -> FixtureResult<()> {
    connection.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,?3,'synthetic pre-capacity Team fixture','ACTIVE',1,'synthetic:legacy-import',?4,?5,?6,?7,?8,100,100,100)",
        params![
            team_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            name,
            creator.display_name(),
            creator.participation.as_bytes(),
            creator.profile.as_bytes(),
            creator.logical_member.as_bytes(),
            creator.session.as_bytes(),
        ],
    )?;
    for member in members {
        connection.execute(
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

fn seed_team_leadership(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
    leader: Participant,
) -> FixtureResult<()> {
    let scope_kind = if team_id.is_some() { 2_i64 } else { 0_i64 };
    connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,?2,NULL,?3,'ACTIVE',?4,1,NULL,NULL,100)",
        params![
            workspace_id.into_uuid().as_bytes(),
            scope_kind,
            team_id.map(|team| team.as_bytes().to_vec()),
            leader.logical_member.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,?2,zeroblob(16),?3,1,'CONFIRMED',NULL,NULL,?4,1,100)",
        params![
            workspace_id.into_uuid().as_bytes(),
            scope_kind,
            team_id.map_or_else(|| vec![0_u8; 16], |team| team.as_bytes().to_vec()),
            leader.logical_member.as_bytes(),
        ],
    )?;
    Ok(())
}

fn record_nonempty_v57_rows(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    owners: &LegacyOwners,
) -> FixtureResult<V57Rows> {
    let dispatch = record_dispatch_rows(catalog, workspace_id, owners)?;
    let rule = record_rule_row(catalog, workspace_id, owners.orchestrator)?;
    let participation = record_participation_row(catalog, workspace_id, owners.reviewer)?;
    Ok(V57Rows {
        dispatch,
        rule,
        participation,
    })
}

fn record_dispatch_rows(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    owners: &LegacyOwners,
) -> FixtureResult<DispatchRows> {
    let request = TemporaryDispatchAdmissionRequest::new(
        id(0xC040),
        workspace_id,
        id(REQUESTING_TEAM_ID),
        1,
        owners.requester.actor(),
        owners.target.logical_member,
        TemporaryDispatchRequestedScope::new("v57 schema migration fixture intent".to_owned())?,
        timestamp(10_000_000)?,
    )?;
    let outcome = catalog.admit_temporary_dispatch(&request, || timestamp(ADMISSION_NOW_MS))?;
    let admission_created = match outcome {
        TemporaryDispatchAdmissionResult::Admitted(record) => *record,
        TemporaryDispatchAdmissionResult::QuotaRejected { .. } => {
            return Err("fixture temporary dispatch admission was quota-rejected".into());
        }
    };
    let admission = catalog
        .read_temporary_dispatch_request(&request)?
        .ok_or("missing temporary dispatch admission readback")?;
    assert_eq!(admission.request_id(), request.request_id());
    let binding = TemporaryDispatchAdmissionBinding::from_record(&admission_created);

    let target_consent = record_and_read_consent(
        catalog,
        &binding,
        owners.target,
        TemporaryDispatchConsentKind::TargetAcceptance,
        id(0xC041),
    )?;
    let home_consent = record_and_read_consent(
        catalog,
        &binding,
        owners.home_leader,
        TemporaryDispatchConsentKind::HomeLeaderConsent,
        id(0xC042),
    )?;
    let orchestrator_consent = record_and_read_consent(
        catalog,
        &binding,
        owners.orchestrator,
        TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval,
        id(0xC043),
    )?;

    // Existing v57 Reservations were possible before per-Team capacity was
    // introduced. The current write API now fails closed for an unconfigured
    // legacy Team, so this synthetic historic receipt/state is SQL-seeded and
    // then validated by the current read API after migration.
    seed_synthetic_reservation(
        &catalog.connection,
        &binding,
        &target_consent,
        &home_consent,
        &orchestrator_consent,
        timestamp(RESERVATION_NOW_MS)?,
    )?;
    let reservation = catalog
        .read_temporary_dispatch_reservation(&binding, owners.orchestrator.actor())?
        .ok_or("missing synthetic legacy reservation readback")?;
    assert_eq!(
        reservation.state(),
        TemporaryDispatchReservationState::Reserved
    );
    Ok(DispatchRows {
        request,
        admission,
        binding,
        target: owners.target,
        home_leader: owners.home_leader,
        orchestrator: owners.orchestrator,
        target_consent,
        home_consent,
        orchestrator_consent,
        reservation,
    })
}

fn record_and_read_consent(
    catalog: &mut OperationsCatalog,
    binding: &TemporaryDispatchAdmissionBinding,
    actor: Participant,
    kind: TemporaryDispatchConsentKind,
    receipt_id: Uuid,
) -> FixtureResult<TemporaryDispatchConsentReceipt> {
    let command =
        TemporaryDispatchConsentCommand::new(receipt_id, binding.clone(), kind, 0, actor.actor())?;
    let written =
        catalog.record_temporary_dispatch_consent(&command, || timestamp(CONSENT_NOW_MS))?;
    let readback = catalog
        .read_temporary_dispatch_consent(binding, receipt_id, actor.actor())?
        .ok_or("missing temporary dispatch consent readback")?;
    assert_eq!(readback.request_id(), written.request_id());
    assert_eq!(readback.receipt_id(), written.receipt_id());
    Ok(readback)
}

fn seed_synthetic_reservation(
    connection: &Connection,
    binding: &TemporaryDispatchAdmissionBinding,
    target_consent: &TemporaryDispatchConsentReceipt,
    home_consent: &TemporaryDispatchConsentReceipt,
    orchestrator_consent: &TemporaryDispatchConsentReceipt,
    recorded_at: TimestampMillis,
) -> FixtureResult<()> {
    let target_actor = target_consent.actor();
    let actor = orchestrator_consent.actor();
    connection.execute(
        "INSERT INTO ops_temporary_dispatch_reservation_receipts(receipt_id,request_id,revision,state,intent_digest,target_participation_id,target_profile_id,target_membership_revision,requesting_leadership_revision,home_leadership_revision,workspace_leadership_revision,target_consent_receipt_id,home_leader_consent_receipt_id,orchestrator_consent_receipt_id,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms) VALUES(?1,?1,1,'RESERVED',?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
        params![
            binding.request_id().as_bytes(),
            binding.intent_digest().as_slice(),
            target_actor.participation_id().into_uuid().as_bytes(),
            target_actor.profile_id().as_bytes(),
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
            target_actor.participation_id().into_uuid().as_bytes(),
            target_actor.profile_id().as_bytes(),
            binding.requested_scope().as_str(),
            binding.requested_end_at().get(),
            binding.intent_digest().as_slice(),
            recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn record_rule_row(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    author: Participant,
) -> FixtureResult<RuleRow> {
    let rule_id = id(0xC050);
    let request_id = id(0xC051);
    let author_session = AgentSessionId::from_uuid(author.session);
    let direct_token = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(id(0xC052)));
    // This DirectToken ID and Session are synthetic persistence inputs, not
    // bearer credentials or proof of Runtime OperatorPolicy authorization.
    let command = ReviewerQualificationRuleMutationCommand::new(
        workspace_id,
        rule_id,
        request_id,
        None,
        author_session,
        direct_token,
        ReviewerQualificationRuleChange::Publish {
            policy: migration_fixture_policy()?,
        },
    )?;
    let _mutation = catalog.mutate_reviewer_qualification_rule(command)?;
    let record = catalog
        .reviewer_qualification_rule_request(workspace_id, rule_id, request_id)?
        .ok_or("missing qualification rule readback")?;
    Ok(RuleRow {
        rule_id,
        request_id,
        record,
    })
}

fn migration_fixture_policy() -> FixtureResult<ReviewerQualificationRulePolicy> {
    let source_owner =
        EvidenceSourceOwnerId::new(EvidenceOwnerNamespace::RuntimeAdapter, id(0xC053))?;
    let fact_predicates = BTreeSet::from([
        RuntimeFactPredicate::exact_text(
            ActualRuntimeFact::Model,
            BTreeSet::from([ExactRuntimeText::new("v57-fixture-model".to_owned())?]),
        )?,
        RuntimeFactPredicate::exact_text(
            ActualRuntimeFact::ToolVersions,
            BTreeSet::from([ExactRuntimeText::new("v57-fixture-tools".to_owned())?]),
        )?,
        RuntimeFactPredicate::exact_digests(
            ActualRuntimeFact::EnvironmentFingerprint,
            BTreeSet::from([RuntimeFactDigest::new(
                ExactRuntimeText::new("fixture-digest".to_owned())?,
                vec![0x62; 32],
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

fn record_participation_row(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    actor: Participant,
) -> FixtureResult<ParticipationRow> {
    let request_id = id(0xC060);
    let command = ReviewerParticipationCommand::new(
        workspace_id,
        request_id,
        0,
        actor.actor(),
        ReviewerParticipationStatement::Interested,
    )?;
    let _written = catalog.record_reviewer_participation(&command)?;
    let record = catalog
        .reviewer_participation_request(workspace_id, request_id, actor.actor())?
        .ok_or("missing Reviewer participation readback")?;
    Ok(ParticipationRow {
        request_id,
        actor,
        record,
    })
}

fn assert_nonempty_v57_rows(rows: &TableRows) -> TestResult {
    for (table, expected) in [
        ("ops_teams", 2),
        ("ops_team_members", 4),
        ("ops_temporary_dispatch_requests", 1),
        ("ops_temporary_dispatch_quota_state", 1),
        ("ops_temporary_dispatch_consent_receipts", 3),
        ("ops_temporary_dispatch_current_consents", 3),
        ("ops_temporary_dispatch_reservation_receipts", 1),
        ("ops_temporary_dispatch_reservations", 1),
        ("ops_reviewer_qualification_rule_generations", 1),
        ("ops_reviewer_participation_receipts", 1),
        ("ops_reviewer_participation_heads", 1),
    ] {
        assert_eq!(table_row_count(rows, table)?, expected, "table {table}");
    }
    Ok(())
}

fn assert_v57_readbacks(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    records: &V57Rows,
) -> FixtureResult<()> {
    let dispatch = &records.dispatch;
    assert_eq!(
        catalog.read_temporary_dispatch_request(&dispatch.request)?,
        Some(dispatch.admission.clone())
    );
    for (receipt, actor) in [
        (&dispatch.target_consent, dispatch.target),
        (&dispatch.home_consent, dispatch.home_leader),
        (&dispatch.orchestrator_consent, dispatch.orchestrator),
    ] {
        assert_eq!(
            catalog.read_temporary_dispatch_consent(
                &dispatch.binding,
                receipt.receipt_id(),
                actor.actor(),
            )?,
            Some(receipt.clone())
        );
    }
    assert_eq!(
        catalog.read_temporary_dispatch_reservation(
            &dispatch.binding,
            dispatch.orchestrator.actor(),
        )?,
        Some(dispatch.reservation.clone())
    );
    assert_eq!(
        catalog.reviewer_qualification_rule_request(
            workspace_id,
            records.rule.rule_id,
            records.rule.request_id,
        )?,
        Some(records.rule.record.clone())
    );
    assert_eq!(
        catalog.reviewer_participation_request(
            workspace_id,
            records.participation.request_id,
            records.participation.actor.actor(),
        )?,
        Some(records.participation.record.clone())
    );
    let history = catalog.reviewer_participation_history(
        workspace_id,
        records.participation.actor.actor(),
        None,
        8,
    )?;
    assert_eq!(
        history.records(),
        std::slice::from_ref(&records.participation.record)
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

fn assert_team_expansion_empty(
    connection: &Connection,
    objects: &[(&'static str, &'static str, &'static str)],
) -> FixtureResult<()> {
    assert_schema_tables_empty(connection, objects)
}

fn assert_schema_tables_empty(
    connection: &Connection,
    objects: &[(&'static str, &'static str, &'static str)],
) -> FixtureResult<()> {
    for &(kind, name, _) in objects {
        if kind != "table" {
            continue;
        }
        let count: i64 =
            connection.query_row(&format!("SELECT COUNT(*) FROM {name}"), [], |row| {
                row.get(0)
            })?;
        assert_eq!(count, 0, "new Team expansion table {name}");
    }
    Ok(())
}

fn assert_legacy_team_capacity_unset(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
) -> FixtureResult<()> {
    for team_id in [id(REQUESTING_TEAM_ID), id(HOME_TEAM_ID)] {
        assert!(
            catalog.team_capacity(workspace_id, team_id)?.is_none(),
            "legacy Team {team_id} must not receive an inferred cap"
        );
    }
    Ok(())
}

fn assert_no_foreign_key_violations(connection: &Connection) -> FixtureResult<()> {
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    let mut rows = statement.query([])?;
    assert!(
        rows.next()?.is_none(),
        "schema migration left a foreign-key violation"
    );
    Ok(())
}

fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Bounds("migration fixture time"))
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
