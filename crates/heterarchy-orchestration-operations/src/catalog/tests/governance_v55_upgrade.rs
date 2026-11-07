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
        TemporaryDispatchAdmissionRequest, TemporaryDispatchAdmissionResult,
        TemporaryDispatchRequestRecord, TemporaryDispatchRequestedScope,
    },
};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, DirectTokenId,
    TimestampMillis,
};
use rusqlite::types::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::PathBuf,
};
use uuid::Uuid;

const DESTINATION_ID: u128 = 0xA001;
const WORKSPACE_ID: u128 = 0xA002;
const REQUESTING_TEAM_ID: u128 = 0xA003;
const HOME_TEAM_ID: u128 = 0xA004;
const OWNER_NOW_MS: i64 = 1_000_000;

type TableRows = BTreeMap<String, Vec<Vec<Value>>>;

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

struct PersistedGovernanceRows {
    temporary_dispatch_request: TemporaryDispatchAdmissionRequest,
    temporary_dispatch_readback: TemporaryDispatchRequestRecord,
    rule_id: Uuid,
    rule_request_id: Uuid,
    rule_record: crate::reviewer_qualification_rule::ReviewerQualificationRuleGenerationRecord,
}

struct PersistedRuleRow {
    rule_id: Uuid,
    request_id: Uuid,
    record: crate::reviewer_qualification_rule::ReviewerQualificationRuleGenerationRecord,
}

struct TemporaryDispatchOwnerFixture {
    requesting_leader: FixtureParticipant,
    target: FixtureParticipant,
}

struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "operations-governance-v55-upgrade-{}.sqlite",
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
fn v55_upgrade_preserves_nonempty_governance_rows_and_starts_lifecycle_empty()
-> Result<(), Box<dyn Error>> {
    let database = TestDatabase::new();
    let destination_id = id(DESTINATION_ID);
    let workspace_id = WorkspaceId::from_uuid(id(WORKSPACE_ID));
    let mut catalog = OperationsCatalog::open(&database.path, destination_id)
        .map_err(|error| std::io::Error::other(format!("open initial catalog: {error}")))?;
    let persisted = seed_nonempty_v55_rows(&mut catalog, workspace_id)
        .map_err(|error| std::io::Error::other(format!("seed v55 owner records: {error}")))?;
    let v55_objects = schema_v55_objects();
    let original_rows = rows_for_schema(&catalog.connection, &v55_objects)?;
    assert_eq!(
        table_row_count(&original_rows, "ops_temporary_dispatch_requests")?,
        1
    );
    assert_eq!(
        table_row_count(
            &original_rows,
            "ops_reviewer_qualification_rule_generations"
        )?,
        1
    );

    // The post-v55 objects were created empty by the current open.
    // Drop only those objects to reconstruct v55; the two v55 receipt tables
    // remain populated and are included in the before/after row snapshot.
    let lifecycle_objects = crate::temporary_dispatch::lifecycle::lifecycle_schema_objects();
    assert_lifecycle_tables_empty(&catalog.connection, &lifecycle_objects)?;
    let mut newer_objects = lifecycle_objects.clone();
    newer_objects.extend_from_slice(crate::reviewer_participation::schema_objects());
    newer_objects.extend_from_slice(crate::team_expansion::schema_objects());
    newer_objects.extend_from_slice(crate::team_create_source::schema_objects());
    newer_objects.extend_from_slice(crate::team_expansion::workspace_hold_schema_objects());
    newer_objects.extend_from_slice(crate::team_expansion::legacy_slot_recovery_schema_objects());
    super::super::drop_empty_mission_schema_for_legacy_fixture(&catalog.connection, &newer_objects)
        .map_err(|error| std::io::Error::other(format!("restore v55 schema shape: {error}")))?;
    catalog.connection.pragma_update(None, "user_version", 55)?;
    validate_schema_v55(&catalog.connection)?;
    assert_eq!(catalog_version(&catalog.connection, destination_id)?, 55);
    assert_eq!(
        rows_for_schema(&catalog.connection, &v55_objects)?,
        original_rows
    );
    drop(catalog);

    let mut upgraded = OperationsCatalog::open(&database.path, destination_id)
        .map_err(|error| std::io::Error::other(format!("upgrade v55 catalog to v56: {error}")))?;
    assert_eq!(
        catalog_version(&upgraded.connection, destination_id)?,
        VERSION
    );
    validate_schema(&upgraded.connection)?;
    assert_eq!(
        rows_for_schema(&upgraded.connection, &v55_objects)?,
        original_rows
    );
    assert_lifecycle_tables_empty(&upgraded.connection, &lifecycle_objects)?;
    assert_no_foreign_key_violations(&upgraded.connection)?;
    assert_eq!(
        upgraded.read_temporary_dispatch_request(&persisted.temporary_dispatch_request)?,
        Some(persisted.temporary_dispatch_readback.clone())
    );
    assert_eq!(
        upgraded.reviewer_qualification_rule_request(
            workspace_id,
            persisted.rule_id,
            persisted.rule_request_id,
        )?,
        Some(persisted.rule_record.clone())
    );
    drop(upgraded);

    let mut read_only = OperationsCatalog::open_existing_read_only(&database.path, destination_id)
        .map_err(|error| std::io::Error::other(format!("read-only reopen v56 catalog: {error}")))?;
    assert_eq!(
        catalog_version(&read_only.connection, destination_id)?,
        VERSION
    );
    assert_eq!(
        rows_for_schema(&read_only.connection, &v55_objects)?,
        original_rows
    );
    assert_lifecycle_tables_empty(&read_only.connection, &lifecycle_objects)?;
    assert_no_foreign_key_violations(&read_only.connection)?;
    assert_eq!(
        read_only.read_temporary_dispatch_request(&persisted.temporary_dispatch_request)?,
        Some(persisted.temporary_dispatch_readback)
    );
    assert_eq!(
        read_only.reviewer_qualification_rule_request(
            workspace_id,
            persisted.rule_id,
            persisted.rule_request_id,
        )?,
        Some(persisted.rule_record)
    );
    Ok(())
}

fn seed_nonempty_v55_rows(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
) -> Result<PersistedGovernanceRows, Box<dyn Error>> {
    let owner = seed_temporary_dispatch_owner(catalog, workspace_id).map_err(|error| {
        std::io::Error::other(format!("seed verified Workspace and Teams: {error}"))
    })?;
    let (temporary_dispatch_request, temporary_dispatch_readback) =
        admit_temporary_dispatch_fixture(catalog, workspace_id, &owner)
            .map_err(|error| std::io::Error::other(format!("record TF admission: {error}")))?;
    let rule = record_qualification_rule_fixture(catalog, workspace_id, owner.requesting_leader)
        .map_err(|error| std::io::Error::other(format!("record qualification rule: {error}")))?;
    Ok(PersistedGovernanceRows {
        temporary_dispatch_request,
        temporary_dispatch_readback,
        rule_id: rule.rule_id,
        rule_request_id: rule.request_id,
        rule_record: rule.record,
    })
}

fn seed_temporary_dispatch_owner(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
) -> Result<TemporaryDispatchOwnerFixture, Box<dyn Error>> {
    // Synthetic verified-import fixture owner data. TF/rule receipts below are
    // created through their real Operations APIs.
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            id(0xA010).as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            catalog.destination_id().as_bytes(),
            [0x51_u8; 32].as_slice(),
        ],
    ).map_err(|error| std::io::Error::other(format!("insert verified import row: {error}")))?;

    let requesting_leader = FixtureParticipant::from_seed(1);
    let requesting_peer = FixtureParticipant::from_seed(2);
    let target = FixtureParticipant::from_seed(3);
    let home_leader = FixtureParticipant::from_seed(4);
    // These helpers seed synthetic native participation/effect-binding rows;
    // leadership receipts below are also fixture-only owner facts. They are
    // not Forge/Broker evidence or HTTP/Runtime authorization.
    for (member, request_seed) in [
        (requesting_leader, 0xA020),
        (requesting_peer, 0xA021),
        (target, 0xA022),
        (home_leader, 0xA023),
    ] {
        seed_native_participant_identity(
            &catalog.connection,
            workspace_id,
            member,
            id(request_seed),
        )
        .map_err(|error| {
            std::io::Error::other(format!("seed native participant/effect binding: {error}"))
        })?;
    }
    seed_synthetic_legacy_team(
        catalog,
        workspace_id,
        id(REQUESTING_TEAM_ID),
        "Receiving Team",
        &[requesting_leader, requesting_peer],
    )
    .map_err(|error| std::io::Error::other(format!("create requesting Team: {error}")))?;
    seed_synthetic_legacy_team(
        catalog,
        workspace_id,
        id(HOME_TEAM_ID),
        "Home Team",
        &[target, home_leader],
    )
    .map_err(|error| std::io::Error::other(format!("create home Team: {error}")))?;

    // These current Leadership rows are synthetic test facts, not Broker
    // confirmation evidence. They exercise the real TF admission checks.
    seed_test_leadership(
        &catalog.connection,
        workspace_id,
        id(REQUESTING_TEAM_ID),
        requesting_leader,
    )
    .map_err(|error| std::io::Error::other(format!("seed requesting Team leadership: {error}")))?;
    seed_test_leadership(
        &catalog.connection,
        workspace_id,
        id(HOME_TEAM_ID),
        home_leader,
    )
    .map_err(|error| std::io::Error::other(format!("seed home Team leadership: {error}")))?;
    Ok(TemporaryDispatchOwnerFixture {
        requesting_leader,
        target,
    })
}

fn seed_native_participant_identity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant: FixtureParticipant,
    request_seed: Uuid,
) -> Result<(), Box<dyn Error>> {
    // These are synthetic owner records. Insert the logical member before its
    // participation mapping because the current catalog enforces that FK.
    connection
        .execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                workspace_id.into_uuid().as_bytes(),
                participant.profile.as_bytes(),
                participant.participation.as_bytes(),
            ],
        )
        .map_err(|error| std::io::Error::other(format!("insert Workspace participation: {error}")))?;
    connection
        .execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Participant','worker',1,1,1)",
            params![
                workspace_id.into_uuid().as_bytes(),
                participant.participant.as_bytes(),
                participant.session.as_bytes(),
            ],
        )
        .map_err(|error| std::io::Error::other(format!("insert logical member: {error}")))?;
    connection
        .execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![
                request_seed.as_bytes(),
                workspace_id.into_uuid().as_bytes(),
                participant.participation.as_bytes(),
                participant.profile.as_bytes(),
                Uuid::from_u128(request_seed.as_u128() + 1).as_bytes(),
                participant.session.as_bytes(),
                participant.participant.as_bytes(),
            ],
        )
        .map_err(|error| std::io::Error::other(format!("insert participation mapping: {error}")))?;
    connection
        .execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
            params![
                workspace_id.into_uuid().as_bytes(),
                participant.participant.as_bytes(),
                participant.activity.as_bytes(),
                participant.session.as_bytes(),
                participant.agent.as_bytes(),
                Uuid::from_u128(request_seed.as_u128() + 2).as_bytes(),
            ],
        )
        .map_err(|error| std::io::Error::other(format!("insert current effect binding: {error}")))?;
    Ok(())
}

fn admit_temporary_dispatch_fixture(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    owner: &TemporaryDispatchOwnerFixture,
) -> Result<
    (
        TemporaryDispatchAdmissionRequest,
        TemporaryDispatchRequestRecord,
    ),
    Box<dyn Error>,
> {
    let request = TemporaryDispatchAdmissionRequest::new(
        id(0xA040),
        workspace_id,
        id(REQUESTING_TEAM_ID),
        1,
        owner.requesting_leader.actor(),
        owner.target.participant,
        TemporaryDispatchRequestedScope::new("migration fixture work intent".to_owned())?,
        timestamp(10_000_000)?,
    )?;
    let result = catalog.admit_temporary_dispatch(&request, || timestamp(OWNER_NOW_MS))?;
    let admitted = match result {
        TemporaryDispatchAdmissionResult::Admitted(record) => *record,
        TemporaryDispatchAdmissionResult::QuotaRejected { .. } => {
            return Err(std::io::Error::other("fixture TF request was quota-rejected").into());
        }
    };
    assert!(!admitted.replayed());
    let readback = catalog
        .read_temporary_dispatch_request(&request)?
        .ok_or("missing real TF admission readback")?;
    Ok((request, readback))
}

fn record_qualification_rule_fixture(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    requester: FixtureParticipant,
) -> Result<PersistedRuleRow, Box<dyn Error>> {
    let rule_id = id(0xA050);
    let rule_request_id = id(0xA051);
    let policy = migration_fixture_policy()?;
    let requester_session_id = AgentSessionId::from_uuid(requester.session);
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(id(0xA053)));
    // This principal/session pair exercises persistence only; it is not proof
    // of Runtime/Backend authentication or host OperatorPolicy authorization.
    let command = ReviewerQualificationRuleMutationCommand::new(
        workspace_id,
        rule_id,
        rule_request_id,
        None,
        requester_session_id,
        principal,
        ReviewerQualificationRuleChange::Publish { policy },
    )?;
    let _mutation = catalog.mutate_reviewer_qualification_rule(command)?;
    let record = catalog
        .reviewer_qualification_rule_request(workspace_id, rule_id, rule_request_id)?
        .ok_or("missing real qualification rule readback")?;
    assert_eq!(record.requester_session_id(), requester_session_id);
    assert_eq!(record.original_actor(), principal);
    Ok(PersistedRuleRow {
        rule_id,
        request_id: rule_request_id,
        record,
    })
}

fn migration_fixture_policy() -> Result<ReviewerQualificationRulePolicy, Box<dyn Error>> {
    let runtime_owner =
        EvidenceSourceOwnerId::new(EvidenceOwnerNamespace::RuntimeAdapter, id(0xA052))?;
    // User-selected test values only; no accepted model/tool defaults exist.
    let fact_predicates = BTreeSet::from([
        RuntimeFactPredicate::exact_text(
            ActualRuntimeFact::Model,
            BTreeSet::from([ExactRuntimeText::new("v55-fixture-model".to_owned())?]),
        )?,
        RuntimeFactPredicate::exact_text(
            ActualRuntimeFact::ToolVersions,
            BTreeSet::from([ExactRuntimeText::new("v55-fixture-tools".to_owned())?]),
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
            source_owner: runtime_owner,
            fact_predicates,
        }]),
    )?)
}

fn seed_synthetic_legacy_team(
    catalog: &mut OperationsCatalog,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    name: &str,
    members: &[FixtureParticipant],
) -> Result<(), Box<dyn Error>> {
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

fn seed_test_leadership(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    leader: FixtureParticipant,
) -> Result<(), Box<dyn Error>> {
    connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,2,NULL,?2,'STABILIZING',?3,1,1200100,NULL,100)",
        params![
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            leader.participant.as_bytes(),
        ],
    ).map_err(|error| std::io::Error::other(format!("insert ops_leadership row: {error}")))?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,1,'CONFIRMED',NULL,NULL,?3,1,100)",
        params![
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            leader.participant.as_bytes(),
        ],
    )
    .map_err(|error| {
        std::io::Error::other(format!("insert ops_leadership_receipts row: {error}"))
    })?;
    Ok(())
}

fn rows_for_schema(
    connection: &Connection,
    objects: &[(&'static str, &'static str, String)],
) -> Result<TableRows, Box<dyn Error>> {
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

fn assert_lifecycle_tables_empty(
    connection: &Connection,
    objects: &[(&'static str, &'static str, &'static str)],
) -> Result<(), Box<dyn Error>> {
    for &(kind, name, _) in objects {
        if kind != "table" {
            continue;
        }
        let count: i64 =
            connection.query_row(&format!("SELECT COUNT(*) FROM {name}"), [], |row| {
                row.get(0)
            })?;
        assert_eq!(count, 0, "v56 lifecycle table {name} must start empty");
    }
    Ok(())
}

fn assert_no_foreign_key_violations(connection: &Connection) -> Result<(), Box<dyn Error>> {
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    let mut rows = statement.query([])?;
    assert!(
        rows.next()?.is_none(),
        "migration left a foreign-key violation"
    );
    Ok(())
}

fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Bounds("migration fixture time"))
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
