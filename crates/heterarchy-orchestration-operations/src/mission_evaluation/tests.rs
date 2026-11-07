use std::{
    cell::Cell,
    error::Error,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    thread,
};

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
use rusqlite::params;
use uuid::Uuid;

use super::*;

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);
static PLAN_START_TEST_BUSY_OBSERVED: AtomicI64 = AtomicI64::new(0);

fn observe_plan_start_test_busy(_: i32) -> bool {
    PLAN_START_TEST_BUSY_OBSERVED.store(1, Ordering::Release);
    thread::yield_now();
    true
}

struct Fixture {
    catalog: OperationsCatalog,
    _files: TempDatabaseFiles,
    path: PathBuf,
    destination: Uuid,
    workspace: WorkspaceId,
    team: Uuid,
    joined_participation: WorkspaceParticipationId,
    absent_participation: WorkspaceParticipationId,
}

struct TempDatabaseFiles {
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MissionEvaluationCounts {
    requests: i64,
    missions: i64,
    team_heads: i64,
    obligations: i64,
    plans: i64,
    plan_versions: i64,
    plan_cohort_entries: i64,
    plan_starts: i64,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let serial = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mission-evaluation-{}-{serial}-{}.sqlite",
            std::process::id(),
            Uuid::now_v7()
        ));
        let destination = Uuid::now_v7();
        let files = TempDatabaseFiles { path: path.clone() };
        let workspace_uuid = Uuid::now_v7();
        let workspace = WorkspaceId::from_uuid(workspace_uuid);
        let team = Uuid::now_v7();
        let joined_profile = Uuid::now_v7();
        let joined_participation = participation(Uuid::now_v7())?;
        let absent_profile = Uuid::now_v7();
        let absent_participation = participation(Uuid::now_v7())?;
        let catalog = OperationsCatalog::open(&path, destination)?;
        let mut fixture = Self {
            catalog,
            _files: files,
            path,
            destination,
            workspace,
            team,
            joined_participation,
            absent_participation,
        };
        fixture.seed_verified_workspace(workspace)?;
        fixture.insert_team(workspace, team)?;
        fixture.insert_participation(workspace, joined_profile, joined_participation)?;
        fixture.insert_participation(workspace, absent_profile, absent_participation)?;
        fixture.insert_joined_member(workspace, team, joined_participation)?;
        Ok(fixture)
    }

    fn catalog(&self) -> &OperationsCatalog {
        &self.catalog
    }

    fn catalog_mut(&mut self) -> &mut OperationsCatalog {
        &mut self.catalog
    }

    fn seed_verified_workspace(&mut self, workspace: WorkspaceId) -> Result<(), Box<dyn Error>> {
        let destination = self.destination;
        self.catalog_mut().connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                Uuid::now_v7().as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
                [0x51_u8; 32].as_slice(),
            ],
        )?;
        Ok(())
    }

    fn insert_team(&mut self, workspace: WorkspaceId, team_id: Uuid) -> Result<(), Box<dyn Error>> {
        self.catalog_mut().connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'evaluation','mission evaluation','ACTIVE',1,'operator','Local operator',1,1,1)",
            params![team_id.as_bytes(), workspace.into_uuid().as_bytes()],
        )?;
        Ok(())
    }

    fn insert_participation(
        &mut self,
        workspace: WorkspaceId,
        profile_id: Uuid,
        participation_id: WorkspaceParticipationId,
    ) -> Result<(), Box<dyn Error>> {
        self.catalog_mut().connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn insert_joined_member(
        &mut self,
        workspace: WorkspaceId,
        team_id: Uuid,
        participation_id: WorkspaceParticipationId,
    ) -> Result<(), Box<dyn Error>> {
        self.catalog_mut().connection.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,1,NULL,'member')",
            params![
                workspace.into_uuid().as_bytes(),
                team_id.as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn seed_mission_with_obligation(
        &mut self,
        id_base: u128,
    ) -> Result<MissionWithObligation, Box<dyn Error>> {
        let workspace = self.workspace;
        let team = self.team;
        let participation_id = self.joined_participation;
        let mission = mission_id(id_base + 1);
        let created = self.catalog_mut().create_mission(&create_mission_command(
            workspace,
            team,
            uuid(id_base + 2),
            mission,
            "fixture mission",
            10,
        ))?;
        assert_eq!(created.mission().mission_id(), mission);
        let obligation = obligation_id(id_base + 3);
        let command = record_obligation_command(ObligationFixtureInput {
            request_id: uuid(id_base + 4),
            obligation_id: obligation,
            workspace_id: workspace,
            mission_id: mission,
            participation_id,
            work_unit_id: work_unit_id(id_base + 5),
            weight: 4,
            criteria: "fixture acceptance criterion",
            due_at_ms: 300,
            recorded_at_ms: 20,
        });
        let recorded = self.catalog_mut().record_mission_obligation(&command)?;
        assert_eq!(recorded.obligation().obligation_id(), obligation);
        Ok(MissionWithObligation {
            mission,
            obligation,
            obligation_command: command,
        })
    }

    fn seed_mission_with_two_obligations(
        &mut self,
        id_base: u128,
    ) -> Result<MissionWithTwoObligations, Box<dyn Error>> {
        let first = self.seed_mission_with_obligation(id_base)?;
        let second = obligation_id(id_base + 7);
        let command = record_obligation_command(ObligationFixtureInput {
            request_id: uuid(id_base + 6),
            obligation_id: second,
            workspace_id: self.workspace,
            mission_id: first.mission,
            participation_id: self.joined_participation,
            work_unit_id: work_unit_id(id_base + 8),
            weight: 5,
            criteria: "second fixture acceptance criterion",
            due_at_ms: 300,
            recorded_at_ms: 21,
        });
        let recorded = self.catalog_mut().record_mission_obligation(&command)?;
        assert_eq!(recorded.obligation().obligation_id(), second);
        Ok(MissionWithTwoObligations {
            mission: first.mission,
            first: first.obligation,
            second,
        })
    }

    fn add_workspace(&mut self) -> Result<WorkspaceId, Box<dyn Error>> {
        let workspace = WorkspaceId::from_uuid(Uuid::now_v7());
        self.seed_verified_workspace(workspace)?;
        Ok(workspace)
    }

    fn add_team(&mut self) -> Result<Uuid, Box<dyn Error>> {
        let team = Uuid::now_v7();
        let workspace = self.workspace;
        self.insert_team(workspace, team)?;
        Ok(team)
    }

    fn counts(&self) -> Result<MissionEvaluationCounts, Box<dyn Error>> {
        let connection = &self.catalog().connection;
        Ok(MissionEvaluationCounts {
            requests: connection.query_row(
                "SELECT COUNT(*) FROM ops_mission_evaluation_requests",
                [],
                |row| row.get(0),
            )?,
            missions: connection.query_row(
                "SELECT COUNT(*) FROM ops_mission_evaluation_missions",
                [],
                |row| row.get(0),
            )?,
            team_heads: connection.query_row(
                "SELECT COUNT(*) FROM ops_mission_evaluation_team_heads",
                [],
                |row| row.get(0),
            )?,
            obligations: connection.query_row(
                "SELECT COUNT(*) FROM ops_mission_evaluation_obligations",
                [],
                |row| row.get(0),
            )?,
            plans: connection.query_row(
                "SELECT COUNT(*) FROM ops_mission_evaluation_plans",
                [],
                |row| row.get(0),
            )?,
            plan_versions: connection.query_row(
                "SELECT COUNT(*) FROM ops_mission_evaluation_plan_versions",
                [],
                |row| row.get(0),
            )?,
            plan_cohort_entries: connection.query_row(
                "SELECT COUNT(*) FROM ops_mission_evaluation_plan_cohort",
                [],
                |row| row.get(0),
            )?,
            plan_starts: connection.query_row(
                "SELECT COUNT(*) FROM ops_mission_evaluation_plan_starts",
                [],
                |row| row.get(0),
            )?,
        })
    }

    fn reopen(&mut self) -> Result<(), Box<dyn Error>> {
        self.catalog = OperationsCatalog::open(&self.path, self.destination)?;
        Ok(())
    }

    fn insert_actor_binding(
        &mut self,
        workspace: WorkspaceId,
    ) -> Result<(OperationalActor, Uuid), Box<dyn Error>> {
        let profile_id = Uuid::now_v7();
        let participation_id = participation(Uuid::now_v7())?;
        let participant_id = Uuid::now_v7();
        let activity_id = ActivityId::from_uuid(Uuid::now_v7());
        let agent_id = AgentInstanceId::from_uuid(Uuid::now_v7());
        let session_id = AgentSessionId::from_uuid(Uuid::now_v7());
        let request_id = Uuid::now_v7();
        let connection = &self.catalog_mut().connection;
        connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'workspace leader','worker',1,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![
                Uuid::now_v7().as_bytes(),
                workspace.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                Uuid::now_v7().as_bytes(),
                session_id.into_uuid().as_bytes(),
                participant_id.as_bytes(),
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                activity_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                request_id.as_bytes(),
            ],
        )?;
        Ok((
            OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id,
            },
            participant_id,
        ))
    }

    fn set_current_workspace_leader(
        &mut self,
        workspace: WorkspaceId,
        participant_id: Uuid,
    ) -> Result<(), Box<dyn Error>> {
        let connection = &self.catalog_mut().connection;
        connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,1,NULL,NULL,1)",
            params![workspace.into_uuid().as_bytes(), participant_id.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),1,'CONFIRMED',?2,1,1)",
            params![workspace.into_uuid().as_bytes(), participant_id.as_bytes()],
        )?;
        Ok(())
    }

    fn replace_current_workspace_leader(
        &mut self,
        workspace: WorkspaceId,
        participant_id: Uuid,
        revision: i64,
    ) -> Result<(), Box<dyn Error>> {
        let connection = &self.catalog_mut().connection;
        let updated = connection.execute(
            "UPDATE ops_leadership SET leader_participant_id=?1,leadership_revision=?2,updated_at_ms=?3 WHERE workspace_id=?4 AND scope_kind=0 AND activity_id IS NULL AND team_id IS NULL",
            params![
                participant_id.as_bytes(),
                revision,
                revision,
                workspace.into_uuid().as_bytes(),
            ],
        )?;
        assert_eq!(updated, 1);
        connection.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),?2,'CONFIRMED',?3,?2,?2)",
            params![
                workspace.into_uuid().as_bytes(),
                revision,
                participant_id.as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn rebind_actor_session_for_test(
        &mut self,
        workspace: WorkspaceId,
        actor: OperationalActor,
    ) -> Result<OperationalActor, Box<dyn Error>> {
        let OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } = actor
        else {
            return Err(std::io::Error::other("fixture requires participant actor").into());
        };
        let rebound_session = AgentSessionId::from_uuid(Uuid::now_v7());
        let participant_id: Vec<u8> = self.catalog().connection.query_row(
            "SELECT substr(participant_id,1,17) FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3 AND session_id=?4",
            params![
                workspace.into_uuid().as_bytes(),
                activity_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        let participant_id = Uuid::from_bytes(crate::wire::id(participant_id)?);
        let connection = &self.catalog_mut().connection;
        let member_updated = connection.execute(
            "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3 AND session_id=?4",
            params![
                rebound_session.into_uuid().as_bytes(),
                workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
        )?;
        let binding_updated = connection.execute(
            "UPDATE ops_current_effect_bindings SET session_id=?1 WHERE workspace_id=?2 AND activity_id=?3 AND agent_id=?4 AND session_id=?5",
            params![
                rebound_session.into_uuid().as_bytes(),
                workspace.into_uuid().as_bytes(),
                activity_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
        )?;
        assert_eq!(member_updated, 1);
        assert_eq!(binding_updated, 1);
        Ok(OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id: rebound_session,
        })
    }

    fn force_noncurrent_mission_for_test(
        &mut self,
        workspace: WorkspaceId,
        team: Uuid,
        next_mission: MissionId,
    ) -> Result<(), Box<dyn Error>> {
        // Test-only setup models a later custody-authorized head move. Production
        // intentionally exposes no Mission close/cutover operation in this slice.
        let request_id = uuid(0x7ff);
        let connection = &self.catalog_mut().connection;
        connection.execute(
            "INSERT INTO ops_mission_evaluation_requests(request_id,operation,workspace_id,target_id,revision,actor_kind,actor_key,actor_display_name) VALUES(?1,0,?2,?3,1,0,'operator','Local operator')",
            params![
                request_id.as_bytes(),
                workspace.into_uuid().as_bytes(),
                next_mission.into_uuid().as_bytes(),
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_mission_evaluation_missions(mission_id,workspace_id,team_id,create_request_id,purpose,created_at_ms,revision) VALUES(?1,?2,?3,?4,'post-custody fixture Mission',30,1)",
            params![
                next_mission.into_uuid().as_bytes(),
                workspace.into_uuid().as_bytes(),
                team.as_bytes(),
                request_id.as_bytes(),
            ],
        )?;
        connection.execute_batch(
            "DROP TRIGGER ops_mission_evaluation_team_heads_update;
             DROP TRIGGER ops_mission_evaluation_team_heads_delete;",
        )?;
        connection.execute(
            "UPDATE ops_mission_evaluation_team_heads SET mission_id=?1,revision=revision+1 WHERE workspace_id=?2 AND team_id=?3",
            params![
                next_mission.into_uuid().as_bytes(),
                workspace.into_uuid().as_bytes(),
                team.as_bytes(),
            ],
        )?;
        let mut restored = 0;
        for (kind, name, sql) in super::schema_objects() {
            if kind == "trigger"
                && matches!(
                    name,
                    "ops_mission_evaluation_team_heads_update"
                        | "ops_mission_evaluation_team_heads_delete"
                )
            {
                connection.execute_batch(sql)?;
                restored += 1;
            }
        }
        assert_eq!(restored, 2);
        Ok(())
    }
}

impl Drop for TempDatabaseFiles {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = self.path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let _ = fs::remove_file(PathBuf::from(sidecar));
        }
    }
}

fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn mission_id(value: u128) -> MissionId {
    MissionId(uuid(value))
}

fn obligation_id(value: u128) -> MissionObligationId {
    MissionObligationId(uuid(value))
}

fn work_unit_id(value: u128) -> MissionWorkUnitId {
    MissionWorkUnitId(uuid(value))
}

fn plan_id(value: u128) -> MissionEvaluationPlanId {
    MissionEvaluationPlanId(uuid(value))
}

fn plan_version_id(value: u128) -> MissionEvaluationPlanVersionId {
    MissionEvaluationPlanVersionId(uuid(value))
}

fn participation(value: Uuid) -> Result<WorkspaceParticipationId, OperationsError> {
    WorkspaceParticipationId::from_uuid(value)
}

fn operator_authority() -> MissionEvaluationAuthority {
    MissionEvaluationAuthority {
        actor: OperationalActor::LocalOperator,
        expected_workspace_leadership_revision: None,
    }
}

fn create_mission_command(
    workspace_id: WorkspaceId,
    team_id: Uuid,
    request: Uuid,
    mission: MissionId,
    purpose: &str,
    created_at_ms: i64,
) -> MissionCreateCommand {
    MissionCreateCommand {
        request_id: request,
        mission_id: mission,
        workspace_id,
        team_id,
        purpose: purpose.to_owned(),
        created_at_ms,
        authority: operator_authority(),
    }
}

#[derive(Clone, Copy)]
struct ObligationFixtureInput {
    request_id: Uuid,
    obligation_id: MissionObligationId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    participation_id: WorkspaceParticipationId,
    work_unit_id: MissionWorkUnitId,
    weight: u64,
    criteria: &'static str,
    due_at_ms: i64,
    recorded_at_ms: i64,
}

fn record_obligation_command(input: ObligationFixtureInput) -> MissionObligationRecordCommand {
    MissionObligationRecordCommand {
        request_id: input.request_id,
        obligation_id: input.obligation_id,
        workspace_id: input.workspace_id,
        mission_id: input.mission_id,
        participation_id: input.participation_id,
        work_unit_id: input.work_unit_id,
        weight: PositiveWeight(input.weight),
        acceptance_criteria: input.criteria.to_owned(),
        due_at_ms: input.due_at_ms,
        recorded_at_ms: input.recorded_at_ms,
        authority: operator_authority(),
    }
}

struct PlanFixtureInput {
    request_id: Uuid,
    plan_id: MissionEvaluationPlanId,
    version_id: MissionEvaluationPlanVersionId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    expected_head_revision: u64,
    t0_commit_at_ms: i64,
    t1_submission_cutoff_ms: i64,
    t2_checkpoint_ms: i64,
    cohort: Vec<MissionObligationId>,
}

struct MissionWithObligation {
    mission: MissionId,
    obligation: MissionObligationId,
    obligation_command: MissionObligationRecordCommand,
}

struct MissionWithTwoObligations {
    mission: MissionId,
    first: MissionObligationId,
    second: MissionObligationId,
}

fn plan_publish_command(
    input: PlanFixtureInput,
) -> Result<MissionEvaluationPlanPublishCommand, OperationsError> {
    let PlanFixtureInput {
        request_id,
        plan_id,
        version_id,
        workspace_id,
        mission_id,
        expected_head_revision,
        t0_commit_at_ms,
        t1_submission_cutoff_ms,
        t2_checkpoint_ms,
        cohort,
    } = input;
    MissionEvaluationPlanPublishCommand::new(
        request_id,
        plan_id,
        version_id,
        workspace_id,
        mission_id,
        expected_head_revision,
        t0_commit_at_ms,
        t1_submission_cutoff_ms,
        t2_checkpoint_ms,
        cohort
            .into_iter()
            .map(MissionEvaluationPlanObligationInput::new)
            .collect(),
        operator_authority(),
    )
}

#[test]
fn mission_creation_replays_exactly_and_reads_after_reopen() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let team = fixture.team;
    let mission = mission_id(0x101);
    let request = uuid(0x102);
    let command = create_mission_command(
        workspace,
        team,
        request,
        mission,
        "deliver a verified migration",
        100,
    );
    let created = fixture.catalog_mut().create_mission(&command)?;
    assert!(!created.replayed());
    assert_eq!(created.mission().revision(), 1);
    assert!(fixture.catalog_mut().create_mission(&command)?.replayed());
    let before_receipt_read = fixture.counts()?;
    let receipt = fixture
        .catalog()
        .mission_evaluation_request(workspace, request)?
        .ok_or_else(|| std::io::Error::other("Mission request receipt missing"))?;
    assert_eq!(receipt.request_id(), request);
    assert_eq!(receipt.workspace_id(), workspace);
    assert!(matches!(
        receipt.mutation(),
        MissionEvaluationRecordedMutation::MissionCreated(record)
            if record.mission_id() == mission
    ));
    assert_eq!(fixture.counts()?, before_receipt_read);
    assert!(
        fixture
            .catalog()
            .mission_evaluation_request(workspace, uuid(0x105))?
            .is_none()
    );
    let generated_time_changed = create_mission_command(
        workspace,
        team,
        request,
        mission,
        "deliver a verified migration",
        101,
    );
    let generated_time_replay = fixture
        .catalog_mut()
        .create_mission(&generated_time_changed)?;
    assert!(generated_time_replay.replayed());
    assert_eq!(generated_time_replay.mission().created_at_ms(), 100);

    let changed = create_mission_command(
        workspace,
        team,
        request,
        mission,
        "changed request content",
        100,
    );
    assert!(matches!(
        fixture.catalog_mut().create_mission(&changed),
        Err(OperationsError::Conflict(_))
    ));
    let second_workspace = fixture.add_workspace()?;
    assert!(matches!(
        fixture.catalog().mission(second_workspace, mission),
        Err(OperationsError::Conflict(_))
    ));
    assert!(matches!(
        fixture
            .catalog()
            .mission_evaluation_request(second_workspace, request),
        Err(OperationsError::Conflict(_))
    ));
    fixture.reopen()?;
    let readback = fixture
        .catalog()
        .mission(workspace, mission)?
        .ok_or_else(|| std::io::Error::other("Mission readback missing"))?;
    assert_eq!(readback.purpose(), "deliver a verified migration");
    assert_eq!(readback.created_at_ms(), 100);

    let before = fixture.counts()?;
    let duplicate_team_mission = create_mission_command(
        workspace,
        team,
        uuid(0x103),
        mission_id(0x104),
        "a second active Mission",
        101,
    );
    assert!(matches!(
        fixture
            .catalog_mut()
            .create_mission(&duplicate_team_mission),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(fixture.counts()?, before);
    Ok(())
}

#[test]
fn team_mission_head_fails_closed_without_custody_transition() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let team = fixture.team;
    let seeded = fixture.seed_mission_with_obligation(0x700)?;
    assert!(
        fixture
            .catalog()
            .mission(workspace, seeded.mission)?
            .is_some()
    );

    let update = fixture.catalog().connection.execute(
        "UPDATE ops_mission_evaluation_team_heads SET revision=revision+1 WHERE workspace_id=?1 AND team_id=?2",
        params![workspace.into_uuid().as_bytes(), team.as_bytes()],
    );
    assert!(update.is_err());
    let delete = fixture.catalog().connection.execute(
        "DELETE FROM ops_mission_evaluation_team_heads WHERE workspace_id=?1 AND team_id=?2",
        params![workspace.into_uuid().as_bytes(), team.as_bytes()],
    );
    assert!(delete.is_err());
    Ok(())
}

#[test]
fn historical_mission_reads_and_exact_replays_survive_head_change() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let team = fixture.team;
    let seeded = fixture.seed_mission_with_obligation(0x701)?;

    let started_plan = plan_id(0x706);
    let published_plan_command = plan_publish_command(PlanFixtureInput {
        request_id: uuid(0x707),
        plan_id: started_plan,
        version_id: plan_version_id(0x708),
        workspace_id: workspace,
        mission_id: seeded.mission,
        expected_head_revision: 0,
        t0_commit_at_ms: 100,
        t1_submission_cutoff_ms: 200,
        t2_checkpoint_ms: 250,
        cohort: vec![seeded.obligation],
    })?;
    let published = fixture
        .catalog_mut()
        .publish_mission_evaluation_plan(&published_plan_command)?;
    assert_eq!(published.version().revision(), 1);
    let plan_start_command = MissionEvaluationPlanStartCommand::new(
        uuid(0x709),
        workspace,
        seeded.mission,
        started_plan,
        1,
        operator_authority(),
    )?;
    let started = fixture
        .catalog_mut()
        .start_mission_evaluation_plan(&plan_start_command, || Ok(150))?;
    assert_eq!(started.start().revision(), 1);

    let unstarted_plan =
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0x70b),
                plan_id: plan_id(0x70a),
                version_id: plan_version_id(0x70c),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 0,
                t0_commit_at_ms: 110,
                t1_submission_cutoff_ms: 220,
                t2_checkpoint_ms: 270,
                cohort: vec![seeded.obligation],
            })?)?;
    assert_eq!(unstarted_plan.version().revision(), 1);

    fixture.force_noncurrent_mission_for_test(workspace, team, mission_id(0x70d))?;
    fixture.reopen()?;
    assert!(
        fixture
            .catalog()
            .mission(workspace, seeded.mission)?
            .is_some()
    );
    assert!(
        fixture
            .catalog()
            .mission_obligation(workspace, seeded.obligation)?
            .is_some()
    );
    assert!(
        fixture
            .catalog()
            .mission_evaluation_plan_version(workspace, started_plan, 1)?
            .is_some()
    );
    assert!(
        fixture
            .catalog()
            .mission_evaluation_plan_start(workspace, started_plan)?
            .is_some()
    );
    assert!(
        fixture
            .catalog_mut()
            .record_mission_obligation(&seeded.obligation_command)?
            .replayed()
    );
    assert!(
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&published_plan_command)?
            .replayed()
    );
    let replay_clock_calls = Cell::new(0);
    assert!(
        fixture
            .catalog_mut()
            .start_mission_evaluation_plan(&plan_start_command, || {
                replay_clock_calls.set(replay_clock_calls.get() + 1);
                Err(OperationsError::Conflict("replay sampled start clock"))
            })?
            .replayed()
    );
    assert_eq!(replay_clock_calls.get(), 0);
    Ok(())
}

#[test]
fn new_writes_reject_a_noncurrent_mission_without_partial_rows() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let team = fixture.team;
    let joined = fixture.joined_participation;
    let seeded = fixture.seed_mission_with_obligation(0x801)?;
    let unstarted_plan = plan_id(0x810);
    let plan_published =
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0x811),
                plan_id: unstarted_plan,
                version_id: plan_version_id(0x812),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 0,
                t0_commit_at_ms: 100,
                t1_submission_cutoff_ms: 200,
                t2_checkpoint_ms: 250,
                cohort: vec![seeded.obligation],
            })?)?;
    assert_eq!(plan_published.version().revision(), 1);
    fixture.force_noncurrent_mission_for_test(workspace, team, mission_id(0x81b))?;
    let before = fixture.counts()?;

    assert!(matches!(
        fixture
            .catalog_mut()
            .record_mission_obligation(&record_obligation_command(ObligationFixtureInput {
                request_id: uuid(0x814),
                obligation_id: obligation_id(0x815),
                workspace_id: workspace,
                mission_id: seeded.mission,
                participation_id: joined,
                work_unit_id: work_unit_id(0x816),
                weight: 1,
                criteria: "new work on a historical Mission",
                due_at_ms: 300,
                recorded_at_ms: 30,
            })),
        Err(OperationsError::Conflict(
            "mission is not current for exact Team"
        ))
    ));
    assert!(matches!(
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0x817),
                plan_id: plan_id(0x818),
                version_id: plan_version_id(0x819),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 0,
                t0_commit_at_ms: 110,
                t1_submission_cutoff_ms: 210,
                t2_checkpoint_ms: 260,
                cohort: vec![seeded.obligation],
            })?),
        Err(OperationsError::Conflict(
            "mission is not current for exact Team"
        ))
    ));
    let clock_calls = Cell::new(0);
    assert!(matches!(
        fixture.catalog_mut().start_mission_evaluation_plan(
            &MissionEvaluationPlanStartCommand::new(
                uuid(0x81a),
                workspace,
                seeded.mission,
                unstarted_plan,
                1,
                operator_authority(),
            )?,
            || {
                clock_calls.set(clock_calls.get() + 1);
                Err(OperationsError::Conflict(
                    "clock sampled before Mission check",
                ))
            }
        ),
        Err(OperationsError::Conflict(
            "mission is not current for exact Team"
        ))
    ));
    assert_eq!(clock_calls.get(), 0);
    assert_eq!(fixture.counts()?, before);
    Ok(())
}

#[test]
fn obligation_replay_binds_identity_and_immutable_domain_content() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let mission = fixture.seed_mission_with_obligation(0x200)?.mission;
    let participation_id = fixture.joined_participation;
    let work_unit = work_unit_id(0x208);
    let command = record_obligation_command(ObligationFixtureInput {
        request_id: uuid(0x203),
        obligation_id: obligation_id(0x204),
        workspace_id: workspace,
        mission_id: mission,
        participation_id,
        work_unit_id: work_unit,
        weight: 7,
        criteria: "publish a checked artifact",
        due_at_ms: 200,
        recorded_at_ms: 20,
    });
    let recorded = fixture.catalog_mut().record_mission_obligation(&command)?;
    assert!(!recorded.replayed());
    assert_eq!(recorded.obligation().weight().get(), 7);
    assert!(matches!(
        fixture
            .catalog()
            .mission_evaluation_request(workspace, uuid(0x203))?
            .ok_or_else(|| std::io::Error::other("obligation request receipt missing"))?
            .mutation(),
        MissionEvaluationRecordedMutation::ObligationRecorded(record)
            if record.obligation_id() == obligation_id(0x204)
    ));
    assert!(
        fixture
            .catalog_mut()
            .record_mission_obligation(&command)?
            .replayed()
    );

    let server_time_retry = record_obligation_command(ObligationFixtureInput {
        request_id: uuid(0x203),
        obligation_id: obligation_id(0x204),
        workspace_id: workspace,
        mission_id: mission,
        participation_id,
        work_unit_id: work_unit,
        weight: 7,
        criteria: "publish a checked artifact",
        due_at_ms: 200,
        recorded_at_ms: 25,
    });
    let replay = fixture
        .catalog_mut()
        .record_mission_obligation(&server_time_retry)?;
    assert!(replay.replayed());
    assert_eq!(replay.obligation().recorded_at_ms(), 20);

    let changed_weight = record_obligation_command(ObligationFixtureInput {
        request_id: uuid(0x203),
        obligation_id: obligation_id(0x204),
        workspace_id: workspace,
        mission_id: mission,
        participation_id,
        work_unit_id: work_unit,
        weight: 8,
        criteria: "publish a checked artifact",
        due_at_ms: 200,
        recorded_at_ms: 20,
    });
    assert!(matches!(
        fixture
            .catalog_mut()
            .record_mission_obligation(&changed_weight),
        Err(OperationsError::Conflict(_))
    ));

    let alternate_id = record_obligation_command(ObligationFixtureInput {
        request_id: uuid(0x206),
        obligation_id: obligation_id(0x207),
        workspace_id: workspace,
        mission_id: mission,
        participation_id,
        work_unit_id: work_unit,
        weight: 7,
        criteria: "publish a checked artifact",
        due_at_ms: 200,
        recorded_at_ms: 20,
    });
    assert!(matches!(
        fixture
            .catalog_mut()
            .record_mission_obligation(&alternate_id),
        Err(OperationsError::Conflict(_))
    ));
    Ok(())
}

#[test]
fn obligation_weight_is_immutable_and_admission_requires_joined_participant()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let absent = fixture.absent_participation;
    let seeded = fixture.seed_mission_with_obligation(0x220)?;
    let mission = seeded.mission;
    let obligation = seeded.obligation;
    let update_result = fixture.catalog().connection.execute(
        "UPDATE ops_mission_evaluation_obligations SET weight=99 WHERE obligation_id=?1",
        params![obligation.into_uuid().as_bytes()],
    );
    assert!(update_result.is_err());
    assert_eq!(
        fixture
            .catalog()
            .mission_obligation(workspace, obligation)?
            .ok_or_else(|| std::io::Error::other("obligation readback missing"))?
            .weight()
            .get(),
        4
    );
    fixture.reopen()?;
    assert_eq!(
        fixture
            .catalog()
            .mission_obligation(workspace, obligation)?
            .ok_or_else(|| std::io::Error::other("reopened obligation missing"))?
            .weight()
            .get(),
        4
    );

    let before = fixture.counts()?;
    let not_joined = record_obligation_command(ObligationFixtureInput {
        request_id: uuid(0x228),
        obligation_id: obligation_id(0x229),
        workspace_id: workspace,
        mission_id: mission,
        participation_id: absent,
        work_unit_id: work_unit_id(0x22a),
        weight: 1,
        criteria: "must belong to the exact Team",
        due_at_ms: 200,
        recorded_at_ms: 21,
    });
    assert!(matches!(
        fixture.catalog_mut().record_mission_obligation(&not_joined),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(fixture.counts()?, before);
    Ok(())
}

#[test]
fn plan_command_validation_rejects_bad_timeline_and_duplicate_cohort() {
    let workspace = WorkspaceId::from_uuid(Uuid::now_v7());
    let mission = mission_id(0x301);
    let obligation = obligation_id(0x303);
    assert!(matches!(
        plan_publish_command(PlanFixtureInput {
            request_id: uuid(0x309),
            plan_id: plan_id(0x30a),
            version_id: plan_version_id(0x30b),
            workspace_id: workspace,
            mission_id: mission,
            expected_head_revision: 0,
            t0_commit_at_ms: 100,
            t1_submission_cutoff_ms: 130,
            t2_checkpoint_ms: 120,
            cohort: vec![obligation],
        }),
        Err(OperationsError::Conflict(
            "mission evaluation plan timeline"
        ))
    ));
    assert!(matches!(
        plan_publish_command(PlanFixtureInput {
            request_id: uuid(0x30c),
            plan_id: plan_id(0x30d),
            version_id: plan_version_id(0x30e),
            workspace_id: workspace,
            mission_id: mission,
            expected_head_revision: 0,
            t0_commit_at_ms: 100,
            t1_submission_cutoff_ms: 110,
            t2_checkpoint_ms: 120,
            cohort: vec![obligation, obligation],
        }),
        Err(OperationsError::Conflict(
            "mission evaluation plan duplicate obligation"
        ))
    ));
}

#[test]
fn plan_publication_checks_current_mission_cohort_scope_and_empty_cohort()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_two_obligations(0x300)?;
    let second_team = fixture.add_team()?;
    let other_mission = mission_id(0x30c);
    let other_mission_created = fixture
        .catalog_mut()
        .create_mission(&create_mission_command(
            workspace,
            second_team,
            uuid(0x30d),
            other_mission,
            "other Mission scope",
            11,
        ))?;
    assert_eq!(other_mission_created.mission().mission_id(), other_mission);

    let before_late_publish = fixture.counts()?;
    let late_fresh_request = plan_publish_command(PlanFixtureInput {
        request_id: uuid(0x30a),
        plan_id: plan_id(0x30b),
        version_id: plan_version_id(0x30c),
        workspace_id: workspace,
        mission_id: seeded.mission,
        expected_head_revision: 0,
        t0_commit_at_ms: 300,
        t1_submission_cutoff_ms: 200,
        t2_checkpoint_ms: 250,
        cohort: vec![seeded.first],
    })?;
    assert!(matches!(
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&late_fresh_request),
        Err(OperationsError::Conflict(
            "mission evaluation plan timeline"
        ))
    ));
    assert_eq!(fixture.counts()?, before_late_publish);

    let before_wrong_scope = fixture.counts()?;
    let wrong_scope = plan_publish_command(PlanFixtureInput {
        request_id: uuid(0x30e),
        plan_id: plan_id(0x30f),
        version_id: plan_version_id(0x310),
        workspace_id: workspace,
        mission_id: other_mission,
        expected_head_revision: 0,
        t0_commit_at_ms: 100,
        t1_submission_cutoff_ms: 110,
        t2_checkpoint_ms: 120,
        cohort: vec![seeded.first],
    })?;
    assert!(matches!(
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&wrong_scope),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(fixture.counts()?, before_wrong_scope);

    let empty_plan = plan_publish_command(PlanFixtureInput {
        request_id: uuid(0x311),
        plan_id: plan_id(0x312),
        version_id: plan_version_id(0x313),
        workspace_id: workspace,
        mission_id: other_mission,
        expected_head_revision: 0,
        t0_commit_at_ms: 100,
        t1_submission_cutoff_ms: 110,
        t2_checkpoint_ms: 120,
        cohort: Vec::new(),
    })?;
    let empty = fixture
        .catalog_mut()
        .publish_mission_evaluation_plan(&empty_plan)?;
    assert!(empty.version().cohort().is_empty());
    Ok(())
}

#[test]
fn plan_version_replay_checks_domain_content_and_ignores_worker_t0() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_two_obligations(0x400)?;
    let plan = plan_id(0x414);
    let version_id = plan_version_id(0x415);
    let command = plan_publish_command(PlanFixtureInput {
        request_id: uuid(0x416),
        plan_id: plan,
        version_id,
        workspace_id: workspace,
        mission_id: seeded.mission,
        expected_head_revision: 0,
        t0_commit_at_ms: 100,
        t1_submission_cutoff_ms: 200,
        t2_checkpoint_ms: 250,
        cohort: vec![seeded.second, seeded.first],
    })?;
    let published = fixture
        .catalog_mut()
        .publish_mission_evaluation_plan(&command)?;
    assert!(!published.replayed());
    assert_eq!(published.version().revision(), 1);
    assert_eq!(
        published.version().cohort()[0].obligation_id(),
        seeded.first
    );
    assert_eq!(published.version().cohort()[0].weight().get(), 4);
    assert_eq!(
        published.version().cohort()[1].obligation_id(),
        seeded.second
    );
    assert_eq!(published.version().cohort()[1].weight().get(), 5);
    assert!(matches!(
        fixture
            .catalog()
            .mission_evaluation_request(workspace, uuid(0x416))?
            .ok_or_else(|| std::io::Error::other("plan request receipt missing"))?
            .mutation(),
        MissionEvaluationRecordedMutation::PlanPublished(version)
            if version.version_id() == version_id
    ));
    assert!(
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&command)?
            .replayed()
    );

    let refreshed_worker_time = plan_publish_command(PlanFixtureInput {
        request_id: uuid(0x416),
        plan_id: plan,
        version_id,
        workspace_id: workspace,
        mission_id: seeded.mission,
        expected_head_revision: 0,
        t0_commit_at_ms: 300,
        t1_submission_cutoff_ms: 200,
        t2_checkpoint_ms: 250,
        cohort: vec![seeded.first, seeded.second],
    })?;
    let replay = fixture
        .catalog_mut()
        .publish_mission_evaluation_plan(&refreshed_worker_time)?;
    assert!(replay.replayed());
    assert_eq!(replay.version().t0_commit_at_ms(), 100);

    let changed_cutoff = plan_publish_command(PlanFixtureInput {
        request_id: uuid(0x416),
        plan_id: plan,
        version_id,
        workspace_id: workspace,
        mission_id: seeded.mission,
        expected_head_revision: 0,
        t0_commit_at_ms: 100,
        t1_submission_cutoff_ms: 200,
        t2_checkpoint_ms: 260,
        cohort: vec![seeded.first, seeded.second],
    })?;
    assert!(matches!(
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&changed_cutoff),
        Err(OperationsError::Conflict(_))
    ));
    Ok(())
}

#[test]
fn plan_revision_history_preserves_prior_versions_and_weight_snapshots()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_two_obligations(0x420)?;
    let plan = plan_id(0x42a);
    let first = fixture
        .catalog_mut()
        .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
            request_id: uuid(0x42b),
            plan_id: plan,
            version_id: plan_version_id(0x42c),
            workspace_id: workspace,
            mission_id: seeded.mission,
            expected_head_revision: 0,
            t0_commit_at_ms: 100,
            t1_submission_cutoff_ms: 200,
            t2_checkpoint_ms: 250,
            cohort: vec![seeded.first, seeded.second],
        })?)?;
    assert_eq!(first.version().revision(), 1);
    let second = fixture
        .catalog_mut()
        .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
            request_id: uuid(0x42d),
            plan_id: plan,
            version_id: plan_version_id(0x42e),
            workspace_id: workspace,
            mission_id: seeded.mission,
            expected_head_revision: 1,
            t0_commit_at_ms: 110,
            t1_submission_cutoff_ms: 220,
            t2_checkpoint_ms: 270,
            cohort: vec![seeded.first, seeded.second],
        })?)?;
    assert_eq!(second.version().revision(), 2);
    assert_eq!(
        fixture
            .catalog()
            .mission_evaluation_plan_version(workspace, plan, 1)?
            .ok_or_else(|| std::io::Error::other("plan version 1 missing"))?
            .t1_submission_cutoff_ms(),
        200
    );
    assert_eq!(
        fixture
            .catalog()
            .mission_evaluation_plan_version(workspace, plan, 2)?
            .ok_or_else(|| std::io::Error::other("plan version 2 missing"))?
            .t1_submission_cutoff_ms(),
        220
    );
    fixture.reopen()?;
    assert_eq!(
        fixture
            .catalog()
            .mission_evaluation_plan_version(workspace, plan, 1)?
            .ok_or_else(|| std::io::Error::other("reopened plan version missing"))?
            .cohort()[1]
            .weight()
            .get(),
        5
    );
    Ok(())
}

#[test]
fn plan_start_replays_exact_revision_and_freezes_its_head() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_obligation(0x401)?;
    let plan = plan_id(0x406);
    let publish_command = plan_publish_command(PlanFixtureInput {
        request_id: uuid(0x407),
        plan_id: plan,
        version_id: plan_version_id(0x408),
        workspace_id: workspace,
        mission_id: seeded.mission,
        expected_head_revision: 0,
        t0_commit_at_ms: 100,
        t1_submission_cutoff_ms: 200,
        t2_checkpoint_ms: 250,
        cohort: vec![seeded.obligation],
    })?;
    let published = fixture
        .catalog_mut()
        .publish_mission_evaluation_plan(&publish_command)?;
    assert_eq!(published.version().revision(), 1);

    let start_command = MissionEvaluationPlanStartCommand::new(
        uuid(0x409),
        workspace,
        seeded.mission,
        plan,
        1,
        operator_authority(),
    )?;
    let started = fixture
        .catalog_mut()
        .start_mission_evaluation_plan(&start_command, || Ok(150))?;
    assert!(!started.replayed());
    assert_eq!(started.start().revision(), 1);
    assert!(matches!(
        fixture
            .catalog()
            .mission_evaluation_request(workspace, uuid(0x409))?
            .ok_or_else(|| std::io::Error::other("plan start request receipt missing"))?
            .mutation(),
        MissionEvaluationRecordedMutation::PlanStarted(record)
            if record.plan_id() == plan
    ));
    let replay_clock_calls = Cell::new(0);
    assert!(
        fixture
            .catalog_mut()
            .start_mission_evaluation_plan(&start_command, || {
                replay_clock_calls.set(replay_clock_calls.get() + 1);
                Err(OperationsError::Conflict("replay sampled start clock"))
            })?
            .replayed()
    );

    let replay = fixture
        .catalog_mut()
        .start_mission_evaluation_plan(&start_command, || {
            replay_clock_calls.set(replay_clock_calls.get() + 1);
            Err(OperationsError::Conflict("replay sampled start clock"))
        })?;
    assert!(replay.replayed());
    assert_eq!(replay.start().started_at_ms(), 150);
    assert_eq!(replay_clock_calls.get(), 0);

    let before_conflict = fixture.counts()?;
    assert!(matches!(
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0x40a),
                plan_id: plan,
                version_id: plan_version_id(0x40b),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 1,
                t0_commit_at_ms: 110,
                t1_submission_cutoff_ms: 220,
                t2_checkpoint_ms: 270,
                cohort: vec![seeded.obligation],
            })?),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(fixture.counts()?, before_conflict);
    Ok(())
}

#[test]
fn plan_start_samples_once_inside_the_inclusive_t0_exclusive_t1_window()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_obligation(0x450)?;

    for (request, plan, version, started_at_ms, accepted) in [
        (0xA001, 0xA101, 0xA201, 99, false),
        (0xA002, 0xA102, 0xA202, 100, true),
        (0xA003, 0xA103, 0xA203, 199, true),
        (0xA004, 0xA104, 0xA204, 200, false),
        (0xA005, 0xA105, 0xA205, 201, false),
    ] {
        let plan_id = plan_id(plan);
        let published =
            fixture
                .catalog_mut()
                .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                    request_id: uuid(request),
                    plan_id,
                    version_id: plan_version_id(version),
                    workspace_id: workspace,
                    mission_id: seeded.mission,
                    expected_head_revision: 0,
                    t0_commit_at_ms: 100,
                    t1_submission_cutoff_ms: 200,
                    t2_checkpoint_ms: 250,
                    cohort: vec![seeded.obligation],
                })?)?;
        assert_eq!(published.version().revision(), 1);

        let command = MissionEvaluationPlanStartCommand::new(
            uuid(request + 0x100),
            workspace,
            seeded.mission,
            plan_id,
            1,
            operator_authority(),
        )?;
        let before = fixture.counts()?;
        let clock_calls = Cell::new(0);
        let result = fixture
            .catalog_mut()
            .start_mission_evaluation_plan(&command, || {
                clock_calls.set(clock_calls.get() + 1);
                Ok(started_at_ms)
            });
        assert_eq!(
            clock_calls.get(),
            1,
            "sample one trusted time per fresh start"
        );

        if accepted {
            let started = result?;
            assert!(!started.replayed());
            assert_eq!(started.start().started_at_ms(), started_at_ms);
        } else {
            assert!(matches!(
                result,
                Err(OperationsError::Conflict(
                    "mission evaluation plan start outside T0/T1 window"
                ))
            ));
            assert_eq!(
                fixture.counts()?,
                before,
                "rejected start must leave no rows"
            );
        }
    }

    Ok(())
}

#[test]
fn plan_start_clock_failure_does_not_create_a_request_or_start() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_obligation(0x490)?;
    let plan = plan_id(0xA106);
    let published =
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0xA006),
                plan_id: plan,
                version_id: plan_version_id(0xA206),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 0,
                t0_commit_at_ms: 100,
                t1_submission_cutoff_ms: 200,
                t2_checkpoint_ms: 250,
                cohort: vec![seeded.obligation],
            })?)?;
    assert_eq!(published.version().revision(), 1);

    let command = MissionEvaluationPlanStartCommand::new(
        uuid(0xA007),
        workspace,
        seeded.mission,
        plan,
        1,
        operator_authority(),
    )?;
    let before = fixture.counts()?;
    let clock_calls = Cell::new(0);
    assert!(matches!(
        fixture
            .catalog_mut()
            .start_mission_evaluation_plan(&command, || {
                clock_calls.set(clock_calls.get() + 1);
                Err(OperationsError::Conflict("test clock unavailable"))
            }),
        Err(OperationsError::Conflict("test clock unavailable"))
    ));
    assert_eq!(clock_calls.get(), 1);
    assert_eq!(fixture.counts()?, before);
    Ok(())
}

#[test]
fn late_legacy_plan_start_replay_returns_its_original_receipt_without_clock()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_obligation(0x470)?;
    let plan = plan_id(0xA107);
    let published =
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0xA008),
                plan_id: plan,
                version_id: plan_version_id(0xA207),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 0,
                t0_commit_at_ms: 100,
                t1_submission_cutoff_ms: 200,
                t2_checkpoint_ms: 250,
                cohort: vec![seeded.obligation],
            })?)?;
    assert_eq!(published.version().revision(), 1);

    let request_id = uuid(0xA009);
    fixture.catalog_mut().connection.execute(
        "INSERT INTO ops_mission_evaluation_requests(request_id,operation,workspace_id,target_id,revision,actor_kind,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,expected_leadership_revision) VALUES(?1,3,?2,?3,1,0,'operator','Local operator',NULL,NULL,NULL,NULL,NULL)",
        params![
            request_id.as_bytes(),
            workspace.into_uuid().as_bytes(),
            plan.into_uuid().as_bytes(),
        ],
    )?;
    fixture.catalog_mut().connection.execute(
        "INSERT INTO ops_mission_evaluation_plan_starts(plan_id,workspace_id,mission_id,revision,request_id,started_at_ms) VALUES(?1,?2,?3,1,?4,250)",
        params![
            plan.into_uuid().as_bytes(),
            workspace.into_uuid().as_bytes(),
            seeded.mission.into_uuid().as_bytes(),
            request_id.as_bytes(),
        ],
    )?;

    let command = MissionEvaluationPlanStartCommand::new(
        request_id,
        workspace,
        seeded.mission,
        plan,
        1,
        operator_authority(),
    )?;
    let clock_calls = Cell::new(0);
    let replay = fixture
        .catalog_mut()
        .start_mission_evaluation_plan(&command, || {
            clock_calls.set(clock_calls.get() + 1);
            Err(OperationsError::Conflict("legacy replay sampled clock"))
        })?;
    assert!(replay.replayed());
    assert_eq!(replay.start().started_at_ms(), 250);
    assert_eq!(clock_calls.get(), 0);
    Ok(())
}

#[test]
fn plan_start_waiting_on_another_connection_samples_after_t1_and_rejects()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_obligation(0x480)?;
    let plan = plan_id(0xA108);
    let t0_ms = 100;
    let t1_ms = 200;
    let published =
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0xA00A),
                plan_id: plan,
                version_id: plan_version_id(0xA208),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 0,
                t0_commit_at_ms: t0_ms,
                t1_submission_cutoff_ms: t1_ms,
                t2_checkpoint_ms: t1_ms + 1_000,
                cohort: vec![seeded.obligation],
            })?)?;
    assert_eq!(published.version().revision(), 1);
    let command = MissionEvaluationPlanStartCommand::new(
        uuid(0xA00B),
        workspace,
        seeded.mission,
        plan,
        1,
        operator_authority(),
    )?;
    let before = fixture.counts()?;

    let mut waiting_catalog = OperationsCatalog::open(&fixture.path, fixture.destination)?;
    waiting_catalog
        .connection
        .busy_handler(Some(observe_plan_start_test_busy))?;

    let mut blocking_connection = rusqlite::Connection::open(&fixture.path)?;
    let blocking_transaction =
        blocking_connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let clock_ms = Arc::new(AtomicI64::new(150));
    assert!(clock_ms.load(Ordering::Acquire) < t1_ms);

    // The second connection attempts PlanStart while the first owns SQLite's
    // writer reservation. Advance the injected server clock to T1 while it is
    // blocked, then release the reservation. This tests lock-wait inclusion;
    // it does not establish that a commit whose sample precedes T1 completes
    // before T1.
    PLAN_START_TEST_BUSY_OBSERVED.store(0, Ordering::Release);
    let sampled_clock = Arc::clone(&clock_ms);
    let worker = thread::spawn(move || {
        waiting_catalog
            .start_mission_evaluation_plan(&command, || Ok(sampled_clock.load(Ordering::Acquire)))
    });

    while PLAN_START_TEST_BUSY_OBSERVED.load(Ordering::Acquire) == 0 {
        thread::yield_now();
    }
    assert_eq!(clock_ms.load(Ordering::Acquire), 150);
    clock_ms.store(t1_ms, Ordering::Release);
    drop(blocking_transaction);

    let result = worker
        .join()
        .map_err(|_| std::io::Error::other("plan start lock waiter panicked"))?;
    assert_eq!(PLAN_START_TEST_BUSY_OBSERVED.load(Ordering::Acquire), 1);
    assert!(matches!(
        result,
        Err(OperationsError::Conflict(
            "mission evaluation plan start outside T0/T1 window"
        ))
    ));
    assert_eq!(
        fixture.counts()?,
        before,
        "late lock acquisition must not start"
    );
    Ok(())
}

#[test]
fn started_plan_receipt_survives_catalog_reopen() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_obligation(0x420)?;
    let plan = plan_id(0x426);
    let published =
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0x427),
                plan_id: plan,
                version_id: plan_version_id(0x428),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 0,
                t0_commit_at_ms: 100,
                t1_submission_cutoff_ms: 200,
                t2_checkpoint_ms: 250,
                cohort: vec![seeded.obligation],
            })?)?;
    assert_eq!(published.version().revision(), 1);
    let started = fixture.catalog_mut().start_mission_evaluation_plan(
        &MissionEvaluationPlanStartCommand::new(
            uuid(0x429),
            workspace,
            seeded.mission,
            plan,
            1,
            operator_authority(),
        )?,
        || Ok(150),
    )?;
    assert_eq!(started.start().revision(), 1);
    fixture.reopen()?;
    let start = fixture
        .catalog()
        .mission_evaluation_plan_start(workspace, plan)?
        .ok_or_else(|| std::io::Error::other("reopened plan start missing"))?;
    assert_eq!(start.request_id(), uuid(0x429));
    assert_eq!(start.revision(), 1);
    assert_eq!(start.started_at_ms(), 150);
    Ok(())
}

#[test]
fn a_started_plan_does_not_block_a_distinct_plan_id() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let seeded = fixture.seed_mission_with_obligation(0x430)?;
    let first_plan = plan_id(0x43a);
    let first_published =
        fixture
            .catalog_mut()
            .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
                request_id: uuid(0x43b),
                plan_id: first_plan,
                version_id: plan_version_id(0x43c),
                workspace_id: workspace,
                mission_id: seeded.mission,
                expected_head_revision: 0,
                t0_commit_at_ms: 100,
                t1_submission_cutoff_ms: 200,
                t2_checkpoint_ms: 250,
                cohort: vec![seeded.obligation],
            })?)?;
    assert_eq!(first_published.version().revision(), 1);
    let start = MissionEvaluationPlanStartCommand::new(
        uuid(0x43d),
        workspace,
        seeded.mission,
        first_plan,
        1,
        operator_authority(),
    )?;
    let first_started = fixture
        .catalog_mut()
        .start_mission_evaluation_plan(&start, || Ok(150))?;
    assert_eq!(first_started.start().revision(), 1);

    let second_plan = plan_id(0x43e);
    let second = fixture
        .catalog_mut()
        .publish_mission_evaluation_plan(&plan_publish_command(PlanFixtureInput {
            request_id: uuid(0x43f),
            plan_id: second_plan,
            version_id: plan_version_id(0x440),
            workspace_id: workspace,
            mission_id: seeded.mission,
            expected_head_revision: 0,
            t0_commit_at_ms: 160,
            t1_submission_cutoff_ms: 230,
            t2_checkpoint_ms: 270,
            cohort: vec![seeded.obligation],
        })?)?;
    assert_eq!(second.version().revision(), 1);
    assert_ne!(second.version().plan_id(), first_plan);
    Ok(())
}

#[test]
fn participant_mutation_requires_the_exact_current_workspace_leader() -> Result<(), Box<dyn Error>>
{
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let team = fixture.team;
    let (actor, participant_id) = fixture.insert_actor_binding(workspace)?;
    assert!(MissionEvaluationAuthority::new(actor, None).is_err());
    assert!(MissionEvaluationAuthority::new(OperationalActor::LocalOperator, Some(1)).is_err());

    let denied = MissionCreateCommand::new(
        uuid(0x500),
        mission_id(0x503),
        workspace,
        team,
        "must be current Workspace leader".to_owned(),
        10,
        MissionEvaluationAuthority::new(actor, Some(1))?,
    )?;
    let before = fixture.counts()?;
    assert!(matches!(
        fixture.catalog_mut().create_mission(&denied),
        Err(OperationsError::Conflict(
            "mission evaluation requires current Workspace leader"
        ))
    ));
    assert_eq!(fixture.counts()?, before);

    fixture.set_current_workspace_leader(workspace, participant_id)?;
    let valid = MissionCreateCommand::new(
        uuid(0x501),
        mission_id(0x502),
        workspace,
        team,
        "current leader creates Mission".to_owned(),
        10,
        MissionEvaluationAuthority::new(actor, Some(1))?,
    )?;
    let created = fixture.catalog_mut().create_mission(&valid)?;
    assert_eq!(created.mission().mission_id(), mission_id(0x502));

    let (_, replacement_participant) = fixture.insert_actor_binding(workspace)?;
    fixture.replace_current_workspace_leader(workspace, replacement_participant, 2)?;
    let stale = MissionCreateCommand::new(
        uuid(0x504),
        mission_id(0x505),
        workspace,
        team,
        "stale leader cannot mutate".to_owned(),
        11,
        MissionEvaluationAuthority::new(actor, Some(2))?,
    )?;
    assert!(matches!(
        fixture.catalog_mut().create_mission(&stale),
        Err(OperationsError::Conflict(
            "mission evaluation Workspace leadership changed"
        ))
    ));
    Ok(())
}

#[test]
fn actor_bound_receipts_survive_role_change_and_session_rebind() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let workspace = fixture.workspace;
    let team = fixture.team;
    let (original_actor, original_participant) = fixture.insert_actor_binding(workspace)?;
    fixture.set_current_workspace_leader(workspace, original_participant)?;
    let command = MissionCreateCommand::new(
        uuid(0x501),
        mission_id(0x502),
        workspace,
        team,
        "Mission request owned by the original actor".to_owned(),
        10,
        MissionEvaluationAuthority::new(original_actor, Some(1))?,
    )?;
    let created = fixture.catalog_mut().create_mission(&command)?;
    assert_eq!(created.mission().mission_id(), mission_id(0x502));

    let (replacement_actor, replacement_participant) = fixture.insert_actor_binding(workspace)?;
    fixture.replace_current_workspace_leader(workspace, replacement_participant, 2)?;
    let before_receipts = fixture.counts()?;
    let original_receipt = fixture
        .catalog()
        .mission_evaluation_request_for_actor(workspace, uuid(0x501), original_actor)?
        .ok_or_else(|| std::io::Error::other("original actor receipt missing"))?;
    assert!(matches!(
        original_receipt.mutation(),
        MissionEvaluationRecordedMutation::MissionCreated(record)
            if record.mission_id() == mission_id(0x502)
    ));
    let rebound_actor = fixture.rebind_actor_session_for_test(workspace, original_actor)?;
    let rebound_receipt = fixture
        .catalog()
        .mission_evaluation_request_for_actor(workspace, uuid(0x501), rebound_actor)?
        .ok_or_else(|| std::io::Error::other("rebound actor receipt missing"))?;
    assert!(matches!(
        rebound_receipt.mutation(),
        MissionEvaluationRecordedMutation::MissionCreated(record)
            if record.mission_id() == mission_id(0x502)
    ));
    assert!(matches!(
        fixture.catalog().mission_evaluation_request_for_actor(
            workspace,
            uuid(0x501),
            original_actor,
        ),
        Err(OperationsError::Conflict(
            "operational actor is not a Ready Workspace participant"
        ))
    ));
    assert!(matches!(
        fixture.catalog().mission_evaluation_request_for_actor(
            workspace,
            uuid(0x501),
            replacement_actor,
        ),
        Err(OperationsError::Conflict(
            "mission evaluation request actor scope"
        ))
    ));
    assert_eq!(fixture.counts()?, before_receipts);

    let rebound_command = MissionCreateCommand::new(
        uuid(0x501),
        mission_id(0x502),
        workspace,
        team,
        "Mission request owned by the original actor".to_owned(),
        10,
        MissionEvaluationAuthority::new(rebound_actor, Some(1))?,
    )?;
    assert!(
        fixture
            .catalog_mut()
            .create_mission(&rebound_command)?
            .replayed()
    );
    let cross_actor_replay = MissionCreateCommand::new(
        uuid(0x501),
        mission_id(0x502),
        workspace,
        team,
        "same payload but different actor".to_owned(),
        99,
        MissionEvaluationAuthority::new(replacement_actor, Some(2))?,
    )?;
    assert!(matches!(
        fixture.catalog_mut().create_mission(&cross_actor_replay),
        Err(OperationsError::Conflict(_))
    ));
    Ok(())
}
