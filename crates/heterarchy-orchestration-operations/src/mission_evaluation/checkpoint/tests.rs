use std::{
    error::Error,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    MissionCreateCommand, MissionEvaluationPlanPublishCommand, MissionEvaluationPlanStartCommand,
    MissionEvaluationPlanVersionId,
};
use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
use rusqlite::params;
use uuid::Uuid;

use super::*;

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);

fn alternate_source_command(
    source: &MissionT2DeferCommand,
    authority: MissionEvaluationAuthority,
    reason: Option<String>,
) -> Result<MissionT2DeferCommand, OperationsError> {
    let original = source.content();
    let content = MissionT2DeferContent::new(
        original.request_id(),
        original.checkpoint_id(),
        original.workspace_id(),
        original.mission_id(),
        original.plan_id(),
        original.revision(),
        original.expected_previous_checkpoint_id(),
        reason.unwrap_or_else(|| original.reason().to_owned()),
        original.next_check_condition().to_owned(),
        original.next_check_at_ms(),
    )?;
    MissionT2DeferCommand::new(content, authority)
}

struct Fixture {
    catalog: OperationsCatalog,
    _files: TempDatabaseFiles,
    destination: Uuid,
    workspace: WorkspaceId,
    mission: MissionId,
    plan: MissionEvaluationPlanId,
    authority: MissionEvaluationAuthority,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let serial = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mission-t2-checkpoint-{}-{serial}-{}.sqlite",
            std::process::id(),
            Uuid::now_v7()
        ));
        let destination = Uuid::now_v7();
        let workspace = WorkspaceId::from_uuid(Uuid::now_v7());
        let team_id = Uuid::now_v7();
        let actor = OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(Uuid::now_v7()),
            agent_id: AgentInstanceId::from_uuid(Uuid::now_v7()),
            session_id: AgentSessionId::from_uuid(Uuid::now_v7()),
        };
        let authority = MissionEvaluationAuthority::new(actor, Some(1))?;
        let profile_id = Uuid::now_v7();
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let participant_id = Uuid::now_v7();
        let files = TempDatabaseFiles { path: path.clone() };
        let catalog = OperationsCatalog::open(&path, destination)?;
        let mut fixture = Self {
            catalog,
            _files: files,
            destination,
            workspace,
            mission: MissionId::from_uuid(Uuid::now_v7())?,
            plan: MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?,
            authority,
        };
        fixture.seed_workspace_and_actor(
            team_id,
            profile_id,
            participation_id,
            participant_id,
            actor,
        )?;
        fixture
            .catalog
            .connection
            .execute_batch("PRAGMA foreign_keys=ON")?;
        let schema_exists = fixture.catalog.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ops_mission_evaluation_t2_deferred_checkpoints')",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if !schema_exists {
            super::schema::create_schema(&fixture.catalog.connection)?;
        }
        fixture.create_started_plan(team_id)?;
        Ok(fixture)
    }

    fn seed_workspace_and_actor(
        &mut self,
        team_id: Uuid,
        profile_id: Uuid,
        participation_id: WorkspaceParticipationId,
        participant_id: Uuid,
        actor: OperationalActor,
    ) -> Result<(), Box<dyn Error>> {
        let migration_id = Uuid::now_v7();
        self.catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                migration_id.as_bytes(),
                self.workspace.into_uuid().as_bytes(),
                self.destination.as_bytes(),
                [0x51_u8; 32].as_slice(),
            ],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'evaluation','checkpoint','ACTIVE',1,'operator','Local operator',1,1,1)",
            params![team_id.as_bytes(), self.workspace.into_uuid().as_bytes()],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                self.workspace.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        let OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } = actor
        else {
            return Err("fixture requires a participant actor".into());
        };
        self.catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Workspace Orchestrator','worker',1,1,1)",
            params![
                self.workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![
                Uuid::now_v7().as_bytes(),
                self.workspace.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                Uuid::now_v7().as_bytes(),
                session_id.into_uuid().as_bytes(),
                participant_id.as_bytes(),
            ],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
            params![
                self.workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                activity_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                Uuid::now_v7().as_bytes(),
            ],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,1,NULL,NULL,1)",
            params![self.workspace.into_uuid().as_bytes(), participant_id.as_bytes()],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),1,'CONFIRMED',?2,1,1)",
            params![self.workspace.into_uuid().as_bytes(), participant_id.as_bytes()],
        )?;
        Ok(())
    }

    fn create_started_plan(&mut self, team_id: Uuid) -> Result<(), Box<dyn Error>> {
        let created = self.catalog.create_mission(&MissionCreateCommand::new(
            Uuid::now_v7(),
            self.mission,
            self.workspace,
            team_id,
            "T2 checkpoint fixture".to_owned(),
            10,
            self.authority,
        )?)?;
        assert_eq!(created.mission().mission_id(), self.mission);
        let published = self.catalog.publish_mission_evaluation_plan(
            &MissionEvaluationPlanPublishCommand::new(
                Uuid::now_v7(),
                self.plan,
                MissionEvaluationPlanVersionId::from_uuid(Uuid::now_v7())?,
                self.workspace,
                self.mission,
                0,
                20,
                30,
                40,
                Vec::new(),
                self.authority,
            )?,
        )?;
        assert_eq!(published.version().plan_id(), self.plan);
        let started = self.catalog.start_mission_evaluation_plan(
            &MissionEvaluationPlanStartCommand::new(
                Uuid::now_v7(),
                self.workspace,
                self.mission,
                self.plan,
                1,
                self.authority,
            )?,
            || Ok(25),
        )?;
        assert_eq!(started.start().revision(), 1);
        Ok(())
    }

    fn command(
        &self,
        request_id: Uuid,
        checkpoint_id: Uuid,
        previous: Option<MissionT2CheckpointId>,
        next_check_at_ms: i64,
    ) -> Result<MissionT2DeferCommand, OperationsError> {
        self.command_with_authority(
            request_id,
            checkpoint_id,
            previous,
            next_check_at_ms,
            self.authority,
        )
    }

    fn command_with_authority(
        &self,
        request_id: Uuid,
        checkpoint_id: Uuid,
        previous: Option<MissionT2CheckpointId>,
        next_check_at_ms: i64,
        authority: MissionEvaluationAuthority,
    ) -> Result<MissionT2DeferCommand, OperationsError> {
        let content = MissionT2DeferContent::new(
            request_id,
            MissionT2CheckpointId::from_uuid(checkpoint_id)?,
            self.workspace,
            self.mission,
            self.plan,
            1,
            previous,
            "independent review is pending".to_owned(),
            "read the exact submitted artifact review receipt".to_owned(),
            next_check_at_ms,
        )?;
        MissionT2DeferCommand::new(content, authority)
    }

    fn rebind_actor_session(
        &mut self,
        actor: OperationalActor,
    ) -> Result<OperationalActor, Box<dyn Error>> {
        let OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } = actor
        else {
            return Err("fixture requires a participant actor".into());
        };
        let rebound_session = AgentSessionId::from_uuid(Uuid::now_v7());
        let participant_id: Vec<u8> = self.catalog.connection.query_row(
            "SELECT substr(participant_id,1,17) FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3 AND session_id=?4",
            params![
                self.workspace.into_uuid().as_bytes(),
                activity_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        let participant_id = Uuid::from_bytes(crate::wire::id(participant_id)?);
        let member_updated = self.catalog.connection.execute(
            "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3 AND session_id=?4",
            params![
                rebound_session.into_uuid().as_bytes(),
                self.workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
        )?;
        let binding_updated = self.catalog.connection.execute(
            "UPDATE ops_current_effect_bindings SET session_id=?1 WHERE workspace_id=?2 AND activity_id=?3 AND agent_id=?4 AND session_id=?5",
            params![
                rebound_session.into_uuid().as_bytes(),
                self.workspace.into_uuid().as_bytes(),
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

    fn replace_current_workspace_leader(
        &mut self,
        revision: i64,
    ) -> Result<MissionEvaluationAuthority, Box<dyn Error>> {
        let profile_id = Uuid::now_v7();
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let participant_id = Uuid::now_v7();
        let activity_id = ActivityId::from_uuid(Uuid::now_v7());
        let agent_id = AgentInstanceId::from_uuid(Uuid::now_v7());
        let session_id = AgentSessionId::from_uuid(Uuid::now_v7());
        self.catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                self.workspace.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Successor Workspace Orchestrator','worker',1,1,1)",
            params![
                self.workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![
                Uuid::now_v7().as_bytes(),
                self.workspace.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                Uuid::now_v7().as_bytes(),
                session_id.into_uuid().as_bytes(),
                participant_id.as_bytes(),
            ],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
            params![
                self.workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                activity_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                Uuid::now_v7().as_bytes(),
            ],
        )?;
        let updated = self.catalog.connection.execute(
            "UPDATE ops_leadership SET leader_participant_id=?1,leadership_revision=?2,updated_at_ms=?2 WHERE workspace_id=?3 AND scope_kind=0 AND activity_id IS NULL AND team_id IS NULL",
            params![
                participant_id.as_bytes(),
                revision,
                self.workspace.into_uuid().as_bytes(),
            ],
        )?;
        assert_eq!(updated, 1);
        self.catalog.connection.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),?2,'CONFIRMED',?3,?2,?2)",
            params![
                self.workspace.into_uuid().as_bytes(),
                revision,
                participant_id.as_bytes(),
            ],
        )?;
        Ok(MissionEvaluationAuthority::new(
            OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id,
            },
            Some(revision),
        )?)
    }

    fn insert_profile_binding(
        &mut self,
        workspace: WorkspaceId,
        profile_id: Uuid,
        participation_id: WorkspaceParticipationId,
        participant_id: Uuid,
        actor: OperationalActor,
        display_name: &str,
    ) -> Result<(), Box<dyn Error>> {
        let OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } = actor
        else {
            return Err("fixture requires a participant actor".into());
        };
        let profile_exists: bool = self.catalog.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2 AND participation_id=?3)",
            params![
                workspace.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        if !profile_exists {
            self.catalog.connection.execute(
                "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
                params![
                    workspace.into_uuid().as_bytes(),
                    profile_id.as_bytes(),
                    participation_id.into_uuid().as_bytes(),
                ],
            )?;
        }
        self.catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
                display_name,
            ],
        )?;
        self.catalog.connection.execute(
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
        self.catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                activity_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                Uuid::now_v7().as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn create_foreign_workspace_leader(
        &mut self,
    ) -> Result<(WorkspaceId, MissionEvaluationAuthority), Box<dyn Error>> {
        let workspace = WorkspaceId::from_uuid(Uuid::now_v7());
        self.catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                Uuid::now_v7().as_bytes(),
                workspace.into_uuid().as_bytes(),
                self.destination.as_bytes(),
                [0x52_u8; 32].as_slice(),
            ],
        )?;
        let actor = OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(Uuid::now_v7()),
            agent_id: AgentInstanceId::from_uuid(Uuid::now_v7()),
            session_id: AgentSessionId::from_uuid(Uuid::now_v7()),
        };
        let (activity_id, agent_id, session_id) = match actor {
            OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id,
            } => (activity_id, agent_id, session_id),
            OperationalActor::LocalOperator => return Err("unexpected operator".into()),
        };
        let participant_id = Uuid::now_v7();
        self.insert_profile_binding(
            workspace,
            Uuid::now_v7(),
            WorkspaceParticipationId::from_uuid(Uuid::now_v7())?,
            participant_id,
            actor,
            "Foreign Workspace Orchestrator",
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,1,NULL,NULL,1)",
            params![workspace.into_uuid().as_bytes(), participant_id.as_bytes()],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),1,'CONFIRMED',?2,1,1)",
            params![workspace.into_uuid().as_bytes(), participant_id.as_bytes()],
        )?;
        Ok((
            workspace,
            MissionEvaluationAuthority::new(
                OperationalActor::Participant {
                    activity_id,
                    agent_id,
                    session_id,
                },
                Some(1),
            )?,
        ))
    }
}

struct TempDatabaseFiles {
    path: PathBuf,
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

#[test]
fn defer_replay_returns_original_clock_and_rejects_changed_intent() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let command = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, i64::MAX)?;
    let first = fixture
        .catalog
        .defer_mission_t2_checkpoint(&command, || Ok(40))?;
    assert!(!first.replayed());
    assert_eq!(first.checkpoint().recorded_at_ms(), 40);
    let replay = fixture.catalog.defer_mission_t2_checkpoint(&command, || {
        Err(OperationsError::Conflict("replay sampled a new clock"))
    })?;
    assert!(replay.replayed());
    assert_eq!(replay.checkpoint(), first.checkpoint());

    let changed_intent = alternate_source_command(
        &command,
        command.authority,
        Some("different review reason".to_owned()),
    )?;
    assert!(matches!(
        fixture
            .catalog
            .defer_mission_t2_checkpoint(&changed_intent, || {
                Err(OperationsError::Conflict("replay sampled a new clock"))
            }),
        Err(OperationsError::Conflict(
            "mission T2 defer request replay mismatch"
        ))
    ));
    Ok(())
}

#[test]
fn request_readback_follows_profile_re_registration_but_rejects_another_profile()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let command = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, 100)?;
    let first = fixture
        .catalog
        .defer_mission_t2_checkpoint(&command, || Ok(40))?;
    let owner = first.checkpoint().accountable_owner();
    let rebound_actor = OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(Uuid::now_v7()),
        agent_id: AgentInstanceId::from_uuid(Uuid::now_v7()),
        session_id: AgentSessionId::from_uuid(Uuid::now_v7()),
    };
    fixture.insert_profile_binding(
        fixture.workspace,
        owner.profile_id().ok_or("Profile-backed actor missing")?,
        owner
            .participation_id()
            .ok_or("Profile-backed participation missing")?,
        Uuid::now_v7(),
        rebound_actor,
        "same Profile after participant re-registration",
    )?;
    let rebound_authority = MissionEvaluationAuthority::new(rebound_actor, Some(1))?;
    assert_eq!(
        fixture.catalog.mission_t2_deferred_checkpoint_for_request(
            fixture.workspace,
            command.content().request_id(),
            rebound_authority,
        )?,
        Some(first.checkpoint().clone())
    );
    let rebound_command = alternate_source_command(&command, rebound_authority, None)?;
    assert!(matches!(
        fixture
            .catalog
            .defer_mission_t2_checkpoint(&rebound_command, || {
                Err(OperationsError::Conflict("replay sampled a new clock"))
            }),
        Err(OperationsError::Conflict(
            "mission T2 defer request replay mismatch"
        ))
    ));

    let other_actor = OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(Uuid::now_v7()),
        agent_id: AgentInstanceId::from_uuid(Uuid::now_v7()),
        session_id: AgentSessionId::from_uuid(Uuid::now_v7()),
    };
    fixture.insert_profile_binding(
        fixture.workspace,
        Uuid::now_v7(),
        WorkspaceParticipationId::from_uuid(Uuid::now_v7())?,
        Uuid::now_v7(),
        other_actor,
        "unrelated Profile",
    )?;
    let unrelated_authority = MissionEvaluationAuthority::new(other_actor, Some(1))?;
    let unrelated_command = alternate_source_command(&command, unrelated_authority, None)?;
    assert!(matches!(
        fixture
            .catalog
            .defer_mission_t2_checkpoint(&unrelated_command, || {
                Err(OperationsError::Conflict(
                    "unrelated actor sampled the clock",
                ))
            }),
        Err(OperationsError::Conflict(
            "mission T2 defer request replay mismatch"
        ))
    ));
    assert!(matches!(
        fixture.catalog.mission_t2_deferred_checkpoint_for_request(
            fixture.workspace,
            command.content().request_id(),
            unrelated_authority,
        ),
        Err(OperationsError::Conflict(
            "mission evaluation Workspace leadership changed"
        ))
    ));
    Ok(())
}

#[test]
fn request_readback_treats_foreign_and_absent_ids_the_same() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let command = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, 100)?;
    let recorded = fixture
        .catalog
        .defer_mission_t2_checkpoint(&command, || Ok(40))?;
    assert!(!recorded.replayed());
    let (foreign_workspace, foreign_leader) = fixture.create_foreign_workspace_leader()?;
    let foreign_id_read = fixture.catalog.mission_t2_deferred_checkpoint_for_request(
        foreign_workspace,
        command.content().request_id(),
        foreign_leader,
    )?;
    let absent_id_read = fixture.catalog.mission_t2_deferred_checkpoint_for_request(
        foreign_workspace,
        Uuid::now_v7(),
        foreign_leader,
    )?;
    assert_eq!(foreign_id_read, None);
    assert_eq!(absent_id_read, None);
    Ok(())
}

#[test]
fn request_readback_survives_session_rebind_and_current_orchestrator_change()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let command = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, 100)?;
    let first = fixture
        .catalog
        .defer_mission_t2_checkpoint(&command, || Ok(40))?;
    let original_actor = command.authority.actor;
    let rebound_actor = fixture.rebind_actor_session(original_actor)?;
    let rebound_authority = MissionEvaluationAuthority::new(rebound_actor, Some(1))?;
    let rebound_command = alternate_source_command(&command, rebound_authority, None)?;
    let replay = fixture
        .catalog
        .defer_mission_t2_checkpoint(&rebound_command, || {
            Err(OperationsError::Conflict("replay sampled a new clock"))
        })?;
    assert!(replay.replayed());
    assert_eq!(replay.checkpoint(), first.checkpoint());
    assert_eq!(
        fixture.catalog.mission_t2_deferred_checkpoint_for_request(
            fixture.workspace,
            command.content().request_id(),
            rebound_authority,
        )?,
        Some(first.checkpoint().clone())
    );
    let original_session = match original_actor {
        OperationalActor::Participant { session_id, .. } => session_id,
        OperationalActor::LocalOperator => return Err("unexpected operator".into()),
    };
    assert_eq!(
        first.checkpoint().accountable_owner().session_id(),
        original_session
    );
    let current_orchestrator = fixture.replace_current_workspace_leader(2)?;
    assert_eq!(
        fixture.catalog.mission_t2_deferred_checkpoint_for_request(
            fixture.workspace,
            command.content().request_id(),
            current_orchestrator,
        )?,
        Some(first.checkpoint().clone())
    );
    Ok(())
}

#[test]
fn repeated_deferrals_append_and_keep_the_published_plan_unchanged() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let first_command = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, i64::MAX)?;
    let first = fixture
        .catalog
        .defer_mission_t2_checkpoint(&first_command, || Ok(40))?;
    // This explicit follow-up check happens before the prior check time. The
    // prior record remains intact and the new reason/time are recorded.
    let second_content = MissionT2DeferContent::new(
        Uuid::now_v7(),
        MissionT2CheckpointId::from_uuid(Uuid::now_v7())?,
        fixture.workspace,
        fixture.mission,
        fixture.plan,
        1,
        Some(first.checkpoint().checkpoint_id()),
        "review remains pending".to_owned(),
        "query the independent review receipt again".to_owned(),
        80,
    )?;
    let second_command = MissionT2DeferCommand::new(second_content, fixture.authority)?;
    let second = fixture
        .catalog
        .defer_mission_t2_checkpoint(&second_command, || Ok(41))?;
    assert_eq!(first.checkpoint().sequence(), 0);
    assert_eq!(second.checkpoint().sequence(), 1);
    assert_eq!(
        second.checkpoint().previous_checkpoint_id(),
        Some(first.checkpoint().checkpoint_id())
    );
    assert_eq!(
        fixture.catalog.latest_mission_t2_deferred_checkpoint(
            fixture.workspace,
            fixture.plan,
            1
        )?,
        Some(second.checkpoint().clone())
    );
    let count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE plan_id=?1 AND revision=1",
        params![fixture.plan.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(count, 2);
    let unchanged_plan = fixture
        .catalog
        .mission_evaluation_plan_version(fixture.workspace, fixture.plan, 1)?
        .ok_or("frozen plan missing")?;
    assert_eq!(unchanged_plan.t2_checkpoint_ms(), 40);
    Ok(())
}

#[test]
fn defer_rejects_early_time_stale_revision_predecessor_and_leadership() -> Result<(), Box<dyn Error>>
{
    let mut fixture = Fixture::new()?;
    let early_command = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, 50)?;
    assert!(matches!(
        fixture
            .catalog
            .defer_mission_t2_checkpoint(&early_command, || Ok(39)),
        Err(OperationsError::Conflict(
            "mission T2 defer before checkpoint"
        ))
    ));
    let stale_revision_content = MissionT2DeferContent::new(
        Uuid::now_v7(),
        MissionT2CheckpointId::from_uuid(Uuid::now_v7())?,
        fixture.workspace,
        fixture.mission,
        fixture.plan,
        2,
        None,
        "review pending".to_owned(),
        "query review receipt".to_owned(),
        50,
    )?;
    let stale_revision = MissionT2DeferCommand::new(stale_revision_content, fixture.authority)?;
    assert!(matches!(
        fixture
            .catalog
            .defer_mission_t2_checkpoint(&stale_revision, || Ok(40)),
        Err(OperationsError::Conflict(
            "mission evidence frozen plan scope"
        ))
    ));
    let first_command = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, 50)?;
    let first = fixture
        .catalog
        .defer_mission_t2_checkpoint(&first_command, || Ok(40))?;
    let stale_predecessor = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, 51)?;
    assert!(matches!(
        fixture
            .catalog
            .defer_mission_t2_checkpoint(&stale_predecessor, || Ok(41)),
        Err(OperationsError::Conflict(
            "mission T2 defer predecessor changed"
        ))
    ));
    let stale_authority = MissionEvaluationAuthority::new(fixture.authority.actor, Some(2))?;
    let stale_leader = fixture.command_with_authority(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Some(first.checkpoint().checkpoint_id()),
        60,
        stale_authority,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .defer_mission_t2_checkpoint(&stale_leader, || {
                Err(OperationsError::Conflict(
                    "clock sampled before leadership check",
                ))
            }),
        Err(OperationsError::Conflict(
            "mission evaluation Workspace leadership changed"
        ))
    ));
    Ok(())
}

#[test]
fn defer_requires_a_fresh_operations_clock_and_future_next_check() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let command = fixture.command(Uuid::now_v7(), Uuid::now_v7(), None, 60)?;
    assert!(matches!(
        fixture.catalog.defer_mission_t2_checkpoint(&command, || {
            Err(OperationsError::Conflict("test clock unavailable"))
        }),
        Err(OperationsError::Conflict("test clock unavailable"))
    ));
    let count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_mission_evaluation_t2_deferred_checkpoints",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(count, 0);
    let accepted = fixture
        .catalog
        .defer_mission_t2_checkpoint(&command, || Ok(42))?;
    let regressed = fixture.command(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Some(accepted.checkpoint().checkpoint_id()),
        50,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .defer_mission_t2_checkpoint(&regressed, || Ok(41)),
        Err(OperationsError::Conflict("mission T2 defer time regressed"))
    ));
    let due = fixture.command(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Some(accepted.checkpoint().checkpoint_id()),
        42,
    )?;
    assert!(matches!(
        fixture.catalog.defer_mission_t2_checkpoint(&due, || Ok(42)),
        Err(OperationsError::Conflict(
            "mission T2 next check time is not in the future"
        ))
    ));
    Ok(())
}
