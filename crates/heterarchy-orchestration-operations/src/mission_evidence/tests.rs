use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use forge_domain::{
    ActivityId, AgentAssignmentId, AgentAssignmentRevision, AgentInstanceId, AgentSessionId,
    EvaluationAdmissionTiming, EvaluationCutId, EvaluationDigest, EvaluationManifestId,
    EvaluationManifestMember, EvaluationOriginalAssignmentBinding, EvaluationOriginalBindingStatus,
    EvaluationPlanBinding, EvaluationPlanId, EvaluationRegisteredManifest,
    EvaluationRegisteredManifestMember, EvaluationReviewCut, EvaluationReviewCutId,
    EvaluationReviewCutMember, EvaluationReviewState, EvaluationRevision, EvaluationSourceIdentity,
    EvaluationSourceSequence, EvaluationT1Coverage, EvaluationT1CoverageCut,
    EvaluationT1CoverageMember, ForgeBackendId, OrchestrationIssuerId, TimestampMillis,
    WorkspaceId,
};
use rusqlite::{params, types::Value};
use uuid::Uuid;

use crate::{
    ConfigureAdmissionIssuerCommand, ImportHandoffSealCommand, MIN_ADMISSION_TTL_MS,
    MissionCreateCommand, MissionEvaluationAuthority, MissionEvaluationPlanId,
    MissionEvaluationPlanObligationInput, MissionEvaluationPlanPublishCommand,
    MissionEvaluationPlanStartCommand, MissionEvaluationPlanVersionId, MissionId,
    MissionObligationId, MissionObligationRecordCommand, MissionWorkUnitId, OperationalActor,
    OperationsCatalog, PositiveWeight, WorkspaceParticipationId,
};

use super::*;

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
struct TestObligation {
    id: MissionObligationId,
    work_unit_id: MissionWorkUnitId,
    participation_id: WorkspaceParticipationId,
    weight: u64,
    due_at_ms: i64,
}

#[derive(Clone, Copy)]
struct ManifestSpec {
    plan_id: MissionEvaluationPlanId,
    plan_digest: EvaluationDigest,
    deadline_ms: i64,
    delegation_digest: [u8; 32],
    manifest_seed: u128,
    target_override: Option<(AgentInstanceId, AgentSessionId)>,
    criteria_override: Option<EvaluationDigest>,
    activity_override: Option<ActivityId>,
    assignment_override: Option<AgentAssignmentId>,
}

impl ManifestSpec {
    fn new(
        plan_id: MissionEvaluationPlanId,
        plan_digest: EvaluationDigest,
        deadline_ms: i64,
        delegation_digest: [u8; 32],
        manifest_seed: u128,
    ) -> Self {
        Self {
            plan_id,
            plan_digest,
            deadline_ms,
            delegation_digest,
            manifest_seed,
            target_override: None,
            criteria_override: None,
            activity_override: None,
            assignment_override: None,
        }
    }

    fn with_target(mut self, value: Option<(AgentInstanceId, AgentSessionId)>) -> Self {
        self.target_override = value;
        self
    }

    fn with_criteria(mut self, value: Option<EvaluationDigest>) -> Self {
        self.criteria_override = value;
        self
    }

    fn with_activity(mut self, value: Option<ActivityId>) -> Self {
        self.activity_override = value;
        self
    }

    fn with_assignment(mut self, value: Option<AgentAssignmentId>) -> Self {
        self.assignment_override = value;
        self
    }
}

struct TestDatabaseFiles {
    path: PathBuf,
}

impl Drop for TestDatabaseFiles {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = self.path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let _ = fs::remove_file(PathBuf::from(sidecar));
        }
    }
}

struct Fixture {
    catalog: OperationsCatalog,
    _files: TestDatabaseFiles,
    path: PathBuf,
    destination: Uuid,
    workspace: WorkspaceId,
    mission_id: MissionId,
    plan_id: MissionEvaluationPlanId,
    migration_id: Uuid,
    source_digest: [u8; 32],
    backend_id: ForgeBackendId,
    participation_id: WorkspaceParticipationId,
    obligations: [TestObligation; 2],
    digests: Option<MissionEvidencePlanDigests>,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let serial = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mission-evidence-{}-{serial}-{}.sqlite",
            std::process::id(),
            Uuid::now_v7()
        ));
        let destination = Uuid::now_v7();
        let workspace = WorkspaceId::from_uuid(Uuid::now_v7());
        let mission_id = MissionId::from_uuid(Uuid::now_v7())?;
        let plan_id = MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?;
        let migration_id = Uuid::now_v7();
        let source_digest = [0x51; 32];
        let backend_id = ForgeBackendId::from_uuid(Uuid::now_v7());
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let profile_id = Uuid::now_v7();
        let team_id = Uuid::now_v7();
        let files = TestDatabaseFiles { path: path.clone() };
        let catalog = OperationsCatalog::open(&path, destination)?;
        let mut fixture = Self {
            catalog,
            _files: files,
            path,
            destination,
            workspace,
            mission_id,
            plan_id,
            migration_id,
            source_digest,
            backend_id,
            participation_id,
            obligations: [
                TestObligation {
                    id: MissionObligationId::from_uuid(Uuid::now_v7())?,
                    work_unit_id: MissionWorkUnitId::from_uuid(Uuid::now_v7())?,
                    participation_id,
                    weight: 3,
                    due_at_ms: 180,
                },
                TestObligation {
                    id: MissionObligationId::from_uuid(Uuid::now_v7())?,
                    work_unit_id: MissionWorkUnitId::from_uuid(Uuid::now_v7())?,
                    participation_id,
                    weight: 5,
                    due_at_ms: 240,
                },
            ],
            digests: None,
        };
        fixture.seed_workspace_and_team(profile_id, team_id)?;
        fixture.seed_mission_and_plan(team_id)?;
        fixture.refresh_plan_digests()?;
        Ok(fixture)
    }

    fn authority() -> Result<MissionEvaluationAuthority, crate::OperationsError> {
        MissionEvaluationAuthority::new(OperationalActor::LocalOperator, None)
    }

    fn seed_workspace_and_team(
        &mut self,
        profile_id: Uuid,
        team_id: Uuid,
    ) -> Result<(), Box<dyn Error>> {
        let issuer = self
            .catalog
            .ensure_admission_issuer(ConfigureAdmissionIssuerCommand {
                issuer: OrchestrationIssuerId::from_uuid(Uuid::now_v7()),
                backend_id: self.backend_id,
                admission_ttl_ms: MIN_ADMISSION_TTL_MS,
                now_ms: 1,
            })?;
        assert_eq!(issuer.backend_id(), self.backend_id);
        self.catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                self.migration_id.as_bytes(),
                self.workspace.into_uuid().as_bytes(),
                self.destination.as_bytes(),
                self.source_digest.as_slice(),
            ],
        )?;
        for ordinal in 0..5_i64 {
            let cursor = [u8::try_from(ordinal + 1)?];
            self.catalog.connection.execute(
                "INSERT INTO ops_import_pages(migration_id,ordinal,cursor,next_cursor,table_kind,record_count,digest) VALUES(?1,?2,?3,NULL,?2,0,?4)",
                params![
                    self.migration_id.as_bytes(),
                    ordinal,
                    cursor.as_slice(),
                    self.source_digest.as_slice(),
                ],
            )?;
        }
        let seal = self
            .catalog
            .seal_import_handoff(&ImportHandoffSealCommand {
                migration_id: self.migration_id,
                workspace_id: self.workspace.into_uuid(),
                destination_id: self.destination,
                expected_source_revision: 1,
                expected_verified_digest: self.source_digest,
                sealed_at_ms: 2,
            })?;
        assert_eq!(seal.backend_id(), self.backend_id);
        self.catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'evidence-test','mission evidence','ACTIVE',1,'operator','Local operator',1,1,1)",
            params![team_id.as_bytes(), self.workspace.into_uuid().as_bytes()],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                self.workspace.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                self.participation_id.into_uuid().as_bytes(),
            ],
        )?;
        self.catalog.connection.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,1,NULL,'member')",
            params![
                self.workspace.into_uuid().as_bytes(),
                team_id.as_bytes(),
                self.participation_id.into_uuid().as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn seed_mission_and_plan(&mut self, team_id: Uuid) -> Result<(), Box<dyn Error>> {
        let authority = Self::authority()?;
        let created = self.catalog.create_mission(&MissionCreateCommand::new(
            Uuid::now_v7(),
            self.mission_id,
            self.workspace,
            team_id,
            "bounded evidence journal fixture".to_owned(),
            10,
            authority,
        )?)?;
        assert_eq!(created.mission().mission_id(), self.mission_id);
        let criteria = ["first frozen criterion", "second frozen criterion"];
        for (index, obligation) in self.obligations.iter().enumerate() {
            let recorded =
                self.catalog
                    .record_mission_obligation(&MissionObligationRecordCommand::new(
                        Uuid::now_v7(),
                        obligation.id,
                        self.workspace,
                        self.mission_id,
                        obligation.participation_id,
                        obligation.work_unit_id,
                        PositiveWeight::new(obligation.weight)?,
                        criteria[index].to_owned(),
                        obligation.due_at_ms,
                        20 + i64::try_from(index)?,
                        authority,
                    )?)?;
            assert_eq!(recorded.obligation().obligation_id(), obligation.id);
        }
        let published = self.catalog.publish_mission_evaluation_plan(
            &MissionEvaluationPlanPublishCommand::new(
                Uuid::now_v7(),
                self.plan_id,
                MissionEvaluationPlanVersionId::from_uuid(Uuid::now_v7())?,
                self.workspace,
                self.mission_id,
                0,
                100,
                200,
                260,
                self.obligations
                    .iter()
                    .map(|obligation| MissionEvaluationPlanObligationInput::new(obligation.id))
                    .collect(),
                authority,
            )?,
        )?;
        assert_eq!(published.version().revision(), 1);
        let started = self.catalog.start_mission_evaluation_plan(
            &MissionEvaluationPlanStartCommand::new(
                Uuid::now_v7(),
                self.workspace,
                self.mission_id,
                self.plan_id,
                1,
                authority,
            )?,
            || Ok(150),
        )?;
        assert_eq!(started.start().revision(), 1);
        Ok(())
    }

    fn refresh_plan_digests(&mut self) -> Result<(), Box<dyn Error>> {
        self.digests = Some(self.catalog.mission_evidence_plan_digests(
            self.workspace,
            self.plan_id,
            1,
        )?);
        Ok(())
    }

    fn digests(&self) -> Result<&MissionEvidencePlanDigests, crate::OperationsError> {
        self.digests
            .as_ref()
            .ok_or(crate::OperationsError::Corrupt("test plan digests missing"))
    }

    fn source_identity(
        &self,
        activity_id: ActivityId,
        delegation_digest: [u8; 32],
    ) -> Result<EvaluationSourceIdentity, Box<dyn Error>> {
        Ok(EvaluationSourceIdentity::new(
            self.backend_id,
            self.workspace,
            activity_id,
            self.migration_id,
            self.destination,
            EvaluationRevision::new(1)?,
            EvaluationDigest::new(delegation_digest)?,
        )?)
    }

    fn manifest(
        &self,
        index: usize,
        spec: ManifestSpec,
    ) -> Result<EvaluationRegisteredManifest, Box<dyn Error>> {
        let obligation = self.obligations[index];
        let activity_id = spec
            .activity_override
            .unwrap_or_else(|| ActivityId::from_uuid(Uuid::from_u128(0xA100 + index as u128)));
        let assignment_id = spec.assignment_override.unwrap_or_else(|| {
            AgentAssignmentId::from_uuid(Uuid::from_u128(0xA200 + index as u128))
        });
        let (target_agent_id, target_session_id) = spec.target_override.unwrap_or((
            AgentInstanceId::from_uuid(Uuid::from_u128(0xA300 + index as u128)),
            AgentSessionId::from_uuid(Uuid::from_u128(0xA400 + index as u128)),
        ));
        let criteria_digest = self
            .digests()?
            .member_criteria_digests()
            .iter()
            .find(|value| value.obligation_id() == obligation.id)
            .ok_or_else(|| std::io::Error::other("criteria digest missing"))?
            .digest();
        let declaration = EvaluationManifestMember::new(
            forge_domain::EvaluationWorkUnitId::new(obligation.work_unit_id.into_uuid())?,
            forge_domain::EvaluationObligationId::new(obligation.id.into_uuid())?,
            assignment_id,
            AgentAssignmentRevision::new(1)?,
            target_agent_id,
            target_session_id,
            spec.criteria_override.unwrap_or(criteria_digest),
        )?;
        let source = self.source_identity(activity_id, spec.delegation_digest)?;
        let plan = EvaluationPlanBinding::new(
            EvaluationPlanId::new(spec.plan_id.into_uuid())?,
            EvaluationRevision::new(1)?,
            spec.plan_digest,
        );
        let proposal = forge_domain::EvaluationManifestProposal::new(
            EvaluationManifestId::new(Uuid::from_u128(spec.manifest_seed + index as u128))?,
            EvaluationRevision::new(1)?,
            source,
            plan,
            TimestampMillis::new(spec.deadline_ms)?,
            vec![declaration],
        )?;
        Ok(EvaluationRegisteredManifest::from_source_records(
            proposal,
            vec![EvaluationRegisteredManifestMember::new(
                declaration,
                EvaluationOriginalBindingStatus::UnknownHistorical,
            )],
            EvaluationSourceSequence::new(2)?,
            EvaluationAdmissionTiming::Trusted(TimestampMillis::new(150)?),
        )?)
    }

    fn manifest_bundle(
        &self,
        criteria_override: Option<(usize, EvaluationDigest)>,
        target_override: Option<(usize, AgentInstanceId, AgentSessionId)>,
        delegation_digest: [u8; 32],
    ) -> Result<MissionEvidenceManifestBundle, Box<dyn Error>> {
        let manifests = (0..self.obligations.len())
            .map(|index| {
                let spec = ManifestSpec::new(
                    self.plan_id,
                    self.digests()?.plan_digest(),
                    200,
                    delegation_digest,
                    0xA500,
                )
                .with_target(
                    target_override
                        .filter(|(target_index, _, _)| *target_index == index)
                        .map(|(_, agent, session)| (agent, session)),
                )
                .with_criteria(
                    criteria_override
                        .filter(|(criteria_index, _)| *criteria_index == index)
                        .map(|(_, digest)| digest),
                );
                self.manifest(index, spec)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MissionEvidenceManifestBundle::new(manifests)?)
    }

    fn cut_bundle(
        binding: &MissionEvidenceManifestBindingRecord,
        review_high_waters: [i64; 2],
        review_seed: u128,
    ) -> Result<MissionEvidenceObservationBundle, Box<dyn Error>> {
        Self::cut_bundle_with_previous(binding, None, review_high_waters, review_seed)
    }

    fn cut_bundle_with_previous(
        binding: &MissionEvidenceManifestBindingRecord,
        previous: Option<&MissionEvidenceObservationRecord>,
        review_high_waters: [i64; 2],
        review_seed: u128,
    ) -> Result<MissionEvidenceObservationBundle, Box<dyn Error>> {
        let partitions = binding
            .manifest_bundle()
            .manifests()
            .iter()
            .enumerate()
            .map(|(index, manifest)| {
                let activity = manifest.proposal().source().activity_id();
                if let Some(previous) = previous
                    && let Some(prior) = previous
                        .cuts()
                        .partitions()
                        .iter()
                        .find(|partition| partition.activity_id() == activity)
                    && prior.review_cut().source_high_water().get() == review_high_waters[index]
                {
                    return Ok(prior.clone());
                }
                let declaration = *manifest
                    .proposal()
                    .members()
                    .first()
                    .ok_or_else(|| std::io::Error::other("manifest member missing"))?;
                let coverage = EvaluationT1Coverage::NoEligibleSubmission;
                let t1 = EvaluationT1CoverageCut::from_source_records(
                    EvaluationCutId::new(Uuid::from_u128(activity.into_uuid().as_u128() ^ 0xB500))?,
                    manifest.proposal(),
                    manifest.proposal().deadline(),
                    TimestampMillis::new(201)?,
                    EvaluationSourceSequence::new(3)?,
                    vec![EvaluationT1CoverageMember::new(declaration, coverage)],
                )?;
                let review = EvaluationReviewCut::from_source_records(
                    EvaluationReviewCutId::new(Uuid::from_u128(review_seed + index as u128))?,
                    &t1,
                    EvaluationSourceSequence::new(review_high_waters[index])?,
                    vec![EvaluationReviewCutMember::new(
                        declaration,
                        coverage,
                        None,
                        EvaluationReviewState::NotApplicable,
                    )],
                )?;
                Ok(MissionEvidenceObservationPartition::new(
                    activity, t1, review,
                )?)
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        Ok(MissionEvidenceObservationBundle::new(partitions)?)
    }

    fn bind_command(
        &self,
        request_id: Uuid,
        bundle: MissionEvidenceManifestBundle,
    ) -> Result<MissionEvidenceManifestBindingCommand, crate::OperationsError> {
        MissionEvidenceManifestBindingCommand::new(
            request_id,
            self.workspace,
            self.plan_id,
            1,
            bundle,
            160,
            Self::authority()?,
        )
    }

    fn publish_and_start_plan(
        &mut self,
        plan_id: MissionEvaluationPlanId,
        t0_ms: i64,
        t1_ms: i64,
        t2_ms: i64,
        started_at_ms: i64,
    ) -> Result<MissionEvidencePlanDigests, Box<dyn Error>> {
        let authority = Self::authority()?;
        let published = self.catalog.publish_mission_evaluation_plan(
            &MissionEvaluationPlanPublishCommand::new(
                Uuid::now_v7(),
                plan_id,
                MissionEvaluationPlanVersionId::from_uuid(Uuid::now_v7())?,
                self.workspace,
                self.mission_id,
                0,
                t0_ms,
                t1_ms,
                t2_ms,
                self.obligations
                    .iter()
                    .map(|obligation| MissionEvaluationPlanObligationInput::new(obligation.id))
                    .collect(),
                authority,
            )?,
        )?;
        assert_eq!(published.version().revision(), 1);
        let started = self.catalog.start_mission_evaluation_plan(
            &MissionEvaluationPlanStartCommand::new(
                Uuid::now_v7(),
                self.workspace,
                self.mission_id,
                plan_id,
                1,
                authority,
            )?,
            || Ok(started_at_ms),
        )?;
        assert_eq!(started.start().revision(), 1);
        Ok(self
            .catalog
            .mission_evidence_plan_digests(self.workspace, plan_id, 1)?)
    }

    fn reopen(&mut self) -> Result<(), Box<dyn Error>> {
        self.close_catalog()?;
        self.catalog = OperationsCatalog::open(&self.path, self.destination)?;
        Ok(())
    }

    fn close_catalog(&mut self) -> Result<(), Box<dyn Error>> {
        let placeholder = rusqlite::Connection::open_in_memory()?;
        let connection = std::mem::replace(&mut self.catalog.connection, placeholder);
        drop(connection);
        Ok(())
    }
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn set_original_binding_status(
    bundle: &MissionEvidenceManifestBundle,
    obligation_id: MissionObligationId,
    status: EvaluationOriginalBindingStatus,
) -> Result<MissionEvidenceManifestBundle, Box<dyn Error>> {
    // Structural source projection only; this fixture is not an owner receipt.
    let mut replaced = false;
    let manifests = bundle
        .manifests()
        .iter()
        .map(|manifest| {
            let members = manifest
                .members()
                .iter()
                .map(|member| {
                    if member.declaration().obligation_id().into_uuid() == obligation_id.into_uuid()
                    {
                        replaced = true;
                        EvaluationRegisteredManifestMember::new(member.declaration(), status)
                    } else {
                        *member
                    }
                })
                .collect();
            EvaluationRegisteredManifest::from_source_records(
                manifest.proposal().clone(),
                members,
                manifest.source_sequence(),
                manifest.seal_timing(),
            )
            .map_err(|error| -> Box<dyn Error> { Box::new(error) })
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if !replaced {
        return Err(std::io::Error::other("binding fixture member missing").into());
    }
    Ok(MissionEvidenceManifestBundle::new(manifests)?)
}

fn reject_invalid_manifest_bindings(
    fixture: &mut Fixture,
    valid_bundle: &MissionEvidenceManifestBundle,
) -> Result<(), Box<dyn Error>> {
    let incomplete = MissionEvidenceManifestBundle::new(vec![valid_bundle.manifests()[0].clone()])?;
    let incomplete_command = fixture.bind_command(Uuid::now_v7(), incomplete)?;
    assert!(matches!(
        fixture
            .catalog
            .bind_mission_evidence_manifest(&incomplete_command),
        Err(crate::OperationsError::Conflict(_))
    ));

    let wrong_criteria = fixture.manifest_bundle(
        Some((1, EvaluationDigest::new([0x91; 32])?)),
        None,
        fixture.source_digest,
    )?;
    let wrong_criteria_command = fixture.bind_command(Uuid::now_v7(), wrong_criteria)?;
    assert!(matches!(
        fixture
            .catalog
            .bind_mission_evidence_manifest(&wrong_criteria_command),
        Err(crate::OperationsError::Conflict(
            "mission evidence manifest local terms"
        ))
    ));

    let mismatched_source = MissionEvidenceManifestBundle::new(vec![
        fixture.manifest(
            0,
            ManifestSpec::new(
                fixture.plan_id,
                fixture.digests()?.plan_digest(),
                200,
                [0x92; 32],
                0xA800,
            ),
        )?,
        fixture.manifest(
            1,
            ManifestSpec::new(
                fixture.plan_id,
                fixture.digests()?.plan_digest(),
                200,
                [0x93; 32],
                0xA800,
            ),
        )?,
    ]);
    assert!(matches!(
        mismatched_source,
        Err(crate::OperationsError::Conflict(
            "mission evidence manifest partition scope"
        ))
    ));
    assert_eq!(binding_count(fixture)?, 0);
    Ok(())
}

fn binding_count(fixture: &Fixture) -> Result<i64, Box<dyn Error>> {
    Ok(fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_mission_evidence_bindings",
        [],
        |row| row.get::<_, i64>(0),
    )?)
}

fn bind_pending_manifest(
    fixture: &mut Fixture,
    bundle: MissionEvidenceManifestBundle,
) -> Result<MissionEvidenceManifestBindingRecord, Box<dyn Error>> {
    let command = fixture.bind_command(Uuid::now_v7(), bundle)?;
    let binding = fixture
        .catalog
        .bind_mission_evidence_manifest(&command)?
        .binding()
        .clone();
    assert_eq!(binding.state(), MissionEvidenceBindingState::Pending);
    assert_eq!(binding.manifest_bundle().manifests().len(), 2);
    assert!(binding.members().iter().all(|member| matches!(
        member.target_identity(),
        MissionEvidenceIdentityResolution::Pending {
            reason: MissionEvidenceIdentityPendingReason::NoRegistrationHistory,
            ..
        }
    )));
    Ok(binding)
}

fn record_first_pending_observation(
    fixture: &mut Fixture,
    binding: &MissionEvidenceManifestBindingRecord,
) -> Result<
    (
        MissionEvidenceObservationCommand,
        MissionEvidenceObservationRecord,
    ),
    Box<dyn Error>,
> {
    let command = MissionEvidenceObservationCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        fixture.plan_id,
        1,
        0,
        Fixture::cut_bundle(binding, [5, 6], 0xB600)?,
        210,
        Fixture::authority()?,
    )?;
    let observation = fixture
        .catalog
        .record_mission_evidence_observation(&command)?
        .observation()
        .clone();
    assert_eq!(observation.revision(), 1);
    assert!(observation.members().iter().all(|member| matches!(
        member.disposition(),
        MissionEvidenceDisposition::Unresolved(MissionEvidenceUnresolvedReason::BindingPending)
    )));
    assert!(observation.members().iter().all(|member| matches!(
        member.target_identity(),
        MissionEvidenceIdentityResolution::Pending {
            reason: MissionEvidenceIdentityPendingReason::NoRegistrationHistory,
            ..
        }
    )));
    assert!(observation.cuts().partitions().iter().all(|partition| {
        partition.review_cut().members().iter().all(|member| {
            member.coverage() == EvaluationT1Coverage::NoEligibleSubmission
                && member.review() == EvaluationReviewState::NotApplicable
        })
    }));
    assert_eq!(observation.measures().len(), 1);
    assert_eq!(observation.measures()[0].planned_due_weight(), 3);
    assert_eq!(observation.measures()[0].accepted_original_due_weight(), 0);
    assert_eq!(observation.measures()[0].unresolved_due_weight(), 3);
    assert_eq!(observation.measures()[0].future_weight(), 5);
    Ok((command, observation))
}

struct AdvancedObservations {
    second: MissionEvidenceObservationRecord,
    partially_advanced: MissionEvidenceObservationRecord,
}

fn record_advanced_pending_observations(
    fixture: &mut Fixture,
    binding: &MissionEvidenceManifestBindingRecord,
    first_command: &MissionEvidenceObservationCommand,
    first: &MissionEvidenceObservationRecord,
) -> Result<AdvancedObservations, Box<dyn Error>> {
    let changed_replay = MissionEvidenceObservationCommand::new(
        first_command.request_id(),
        fixture.workspace,
        fixture.plan_id,
        1,
        0,
        Fixture::cut_bundle(binding, [6, 5], 0xB610)?,
        211,
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .record_mission_evidence_observation(&changed_replay),
        Err(crate::OperationsError::Conflict(
            "mission evidence request replay"
        ))
    ));

    let second_command = MissionEvidenceObservationCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        fixture.plan_id,
        1,
        1,
        Fixture::cut_bundle(binding, [6, 7], 0xB620)?,
        212,
        Fixture::authority()?,
    )?;
    let second = fixture
        .catalog
        .record_mission_evidence_observation(&second_command)?
        .observation()
        .clone();
    assert_eq!(second.revision(), 2);
    assert_eq!(second.measures(), first.measures());

    let advance = MissionEvidenceObservationCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        fixture.plan_id,
        1,
        2,
        Fixture::cut_bundle_with_previous(binding, Some(&second), [7, 7], 0xB625)?,
        212,
        Fixture::authority()?,
    )?;
    let partially_advanced = fixture
        .catalog
        .record_mission_evidence_observation(&advance)?
        .observation()
        .clone();
    assert_eq!(partially_advanced.revision(), 3);
    assert_eq!(partially_advanced.measures(), second.measures());
    let unchanged_partition = second
        .cuts()
        .partitions()
        .iter()
        .find(|partition| partition.review_cut().source_high_water().get() == 7)
        .ok_or_else(|| std::io::Error::other("unchanged cut partition missing"))?;
    assert!(
        partially_advanced
            .cuts()
            .partitions()
            .contains(unchanged_partition)
    );
    Ok(AdvancedObservations {
        second,
        partially_advanced,
    })
}

fn assert_observation_cas_replay(
    fixture: &mut Fixture,
    binding: &MissionEvidenceManifestBindingRecord,
    first_command: &MissionEvidenceObservationCommand,
    first: &MissionEvidenceObservationRecord,
    second: &MissionEvidenceObservationRecord,
) -> Result<(), Box<dyn Error>> {
    let regressing_partition = MissionEvidenceObservationCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        fixture.plan_id,
        1,
        3,
        Fixture::cut_bundle(binding, [8, 6], 0xB630)?,
        213,
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .record_mission_evidence_observation(&regressing_partition),
        Err(crate::OperationsError::Conflict(
            "mission evidence source partition regression"
        ))
    ));
    let stale_cas = MissionEvidenceObservationCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        fixture.plan_id,
        1,
        1,
        Fixture::cut_bundle(binding, [7, 8], 0xB640)?,
        214,
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .record_mission_evidence_observation(&stale_cas),
        Err(crate::OperationsError::Conflict(
            "mission evidence observation head revision"
        ))
    ));
    let replay = fixture
        .catalog
        .record_mission_evidence_observation(first_command)?;
    assert!(replay.replayed());
    assert_eq!(replay.observation(), first);
    assert_eq!(
        fixture
            .catalog
            .mission_evidence_observation(fixture.workspace, second.request_id())?,
        Some(second.clone())
    );
    assert!(matches!(
        fixture.catalog.mission_evidence_request_for_actor(
            fixture.workspace,
            second.request_id(),
            OperationalActor::LocalOperator,
        )?,
        Some(MissionEvidenceMutationRecord::Observation(record)) if record == *second
    ));
    assert_eq!(
        fixture.catalog.connection.query_row(
            "SELECT revision FROM ops_mission_evidence_heads WHERE binding_request_id=?1",
            params![binding.request_id().as_bytes()],
            |row| row.get::<_, i64>(0),
        )?,
        3
    );
    Ok(())
}

#[test]
fn cross_activity_observations_preserve_pending_weight_and_replay_across_restart()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let valid_bundle = fixture.manifest_bundle(None, None, fixture.source_digest)?;
    reject_invalid_manifest_bindings(&mut fixture, &valid_bundle)?;
    let binding = bind_pending_manifest(&mut fixture, valid_bundle)?;
    let (first_command, first) = record_first_pending_observation(&mut fixture, &binding)?;
    let advanced =
        record_advanced_pending_observations(&mut fixture, &binding, &first_command, &first)?;
    assert_observation_cas_replay(
        &mut fixture,
        &binding,
        &first_command,
        &first,
        &advanced.second,
    )?;

    fixture.reopen()?;
    assert_eq!(
        fixture
            .catalog
            .mission_evidence_binding(fixture.workspace, fixture.plan_id, 1)?,
        Some(binding)
    );
    assert_eq!(
        fixture
            .catalog
            .mission_evidence_observation(fixture.workspace, advanced.second.request_id())?,
        Some(advanced.second)
    );
    assert_eq!(
        fixture.catalog.mission_evidence_observation(
            fixture.workspace,
            advanced.partially_advanced.request_id(),
        )?,
        Some(advanced.partially_advanced)
    );
    Ok(())
}

fn manifest_preparation_command(
    fixture: &Fixture,
) -> Result<MissionEvidenceManifestPreparationCommand, Box<dyn Error>> {
    let partitions = fixture
        .obligations
        .iter()
        .enumerate()
        .map(|(index, obligation)| {
            let member = MissionEvidenceManifestCandidateMember::new(
                obligation.id,
                AgentAssignmentId::from_uuid(Uuid::from_u128(0xAD20 + index as u128)),
                AgentAssignmentRevision::new(1)?,
                AgentInstanceId::from_uuid(Uuid::from_u128(0xAD30 + index as u128)),
                AgentSessionId::from_uuid(Uuid::from_u128(0xAD40 + index as u128)),
            )?;
            Ok(MissionEvidenceManifestCandidatePartition::new(
                ActivityId::from_uuid(Uuid::from_u128(0xA100 + index as u128)),
                EvaluationManifestId::new(Uuid::from_u128(0xAD50 + index as u128))?,
                EvaluationRevision::new(1)?,
                vec![member],
            )?)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(MissionEvidenceManifestPreparationCommand::new(
        fixture.workspace,
        fixture.plan_id,
        1,
        fixture.backend_id,
        partitions,
        Fixture::authority()?,
    )?)
}

fn assert_preparation_projection(
    fixture: &Fixture,
    preparation: &MissionEvidenceManifestPreparation,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        preparation.status(),
        MissionEvidenceManifestPreparationStatus::Pending
    );
    assert_eq!(preparation.proposals().len(), 2);
    assert_eq!(preparation.pending_targets().len(), 2);
    for proposal in preparation.proposals() {
        assert_eq!(proposal.source().backend_id(), fixture.backend_id);
        assert_eq!(proposal.source().workspace_id(), fixture.workspace);
        assert_eq!(
            proposal.source().delegation_migration_id(),
            fixture.migration_id
        );
        assert_eq!(
            proposal.source().delegation_destination_id(),
            fixture.destination
        );
        assert_eq!(proposal.source().delegation_freeze_revision().get(), 1);
        assert_eq!(
            proposal.source().delegation_manifest_digest().as_bytes(),
            &fixture.source_digest
        );
        assert_eq!(
            proposal.plan().plan_id().into_uuid(),
            fixture.plan_id.into_uuid()
        );
        assert_eq!(proposal.plan().revision().get(), 1);
        assert_eq!(proposal.plan().digest(), fixture.digests()?.plan_digest());
        let declaration = proposal
            .members()
            .first()
            .ok_or_else(|| std::io::Error::other("prepared proposal member missing"))?;
        let local = fixture
            .obligations
            .iter()
            .find(|obligation| obligation.id.into_uuid() == declaration.obligation_id().into_uuid())
            .ok_or_else(|| std::io::Error::other("prepared obligation missing"))?;
        assert_eq!(
            declaration.work_unit_id().into_uuid(),
            local.work_unit_id.into_uuid()
        );
        assert_eq!(
            declaration.criteria_digest(),
            fixture
                .digests()?
                .member_criteria_digests()
                .iter()
                .find(|criteria| criteria.obligation_id() == local.id)
                .ok_or_else(|| std::io::Error::other("prepared criteria missing"))?
                .digest()
        );
    }
    assert!(preparation.pending_targets().iter().all(|target| matches!(
        target.identity(),
        MissionEvidenceIdentityResolution::Pending {
            reason: MissionEvidenceIdentityPendingReason::NoRegistrationHistory,
            ..
        }
    )));
    Ok(())
}

fn assert_preparation_wrote_nothing(fixture: &Fixture) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_mission_evidence_bindings",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    assert_eq!(
        fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_mission_evidence_requests",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        0
    );
    Ok(())
}

fn assert_preparation_rejections(
    fixture: &Fixture,
    command: &MissionEvidenceManifestPreparationCommand,
) -> Result<(), Box<dyn Error>> {
    let foreign_backend = MissionEvidenceManifestPreparationCommand::new(
        fixture.workspace,
        fixture.plan_id,
        1,
        ForgeBackendId::from_uuid(Uuid::now_v7()),
        command.partitions().to_vec(),
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .prepare_mission_evidence_manifests(&foreign_backend),
        Err(crate::OperationsError::Conflict(
            "mission evidence source handoff scope"
        ))
    ));
    let incomplete = MissionEvidenceManifestPreparationCommand::new(
        fixture.workspace,
        fixture.plan_id,
        1,
        fixture.backend_id,
        vec![command.partitions()[0].clone()],
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .prepare_mission_evidence_manifests(&incomplete),
        Err(crate::OperationsError::Conflict(
            "mission evidence preparation incomplete cohort"
        ))
    ));
    Ok(())
}

#[test]
fn manifest_preparation_derives_local_terms_and_returns_pending_without_writing()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let command = manifest_preparation_command(&fixture)?;
    let preparation = fixture
        .catalog
        .prepare_mission_evidence_manifests(&command)?;
    assert_preparation_projection(&fixture, &preparation)?;
    assert_preparation_wrote_nothing(&fixture)?;
    assert_preparation_rejections(&fixture, &command)?;
    Ok(())
}

#[test]
fn source_original_assignment_target_cannot_change_in_a_later_plan() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let first_bundle = fixture.manifest_bundle(None, None, fixture.source_digest)?;
    let first_command = fixture.bind_command(Uuid::now_v7(), first_bundle)?;
    let first_binding = fixture
        .catalog
        .bind_mission_evidence_manifest(&first_command)?;
    assert!(!first_binding.replayed());
    assert_eq!(
        first_binding.binding().state(),
        MissionEvidenceBindingState::Pending
    );

    let later_plan = MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?;
    let later_digests = fixture.publish_and_start_plan(later_plan, 220, 300, 360, 250)?;
    let changed_target = (
        AgentInstanceId::from_uuid(Uuid::from_u128(0xA701)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xA702)),
    );
    let later_bundle = MissionEvidenceManifestBundle::new(vec![
        fixture.manifest(
            0,
            ManifestSpec::new(
                later_plan,
                later_digests.plan_digest(),
                300,
                fixture.source_digest,
                0xA900,
            )
            .with_target(Some(changed_target)),
        )?,
        fixture.manifest(
            1,
            ManifestSpec::new(
                later_plan,
                later_digests.plan_digest(),
                300,
                fixture.source_digest,
                0xA900,
            ),
        )?,
    ])?;
    let later_command = MissionEvidenceManifestBindingCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        later_plan,
        1,
        later_bundle,
        260,
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture
            .catalog
            .bind_mission_evidence_manifest(&later_command),
        Err(crate::OperationsError::Conflict(
            "mission evidence original assignment identity changed"
        ))
    ));
    assert_eq!(
        fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_mission_evidence_bindings",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        1
    );
    Ok(())
}

fn bound_original_status() -> Result<EvaluationOriginalBindingStatus, Box<dyn Error>> {
    Ok(EvaluationOriginalBindingStatus::Bound(
        EvaluationOriginalAssignmentBinding::new(
            EvaluationDigest::new([0xAD; 32])?,
            AgentAssignmentRevision::new(1)?,
            EvaluationSourceSequence::new(2)?,
        ),
    ))
}

fn bind_initial_bound_source(
    fixture: &mut Fixture,
    status: EvaluationOriginalBindingStatus,
) -> Result<(), Box<dyn Error>> {
    let bundle = set_original_binding_status(
        &fixture.manifest_bundle(None, None, fixture.source_digest)?,
        fixture.obligations[0].id,
        status,
    )?;
    let command = fixture.bind_command(Uuid::now_v7(), bundle)?;
    let outcome = fixture.catalog.bind_mission_evidence_manifest(&command)?;
    assert!(!outcome.replayed());
    assert_eq!(
        outcome.binding().state(),
        MissionEvidenceBindingState::Pending
    );
    Ok(())
}

fn bind_same_original_cumulative_plan(
    fixture: &mut Fixture,
    status: EvaluationOriginalBindingStatus,
) -> Result<(), Box<dyn Error>> {
    let plan = MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?;
    let digests = fixture.publish_and_start_plan(plan, 220, 300, 360, 250)?;
    let bundle = set_original_binding_status(
        &MissionEvidenceManifestBundle::new(vec![
            fixture.manifest(
                0,
                ManifestSpec::new(
                    plan,
                    digests.plan_digest(),
                    300,
                    fixture.source_digest,
                    0xAA00,
                ),
            )?,
            fixture.manifest(
                1,
                ManifestSpec::new(
                    plan,
                    digests.plan_digest(),
                    300,
                    fixture.source_digest,
                    0xAA00,
                ),
            )?,
        ])?,
        fixture.obligations[0].id,
        status,
    )?;
    let command = MissionEvidenceManifestBindingCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        plan,
        1,
        bundle,
        260,
        Fixture::authority()?,
    )?;
    let outcome = fixture.catalog.bind_mission_evidence_manifest(&command)?;
    assert_eq!(
        outcome.binding().state(),
        MissionEvidenceBindingState::Pending
    );
    Ok(())
}

fn reject_changed_assignment(fixture: &mut Fixture) -> Result<(), Box<dyn Error>> {
    let plan = MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?;
    let digests = fixture.publish_and_start_plan(plan, 370, 450, 500, 400)?;
    let assignment = AgentAssignmentId::from_uuid(Uuid::now_v7());
    let bundle = MissionEvidenceManifestBundle::new(vec![
        fixture.manifest(
            0,
            ManifestSpec::new(
                plan,
                digests.plan_digest(),
                450,
                fixture.source_digest,
                0xAB00,
            )
            .with_assignment(Some(assignment)),
        )?,
        fixture.manifest(
            1,
            ManifestSpec::new(
                plan,
                digests.plan_digest(),
                450,
                fixture.source_digest,
                0xAB00,
            ),
        )?,
    ])?;
    let command = MissionEvidenceManifestBindingCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        plan,
        1,
        bundle,
        410,
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture.catalog.bind_mission_evidence_manifest(&command),
        Err(crate::OperationsError::Conflict(
            "mission evidence original assignment identity changed"
        ))
    ));
    Ok(())
}

fn reject_changed_activity(fixture: &mut Fixture) -> Result<(), Box<dyn Error>> {
    let plan = MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?;
    let digests = fixture.publish_and_start_plan(plan, 510, 600, 650, 540)?;
    let activity = ActivityId::from_uuid(Uuid::from_u128(0xAC00));
    let bundle = MissionEvidenceManifestBundle::new(vec![
        fixture.manifest(
            0,
            ManifestSpec::new(
                plan,
                digests.plan_digest(),
                600,
                fixture.source_digest,
                0xAC10,
            )
            .with_activity(Some(activity)),
        )?,
        fixture.manifest(
            1,
            ManifestSpec::new(
                plan,
                digests.plan_digest(),
                600,
                fixture.source_digest,
                0xAC10,
            ),
        )?,
    ])?;
    let command = MissionEvidenceManifestBindingCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        plan,
        1,
        bundle,
        550,
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture.catalog.bind_mission_evidence_manifest(&command),
        Err(crate::OperationsError::Conflict(
            "mission evidence original assignment identity changed"
        ))
    ));
    Ok(())
}

fn reject_bound_to_unknown_downgrade(fixture: &mut Fixture) -> Result<(), Box<dyn Error>> {
    let plan = MissionEvaluationPlanId::from_uuid(Uuid::now_v7())?;
    let digests = fixture.publish_and_start_plan(plan, 660, 750, 800, 700)?;
    let bundle = MissionEvidenceManifestBundle::new(vec![
        fixture.manifest(
            0,
            ManifestSpec::new(
                plan,
                digests.plan_digest(),
                750,
                fixture.source_digest,
                0xAD00,
            ),
        )?,
        fixture.manifest(
            1,
            ManifestSpec::new(
                plan,
                digests.plan_digest(),
                750,
                fixture.source_digest,
                0xAD00,
            ),
        )?,
    ])?;
    let command = MissionEvidenceManifestBindingCommand::new(
        Uuid::now_v7(),
        fixture.workspace,
        plan,
        1,
        bundle,
        710,
        Fixture::authority()?,
    )?;
    assert!(matches!(
        fixture.catalog.bind_mission_evidence_manifest(&command),
        Err(crate::OperationsError::Conflict(
            "mission evidence original binding history changed"
        ))
    ));
    Ok(())
}

#[test]
fn cumulative_source_binding_requires_same_assignment_and_activity_identity()
-> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let status = bound_original_status()?;
    bind_initial_bound_source(&mut fixture, status)?;
    bind_same_original_cumulative_plan(&mut fixture, status)?;
    reject_changed_assignment(&mut fixture)?;
    reject_changed_activity(&mut fixture)?;
    reject_bound_to_unknown_downgrade(&mut fixture)?;
    assert_eq!(binding_count(&fixture)?, 2);
    Ok(())
}

type CatalogRowsSnapshot = BTreeMap<String, Vec<Vec<Value>>>;

fn snapshot_catalog_rows(
    connection: &rusqlite::Connection,
) -> Result<CatalogRowsSnapshot, Box<dyn Error>> {
    let table_names = {
        let mut statement = connection.prepare(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut snapshot = BTreeMap::new();
    for name in table_names {
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        let mut statement = connection.prepare(&format!("SELECT * FROM {quoted_name}"))?;
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|column| row.get::<_, Value>(column))
                    .collect::<Result<Vec<_>, _>>()
            })?
            .collect::<Result<Vec<_>, _>>()?;
        snapshot.insert(name, rows);
    }
    Ok(snapshot)
}

fn assert_schema55_governance_tables_empty(
    connection: &rusqlite::Connection,
) -> Result<(), Box<dyn Error>> {
    let objects = crate::temporary_dispatch::schema_objects()
        .into_iter()
        .chain(
            crate::reviewer_qualification_rule::schema_objects()
                .iter()
                .copied(),
        );
    for (kind, name, _) in objects {
        if kind == "table" {
            let count: i64 =
                connection.query_row(&format!("SELECT COUNT(*) FROM {name}"), [], |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0, "new governance table {name} must be empty");
        }
    }
    Ok(())
}

fn assert_foreign_keys_clean(connection: &rusqlite::Connection) -> Result<(), Box<dyn Error>> {
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    let violations = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        violations.is_empty(),
        "foreign-key check found {violations:?}"
    );
    Ok(())
}

#[test]
fn schema_v54_upgrade_preserves_nonempty_mission_evidence_records() -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new()?;
    let bundle = fixture.manifest_bundle(None, None, fixture.source_digest)?;
    let binding = bind_pending_manifest(&mut fixture, bundle)?;
    let (_, observation) = record_first_pending_observation(&mut fixture, &binding)?;
    assert_eq!(binding_count(&fixture)?, 1);
    let observation_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_mission_evidence_observations",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(observation_count, 1);
    let observation_member_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_mission_evidence_observation_members WHERE observation_request_id=?1",
        params![observation.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(observation_member_count, 2);

    let before = snapshot_catalog_rows(&fixture.catalog.connection)?;
    crate::catalog::drop_v55_governance_schema_for_legacy_fixture(&fixture.catalog.connection)?;
    fixture
        .catalog
        .connection
        .pragma_update(None, "user_version", 54)?;
    fixture.reopen()?;
    assert_eq!(
        fixture
            .catalog
            .connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
        crate::catalog::VERSION
    );
    assert_eq!(snapshot_catalog_rows(&fixture.catalog.connection)?, before);
    assert_eq!(
        fixture
            .catalog
            .mission_evidence_binding(fixture.workspace, fixture.plan_id, 1)?,
        Some(binding.clone())
    );
    assert_eq!(
        fixture
            .catalog
            .mission_evidence_observation(fixture.workspace, observation.request_id())?,
        Some(observation.clone())
    );
    assert_schema55_governance_tables_empty(&fixture.catalog.connection)?;
    assert_foreign_keys_clean(&fixture.catalog.connection)?;

    fixture.close_catalog()?;
    let read_only = OperationsCatalog::open_existing_read_only(&fixture.path, fixture.destination)?;
    assert_eq!(snapshot_catalog_rows(&read_only.connection)?, before);
    assert_eq!(
        read_only.mission_evidence_binding(fixture.workspace, fixture.plan_id, 1)?,
        Some(binding)
    );
    assert_eq!(
        read_only.mission_evidence_observation(fixture.workspace, observation.request_id())?,
        Some(observation)
    );
    assert_schema55_governance_tables_empty(&read_only.connection)?;
    assert_foreign_keys_clean(&read_only.connection)?;
    drop(read_only);
    Ok(())
}

#[cfg(test)]
mod actor_alias_tests {
    use super::super::model::{
        MissionEvidenceIdentityActor, MissionEvidenceIdentityProofParts,
        MissionEvidenceParticipationProvenance, MissionEvidenceProfileProvenance,
        MissionEvidenceRegistrationProvenance,
    };
    use super::*;

    fn proof(
        actor: u128,
        session: u128,
        participant: u128,
        profile: u128,
    ) -> Result<MissionEvidenceIdentityProof, crate::OperationsError> {
        MissionEvidenceIdentityProof::from_parts(&MissionEvidenceIdentityProofParts {
            actor: MissionEvidenceIdentityActor {
                agent: AgentInstanceId::from_uuid(id(actor)),
                session: AgentSessionId::from_uuid(id(session)),
                participant: id(participant),
            },
            registration: MissionEvidenceRegistrationProvenance {
                kind: MissionEvidenceRegistrationKind::Original,
                request_id: id(actor + 2000),
                migration_id: id(3000),
                destination_id: id(4000),
                freeze_revision: 1,
                membership_commit_digest: [0x81; 32],
                activation_receipt_id: id(actor + 5000),
                activated_at_ms: 10,
            },
            profile: Some(MissionEvidenceProfileProvenance {
                profile_id: id(profile),
                mapping_request_id: id(profile + 6000),
                profile_revision: 1,
                session_revision: 1,
            }),
            participation: Some(MissionEvidenceParticipationProvenance {
                participation_id: WorkspaceParticipationId::from_uuid(id(participant + 1000))?,
                mapping_request_id: Some(id(participant + 7000)),
                participant_membership_revision: 1,
                mapped_at_ms: Some(11),
            }),
            replacement: None,
        })
    }

    #[test]
    fn different_sessions_under_one_profile_are_not_independent_reviewers()
    -> Result<(), crate::OperationsError> {
        let author = proof(10, 11, 12, 13)?;
        let rebound_reviewer = proof(20, 21, 12, 13)?;
        assert!(super::super::evaluate::same_logical_actor(
            &author,
            &rebound_reviewer
        )?);

        let independent = proof(30, 31, 32, 33)?;
        assert!(!super::super::evaluate::same_logical_actor(
            &author,
            &independent
        )?);
        Ok(())
    }

    #[test]
    fn verified_target_lineage_rejects_a_foreign_participation()
    -> Result<(), crate::OperationsError> {
        let target = proof(40, 41, 42, 43)?;
        let identity = MissionEvidenceIdentityResolution::Verified(Box::new(target.clone()));
        let participation = target
            .participation_id()
            .ok_or(crate::OperationsError::Corrupt(
                "test participation missing",
            ))?;
        assert!(super::super::store::target_identity_matches_participation(
            &identity,
            participation
        )?);
        assert!(matches!(
            super::super::store::target_identity_matches_participation(
                &identity,
                WorkspaceParticipationId::from_uuid(id(5000))?
            ),
            Err(crate::OperationsError::Conflict(
                "mission evidence target participation foreign"
            ))
        ));
        assert!(!super::super::store::target_identity_matches_participation(
            &MissionEvidenceIdentityResolution::Pending {
                agent_id: target.actor_agent_id(),
                session_id: target.actor_session_id(),
                reason: MissionEvidenceIdentityPendingReason::NoRegistrationHistory,
                registration_request_id: None,
                activation_receipt_id: None,
            },
            participation
        )?);
        Ok(())
    }
}
