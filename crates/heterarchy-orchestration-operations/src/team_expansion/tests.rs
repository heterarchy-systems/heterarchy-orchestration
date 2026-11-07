use super::{
    TeamCapacitySetCommand, TeamExpansionDecision, TeamExpansionDecisionCommand,
    TeamExpansionDecisionState, TeamExpansionRevisions, TeamExpansionScope,
    TeamExpansionSlotAssignmentCommand, TeamExpansionSlotRef, TeamMemberCapacity,
};
use crate::{
    OperationalActor, OperationsCatalog, OperationsError, TeamCreateCommand, TeamJoinCommand,
    TeamRosterMemberInput, WorkspaceParticipationId, teams::tests::create_connection,
};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AgentSessionOpenRequestId, AuthenticatedPrincipal,
    OpenAgentSession, RegisteredAgentSession, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_sessions::{
    AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileId,
    AgentProfileLogicalSessionLink, AgentProfileProviderFamily, AgentSessionCatalog,
    CreateAgentProfile, link_existing_logical_session,
};
use rusqlite::{Connection, params};
use std::{
    cell::Cell,
    error::Error,
    fs::OpenOptions,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};
use uuid::Uuid;

mod permanent_transfer_preparation;
mod permanent_transfer_renewal;
mod source_dispatch;

const DESTINATION: Uuid = Uuid::from_u128(0x9000);
const WORKSPACE: WorkspaceId = WorkspaceId::from_uuid(Uuid::from_u128(0x1000));
const ORCHESTRATOR_PARTICIPANT: Uuid = Uuid::from_u128(0x5000);
const ORCHESTRATOR_PROFILE: Uuid = Uuid::from_u128(0x3000);
const ORCHESTRATOR_PARTICIPATION: Uuid = Uuid::from_u128(0x4000);
const ORCHESTRATOR_ACTIVITY: Uuid = Uuid::from_u128(0x6000);
const ORCHESTRATOR_AGENT: Uuid = Uuid::from_u128(0x7000);
const ORCHESTRATOR_SESSION: Uuid = Uuid::from_u128(0x8000);
const MEMBER_TWO_PARTICIPANT: Uuid = Uuid::from_u128(0x5100);
const MEMBER_TWO_ACTIVITY: Uuid = Uuid::from_u128(0x6100);
const MEMBER_TWO_AGENT: Uuid = Uuid::from_u128(0x7100);
const MEMBER_TWO_SESSION: Uuid = Uuid::from_u128(0x8100);
const MEMBER_THREE_PROFILE: Uuid = Uuid::from_u128(0x3200);
const MEMBER_THREE_PARTICIPATION: Uuid = Uuid::from_u128(0x4200);
const MEMBER_THREE_PARTICIPANT: Uuid = Uuid::from_u128(0x5200);
const MEMBER_THREE_ACTIVITY: Uuid = Uuid::from_u128(0x6200);
const MEMBER_THREE_AGENT: Uuid = Uuid::from_u128(0x7200);
const MEMBER_THREE_SESSION: Uuid = Uuid::from_u128(0x8200);
const MEMBER_FOUR_PROFILE: Uuid = Uuid::from_u128(0x3300);
const MEMBER_FOUR_PARTICIPATION: Uuid = Uuid::from_u128(0x4300);
const MEMBER_FOUR_PARTICIPANT: Uuid = Uuid::from_u128(0x5300);
const MEMBER_FOUR_ACTIVITY: Uuid = Uuid::from_u128(0x6300);
const MEMBER_FOUR_AGENT: Uuid = Uuid::from_u128(0x7300);
const MEMBER_FOUR_SESSION: Uuid = Uuid::from_u128(0x8300);
const MEMBER_FIVE_PROFILE: Uuid = Uuid::from_u128(0x3400);
const MEMBER_FIVE_PARTICIPATION: Uuid = Uuid::from_u128(0x4400);
const MEMBER_FIVE_PARTICIPANT: Uuid = Uuid::from_u128(0x5400);
const MEMBER_FIVE_ACTIVITY: Uuid = Uuid::from_u128(0x6400);
const MEMBER_FIVE_AGENT: Uuid = Uuid::from_u128(0x7400);
const MEMBER_FIVE_SESSION: Uuid = Uuid::from_u128(0x8400);
const MEMBER_SIX_PROFILE: Uuid = Uuid::from_u128(0x3500);
const MEMBER_SIX_PARTICIPATION: Uuid = Uuid::from_u128(0x4500);
const MEMBER_SIX_PARTICIPANT: Uuid = Uuid::from_u128(0x5500);
const MEMBER_SIX_ACTIVITY: Uuid = Uuid::from_u128(0x6500);
const MEMBER_SIX_AGENT: Uuid = Uuid::from_u128(0x7500);
const MEMBER_SIX_SESSION: Uuid = Uuid::from_u128(0x8500);
const MEMBER_SEVEN_PROFILE: Uuid = Uuid::from_u128(0x3600);
const MEMBER_SEVEN_PARTICIPATION: Uuid = Uuid::from_u128(0x4600);
const MEMBER_SEVEN_PARTICIPANT: Uuid = Uuid::from_u128(0x5600);
const MEMBER_SEVEN_ACTIVITY: Uuid = Uuid::from_u128(0x6600);
const MEMBER_SEVEN_AGENT: Uuid = Uuid::from_u128(0x7600);
const MEMBER_SEVEN_SESSION: Uuid = Uuid::from_u128(0x8600);
const MEMBER_EIGHT_PROFILE: Uuid = Uuid::from_u128(0x3700);
const MEMBER_EIGHT_PARTICIPATION: Uuid = Uuid::from_u128(0x4700);
const MEMBER_EIGHT_PARTICIPANT: Uuid = Uuid::from_u128(0x5700);
const MEMBER_EIGHT_ACTIVITY: Uuid = Uuid::from_u128(0x6700);
const MEMBER_EIGHT_AGENT: Uuid = Uuid::from_u128(0x7700);
const MEMBER_EIGHT_SESSION: Uuid = Uuid::from_u128(0x8700);
const MEMBER_SIX_ALIAS_PARTICIPANT: Uuid = Uuid::from_u128(0x5510);
const MEMBER_SIX_ALIAS_ACTIVITY: Uuid = Uuid::from_u128(0x6510);
const MEMBER_SIX_ALIAS_AGENT: Uuid = Uuid::from_u128(0x7510);
const MEMBER_SIX_ALIAS_SESSION: Uuid = Uuid::from_u128(0x8510);
const TEAM_ID: Uuid = Uuid::from_u128(0xE100);
const SECOND_TEAM_ID: Uuid = Uuid::from_u128(0xE101);
const BYPASS_TEAM_ID: Uuid = Uuid::from_u128(0xE103);

type TestResult = Result<(), Box<dyn Error>>;

trait TeamExpansionDecisionTestExt {
    fn decide_team_expansion_v2_test(
        &mut self,
        command: &TeamExpansionDecisionCommand,
        trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<super::TeamExpansionDecisionReceipt, OperationsError>;

    fn decide_team_expansion_v2_for_existing(
        &mut self,
        command: &TeamExpansionDecisionCommand,
        targets: &[(Uuid, Uuid)],
        trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<super::TeamExpansionDecisionReceipt, OperationsError>;
}

impl TeamExpansionDecisionTestExt for OperationsCatalog {
    fn decide_team_expansion_v2_test(
        &mut self,
        command: &TeamExpansionDecisionCommand,
        trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<super::TeamExpansionDecisionReceipt, OperationsError> {
        let plans = match command.decision {
            TeamExpansionDecision::Approve { approved_count } => (0..approved_count)
                .map(|ordinal| {
                    super::TeamExpansionApprovalSlotPlan::new(
                        ordinal,
                        super::TeamExpansionWorkspaceTarget::NewProfile,
                    )
                })
                .collect(),
            TeamExpansionDecision::Reject => Vec::new(),
        };
        self.decide_team_expansion_v2(
            &super::TeamExpansionDecisionV2Command::new(command.clone(), plans),
            trusted_now,
        )
    }

    fn decide_team_expansion_v2_for_existing(
        &mut self,
        command: &TeamExpansionDecisionCommand,
        targets: &[(Uuid, Uuid)],
        trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<super::TeamExpansionDecisionReceipt, OperationsError> {
        let plans = targets
            .iter()
            .enumerate()
            .map(|(ordinal, (profile_id, participation_id))| {
                Ok(super::TeamExpansionApprovalSlotPlan::new(
                    u8::try_from(ordinal)
                        .map_err(|_| OperationsError::Bounds("test target ordinal"))?,
                    super::TeamExpansionWorkspaceTarget::ExistingWorkspaceProfile {
                        profile_id: *profile_id,
                        participation_id: WorkspaceParticipationId::from_uuid(*participation_id)
                            .map_err(|_| OperationsError::Conflict("test participation"))?,
                    },
                ))
            })
            .collect::<Result<Vec<_>, OperationsError>>()?;
        self.decide_team_expansion_v2(
            &super::TeamExpansionDecisionV2Command::new(command.clone(), plans),
            trusted_now,
        )
    }
}
static DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Conflict("test timestamp"))
}

fn team_create_owner_mutation_counts(
    catalog: &OperationsCatalog,
) -> Result<[i64; 6], Box<dyn Error>> {
    Ok([
        count_rows(&catalog.connection, "ops_teams")?,
        count_rows(&catalog.connection, "ops_team_members")?,
        count_rows(&catalog.connection, "ops_team_requests")?,
        count_rows(&catalog.connection, "ops_team_capacity_events")?,
        count_rows(&catalog.connection, "ops_team_member_events")?,
        count_rows(&catalog.connection, "ops_team_create_roster")?,
    ])
}

fn roster_digest_for_create_command(
    command: &TeamCreateCommand,
) -> Result<super::TeamCreateRosterDigest, OperationsError> {
    let members: Vec<_> = command
        .initial_members
        .iter()
        .map(|member| {
            super::TeamRosterDigestInput::new(member.activity_id, member.session_id, &member.role)
        })
        .collect();
    super::team_create_roster_digest(&members)
}

fn stored_team_create_roster_digest(
    connection: &Connection,
    request_id: Uuid,
) -> Result<super::TeamCreateRosterDigest, Box<dyn Error>> {
    let stored_rows = {
        let mut statement = connection.prepare(
            "SELECT activity_id,session_id,role FROM ops_team_create_roster WHERE request_id=?1 ORDER BY ordinal",
        )?;
        let rows = statement.query_map(params![request_id.as_bytes()], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    let inputs = stored_rows
        .iter()
        .map(|(activity, session, role)| {
            Ok(super::TeamRosterDigestInput::new(
                ActivityId::from_uuid(Uuid::from_slice(activity)?),
                AgentSessionId::from_uuid(Uuid::from_slice(session)?),
                role,
            ))
        })
        .collect::<Result<Vec<_>, uuid::Error>>()?;
    Ok(super::team_create_roster_digest(&inputs)?)
}

fn assert_capped_create_source(
    readback: &super::TeamMutationOwnerReadback,
    expected_digest: super::TeamCreateRosterDigest,
    expected_capacity: u8,
) -> TestResult {
    assert_eq!(
        readback.mutation().initial_roster_digest(),
        Some(expected_digest)
    );
    assert_eq!(
        readback
            .mutation()
            .team_capacity()
            .map(|capacity| capacity.max_members().get()),
        Some(expected_capacity)
    );
    let Some(super::TeamMutationSourceEvidence::CappedCreate {
        capacity,
        roster_digest,
    }) = readback.source()
    else {
        return Err("capped CREATE source evidence missing".into());
    };
    assert_eq!(*roster_digest, expected_digest);
    assert_eq!(capacity.max_members().get(), expected_capacity);
    assert_eq!(capacity.revision(), 1);
    Ok(())
}

fn prepare_v2_join_slots(
    catalog: &mut OperationsCatalog,
    team: &crate::TeamMutationRecord,
    requester: OperationalActor,
) -> Result<[TeamExpansionSlotRef; 2], Box<dyn Error>> {
    let request = catalog
        .request_team_expansion(&expansion_request(0xF740, team, requester, 2)?, || {
            timestamp(100)
        })?;
    let pending = catalog.decide_team_expansion_v2_for_existing(
        &decide_command(
            Uuid::from_u128(0xF741),
            &request,
            TeamExpansionDecision::Approve { approved_count: 2 },
            orchestrator(),
            1,
        ),
        &[
            (MEMBER_FIVE_PROFILE, MEMBER_FIVE_PARTICIPATION),
            (MEMBER_EIGHT_PROFILE, MEMBER_EIGHT_PARTICIPATION),
        ],
        || timestamp(200),
    )?;
    let decision = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    let slot_five = TeamExpansionSlotRef::new(decision.decision_id(), 0);
    let slot_eight = TeamExpansionSlotRef::new(decision.decision_id(), 1);
    let assign_five = build_slot_assignment_command(0xF742, slot_five, team, fifth_participant())?;
    let assign_eight =
        build_slot_assignment_command(0xF743, slot_eight, team, eighth_participant())?;
    catalog.assign_team_expansion_slot(&assign_five, || timestamp(400))?;
    catalog.assign_team_expansion_slot(&assign_eight, || timestamp(401))?;
    Ok([slot_five, slot_eight])
}

fn assert_v2_join_source(
    catalog: &OperationsCatalog,
    request_id: Uuid,
    reader: OperationalActor,
    expected_actor: OperationalActor,
    slot: TeamExpansionSlotRef,
    expected_role: &str,
) -> TestResult {
    let owner_readback = catalog
        .team_mutation_owner_readback(WORKSPACE, TEAM_ID, request_id, reader)?
        .ok_or("authorized v2 Team Join readback missing")?;
    assert_eq!(owner_readback.mutation().expansion_slot(), Some(slot));
    let Some(super::TeamMutationSourceEvidence::SlotBackedJoin(source)) = owner_readback.source()
    else {
        return Err("v2 slot-backed Join source evidence missing".into());
    };
    assert_eq!(source.actor(), expected_actor);
    assert_eq!(source.slot(), slot);
    assert_eq!(source.role(), expected_role);
    Ok(())
}

fn slot_backed_join_command(
    request_id: u128,
    expected_team_revision: u64,
    actor: OperationalActor,
    slot: TeamExpansionSlotRef,
    role: &str,
    observed_at: i64,
) -> Result<TeamJoinCommand, OperationsError> {
    Ok(TeamJoinCommand {
        request_id: Uuid::from_u128(request_id),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision,
        actor,
        role: role.to_owned(),
        expansion_slot: Some(slot),
        observed_at: timestamp(observed_at)?,
    })
}

fn assert_join_source_replay_and_payload_conflicts(
    catalog: &mut OperationsCatalog,
    command: &TeamJoinCommand,
    original_actor: OperationalActor,
    slot: TeamExpansionSlotRef,
    changed_slot: TeamExpansionSlotRef,
) -> Result<[i64; 5], Box<dyn Error>> {
    let before = team_join_mutation_counts(catalog)?;
    let team_revision_before = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing before exact Join replay")?
        .revision();
    let replay = catalog.join_team(command.clone())?;
    assert!(replay.replayed());
    assert_eq!(replay.expansion_slot(), Some(slot));
    assert_v2_join_source(
        catalog,
        command.request_id,
        original_actor,
        original_actor,
        slot,
        &command.role,
    )?;

    let changed_slot_command = TeamJoinCommand {
        expansion_slot: Some(changed_slot),
        ..command.clone()
    };
    assert!(matches!(
        catalog.join_team(changed_slot_command),
        Err(OperationsError::Conflict("team request replay"))
    ));
    let changed_role_command = TeamJoinCommand {
        role: "changed source role".to_owned(),
        ..command.clone()
    };
    assert!(matches!(
        catalog.join_team(changed_role_command),
        Err(OperationsError::Conflict("team request replay"))
    ));
    assert_eq!(team_join_mutation_counts(catalog)?, before);
    assert_eq!(
        catalog
            .team(WORKSPACE, TEAM_ID)?
            .ok_or("Team missing after changed Join replay")?
            .revision(),
        team_revision_before
    );
    assert_v2_join_source(
        catalog,
        command.request_id,
        original_actor,
        original_actor,
        slot,
        &command.role,
    )?;
    Ok(before)
}

fn assert_rebound_actor_cannot_replace_committed_join_source(
    catalog: &mut OperationsCatalog,
    original_command: &TeamJoinCommand,
    original_actor: OperationalActor,
    rebound_actor: OperationalActor,
    slot: TeamExpansionSlotRef,
    role: &str,
    counts_before: [i64; 5],
) -> TestResult {
    let team_revision_before = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing before rebound-session replay")?
        .revision();
    let changed_session_command = TeamJoinCommand {
        actor: rebound_actor,
        ..original_command.clone()
    };
    assert!(matches!(
        catalog.join_team(changed_session_command),
        Err(OperationsError::Conflict("team request replay"))
    ));
    assert_eq!(team_join_mutation_counts(catalog)?, counts_before);
    assert_eq!(
        catalog
            .team(WORKSPACE, TEAM_ID)?
            .ok_or("Team missing after rebound-session replay")?
            .revision(),
        team_revision_before
    );
    assert_v2_join_source(
        catalog,
        original_command.request_id,
        rebound_actor,
        original_actor,
        slot,
        role,
    )?;
    assert_v2_join_source(
        catalog,
        original_command.request_id,
        orchestrator(),
        original_actor,
        slot,
        role,
    )
}

fn team_join_mutation_counts(catalog: &OperationsCatalog) -> Result<[i64; 5], Box<dyn Error>> {
    Ok([
        count_rows(&catalog.connection, "ops_teams")?,
        count_rows(&catalog.connection, "ops_team_members")?,
        count_rows(&catalog.connection, "ops_team_requests")?,
        count_rows(&catalog.connection, "ops_team_member_events")?,
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
    ])
}

struct TemporaryDatabase {
    path: PathBuf,
}

impl TemporaryDatabase {
    fn new() -> Result<Self, IoError> {
        for _ in 0..64 {
            let sequence = DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "team-expansion-owner-{}-{sequence}.sqlite",
                std::process::id()
            ));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    drop(file);
                    std::fs::remove_file(&path)?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(IoError::new(
            ErrorKind::AlreadyExists,
            "no unique test database path",
        ))
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.path.as_os_str().to_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(path));
        }
    }
}

struct TemporarySessionCatalogDirectory {
    path: PathBuf,
}

impl TemporarySessionCatalogDirectory {
    fn new() -> Result<Self, IoError> {
        for _ in 0..64 {
            let sequence = DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "team-expansion-session-catalog-{}-{sequence}",
                std::process::id()
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(IoError::new(
            ErrorKind::AlreadyExists,
            "no unique test Session catalog directory",
        ))
    }
}

impl Drop for TemporarySessionCatalogDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct SyntheticProfileSessionPair {
    old_session: RegisteredAgentSession,
    new_session: RegisteredAgentSession,
    new_link: AgentProfileLogicalSessionLink,
}

fn create_synthetic_profile_session_pair(
    profiles: &mut AgentProfileCatalog,
    sessions: &mut AgentSessionCatalog,
    profile_uuid: Uuid,
    old_session_uuid: Uuid,
    new_session_uuid: Uuid,
    seed: u128,
) -> Result<SyntheticProfileSessionPair, Box<dyn Error>> {
    let profile_id = AgentProfileId::from_uuid(profile_uuid)?;
    let profile = CreateAgentProfile::new(
        profile_id,
        Uuid::from_u128(seed + 1),
        format!("team-expansion-profile-{seed:x}"),
        "Synthetic Team expansion Profile".to_owned(),
        AgentProfileProviderFamily::ChatGpt,
        Some("gpt-5.6-luna".to_owned()),
        Some("max".to_owned()),
        vec!["rust".to_owned()],
        AgentProfileCreationOrigin::UserManual,
        timestamp(10)?,
    )?;
    profiles.create_profile(&profile)?;
    let principal = AuthenticatedPrincipal::TrustedStdio;
    let old_session = sessions
        .open_session(&OpenAgentSession::new(
            AgentSessionId::from_uuid(old_session_uuid),
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(seed + 2)),
            principal,
            timestamp(11)?,
        ))?
        .session()
        .clone();
    let new_session = sessions
        .open_session(&OpenAgentSession::new(
            AgentSessionId::from_uuid(new_session_uuid),
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(seed + 3)),
            principal,
            timestamp(12)?,
        ))?
        .session()
        .clone();
    let _old_link = link_existing_logical_session(
        profiles,
        sessions,
        profile_id,
        old_session.id(),
        principal,
        Uuid::from_u128(seed + 4),
        timestamp(13)?,
    )?
    .link();
    let new_link = link_existing_logical_session(
        profiles,
        sessions,
        profile_id,
        new_session.id(),
        principal,
        Uuid::from_u128(seed + 5),
        timestamp(14)?,
    )?
    .link();
    Ok(SyntheticProfileSessionPair {
        old_session,
        new_session,
        new_link,
    })
}

#[derive(Clone, Copy)]
struct SyntheticSessionRebindTarget {
    profile: Uuid,
    participation: Uuid,
    participant: Uuid,
    original_actor: OperationalActor,
    rebound_activity: Uuid,
    rebound_agent: Uuid,
    rebind_request: Uuid,
    mapping_request: Uuid,
    effect_request: Uuid,
}

fn rebind_synthetic_profile_session(
    catalog: &mut OperationsCatalog,
    source: &SyntheticProfileSessionPair,
    target: SyntheticSessionRebindTarget,
) -> Result<OperationalActor, Box<dyn Error>> {
    let OperationalActor::Participant { session_id, .. } = target.original_actor else {
        return Err("Session rebind fixture requires participant source".into());
    };
    if session_id != source.old_session.id() {
        return Err("Session rebind fixture old Session mismatch".into());
    }
    let removed = catalog.connection.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            target.participant.as_bytes()
        ],
    )?;
    if removed != 1 {
        return Err("Session rebind fixture expected one drained effect binding".into());
    }
    let command = crate::WorkspaceParticipationSessionRebindCommand::new(
        target.rebind_request,
        WORKSPACE,
        source.old_session.id(),
        1,
        timestamp(20)?,
    )?;
    let receipt = catalog.rebind_workspace_participation_session(
        command,
        source.new_link,
        &source.new_session,
    )?;
    assert_eq!(receipt.participant_id(), target.participant);
    assert_eq!(receipt.participation_id().into_uuid(), target.participation);
    assert_eq!(receipt.profile_id().into_uuid(), target.profile);

    // Synthetic Operations current-effect evidence models the post-rebind
    // source activation; the Session and Profile catalogs above are real local
    // owner APIs, not an Agent runtime or Forge authority roundtrip.
    catalog.connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            target.participant.as_bytes(),
            target.rebound_activity.as_bytes(),
            source.new_session.id().into_uuid().as_bytes(),
            target.rebound_agent.as_bytes(),
            target.effect_request.as_bytes(),
        ],
    )?;
    let rebound_identity = catalog
        .logical_identity_for_session(WORKSPACE, source.new_session.id())?
        .ok_or("rebound logical Session identity missing")?;
    assert_eq!(rebound_identity.participant_id(), target.participant);
    let mapping = catalog.map_workspace_participation(
        target.mapping_request,
        WorkspaceParticipationId::from_uuid(target.participation)?,
        source.new_link,
        &rebound_identity,
        1,
        timestamp(30)?,
    )?;
    assert_eq!(
        mapping.participation().profile_id().into_uuid(),
        target.profile
    );
    Ok(actor(
        target.rebound_activity,
        target.rebound_agent,
        source.new_session.id().into_uuid(),
    ))
}

fn rebind_fifth_profile_session(
    catalog: &mut OperationsCatalog,
    source: &SyntheticProfileSessionPair,
) -> Result<OperationalActor, Box<dyn Error>> {
    rebind_synthetic_profile_session(
        catalog,
        source,
        SyntheticSessionRebindTarget {
            profile: MEMBER_FIVE_PROFILE,
            participation: MEMBER_FIVE_PARTICIPATION,
            participant: MEMBER_FIVE_PARTICIPANT,
            original_actor: fifth_participant(),
            rebound_activity: Uuid::from_u128(0xF824),
            rebound_agent: Uuid::from_u128(0xF825),
            rebind_request: Uuid::from_u128(0xF826),
            mapping_request: Uuid::from_u128(0xF827),
            effect_request: Uuid::from_u128(0xF828),
        },
    )
}

fn rebind_eighth_profile_session(
    catalog: &mut OperationsCatalog,
    source: &SyntheticProfileSessionPair,
) -> Result<OperationalActor, Box<dyn Error>> {
    rebind_synthetic_profile_session(
        catalog,
        source,
        SyntheticSessionRebindTarget {
            profile: MEMBER_EIGHT_PROFILE,
            participation: MEMBER_EIGHT_PARTICIPATION,
            participant: MEMBER_EIGHT_PARTICIPANT,
            original_actor: eighth_participant(),
            rebound_activity: Uuid::from_u128(0xF814),
            rebound_agent: Uuid::from_u128(0xF815),
            rebind_request: Uuid::from_u128(0xF816),
            mapping_request: Uuid::from_u128(0xF817),
            effect_request: Uuid::from_u128(0xF818),
        },
    )
}

fn rebind_seventh_profile_session(
    catalog: &mut OperationsCatalog,
    source: &SyntheticProfileSessionPair,
) -> Result<OperationalActor, Box<dyn Error>> {
    rebind_synthetic_profile_session(
        catalog,
        source,
        SyntheticSessionRebindTarget {
            profile: MEMBER_SEVEN_PROFILE,
            participation: MEMBER_SEVEN_PARTICIPATION,
            participant: MEMBER_SEVEN_PARTICIPANT,
            original_actor: seventh_participant(),
            rebound_activity: Uuid::from_u128(0xFB40),
            rebound_agent: Uuid::from_u128(0xFB41),
            rebind_request: Uuid::from_u128(0xFB42),
            mapping_request: Uuid::from_u128(0xFB43),
            effect_request: Uuid::from_u128(0xFB44),
        },
    )
}

fn assert_owner_mutation_readback_hidden(
    catalog: &OperationsCatalog,
    request_id: Uuid,
    missing_request_id: Uuid,
) -> TestResult {
    let hidden = catalog.team_mutation_owner_readback(
        WORKSPACE,
        TEAM_ID,
        request_id,
        sixth_participant(),
    )?;
    let missing = catalog.team_mutation_owner_readback(
        WORKSPACE,
        TEAM_ID,
        missing_request_id,
        sixth_participant(),
    )?;
    assert_eq!(hidden, None);
    assert_eq!(hidden, missing);
    Ok(())
}

fn assert_authorized_join_source_corruption_fails_closed(
    catalog: &mut OperationsCatalog,
    request_id: Uuid,
) -> TestResult {
    catalog.connection.execute(
        "UPDATE ops_team_requests SET actor_session_id=?1 WHERE request_id=?2",
        params![Uuid::from_u128(0xF8FF).as_bytes(), request_id.as_bytes()],
    )?;
    assert!(matches!(
        catalog.team_mutation_owner_readback(WORKSPACE, TEAM_ID, request_id, orchestrator()),
        Err(OperationsError::Corrupt(_))
    ));
    assert_owner_mutation_readback_hidden(catalog, request_id, Uuid::from_u128(0xF8FE))
}

fn assert_rebound_join_source_is_immutable_and_legacy_source_absent(
    catalog: &mut OperationsCatalog,
    original_join: &TeamJoinCommand,
    original_actor: OperationalActor,
    slot: TeamExpansionSlotRef,
    role: &str,
    counts_before_replay: [i64; 5],
) -> TestResult {
    let session_directory = TemporarySessionCatalogDirectory::new()?;
    let mut profiles =
        AgentProfileCatalog::open(session_directory.path.join("agent-profiles.sqlite3"))?;
    let mut sessions =
        AgentSessionCatalog::open(session_directory.path.join("agent-sessions.sqlite3"))?;
    let profile_sessions = create_synthetic_profile_session_pair(
        &mut profiles,
        &mut sessions,
        MEMBER_EIGHT_PROFILE,
        MEMBER_EIGHT_SESSION,
        Uuid::from_u128(0xF810),
        0xF800,
    )?;
    let rebound_actor = rebind_eighth_profile_session(catalog, &profile_sessions)?;
    assert_rebound_actor_cannot_replace_committed_join_source(
        catalog,
        original_join,
        original_actor,
        rebound_actor,
        slot,
        role,
        counts_before_replay,
    )?;

    // Synthetic migrated-v1 shape: the old JOIN has no serialized source actor.
    catalog.connection.execute(
        "UPDATE ops_team_requests SET request_actor_kind=NULL,request_actor_activity_id=NULL,request_actor_agent_id=NULL,request_leadership_revision=NULL WHERE request_id=?1",
        params![original_join.request_id.as_bytes()],
    )?;
    let legacy_join = catalog
        .team_mutation_owner_readback(WORKSPACE, TEAM_ID, original_join.request_id, orchestrator())?
        .ok_or("current Orchestrator cannot read source-less legacy JOIN")?;
    assert_eq!(legacy_join.source(), None);
    assert_eq!(legacy_join.mutation().expansion_slot(), Some(slot));
    Ok(())
}

fn assert_rebound_profile_consumes_assigned_slot_with_new_request(
    catalog: &mut OperationsCatalog,
    slot: TeamExpansionSlotRef,
) -> TestResult {
    let session_directory = TemporarySessionCatalogDirectory::new()?;
    let mut profiles =
        AgentProfileCatalog::open(session_directory.path.join("agent-profiles.sqlite3"))?;
    let mut sessions =
        AgentSessionCatalog::open(session_directory.path.join("agent-sessions.sqlite3"))?;
    let profile_sessions = create_synthetic_profile_session_pair(
        &mut profiles,
        &mut sessions,
        MEMBER_FIVE_PROFILE,
        MEMBER_FIVE_SESSION,
        Uuid::from_u128(0xF726),
        0xF720,
    )?;
    let rebound_actor = rebind_fifth_profile_session(catalog, &profile_sessions)?;
    let current_team = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing after Profile Session rebind")?;
    assert_eq!(current_team.revision(), 2);

    let role = "source-member-five-rebound";
    let join = slot_backed_join_command(
        0xF751,
        current_team.revision(),
        rebound_actor,
        slot,
        role,
        600,
    )?;
    let joined = catalog.join_team(join.clone())?;
    assert_eq!(joined.expansion_slot(), Some(slot));
    let membership = joined.membership().ok_or("rebound Profile join missing")?;
    assert_eq!(
        membership.participation_id().into_uuid(),
        MEMBER_FIVE_PARTICIPATION
    );
    assert_eq!(membership.profile_id(), Some(MEMBER_FIVE_PROFILE));
    assert_v2_join_source(
        catalog,
        join.request_id,
        rebound_actor,
        rebound_actor,
        slot,
        role,
    )?;
    assert_v2_join_source(
        catalog,
        join.request_id,
        orchestrator(),
        rebound_actor,
        slot,
        role,
    )?;
    assert_owner_mutation_readback_hidden(catalog, join.request_id, Uuid::from_u128(0xF752))?;
    assert_authorized_join_source_corruption_fails_closed(catalog, join.request_id)
}

fn prepare_session_rebind_test_columns(connection: &Connection) -> Result<(), Box<dyn Error>> {
    // The shared lightweight Operations test connection uses an older logical
    // member projection; this source fixture adds the Session replacement fields.
    connection.execute_batch(
        "ALTER TABLE ops_logical_members ADD COLUMN principal_kind INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE ops_logical_members ADD COLUMN principal_id BLOB;
         ALTER TABLE ops_logical_members ADD COLUMN declared_role TEXT NOT NULL DEFAULT 'worker';
         ALTER TABLE ops_logical_members ADD COLUMN transport_kind INTEGER NOT NULL DEFAULT 1;
         ALTER TABLE ops_logical_members ADD COLUMN membership_revision INTEGER NOT NULL DEFAULT 1;
         ALTER TABLE ops_logical_members ADD COLUMN created_at_ms INTEGER NOT NULL DEFAULT 1;",
    )?;
    crate::activation::create_schema(connection)?;
    crate::presence::create_schema(connection)?;
    // The lightweight fixture's Workspace/Team authorities are synthetic,
    // and its connection omits production work-roster admission tables.
    // Session rebind consults these real tables; add schema only, no barriers.
    crate::leadership::work_roster::create_schema(connection)?;
    Ok(())
}

fn workspace_capacity_fixture() -> Result<OperationsCatalog, Box<dyn Error>> {
    workspace_capacity_fixture_from(None)
}

fn workspace_capacity_fixture_at(path: &Path) -> Result<OperationsCatalog, Box<dyn Error>> {
    workspace_capacity_fixture_from(Some(path))
}

fn workspace_capacity_fixture_from(
    path: Option<&Path>,
) -> Result<OperationsCatalog, Box<dyn Error>> {
    let path = path.map(Path::to_path_buf);
    let connection = create_connection(path.as_ref())?;
    crate::election_subject::create_schema(&connection)?;
    // Profile-bound Team creator resolution reads the registration owner
    // before falling back to the synthetic native Profile mapping.
    crate::registration::create_schema(&connection)?;
    crate::registration_profile::create_schema(&connection)?;
    crate::teams::tests::ensure_test_workspace_orchestrator(&connection)?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        WORKSPACE,
        MEMBER_FOUR_PROFILE,
        MEMBER_FOUR_PARTICIPATION,
        MEMBER_FOUR_PARTICIPANT,
        MEMBER_FOUR_ACTIVITY,
        MEMBER_FOUR_AGENT,
        MEMBER_FOUR_SESSION,
        Uuid::from_u128(0xF10F),
    )?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        WORKSPACE,
        MEMBER_FIVE_PROFILE,
        MEMBER_FIVE_PARTICIPATION,
        MEMBER_FIVE_PARTICIPANT,
        MEMBER_FIVE_ACTIVITY,
        MEMBER_FIVE_AGENT,
        MEMBER_FIVE_SESSION,
        Uuid::from_u128(0xF110),
    )?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        WORKSPACE,
        MEMBER_SIX_PROFILE,
        MEMBER_SIX_PARTICIPATION,
        MEMBER_SIX_PARTICIPANT,
        MEMBER_SIX_ACTIVITY,
        MEMBER_SIX_AGENT,
        MEMBER_SIX_SESSION,
        Uuid::from_u128(0xF111),
    )?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        WORKSPACE,
        MEMBER_SEVEN_PROFILE,
        MEMBER_SEVEN_PARTICIPATION,
        MEMBER_SEVEN_PARTICIPANT,
        MEMBER_SEVEN_ACTIVITY,
        MEMBER_SEVEN_AGENT,
        MEMBER_SEVEN_SESSION,
        Uuid::from_u128(0xF112),
    )?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        WORKSPACE,
        MEMBER_EIGHT_PROFILE,
        MEMBER_EIGHT_PARTICIPATION,
        MEMBER_EIGHT_PARTICIPANT,
        MEMBER_EIGHT_ACTIVITY,
        MEMBER_EIGHT_AGENT,
        MEMBER_EIGHT_SESSION,
        Uuid::from_u128(0xF113),
    )?;
    seed_profile_session_alias(&connection)?;
    ensure_expansion_test_schemas(&connection)?;
    // `ensure_test_workspace_orchestrator` creates a clearly labeled synthetic
    // owner receipt only when the fixture lacks one.
    Ok(OperationsCatalog {
        connection,
        destination: DESTINATION,
    })
}

fn ensure_expansion_test_schemas(connection: &Connection) -> Result<(), Box<dyn Error>> {
    let has_expansion_schema = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_team_capacity_events')",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !has_expansion_schema {
        let transaction = connection.unchecked_transaction()?;
        super::create_schema(&transaction)?;
        transaction.commit()?;
    }
    let has_hold_schema = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_team_expansion_workspace_hold_manifests')",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !has_hold_schema {
        let transaction = connection.unchecked_transaction()?;
        super::create_workspace_hold_schema(&transaction)?;
        super::create_legacy_slot_recovery_schema(&transaction)?;
        transaction.execute(super::workspace_hold::PRE_HOLD_DECISION_BACKFILL_SQL, [])?;
        transaction.commit()?;
    }
    let has_transfer_source_schema = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_team_expansion_v3_manifests')",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !has_transfer_source_schema {
        let transaction = connection.unchecked_transaction()?;
        super::create_existing_home_transfer_schema(&transaction)?;
        transaction.commit()?;
    }
    let has_permanent_transfer_schema = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_team_permanent_transfer_intents')",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !has_permanent_transfer_schema {
        crate::team_permanent_transfer::schema::create_schema(connection)?;
        crate::team_permanent_transfer::generation::create_schema(connection)?;
    }
    Ok(())
}

fn actor(activity: Uuid, agent: Uuid, session: Uuid) -> OperationalActor {
    OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(activity),
        agent_id: AgentInstanceId::from_uuid(agent),
        session_id: AgentSessionId::from_uuid(session),
    }
}

fn orchestrator() -> OperationalActor {
    actor(
        ORCHESTRATOR_ACTIVITY,
        ORCHESTRATOR_AGENT,
        ORCHESTRATOR_SESSION,
    )
}

fn fourth_participant() -> OperationalActor {
    actor(MEMBER_FOUR_ACTIVITY, MEMBER_FOUR_AGENT, MEMBER_FOUR_SESSION)
}

fn fifth_participant() -> OperationalActor {
    actor(MEMBER_FIVE_ACTIVITY, MEMBER_FIVE_AGENT, MEMBER_FIVE_SESSION)
}

fn sixth_participant() -> OperationalActor {
    actor(MEMBER_SIX_ACTIVITY, MEMBER_SIX_AGENT, MEMBER_SIX_SESSION)
}

fn seventh_participant() -> OperationalActor {
    actor(
        MEMBER_SEVEN_ACTIVITY,
        MEMBER_SEVEN_AGENT,
        MEMBER_SEVEN_SESSION,
    )
}

fn eighth_participant() -> OperationalActor {
    actor(
        MEMBER_EIGHT_ACTIVITY,
        MEMBER_EIGHT_AGENT,
        MEMBER_EIGHT_SESSION,
    )
}

fn create_team_command(request_id: u128) -> Result<TeamCreateCommand, OperationsError> {
    Ok(TeamCreateCommand {
        request_id: Uuid::from_u128(request_id),
        team_id: TEAM_ID,
        workspace_id: WORKSPACE,
        actor: orchestrator(),
        expected_sender_leadership_revision: Some(1),
        name: "Capacity fixture".to_owned(),
        purpose: "Exercise explicit Team capacity and its history".to_owned(),
        initial_members: vec![
            TeamRosterMemberInput {
                activity_id: ActivityId::from_uuid(MEMBER_TWO_ACTIVITY),
                agent_id: AgentInstanceId::from_uuid(MEMBER_TWO_AGENT),
                session_id: AgentSessionId::from_uuid(MEMBER_TWO_SESSION),
                role: "member".to_owned(),
            },
            TeamRosterMemberInput {
                activity_id: ActivityId::from_uuid(MEMBER_THREE_ACTIVITY),
                agent_id: AgentInstanceId::from_uuid(MEMBER_THREE_AGENT),
                session_id: AgentSessionId::from_uuid(MEMBER_THREE_SESSION),
                role: "member".to_owned(),
            },
        ],
        observed_at: timestamp(10)?,
    })
}

fn create_team_command_with_roster(
    request_id: u128,
    team_id: Uuid,
    roster: &[(OperationalActor, &'static str)],
) -> Result<TeamCreateCommand, OperationsError> {
    Ok(TeamCreateCommand {
        request_id: Uuid::from_u128(request_id),
        team_id,
        workspace_id: WORKSPACE,
        actor: orchestrator(),
        expected_sender_leadership_revision: Some(1),
        name: "Scoped capacity fixture".to_owned(),
        purpose: "Exercise Workspace-wide expansion slot exclusivity".to_owned(),
        initial_members: roster
            .iter()
            .map(|(member, role)| match member {
                OperationalActor::Participant {
                    activity_id,
                    agent_id,
                    session_id,
                } => Ok(TeamRosterMemberInput {
                    activity_id: *activity_id,
                    agent_id: *agent_id,
                    session_id: *session_id,
                    role: (*role).to_owned(),
                }),
                OperationalActor::LocalOperator => Err(OperationsError::Conflict(
                    "Team fixture roster must use participant identities",
                )),
            })
            .collect::<Result<Vec<_>, _>>()?,
        observed_at: timestamp(10)?,
    })
}

fn configured_requester_team() -> Result<
    (
        OperationsCatalog,
        crate::TeamMutationRecord,
        OperationalActor,
    ),
    Box<dyn Error>,
> {
    configured_requester_team_with_capacity(workspace_capacity_fixture()?, 4)
}

fn configured_requester_team_with_capacity(
    mut catalog: OperationsCatalog,
    capacity: u8,
) -> Result<
    (
        OperationsCatalog,
        crate::TeamMutationRecord,
        OperationalActor,
    ),
    Box<dyn Error>,
> {
    let created = catalog.create_team_with_capacity(
        create_team_command(0xF200)?,
        TeamMemberCapacity::new(capacity)?,
    )?;
    seed_team_leader(&catalog.connection, TEAM_ID, MEMBER_TWO_PARTICIPANT)?;
    Ok((
        catalog,
        created,
        actor(MEMBER_TWO_ACTIVITY, MEMBER_TWO_AGENT, MEMBER_TWO_SESSION),
    ))
}

fn create_second_team_with_capacity(
    catalog: &mut OperationsCatalog,
) -> Result<crate::TeamMutationRecord, Box<dyn Error>> {
    let members = [
        (fifth_participant(), "Leader"),
        (seventh_participant(), "Member"),
    ];
    let created = catalog.create_team_with_capacity(
        create_team_command_with_roster(0xF201, SECOND_TEAM_ID, &members)?,
        TeamMemberCapacity::new(4)?,
    )?;
    seed_team_leader(&catalog.connection, SECOND_TEAM_ID, MEMBER_FIVE_PARTICIPANT)?;
    Ok(created)
}

struct TwoTeamApprovedSlots {
    team_a: crate::TeamMutationRecord,
    team_b: crate::TeamMutationRecord,
    slot_a: TeamExpansionSlotRef,
    slot_b: TeamExpansionSlotRef,
}

fn approve_and_anchor_one_slot(
    catalog: &mut OperationsCatalog,
    team_id: Uuid,
    request: &super::TeamExpansionRequestReceipt,
    decision_id: Uuid,
    decision_at: i64,
    anchor_at: i64,
    target: (Uuid, Uuid),
) -> Result<super::TeamExpansionDecisionReceipt, Box<dyn Error>> {
    let pending = catalog.decide_team_expansion_v2_for_existing(
        &decide_command_for(
            decision_id,
            team_id,
            request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        &[target],
        || timestamp(decision_at),
    )?;
    assert_eq!(
        pending.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    let approval = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(anchor_at),
    )?;
    assert_eq!(approval.state(), TeamExpansionDecisionState::Approved);
    assert!(approval.cooldown_until().is_some());
    Ok(approval)
}

fn prepare_two_team_approved_slots(
    catalog: &mut OperationsCatalog,
    team_a_create_request_id: u128,
    expansion_request_seed: u128,
) -> Result<TwoTeamApprovedSlots, Box<dyn Error>> {
    let team_a_members = [
        (
            actor(MEMBER_TWO_ACTIVITY, MEMBER_TWO_AGENT, MEMBER_TWO_SESSION),
            "Leader",
        ),
        (
            actor(
                MEMBER_THREE_ACTIVITY,
                MEMBER_THREE_AGENT,
                MEMBER_THREE_SESSION,
            ),
            "Member",
        ),
    ];
    let team_a = catalog.create_team_with_capacity(
        create_team_command_with_roster(team_a_create_request_id, TEAM_ID, &team_a_members)?,
        TeamMemberCapacity::new(4)?,
    )?;
    seed_team_leader(&catalog.connection, TEAM_ID, MEMBER_TWO_PARTICIPANT)?;
    let team_b = create_second_team_with_capacity(catalog)?;

    let request_a = catalog.request_team_expansion(
        &expansion_request_for(
            expansion_request_seed,
            TEAM_ID,
            &team_a,
            actor(MEMBER_TWO_ACTIVITY, MEMBER_TWO_AGENT, MEMBER_TWO_SESSION),
            1,
        )?,
        || timestamp(100),
    )?;
    let request_b = catalog.request_team_expansion(
        &expansion_request_for(
            expansion_request_seed + 1,
            SECOND_TEAM_ID,
            &team_b,
            fifth_participant(),
            1,
        )?,
        || timestamp(101),
    )?;
    let approval_a = approve_and_anchor_one_slot(
        catalog,
        TEAM_ID,
        &request_a,
        Uuid::from_u128(expansion_request_seed + 2),
        110,
        120,
        (MEMBER_SIX_PROFILE, MEMBER_SIX_PARTICIPATION),
    )?;
    let approval_b = approve_and_anchor_one_slot(
        catalog,
        SECOND_TEAM_ID,
        &request_b,
        Uuid::from_u128(expansion_request_seed + 3),
        111,
        121,
        (MEMBER_EIGHT_PROFILE, MEMBER_EIGHT_PARTICIPATION),
    )?;
    Ok(TwoTeamApprovedSlots {
        team_a,
        team_b,
        slot_a: TeamExpansionSlotRef::new(approval_a.decision_id(), 0),
        slot_b: TeamExpansionSlotRef::new(approval_b.decision_id(), 0),
    })
}

fn build_slot_assignment_command(
    request_id: u128,
    slot: TeamExpansionSlotRef,
    team: &crate::TeamMutationRecord,
    target: OperationalActor,
) -> Result<TeamExpansionSlotAssignmentCommand, OperationsError> {
    let capacity_revision = team
        .team_capacity()
        .ok_or(OperationsError::Conflict("test Team capacity missing"))?
        .revision();
    Ok(TeamExpansionSlotAssignmentCommand::new(
        Uuid::from_u128(request_id),
        slot,
        TeamExpansionScope::new(WORKSPACE, team.team().team_id()),
        TeamExpansionRevisions::new(team.team().revision(), capacity_revision, 1),
        target,
        orchestrator(),
    ))
}

fn assign_slot_to_profile(
    catalog: &mut OperationsCatalog,
    request_id: u128,
    slot: TeamExpansionSlotRef,
    team: &crate::TeamMutationRecord,
    target: OperationalActor,
    recorded_at: i64,
) -> Result<super::TeamExpansionSlotAssignmentReceipt, Box<dyn Error>> {
    let command = build_slot_assignment_command(request_id, slot, team, target)?;
    Ok(catalog.assign_team_expansion_slot(&command, || timestamp(recorded_at))?)
}

fn seed_profile_session_alias(connection: &Connection) -> Result<(), rusqlite::Error> {
    // This synthetic mapping models one stable Profile/participation with a
    // separately bound Session. It is used only for Operations alias checks.
    let mapping_request_id = Uuid::from_u128(0xF120);
    connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,display_name) VALUES(?1,?2,?3,'Profile alias')",
        params![WORKSPACE.into_uuid().as_bytes(), MEMBER_SIX_ALIAS_PARTICIPANT.as_bytes(), MEMBER_SIX_ALIAS_SESSION.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,2)",
        params![
            mapping_request_id.as_bytes(),
            WORKSPACE.into_uuid().as_bytes(),
            MEMBER_SIX_PARTICIPATION.as_bytes(),
            MEMBER_SIX_PROFILE.as_bytes(),
            Uuid::from_u128(0xF121).as_bytes(),
            MEMBER_SIX_ALIAS_SESSION.as_bytes(),
            MEMBER_SIX_ALIAS_PARTICIPANT.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            MEMBER_SIX_ALIAS_PARTICIPANT.as_bytes(),
            MEMBER_SIX_ALIAS_ACTIVITY.as_bytes(),
            MEMBER_SIX_ALIAS_SESSION.as_bytes(),
            MEMBER_SIX_ALIAS_AGENT.as_bytes(),
            Uuid::from_u128(0xF122).as_bytes(),
        ],
    )?;
    Ok(())
}

fn decide_command(
    decision_id: Uuid,
    request: &super::TeamExpansionRequestReceipt,
    decision: TeamExpansionDecision,
    actor: OperationalActor,
    workspace_leadership_revision: i64,
) -> TeamExpansionDecisionCommand {
    decide_command_for(
        decision_id,
        TEAM_ID,
        request,
        decision,
        actor,
        workspace_leadership_revision,
    )
}

fn decide_command_for(
    decision_id: Uuid,
    team_id: Uuid,
    request: &super::TeamExpansionRequestReceipt,
    decision: TeamExpansionDecision,
    actor: OperationalActor,
    workspace_leadership_revision: i64,
) -> TeamExpansionDecisionCommand {
    TeamExpansionDecisionCommand::new(
        decision_id,
        request.request_id(),
        TeamExpansionScope::new(WORKSPACE, team_id),
        TeamExpansionRevisions::new(
            request.expected_team_revision(),
            request.capacity_revision(),
            workspace_leadership_revision,
        ),
        request.requester_leadership_revision(),
        decision,
        actor,
    )
}

fn replace_workspace_orchestrator(
    connection: &Connection,
    new_leader: Uuid,
) -> Result<(), rusqlite::Error> {
    let updated = connection.execute(
        "UPDATE ops_leadership SET leader_participant_id=?1,leadership_revision=2,updated_at_ms=250 WHERE workspace_id=?2 AND scope_kind=0 AND activity_id IS NULL AND team_id IS NULL AND leadership_revision=1",
        params![new_leader.as_bytes(), WORKSPACE.into_uuid().as_bytes()],
    )?;
    if updated != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),2,'CONFIRMED',?2,2,250)",
        params![WORKSPACE.into_uuid().as_bytes(), new_leader.as_bytes()],
    )?;
    Ok(())
}

fn expansion_request(
    request_id: u128,
    team: &crate::TeamMutationRecord,
    requester: OperationalActor,
    requested_count: u8,
) -> Result<super::TeamExpansionRequestCommand, OperationsError> {
    expansion_request_for(request_id, TEAM_ID, team, requester, requested_count)
}

fn expansion_request_for(
    request_id: u128,
    team_id: Uuid,
    team: &crate::TeamMutationRecord,
    requester: OperationalActor,
    requested_count: u8,
) -> Result<super::TeamExpansionRequestCommand, OperationsError> {
    let capacity = team
        .team_capacity()
        .ok_or(OperationsError::Conflict("test Team capacity missing"))?;
    Ok(super::TeamExpansionRequestCommand::new(
        Uuid::from_u128(request_id),
        TeamExpansionScope::new(WORKSPACE, team_id),
        TeamExpansionRevisions::new(team.team().revision(), capacity.revision(), 1),
        requested_count,
        "approve bounded Team capacity".to_owned(),
        requester,
    ))
}

fn seed_team_leader(
    connection: &Connection,
    team_id: Uuid,
    leader_participant_id: Uuid,
) -> Result<(), rusqlite::Error> {
    // Synthetic current Team leadership rows exercise the Operations owner;
    // they are not a live Broker-backed election or confirmation.
    connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,2,NULL,?2,'ACTIVE',?3,1,NULL,NULL,1)",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            team_id.as_bytes(),
            leader_participant_id.as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,1,'CONFIRMED',?3,1,1)",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            team_id.as_bytes(),
            leader_participant_id.as_bytes(),
        ],
    )?;
    Ok(())
}

fn replace_team_leader(
    connection: &Connection,
    team_id: Uuid,
    leader_participant_id: Uuid,
) -> Result<(), rusqlite::Error> {
    let updated = connection.execute(
        "UPDATE ops_leadership SET leader_participant_id=?1,leadership_revision=2,updated_at_ms=510 WHERE workspace_id=?2 AND scope_kind=2 AND team_id=?3 AND leadership_revision=1",
        params![
            leader_participant_id.as_bytes(),
            WORKSPACE.into_uuid().as_bytes(),
            team_id.as_bytes(),
        ],
    )?;
    if updated != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,2,'CONFIRMED',?3,2,510)",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            team_id.as_bytes(),
            leader_participant_id.as_bytes(),
        ],
    )?;
    Ok(())
}

#[test]
fn explicit_capacity_preserves_configured_values_without_inventing_a_legacy_cap() {
    assert!(matches!(TeamMemberCapacity::new(0), Ok(capacity) if capacity.get() == 0));
    assert!(matches!(TeamMemberCapacity::new(8), Ok(capacity) if capacity.get() == 8));
    assert!(matches!(
        TeamMemberCapacity::new(9),
        Err(OperationsError::Bounds("Team member capacity"))
    ));
}

#[test]
fn team_roster_digest_binds_canonical_activity_session_and_role_bytes() -> TestResult {
    let first_activity = ActivityId::from_uuid(Uuid::from_u128(0xF901));
    let second_activity = ActivityId::from_uuid(Uuid::from_u128(0xF902));
    let changed_activity = ActivityId::from_uuid(Uuid::from_u128(0xF903));
    let first_session = AgentSessionId::from_uuid(Uuid::from_u128(0xF911));
    let second_session = AgentSessionId::from_uuid(Uuid::from_u128(0xF912));
    let changed_session = AgentSessionId::from_uuid(Uuid::from_u128(0xF913));
    let base = [
        super::TeamRosterDigestInput::new(first_activity, first_session, "a"),
        super::TeamRosterDigestInput::new(second_activity, second_session, "bc"),
    ];
    let digest = super::team_create_roster_digest(&base)?;
    assert_eq!(
        super::team_create_roster_digest(&[base[1], base[0]])?,
        digest,
        "canonical roster digest is independent of input order"
    );
    assert_ne!(
        super::team_create_roster_digest(&[
            super::TeamRosterDigestInput::new(changed_activity, first_session, "a"),
            base[1],
        ])?,
        digest,
        "Activity identity is source evidence"
    );
    assert_ne!(
        super::team_create_roster_digest(&[
            super::TeamRosterDigestInput::new(first_activity, changed_session, "a"),
            base[1],
        ])?,
        digest,
        "Session identity is source evidence"
    );
    assert_ne!(
        super::team_create_roster_digest(&[
            super::TeamRosterDigestInput::new(first_activity, first_session, "member"),
            base[1],
        ])?,
        digest,
        "role bytes are source evidence"
    );
    let resegmented_roles = [
        super::TeamRosterDigestInput::new(first_activity, first_session, "ab"),
        super::TeamRosterDigestInput::new(second_activity, second_session, "c"),
    ];
    assert_ne!(
        super::team_create_roster_digest(&resegmented_roles)?,
        digest,
        "length-prefixed roles distinguish the same concatenated bytes"
    );
    let unicode_role = [
        super::TeamRosterDigestInput::new(first_activity, first_session, "é"),
        super::TeamRosterDigestInput::new(second_activity, second_session, "x"),
    ];
    let ascii_role = [
        super::TeamRosterDigestInput::new(first_activity, first_session, "e"),
        super::TeamRosterDigestInput::new(second_activity, second_session, "x"),
    ];
    assert_ne!(
        super::team_create_roster_digest(&unicode_role)?,
        super::team_create_roster_digest(&ascii_role)?,
        "UTF-8 role bytes are hashed exactly"
    );
    assert_eq!(digest.to_hex().len(), 64);
    Ok(())
}

fn assert_legacy_capless_create_replay(catalog: &mut OperationsCatalog) -> TestResult {
    let members = [
        (fifth_participant(), "Leader"),
        (seventh_participant(), "Member"),
    ];
    let request_id = Uuid::from_u128(0xF611);
    let command = create_team_command_with_roster(0xF611, SECOND_TEAM_ID, &members)?;
    let created =
        catalog.create_team_with_capacity(command.clone(), TeamMemberCapacity::new(4)?)?;
    assert_eq!(
        created
            .team_capacity()
            .map(super::TeamCapacitySnapshot::max_members),
        Some(TeamMemberCapacity::new(4)?)
    );

    // This isolated DB models a migrated legacy capless Team: its original
    // CREATE receipt remains, while the newer capacity event is absent.
    catalog
        .connection
        .execute_batch("DROP TRIGGER ops_team_capacity_events_no_delete;")?;
    let deleted = catalog.connection.execute(
        "DELETE FROM ops_team_capacity_events WHERE workspace_id=?1 AND request_id=?2",
        params![WORKSPACE.into_uuid().as_bytes(), request_id.as_bytes()],
    )?;
    assert_eq!(deleted, 1);
    crate::team_create_source::replace_verified_with_legacy_fixture(
        &mut catalog.connection,
        request_id,
        WORKSPACE,
        SECOND_TEAM_ID,
    )?;
    assert_eq!(catalog.team_capacity(WORKSPACE, SECOND_TEAM_ID)?, None);

    let before_replay = team_create_owner_mutation_counts(catalog)?;
    let capless_replay = catalog.create_team(command.clone())?;
    assert!(capless_replay.replayed());
    assert_eq!(capless_replay.team_capacity(), None);
    assert_eq!(team_create_owner_mutation_counts(catalog)?, before_replay);
    assert!(matches!(
        catalog.create_team_with_capacity(command, TeamMemberCapacity::new(4)?),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(team_create_owner_mutation_counts(catalog)?, before_replay);
    Ok(())
}

#[test]
fn explicit_team_creation_records_the_configured_capacity_in_its_original_mutation_receipt()
-> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let capacity = TeamMemberCapacity::new(4)?;
    let command = create_team_command(0xF100)?;
    let created = catalog.create_team_with_capacity(command, capacity)?;

    assert!(created.created());
    assert_eq!(
        created
            .team_capacity()
            .map(super::TeamCapacitySnapshot::max_members),
        Some(capacity)
    );
    assert_eq!(
        catalog
            .team_capacity(WORKSPACE, TEAM_ID)?
            .map(|snapshot| snapshot.max_members()),
        Some(capacity)
    );
    Ok(())
}

fn assert_capacity_create_replay_preserves_original_receipt(
    catalog: &mut OperationsCatalog,
) -> TestResult {
    let command = create_team_command(0xF610)?;
    let original_capacity = TeamMemberCapacity::new(3)?;
    let created = catalog.create_team_with_capacity(command.clone(), original_capacity)?;
    assert!(created.created());
    assert_eq!(
        created
            .team_capacity()
            .map(super::TeamCapacitySnapshot::max_members),
        Some(original_capacity)
    );

    let before_mismatch = team_create_owner_mutation_counts(catalog)?;
    assert!(matches!(
        catalog.create_team(command.clone()),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(team_create_owner_mutation_counts(catalog)?, before_mismatch);
    assert!(matches!(
        catalog.create_team_with_capacity(command.clone(), TeamMemberCapacity::new(4)?),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(team_create_owner_mutation_counts(catalog)?, before_mismatch);

    let exact_replay = catalog.create_team_with_capacity(command.clone(), original_capacity)?;
    assert!(exact_replay.replayed());
    assert_eq!(exact_replay.team().revision(), created.team().revision());
    assert_eq!(
        exact_replay
            .team_capacity()
            .map(super::TeamCapacitySnapshot::max_members),
        Some(original_capacity)
    );
    assert_eq!(team_create_owner_mutation_counts(catalog)?, before_mismatch);

    let capacity_before_change = catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("created capacity missing before head change")?;
    catalog.set_team_capacity(
        &TeamCapacitySetCommand::new(
            Uuid::from_u128(0xF612),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(
                created.team().revision(),
                capacity_before_change.revision(),
                1,
            ),
            TeamMemberCapacity::new(4)?,
            orchestrator(),
        ),
        || timestamp(20),
    )?;
    let before_replay_after_head_change = team_create_owner_mutation_counts(catalog)?;
    let replay_after_head_change = catalog.create_team_with_capacity(command, original_capacity)?;
    assert!(replay_after_head_change.replayed());
    assert_eq!(
        replay_after_head_change.team().revision(),
        created.team().revision()
    );
    assert_eq!(
        replay_after_head_change
            .team_capacity()
            .map(super::TeamCapacitySnapshot::max_members),
        Some(original_capacity)
    );
    assert_eq!(
        catalog
            .team_capacity(WORKSPACE, TEAM_ID)?
            .map(|snapshot| snapshot.max_members().get()),
        Some(4)
    );
    assert_eq!(
        team_create_owner_mutation_counts(catalog)?,
        before_replay_after_head_change
    );
    Ok(())
}

#[test]
fn team_create_replays_preserve_original_capacity_presence_and_value() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    assert_capacity_create_replay_preserves_original_receipt(&mut catalog)?;
    assert_legacy_capless_create_replay(&mut catalog)?;
    Ok(())
}

#[test]
fn capped_create_owner_digest_uses_original_roster_after_rebind_and_cap_change() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    prepare_session_rebind_test_columns(&catalog.connection)?;
    let command = create_team_command(0xF720)?;
    let expected_digest = roster_digest_for_create_command(&command)?;
    let created =
        catalog.create_team_with_capacity(command.clone(), TeamMemberCapacity::new(4)?)?;
    seed_team_leader(&catalog.connection, TEAM_ID, MEMBER_TWO_PARTICIPANT)?;
    assert_eq!(created.initial_roster_digest(), Some(expected_digest));
    assert_eq!(
        stored_team_create_roster_digest(&catalog.connection, command.request_id)?,
        expected_digest,
        "capped CREATE digest matches its immutable stored source rows"
    );

    let initial_readback = catalog
        .team_mutation_owner_readback(WORKSPACE, TEAM_ID, command.request_id, orchestrator())?
        .ok_or("current Orchestrator cannot read capped CREATE")?;
    assert_capped_create_source(&initial_readback, expected_digest, 4)?;

    let session_directory = TemporarySessionCatalogDirectory::new()?;
    let mut profiles =
        AgentProfileCatalog::open(session_directory.path.join("agent-profiles.sqlite3"))?;
    let mut sessions =
        AgentSessionCatalog::open(session_directory.path.join("agent-sessions.sqlite3"))?;
    let profile_sessions = create_synthetic_profile_session_pair(
        &mut profiles,
        &mut sessions,
        MEMBER_THREE_PROFILE,
        MEMBER_THREE_SESSION,
        Uuid::from_u128(0xF726),
        0xF720,
    )?;
    let rebound_actor = rebind_synthetic_profile_session(
        &mut catalog,
        &profile_sessions,
        SyntheticSessionRebindTarget {
            profile: MEMBER_THREE_PROFILE,
            participation: MEMBER_THREE_PARTICIPATION,
            participant: MEMBER_THREE_PARTICIPANT,
            original_actor: actor(
                MEMBER_THREE_ACTIVITY,
                MEMBER_THREE_AGENT,
                MEMBER_THREE_SESSION,
            ),
            rebound_activity: Uuid::from_u128(0xF727),
            rebound_agent: Uuid::from_u128(0xF728),
            rebind_request: Uuid::from_u128(0xF729),
            mapping_request: Uuid::from_u128(0xF72A),
            effect_request: Uuid::from_u128(0xF72B),
        },
    )?;
    assert_eq!(
        rebound_actor,
        actor(
            Uuid::from_u128(0xF727),
            Uuid::from_u128(0xF728),
            Uuid::from_u128(0xF726),
        )
    );
    let after_rebind = catalog
        .team_mutation_owner_readback(WORKSPACE, TEAM_ID, command.request_id, orchestrator())?
        .ok_or("capped CREATE disappeared after Session rebind")?;
    assert_capped_create_source(&after_rebind, expected_digest, 4)?;
    assert_eq!(
        stored_team_create_roster_digest(&catalog.connection, command.request_id)?,
        expected_digest,
        "Session rebind preserves the original stored roster source"
    );

    let current_team = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team disappeared before later cap change")?;
    let current_capacity = catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("capacity disappeared before later cap change")?;
    catalog.set_team_capacity(
        &TeamCapacitySetCommand::new(
            Uuid::from_u128(0xF72C),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(current_team.revision(), current_capacity.revision(), 1),
            TeamMemberCapacity::new(5)?,
            orchestrator(),
        ),
        || timestamp(60),
    )?;
    assert_eq!(
        catalog
            .team_capacity(WORKSPACE, TEAM_ID)?
            .map(|capacity| capacity.max_members().get()),
        Some(5)
    );
    let after_cap_change = catalog
        .team_mutation_owner_readback(WORKSPACE, TEAM_ID, command.request_id, orchestrator())?
        .ok_or("capped CREATE disappeared after later capacity generation")?;
    assert_capped_create_source(&after_cap_change, expected_digest, 4)?;
    assert_eq!(
        stored_team_create_roster_digest(&catalog.connection, command.request_id)?,
        expected_digest,
        "later capacity generations do not rewrite the original roster source"
    );
    Ok(())
}

#[test]
fn v2_slot_join_source_replays_exactly_and_allows_rebound_profile_with_new_request_id() -> TestResult
{
    // Team, O, participants, and post-rebind effect bindings are synthetic
    // Operations fixtures; Profile/Session links and rebind use their local APIs.
    let (mut catalog, team, requester) = configured_requester_team()?;
    let [slot_five, slot_eight] = prepare_v2_join_slots(&mut catalog, &team, requester)?;
    prepare_session_rebind_test_columns(&catalog.connection)?;
    let original_eight = eighth_participant();
    let role_eight = "source-member-eight";
    let join_eight = slot_backed_join_command(
        0xF750,
        team.team().revision(),
        original_eight,
        slot_eight,
        role_eight,
        500,
    )?;
    let joined_eight = catalog.join_team(join_eight.clone())?;
    assert_eq!(joined_eight.expansion_slot(), Some(slot_eight));
    let counts_after_eight_join = assert_join_source_replay_and_payload_conflicts(
        &mut catalog,
        &join_eight,
        original_eight,
        slot_eight,
        slot_five,
    )?;

    assert_rebound_join_source_is_immutable_and_legacy_source_absent(
        &mut catalog,
        &join_eight,
        original_eight,
        slot_eight,
        role_eight,
        counts_after_eight_join,
    )?;
    assert_rebound_profile_consumes_assigned_slot_with_new_request(&mut catalog, slot_five)
}

fn insert_source_less_legacy_create_request(
    connection: &Connection,
    request_id: Uuid,
    team_id: Uuid,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO ops_team_requests(request_id,operation,workspace_id,team_id,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,name,purpose,expected_team_revision,result_team_state,result_team_revision,result_team_updated_at_ms,result_membership_state,result_membership_participation_id,result_membership_revision,result_membership_joined_at_ms,result_membership_left_at_ms,created,observed_at_ms,request_actor_kind,request_actor_activity_id,request_actor_agent_id,request_leadership_revision,role,result_membership_role) VALUES(?1,'CREATE',?2,?3,'legacy','Legacy creator',?4,?5,?6,?7,'Legacy capacity unknown','historical fixture',NULL,'ACTIVE',1,1,NULL,NULL,NULL,NULL,NULL,1,1,NULL,NULL,NULL,NULL,NULL,NULL)",
        params![
            request_id.as_bytes(),
            WORKSPACE.into_uuid().as_bytes(),
            team_id.as_bytes(),
            ORCHESTRATOR_PARTICIPATION.as_bytes(),
            ORCHESTRATOR_PROFILE.as_bytes(),
            ORCHESTRATOR_PARTICIPANT.as_bytes(),
            ORCHESTRATOR_SESSION.as_bytes(),
        ],
    )?;
    Ok(())
}

#[test]
fn legacy_capless_owner_readback_does_not_synthesize_source_digest_or_actor() -> TestResult {
    let catalog = workspace_capacity_fixture()?;
    let legacy_team = Uuid::from_u128(0xE104);
    let request_id = Uuid::from_u128(0xF730);
    insert_legacy_team_without_capacity(&catalog.connection, legacy_team)?;
    insert_source_less_legacy_create_request(&catalog.connection, request_id, legacy_team)?;

    let owner_readback = catalog
        .team_mutation_owner_readback(WORKSPACE, legacy_team, request_id, orchestrator())?
        .ok_or("current Orchestrator cannot read source-less legacy CREATE")?;
    assert_eq!(owner_readback.source(), None);
    assert_eq!(owner_readback.mutation().team_capacity(), None);
    assert_eq!(owner_readback.mutation().initial_roster_digest(), None);
    assert!(owner_readback.mutation().initial_members().is_empty());
    Ok(())
}

#[test]
fn team_create_rolls_back_when_explicit_capacity_cannot_hold_its_initial_roster() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let result = catalog
        .create_team_with_capacity(create_team_command(0xF101)?, TeamMemberCapacity::new(1)?);
    let Err(error) = result else {
        return Err("one slot cannot hold the existing two-member minimum roster".into());
    };

    assert!(matches!(
        error,
        OperationsError::Bounds("team initial roster")
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_teams")?,
        0,
        "team identity must not be committed before capacity validation"
    );
    assert_eq!(count_rows(&catalog.connection, "ops_team_members")?, 0);
    assert_eq!(count_rows(&catalog.connection, "ops_team_requests")?, 0);
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_capacity_events")?,
        0
    );
    Ok(())
}

#[test]
fn orchestrator_cannot_lower_team_capacity_below_committed_membership() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let created = catalog
        .create_team_with_capacity(create_team_command(0xF102)?, TeamMemberCapacity::new(4)?)?;
    let original_revision = created.team().revision();
    let command = TeamCapacitySetCommand::new(
        Uuid::from_u128(0xF103),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(original_revision, 1, 1),
        TeamMemberCapacity::new(1)?,
        orchestrator(),
    );

    let error = catalog.set_team_capacity(&command, || {
        TimestampMillis::new(20).map_err(|_| OperationsError::Conflict("test timestamp"))
    });
    let Err(error) = error else {
        return Err("capacity cannot be reduced beneath committed Team members".into());
    };
    assert!(matches!(error, OperationsError::Bounds(_)));
    let current_capacity = catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("created Team lost its explicit capacity")?;
    assert_eq!(current_capacity.max_members().get(), 4);
    let current_team = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("created Team lost its authoritative record")?;
    assert_eq!(current_team.revision(), original_revision);
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_capacity_events")?,
        1
    );
    Ok(())
}

#[test]
fn capacity_change_cas_keeps_the_original_receipt_after_a_later_generation() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let created = catalog
        .create_team_with_capacity(create_team_command(0xF110)?, TeamMemberCapacity::new(4)?)?;
    let first_request = Uuid::from_u128(0xF111);
    let first_command = TeamCapacitySetCommand::new(
        first_request,
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(created.team().revision(), 1, 1),
        TeamMemberCapacity::new(3)?,
        orchestrator(),
    );
    let first = catalog.set_team_capacity(&first_command, || timestamp(20))?;
    assert_eq!(first.snapshot().max_members().get(), 3);
    assert_eq!(first.snapshot().revision(), 2);

    let second = catalog.set_team_capacity(
        &TeamCapacitySetCommand::new(
            Uuid::from_u128(0xF112),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(
                first.result_team_revision(),
                first.snapshot().revision(),
                1,
            ),
            TeamMemberCapacity::new(4)?,
            orchestrator(),
        ),
        || timestamp(30),
    )?;
    assert_eq!(second.snapshot().max_members().get(), 4);
    assert_eq!(
        catalog
            .team_capacity(WORKSPACE, TEAM_ID)?
            .map(|snapshot| snapshot.revision()),
        Some(3)
    );

    let persisted = catalog
        .team_capacity_set_receipt(WORKSPACE, first_request, orchestrator())?
        .ok_or("capacity generation receipt disappeared")?;
    assert_eq!(persisted.snapshot().max_members().get(), 3);
    assert_eq!(persisted.snapshot().revision(), 2);
    let sampled_clock = Cell::new(false);
    let replay = catalog.set_team_capacity(&first_command, || {
        sampled_clock.set(true);
        timestamp(999)
    })?;
    assert!(replay.replayed());
    assert_eq!(replay.snapshot().max_members().get(), 3);
    assert!(
        !sampled_clock.get(),
        "exact replay precedes clock/quota work"
    );
    Ok(())
}

fn assert_team_expansion_request_replay(
    catalog: &mut OperationsCatalog,
    request_command: &super::TeamExpansionRequestCommand,
    request: &super::TeamExpansionRequestReceipt,
) -> TestResult {
    assert_eq!(request.requested_count(), 2);
    let request_clock_sampled = Cell::new(false);
    let request_replay = catalog.request_team_expansion(request_command, || {
        request_clock_sampled.set(true);
        timestamp(999)
    })?;
    assert!(request_replay.replayed());
    assert_eq!(request_replay.request_id(), request.request_id());
    assert_eq!(request_replay.team_id(), request.team_id());
    assert_eq!(request_replay.requested_count(), request.requested_count());
    assert_eq!(request_replay.recorded_at(), request.recorded_at());
    assert!(
        !request_clock_sampled.get(),
        "exact Team Leader request replay must precede cooldown clock work"
    );
    Ok(())
}

fn assert_pending_expansion_decision_replay(
    catalog: &mut OperationsCatalog,
    decision_command: &TeamExpansionDecisionCommand,
    pending: &super::TeamExpansionDecisionReceipt,
) -> TestResult {
    assert_eq!(
        pending.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    assert_eq!(pending.approved_count(), 1);
    assert_eq!(pending.cooldown_started_at(), None);
    assert_eq!(pending.cooldown_until(), None);
    let decision_clock_sampled = Cell::new(false);
    let pending_replay = catalog.decide_team_expansion_v2_test(decision_command, || {
        decision_clock_sampled.set(true);
        timestamp(999)
    })?;
    assert!(pending_replay.replayed());
    assert_eq!(pending_replay.decision_id(), pending.decision_id());
    assert_eq!(
        pending_replay.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        1
    );
    assert!(
        !decision_clock_sampled.get(),
        "decision replay must not create another hold or sample a new approval time"
    );
    Ok(())
}

fn assert_pending_hold_rejects_capacity_reduction_and_new_requests(
    catalog: &mut OperationsCatalog,
    team: &crate::TeamMutationRecord,
    requester: OperationalActor,
) -> TestResult {
    // The unanchored approval hold remains capacity. Lowering the configured
    // cap beneath JOINED + hold must fail without changing Team revision.
    let lower_cap = catalog.set_team_capacity(
        &TeamCapacitySetCommand::new(
            Uuid::from_u128(0xF302),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(
                team.team().revision(),
                team.team_capacity().ok_or("missing cap")?.revision(),
                1,
            ),
            TeamMemberCapacity::new(2)?,
            orchestrator(),
        ),
        || timestamp(220),
    );
    assert!(matches!(lower_cap, Err(OperationsError::Bounds(_))));
    assert_eq!(
        catalog
            .team(WORKSPACE, TEAM_ID)?
            .ok_or("missing Team")?
            .revision(),
        1
    );

    let blocked_request_id = Uuid::from_u128(0xF303);
    let blocked_request = expansion_request(0xF303, team, requester, 1)?;
    assert!(matches!(
        catalog.request_team_expansion(&blocked_request, || timestamp(230)),
        Err(OperationsError::Conflict(
            "Team expansion cooldown anchor pending"
        ))
    ));
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, blocked_request_id, requester)?,
        None
    );
    Ok(())
}

fn assert_cooldown_anchor_replay(
    catalog: &mut OperationsCatalog,
    request: &super::TeamExpansionRequestReceipt,
    pending: &super::TeamExpansionDecisionReceipt,
    anchored: &super::TeamExpansionDecisionReceipt,
) -> TestResult {
    let sampled_clock = Cell::new(false);
    let replay = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        2,
        fourth_participant(),
        || {
            sampled_clock.set(true);
            timestamp(999)
        },
    )?;
    assert!(replay.replayed());
    assert_eq!(replay.cooldown_until(), anchored.cooldown_until());
    assert!(
        !sampled_clock.get(),
        "anchor replay must not extend the cooldown"
    );
    Ok(())
}

#[test]
fn approved_hold_waits_for_post_commit_anchor_and_survives_orchestrator_change() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request_command = expansion_request(0xF300, &team, requester, 2)?;
    let request = catalog.request_team_expansion(&request_command, || timestamp(100))?;
    assert_team_expansion_request_replay(&mut catalog, &request_command, &request)?;

    let decision_command = decide_command(
        Uuid::from_u128(0xF301),
        &request,
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator(),
        1,
    );
    let pending = catalog.decide_team_expansion_v2_test(&decision_command, || timestamp(200))?;
    assert_pending_expansion_decision_replay(&mut catalog, &decision_command, &pending)?;
    assert_pending_hold_rejects_capacity_reduction_and_new_requests(
        &mut catalog,
        &team,
        requester,
    )?;

    // A new Workspace Orchestrator finalizes only the existing immutable
    // decision. This is synthetic leadership owner data, not a live election.
    replace_workspace_orchestrator(&catalog.connection, MEMBER_FOUR_PARTICIPANT)?;
    let anchored = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        2,
        fourth_participant(),
        || timestamp(300),
    )?;
    assert_eq!(anchored.state(), TeamExpansionDecisionState::Approved);
    assert_eq!(anchored.cooldown_started_at(), Some(timestamp(300)?));
    assert_eq!(
        anchored.cooldown_until().map(TimestampMillis::get),
        Some(3_600_300)
    );
    assert_eq!(anchored.actor().participant_id(), ORCHESTRATOR_PARTICIPANT);
    assert_eq!(
        anchored
            .finalized_by()
            .map(super::TeamExpansionActor::participant_id),
        Some(MEMBER_FOUR_PARTICIPANT)
    );
    assert_cooldown_anchor_replay(&mut catalog, &request, &pending, &anchored)?;
    Ok(())
}

#[test]
fn anchored_approval_blocks_new_requests_until_the_exact_until_boundary() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request = catalog
        .request_team_expansion(&expansion_request(0xF310, &team, requester, 2)?, || {
            timestamp(100)
        })?;
    let pending = catalog.decide_team_expansion_v2_test(
        &decide_command(
            Uuid::from_u128(0xF311),
            &request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        || timestamp(200),
    )?;
    let anchored = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    let cooldown_until = anchored
        .cooldown_until()
        .ok_or("approved cooldown anchor missing")?;

    let next_request_id = Uuid::from_u128(0xF312);
    let next = expansion_request(0xF312, &team, requester, 1)?;
    assert!(matches!(
        catalog.request_team_expansion(&next, || timestamp(cooldown_until.get() - 1)),
        Err(OperationsError::Conflict("Team expansion cooldown active"))
    ));
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, next_request_id, requester)?,
        None
    );
    let admitted = catalog.request_team_expansion(&next, || Ok(cooldown_until))?;
    assert_eq!(admitted.request_id(), next_request_id);
    assert_eq!(admitted.recorded_at(), cooldown_until);
    Ok(())
}

#[test]
fn v2_readback_requires_the_exact_plan_and_v1_cannot_replay_it() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request = catalog
        .request_team_expansion(&expansion_request(0xF71A, &team, requester, 1)?, || {
            timestamp(100)
        })?;
    let base = decide_command(
        Uuid::from_u128(0xF71B),
        &request,
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator(),
        1,
    );
    let intent = super::TeamExpansionDecisionV2Command::new(
        base.clone(),
        vec![super::TeamExpansionApprovalSlotPlan::new(
            0,
            super::TeamExpansionWorkspaceTarget::NewProfile,
        )],
    );
    let receipt = catalog.decide_team_expansion_v2(&intent, || timestamp(200))?;
    assert_eq!(
        catalog.team_expansion_decision_v2_readback(&intent, orchestrator())?,
        Some(receipt)
    );
    assert!(
        catalog
            .team_expansion_decision(WORKSPACE, base.decision_id, requester)?
            .is_some()
    );
    let changed = super::TeamExpansionDecisionV2Command::new(
        base.clone(),
        vec![super::TeamExpansionApprovalSlotPlan::new(
            0,
            super::TeamExpansionWorkspaceTarget::ExistingWorkspaceProfile {
                profile_id: MEMBER_FIVE_PROFILE,
                participation_id: WorkspaceParticipationId::from_uuid(MEMBER_FIVE_PARTICIPATION)?,
            },
        )],
    );
    assert_eq!(
        catalog.team_expansion_decision_v2_readback(&intent, requester)?,
        None
    );
    assert_eq!(
        catalog.team_expansion_decision_v2_readback(&changed, requester)?,
        None
    );
    assert!(matches!(
        catalog.team_expansion_decision_v2_readback(&changed, orchestrator()),
        Err(OperationsError::Conflict(
            "Team expansion V2 decision readback mismatch"
        ))
    ));
    let clock_sampled = Cell::new(false);
    assert!(matches!(
        catalog.decide_team_expansion(&base, || {
            clock_sampled.set(true);
            timestamp(999)
        }),
        Err(OperationsError::Conflict("Team expansion decision replay"))
    ));
    assert!(!clock_sampled.get());
    Ok(())
}

#[test]
fn pre_hold_unused_slot_is_reserved_now_and_replays_after_orchestrator_change() -> TestResult {
    let (mut catalog, team, requester) =
        configured_requester_team_with_capacity(workspace_capacity_fixture()?, 5)?;
    let request = catalog
        .request_team_expansion(&expansion_request(0xF71C, &team, requester, 3)?, || {
            timestamp(100)
        })?;
    let decision_id = Uuid::from_u128(0xF71D);
    let pending = catalog.decide_team_expansion_v2(
        &super::TeamExpansionDecisionV2Command::new(
            decide_command(
                decision_id,
                &request,
                TeamExpansionDecision::Approve { approved_count: 3 },
                orchestrator(),
                1,
            ),
            vec![
                super::TeamExpansionApprovalSlotPlan::new(
                    0,
                    super::TeamExpansionWorkspaceTarget::NewProfile,
                ),
                super::TeamExpansionApprovalSlotPlan::new(
                    1,
                    super::TeamExpansionWorkspaceTarget::NewProfile,
                ),
                super::TeamExpansionApprovalSlotPlan::new(
                    2,
                    super::TeamExpansionWorkspaceTarget::NewProfile,
                ),
            ],
        ),
        || timestamp(200),
    )?;
    let anchored = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    convert_decision_to_pre_hold_fixture(
        &catalog.connection,
        decision_id,
        request.request_id(),
        TEAM_ID,
    )?;
    let current_team = catalog.team(WORKSPACE, TEAM_ID)?.ok_or("Team missing")?;
    let current_capacity = catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("capacity missing")?;
    let collision_revisions =
        TeamExpansionRevisions::new(current_team.revision(), current_capacity.revision(), 1);
    assert_recovery_id_collisions(
        &mut catalog,
        decision_id,
        request.request_id(),
        collision_revisions,
    )?;
    let intent = super::LegacySlotRecoveryIntent::new(
        Uuid::from_u128(0xF71E),
        TeamExpansionSlotRef::new(decision_id, 0),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(current_team.revision(), current_capacity.revision(), 1),
        MEMBER_FIVE_PROFILE,
        WorkspaceParticipationId::from_uuid(MEMBER_FIVE_PARTICIPATION)?,
    );
    assert!(matches!(
        catalog.recover_legacy_team_expansion_slot(
            &super::LegacySlotRecoveryCommand::new(intent, 1, orchestrator()),
            || timestamp(299),
        ),
        Err(OperationsError::Conflict(
            "Team expansion trusted clock regressed"
        ))
    ));
    let receipt = catalog.recover_legacy_team_expansion_slot(
        &super::LegacySlotRecoveryCommand::new(intent, 1, orchestrator()),
        || timestamp(400),
    )?;
    assert!(!receipt.replayed());
    continue_legacy_recovery_scenario(
        &mut catalog,
        &team,
        LegacyRecoveryContinuation {
            decision_id,
            current_team_revision: current_team.revision(),
            capacity_revision: current_capacity.revision(),
            intent,
            receipt,
            cooldown_started_at: anchored.cooldown_started_at(),
            cooldown_until: anchored.cooldown_until(),
        },
    )
}

fn assert_recovery_id_collisions(
    catalog: &mut OperationsCatalog,
    decision_id: Uuid,
    approval_request_id: Uuid,
    revisions: TeamExpansionRevisions,
) -> TestResult {
    for (request_id, expected) in [
        (decision_id, "legacy Team expansion recovery identity"),
        (
            approval_request_id,
            "legacy recovery request reuses approval identity",
        ),
    ] {
        let collision = super::LegacySlotRecoveryIntent::new(
            request_id,
            TeamExpansionSlotRef::new(decision_id, 0),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            revisions,
            MEMBER_FIVE_PROFILE,
            WorkspaceParticipationId::from_uuid(MEMBER_FIVE_PARTICIPATION)?,
        );
        let result = catalog.recover_legacy_team_expansion_slot(
            &super::LegacySlotRecoveryCommand::new(collision, 1, orchestrator()),
            || timestamp(350),
        );
        assert!(matches!(result, Err(OperationsError::Conflict(message)) if message == expected));
    }
    Ok(())
}

struct LegacyRecoveryContinuation {
    decision_id: Uuid,
    current_team_revision: u64,
    capacity_revision: u64,
    intent: super::LegacySlotRecoveryIntent,
    receipt: super::LegacySlotRecoveryReceipt,
    cooldown_started_at: Option<TimestampMillis>,
    cooldown_until: Option<TimestampMillis>,
}

fn continue_legacy_recovery_scenario(
    catalog: &mut OperationsCatalog,
    team: &crate::TeamMutationRecord,
    context: LegacyRecoveryContinuation,
) -> TestResult {
    let decision_id = context.decision_id;
    let intent = context.intent;
    let second_intent = super::LegacySlotRecoveryIntent::new(
        Uuid::from_u128(0xF71F),
        TeamExpansionSlotRef::new(decision_id, 1),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(context.current_team_revision, context.capacity_revision, 1),
        MEMBER_SIX_PROFILE,
        WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
    );
    let second_regression = catalog.recover_legacy_team_expansion_slot(
        &super::LegacySlotRecoveryCommand::new(second_intent, 1, orchestrator()),
        || timestamp(399),
    );
    assert!(
        matches!(
            &second_regression,
            Err(OperationsError::Conflict(
                "Team expansion trusted clock regressed"
            ))
        ),
        "unexpected second recovery result: {second_regression:?}"
    );
    catalog.recover_legacy_team_expansion_slot(
        &super::LegacySlotRecoveryCommand::new(second_intent, 1, orchestrator()),
        || timestamp(401),
    )?;
    assert_changed_recovery_is_rejected(catalog, &context)?;
    let slot = TeamExpansionSlotRef::new(decision_id, 0);
    assign_slot_to_profile(catalog, 0xF720, slot, team, fifth_participant(), 500)?;
    catalog.join_team(TeamJoinCommand {
        request_id: Uuid::from_u128(0xF721),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.team().revision(),
        actor: fifth_participant(),
        role: "recovered member".to_owned(),
        expansion_slot: Some(slot),
        observed_at: timestamp(501)?,
    })?;
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    let preserved = catalog
        .team_expansion_decision(WORKSPACE, decision_id, orchestrator())?
        .ok_or("recovered decision missing")?;
    assert_eq!(preserved.cooldown_started_at(), context.cooldown_started_at);
    assert_eq!(preserved.cooldown_until(), context.cooldown_until);
    catalog.leave_team(crate::TeamLeaveCommand {
        request_id: Uuid::from_u128(0xF722),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.team().revision() + 1,
        actor: fifth_participant(),
        observed_at: timestamp(502)?,
    })?;
    let after_leave = catalog.team(WORKSPACE, TEAM_ID)?.ok_or("Team missing")?;
    let reuse_intent = super::LegacySlotRecoveryIntent::new(
        Uuid::from_u128(0xF723),
        TeamExpansionSlotRef::new(decision_id, 2),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(after_leave.revision(), context.capacity_revision, 1),
        MEMBER_FIVE_PROFILE,
        WorkspaceParticipationId::from_uuid(MEMBER_FIVE_PARTICIPATION)?,
    );
    catalog.recover_legacy_team_expansion_slot(
        &super::LegacySlotRecoveryCommand::new(reuse_intent, 1, orchestrator()),
        || timestamp(503),
    )?;
    replace_workspace_orchestrator(&catalog.connection, MEMBER_FOUR_PARTICIPANT)?;
    let replay = catalog.recover_legacy_team_expansion_slot(
        &super::LegacySlotRecoveryCommand::new(intent, 2, fourth_participant()),
        || timestamp(999),
    )?;
    assert!(replay.replayed());
    assert_eq!(
        catalog.legacy_team_expansion_slot_recovery(&intent, orchestrator())?,
        Some(context.receipt)
    );
    assert_recovery_corruption_is_hidden_from_unrelated_reader(catalog, intent)?;
    Ok(())
}

fn assert_recovery_corruption_is_hidden_from_unrelated_reader(
    catalog: &mut OperationsCatalog,
    intent: super::LegacySlotRecoveryIntent,
) -> TestResult {
    catalog.connection.execute_batch(
        "DROP TRIGGER ops_team_expansion_legacy_slot_recoveries_no_update;
         PRAGMA ignore_check_constraints=ON;",
    )?;
    catalog.connection.execute(
        "UPDATE ops_team_expansion_legacy_slot_recoveries SET actor_session_id=X'01' WHERE request_id=?1",
        params![intent.request_id.as_bytes()],
    )?;
    assert_eq!(
        catalog.legacy_team_expansion_slot_recovery(&intent, sixth_participant())?,
        None
    );
    assert!(matches!(
        catalog.legacy_team_expansion_slot_recovery(&intent, fourth_participant()),
        Err(OperationsError::Corrupt(_))
    ));
    Ok(())
}

fn assert_changed_recovery_is_rejected(
    catalog: &mut OperationsCatalog,
    context: &LegacyRecoveryContinuation,
) -> TestResult {
    let changed = super::LegacySlotRecoveryIntent::new(
        context.intent.request_id,
        context.intent.slot,
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(context.current_team_revision, context.capacity_revision, 1),
        MEMBER_SIX_PROFILE,
        WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
    );
    assert!(matches!(
        catalog.legacy_team_expansion_slot_recovery(&changed, orchestrator()),
        Err(OperationsError::Conflict(
            "legacy Team expansion recovery readback mismatch"
        ))
    ));
    assert!(matches!(
        catalog.recover_legacy_team_expansion_slot(
            &super::LegacySlotRecoveryCommand::new(changed, 1, orchestrator()),
            || timestamp(999),
        ),
        Err(OperationsError::Conflict(
            "legacy Team expansion recovery replay"
        ))
    ));
    Ok(())
}

#[test]
fn concurrent_recovery_requests_reserve_one_legacy_slot_once() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let (mut setup, team, requester) =
        configured_requester_team_with_capacity(workspace_capacity_fixture_at(&database.path)?, 4)?;
    let request = setup
        .request_team_expansion(&expansion_request(0xF730, &team, requester, 2)?, || {
            timestamp(100)
        })?;
    let decision_id = Uuid::from_u128(0xF731);
    let pending = setup.decide_team_expansion_v2(
        &super::TeamExpansionDecisionV2Command::new(
            decide_command(
                decision_id,
                &request,
                TeamExpansionDecision::Approve { approved_count: 2 },
                orchestrator(),
                1,
            ),
            vec![
                super::TeamExpansionApprovalSlotPlan::new(
                    0,
                    super::TeamExpansionWorkspaceTarget::NewProfile,
                ),
                super::TeamExpansionApprovalSlotPlan::new(
                    1,
                    super::TeamExpansionWorkspaceTarget::NewProfile,
                ),
            ],
        ),
        || timestamp(200),
    )?;
    setup.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    convert_decision_to_pre_hold_fixture(
        &setup.connection,
        decision_id,
        request.request_id(),
        TEAM_ID,
    )?;
    let team_head = setup.team(WORKSPACE, TEAM_ID)?.ok_or("Team missing")?;
    let capacity = setup
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("capacity missing")?;
    drop(setup);
    let target_participation = WorkspaceParticipationId::from_uuid(MEMBER_FIVE_PARTICIPATION)?;
    let make_intent = |request_id| {
        super::LegacySlotRecoveryIntent::new(
            request_id,
            TeamExpansionSlotRef::new(decision_id, 0),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team_head.revision(), capacity.revision(), 1),
            MEMBER_FIVE_PROFILE,
            target_participation,
        )
    };
    let first_intent = make_intent(Uuid::from_u128(0xF732));
    let second_intent = make_intent(Uuid::from_u128(0xF733));
    let barrier = Arc::new(Barrier::new(3));
    let first = start_recovery_contender(&database.path, first_intent, Arc::clone(&barrier), 400);
    let second = start_recovery_contender(&database.path, second_intent, Arc::clone(&barrier), 401);
    barrier.wait();
    let results = [
        first.join().map_err(|_| "first recovery panicked")?,
        second.join().map_err(|_| "second recovery panicked")?,
    ];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    let check = crate::teams::tests::create_connection(Some(&database.path))?;
    assert_eq!(
        count_rows(&check, "ops_team_expansion_legacy_slot_recoveries")?,
        1
    );
    seed_pending_workspace_identities_to_eleven(&check)?;
    drop(check);
    race_zero_increment_recovery_with_registration(
        &database.path,
        super::LegacySlotRecoveryIntent::new(
            Uuid::from_u128(0xF738),
            TeamExpansionSlotRef::new(decision_id, 1),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team_head.revision(), capacity.revision(), 1),
            MEMBER_SIX_PROFILE,
            WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
        ),
    )?;
    Ok(())
}

#[test]
fn pre_v61_assigned_legacy_slot_replays_and_joins_without_recovery() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request = catalog
        .request_team_expansion(&expansion_request(0xF734, &team, requester, 1)?, || {
            timestamp(100)
        })?;
    let decision_id = Uuid::from_u128(0xF735);
    let pending = catalog.decide_team_expansion_v2_for_existing(
        &decide_command(
            decision_id,
            &request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        &[(MEMBER_FIVE_PROFILE, MEMBER_FIVE_PARTICIPATION)],
        || timestamp(200),
    )?;
    catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    let slot = TeamExpansionSlotRef::new(decision_id, 0);
    let assignment_command =
        build_slot_assignment_command(0xF736, slot, &team, fifth_participant())?;
    let assigned = catalog.assign_team_expansion_slot(&assignment_command, || timestamp(400))?;
    convert_decision_to_pre_hold_fixture(
        &catalog.connection,
        decision_id,
        request.request_id(),
        TEAM_ID,
    )?;
    let replay = catalog.assign_team_expansion_slot(&assignment_command, || timestamp(999))?;
    assert!(replay.replayed());
    assert_eq!(replay.profile_id(), assigned.profile_id());
    catalog.join_team(TeamJoinCommand {
        request_id: Uuid::from_u128(0xF737),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.team().revision(),
        actor: fifth_participant(),
        role: "legacy assigned member".to_owned(),
        expansion_slot: Some(slot),
        observed_at: timestamp(401)?,
    })?;
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    Ok(())
}

fn start_recovery_contender(
    path: &Path,
    intent: super::LegacySlotRecoveryIntent,
    barrier: Arc<Barrier>,
    recorded_at: i64,
) -> thread::JoinHandle<Result<super::LegacySlotRecoveryReceipt, OperationsError>> {
    let path = path.to_path_buf();
    thread::spawn(move || {
        let connection = crate::teams::tests::create_connection(Some(&path))
            .map_err(|_| OperationsError::Conflict("recovery connection"))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(OperationsError::from)?;
        let mut catalog = OperationsCatalog {
            connection,
            destination: DESTINATION,
        };
        barrier.wait();
        catalog.recover_legacy_team_expansion_slot(
            &super::LegacySlotRecoveryCommand::new(intent, 1, orchestrator()),
            || timestamp(recorded_at),
        )
    })
}

fn seed_pending_workspace_identities_to_eleven(connection: &Connection) -> TestResult {
    let mut used = super::workspace_hold::workspace_capacity_used(connection, WORKSPACE)?;
    let mut seed = 0_u128;
    while used < 11 {
        let participant = Uuid::from_u128(0xFA00 + seed);
        let session = Uuid::from_u128(0xFB00 + seed);
        connection.execute(
            "INSERT INTO ops_pending_participants(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind) VALUES(?1,?2,?3,0,NULL,?4,'worker',1)",
            params![
                WORKSPACE.into_uuid().as_bytes(),
                participant.as_bytes(),
                session.as_bytes(),
                format!("Pending {seed}"),
            ],
        )?;
        seed += 1;
        used = super::workspace_hold::workspace_capacity_used(connection, WORKSPACE)?;
    }
    assert_eq!(used, 11);
    Ok(())
}

fn race_zero_increment_recovery_with_registration(
    path: &Path,
    recovery_intent: super::LegacySlotRecoveryIntent,
) -> TestResult {
    let barrier = Arc::new(Barrier::new(3));
    let recovery = start_recovery_contender(path, recovery_intent, Arc::clone(&barrier), 500);
    let registration_path = path.to_path_buf();
    let registration_barrier = Arc::clone(&barrier);
    let registration = thread::spawn(move || -> Result<(), OperationsError> {
        let mut connection = crate::teams::tests::create_connection(Some(&registration_path))
            .map_err(|_| OperationsError::Conflict("registration connection"))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        registration_barrier.wait();
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let participant = Uuid::from_u128(0xFC00);
        super::workspace_hold::ensure_registration_capacity_with_holds(
            &transaction,
            WORKSPACE,
            None,
            participant,
        )?;
        transaction.execute(
            "INSERT INTO ops_pending_participants(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind) VALUES(?1,?2,?3,0,NULL,'Concurrent registration','worker',1)",
            params![
                WORKSPACE.into_uuid().as_bytes(),
                participant.as_bytes(),
                Uuid::from_u128(0xFC01).as_bytes(),
            ],
        )?;
        transaction.commit()?;
        Ok(())
    });
    barrier.wait();
    recovery.join().map_err(|_| "recovery thread panicked")??;
    registration
        .join()
        .map_err(|_| "registration thread panicked")??;
    let check_path = path.to_path_buf();
    let check = crate::teams::tests::create_connection(Some(&check_path))?;
    assert_eq!(
        super::workspace_hold::workspace_capacity_used(&check, WORKSPACE)?,
        12
    );
    assert_eq!(
        count_rows(&check, "ops_team_expansion_legacy_slot_recoveries")?,
        2
    );
    Ok(())
}

fn convert_decision_to_pre_hold_fixture(
    connection: &Connection,
    decision_id: Uuid,
    request_id: Uuid,
    team_id: Uuid,
) -> TestResult {
    connection.execute_batch(
        "DROP TRIGGER ops_team_expansion_workspace_hold_plans_no_delete;
         DROP TRIGGER ops_team_expansion_workspace_hold_manifests_no_delete;",
    )?;
    connection.execute(
        "DELETE FROM ops_team_expansion_workspace_hold_plans WHERE decision_id=?1",
        params![decision_id.as_bytes()],
    )?;
    connection.execute(
        "DELETE FROM ops_team_expansion_workspace_hold_manifests WHERE decision_id=?1",
        params![decision_id.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_pre_hold_decisions(decision_id,request_id,workspace_id,team_id,decision_state,classification) VALUES(?1,?2,?3,?4,'APPROVED','PRE_HOLD')",
        params![decision_id.as_bytes(),request_id.as_bytes(),WORKSPACE.into_uuid().as_bytes(),team_id.as_bytes()],
    )?;
    Ok(())
}

#[test]
fn only_the_current_team_leader_requests_and_only_the_workspace_orchestrator_decides() -> TestResult
{
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request_id = Uuid::from_u128(0xF330);
    let request_command = expansion_request(0xF330, &team, requester, 1)?;
    let outsider_request = expansion_request(0xF330, &team, sixth_participant(), 1)?;
    assert!(matches!(
        catalog.request_team_expansion(&outsider_request, || timestamp(100)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, request_id, sixth_participant())?,
        None
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_requests")?,
        0
    );
    let request = catalog.request_team_expansion(&request_command, || timestamp(100))?;

    let decision_id = Uuid::from_u128(0xF331);
    let wrong_decider = decide_command(
        decision_id,
        &request,
        TeamExpansionDecision::Approve { approved_count: 1 },
        requester,
        1,
    );
    assert!(matches!(
        catalog.decide_team_expansion_v2_test(&wrong_decider, || timestamp(200)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(
        catalog.team_expansion_decision(WORKSPACE, decision_id, sixth_participant())?,
        None
    );

    // An invalid oversized approval leaves no decision or capacity hold; the
    // exact same immutable request can be decided with a bounded amount.
    let oversized = decide_command(
        decision_id,
        &request,
        TeamExpansionDecision::Approve { approved_count: 2 },
        orchestrator(),
        1,
    );
    assert!(matches!(
        catalog.decide_team_expansion_v2_test(&oversized, || timestamp(200)),
        Err(OperationsError::Bounds(_))
    ));
    assert_eq!(
        catalog.team_expansion_decision(WORKSPACE, decision_id, orchestrator())?,
        None
    );
    let approved = catalog.decide_team_expansion_v2_test(
        &decide_command(
            decision_id,
            &request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        || timestamp(200),
    )?;
    assert_eq!(
        approved.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    Ok(())
}

#[test]
fn rejected_expansion_has_no_cooldown_or_capacity_hold() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request = catalog
        .request_team_expansion(&expansion_request(0xF315, &team, requester, 2)?, || {
            timestamp(100)
        })?;
    let rejected = catalog.decide_team_expansion_v2_test(
        &decide_command(
            Uuid::from_u128(0xF316),
            &request,
            TeamExpansionDecision::Reject,
            orchestrator(),
            1,
        ),
        || timestamp(200),
    )?;
    assert_eq!(rejected.state(), TeamExpansionDecisionState::Rejected);
    assert_eq!(rejected.approved_count(), 0);
    assert_eq!(rejected.cooldown_started_at(), None);
    assert_eq!(rejected.cooldown_until(), None);
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        0
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_cooldown_anchors")?,
        0
    );
    Ok(())
}

#[test]
fn a_preexisting_pending_request_cannot_be_approved_during_lock_but_can_at_expiry() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request_a = catalog
        .request_team_expansion(&expansion_request(0xF317, &team, requester, 1)?, || {
            timestamp(100)
        })?;
    let request_b = catalog
        .request_team_expansion(&expansion_request(0xF318, &team, requester, 1)?, || {
            timestamp(101)
        })?;
    let decision_a = catalog.decide_team_expansion_v2_for_existing(
        &decide_command(
            Uuid::from_u128(0xF319),
            &request_a,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        &[(MEMBER_FIVE_PROFILE, MEMBER_FIVE_PARTICIPATION)],
        || timestamp(200),
    )?;
    let anchored_a = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request_a.request_id(),
        decision_a.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    let until = anchored_a.cooldown_until().ok_or("anchor A missing")?;
    let decision_b_id = Uuid::from_u128(0xF31A);
    let decision_b = decide_command(
        decision_b_id,
        &request_b,
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator(),
        1,
    );

    assert!(matches!(
        catalog.decide_team_expansion_v2_test(&decision_b, || timestamp(until.get() - 1)),
        Err(OperationsError::Conflict("Team expansion cooldown active"))
    ));
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, request_b.request_id(), requester)?,
        Some(request_b.clone()),
        "the pending request remains available with its original identity"
    );
    assert_eq!(
        catalog.team_expansion_decision(WORKSPACE, decision_b_id, orchestrator())?,
        None,
        "blocked approval must not create a decision receipt or hold"
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        1
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        1
    );

    let approved_b = catalog.decide_team_expansion_v2_test(&decision_b, || Ok(until))?;
    assert_eq!(approved_b.decision_id(), decision_b_id);
    assert_eq!(
        approved_b.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    assert_eq!(approved_b.recorded_at(), until);
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        2
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        2
    );
    Ok(())
}

fn decision_command_at_current_team_revisions(
    decision_id: Uuid,
    request: &super::TeamExpansionRequestReceipt,
    team: &crate::TeamRecord,
    capacity: &super::TeamCapacitySnapshot,
) -> TeamExpansionDecisionCommand {
    TeamExpansionDecisionCommand::new(
        decision_id,
        request.request_id(),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
        request.requester_leadership_revision(),
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator(),
    )
}

fn assert_pending_approval_waits_for_expiry_and_current_revisions(
    catalog: &mut OperationsCatalog,
    request: &super::TeamExpansionRequestReceipt,
    requester: OperationalActor,
    decision_id: Uuid,
    current_command: &TeamExpansionDecisionCommand,
    cooldown_until: TimestampMillis,
) -> TestResult {
    assert!(matches!(
        catalog.decide_team_expansion_v2_test(current_command, || {
            TimestampMillis::new(cooldown_until.get() - 1)
                .map_err(|_| OperationsError::Conflict("test timestamp"))
        }),
        Err(OperationsError::Conflict("Team expansion cooldown active"))
    ));
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, request.request_id(), requester)?,
        Some(request.clone())
    );
    assert_eq!(
        catalog.team_expansion_decision(WORKSPACE, decision_id, orchestrator())?,
        None
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        1
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        1
    );

    let current_team = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing after B cooldown")?;
    let stale_capacity_command = TeamExpansionDecisionCommand::new(
        decision_id,
        request.request_id(),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(current_team.revision(), request.capacity_revision(), 1),
        request.requester_leadership_revision(),
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator(),
    );
    assert!(matches!(
        catalog.decide_team_expansion_v2_test(&stale_capacity_command, || Ok(cooldown_until)),
        Err(OperationsError::Conflict(
            "Team expansion decision capacity revision"
        ))
    ));
    assert_eq!(
        catalog.team_expansion_decision(WORKSPACE, decision_id, orchestrator())?,
        None
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        1
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        1
    );

    let stale_command = decide_command(
        decision_id,
        request,
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator(),
        1,
    );
    assert!(matches!(
        catalog.decide_team_expansion_v2_test(&stale_command, || Ok(cooldown_until)),
        Err(OperationsError::Conflict(
            "Team expansion decision Team revision"
        ))
    ));
    assert_eq!(
        catalog.team_expansion_decision(WORKSPACE, decision_id, orchestrator())?,
        None
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        1
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        1
    );
    Ok(())
}

struct PendingExpansionAfterJoin {
    request: super::TeamExpansionRequestReceipt,
    cooldown_until: TimestampMillis,
    current_team: crate::TeamRecord,
    current_capacity: super::TeamCapacitySnapshot,
}

fn prepare_pending_expansion_after_first_join(
    catalog: &mut OperationsCatalog,
    team: &crate::TeamMutationRecord,
    requester: OperationalActor,
) -> Result<PendingExpansionAfterJoin, Box<dyn Error>> {
    let request_a = catalog
        .request_team_expansion(&expansion_request(0xF390, team, requester, 1)?, || {
            timestamp(100)
        })?;
    let request_b = catalog
        .request_team_expansion(&expansion_request(0xF391, team, requester, 1)?, || {
            timestamp(101)
        })?;
    let decision_a = catalog.decide_team_expansion_v2_for_existing(
        &decide_command(
            Uuid::from_u128(0xF392),
            &request_a,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        &[(MEMBER_FIVE_PROFILE, MEMBER_FIVE_PARTICIPATION)],
        || timestamp(200),
    )?;
    let anchored_a = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request_a.request_id(),
        decision_a.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    let cooldown_until = anchored_a.cooldown_until().ok_or("anchor A missing")?;
    let slot_a = TeamExpansionSlotRef::new(decision_a.decision_id(), 0);
    let assignment =
        assign_slot_to_profile(catalog, 0xF394, slot_a, team, fifth_participant(), 400)?;
    assert_eq!(assignment.profile_id(), MEMBER_FIVE_PROFILE);
    let joined = catalog.join_team(TeamJoinCommand {
        request_id: Uuid::from_u128(0xF395),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.team().revision(),
        actor: fifth_participant(),
        role: "approved member".to_owned(),
        expansion_slot: Some(slot_a),
        observed_at: timestamp(500)?,
    })?;
    assert_eq!(joined.team().revision(), 2);

    let prior_capacity = catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("Team capacity missing after A join")?;
    let cap_change = catalog.set_team_capacity(
        &TeamCapacitySetCommand::new(
            Uuid::from_u128(0xF396),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(joined.team().revision(), prior_capacity.revision(), 1),
            TeamMemberCapacity::new(5)?,
            orchestrator(),
        ),
        || timestamp(550),
    )?;
    let current_team = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing after O cap change")?;
    let current_capacity = catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("capacity missing after O cap change")?;
    assert_eq!(current_team.revision(), cap_change.result_team_revision());
    assert_eq!(
        current_capacity.revision(),
        cap_change.snapshot().revision()
    );
    Ok(PendingExpansionAfterJoin {
        request: request_b,
        cooldown_until,
        current_team,
        current_capacity,
    })
}

#[test]
fn pending_request_retries_with_current_revisions_after_join_and_cooldown() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let scenario = prepare_pending_expansion_after_first_join(&mut catalog, &team, requester)?;
    assert_eq!(scenario.request.expected_team_revision(), 1);
    assert_eq!(scenario.request.capacity_revision(), 1);

    let decision_id = Uuid::from_u128(0xF393);
    let current_command = decision_command_at_current_team_revisions(
        decision_id,
        &scenario.request,
        &scenario.current_team,
        &scenario.current_capacity,
    );
    assert_pending_approval_waits_for_expiry_and_current_revisions(
        &mut catalog,
        &scenario.request,
        requester,
        decision_id,
        &current_command,
        scenario.cooldown_until,
    )?;

    let approved =
        catalog.decide_team_expansion_v2_test(&current_command, || Ok(scenario.cooldown_until))?;
    assert_eq!(approved.decision_id(), decision_id);
    assert_eq!(approved.request_id(), scenario.request.request_id());
    assert_eq!(
        approved.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    assert_eq!(approved.recorded_at(), scenario.cooldown_until);
    assert_eq!(
        approved.expected_team_revision(),
        scenario.current_team.revision()
    );
    assert_eq!(
        approved.capacity_revision(),
        scenario.current_capacity.revision()
    );
    assert_eq!(scenario.request.expected_team_revision(), 1);
    assert_eq!(scenario.request.capacity_revision(), 1);
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, scenario.request.request_id(), requester)?,
        Some(scenario.request.clone())
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_requests")?,
        2
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        2
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        2
    );

    let mismatched_payload = TeamExpansionDecisionCommand::new(
        decision_id,
        scenario.request.request_id(),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(
            scenario.current_team.revision(),
            scenario.current_capacity.revision(),
            1,
        ),
        scenario.request.requester_leadership_revision(),
        TeamExpansionDecision::Reject,
        orchestrator(),
    );
    assert!(matches!(
        catalog.decide_team_expansion_v2_test(&mismatched_payload, || timestamp(999)),
        Err(OperationsError::Conflict("Team expansion decision replay"))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        2
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        2
    );
    Ok(())
}

struct SlotAssignmentTimeFence {
    cap_set_command: TeamCapacitySetCommand,
    cap_set_receipt: super::TeamCapacityReceipt,
    team_head: crate::TeamRecord,
    capacity_head: super::TeamCapacitySnapshot,
    regressed_at: TimestampMillis,
    source_counts: [i64; 6],
    team_counts: [i64; 6],
}

fn team_expansion_source_counts(catalog: &OperationsCatalog) -> Result<[i64; 6], Box<dyn Error>> {
    Ok([
        count_rows(&catalog.connection, "ops_team_capacity_events")?,
        count_rows(&catalog.connection, "ops_team_expansion_requests")?,
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        count_rows(&catalog.connection, "ops_team_expansion_slot_assignments")?,
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
    ])
}

fn prepare_slot_assignment_time_fence(
    catalog: &mut OperationsCatalog,
    created: &crate::TeamMutationRecord,
    requester: OperationalActor,
) -> Result<SlotAssignmentTimeFence, Box<dyn Error>> {
    let cap_set_command = TeamCapacitySetCommand::new(
        Uuid::from_u128(0xF630),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(created.team().revision(), 1, 1),
        TeamMemberCapacity::new(5)?,
        orchestrator(),
    );
    let cap_set_receipt = catalog.set_team_capacity(&cap_set_command, || timestamp(350))?;
    let team_after_cap_set = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing after capacity set")?;
    let capacity_after_cap_set = catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("capacity missing after capacity set")?;

    let request = catalog.request_team_expansion(
        &super::TeamExpansionRequestCommand::new(
            Uuid::from_u128(0xF631),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(
                team_after_cap_set.revision(),
                capacity_after_cap_set.revision(),
                1,
            ),
            1,
            "reserve one approved slot".to_owned(),
            requester,
        ),
        || timestamp(400),
    )?;
    let pending = catalog.decide_team_expansion_v2_for_existing(
        &decide_command(
            Uuid::from_u128(0xF632),
            &request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        &[(MEMBER_FIVE_PROFILE, MEMBER_FIVE_PARTICIPATION)],
        || timestamp(500),
    )?;
    let anchored = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(600),
    )?;
    let cooldown_until = anchored
        .cooldown_until()
        .ok_or("assignment anchor missing")?;
    let slot = TeamExpansionSlotRef::new(anchored.decision_id(), 0);
    let assignment = TeamExpansionSlotAssignmentCommand::new(
        Uuid::from_u128(0xF633),
        slot,
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(
            team_after_cap_set.revision(),
            capacity_after_cap_set.revision(),
            1,
        ),
        fifth_participant(),
        orchestrator(),
    );
    let assignment_at = 3_600_800;
    let regressed_at = timestamp(assignment_at - 1)?;
    assert!(regressed_at.get() >= cooldown_until.get());
    let assigned = catalog.assign_team_expansion_slot(&assignment, || timestamp(assignment_at))?;
    assert_eq!(assigned.recorded_at(), timestamp(assignment_at)?);
    Ok(SlotAssignmentTimeFence {
        cap_set_command,
        cap_set_receipt,
        team_head: team_after_cap_set,
        capacity_head: capacity_after_cap_set,
        regressed_at,
        source_counts: team_expansion_source_counts(catalog)?,
        team_counts: team_create_owner_mutation_counts(catalog)?,
    })
}

fn assert_exact_cap_replay_precedes_time_guard(
    catalog: &mut OperationsCatalog,
    fence: &SlotAssignmentTimeFence,
) -> TestResult {
    let sampled_clock = Cell::new(false);
    let exact_cap_replay = catalog.set_team_capacity(&fence.cap_set_command, || {
        sampled_clock.set(true);
        Ok(fence.regressed_at)
    })?;
    assert!(exact_cap_replay.replayed());
    assert_eq!(
        exact_cap_replay.request_id(),
        fence.cap_set_receipt.request_id()
    );
    assert_eq!(
        exact_cap_replay.snapshot(),
        fence.cap_set_receipt.snapshot()
    );
    assert!(
        !sampled_clock.get(),
        "exact cap replay precedes the clock guard"
    );
    assert_eq!(
        catalog.team(WORKSPACE, TEAM_ID)?,
        Some(fence.team_head.clone())
    );
    assert_eq!(
        catalog.team_capacity(WORKSPACE, TEAM_ID)?,
        Some(fence.capacity_head.clone())
    );
    assert_eq!(team_expansion_source_counts(catalog)?, fence.source_counts);
    assert_eq!(
        team_create_owner_mutation_counts(catalog)?,
        fence.team_counts
    );
    Ok(())
}

fn assert_new_writes_reject_assignment_time_regression(
    catalog: &mut OperationsCatalog,
    fence: &SlotAssignmentTimeFence,
    requester: OperationalActor,
) -> TestResult {
    let new_cap = TeamCapacitySetCommand::new(
        Uuid::from_u128(0xF634),
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(
            fence.team_head.revision(),
            fence.capacity_head.revision(),
            1,
        ),
        TeamMemberCapacity::new(6)?,
        orchestrator(),
    );
    assert!(matches!(
        catalog.set_team_capacity(&new_cap, || Ok(fence.regressed_at)),
        Err(OperationsError::Conflict(
            "Team expansion trusted clock regressed"
        ))
    ));

    let new_request_id = Uuid::from_u128(0xF635);
    let new_request = super::TeamExpansionRequestCommand::new(
        new_request_id,
        TeamExpansionScope::new(WORKSPACE, TEAM_ID),
        TeamExpansionRevisions::new(
            fence.team_head.revision(),
            fence.capacity_head.revision(),
            1,
        ),
        1,
        "earlier trusted request timestamp".to_owned(),
        requester,
    );
    assert!(matches!(
        catalog.request_team_expansion(&new_request, || Ok(fence.regressed_at)),
        Err(OperationsError::Conflict(
            "Team expansion trusted clock regressed"
        ))
    ));
    assert_eq!(
        catalog.team(WORKSPACE, TEAM_ID)?,
        Some(fence.team_head.clone())
    );
    assert_eq!(
        catalog.team_capacity(WORKSPACE, TEAM_ID)?,
        Some(fence.capacity_head.clone())
    );
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, new_request_id, requester)?,
        None
    );
    assert_eq!(team_expansion_source_counts(catalog)?, fence.source_counts);
    assert_eq!(
        team_create_owner_mutation_counts(catalog)?,
        fence.team_counts
    );
    Ok(())
}

#[test]
fn slot_assignment_time_fences_earlier_new_capacity_and_request_writes() -> TestResult {
    let (mut catalog, created, requester) = configured_requester_team()?;
    let fence = prepare_slot_assignment_time_fence(&mut catalog, &created, requester)?;
    assert_exact_cap_replay_precedes_time_guard(&mut catalog, &fence)?;
    assert_new_writes_reject_assignment_time_regression(&mut catalog, &fence, requester)?;
    Ok(())
}

fn assert_only_one_concurrent_approval_was_persisted(
    database_path: &PathBuf,
    first_result: Result<super::TeamExpansionDecisionReceipt, OperationsError>,
    second_result: Result<super::TeamExpansionDecisionReceipt, OperationsError>,
) -> TestResult {
    let successes = usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
    assert_eq!(
        successes, 1,
        "only one approval may reserve the final Team slot"
    );
    let losing_result = if first_result.is_err() {
        first_result
    } else {
        second_result
    };
    assert!(matches!(
        losing_result,
        Err(
            OperationsError::Conflict("Team expansion cooldown anchor is pending")
                | OperationsError::Bounds("Team expansion capacity")
        )
    ));

    let reopened = OperationsCatalog {
        connection: crate::teams::tests::create_connection(Some(database_path))?,
        destination: DESTINATION,
    };
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_requests")?,
        2,
        "both Leader requests remain historical even though one approval lost"
    );
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_decisions")?,
        1
    );
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_slots")?,
        1
    );
    assert_eq!(
        reopened
            .team_capacity(WORKSPACE, TEAM_ID)?
            .ok_or("Team cap missing")?
            .max_members()
            .get(),
        3
    );
    assert_eq!(count_rows(&reopened.connection, "ops_team_members")?, 2);
    drop(reopened);
    Ok(())
}

#[test]
fn concurrent_approvals_serialize_the_last_team_capacity_slot() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let (mut setup_catalog, team, requester) =
        configured_requester_team_with_capacity(workspace_capacity_fixture_at(&database.path)?, 3)?;
    let first_request = setup_catalog
        .request_team_expansion(&expansion_request(0xF401, &team, requester, 1)?, || {
            timestamp(100)
        })?;
    let second_request = setup_catalog
        .request_team_expansion(&expansion_request(0xF402, &team, requester, 1)?, || {
            timestamp(101)
        })?;
    assert_eq!(first_request.requested_count(), 1);
    assert_eq!(second_request.requested_count(), 1);
    drop(setup_catalog);

    let first_connection = crate::teams::tests::create_connection(Some(&database.path))?;
    let second_connection = crate::teams::tests::create_connection(Some(&database.path))?;
    first_connection.busy_timeout(Duration::from_secs(5))?;
    second_connection.busy_timeout(Duration::from_secs(5))?;
    let mut first_catalog = OperationsCatalog {
        connection: first_connection,
        destination: DESTINATION,
    };
    let mut second_catalog = OperationsCatalog {
        connection: second_connection,
        destination: DESTINATION,
    };
    let barrier = Arc::new(Barrier::new(3));
    let first_command = decide_command(
        Uuid::from_u128(0xF403),
        &first_request,
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator(),
        1,
    );
    let second_command = decide_command(
        Uuid::from_u128(0xF404),
        &second_request,
        TeamExpansionDecision::Approve { approved_count: 1 },
        orchestrator(),
        1,
    );
    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        first_catalog.decide_team_expansion_v2_test(&first_command, || timestamp(200))
    });
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        second_barrier.wait();
        second_catalog.decide_team_expansion_v2_test(&second_command, || timestamp(201))
    });
    barrier.wait();
    let first_result = first
        .join()
        .map_err(|_| IoError::other("first approval contender panicked"))?;
    let second_result = second
        .join()
        .map_err(|_| IoError::other("second approval contender panicked"))?;
    assert_only_one_concurrent_approval_was_persisted(&database.path, first_result, second_result)
}

#[test]
fn concurrent_team_approvals_reserve_one_stable_profile_once() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let mut setup = workspace_capacity_fixture_at(&database.path)?;
    let (request_a, request_b) = prepare_two_team_requests_for_profile_race(&mut setup)?;
    drop(setup);

    let barrier = Arc::new(Barrier::new(3));
    let first_barrier = Arc::clone(&barrier);
    let first_path = database.path.clone();
    let first = thread::spawn(move || {
        let connection = crate::teams::tests::create_connection(Some(&first_path))
            .map_err(|_| OperationsError::Conflict("first approval connection"))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(OperationsError::from)?;
        let mut catalog = OperationsCatalog {
            connection,
            destination: DESTINATION,
        };
        first_barrier.wait();
        catalog.decide_team_expansion_v2_for_existing(
            &decide_command_for(
                Uuid::from_u128(0xF40A),
                TEAM_ID,
                &request_a,
                TeamExpansionDecision::Approve { approved_count: 1 },
                orchestrator(),
                1,
            ),
            &[(MEMBER_SIX_PROFILE, MEMBER_SIX_PARTICIPATION)],
            || timestamp(200),
        )
    });
    let second_barrier = Arc::clone(&barrier);
    let second_path = database.path.clone();
    let second = thread::spawn(move || {
        let connection = crate::teams::tests::create_connection(Some(&second_path))
            .map_err(|_| OperationsError::Conflict("second approval connection"))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(OperationsError::from)?;
        let mut catalog = OperationsCatalog {
            connection,
            destination: DESTINATION,
        };
        second_barrier.wait();
        catalog.decide_team_expansion_v2_for_existing(
            &decide_command_for(
                Uuid::from_u128(0xF40B),
                SECOND_TEAM_ID,
                &request_b,
                TeamExpansionDecision::Approve { approved_count: 1 },
                orchestrator(),
                1,
            ),
            &[(MEMBER_SIX_PROFILE, MEMBER_SIX_PARTICIPATION)],
            || timestamp(201),
        )
    });
    barrier.wait();
    let results = [
        first.join().map_err(|_| "first approval panicked")?,
        second.join().map_err(|_| "second approval panicked")?,
    ];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert!(results.iter().any(|result| matches!(
        result,
        Err(OperationsError::Conflict(
            "Profile already has an active Team expansion hold"
        ))
    )));

    let reopened = OperationsCatalog {
        connection: crate::teams::tests::create_connection(Some(&database.path))?,
        destination: DESTINATION,
    };
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_decisions")?,
        1
    );
    assert_eq!(
        count_rows(
            &reopened.connection,
            "ops_team_expansion_workspace_hold_manifests"
        )?,
        1
    );
    assert_eq!(
        count_rows(
            &reopened.connection,
            "ops_team_expansion_workspace_hold_plans"
        )?,
        1
    );
    Ok(())
}

fn prepare_two_team_requests_for_profile_race(
    catalog: &mut OperationsCatalog,
) -> Result<
    (
        super::TeamExpansionRequestReceipt,
        super::TeamExpansionRequestReceipt,
    ),
    Box<dyn Error>,
> {
    let team_a_members = [
        (
            actor(MEMBER_TWO_ACTIVITY, MEMBER_TWO_AGENT, MEMBER_TWO_SESSION),
            "Leader",
        ),
        (
            actor(
                MEMBER_THREE_ACTIVITY,
                MEMBER_THREE_AGENT,
                MEMBER_THREE_SESSION,
            ),
            "Member",
        ),
    ];
    let team_a = catalog.create_team_with_capacity(
        create_team_command_with_roster(0xF405, TEAM_ID, &team_a_members)?,
        TeamMemberCapacity::new(4)?,
    )?;
    seed_team_leader(&catalog.connection, TEAM_ID, MEMBER_TWO_PARTICIPANT)?;
    let team_b = create_second_team_with_capacity(catalog)?;
    let request_a = catalog.request_team_expansion(
        &expansion_request_for(
            0xF406,
            TEAM_ID,
            &team_a,
            actor(MEMBER_TWO_ACTIVITY, MEMBER_TWO_AGENT, MEMBER_TWO_SESSION),
            1,
        )?,
        || timestamp(100),
    )?;
    let request_b = catalog.request_team_expansion(
        &expansion_request_for(0xF407, SECOND_TEAM_ID, &team_b, fifth_participant(), 1)?,
        || timestamp(101),
    )?;
    Ok((request_a, request_b))
}

#[test]
fn one_approved_slot_can_be_assigned_to_one_exact_ready_profile_and_read_back() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request = catalog
        .request_team_expansion(&expansion_request(0xF320, &team, requester, 2)?, || {
            timestamp(100)
        })?;
    let decision_command = decide_command(
        Uuid::from_u128(0xF321),
        &request,
        TeamExpansionDecision::Approve { approved_count: 2 },
        orchestrator(),
        1,
    );
    let pending = catalog.decide_team_expansion_v2(
        &super::TeamExpansionDecisionV2Command::new(
            decision_command,
            vec![
                super::TeamExpansionApprovalSlotPlan::new(
                    0,
                    super::TeamExpansionWorkspaceTarget::ExistingWorkspaceProfile {
                        profile_id: MEMBER_FIVE_PROFILE,
                        participation_id: WorkspaceParticipationId::from_uuid(
                            MEMBER_FIVE_PARTICIPATION,
                        )?,
                    },
                ),
                super::TeamExpansionApprovalSlotPlan::new(
                    1,
                    super::TeamExpansionWorkspaceTarget::NewProfile,
                ),
            ],
        ),
        || timestamp(200),
    )?;
    let decision = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    let slot = TeamExpansionSlotRef::new(decision.decision_id(), 0);
    let command = build_slot_assignment_command(0xF322, slot, &team, fifth_participant())?;
    let assigned = catalog.assign_team_expansion_slot(&command, || timestamp(400))?;
    assert_eq!(assigned.slot(), slot);
    assert_eq!(
        assigned.participation_id().into_uuid(),
        MEMBER_FIVE_PARTICIPATION
    );
    assert_eq!(assigned.profile_id(), MEMBER_FIVE_PROFILE);
    assert!(!assigned.replayed());

    assert_eq!(
        catalog.team_expansion_slot_assignment(WORKSPACE, slot, orchestrator())?,
        Some(assigned.clone())
    );
    assert_eq!(
        catalog.team_expansion_slot_assignment(WORKSPACE, slot, fifth_participant())?,
        Some(assigned)
    );

    let reused_for_other_target =
        build_slot_assignment_command(0xF323, slot, &team, sixth_participant())?;
    assert!(matches!(
        catalog.assign_team_expansion_slot(&reused_for_other_target, || timestamp(500)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_assignments")?,
        1,
        "one approved ordinal must retain one exact target"
    );
    Ok(())
}

#[test]
fn unrelated_profile_cannot_distinguish_existing_from_missing_exact_read_ids() -> TestResult {
    let (mut catalog, created, requester) = configured_requester_team()?;
    let fixture = prepare_exact_read_fixture(&mut catalog, &created, requester)?;

    // Synthetic Workspace leadership replacement lets the original cap
    // issuer and the new current Orchestrator exercise separate read grants.
    replace_workspace_orchestrator(&catalog.connection, MEMBER_FOUR_PARTICIPANT)?;
    assert_unrelated_request_and_decision_reads_are_hidden(&catalog, &fixture)?;
    assert_unrelated_capacity_and_assignment_reads_are_hidden(&catalog, &fixture)?;
    assert_exact_reads_remain_available(&catalog, &fixture)?;
    assert_authorized_corruption_is_reported(&mut catalog, &fixture)?;
    Ok(())
}

struct ExactReadFixture {
    capacity_request_id: Uuid,
    capacity_receipt: super::TeamCapacityReceipt,
    request_id: Uuid,
    request: super::TeamExpansionRequestReceipt,
    decision_id: Uuid,
    decision: super::TeamExpansionDecisionReceipt,
    slot: TeamExpansionSlotRef,
    assignment: super::TeamExpansionSlotAssignmentReceipt,
    requester: OperationalActor,
}

fn prepare_exact_read_fixture(
    catalog: &mut OperationsCatalog,
    created: &crate::TeamMutationRecord,
    requester: OperationalActor,
) -> Result<ExactReadFixture, Box<dyn Error>> {
    let capacity_request_id = Uuid::from_u128(0xF541);
    let created_capacity = created
        .team_capacity()
        .ok_or("created Team capacity missing")?;
    let capacity_receipt = catalog.set_team_capacity(
        &TeamCapacitySetCommand::new(
            capacity_request_id,
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(created.team().revision(), created_capacity.revision(), 1),
            TeamMemberCapacity::new(5)?,
            orchestrator(),
        ),
        || timestamp(20),
    )?;
    assert_eq!(
        capacity_receipt.actor().participant_id(),
        ORCHESTRATOR_PARTICIPANT
    );

    let team = catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team disappeared after cap setting")?;
    let capacity = catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("Team capacity disappeared after cap setting")?;
    let request_id = Uuid::from_u128(0xF542);
    let request = catalog.request_team_expansion(
        &super::TeamExpansionRequestCommand::new(
            request_id,
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
            1,
            "verify exact-read privacy".to_owned(),
            requester,
        ),
        || timestamp(100),
    )?;
    let decision_id = Uuid::from_u128(0xF543);
    let pending = catalog.decide_team_expansion_v2_for_existing(
        &decide_command(
            decision_id,
            &request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        &[(MEMBER_FIVE_PROFILE, MEMBER_FIVE_PARTICIPATION)],
        || timestamp(200),
    )?;
    let decision = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request_id,
        decision_id,
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    assert_eq!(decision.decision_id(), pending.decision_id());
    let slot = TeamExpansionSlotRef::new(decision_id, 0);
    let assignment = catalog.assign_team_expansion_slot(
        &TeamExpansionSlotAssignmentCommand::new(
            Uuid::from_u128(0xF544),
            slot,
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
            fifth_participant(),
            orchestrator(),
        ),
        || timestamp(400),
    )?;
    assert_eq!(assignment.profile_id(), MEMBER_FIVE_PROFILE);
    Ok(ExactReadFixture {
        capacity_request_id,
        capacity_receipt,
        request_id,
        request,
        decision_id,
        decision,
        slot,
        assignment,
        requester,
    })
}

fn assert_unrelated_request_and_decision_reads_are_hidden(
    catalog: &OperationsCatalog,
    fixture: &ExactReadFixture,
) -> TestResult {
    let unrelated = sixth_participant();
    let missing_request_id = Uuid::from_u128(0xF551);
    let missing_decision_id = Uuid::from_u128(0xF552);

    let hidden_request =
        catalog.team_expansion_request(WORKSPACE, fixture.request_id, unrelated)?;
    let missing_request =
        catalog.team_expansion_request(WORKSPACE, missing_request_id, unrelated)?;
    assert_eq!(hidden_request, None);
    assert_eq!(hidden_request, missing_request);

    let hidden_decision =
        catalog.team_expansion_decision(WORKSPACE, fixture.decision_id, unrelated)?;
    let missing_decision =
        catalog.team_expansion_decision(WORKSPACE, missing_decision_id, unrelated)?;
    assert_eq!(hidden_decision, None);
    assert_eq!(hidden_decision, missing_decision);
    Ok(())
}

fn assert_unrelated_capacity_and_assignment_reads_are_hidden(
    catalog: &OperationsCatalog,
    fixture: &ExactReadFixture,
) -> TestResult {
    let unrelated = sixth_participant();
    let missing_capacity_request_id = Uuid::from_u128(0xF553);
    let missing_slot = TeamExpansionSlotRef::new(Uuid::from_u128(0xF554), 0);
    let hidden_capacity =
        catalog.team_capacity_set_receipt(WORKSPACE, fixture.capacity_request_id, unrelated)?;
    let missing_capacity =
        catalog.team_capacity_set_receipt(WORKSPACE, missing_capacity_request_id, unrelated)?;
    assert_eq!(hidden_capacity, None);
    assert_eq!(hidden_capacity, missing_capacity);

    let hidden_assignment =
        catalog.team_expansion_slot_assignment(WORKSPACE, fixture.slot, unrelated)?;
    let missing_assignment =
        catalog.team_expansion_slot_assignment(WORKSPACE, missing_slot, unrelated)?;
    assert_eq!(hidden_assignment, None);
    assert_eq!(hidden_assignment, missing_assignment);
    Ok(())
}

fn assert_exact_reads_remain_available(
    catalog: &OperationsCatalog,
    fixture: &ExactReadFixture,
) -> TestResult {
    let current_orchestrator = fourth_participant();
    // Original request author and requester retain their exact records.
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, fixture.request_id, fixture.requester)?,
        Some(fixture.request.clone())
    );
    assert_eq!(
        catalog.team_expansion_decision(WORKSPACE, fixture.decision_id, fixture.requester)?,
        Some(fixture.decision.clone())
    );
    // The original cap issuer remains authorized; the current Orchestrator
    // can read every record in the Workspace.
    assert_eq!(
        catalog.team_capacity_set_receipt(
            WORKSPACE,
            fixture.capacity_request_id,
            orchestrator(),
        )?,
        Some(fixture.capacity_receipt.clone())
    );
    assert_eq!(
        catalog.team_capacity_set_receipt(
            WORKSPACE,
            fixture.capacity_request_id,
            current_orchestrator,
        )?,
        Some(fixture.capacity_receipt.clone())
    );
    assert_eq!(
        catalog.team_expansion_request(WORKSPACE, fixture.request_id, current_orchestrator)?,
        Some(fixture.request.clone())
    );
    assert_eq!(
        catalog.team_expansion_decision(WORKSPACE, fixture.decision_id, current_orchestrator)?,
        Some(fixture.decision.clone())
    );
    assert_eq!(
        catalog.team_expansion_slot_assignment(WORKSPACE, fixture.slot, fifth_participant())?,
        Some(fixture.assignment.clone())
    );
    assert_eq!(
        catalog.team_expansion_slot_assignment(WORKSPACE, fixture.slot, current_orchestrator)?,
        Some(fixture.assignment.clone())
    );
    Ok(())
}

fn assert_authorized_corruption_is_reported(
    catalog: &mut OperationsCatalog,
    fixture: &ExactReadFixture,
) -> TestResult {
    // Corruption is injected only after authorization fixtures are established;
    // the exact requester must receive a corruption error, never an empty result.
    catalog.connection.execute_batch(
        "DROP TRIGGER ops_team_expansion_requests_no_update; PRAGMA ignore_check_constraints=ON;",
    )?;
    let changed = catalog.connection.execute(
        "UPDATE ops_team_expansion_requests SET requested_count=0 WHERE workspace_id=?1 AND request_id=?2",
        params![WORKSPACE.into_uuid().as_bytes(), fixture.request_id.as_bytes()],
    )?;
    assert_eq!(changed, 1);
    assert!(matches!(
        catalog.team_expansion_request(WORKSPACE, fixture.request_id, fixture.requester),
        Err(OperationsError::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn immutable_slot_plans_reject_retargeting_and_survive_other_slot_consumption() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let teams = prepare_two_team_approved_slots(&mut catalog, 0xF200, 0xF360)?;
    let assignment = assign_slot_to_profile(
        &mut catalog,
        0xF364,
        teams.slot_a,
        &teams.team_a,
        sixth_participant(),
        130,
    )?;
    assert_eq!(assignment.profile_id(), MEMBER_SIX_PROFILE);
    assert_competing_alias_assignment_is_rejected(&mut catalog, &teams)?;
    assert_initial_roster_cannot_bypass_assigned_profile(&mut catalog)?;
    join_and_leave_assigned_profile(&mut catalog, &teams)?;
    assert_slot_can_be_reassigned_after_leave(&mut catalog, &teams)?;
    Ok(())
}

fn assert_competing_alias_assignment_is_rejected(
    catalog: &mut OperationsCatalog,
    teams: &TwoTeamApprovedSlots,
) -> TestResult {
    let alias_target = actor(
        MEMBER_SIX_ALIAS_ACTIVITY,
        MEMBER_SIX_ALIAS_AGENT,
        MEMBER_SIX_ALIAS_SESSION,
    );
    let competing_alias =
        build_slot_assignment_command(0xF365, teams.slot_b, &teams.team_b, alias_target)?;
    assert!(matches!(
        catalog.assign_team_expansion_slot(&competing_alias, || timestamp(140)),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_assignments")?,
        1
    );
    Ok(())
}

fn assert_initial_roster_cannot_bypass_assigned_profile(
    catalog: &mut OperationsCatalog,
) -> TestResult {
    // A Team C roster cannot bypass an already assigned but unconsumed slot.
    let team_c_roster = [
        (sixth_participant(), "Member"),
        (eighth_participant(), "Member"),
    ];
    let before_teams = count_rows(&catalog.connection, "ops_teams")?;
    let before_members = count_rows(&catalog.connection, "ops_team_members")?;
    let create_team_c = catalog.create_team_with_capacity(
        create_team_command_with_roster(0xF366, BYPASS_TEAM_ID, &team_c_roster)?,
        TeamMemberCapacity::new(4)?,
    );
    assert!(matches!(create_team_c, Err(OperationsError::Conflict(_))));
    assert_eq!(count_rows(&catalog.connection, "ops_teams")?, before_teams);
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_members")?,
        before_members
    );
    Ok(())
}

fn join_and_leave_assigned_profile(
    catalog: &mut OperationsCatalog,
    teams: &TwoTeamApprovedSlots,
) -> TestResult {
    catalog.join_team(TeamJoinCommand {
        request_id: Uuid::from_u128(0xF367),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: teams.team_a.team().revision(),
        actor: sixth_participant(),
        role: "approved member".to_owned(),
        expansion_slot: Some(teams.slot_a),
        observed_at: timestamp(150)?,
    })?;
    let expected_revision = teams
        .team_a
        .team()
        .revision()
        .checked_add(1)
        .ok_or("Team revision overflow")?;
    catalog.leave_team(crate::TeamLeaveCommand {
        request_id: Uuid::from_u128(0xF368),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: expected_revision,
        actor: sixth_participant(),
        observed_at: timestamp(160)?,
    })?;
    Ok(())
}

fn assert_slot_can_be_reassigned_after_leave(
    catalog: &mut OperationsCatalog,
    teams: &TwoTeamApprovedSlots,
) -> TestResult {
    // Consuming/leaving the first slot does not rewrite the second approval's
    // immutable target plan.
    let reassign =
        build_slot_assignment_command(0xF369, teams.slot_b, &teams.team_b, eighth_participant())?;
    let reassigned = catalog.assign_team_expansion_slot(&reassign, || timestamp(170))?;
    assert_eq!(
        reassigned.participation_id().into_uuid(),
        MEMBER_EIGHT_PARTICIPATION
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_assignments")?,
        2
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    Ok(())
}

#[test]
fn concurrent_slot_assignment_cannot_override_the_approval_target() -> TestResult {
    // The Team rosters, Workspace Orchestrator, Team Leaders, and Profile
    // aliases below are synthetic Operations fixtures, not live election or
    // provisioning evidence.
    let database = TemporaryDatabase::new()?;
    let mut setup_catalog = workspace_capacity_fixture_at(&database.path)?;
    let teams = prepare_two_team_approved_slots(&mut setup_catalog, 0xF380, 0xF381)?;
    let direct_session_candidate = slot_race_candidate(
        0xF385,
        teams.slot_a,
        &teams.team_a,
        sixth_participant(),
        MEMBER_SIX_PARTICIPANT,
    )?;
    let target_alias = actor(
        MEMBER_SIX_ALIAS_ACTIVITY,
        MEMBER_SIX_ALIAS_AGENT,
        MEMBER_SIX_ALIAS_SESSION,
    );
    let alias_session_candidate = slot_race_candidate(
        0xF386,
        teams.slot_b,
        &teams.team_b,
        target_alias,
        MEMBER_SIX_ALIAS_PARTICIPANT,
    )?;
    drop(setup_catalog);

    let outcome = race_profile_slot_assignments(
        &database.path,
        &direct_session_candidate,
        &alias_session_candidate,
    )?;
    verify_winning_slot_join_and_home(&database.path, &outcome)
}

struct SlotRaceCandidate {
    command: TeamExpansionSlotAssignmentCommand,
    slot: TeamExpansionSlotRef,
    team_id: Uuid,
    team_revision: u64,
    target: OperationalActor,
    participant_id: Uuid,
}

fn slot_race_candidate(
    request_id: u128,
    slot: TeamExpansionSlotRef,
    team: &crate::TeamMutationRecord,
    target: OperationalActor,
    participant_id: Uuid,
) -> Result<SlotRaceCandidate, Box<dyn Error>> {
    Ok(SlotRaceCandidate {
        command: build_slot_assignment_command(request_id, slot, team, target)?,
        slot,
        team_id: team.team().team_id(),
        team_revision: team.team().revision(),
        target,
        participant_id,
    })
}

struct SlotAssignmentRaceParticipants<'a> {
    winner: &'a SlotRaceCandidate,
    loser: &'a SlotRaceCandidate,
}

struct SlotAssignmentRaceOutcome<'a> {
    winner_assignment: super::TeamExpansionSlotAssignmentReceipt,
    participants: SlotAssignmentRaceParticipants<'a>,
}

fn race_profile_slot_assignments<'a>(
    database_path: &PathBuf,
    first_contender: &'a SlotRaceCandidate,
    second_contender: &'a SlotRaceCandidate,
) -> Result<SlotAssignmentRaceOutcome<'a>, Box<dyn Error>> {
    let first_connection = crate::teams::tests::create_connection(Some(database_path))?;
    let second_connection = crate::teams::tests::create_connection(Some(database_path))?;
    first_connection.busy_timeout(Duration::from_secs(5))?;
    second_connection.busy_timeout(Duration::from_secs(5))?;
    let mut first_catalog = OperationsCatalog {
        connection: first_connection,
        destination: DESTINATION,
    };
    let mut second_catalog = OperationsCatalog {
        connection: second_connection,
        destination: DESTINATION,
    };
    let barrier = Arc::new(Barrier::new(3));
    let first_command = first_contender.command;
    let second_command = second_contender.command;
    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        first_catalog.assign_team_expansion_slot(&first_command, || timestamp(400))
    });
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        second_barrier.wait();
        second_catalog.assign_team_expansion_slot(&second_command, || timestamp(401))
    });
    barrier.wait();
    let first_result = first
        .join()
        .map_err(|_| IoError::other("first slot assignment contender panicked"))?;
    let second_result = second
        .join()
        .map_err(|_| IoError::other("second slot assignment contender panicked"))?;
    let successes = usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
    assert_eq!(
        successes, 1,
        "one stable Profile may hold only one unconsumed Workspace slot"
    );
    let (winner_assignment, losing_result, winner, loser) = match (first_result, second_result) {
        (Ok(winner_assignment), Err(error)) => {
            (winner_assignment, error, first_contender, second_contender)
        }
        (Err(error), Ok(winner_assignment)) => {
            (winner_assignment, error, second_contender, first_contender)
        }
        _ => return Err("expected exactly one successful slot assignment".into()),
    };
    assert!(matches!(losing_result, OperationsError::Conflict(_)));
    assert_eq!(winner_assignment.slot(), winner.slot);
    assert_eq!(winner_assignment.team_id(), winner.team_id);
    assert_eq!(
        winner_assignment.participation_id().into_uuid(),
        MEMBER_SIX_PARTICIPATION
    );
    assert_eq!(winner_assignment.profile_id(), MEMBER_SIX_PROFILE);
    assert_eq!(winner_assignment.participant_id(), winner.participant_id);
    Ok(SlotAssignmentRaceOutcome {
        winner_assignment,
        participants: SlotAssignmentRaceParticipants { winner, loser },
    })
}

fn assert_single_persisted_profile_home(catalog: &OperationsCatalog) -> TestResult {
    let joined_homes: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_team_members WHERE workspace_id=?1 AND participation_id=?2 AND state='JOINED'",
        params![WORKSPACE.into_uuid().as_bytes(), MEMBER_SIX_PARTICIPATION.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(
        joined_homes, 1,
        "the Profile has exactly one persisted home Team"
    );
    Ok(())
}

fn verify_winning_slot_join_and_home(
    database_path: &PathBuf,
    outcome: &SlotAssignmentRaceOutcome<'_>,
) -> TestResult {
    let mut reopened = OperationsCatalog {
        connection: crate::teams::tests::create_connection(Some(database_path))?,
        destination: DESTINATION,
    };
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_slot_assignments")?,
        1,
        "the losing Team has no assignment row"
    );
    assert_eq!(
        reopened.team_expansion_slot_assignment(
            WORKSPACE,
            outcome.participants.winner.slot,
            orchestrator(),
        )?,
        Some(outcome.winner_assignment.clone())
    );
    assert_eq!(
        reopened.team_expansion_slot_assignment(
            WORKSPACE,
            outcome.participants.loser.slot,
            orchestrator(),
        )?,
        None
    );

    let joined = reopened.join_team(TeamJoinCommand {
        request_id: Uuid::from_u128(0xF387),
        workspace_id: WORKSPACE,
        team_id: outcome.participants.winner.team_id,
        expected_team_revision: outcome.participants.winner.team_revision,
        actor: outcome.participants.winner.target,
        role: "approved member".to_owned(),
        expansion_slot: Some(outcome.participants.winner.slot),
        observed_at: timestamp(500)?,
    })?;
    assert_eq!(
        joined.expansion_slot(),
        Some(outcome.participants.winner.slot)
    );
    let membership = joined.membership().ok_or("winner join has no membership")?;
    assert_eq!(
        membership.participation_id().into_uuid(),
        MEMBER_SIX_PARTICIPATION
    );
    assert_eq!(membership.profile_id(), Some(MEMBER_SIX_PROFILE));
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    drop(reopened);

    let after_reopen = OperationsCatalog {
        connection: crate::teams::tests::create_connection(Some(database_path))?,
        destination: DESTINATION,
    };
    assert_single_persisted_profile_home(&after_reopen)?;
    assert_eq!(
        count_rows(
            &after_reopen.connection,
            "ops_team_expansion_slot_assignments"
        )?,
        1
    );
    assert_eq!(
        count_rows(&after_reopen.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    assert_eq!(
        after_reopen.team_expansion_slot_assignment(
            WORKSPACE,
            outcome.participants.winner.slot,
            orchestrator(),
        )?,
        Some(outcome.winner_assignment.clone())
    );
    assert_eq!(
        after_reopen.team_expansion_slot_assignment(
            WORKSPACE,
            outcome.participants.loser.slot,
            orchestrator()
        )?,
        None
    );
    Ok(())
}

#[test]
fn approved_slot_is_consumed_atomically_by_exact_target_join_and_cannot_be_reused() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let expansion_request = catalog
        .request_team_expansion(&expansion_request(0xF350, &team, requester, 1)?, || {
            timestamp(100)
        })?;
    let pending = catalog.decide_team_expansion_v2_for_existing(
        &decide_command(
            Uuid::from_u128(0xF351),
            &expansion_request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        &[(MEMBER_FIVE_PROFILE, MEMBER_FIVE_PARTICIPATION)],
        || timestamp(200),
    )?;
    assert_eq!(
        pending.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        expansion_request.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;

    let slot = TeamExpansionSlotRef::new(pending.decision_id(), 0);
    let assignment_command =
        build_slot_assignment_command(0xF352, slot, &team, fifth_participant())?;
    let assignment = catalog.assign_team_expansion_slot(&assignment_command, || timestamp(400))?;
    assert_eq!(
        assignment.participation_id().into_uuid(),
        MEMBER_FIVE_PARTICIPATION
    );
    assert_eq!(assignment.profile_id(), MEMBER_FIVE_PROFILE);
    assert_unapproved_ordinal_is_rejected_without_changing_hold(
        &mut catalog,
        &team,
        pending.decision_id(),
    )?;

    let join = TeamJoinCommand {
        request_id: Uuid::from_u128(0xF353),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.team().revision(),
        actor: fifth_participant(),
        role: "approved-member".to_owned(),
        expansion_slot: Some(slot),
        observed_at: timestamp(500)?,
    };
    let joined = catalog.join_team(join.clone())?;
    assert_eq!(joined.expansion_slot(), Some(slot));
    assert!(!joined.replayed());
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    assert_eq!(count_rows(&catalog.connection, "ops_team_members")?, 3);

    let replay = catalog.join_team(join.clone())?;
    assert!(replay.replayed());
    assert_eq!(replay.expansion_slot(), Some(slot));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    assert_eq!(count_rows(&catalog.connection, "ops_team_members")?, 3);

    let wrong_target = TeamJoinCommand {
        request_id: Uuid::from_u128(0xF354),
        actor: sixth_participant(),
        observed_at: timestamp(510)?,
        ..join
    };
    assert!(matches!(
        catalog.join_team(wrong_target),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    assert_eq!(count_rows(&catalog.connection, "ops_team_members")?, 3);
    Ok(())
}

fn assert_unapproved_ordinal_is_rejected_without_changing_hold(
    catalog: &mut OperationsCatalog,
    team: &crate::TeamMutationRecord,
    decision_id: Uuid,
) -> TestResult {
    let occupied_before = super::full_team_occupancy(&catalog.connection, WORKSPACE, TEAM_ID)?;
    assert_eq!(occupied_before, 3, "two joined members plus one held slot");
    let unapproved_ordinal = TeamExpansionSlotRef::new(decision_id, 1);
    let command =
        build_slot_assignment_command(0xF355, unapproved_ordinal, team, sixth_participant())?;
    assert!(matches!(
        catalog.assign_team_expansion_slot(&command, || timestamp(410)),
        Err(OperationsError::Conflict(
            "Team expansion slot is not approved"
        ))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slots")?,
        1,
        "ordinal one was not materialized from a one-slot approval"
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_assignments")?,
        1,
        "the rejected ordinal created no assignment"
    );
    assert_eq!(
        super::full_team_occupancy(&catalog.connection, WORKSPACE, TEAM_ID)?,
        occupied_before,
        "rejecting an unapproved ordinal leaves the existing hold unchanged"
    );
    Ok(())
}

struct ExistingHomeTransferFixture {
    source_team_id: Uuid,
    destination_a: crate::TeamMutationRecord,
    request_a: super::TeamExpansionRequestReceipt,
    request_b: super::TeamExpansionRequestReceipt,
}

fn fill_workspace_to_twelve(catalog: &OperationsCatalog) -> TestResult {
    let used =
        super::workspace_hold::workspace_capacity_snapshot(&catalog.connection, WORKSPACE)?.used();
    for ordinal in used..12 {
        let offset = u128::try_from(ordinal)?;
        crate::teams::tests::seed_participant_identity(
            &catalog.connection,
            WORKSPACE,
            Uuid::from_u128(0xA000 + offset),
            Uuid::from_u128(0xB000 + offset),
            Uuid::from_u128(0xC000 + offset),
            Uuid::from_u128(0xD000 + offset),
            Uuid::from_u128(0xE000 + offset),
            Uuid::from_u128(0xF000 + offset),
            Uuid::from_u128(0xFA00 + offset),
        )?;
    }
    assert_eq!(
        super::workspace_hold::workspace_capacity_snapshot(&catalog.connection, WORKSPACE)?.used(),
        12
    );
    Ok(())
}

fn prepare_existing_home_transfer_fixture(
    catalog: &mut OperationsCatalog,
) -> Result<ExistingHomeTransferFixture, Box<dyn Error>> {
    prepare_existing_home_transfer_fixture_with_successor(catalog, false)
}

fn prepare_existing_home_transfer_fixture_with_successor(
    catalog: &mut OperationsCatalog,
    include_successor: bool,
) -> Result<ExistingHomeTransferFixture, Box<dyn Error>> {
    let mut source_members = vec![
        (sixth_participant(), "Transfer target"),
        (seventh_participant(), "Source Leader"),
    ];
    if include_successor {
        source_members.push((fourth_participant(), "Successor"));
    }
    catalog.create_team_with_capacity(
        create_team_command_with_roster(0xFB00, TEAM_ID, &source_members)?,
        TeamMemberCapacity::new(4)?,
    )?;
    seed_team_leader(&catalog.connection, TEAM_ID, MEMBER_SEVEN_PARTICIPANT)?;

    let first_destination_roster = [
        (
            actor(MEMBER_TWO_ACTIVITY, MEMBER_TWO_AGENT, MEMBER_TWO_SESSION),
            "Leader",
        ),
        (
            actor(
                MEMBER_THREE_ACTIVITY,
                MEMBER_THREE_AGENT,
                MEMBER_THREE_SESSION,
            ),
            "Member",
        ),
    ];
    let destination_a = catalog.create_team_with_capacity(
        create_team_command_with_roster(0xFB01, SECOND_TEAM_ID, &first_destination_roster)?,
        TeamMemberCapacity::new(4)?,
    )?;
    seed_team_leader(&catalog.connection, SECOND_TEAM_ID, MEMBER_TWO_PARTICIPANT)?;

    let second_destination_roster = [
        (fifth_participant(), "Leader"),
        (eighth_participant(), "Member"),
    ];
    let destination_b = catalog.create_team_with_capacity(
        create_team_command_with_roster(0xFB02, BYPASS_TEAM_ID, &second_destination_roster)?,
        TeamMemberCapacity::new(4)?,
    )?;
    seed_team_leader(&catalog.connection, BYPASS_TEAM_ID, MEMBER_FIVE_PARTICIPANT)?;

    let request_a = catalog.request_team_expansion(
        &expansion_request_for(
            0xFB03,
            SECOND_TEAM_ID,
            &destination_a,
            actor(MEMBER_TWO_ACTIVITY, MEMBER_TWO_AGENT, MEMBER_TWO_SESSION),
            1,
        )?,
        || timestamp(100),
    )?;
    let request_b = catalog.request_team_expansion(
        &expansion_request_for(
            0xFB04,
            BYPASS_TEAM_ID,
            &destination_b,
            fifth_participant(),
            1,
        )?,
        || timestamp(101),
    )?;
    Ok(ExistingHomeTransferFixture {
        source_team_id: TEAM_ID,
        destination_a,
        request_a,
        request_b,
    })
}

fn existing_home_v3_command(
    decision_id: Uuid,
    destination_team_id: Uuid,
    request: &super::TeamExpansionRequestReceipt,
    source_team_id: Uuid,
    source_membership_revision: u64,
) -> Result<super::TeamExpansionDecisionV3Command, OperationsError> {
    Ok(super::TeamExpansionDecisionV3Command::new(
        decide_command_for(
            decision_id,
            destination_team_id,
            request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        vec![super::TeamExpansionApprovalSlotPlanV3::new(
            0,
            super::TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer {
                profile_id: MEMBER_SIX_PROFILE,
                participation_id: WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
                source_team_id,
                source_membership_revision,
            },
        )],
    ))
}

#[test]
fn v3_records_exact_existing_home_source_but_does_not_authorize_transfer() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let fixture = prepare_existing_home_transfer_fixture(&mut catalog)?;
    let command = existing_home_v3_command(
        Uuid::from_u128(0xFB05),
        SECOND_TEAM_ID,
        &fixture.request_a,
        fixture.source_team_id,
        1,
    )?;
    let pending = catalog.decide_team_expansion_v3(&command, || timestamp(200))?;
    assert_eq!(
        pending.state(),
        TeamExpansionDecisionState::ApprovedPendingAnchor
    );
    assert_eq!(
        catalog.team_expansion_decision_v3_readback(&command, orchestrator())?,
        Some(pending.clone())
    );
    assert!(
        catalog
            .decide_team_expansion_v3(&command, || timestamp(201))?
            .replayed()
    );

    let v2_attempt = super::TeamExpansionDecisionV2Command::new(
        command.decision().clone(),
        vec![super::TeamExpansionApprovalSlotPlan::new(
            0,
            super::TeamExpansionWorkspaceTarget::ExistingWorkspaceProfile {
                profile_id: MEMBER_SIX_PROFILE,
                participation_id: WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
            },
        )],
    );
    assert!(matches!(
        catalog.decide_team_expansion_v2(&v2_attempt, || timestamp(202)),
        Err(OperationsError::Conflict("Team expansion decision replay"))
    ));

    let changed = existing_home_v3_command(
        Uuid::from_u128(0xFB05),
        SECOND_TEAM_ID,
        &fixture.request_a,
        fixture.source_team_id,
        2,
    )?;
    assert!(matches!(
        catalog.team_expansion_decision_v3_readback(&changed, orchestrator()),
        Err(OperationsError::Conflict(
            "Team expansion V3 decision readback mismatch"
        ))
    ));

    let anchored = catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        fixture.request_a.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    let assignment = build_slot_assignment_command(
        0xFB0B,
        TeamExpansionSlotRef::new(anchored.decision_id(), 0),
        &fixture.destination_a,
        sixth_participant(),
    )?;
    assert!(matches!(
        catalog.assign_team_expansion_slot(&assignment, || timestamp(400)),
        Err(OperationsError::Conflict(
            "existing Home transfer requires atomic cutover"
        ))
    ));

    let source_membership: (String, i64) = catalog.connection.query_row(
        "SELECT state,revision FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            fixture.source_team_id.as_bytes(),
            MEMBER_SIX_PARTICIPATION.as_bytes(),
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(source_membership, ("JOINED".to_owned(), 1));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_assignments")?,
        0
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        0
    );
    Ok(())
}

#[test]
fn replacement_orchestrator_recovers_original_v3_plan_without_prior_context() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let fixture = prepare_existing_home_transfer_fixture(&mut catalog)?;
    let command = existing_home_v3_command(
        Uuid::from_u128(0xFB10),
        SECOND_TEAM_ID,
        &fixture.request_a,
        fixture.source_team_id,
        1,
    )?;
    let pending = catalog.decide_team_expansion_v3(&command, || timestamp(200))?;
    replace_workspace_orchestrator(&catalog.connection, MEMBER_FOUR_PARTICIPANT)?;
    let before = [
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        count_rows(
            &catalog.connection,
            "ops_team_expansion_workspace_hold_plans",
        )?,
        count_rows(
            &catalog.connection,
            "ops_team_expansion_existing_home_transfers",
        )?,
    ];

    let recovered = catalog
        .team_expansion_decision_v3_projection(
            WORKSPACE,
            pending.decision_id(),
            fourth_participant(),
        )?
        .ok_or("replacement Orchestrator V3 projection missing")?;
    assert_eq!(recovered.decision(), &pending);
    assert_eq!(recovered.slot_plan(), command.slot_plan());
    assert_eq!(recovered.original_issuer(), pending.actor());
    assert_eq!(
        catalog.team_expansion_decision_v3_projection(
            WORKSPACE,
            pending.decision_id(),
            fifth_participant(),
        )?,
        None
    );
    assert_eq!(
        catalog.team_expansion_decision_v3_projection(
            WORKSPACE,
            Uuid::from_u128(0xFB11),
            fifth_participant(),
        )?,
        None
    );
    assert_eq!(
        [
            count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
            count_rows(
                &catalog.connection,
                "ops_team_expansion_workspace_hold_plans"
            )?,
            count_rows(
                &catalog.connection,
                "ops_team_expansion_existing_home_transfers",
            )?,
        ],
        before
    );
    Ok(())
}

#[test]
fn v3_projection_hides_corruption_from_unauthorized_reader() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let fixture = prepare_existing_home_transfer_fixture(&mut catalog)?;
    let command = existing_home_v3_command(
        Uuid::from_u128(0xFB12),
        SECOND_TEAM_ID,
        &fixture.request_a,
        fixture.source_team_id,
        1,
    )?;
    let pending = catalog.decide_team_expansion_v3(&command, || timestamp(200))?;
    catalog.connection.execute_batch(
        "DROP TRIGGER ops_team_expansion_existing_home_transfers_no_delete;
         DELETE FROM ops_team_expansion_existing_home_transfers;",
    )?;
    assert_eq!(
        catalog.team_expansion_decision_v3_projection(
            WORKSPACE,
            pending.decision_id(),
            fifth_participant(),
        )?,
        None
    );
    assert!(matches!(
        catalog.team_expansion_decision_v3_projection(
            WORKSPACE,
            pending.decision_id(),
            orchestrator(),
        ),
        Err(OperationsError::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn v3_projection_returns_none_for_v2_decision_class() -> TestResult {
    let (mut catalog, team, requester) = configured_requester_team()?;
    let request = catalog
        .request_team_expansion(&expansion_request(0xFB13, &team, requester, 1)?, || {
            timestamp(100)
        })?;
    let pending = catalog.decide_team_expansion_v2_test(
        &decide_command(
            Uuid::from_u128(0xFB14),
            &request,
            TeamExpansionDecision::Approve { approved_count: 1 },
            orchestrator(),
            1,
        ),
        || timestamp(200),
    )?;
    assert_eq!(
        catalog.team_expansion_decision_v3_projection(
            WORKSPACE,
            pending.decision_id(),
            orchestrator(),
        )?,
        None
    );
    Ok(())
}

struct PermanentTransferIntentFixture {
    catalog: OperationsCatalog,
    source: ExistingHomeTransferFixture,
    proposal: crate::TeamPermanentTransferIntentCommand,
    intent: crate::TeamPermanentTransferIntentReceipt,
}

fn prepare_permanent_transfer_intent() -> Result<PermanentTransferIntentFixture, Box<dyn Error>> {
    prepare_permanent_transfer_intent_with_catalog(workspace_capacity_fixture()?)
}

fn prepare_permanent_transfer_intent_with_catalog(
    mut catalog: OperationsCatalog,
) -> Result<PermanentTransferIntentFixture, Box<dyn Error>> {
    let source = prepare_existing_home_transfer_fixture_with_successor(&mut catalog, true)?;
    let approval_command = existing_home_v3_command(
        Uuid::from_u128(0xFB20),
        SECOND_TEAM_ID,
        &source.request_a,
        source.source_team_id,
        1,
    )?;
    let pending = catalog.decide_team_expansion_v3(&approval_command, || timestamp(200))?;
    catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        source.request_a.request_id(),
        pending.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;

    let proposal = crate::TeamPermanentTransferIntentCommand::new(
        Uuid::from_u128(0xFB21),
        crate::TeamPermanentTransferScope::new(
            WORKSPACE,
            source.source_team_id,
            SECOND_TEAM_ID,
            WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
        ),
        crate::TeamPermanentTransferDestinationSlot::new(
            source.request_a.request_id(),
            pending.decision_id(),
            0,
        ),
        crate::TeamPermanentTransferRevisions::new(
            source.destination_a.team().revision(),
            1,
            1,
            1,
            1,
        ),
        orchestrator(),
    )?;
    let intent = catalog.propose_team_permanent_transfer(&proposal, || timestamp(400))?;
    Ok(PermanentTransferIntentFixture {
        catalog,
        source,
        proposal,
        intent,
    })
}

#[test]
fn permanent_transfer_records_proposal_and_two_consents_without_cutover() -> TestResult {
    let PermanentTransferIntentFixture {
        mut catalog,
        source,
        proposal,
        intent,
    } = prepare_permanent_transfer_intent()?;
    assert!(!intent.replayed());
    assert!(
        catalog
            .propose_team_permanent_transfer(&proposal, || timestamp(401))?
            .replayed()
    );

    let source_consent_command = crate::TeamPermanentTransferConsentCommand::new(
        Uuid::from_u128(0xFB22),
        WORKSPACE,
        intent.transfer_id(),
        crate::TeamPermanentTransferConsentKind::SourceHomeLeader,
        seventh_participant(),
    )?;
    let source_consent = catalog
        .record_team_permanent_transfer_consent(&source_consent_command, || timestamp(500))?;
    let target_acceptance = catalog.record_team_permanent_transfer_consent(
        &crate::TeamPermanentTransferConsentCommand::new(
            Uuid::from_u128(0xFB23),
            WORKSPACE,
            intent.transfer_id(),
            crate::TeamPermanentTransferConsentKind::TargetAgentAcceptance,
            sixth_participant(),
        )?,
        || timestamp(501),
    )?;
    let rebound_source_leader =
        assert_permanent_transfer_consent_readback(&mut catalog, &intent, &source_consent)?;
    let rebound_source_consent = crate::TeamPermanentTransferConsentCommand::new(
        source_consent.receipt_id(),
        WORKSPACE,
        intent.transfer_id(),
        crate::TeamPermanentTransferConsentKind::SourceHomeLeader,
        rebound_source_leader,
    )?;
    assert!(
        catalog
            .record_team_permanent_transfer_consent(&rebound_source_consent, || timestamp(502))?
            .replayed()
    );
    let changed_receipt = crate::TeamPermanentTransferConsentCommand::new(
        Uuid::from_u128(0xFB22),
        WORKSPACE,
        intent.transfer_id(),
        crate::TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        sixth_participant(),
    )?;
    assert!(matches!(
        catalog.record_team_permanent_transfer_consent(&changed_receipt, || timestamp(503)),
        Err(OperationsError::Conflict(
            "permanent Team transfer consent replay"
        ))
    ));
    let snapshot = catalog
        .read_team_permanent_transfer_snapshot(WORKSPACE, intent.transfer_id(), orchestrator())?
        .ok_or("permanent transfer snapshot missing")?;
    assert_eq!(snapshot.intent(), &intent);
    assert_eq!(snapshot.source_home_leader_consent(), Some(&source_consent));
    assert_eq!(snapshot.target_agent_acceptance(), Some(&target_acceptance));
    assert_eq!(
        catalog.read_team_permanent_transfer_snapshot(
            WORKSPACE,
            intent.transfer_id(),
            fifth_participant(),
        )?,
        None
    );
    let source_state: String = catalog.connection.query_row(
        "SELECT state FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            source.source_team_id.as_bytes(),
            MEMBER_SIX_PARTICIPATION.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    assert_eq!(source_state, "JOINED");
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_assignments")?,
        0
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        0
    );
    Ok(())
}

fn assert_permanent_transfer_consent_readback(
    catalog: &mut OperationsCatalog,
    intent: &crate::TeamPermanentTransferIntentReceipt,
    consent: &crate::TeamPermanentTransferConsentReceipt,
) -> Result<OperationalActor, Box<dyn Error>> {
    for reader in [seventh_participant(), orchestrator()] {
        assert_eq!(
            catalog
                .read_team_permanent_transfer_consent(
                    WORKSPACE,
                    intent.transfer_id(),
                    consent.receipt_id(),
                    reader,
                )
                .map_err(|source| IoError::other(format!("consent owner/O readback: {source}")))?,
            Some(consent.clone())
        );
    }
    assert_eq!(
        catalog.read_team_permanent_transfer_consent(
            WORKSPACE,
            intent.transfer_id(),
            consent.receipt_id(),
            sixth_participant(),
        )?,
        None
    );
    assert_eq!(
        catalog.read_team_permanent_transfer_consent(
            WORKSPACE,
            intent.transfer_id(),
            Uuid::from_u128(0xFB45),
            seventh_participant(),
        )?,
        None
    );
    replace_team_leader(&catalog.connection, TEAM_ID, MEMBER_FOUR_PARTICIPANT)?;
    prepare_session_rebind_test_columns(&catalog.connection)?;
    let session_directory = TemporarySessionCatalogDirectory::new()?;
    let mut profiles =
        AgentProfileCatalog::open(session_directory.path.join("agent-profiles.sqlite3"))?;
    let mut sessions =
        AgentSessionCatalog::open(session_directory.path.join("agent-sessions.sqlite3"))?;
    let session_pair = create_synthetic_profile_session_pair(
        &mut profiles,
        &mut sessions,
        MEMBER_SEVEN_PROFILE,
        MEMBER_SEVEN_SESSION,
        Uuid::from_u128(0xFB46),
        0xFB50,
    )?;
    let rebound_source_leader = rebind_seventh_profile_session(catalog, &session_pair)?;
    assert_eq!(
        catalog
            .read_team_permanent_transfer_consent(
                WORKSPACE,
                intent.transfer_id(),
                consent.receipt_id(),
                rebound_source_leader,
            )
            .map_err(|source| IoError::other(format!("rebound consentor readback: {source}")))?,
        Some(consent.clone())
    );
    Ok(rebound_source_leader)
}

#[test]
fn permanent_transfer_rejects_wrong_actors_stale_source_and_duplicate_ids_atomically() -> TestResult
{
    let PermanentTransferIntentFixture {
        mut catalog,
        source,
        proposal,
        intent,
    } = prepare_permanent_transfer_intent()?;
    assert_wrong_consent_actors_and_stale_home(&mut catalog, &source, &intent)?;
    assert_duplicate_intents_and_stale_leadership(&mut catalog, &source, &proposal, &intent)
}

fn assert_wrong_consent_actors_and_stale_home(
    catalog: &mut OperationsCatalog,
    source: &ExistingHomeTransferFixture,
    intent: &crate::TeamPermanentTransferIntentReceipt,
) -> TestResult {
    let wrong_leader = crate::TeamPermanentTransferConsentCommand::new(
        Uuid::from_u128(0xFB30),
        WORKSPACE,
        intent.transfer_id(),
        crate::TeamPermanentTransferConsentKind::SourceHomeLeader,
        sixth_participant(),
    )?;
    assert!(
        catalog
            .record_team_permanent_transfer_consent(&wrong_leader, || timestamp(500))
            .is_err()
    );
    let wrong_target = crate::TeamPermanentTransferConsentCommand::new(
        Uuid::from_u128(0xFB31),
        WORKSPACE,
        intent.transfer_id(),
        crate::TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        eighth_participant(),
    )?;
    assert!(
        catalog
            .record_team_permanent_transfer_consent(&wrong_target, || timestamp(501))
            .is_err()
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_permanent_transfer_consents")?,
        0
    );
    catalog.connection.execute(
        "UPDATE ops_team_members SET revision=2 WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3 AND revision=1",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            source.source_team_id.as_bytes(),
            MEMBER_SIX_PARTICIPATION.as_bytes(),
        ],
    )?;
    let stale_home = crate::TeamPermanentTransferConsentCommand::new(
        Uuid::from_u128(0xFB34),
        WORKSPACE,
        intent.transfer_id(),
        crate::TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        sixth_participant(),
    )?;
    assert!(matches!(
        catalog.record_team_permanent_transfer_consent(&stale_home, || timestamp(502)),
        Err(OperationsError::Conflict(
            "permanent Team transfer source membership changed"
        ))
    ));
    catalog.connection.execute(
        "UPDATE ops_team_members SET revision=1 WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3 AND revision=2",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            source.source_team_id.as_bytes(),
            MEMBER_SIX_PARTICIPATION.as_bytes(),
        ],
    )?;
    Ok(())
}

fn assert_duplicate_intents_and_stale_leadership(
    catalog: &mut OperationsCatalog,
    source: &ExistingHomeTransferFixture,
    proposal: &crate::TeamPermanentTransferIntentCommand,
    intent: &crate::TeamPermanentTransferIntentReceipt,
) -> TestResult {
    let duplicate = crate::TeamPermanentTransferIntentCommand::new(
        Uuid::from_u128(0xFB32),
        crate::TeamPermanentTransferScope::new(
            WORKSPACE,
            source.source_team_id,
            SECOND_TEAM_ID,
            WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
        ),
        crate::TeamPermanentTransferDestinationSlot::new(
            source.request_a.request_id(),
            intent.destination_decision_id(),
            intent.destination_ordinal(),
        ),
        crate::TeamPermanentTransferRevisions::new(
            source.destination_a.team().revision(),
            1,
            1,
            1,
            1,
        ),
        orchestrator(),
    )?;
    assert!(matches!(
        catalog.propose_team_permanent_transfer(&duplicate, || timestamp(502)),
        Err(OperationsError::Conflict(
            "permanent Team transfer destination slot already has an intent"
        ))
    ));
    let changed_same_id = crate::TeamPermanentTransferIntentCommand::new(
        intent.transfer_id(),
        crate::TeamPermanentTransferScope::new(
            WORKSPACE,
            source.source_team_id,
            SECOND_TEAM_ID,
            WorkspaceParticipationId::from_uuid(MEMBER_SIX_PARTICIPATION)?,
        ),
        crate::TeamPermanentTransferDestinationSlot::new(
            intent.destination_request_id(),
            intent.destination_decision_id(),
            intent.destination_ordinal(),
        ),
        crate::TeamPermanentTransferRevisions::new(
            intent.destination_team_revision(),
            intent.destination_capacity_revision(),
            intent.destination_workspace_leadership_revision(),
            2,
            intent.source_leadership_revision(),
        ),
        orchestrator(),
    )?;
    assert!(matches!(
        catalog.propose_team_permanent_transfer(&changed_same_id, || timestamp(503)),
        Err(OperationsError::Conflict(
            "permanent Team transfer intent replay"
        ))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_permanent_transfer_intents")?,
        1
    );
    catalog.connection.execute(
        "UPDATE ops_leadership SET leadership_revision=2,updated_at_ms=504 WHERE workspace_id=?1 AND scope_kind=2 AND team_id=?2 AND leadership_revision=1",
        params![WORKSPACE.into_uuid().as_bytes(), source.source_team_id.as_bytes()],
    )?;
    let source_consent = crate::TeamPermanentTransferConsentCommand::new(
        Uuid::from_u128(0xFB33),
        WORKSPACE,
        intent.transfer_id(),
        crate::TeamPermanentTransferConsentKind::SourceHomeLeader,
        seventh_participant(),
    )?;
    assert!(matches!(
        catalog.record_team_permanent_transfer_consent(&source_consent, || timestamp(505)),
        Err(OperationsError::Conflict(
            "permanent Team transfer source leadership changed"
        ))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_permanent_transfer_consents")?,
        0
    );
    assert!(
        catalog
            .propose_team_permanent_transfer(proposal, || timestamp(506))?
            .replayed()
    );
    Ok(())
}

#[test]
fn full_workspace_two_destination_transfer_approvals_have_one_winner() -> TestResult {
    let database = TemporaryDatabase::new()?;
    let mut catalog = workspace_capacity_fixture_at(&database.path)?;
    fill_workspace_to_twelve(&catalog)?;
    let fixture = prepare_existing_home_transfer_fixture(&mut catalog)?;
    let command_a = existing_home_v3_command(
        Uuid::from_u128(0xFB06),
        SECOND_TEAM_ID,
        &fixture.request_a,
        fixture.source_team_id,
        1,
    )?;
    let command_b = existing_home_v3_command(
        Uuid::from_u128(0xFB07),
        BYPASS_TEAM_ID,
        &fixture.request_b,
        fixture.source_team_id,
        1,
    )?;
    drop(catalog);

    let barrier = Arc::new(Barrier::new(3));
    let first = spawn_v3_approval(&database.path, Arc::clone(&barrier), command_a, 200);
    let second = spawn_v3_approval(&database.path, Arc::clone(&barrier), command_b, 201);
    barrier.wait();
    let results = [
        first.join().map_err(|_| "first V3 approval panicked")?,
        second.join().map_err(|_| "second V3 approval panicked")?,
    ];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(results.iter().any(|result| matches!(
        result,
        Err(OperationsError::Conflict(
            "Profile already has an active Team expansion hold"
        ))
    )));

    let reopened = OperationsCatalog {
        connection: create_connection(Some(&database.path))?,
        destination: DESTINATION,
    };
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_decisions")?,
        1
    );
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_v3_manifests")?,
        1
    );
    assert_eq!(
        count_rows(
            &reopened.connection,
            "ops_team_expansion_existing_home_transfers"
        )?,
        1
    );
    assert_eq!(
        count_rows(&reopened.connection, "ops_team_expansion_cooldown_anchors")?,
        0
    );
    assert_eq!(
        super::workspace_hold::workspace_capacity_snapshot(&reopened.connection, WORKSPACE)?.used(),
        12
    );
    Ok(())
}

fn spawn_v3_approval(
    path: &Path,
    barrier: Arc<Barrier>,
    command: super::TeamExpansionDecisionV3Command,
    decided_at: i64,
) -> thread::JoinHandle<Result<super::TeamExpansionDecisionReceipt, OperationsError>> {
    let path = path.to_path_buf();
    thread::spawn(move || {
        let connection = create_connection(Some(&path))
            .map_err(|_| OperationsError::Conflict("V3 approval connection"))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(OperationsError::from)?;
        let mut catalog = OperationsCatalog {
            connection,
            destination: DESTINATION,
        };
        barrier.wait();
        catalog.decide_team_expansion_v3(&command, || timestamp(decided_at))
    })
}

#[test]
fn v3_wrong_or_stale_home_source_rolls_back_decision_and_hold() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let fixture = prepare_existing_home_transfer_fixture(&mut catalog)?;
    let wrong_team = existing_home_v3_command(
        Uuid::from_u128(0xFB08),
        SECOND_TEAM_ID,
        &fixture.request_a,
        SECOND_TEAM_ID,
        1,
    )?;
    assert!(matches!(
        catalog.decide_team_expansion_v3(&wrong_team, || timestamp(200)),
        Err(OperationsError::Bounds(
            "Team expansion existing Home transfer source"
        ))
    ));
    let stale_revision = existing_home_v3_command(
        Uuid::from_u128(0xFB09),
        SECOND_TEAM_ID,
        &fixture.request_a,
        fixture.source_team_id,
        2,
    )?;
    assert!(matches!(
        catalog.decide_team_expansion_v3(&stale_revision, || timestamp(200)),
        Err(OperationsError::Conflict(
            "Team expansion transfer source membership changed"
        ))
    ));
    let regressed_clock = existing_home_v3_command(
        Uuid::from_u128(0xFB0A),
        SECOND_TEAM_ID,
        &fixture.request_a,
        fixture.source_team_id,
        1,
    )?;
    assert!(matches!(
        catalog.decide_team_expansion_v3(&regressed_clock, || timestamp(99)),
        Err(OperationsError::Conflict(
            "Team expansion trusted clock regressed"
        ))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        0
    );
    assert_eq!(
        count_rows(
            &catalog.connection,
            "ops_team_expansion_workspace_hold_manifests"
        )?,
        0
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_v3_manifests")?,
        0
    );
    Ok(())
}

#[test]
fn retired_source_team_with_historical_joined_row_cannot_start_v3_approval() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let fixture = prepare_existing_home_transfer_fixture(&mut catalog)?;
    catalog.retire_team(crate::TeamRetireCommand {
        request_id: Uuid::from_u128(0xFB0C),
        workspace_id: WORKSPACE,
        team_id: fixture.source_team_id,
        expected_team_revision: 1,
        actor: OperationalActor::LocalOperator,
        observed_at: timestamp(150)?,
    })?;
    let historical_state: String = catalog.connection.query_row(
        "SELECT state FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            fixture.source_team_id.as_bytes(),
            MEMBER_SIX_PARTICIPATION.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    assert_eq!(historical_state, "JOINED");

    let command = existing_home_v3_command(
        Uuid::from_u128(0xFB0D),
        SECOND_TEAM_ID,
        &fixture.request_a,
        fixture.source_team_id,
        1,
    )?;
    assert!(matches!(
        catalog.decide_team_expansion_v3(&command, || timestamp(200)),
        Err(OperationsError::Conflict(
            "Team expansion transfer target must have one Home Team"
        ))
    ));
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_decisions")?,
        0
    );
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_v3_manifests")?,
        0
    );
    Ok(())
}

#[test]
fn a_new_member_cannot_bypass_the_orchestrator_slot_with_direct_self_join() -> TestResult {
    let (mut catalog, team, _) = configured_requester_team()?;
    let before = count_rows(&catalog.connection, "ops_team_members")?;
    let command = TeamJoinCommand {
        request_id: Uuid::from_u128(0xF340),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.team().revision(),
        actor: fifth_participant(),
        role: "member".to_owned(),
        expansion_slot: None,
        observed_at: timestamp(20)?,
    };
    assert!(matches!(
        catalog.join_team(command),
        Err(OperationsError::Conflict(_))
    ));
    assert_eq!(count_rows(&catalog.connection, "ops_team_members")?, before);
    assert_eq!(count_rows(&catalog.connection, "ops_team_requests")?, 1);
    let recorded: bool = catalog.connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3 AND state='JOINED')",
        params![WORKSPACE.into_uuid().as_bytes(), TEAM_ID.as_bytes(), MEMBER_FIVE_PARTICIPATION.as_bytes()],
        |row| row.get(0),
    )?;
    assert!(!recorded);
    Ok(())
}

#[test]
fn historical_team_without_a_capacity_event_is_read_as_unknown_not_as_eight() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let legacy_team = Uuid::from_u128(0xE101);
    insert_legacy_team_without_capacity(&catalog.connection, legacy_team)?;

    assert_eq!(catalog.team_capacity(WORKSPACE, legacy_team)?, None);
    let configured = catalog.set_team_capacity(
        &TeamCapacitySetCommand::new(
            Uuid::from_u128(0xF104),
            TeamExpansionScope::new(WORKSPACE, legacy_team),
            TeamExpansionRevisions::new(1, 0, 1),
            TeamMemberCapacity::new(4)?,
            orchestrator(),
        ),
        || timestamp(2),
    )?;
    assert_eq!(configured.expected_capacity_revision(), 0);
    assert_eq!(configured.snapshot().revision(), 1);
    assert_eq!(configured.snapshot().max_members().get(), 4);
    assert_eq!(
        catalog
            .team_capacity(WORKSPACE, legacy_team)?
            .map(|snapshot| snapshot.max_members()),
        Some(TeamMemberCapacity::new(4)?)
    );
    Ok(())
}

fn insert_legacy_team_without_capacity(
    connection: &Connection,
    team_id: Uuid,
) -> Result<(), rusqlite::Error> {
    // Synthetic pre-schema-58 Team row. No cap is inferred or inserted.
    connection.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'Legacy capacity unknown','historical fixture','ACTIVE',1,'legacy','Legacy creator',?3,?4,?5,?6,1,1,1)",
        params![
            team_id.as_bytes(),
            WORKSPACE.into_uuid().as_bytes(),
            ORCHESTRATOR_PARTICIPATION.as_bytes(),
            ORCHESTRATOR_PROFILE.as_bytes(),
            ORCHESTRATOR_PARTICIPANT.as_bytes(),
            ORCHESTRATOR_SESSION.as_bytes(),
        ],
    )?;
    Ok(())
}

fn count_rows(connection: &Connection, table: &str) -> Result<i64, rusqlite::Error> {
    connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
}

fn leave_transfer_source_home(fixture: &mut PermanentTransferIntentFixture) -> TestResult {
    let revision: i64 = fixture.catalog.connection.query_row(
        "SELECT revision FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            fixture.source.source_team_id.as_bytes()
        ],
        |row| row.get(0),
    )?;
    fixture.catalog.leave_team(crate::TeamLeaveCommand {
        request_id: Uuid::from_u128(0xFB80),
        workspace_id: WORKSPACE,
        team_id: fixture.source.source_team_id,
        expected_team_revision: u64::try_from(revision)?,
        actor: sixth_participant(),
        observed_at: timestamp(600)?,
    })?;
    Ok(())
}

#[test]
fn existing_home_transfer_slot_cannot_be_assigned_after_generic_source_leave() -> TestResult {
    let mut fixture = prepare_permanent_transfer_intent()?;
    leave_transfer_source_home(&mut fixture)?;
    let slot = TeamExpansionSlotRef::new(fixture.intent.destination_decision_id(), 0);
    let command = build_slot_assignment_command(
        0xFB81,
        slot,
        &fixture.source.destination_a,
        sixth_participant(),
    )?;
    let before = count_rows(
        &fixture.catalog.connection,
        "ops_team_expansion_slot_assignments",
    )?;
    let assigned = fixture
        .catalog
        .assign_team_expansion_slot(&command, || timestamp(700));
    assert!(
        matches!(
            assigned,
            Err(OperationsError::Conflict(
                "existing Home transfer requires atomic cutover"
            ))
        ),
        "unexpected assignment outcome: {assigned:?}"
    );
    assert_eq!(
        count_rows(
            &fixture.catalog.connection,
            "ops_team_expansion_slot_assignments"
        )?,
        before
    );
    Ok(())
}

#[test]
fn existing_home_transfer_historical_assignment_replays_but_cannot_join() -> TestResult {
    let mut fixture = prepare_permanent_transfer_intent()?;
    leave_transfer_source_home(&mut fixture)?;
    let slot = TeamExpansionSlotRef::new(fixture.intent.destination_decision_id(), 0);
    let command = build_slot_assignment_command(
        0xFB82,
        slot,
        &fixture.source.destination_a,
        sixth_participant(),
    )?;
    // Model an assignment committed by the former generic admission path.
    // This test-only history seeding does not grant new transfer authority.
    let transaction = fixture.catalog.connection.unchecked_transaction()?;
    let (issuer, _) = super::validate_actor(&transaction, WORKSPACE, orchestrator())?;
    let (target, _) = super::validate_actor(&transaction, WORKSPACE, sixth_participant())?;
    let original =
        super::record_slot_assignment(&transaction, &command, &issuer, &target, timestamp(700)?)?;
    transaction.commit()?;
    for candidate in [slot, TeamExpansionSlotRef::new(Uuid::from_u128(0xFB89), 0)] {
        assert!(matches!(
            super::validate_consumable_slot(
                &fixture.catalog.connection,
                WORKSPACE,
                SECOND_TEAM_ID,
                candidate,
                WorkspaceParticipationId::from_uuid(MEMBER_EIGHT_PARTICIPATION)?,
                MEMBER_EIGHT_PROFILE,
            ),
            Err(OperationsError::Conflict(
                "Team expansion slot is not assigned to this Profile"
            ))
        ));
    }
    let replay = fixture
        .catalog
        .assign_team_expansion_slot(&command, || timestamp(701))?;
    assert!(replay.replayed());
    assert_eq!(replay.request_id(), original.request_id());
    assert_eq!(
        fixture
            .catalog
            .team_expansion_slot_assignment(WORKSPACE, slot, orchestrator())?,
        Some(original)
    );
    let before = count_rows(&fixture.catalog.connection, "ops_team_members")?;
    assert!(matches!(
        fixture.catalog.join_team(TeamJoinCommand {
            request_id: Uuid::from_u128(0xFB83),
            workspace_id: WORKSPACE,
            team_id: SECOND_TEAM_ID,
            expected_team_revision: fixture.source.destination_a.team().revision(),
            actor: sixth_participant(),
            role: "transfer bypass".to_owned(),
            expansion_slot: Some(slot),
            observed_at: timestamp(800)?,
        }),
        Err(OperationsError::Conflict(
            "existing Home transfer requires atomic cutover"
        ))
    ));
    assert_eq!(
        count_rows(&fixture.catalog.connection, "ops_team_members")?,
        before
    );
    assert_eq!(
        count_rows(&fixture.catalog.connection, "ops_team_expansion_slot_uses")?,
        0
    );
    Ok(())
}

#[test]
fn missing_existing_home_transfer_source_is_not_treated_as_an_ordinary_slot() -> TestResult {
    let mut fixture = prepare_permanent_transfer_intent()?;
    leave_transfer_source_home(&mut fixture)?;
    // Explicit damaged-database fixture: bypass its immutable trigger only here.
    fixture.catalog.connection.execute_batch("DROP TRIGGER ops_team_expansion_existing_home_transfers_no_delete; DELETE FROM ops_team_expansion_existing_home_transfers;")?;
    let slot = TeamExpansionSlotRef::new(fixture.intent.destination_decision_id(), 0);
    let command = build_slot_assignment_command(
        0xFB84,
        slot,
        &fixture.source.destination_a,
        sixth_participant(),
    )?;
    assert!(matches!(
        fixture
            .catalog
            .assign_team_expansion_slot(&command, || timestamp(700)),
        Err(OperationsError::Corrupt(
            "Team expansion V3 transfer cardinality"
        ))
    ));
    assert_eq!(
        count_rows(
            &fixture.catalog.connection,
            "ops_team_expansion_slot_assignments"
        )?,
        0
    );
    Ok(())
}

#[test]
fn ordinary_slot_in_mixed_v3_approval_still_assigns_and_joins() -> TestResult {
    let mut catalog = workspace_capacity_fixture()?;
    let source = prepare_existing_home_transfer_fixture_with_successor(&mut catalog, true)?;
    let request = catalog.request_team_expansion(
        &expansion_request_for(
            0xFB85,
            SECOND_TEAM_ID,
            &source.destination_a,
            actor(MEMBER_TWO_ACTIVITY, MEMBER_TWO_AGENT, MEMBER_TWO_SESSION),
            2,
        )?,
        || timestamp(102),
    )?;
    let command = super::TeamExpansionDecisionV3Command::new(
        decide_command_for(
            Uuid::from_u128(0xFB86),
            SECOND_TEAM_ID,
            &request,
            TeamExpansionDecision::Approve { approved_count: 2 },
            orchestrator(),
            1,
        ),
        vec![
            super::TeamExpansionApprovalSlotPlanV3::new(
                0,
                super::TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer {
                    profile_id: MEMBER_SIX_PROFILE,
                    participation_id: WorkspaceParticipationId::from_uuid(
                        MEMBER_SIX_PARTICIPATION,
                    )?,
                    source_team_id: source.source_team_id,
                    source_membership_revision: 1,
                },
            ),
            super::TeamExpansionApprovalSlotPlanV3::new(
                1,
                super::TeamExpansionWorkspaceTargetV3::ExistingWorkspaceProfile {
                    profile_id: ORCHESTRATOR_PROFILE,
                    participation_id: WorkspaceParticipationId::from_uuid(
                        ORCHESTRATOR_PARTICIPATION,
                    )?,
                },
            ),
        ],
    );
    let approved = catalog.decide_team_expansion_v3(&command, || timestamp(200))?;
    catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        approved.decision_id(),
        1,
        orchestrator(),
        || timestamp(300),
    )?;
    let slot = TeamExpansionSlotRef::new(approved.decision_id(), 1);
    let assignment =
        build_slot_assignment_command(0xFB87, slot, &source.destination_a, orchestrator())?;
    catalog.assign_team_expansion_slot(&assignment, || timestamp(400))?;
    catalog.join_team(TeamJoinCommand {
        request_id: Uuid::from_u128(0xFB88),
        workspace_id: WORKSPACE,
        team_id: SECOND_TEAM_ID,
        expected_team_revision: source.destination_a.team().revision(),
        actor: orchestrator(),
        role: "ordinary approved member".to_owned(),
        expansion_slot: Some(slot),
        observed_at: timestamp(500)?,
    })?;
    assert_eq!(
        count_rows(&catalog.connection, "ops_team_expansion_slot_uses")?,
        1
    );
    Ok(())
}
