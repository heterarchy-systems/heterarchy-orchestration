use std::error::Error;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, params};
use uuid::Uuid;

use super::*;
use crate::{
    OperationalActor, OperationsCatalog, TeamCreateCommand, TeamJoinCommand, TeamLeaveCommand,
    TeamMemberCapacity, TeamRosterMemberInput,
    team_expansion::{
        TeamExpansionDecision, TeamExpansionDecisionCommand, TeamExpansionRequestCommand,
        TeamExpansionRevisions, TeamExpansionScope, TeamExpansionSlotAssignmentCommand,
        TeamExpansionSlotRef,
    },
};

const DESTINATION: Uuid = Uuid::from_u128(0x9000);
const WORKSPACE: WorkspaceId = WorkspaceId::from_uuid(Uuid::from_u128(0x1000));
const WORKSPACE_TWO: WorkspaceId = WorkspaceId::from_uuid(Uuid::from_u128(0x2000));
const TEAM_ID: Uuid = Uuid::from_u128(0xEE01);
const CREATE_REQUEST: Uuid = Uuid::from_u128(0xEE02);
const DEFAULT_LEADER_PARTICIPANT: Uuid = Uuid::from_u128(0x5000);
const TENURE_PROFILE: Uuid = Uuid::from_u128(0xEE11);
const TENURE_PARTICIPATION: Uuid = Uuid::from_u128(0xEE12);
const TENURE_PARTICIPANT: Uuid = Uuid::from_u128(0xEE13);
const TENURE_ACTIVITY: Uuid = Uuid::from_u128(0xEE14);
const TENURE_AGENT: Uuid = Uuid::from_u128(0xEE15);
const TENURE_SESSION: Uuid = Uuid::from_u128(0xEE16);
const SECOND_PROFILE: Uuid = Uuid::from_u128(0xEE31);
const SECOND_PARTICIPATION: Uuid = Uuid::from_u128(0xEE32);
const SECOND_PARTICIPANT: Uuid = Uuid::from_u128(0xEE33);
const SECOND_ACTIVITY: Uuid = Uuid::from_u128(0xEE34);
const SECOND_AGENT: Uuid = Uuid::from_u128(0xEE35);
const SECOND_SESSION: Uuid = Uuid::from_u128(0xEE36);
const FIRST_JOIN_PROFILE: Uuid = Uuid::from_u128(0xEE71);
const FIRST_JOIN_PARTICIPATION: Uuid = Uuid::from_u128(0xEE72);
const FIRST_JOIN_PARTICIPANT: Uuid = Uuid::from_u128(0xEE73);
const FIRST_JOIN_ACTIVITY: Uuid = Uuid::from_u128(0xEE74);
const FIRST_JOIN_AGENT: Uuid = Uuid::from_u128(0xEE75);
const FIRST_JOIN_SESSION: Uuid = Uuid::from_u128(0xEE76);

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Copy)]
struct MemberIdentity {
    profile: Uuid,
    participation: WorkspaceParticipationId,
    participant: Uuid,
    activity: ActivityId,
    agent: AgentInstanceId,
    session: AgentSessionId,
}

impl MemberIdentity {
    fn actor(self) -> OperationalActor {
        OperationalActor::Participant {
            activity_id: self.activity,
            agent_id: self.agent,
            session_id: self.session,
        }
    }
}

struct MembershipFixture {
    catalog: OperationsCatalog,
    member: MemberIdentity,
    second_member: MemberIdentity,
}

fn fixture() -> TestResult<MembershipFixture> {
    // Existing Operations helpers provide synthetic identity, Team authority,
    // and import facts. This is not a Broker/Forge or user-attested live test.
    let connection = crate::teams::tests::create_connection(None)?;
    crate::registration::create_schema(&connection)?;
    crate::teams::tests::ensure_team_expansion_schema(&connection)?;
    crate::teams::tests::ensure_test_workspace_orchestrator(&connection)?;
    schema::create_schema_for_test(&connection)?;

    crate::teams::tests::seed_participant_identity(
        &connection,
        WORKSPACE,
        TENURE_PROFILE,
        TENURE_PARTICIPATION,
        TENURE_PARTICIPANT,
        TENURE_ACTIVITY,
        TENURE_AGENT,
        TENURE_SESSION,
        Uuid::from_u128(0xEE17),
    )?;
    crate::teams::tests::seed_participant_identity(
        &connection,
        WORKSPACE,
        SECOND_PROFILE,
        SECOND_PARTICIPATION,
        SECOND_PARTICIPANT,
        SECOND_ACTIVITY,
        SECOND_AGENT,
        SECOND_SESSION,
        Uuid::from_u128(0xEE37),
    )?;
    let member = MemberIdentity {
        profile: TENURE_PROFILE,
        participation: WorkspaceParticipationId::from_uuid(TENURE_PARTICIPATION)?,
        participant: TENURE_PARTICIPANT,
        activity: ActivityId::from_uuid(TENURE_ACTIVITY),
        agent: AgentInstanceId::from_uuid(TENURE_AGENT),
        session: AgentSessionId::from_uuid(TENURE_SESSION),
    };
    let second_member = MemberIdentity {
        profile: SECOND_PROFILE,
        participation: WorkspaceParticipationId::from_uuid(SECOND_PARTICIPATION)?,
        participant: SECOND_PARTICIPANT,
        activity: ActivityId::from_uuid(SECOND_ACTIVITY),
        agent: AgentInstanceId::from_uuid(SECOND_AGENT),
        session: AgentSessionId::from_uuid(SECOND_SESSION),
    };

    let create = TeamCreateCommand {
        request_id: CREATE_REQUEST,
        team_id: TEAM_ID,
        workspace_id: WORKSPACE,
        actor: OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0x6000)),
            agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0x7000)),
            session_id: AgentSessionId::from_uuid(Uuid::from_u128(0x8000)),
        },
        expected_sender_leadership_revision: Some(1),
        name: "Tenure fixture".to_owned(),
        purpose: "Verify post-commit membership tenure anchors".to_owned(),
        initial_members: vec![
            TeamRosterMemberInput {
                activity_id: ActivityId::from_uuid(Uuid::from_u128(0x6000)),
                agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0x7000)),
                session_id: AgentSessionId::from_uuid(Uuid::from_u128(0x8000)),
                role: "leader".to_owned(),
            },
            TeamRosterMemberInput {
                activity_id: member.activity,
                agent_id: member.agent,
                session_id: member.session,
                role: "member".to_owned(),
            },
            TeamRosterMemberInput {
                activity_id: second_member.activity,
                agent_id: second_member.agent,
                session_id: second_member.session,
                role: "member".to_owned(),
            },
        ],
        observed_at: timestamp(100)?,
    };
    let mut catalog = OperationsCatalog {
        connection,
        destination: DESTINATION,
    };
    catalog.create_team_with_capacity(create, TeamMemberCapacity::new(4)?)?;
    seed_team_leadership(&catalog.connection)?;
    Ok(MembershipFixture {
        catalog,
        member,
        second_member,
    })
}

fn command(
    workspace_id: WorkspaceId,
    team_id: Uuid,
    member: MemberIdentity,
    request_id: Uuid,
    revision: u64,
) -> Result<TeamMembershipTenureAnchorCommand, OperationsError> {
    TeamMembershipTenureAnchorCommand::new(
        workspace_id,
        team_id,
        member.participation,
        revision,
        request_id,
        member.actor(),
    )
}

fn anchor_at(
    catalog: &mut OperationsCatalog,
    command: &TeamMembershipTenureAnchorCommand,
    at: i64,
) -> Result<TeamMembershipTenureAnchor, OperationsError> {
    catalog.establish_team_membership_tenure_anchor_with_clock(command, || timestamp(at))
}

fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Conflict("test timestamp"))
}

#[test]
fn anchor_requires_exact_committed_event_and_samples_only_after_source_read() -> TestResult {
    let mut fixture = fixture()?;
    let exact = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;

    let absent = command(
        WORKSPACE,
        TEAM_ID,
        fixture.member,
        Uuid::from_u128(0xEE03),
        1,
    )?;
    let result = anchor_at(&mut fixture.catalog, &absent, 101);
    assert!(matches!(
        result,
        Err(OperationsError::Conflict("Team membership event missing"))
    ));
    assert_anchor_count(&fixture.catalog.connection, 0)?;

    let regressed = anchor_at(&mut fixture.catalog, &exact, 99);
    assert!(matches!(
        regressed,
        Err(OperationsError::Conflict(
            "Team tenure anchor clock regression"
        ))
    ));
    assert_anchor_count(&fixture.catalog.connection, 0)?;

    let source_unavailable = fixture
        .catalog
        .establish_team_membership_tenure_anchor_with_clock(&exact, || {
            Err(OperationsError::Conflict("test clock unavailable"))
        });
    assert!(matches!(
        source_unavailable,
        Err(OperationsError::Conflict("test clock unavailable"))
    ));
    assert_anchor_count(&fixture.catalog.connection, 0)?;

    let anchored = anchor_at(&mut fixture.catalog, &exact, 101)?;
    assert_eq!(anchored.membership_state(), TeamMembershipState::Joined);
    assert_eq!(anchored.membership_observed_at(), timestamp(100)?);
    assert_eq!(anchored.anchored_at(), timestamp(101)?);
    assert_ne!(anchored.membership_event_digest(), [0; 32]);
    assert_eq!(
        anchored.source_kind(),
        TeamMembershipTenureSourceKind::ExplicitCapacityCreate
    );
    assert_ne!(anchored.source_digest(), [0; 32]);
    assert!(!anchored.replayed());
    assert_anchor_count(&fixture.catalog.connection, 1)?;
    Ok(())
}

#[test]
fn shared_create_request_anchors_each_initial_roster_member_independently() -> TestResult {
    let mut fixture = fixture()?;
    let first_command = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    let second_command = command(WORKSPACE, TEAM_ID, fixture.second_member, CREATE_REQUEST, 1)?;

    let first = anchor_at(&mut fixture.catalog, &first_command, 101)?;
    let second = anchor_at(&mut fixture.catalog, &second_command, 102)?;
    assert_eq!(
        first.membership_request_id(),
        second.membership_request_id()
    );
    assert_ne!(first.participation_id(), second.participation_id());
    assert_anchor_count(&fixture.catalog.connection, 2)?;
    Ok(())
}

#[test]
fn legacy_capless_create_event_is_not_backfilled_as_tenure() -> TestResult {
    let mut fixture = fixture()?;
    fixture
        .catalog
        .connection
        .execute_batch("DROP TRIGGER ops_team_capacity_events_no_delete;")?;
    fixture.catalog.connection.execute(
        "DELETE FROM ops_team_capacity_events WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3",
        params![CREATE_REQUEST.as_bytes(), WORKSPACE.into_uuid().as_bytes(), TEAM_ID.as_bytes()],
    )?;
    let exact = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    assert!(matches!(
        anchor_at(&mut fixture.catalog, &exact, 101),
        Err(OperationsError::Conflict(
            "Team tenure explicit capacity source missing"
        ))
    ));
    assert_anchor_count(&fixture.catalog.connection, 0)?;
    Ok(())
}

#[test]
fn first_join_created_true_revision_one_is_anchored_and_mismatched_pair_is_rejected() -> TestResult
{
    let mut fixture = fixture()?;
    let joiner = seed_first_joiner(&fixture.catalog.connection)?;
    let team = fixture
        .catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing")?;
    let capacity = fixture
        .catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("Team capacity missing")?;
    let leader = OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(Uuid::from_u128(0x6000)),
        agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0x7000)),
        session_id: AgentSessionId::from_uuid(Uuid::from_u128(0x8000)),
    };
    let request_id = Uuid::from_u128(0xEF01);
    let request = fixture.catalog.request_team_expansion(
        &TeamExpansionRequestCommand::new(
            request_id,
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
            1,
            "first membership request".to_owned(),
            leader,
        ),
        || timestamp(200),
    )?;
    let pending = fixture.catalog.decide_team_expansion(
        &TeamExpansionDecisionCommand::new(
            Uuid::from_u128(0xEF02),
            request.request_id(),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
            1,
            TeamExpansionDecision::Approve { approved_count: 1 },
            leader,
        ),
        || timestamp(300),
    )?;
    let approval = fixture.catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        leader,
        || timestamp(301),
    )?;
    let slot = TeamExpansionSlotRef::new(approval.decision_id(), 0);
    let cooldown_until = approval.cooldown_until().ok_or("cooldown anchor missing")?;
    fixture.catalog.assign_team_expansion_slot(
        &TeamExpansionSlotAssignmentCommand::new(
            Uuid::from_u128(0xEF03),
            slot,
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
            joiner.actor(),
            leader,
        ),
        || Ok(cooldown_until),
    )?;
    let join_request = Uuid::from_u128(0xEF04);
    let joined_at = cooldown_until.get() + 1;
    let joined = fixture.catalog.join_team(TeamJoinCommand {
        request_id: join_request,
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.revision(),
        actor: joiner.actor(),
        role: "member".to_owned(),
        expansion_slot: Some(slot),
        observed_at: timestamp(joined_at)?,
    })?;
    assert!(
        joined.created(),
        "first membership insertion reports created=true"
    );
    assert_eq!(
        joined
            .membership()
            .ok_or("first join membership missing")?
            .revision(),
        1
    );

    let anchor_command = command(WORKSPACE, TEAM_ID, joiner, join_request, 1)?;
    fixture.catalog.connection.execute(
        "UPDATE ops_team_requests SET created=0 WHERE request_id=?1",
        params![join_request.as_bytes()],
    )?;
    assert!(matches!(
        anchor_at(&mut fixture.catalog, &anchor_command, joined_at + 1),
        Err(OperationsError::Corrupt(
            "Team tenure JOIN creation/revision mismatch"
        ))
    ));
    assert_anchor_count(&fixture.catalog.connection, 0)?;

    fixture.catalog.connection.execute(
        "UPDATE ops_team_requests SET created=1 WHERE request_id=?1",
        params![join_request.as_bytes()],
    )?;
    let anchor = anchor_at(&mut fixture.catalog, &anchor_command, joined_at + 1)?;
    assert_eq!(anchor.membership_revision(), 1);
    assert_eq!(
        anchor.source_kind(),
        TeamMembershipTenureSourceKind::ApprovedSlotJoin
    );
    assert_anchor_count(&fixture.catalog.connection, 1)?;
    Ok(())
}

#[test]
fn exact_replay_and_session_rebind_keep_the_stable_profile_receipt() -> TestResult {
    let mut fixture = fixture()?;
    let exact = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    let first = anchor_at(&mut fixture.catalog, &exact, 101)?;

    let mut sampled = false;
    let replay = fixture
        .catalog
        .establish_team_membership_tenure_anchor_with_clock(&exact, || {
            sampled = true;
            Err(OperationsError::Conflict(
                "exact replay must not sample a new time",
            ))
        })?;
    assert!(replay.replayed());
    assert!(!sampled);
    assert_eq!(replay.anchored_at(), first.anchored_at());
    assert_eq!(replay.actor_session_id(), first.actor_session_id());
    assert_anchor_count(&fixture.catalog.connection, 1)?;

    // The old source Session is no longer the current effect binding after
    // rebind; the existing immutable anchor still verifies its stored source.
    fixture.catalog.connection.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3",
        params![WORKSPACE.into_uuid().as_bytes(), fixture.member.activity.into_uuid().as_bytes(), fixture.member.agent.into_uuid().as_bytes()],
    )?;
    let alias = seed_profile_session_alias(&fixture.catalog.connection, fixture.member)?;
    let alias_command = command(WORKSPACE, TEAM_ID, alias, CREATE_REQUEST, 1)?;
    let mut rebind_clock_sampled = false;
    let rebound_replay = fixture
        .catalog
        .establish_team_membership_tenure_anchor_with_clock(&alias_command, || {
            rebind_clock_sampled = true;
            Err(OperationsError::Conflict(
                "rebound exact replay must not resample",
            ))
        })?;
    assert!(rebound_replay.replayed());
    assert!(!rebind_clock_sampled);
    assert_eq!(rebound_replay.participation_id(), first.participation_id());
    assert_eq!(rebound_replay.actor_session_id(), first.actor_session_id());

    let readback = fixture.catalog.team_membership_tenure_anchor(
        WORKSPACE,
        TEAM_ID,
        fixture.member.participation,
        1,
        alias.actor(),
    )?;
    let readback =
        readback.ok_or("same logical Profile/participation could not read its anchor")?;
    assert_eq!(
        readback, first,
        "receipt retains the original recorder Session"
    );
    Ok(())
}

#[test]
fn create_anchor_stays_pending_when_roster_session_source_is_unverifiable() -> TestResult {
    let mut fixture = fixture()?;
    let alias = seed_profile_session_alias(&fixture.catalog.connection, fixture.member)?;
    fixture.catalog.connection.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3",
        params![WORKSPACE.into_uuid().as_bytes(), fixture.member.activity.into_uuid().as_bytes(), fixture.member.agent.into_uuid().as_bytes()],
    )?;
    let exact = command(WORKSPACE, TEAM_ID, alias, CREATE_REQUEST, 1)?;
    assert!(matches!(
        anchor_at(&mut fixture.catalog, &exact, 101),
        Err(OperationsError::Conflict(
            "Team tenure roster actor source missing"
        ))
    ));
    assert_anchor_count(&fixture.catalog.connection, 0)?;
    Ok(())
}

#[test]
fn rejoin_uses_a_new_revision_and_keeps_old_generation_evidence() -> TestResult {
    let mut fixture = fixture()?;
    let original = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    let old_anchor = anchor_at(&mut fixture.catalog, &original, 101)?;
    let team = fixture
        .catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing")?;
    fixture.catalog.leave_team(TeamLeaveCommand {
        request_id: Uuid::from_u128(0xEE40),
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.revision(),
        actor: fixture.member.actor(),
        observed_at: timestamp(150)?,
    })?;

    let team = fixture
        .catalog
        .team(WORKSPACE, TEAM_ID)?
        .ok_or("Team missing after leave")?;
    let capacity = fixture
        .catalog
        .team_capacity(WORKSPACE, TEAM_ID)?
        .ok_or("Team capacity missing")?;
    let leader = OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(Uuid::from_u128(0x6000)),
        agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0x7000)),
        session_id: AgentSessionId::from_uuid(Uuid::from_u128(0x8000)),
    };
    let expansion_request = Uuid::from_u128(0xEE42);
    let request = fixture.catalog.request_team_expansion(
        &TeamExpansionRequestCommand::new(
            expansion_request,
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
            1,
            "rejoin after approved slot".to_owned(),
            leader,
        ),
        || timestamp(200),
    )?;
    let pending = fixture.catalog.decide_team_expansion(
        &TeamExpansionDecisionCommand::new(
            Uuid::from_u128(0xEE43),
            request.request_id(),
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
            1,
            TeamExpansionDecision::Approve { approved_count: 1 },
            leader,
        ),
        || timestamp(300),
    )?;
    let approval = fixture.catalog.finalize_team_expansion_cooldown(
        WORKSPACE,
        request.request_id(),
        pending.decision_id(),
        1,
        leader,
        || timestamp(301),
    )?;
    let slot = TeamExpansionSlotRef::new(approval.decision_id(), 0);
    let cooldown_until = approval.cooldown_until().ok_or("cooldown anchor missing")?;
    fixture.catalog.assign_team_expansion_slot(
        &TeamExpansionSlotAssignmentCommand::new(
            Uuid::from_u128(0xEE44),
            slot,
            TeamExpansionScope::new(WORKSPACE, TEAM_ID),
            TeamExpansionRevisions::new(team.revision(), capacity.revision(), 1),
            fixture.member.actor(),
            leader,
        ),
        || Ok(cooldown_until),
    )?;
    let join_request = Uuid::from_u128(0xEE41);
    let joined_at = cooldown_until.get() + 1;
    let joined = fixture.catalog.join_team(TeamJoinCommand {
        request_id: join_request,
        workspace_id: WORKSPACE,
        team_id: TEAM_ID,
        expected_team_revision: team.revision(),
        actor: fixture.member.actor(),
        role: "member".to_owned(),
        expansion_slot: Some(slot),
        observed_at: timestamp(joined_at)?,
    })?;
    assert!(
        !joined.created(),
        "rejoining an existing membership reports created=false"
    );
    let membership = joined.membership().ok_or("rejoin membership missing")?;
    assert_eq!(membership.state(), TeamMembershipState::Joined);
    assert_eq!(membership.revision(), 3);

    let rejoin = command(WORKSPACE, TEAM_ID, fixture.member, join_request, 3)?;
    fixture.catalog.connection.execute(
        "UPDATE ops_team_requests SET created=1 WHERE request_id=?1",
        params![join_request.as_bytes()],
    )?;
    assert!(matches!(
        anchor_at(&mut fixture.catalog, &rejoin, joined_at + 1),
        Err(OperationsError::Corrupt(
            "Team tenure JOIN creation/revision mismatch"
        ))
    ));
    assert_anchor_count(&fixture.catalog.connection, 1)?;
    fixture.catalog.connection.execute(
        "UPDATE ops_team_requests SET created=0 WHERE request_id=?1",
        params![join_request.as_bytes()],
    )?;
    let new_anchor = anchor_at(&mut fixture.catalog, &rejoin, joined_at + 1)?;
    assert_eq!(new_anchor.membership_revision(), 3);
    assert_eq!(new_anchor.membership_observed_at(), timestamp(joined_at)?);
    assert_eq!(
        new_anchor.source_kind(),
        TeamMembershipTenureSourceKind::ApprovedSlotJoin
    );
    assert_ne!(
        new_anchor.membership_request_id(),
        old_anchor.membership_request_id()
    );
    let old_replay = fixture
        .catalog
        .establish_team_membership_tenure_anchor_with_clock(&original, || {
            timestamp(joined_at + 2)
        })?;
    assert!(old_replay.replayed());
    assert_eq!(
        fixture.catalog.team_membership_tenure_anchor(
            WORKSPACE,
            TEAM_ID,
            fixture.member.participation,
            1,
            fixture.member.actor(),
        )?,
        Some(old_anchor),
        "historical revision remains readable after rejoin",
    );
    Ok(())
}

#[test]
fn source_event_mutation_is_detected_on_owner_readback() -> TestResult {
    let mut fixture = fixture()?;
    let exact = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    fixture
        .catalog
        .establish_team_membership_tenure_anchor(&exact)?;
    fixture.catalog.connection.execute(
        "UPDATE ops_team_member_events SET role='changed-after-anchor' WHERE request_id=?1 AND participation_id=?2",
        params![CREATE_REQUEST.as_bytes(), fixture.member.participation.into_uuid().as_bytes()],
    )?;
    assert!(matches!(
        fixture.catalog.team_membership_tenure_anchor(
            WORKSPACE,
            TEAM_ID,
            fixture.member.participation,
            1,
            fixture.member.actor(),
        ),
        Err(OperationsError::Corrupt("Team tenure source event changed"))
    ));
    Ok(())
}

#[test]
fn create_roster_source_mutation_is_detected_on_owner_readback() -> TestResult {
    let mut fixture = fixture()?;
    let exact = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    fixture
        .catalog
        .establish_team_membership_tenure_anchor(&exact)?;
    fixture.catalog.connection.execute(
        "UPDATE ops_team_create_roster SET role='changed-after-anchor' WHERE request_id=?1 AND participation_id=?2",
        params![CREATE_REQUEST.as_bytes(), fixture.member.participation.into_uuid().as_bytes()],
    )?;
    assert!(matches!(
        fixture.catalog.team_membership_tenure_anchor(
            WORKSPACE,
            TEAM_ID,
            fixture.member.participation,
            1,
            fixture.member.actor(),
        ),
        Err(OperationsError::Corrupt("Team tenure source proof changed"))
    ));
    Ok(())
}

#[test]
fn create_capacity_leadership_revision_and_team_creator_must_match() -> TestResult {
    let mut fixture = fixture()?;
    let exact = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    fixture.catalog.connection.execute(
        "UPDATE ops_team_requests SET request_leadership_revision=2 WHERE request_id=?1",
        params![CREATE_REQUEST.as_bytes()],
    )?;
    assert!(matches!(
        anchor_at(&mut fixture.catalog, &exact, 101),
        Err(OperationsError::Corrupt(
            "Team tenure create leadership/request source"
        ))
    ));
    assert_anchor_count(&fixture.catalog.connection, 0)?;
    fixture.catalog.connection.execute(
        "UPDATE ops_team_requests SET request_leadership_revision=1 WHERE request_id=?1",
        params![CREATE_REQUEST.as_bytes()],
    )?;

    fixture.catalog.connection.execute(
        "UPDATE ops_teams SET created_by_session_id=?1 WHERE workspace_id=?2 AND team_id=?3",
        params![
            Uuid::from_u128(0xEFB1).as_bytes(),
            WORKSPACE.into_uuid().as_bytes(),
            TEAM_ID.as_bytes()
        ],
    )?;
    assert!(matches!(
        anchor_at(&mut fixture.catalog, &exact, 101),
        Err(OperationsError::Corrupt(
            "Team tenure create Team creator source"
        ))
    ));
    assert_anchor_count(&fixture.catalog.connection, 0)?;
    fixture.catalog.connection.execute(
        "UPDATE ops_teams SET created_by_session_id=?1 WHERE workspace_id=?2 AND team_id=?3",
        params![
            Uuid::from_u128(0x8000).as_bytes(),
            WORKSPACE.into_uuid().as_bytes(),
            TEAM_ID.as_bytes()
        ],
    )?;
    anchor_at(&mut fixture.catalog, &exact, 101)?;
    assert_anchor_count(&fixture.catalog.connection, 1)?;
    Ok(())
}

#[test]
fn create_roster_activity_agent_session_participant_and_profile_are_cross_checked() -> TestResult {
    let mut fixture = fixture()?;
    let exact = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    let original: (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) = fixture.catalog.connection.query_row(
        "SELECT activity_id,agent_id,session_id,participant_id,profile_id FROM ops_team_create_roster WHERE request_id=?1 AND participation_id=?2",
        params![CREATE_REQUEST.as_bytes(), fixture.member.participation.into_uuid().as_bytes()],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
    )?;
    let mutations = [
        (
            "activity_id",
            Uuid::from_u128(0xEFA1).as_bytes().to_vec(),
            original.0.as_slice(),
        ),
        (
            "agent_id",
            Uuid::from_u128(0xEFA2).as_bytes().to_vec(),
            original.1.as_slice(),
        ),
        (
            "session_id",
            Uuid::from_u128(0xEFA3).as_bytes().to_vec(),
            original.2.as_slice(),
        ),
        (
            "participant_id",
            Uuid::from_u128(0xEFA4).as_bytes().to_vec(),
            original.3.as_slice(),
        ),
        (
            "profile_id",
            Uuid::from_u128(0xEFA5).as_bytes().to_vec(),
            original.4.as_slice(),
        ),
    ];
    for (column, wrong, expected) in mutations {
        update_roster_identity_field(
            &fixture.catalog.connection,
            column,
            fixture.member.participation,
            &wrong,
        )?;
        assert!(matches!(
            anchor_at(&mut fixture.catalog, &exact, 101),
            Err(OperationsError::Conflict(
                "Team tenure roster actor source missing"
            )) | Err(OperationsError::Corrupt(_))
        ));
        assert_anchor_count(&fixture.catalog.connection, 0)?;
        update_roster_identity_field(
            &fixture.catalog.connection,
            column,
            fixture.member.participation,
            expected,
        )?;
    }
    anchor_at(&mut fixture.catalog, &exact, 101)?;
    assert_anchor_count(&fixture.catalog.connection, 1)?;
    Ok(())
}

#[test]
fn foreign_profile_and_another_workspace_cannot_resolve_the_anchor() -> TestResult {
    let mut fixture = fixture()?;
    let exact = command(WORKSPACE, TEAM_ID, fixture.member, CREATE_REQUEST, 1)?;
    anchor_at(&mut fixture.catalog, &exact, 101)?;

    let foreign = seed_other_profile_session(&fixture.catalog.connection)?;
    assert_eq!(
        fixture.catalog.team_membership_tenure_anchor(
            WORKSPACE,
            TEAM_ID,
            fixture.member.participation,
            1,
            foreign,
        )?,
        None,
    );

    let foreign_workspace_member = seed_other_workspace_profile(&fixture.catalog.connection)?;
    let other_workspace = command(
        WORKSPACE_TWO,
        TEAM_ID,
        foreign_workspace_member,
        CREATE_REQUEST,
        1,
    )?;
    assert!(matches!(
        anchor_at(&mut fixture.catalog, &other_workspace, 101),
        Err(OperationsError::Conflict("Team membership event missing"))
    ));
    assert_anchor_count(&fixture.catalog.connection, 1)?;
    Ok(())
}

fn assert_anchor_count(connection: &Connection, expected: i64) -> TestResult {
    let count = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_membership_tenure_anchors",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    assert_eq!(count, expected);
    Ok(())
}

fn update_roster_identity_field(
    connection: &Connection,
    column: &str,
    participation_id: WorkspaceParticipationId,
    value: &[u8],
) -> TestResult {
    let statement = match column {
        "activity_id" => {
            "UPDATE ops_team_create_roster SET activity_id=?1 WHERE request_id=?2 AND participation_id=?3"
        }
        "agent_id" => {
            "UPDATE ops_team_create_roster SET agent_id=?1 WHERE request_id=?2 AND participation_id=?3"
        }
        "session_id" => {
            "UPDATE ops_team_create_roster SET session_id=?1 WHERE request_id=?2 AND participation_id=?3"
        }
        "participant_id" => {
            "UPDATE ops_team_create_roster SET participant_id=?1 WHERE request_id=?2 AND participation_id=?3"
        }
        "profile_id" => {
            "UPDATE ops_team_create_roster SET profile_id=?1 WHERE request_id=?2 AND participation_id=?3"
        }
        _ => return Err("unsupported test roster identity field".into()),
    };
    connection.execute(
        statement,
        params![
            value,
            CREATE_REQUEST.as_bytes(),
            participation_id.into_uuid().as_bytes()
        ],
    )?;
    Ok(())
}

fn seed_team_leadership(connection: &Connection) -> TestResult {
    connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,2,NULL,?2,'ACTIVE',?3,1,NULL,NULL,100)",
        params![WORKSPACE.into_uuid().as_bytes(), TEAM_ID.as_bytes(), DEFAULT_LEADER_PARTICIPANT.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,1,'CONFIRMED',?3,1,100)",
        params![WORKSPACE.into_uuid().as_bytes(), TEAM_ID.as_bytes(), DEFAULT_LEADER_PARTICIPANT.as_bytes()],
    )?;
    Ok(())
}

fn seed_first_joiner(connection: &Connection) -> TestResult<MemberIdentity> {
    crate::teams::tests::seed_participant_identity(
        connection,
        WORKSPACE,
        FIRST_JOIN_PROFILE,
        FIRST_JOIN_PARTICIPATION,
        FIRST_JOIN_PARTICIPANT,
        FIRST_JOIN_ACTIVITY,
        FIRST_JOIN_AGENT,
        FIRST_JOIN_SESSION,
        Uuid::from_u128(0xEF10),
    )?;
    Ok(MemberIdentity {
        profile: FIRST_JOIN_PROFILE,
        participation: WorkspaceParticipationId::from_uuid(FIRST_JOIN_PARTICIPATION)?,
        participant: FIRST_JOIN_PARTICIPANT,
        activity: ActivityId::from_uuid(FIRST_JOIN_ACTIVITY),
        agent: AgentInstanceId::from_uuid(FIRST_JOIN_AGENT),
        session: AgentSessionId::from_uuid(FIRST_JOIN_SESSION),
    })
}

fn seed_profile_session_alias(
    connection: &Connection,
    source: MemberIdentity,
) -> TestResult<MemberIdentity> {
    let alias = MemberIdentity {
        profile: source.profile,
        participation: source.participation,
        participant: Uuid::from_u128(0xEE21),
        activity: ActivityId::from_uuid(Uuid::from_u128(0xEE22)),
        agent: AgentInstanceId::from_uuid(Uuid::from_u128(0xEE23)),
        session: AgentSessionId::from_uuid(Uuid::from_u128(0xEE24)),
    };
    connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,display_name) VALUES(?1,?2,?3,'Reconnect fixture')",
        params![WORKSPACE.into_uuid().as_bytes(), alias.participant.as_bytes(), alias.session.into_uuid().as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,2,?7,2,102)",
        params![Uuid::from_u128(0xEE25).as_bytes(), WORKSPACE.into_uuid().as_bytes(), alias.participation.into_uuid().as_bytes(), alias.profile.as_bytes(), Uuid::from_u128(0xEE26).as_bytes(), alias.session.into_uuid().as_bytes(), alias.participant.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
        params![WORKSPACE.into_uuid().as_bytes(), alias.participant.as_bytes(), alias.activity.into_uuid().as_bytes(), alias.session.into_uuid().as_bytes(), alias.agent.into_uuid().as_bytes(), Uuid::from_u128(0xEE27).as_bytes()],
    )?;
    Ok(alias)
}

fn seed_other_profile_session(connection: &Connection) -> TestResult<OperationalActor> {
    let actor = MemberIdentity {
        profile: Uuid::from_u128(0xEE51),
        participation: WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xEE52))?,
        participant: Uuid::from_u128(0xEE53),
        activity: ActivityId::from_uuid(Uuid::from_u128(0xEE54)),
        agent: AgentInstanceId::from_uuid(Uuid::from_u128(0xEE55)),
        session: AgentSessionId::from_uuid(Uuid::from_u128(0xEE56)),
    };
    crate::teams::tests::seed_participant_identity(
        connection,
        WORKSPACE,
        actor.profile,
        actor.participation.into_uuid(),
        actor.participant,
        actor.activity.into_uuid(),
        actor.agent.into_uuid(),
        actor.session.into_uuid(),
        Uuid::from_u128(0xEE57),
    )?;
    Ok(actor.actor())
}

fn seed_other_workspace_profile(connection: &Connection) -> TestResult<MemberIdentity> {
    let actor = MemberIdentity {
        profile: Uuid::from_u128(0xEE61),
        participation: WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xEE62))?,
        participant: Uuid::from_u128(0xEE63),
        activity: ActivityId::from_uuid(Uuid::from_u128(0xEE64)),
        agent: AgentInstanceId::from_uuid(Uuid::from_u128(0xEE65)),
        session: AgentSessionId::from_uuid(Uuid::from_u128(0xEE66)),
    };
    crate::teams::tests::seed_participant_identity(
        connection,
        WORKSPACE_TWO,
        actor.profile,
        actor.participation.into_uuid(),
        actor.participant,
        actor.activity.into_uuid(),
        actor.agent.into_uuid(),
        actor.session.into_uuid(),
        Uuid::from_u128(0xEE67),
    )?;
    Ok(actor)
}
