use std::error::Error;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, params};
use uuid::Uuid;

mod last_seat_race;

use crate::{
    OperationalActor, OperationsCatalog, OperationsError, TeamCreateCommand, TeamRosterMemberInput,
    team_expansion::TeamMemberCapacity,
    temporary_dispatch::{
        TemporaryDispatchAdmissionRequest, TemporaryDispatchAdmissionResult,
        TemporaryDispatchConsentCommand, TemporaryDispatchConsentKind,
        TemporaryDispatchConsentWithdrawalCommand, TemporaryDispatchRequestedScope,
        TemporaryDispatchReserveCommand,
        lifecycle::{TemporaryDispatchAdmissionBinding, TemporaryDispatchReservationState},
    },
};

const WORKSPACE: WorkspaceId = WorkspaceId::from_uuid(Uuid::from_u128(0x1000));
const DESTINATION: Uuid = Uuid::from_u128(0x9000);
const RECEIVING_TEAM_A: Uuid = Uuid::from_u128(0xD100);
const HOME_TEAM: Uuid = Uuid::from_u128(0xD200);
const RECEIVING_TEAM_B: Uuid = Uuid::from_u128(0xD300);

type TestResult = Result<(), Box<dyn Error>>;

#[derive(Debug, Clone, Copy)]
struct Actor {
    profile: Uuid,
    participation: Uuid,
    participant: Uuid,
    activity: ActivityId,
    agent: AgentInstanceId,
    session: AgentSessionId,
}

impl Actor {
    fn new(seed: u128) -> Self {
        Self {
            profile: id(0xD400 + seed),
            participation: id(0xD500 + seed),
            participant: id(0xD600 + seed),
            activity: ActivityId::from_uuid(id(0xD700 + seed)),
            agent: AgentInstanceId::from_uuid(id(0xD800 + seed)),
            session: AgentSessionId::from_uuid(id(0xD900 + seed)),
        }
    }

    fn input(self) -> OperationalActor {
        OperationalActor::Participant {
            activity_id: self.activity,
            agent_id: self.agent,
            session_id: self.session,
        }
    }

    fn roster(self) -> TeamRosterMemberInput {
        TeamRosterMemberInput {
            activity_id: self.activity,
            agent_id: self.agent,
            session_id: self.session,
            role: "member".to_owned(),
        }
    }

    fn seed(self, connection: &Connection) -> TestResult {
        crate::teams::tests::seed_participant_identity(
            connection,
            WORKSPACE,
            self.profile,
            self.participation,
            self.participant,
            self.activity.into_uuid(),
            self.agent.into_uuid(),
            self.session.into_uuid(),
            id(0xDA00 + (self.profile.as_u128() & 0xff)),
        )?;
        Ok(())
    }
}

struct Fixture {
    catalog: OperationsCatalog,
    receiver_a_leader: Actor,
    receiver_b_leader: Actor,
    home_leader: Actor,
    target_a: Actor,
    orchestrator: Actor,
}

fn fixture(include_home_leader: bool, receiver_a_size: usize) -> Result<Fixture, Box<dyn Error>> {
    let connection = crate::teams::tests::create_connection(None)?;
    fixture_from_connection(connection, include_home_leader, receiver_a_size, true)
}

fn fixture_from_connection(
    connection: Connection,
    include_home_leader: bool,
    receiver_a_size: usize,
    include_receiver_b: bool,
) -> Result<Fixture, Box<dyn Error>> {
    // Alias actors below use Registration-origin effect bindings. This fixture
    // needs the real Registration read schema even when it has no request row.
    crate::registration::create_schema(&connection)?;
    crate::registration_profile::create_schema(&connection)?;
    crate::election_subject::create_schema(&connection)?;
    crate::temporary_dispatch::create_schema(&connection)?;
    crate::teams::tests::ensure_team_expansion_schema(&connection)?;
    let transaction = connection.unchecked_transaction()?;
    crate::team_expansion::create_workspace_hold_schema(&transaction)?;
    crate::team_expansion::create_legacy_slot_recovery_schema(&transaction)?;
    transaction.commit()?;
    crate::teams::tests::ensure_test_workspace_orchestrator(&connection)?;
    let actors = [
        Actor::new(1),
        Actor::new(2),
        Actor::new(3),
        Actor::new(4),
        Actor::new(5),
        Actor::new(6),
        Actor::new(7),
        Actor::new(8),
        Actor::new(9),
        Actor::new(10),
        Actor::new(11),
        Actor::new(12),
        Actor::new(13),
        Actor::new(14),
    ];
    for (index, actor) in actors.into_iter().enumerate() {
        if !include_receiver_b && matches!(index, 7 | 8) {
            continue;
        }
        actor.seed(&connection)?;
    }
    let mut catalog = OperationsCatalog {
        connection,
        destination: DESTINATION,
    };
    seed_fixture_teams(
        &mut catalog,
        &actors,
        include_home_leader,
        receiver_a_size,
        include_receiver_b,
    )?;
    Ok(Fixture {
        catalog,
        receiver_a_leader: actors[0],
        receiver_b_leader: actors[7],
        home_leader: actors[9],
        target_a: actors[10],
        orchestrator: actors[12],
    })
}

fn seed_fixture_teams(
    catalog: &mut OperationsCatalog,
    actors: &[Actor; 14],
    include_home_leader: bool,
    receiver_a_size: usize,
    include_receiver_b: bool,
) -> TestResult {
    let first_receiver_chair = actors[0];
    let second_receiver_chair = actors[7];
    let receiver_b_peer = actors[8];
    let home_leader = actors[9];
    let target_a = actors[10];
    let target_b = actors[11];
    let orchestrator = actors[12];
    let mut receiver_a = vec![first_receiver_chair];
    for actor in actors
        .iter()
        .copied()
        .skip(1)
        .take(receiver_a_size.saturating_sub(1))
    {
        receiver_a.push(actor);
    }
    create_team(catalog, RECEIVING_TEAM_A, 0xDA10, "Receiver A", &receiver_a)?;
    if include_receiver_b {
        create_team(
            catalog,
            RECEIVING_TEAM_B,
            0xDA11,
            "Receiver B",
            &[second_receiver_chair, receiver_b_peer],
        )?;
    }
    let mut home = vec![target_a, target_b];
    if include_home_leader {
        home.push(home_leader);
    }
    create_team(catalog, HOME_TEAM, 0xDA12, "Home", &home)?;
    seed_leadership(
        &catalog.connection,
        WorkspaceLeadership::Team(RECEIVING_TEAM_A),
        first_receiver_chair,
        1,
    )?;
    if include_receiver_b {
        seed_leadership(
            &catalog.connection,
            WorkspaceLeadership::Team(RECEIVING_TEAM_B),
            second_receiver_chair,
            1,
        )?;
    }
    if include_home_leader {
        seed_leadership(
            &catalog.connection,
            WorkspaceLeadership::Team(HOME_TEAM),
            home_leader,
            1,
        )?;
    }
    seed_leadership(
        &catalog.connection,
        WorkspaceLeadership::Workspace,
        orchestrator,
        2,
    )?;
    Ok(())
}

#[derive(Clone, Copy)]
enum WorkspaceLeadership {
    Workspace,
    Team(Uuid),
}

fn create_team(
    catalog: &mut OperationsCatalog,
    team_id: Uuid,
    request_id: u128,
    name: &str,
    members: &[Actor],
) -> TestResult {
    let _record = catalog.create_team_with_capacity(
        TeamCreateCommand {
            request_id: id(request_id),
            team_id,
            workspace_id: WORKSPACE,
            actor: OperationalActor::Participant {
                activity_id: ActivityId::from_uuid(id(0x6000)),
                agent_id: AgentInstanceId::from_uuid(id(0x7000)),
                session_id: AgentSessionId::from_uuid(id(0x8000)),
            },
            expected_sender_leadership_revision: Some(1),
            name: name.to_owned(),
            purpose: "Synthetic lifecycle owner fixture".to_owned(),
            initial_members: members.iter().copied().map(Actor::roster).collect(),
            observed_at: time(10)?,
        },
        TeamMemberCapacity::new(8)?,
    )?;
    Ok(())
}

fn seed_leadership(
    connection: &Connection,
    scope: WorkspaceLeadership,
    leader: Actor,
    revision: i64,
) -> Result<(), rusqlite::Error> {
    let (scope_kind, team_id) = match scope {
        WorkspaceLeadership::Workspace => (0_i64, None),
        WorkspaceLeadership::Team(team_id) => (2_i64, Some(team_id)),
    };
    let team_value = team_id.map(|team| team.as_bytes().to_vec());
    let receipt_team = team_id.map_or_else(|| vec![0_u8; 16], |team| team.as_bytes().to_vec());
    let existing: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS NULL AND team_id IS ?3)",
        params![WORKSPACE.into_uuid().as_bytes(), scope_kind, team_value],
        |row| row.get(0),
    )?;
    if existing {
        connection.execute(
            "UPDATE ops_leadership SET state='ACTIVE',leader_participant_id=?1,leadership_revision=?2,stabilization_until_ms=NULL,current_election_id=NULL,updated_at_ms=100 WHERE workspace_id=?3 AND scope_kind=?4 AND activity_id IS NULL AND team_id IS ?5",
            params![
                leader.participant.as_bytes(),
                revision,
                WORKSPACE.into_uuid().as_bytes(),
                scope_kind,
                team_value,
            ],
        )?;
    } else {
        connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,?2,NULL,?3,'ACTIVE',?4,?5,NULL,NULL,100)",
            params![
                WORKSPACE.into_uuid().as_bytes(),
                scope_kind,
                team_value,
                leader.participant.as_bytes(),
                revision,
            ],
        )?;
    }
    let receipt_ordinal: i64 = connection.query_row(
        "SELECT COALESCE(MAX(receipt_ordinal),0)+1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id=zeroblob(16) AND team_id=?3",
        params![WORKSPACE.into_uuid().as_bytes(), scope_kind, receipt_team],
        |row| row.get(0),
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,?2,zeroblob(16),?3,?4,'CONFIRMED',NULL,NULL,?5,?6,100)",
        params![
            WORKSPACE.into_uuid().as_bytes(),
            scope_kind,
            receipt_team,
            receipt_ordinal,
            leader.participant.as_bytes(),
            revision,
        ],
    )?;
    Ok(())
}

fn admit(
    fixture: &mut Fixture,
    request_id: u128,
    receiving_team_id: Uuid,
    leader: Actor,
    target: Actor,
) -> Result<TemporaryDispatchAdmissionBinding, Box<dyn Error>> {
    let request = TemporaryDispatchAdmissionRequest::new(
        id(request_id),
        WORKSPACE,
        receiving_team_id,
        1,
        leader.input(),
        target.participant,
        TemporaryDispatchRequestedScope::new("one bounded request intent".to_owned())?,
        time(10_000)?,
    )?;
    let result = fixture
        .catalog
        .admit_temporary_dispatch(&request, || time(100))?;
    let TemporaryDispatchAdmissionResult::Admitted(record) = result else {
        return Err("fixture request unexpectedly quota-rejected".into());
    };
    Ok(TemporaryDispatchAdmissionBinding::from_record(&record))
}

fn consent(
    catalog: &mut OperationsCatalog,
    binding: &TemporaryDispatchAdmissionBinding,
    actor: Actor,
    kind: TemporaryDispatchConsentKind,
    receipt_id: u128,
) -> Result<crate::temporary_dispatch::TemporaryDispatchConsentReceipt, OperationsError> {
    let command = TemporaryDispatchConsentCommand::new(
        id(receipt_id),
        binding.clone(),
        kind,
        0,
        actor.input(),
    )?;
    catalog.record_temporary_dispatch_consent(&command, || time(200))
}

fn all_consents(
    fixture: &mut Fixture,
    binding: &TemporaryDispatchAdmissionBinding,
    target: Actor,
    seed: u128,
) -> Result<
    (
        crate::temporary_dispatch::TemporaryDispatchConsentReceipt,
        crate::temporary_dispatch::TemporaryDispatchConsentReceipt,
        crate::temporary_dispatch::TemporaryDispatchConsentReceipt,
    ),
    OperationsError,
> {
    let target_consent = consent(
        &mut fixture.catalog,
        binding,
        target,
        TemporaryDispatchConsentKind::TargetAcceptance,
        seed,
    )?;
    let home_consent = consent(
        &mut fixture.catalog,
        binding,
        fixture.home_leader,
        TemporaryDispatchConsentKind::HomeLeaderConsent,
        seed + 1,
    )?;
    let orchestrator_consent = consent(
        &mut fixture.catalog,
        binding,
        fixture.orchestrator,
        TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval,
        seed + 2,
    )?;
    Ok((target_consent, home_consent, orchestrator_consent))
}

fn reserve(
    fixture: &mut Fixture,
    binding: &TemporaryDispatchAdmissionBinding,
) -> Result<crate::temporary_dispatch::TemporaryDispatchReservationReceipt, OperationsError> {
    let command =
        TemporaryDispatchReserveCommand::new(binding.clone(), 0, fixture.orchestrator.input())?;
    fixture
        .catalog
        .reserve_temporary_dispatch(&command, || time(300))
}

#[test]
fn consent_rejects_foreign_role_and_changed_home_leader_epoch() -> TestResult {
    let mut fixture = fixture(true, 2)?;
    let receiving_leader = fixture.receiver_a_leader;
    let target = fixture.target_a;
    let home_leader = fixture.home_leader;
    let binding = admit(
        &mut fixture,
        0xDB01,
        RECEIVING_TEAM_A,
        receiving_leader,
        target,
    )?;
    let foreign = consent(
        &mut fixture.catalog,
        &binding,
        receiving_leader,
        TemporaryDispatchConsentKind::HomeLeaderConsent,
        0xDB10,
    );
    assert!(matches!(foreign, Err(OperationsError::Conflict(_))));

    let _consents = all_consents(&mut fixture, &binding, target, 0xDB20)?;
    bump_leadership_epoch(&fixture.catalog.connection, HOME_TEAM, home_leader, 2)?;
    assert!(matches!(
        reserve(&mut fixture, &binding),
        Err(OperationsError::Conflict(_))
    ));
    let historical: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_consent_receipts WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(historical, 3);
    Ok(())
}

#[test]
fn admission_binding_read_is_scoped_to_the_exact_consent_authority() -> TestResult {
    let mut fixture = fixture(true, 2)?;
    let receiving_leader = fixture.receiver_a_leader;
    let target = fixture.target_a;
    let home_leader = fixture.home_leader;
    let orchestrator = fixture.orchestrator;
    let foreign_leader = fixture.receiver_b_leader;
    let binding = admit(
        &mut fixture,
        0xDB08,
        RECEIVING_TEAM_A,
        receiving_leader,
        target,
    )?;

    for (kind, actor) in [
        (TemporaryDispatchConsentKind::TargetAcceptance, target),
        (TemporaryDispatchConsentKind::HomeLeaderConsent, home_leader),
        (
            TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval,
            orchestrator,
        ),
    ] {
        let resolved = fixture.catalog.temporary_dispatch_consent_binding(
            WORKSPACE,
            binding.request_id(),
            kind,
            actor.input(),
        )?;
        assert_eq!(resolved, binding);
    }

    assert!(matches!(
        fixture.catalog.temporary_dispatch_consent_binding(
            WORKSPACE,
            binding.request_id(),
            TemporaryDispatchConsentKind::TargetAcceptance,
            foreign_leader.input(),
        ),
        Err(OperationsError::Conflict(_))
    ));

    assert_eq!(
        fixture.catalog.temporary_dispatch_reservation_binding(
            WORKSPACE,
            binding.request_id(),
            receiving_leader.input(),
        )?,
        binding
    );
    assert_eq!(
        fixture.catalog.temporary_dispatch_reservation_binding(
            WORKSPACE,
            binding.request_id(),
            orchestrator.input(),
        )?,
        binding
    );
    assert!(matches!(
        fixture.catalog.temporary_dispatch_reservation_binding(
            WORKSPACE,
            binding.request_id(),
            home_leader.input(),
        ),
        Err(OperationsError::Conflict(_))
    ));
    assert!(matches!(
        fixture.catalog.temporary_dispatch_consent_binding(
            WorkspaceId::from_uuid(id(0xDEAD)),
            binding.request_id(),
            TemporaryDispatchConsentKind::TargetAcceptance,
            target.input(),
        ),
        Err(OperationsError::Conflict(_))
    ));

    let receipt_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_consent_receipts WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    let reservation_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!((receipt_count, reservation_count), (0, 0));
    Ok(())
}

#[test]
fn original_home_leader_can_recover_historical_consent_and_withdrawal_after_role_change()
-> TestResult {
    let mut fixture = fixture(true, 2)?;
    let requester = fixture.receiver_a_leader;
    let target = fixture.target_a;
    let former_home_leader = fixture.home_leader;
    let replacement_leader = fixture.receiver_b_leader;
    let binding = admit(&mut fixture, 0xDB0B, RECEIVING_TEAM_A, requester, target)?;
    let consent_receipt = consent(
        &mut fixture.catalog,
        &binding,
        former_home_leader,
        TemporaryDispatchConsentKind::HomeLeaderConsent,
        0xDB0C,
    )?;
    let withdrawal_command = TemporaryDispatchConsentWithdrawalCommand::new(
        id(0xDB0D),
        consent_receipt.receipt_id(),
        binding.clone(),
        consent_receipt.revision(),
        former_home_leader.input(),
    )?;
    let withdrawal_receipt = fixture
        .catalog
        .withdraw_temporary_dispatch_consent(&withdrawal_command, || time(250))?;

    // Role replacement is simulated directly in this isolated owner fixture.
    // It does not claim to run the live Broker/leadership handoff.
    replace_team_leader(
        &fixture.catalog.connection,
        HOME_TEAM,
        replacement_leader,
        2,
    )?;
    assert!(matches!(
        fixture.catalog.temporary_dispatch_consent_binding(
            WORKSPACE,
            binding.request_id(),
            TemporaryDispatchConsentKind::HomeLeaderConsent,
            former_home_leader.input(),
        ),
        Err(OperationsError::Conflict(_))
    ));

    let historical_binding = fixture
        .catalog
        .temporary_dispatch_consent_readback_binding(
            WORKSPACE,
            binding.request_id(),
            consent_receipt.receipt_id(),
            TemporaryDispatchConsentKind::HomeLeaderConsent,
            former_home_leader.input(),
        )?
        .ok_or("former author could not recover historical consent binding")?;
    assert_eq!(historical_binding, binding);
    assert_eq!(
        fixture.catalog.read_temporary_dispatch_consent(
            &historical_binding,
            consent_receipt.receipt_id(),
            former_home_leader.input(),
        )?,
        Some(consent_receipt)
    );

    let historical_withdrawal_binding = fixture
        .catalog
        .temporary_dispatch_consent_withdrawal_readback_binding(
            WORKSPACE,
            binding.request_id(),
            withdrawal_receipt.receipt_id(),
            withdrawal_receipt.consent_receipt_id(),
            TemporaryDispatchConsentKind::HomeLeaderConsent,
            former_home_leader.input(),
        )?
        .ok_or("former author could not recover historical withdrawal binding")?;
    assert_eq!(historical_withdrawal_binding, binding);
    assert_eq!(
        fixture.catalog.read_temporary_dispatch_consent_withdrawal(
            &historical_withdrawal_binding,
            withdrawal_receipt.receipt_id(),
            withdrawal_receipt.consent_receipt_id(),
            former_home_leader.input(),
        )?,
        Some(withdrawal_receipt)
    );
    Ok(())
}

#[test]
fn reservation_readback_authorizes_before_exposing_state_only_corruption() -> TestResult {
    let mut fixture = fixture(true, 2)?;
    let requester = fixture.receiver_a_leader;
    let unrelated = fixture.receiver_b_leader;
    let target = fixture.target_a;
    let binding = admit(&mut fixture, 0xDB09, RECEIVING_TEAM_A, requester, target)?;

    let Err(before) = fixture
        .catalog
        .read_temporary_dispatch_reservation(&binding, unrelated.input())
    else {
        return Err("unrelated Team leader read a missing reservation".into());
    };
    assert!(matches!(before, OperationsError::Conflict(_)));

    // Deliberately corrupt this isolated test catalog: the state-only row
    // cannot be produced by the real atomic receipt+state writer.
    fixture.catalog.connection.execute(
        "INSERT INTO ops_temporary_dispatch_reservations(request_id,workspace_id,receiving_team_id,home_team_id,target_participant_id,target_participation_id,target_profile_id,requested_scope,requested_end_at_ms,intent_digest,state,revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'RESERVED',1,100)",
        params![
            binding.request_id().as_bytes(),
            binding.workspace_id().into_uuid().as_bytes(),
            binding.requesting_team_id().as_bytes(),
            binding.target_home_team_id().as_bytes(),
            binding.target_participant_id().as_bytes(),
            target.participation.as_bytes(),
            target.profile.as_bytes(),
            binding.requested_scope().as_str(),
            binding.requested_end_at().get(),
            binding.intent_digest().as_slice(),
        ],
    )?;

    let Err(after) = fixture
        .catalog
        .read_temporary_dispatch_reservation(&binding, unrelated.input())
    else {
        return Err("unrelated Team leader read state-only reservation".into());
    };
    assert_eq!(format!("{before:?}"), format!("{after:?}"));
    assert!(matches!(
        fixture
            .catalog
            .read_temporary_dispatch_reservation(&binding, requester.input()),
        Err(OperationsError::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn requester_can_recover_absent_reservation_after_workspace_leader_disappears() -> TestResult {
    let mut fixture = fixture(true, 2)?;
    let requester = fixture.receiver_a_leader;
    let workspace_leader = fixture.orchestrator;
    let target = fixture.target_a;
    let binding = admit(&mut fixture, 0xDB0A, RECEIVING_TEAM_A, requester, target)?;
    assert!(
        fixture
            .catalog
            .read_temporary_dispatch_reservation(&binding, requester.input())?
            .is_none()
    );

    // Simulate the independent Workspace Leader disappearing after the
    // request. This row removal is fixture fault setup, not an Operations
    // leadership command or a claim about normal lifecycle behavior.
    fixture.catalog.connection.execute(
        "DELETE FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=0",
        params![WORKSPACE.into_uuid().as_bytes()],
    )?;

    assert_eq!(
        fixture.catalog.temporary_dispatch_reservation_binding(
            WORKSPACE,
            binding.request_id(),
            requester.input(),
        )?,
        binding
    );
    assert!(
        fixture
            .catalog
            .read_temporary_dispatch_reservation(&binding, requester.input())?
            .is_none()
    );
    assert!(matches!(
        fixture.catalog.temporary_dispatch_reservation_binding(
            WORKSPACE,
            binding.request_id(),
            workspace_leader.input(),
        ),
        Err(OperationsError::Conflict(_))
    ));
    Ok(())
}

#[test]
fn missing_home_leader_holds_new_dispatch_without_releasing_or_reserving() -> TestResult {
    let mut fixture = fixture(false, 2)?;
    let receiving_leader = fixture.receiver_a_leader;
    let target = fixture.target_a;
    let home_leader = fixture.home_leader;
    let orchestrator = fixture.orchestrator;
    let binding = admit(
        &mut fixture,
        0xDB31,
        RECEIVING_TEAM_A,
        receiving_leader,
        target,
    )?;
    let _target = consent(
        &mut fixture.catalog,
        &binding,
        target,
        TemporaryDispatchConsentKind::TargetAcceptance,
        0xDB32,
    )?;
    let missing = consent(
        &mut fixture.catalog,
        &binding,
        home_leader,
        TemporaryDispatchConsentKind::HomeLeaderConsent,
        0xDB33,
    );
    assert!(matches!(missing, Err(OperationsError::Conflict(_))));
    let _orchestrator = consent(
        &mut fixture.catalog,
        &binding,
        orchestrator,
        TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval,
        0xDB34,
    )?;
    assert!(matches!(
        reserve(&mut fixture, &binding),
        Err(OperationsError::Conflict(_))
    ));
    let reservations: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(reservations, 0);
    let admitted: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(admitted, 1);
    Ok(())
}

#[test]
fn withdrawn_target_acceptance_is_historical_and_blocks_reservation() -> TestResult {
    let mut fixture = fixture(true, 2)?;
    let receiving_leader = fixture.receiver_a_leader;
    let target_actor = fixture.target_a;
    let binding = admit(
        &mut fixture,
        0xDB41,
        RECEIVING_TEAM_A,
        receiving_leader,
        target_actor,
    )?;
    let (target, _home, _orchestrator) =
        all_consents(&mut fixture, &binding, target_actor, 0xDB50)?;
    let command = TemporaryDispatchConsentWithdrawalCommand::new(
        id(0xDB60),
        target.receipt_id(),
        binding.clone(),
        target.revision(),
        target_actor.input(),
    )?;
    let withdrawn = fixture
        .catalog
        .withdraw_temporary_dispatch_consent(&command, || time(250))?;
    assert!(!withdrawn.replayed());
    let replay = fixture
        .catalog
        .withdraw_temporary_dispatch_consent(&command, || {
            Err(OperationsError::Conflict("replay must not sample time"))
        })?;
    assert!(replay.replayed());
    assert!(matches!(
        reserve(&mut fixture, &binding),
        Err(OperationsError::Conflict(_))
    ));
    let (consents, withdrawals, reservations): (i64, i64, i64) = fixture.catalog.connection.query_row(
        "SELECT (SELECT COUNT(*) FROM ops_temporary_dispatch_consent_receipts WHERE request_id=?1),(SELECT COUNT(*) FROM ops_temporary_dispatch_consent_withdrawals WHERE request_id=?1),(SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE request_id=?1)",
        params![binding.request_id().as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!((consents, withdrawals, reservations), (3, 1, 0));
    Ok(())
}

#[test]
fn reserve_replay_does_not_add_workspace_registration_or_quota_entries() -> TestResult {
    let mut fixture = fixture(true, 2)?;
    let receiving_leader = fixture.receiver_a_leader;
    let target_actor = fixture.target_a;
    let orchestrator = fixture.orchestrator;
    let participation_count_before: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_workspace_participations WHERE workspace_id=?1",
        params![WORKSPACE.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let binding = admit(
        &mut fixture,
        0xDB71,
        RECEIVING_TEAM_A,
        receiving_leader,
        target_actor,
    )?;
    let _consents = all_consents(&mut fixture, &binding, target_actor, 0xDB80)?;
    let reservation = reserve(&mut fixture, &binding)?;
    assert_eq!(
        reservation.state(),
        TemporaryDispatchReservationState::Reserved
    );
    let command = TemporaryDispatchReserveCommand::new(binding.clone(), 0, orchestrator.input())?;
    let replay = fixture.catalog.reserve_temporary_dispatch(&command, || {
        Err(OperationsError::Conflict("replay must not sample time"))
    })?;
    assert!(replay.replayed());
    let participation_count_after: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_workspace_participations WHERE workspace_id=?1",
        params![WORKSPACE.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let (request_count, quota_sequence, reservation_count): (i64, i64, i64) = fixture.catalog.connection.query_row(
        "SELECT (SELECT COUNT(*) FROM ops_temporary_dispatch_requests WHERE request_id=?1),(SELECT last_sequence FROM ops_temporary_dispatch_quota_state WHERE workspace_id=?2 AND requesting_team_id=?3),(SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE request_id=?1 AND state='RESERVED')",
        params![binding.request_id().as_bytes(), WORKSPACE.into_uuid().as_bytes(), RECEIVING_TEAM_A.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(participation_count_after, participation_count_before);
    assert_eq!(
        (request_count, quota_sequence, reservation_count),
        (1, 1, 1)
    );
    assert_eq!(reservation.revision(), 1);
    Ok(())
}

#[test]
fn same_profile_session_alias_cross_team_request_is_pending_and_consumes_no_second_seat()
-> TestResult {
    let mut fixture = fixture(true, 2)?;
    let requesting_leader_for_a = fixture.receiver_a_leader;
    let requesting_leader_for_b = fixture.receiver_b_leader;
    let target_a = fixture.target_a;
    let binding_a = admit(
        &mut fixture,
        0xDB91,
        RECEIVING_TEAM_A,
        requesting_leader_for_a,
        target_a,
    )?;
    let _consents_a = all_consents(&mut fixture, &binding_a, target_a, 0xDBA0)?;
    let reserved = reserve(&mut fixture, &binding_a)?;
    assert_eq!(
        reserved.state(),
        TemporaryDispatchReservationState::Reserved
    );

    let alias = Actor {
        participant: id(0xDBB0),
        activity: ActivityId::from_uuid(id(0xDBB1)),
        agent: AgentInstanceId::from_uuid(id(0xDBB2)),
        session: AgentSessionId::from_uuid(id(0xDBB3)),
        ..target_a
    };
    seed_profile_session_alias(&fixture.catalog.connection, alias)?;
    let binding_b = admit(
        &mut fixture,
        0xDBB4,
        RECEIVING_TEAM_B,
        requesting_leader_for_b,
        alias,
    )?;
    let _consents_b = all_consents(&mut fixture, &binding_b, alias, 0xDBC0)?;
    let pending = reserve(&mut fixture, &binding_b)?;
    assert_eq!(
        pending.state(),
        TemporaryDispatchReservationState::PendingConflict
    );
    let (reserved_a, reserved_b, pending_b): (i64, i64, i64) = fixture.catalog.connection.query_row(
        "SELECT (SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE receiving_team_id=?1 AND state='RESERVED'),(SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE receiving_team_id=?2 AND state='RESERVED'),(SELECT COUNT(*) FROM ops_temporary_dispatch_reservations WHERE receiving_team_id=?2 AND state='PENDING_CONFLICT')",
        params![RECEIVING_TEAM_A.as_bytes(), RECEIVING_TEAM_B.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!((reserved_a, reserved_b, pending_b), (1, 0, 1));
    Ok(())
}

fn seed_profile_session_alias(
    connection: &Connection,
    alias: Actor,
) -> Result<(), rusqlite::Error> {
    // Test-only logical identity fixture: this second Session/participant alias
    // shares the exact Profile and Workspace participation anchor.
    connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,display_name) VALUES(?1,?2,?3,'same Profile alias')",
        params![WORKSPACE.into_uuid().as_bytes(), alias.participant.as_bytes(), alias.session.into_uuid().as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
        params![WORKSPACE.into_uuid().as_bytes(), alias.participant.as_bytes(), alias.activity.into_uuid().as_bytes(), alias.session.into_uuid().as_bytes(), alias.agent.into_uuid().as_bytes(), id(0xDBB5).as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,101)",
        params![id(0xDBB6).as_bytes(), WORKSPACE.into_uuid().as_bytes(), alias.participation.as_bytes(), alias.profile.as_bytes(), id(0xDBB7).as_bytes(), alias.session.into_uuid().as_bytes(), alias.participant.as_bytes()],
    )?;
    Ok(())
}

fn bump_leadership_epoch(
    connection: &Connection,
    team_id: Uuid,
    leader: Actor,
    revision: i64,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "UPDATE ops_leadership SET leadership_revision=?1,updated_at_ms=200 WHERE workspace_id=?2 AND scope_kind=2 AND team_id=?3",
        params![revision, WORKSPACE.into_uuid().as_bytes(), team_id.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,2,'CONFIRMED',NULL,NULL,?3,?4,200)",
        params![WORKSPACE.into_uuid().as_bytes(), team_id.as_bytes(), leader.participant.as_bytes(), revision],
    )?;
    Ok(())
}

fn replace_team_leader(
    connection: &Connection,
    team_id: Uuid,
    leader: Actor,
    revision: i64,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "UPDATE ops_leadership SET leader_participant_id=?1,leadership_revision=?2,updated_at_ms=250 WHERE workspace_id=?3 AND scope_kind=2 AND team_id=?4",
        params![leader.participant.as_bytes(), revision, WORKSPACE.into_uuid().as_bytes(), team_id.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,2,'CONFIRMED',NULL,NULL,?3,?4,250)",
        params![WORKSPACE.into_uuid().as_bytes(), team_id.as_bytes(), leader.participant.as_bytes(), revision],
    )?;
    Ok(())
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn time(value: i64) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Conflict("test timestamp"))
}
