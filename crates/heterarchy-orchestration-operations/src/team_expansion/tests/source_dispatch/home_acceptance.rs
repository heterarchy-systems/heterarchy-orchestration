//! Native Home admission/abort composition. Initial roster/import/coordinator
//! facts are synthetic, as in the parent fixture. Preparation, Source proposal
//! and abort, Session/Profile holds and Operations admission use actual owners.
//! This does not prove authenticated HTTP, Source Apply, or provider execution.

use super::*;
use crate::home_acceptance::*;
use forge_domain::{HomeHandoffProposalRequest, HomeHandoffTakeRequest};
use forge_persistence::{HomeHandoffAcceptanceOutcomeRecord, HomeHandoffProposalRecord};
use heterarchy_orchestration_sessions::{
    AgentProfile, HomeAcceptanceProfileAcquireOutcome, HomeAcceptanceProfileIntent,
    HomeAcceptanceProfilePinRecord, HomeAcceptanceSessionAcquireOutcome,
    HomeAcceptanceSessionIntent, HomeAcceptanceSessionRecord,
};

mod owners;
mod source;
mod tf_reservation;

const RECIPIENT_AGENT: Uuid = Uuid::from_u128(0xAC02);

fn ts(value: i64) -> SourceDispatchResult<TimestampMillis> {
    Ok(TimestampMillis::new(value)?)
}

struct Fixture {
    root: PathBuf,
    operations: SourceDispatchFixture,
    source: source::Source,
    sessions: AgentSessionCatalog,
    profiles: AgentProfileCatalog,
    profile: AgentProfile,
    session: RegisteredAgentSession,
    proposal: HomeHandoffProposalRecord,
    take: HomeHandoffTakeRequest,
    identity: HomeAcceptanceSourceIdentity,
}

impl Fixture {
    fn new(team_work: bool) -> SourceDispatchResult<Self> {
        let root = std::env::temp_dir().join(format!("home-native-owners-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let mut operations =
            prepare_source_dispatch_fixture(Some(&root.join("operations.sqlite3")))?;
        let mut source = source::Source::new(&root, &operations.preparation)?;
        seed_recipient_alias(&operations.transfer.catalog.connection)?;
        record_original_affinity(&mut operations, &source, team_work)?;
        let (profiles, sessions, profile, session) = owners::create(&root, &operations)?;
        let command = source_dispatch_command(
            &operations.preparation,
            Uuid::from_u128(0xAC40),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51_020),
            source.scope.clone(),
            SOURCE_ISSUER,
            DESTINATION,
            510,
            900,
            Uuid::from_u128(0xAC41),
            [0xA1; 32],
            128,
        )?;
        let dispatched = operations
            .transfer
            .catalog
            .commit_team_permanent_transfer_source_dispatch(&command, || timestamp(510))?;
        assert!(matches!(
            dispatched,
            TeamPermanentTransferSourceDispatchOutcome::Created(_)
        ));
        let prepared = source
            .store
            .prepare_home_transfer_source(
                command.source_request(),
                command.grant(),
                OrchestrationIssuerId::from_uuid(SOURCE_ISSUER),
                ForgeBackendId::from_uuid(SOURCE_BACKEND),
                DESTINATION,
                || ts(550).map_err(|_| forge_persistence::HomeTransferStoreError::Clock),
            )?
            .record()
            .clone();
        let original = TeamPermanentTransferSourceReceiptCommand::new(
            WORKSPACE,
            SOURCE_PREPARATION,
            command.envelope_digest(),
            ts(551)?,
            prepared,
        )?;
        let _recorded = operations
            .transfer
            .catalog
            .record_team_permanent_transfer_source_receipt(&original)?;
        let proposed = HomeHandoffProposalRequest::new(
            Uuid::from_u128(0xAC42),
            Uuid::from_u128(0xAC43),
            SOURCE_PREPARATION,
            source.assignment.id(),
            source.assignment.revision(),
            forge_domain::AgentAssignmentId::from_uuid(Uuid::from_u128(0xAC44)),
            source.actor,
            source.recipient,
            None,
            None,
        )?;
        let proposal = source
            .store
            .propose_home_assignment_handoff(&proposed, || {
                ts(560).map_err(|_| forge_persistence::HomeHandoffProposalStoreError::Clock)
            })?
            .record();
        let take = HomeHandoffTakeRequest::new(
            Uuid::from_u128(0xAC45),
            proposed.handoff_id(),
            proposed.proposal_id(),
            proposal.case_revision(),
            proposed.request_digest(),
            command.source_request().scope(),
            source.recipient,
        )?;
        let identity = HomeAcceptanceSourceIdentity::new(
            OrchestrationIssuerId::from_uuid(SOURCE_ISSUER),
            ForgeBackendId::from_uuid(SOURCE_BACKEND),
            DESTINATION,
        )?;
        Ok(Self {
            root,
            operations,
            source,
            sessions,
            profiles,
            profile,
            session,
            proposal,
            take,
            identity,
        })
    }

    fn request_for(&self, acceptance_id: Uuid) -> SourceDispatchResult<HomeHandoffTakeRequest> {
        Ok(HomeHandoffTakeRequest::new(
            acceptance_id,
            self.proposal.request().handoff_id(),
            self.proposal.request().proposal_id(),
            self.proposal.case_revision(),
            self.proposal.request().request_digest(),
            self.take.scope(),
            self.source.recipient,
        )?)
    }

    fn stage(&mut self) -> SourceDispatchResult<HomeAcceptanceIntentReceipt> {
        Ok(self.operations.transfer.catalog.stage_home_acceptance(
            self.identity,
            self.take,
            self.proposal,
            None,
            || timestamp(600),
        )?)
    }

    fn holds(
        &mut self,
    ) -> SourceDispatchResult<(HomeAcceptanceSessionRecord, HomeAcceptanceProfilePinRecord)> {
        self.holds_for(self.take)
    }

    fn holds_for(
        &mut self,
        take: HomeHandoffTakeRequest,
    ) -> SourceDispatchResult<(HomeAcceptanceSessionRecord, HomeAcceptanceProfilePinRecord)> {
        let session_intent = HomeAcceptanceSessionIntent::new(
            take,
            self.identity.issuer(),
            self.session.revision(),
        )?;
        let held_at = ts(620)?;
        let HomeAcceptanceSessionAcquireOutcome::Held {
            record: session, ..
        } = self.sessions.acquire_home_acceptance_session(
            &session_intent,
            self.session.principal(),
            self.identity.issuer(),
            || Ok(held_at),
        )?
        else {
            return Err("expected actual Session hold".into());
        };
        let profile_intent = HomeAcceptanceProfileIntent::new(
            take,
            self.identity.issuer(),
            Some((self.profile.id(), self.profile.revision())),
        )?;
        let held = session
            .held_reference()
            .ok_or("Session hold reference missing")?;
        let pinned_at = ts(630)?;
        let HomeAcceptanceProfileAcquireOutcome::Held {
            record: profile, ..
        } = self.profiles.acquire_home_acceptance_profile(
            &profile_intent,
            self.identity.issuer(),
            held,
            || Ok(pinned_at),
        )?
        else {
            return Err("expected actual Profile hold".into());
        };
        Ok((session, profile))
    }

    fn abort_source(&mut self) -> SourceDispatchResult<HomeHandoffAcceptanceOutcomeRecord> {
        self.abort_source_for(self.take)
    }

    fn abort_source_for(
        &mut self,
        take: HomeHandoffTakeRequest,
    ) -> SourceDispatchResult<HomeHandoffAcceptanceOutcomeRecord> {
        let source_time = ts(850)?;
        Ok(self.source.store.abort_unstarted_home_assignment_handoff(
            &take,
            self.identity.issuer(),
            self.identity.backend(),
            self.identity.destination(),
            || Ok(source_time),
        )?)
    }

    fn cleanup(self) -> SourceDispatchResult {
        let Self {
            root,
            operations,
            source,
            sessions,
            profiles,
            ..
        } = self;
        drop(operations);
        drop(source);
        drop(sessions);
        drop(profiles);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}

fn seed_recipient_alias(connection: &Connection) -> SourceDispatchResult {
    // An initial Ready alias of the existing stable recipient participation.
    // Registration/authentication of this bootstrap fact is outside this test.
    connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,1,?6)",
        params![WORKSPACE.into_uuid().as_bytes(),MEMBER_FOUR_PARTICIPANT.as_bytes(),MEMBER_SIX_ACTIVITY.as_bytes(),
            MEMBER_FOUR_SESSION.as_bytes(),RECIPIENT_AGENT.as_bytes(),Uuid::from_u128(0xAC46).as_bytes()],
    )?;
    Ok(())
}

fn record_original_affinity(
    operations: &mut SourceDispatchFixture,
    source: &source::Source,
    team_work: bool,
) -> SourceDispatchResult {
    let catalog = &mut operations.transfer.catalog;
    ensure_schema_objects(&catalog.connection, crate::node_team::schema_objects())?;
    ensure_schema_objects(
        &catalog.connection,
        crate::team_assignment::schema_objects(),
    )?;
    ensure_schema_objects(&catalog.connection, crate::effect_fact::schema_objects())?;
    let a = &source.assignment;
    let scope = crate::NodeTeamBindingScope::new(
        a.workspace_id(),
        a.activity_id(),
        a.goal_id(),
        a.goal_created_at(),
        a.dag_revision(),
        a.goal_revision(),
        a.node_id(),
    )?;
    let team = team_work.then_some(operations.preparation.source_team_id());
    let _node = catalog.set_node_team_binding(crate::SetNodeTeamBindingCommand {
        scope,
        expected_revision: None,
        team_id: team,
        actor: OperationalActor::LocalOperator,
        expected_sender_leadership_revision: None,
        observed_at: ts(503)?,
    })?;
    let _root = catalog.record_assignment_affinity(&crate::AssignmentAffinityCommand {
        workspace_id: a.workspace_id(),
        assignment_id: a.id(),
        activity_id: a.activity_id(),
        target_agent_id: a.target().agent_id(),
        target_session_id: a.target().session_id(),
        expected_target_principal: a.target().principal(),
        requester: OperationalActor::LocalOperator,
        team_id: team,
        node_scope: scope,
        expected_binding_revision: Some(1),
        assignment_payload_fingerprint: [0xA3; 32],
        original_offer_payload:
            b"synthetic original-offer transport payload; actual Source assignment identity above"
                .to_vec(),
        observed_at: ts(504)?,
    })?;
    Ok(())
}

#[test]
fn native_home_admission_uses_actual_holders_and_retains_original_grant_on_replay()
-> SourceDispatchResult {
    for team_work in [false, true] {
        let mut f = Fixture::new(team_work)?;
        exercise_native_abort_recovery(&mut f, team_work)?;
        f.cleanup()?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "Keeps one cross-owner acquisition, abort, ordered settlement and delayed-message recovery trace together."
)]
fn exercise_native_abort_recovery(f: &mut Fixture, team_work: bool) -> SourceDispatchResult {
    let _staged = f.stage()?;
    let original_affinity = f
        .operations
        .transfer
        .catalog
        .read_assignment_affinity(WORKSPACE, f.source.assignment.id())?;
    let (session, profile) = f.holds()?;
    assert!(matches!(
        f.sessions.close(
            f.session.id(),
            f.session.principal(),
            f.session.revision(),
            ts(650)?
        ),
        Err(heterarchy_orchestration_sessions::AgentSessionCatalogError::HomeAcceptancePending(_))
    ));
    let disable = heterarchy_orchestration_sessions::ProfileMutationCommand::new(
        Uuid::from_u128(0xAC50),
        f.profile.id(),
        f.profile.revision(),
        heterarchy_orchestration_sessions::ProfileMutation::Disable,
        ts(750)?,
    )?;
    assert!(matches!(
        f.profiles.mutate_profile(&disable),
        Err(heterarchy_orchestration_sessions::AgentProfileError::HomeAcceptanceProfilePinned)
    ));
    let result = f.operations.transfer.catalog.admit_home_acceptance(
        f.identity,
        f.take.acceptance_id(),
        f.proposal,
        None,
        &session,
        &profile,
        || timestamp(640),
        |now, _| Ok((now, timestamp(900)?)),
    )?;
    let HomeAcceptanceAdmissionOutcome::Admitted(admission) = result else {
        return Err("expected admission".into());
    };
    assert_eq!(admission.recipient_profile_id(), f.profile.id());
    assert_eq!(admission.current_node_binding_revision(), Some(1));
    assert_eq!(
        matches!(
            admission.work_authority(),
            forge_domain::HomeHandoffWorkAuthority::HomeTeam(_)
        ),
        team_work
    );
    let replay = f.operations.transfer.catalog.admit_home_acceptance(
        f.identity,
        f.take.acceptance_id(),
        f.proposal,
        None,
        &session,
        &profile,
        || Err(OperationsError::Conflict("replay clock called")),
        |_, _| Err(OperationsError::Conflict("replay window renewed")),
    )?;
    let HomeAcceptanceAdmissionOutcome::Admitted(replayed) = replay else {
        return Err("expected replay".into());
    };
    assert!(replayed.replayed());
    assert_eq!(replayed.grant(), admission.grant());
    let terminal = f.abort_source()?;
    let resolved = f.operations.transfer.catalog.resolve_home_acceptance(
        f.identity,
        &terminal,
        Some(&session),
        Some(&profile),
        || timestamp(700),
    )?;
    assert_eq!(
        resolved.outcome(),
        forge_persistence::HomeHandoffAcceptanceOutcomeKind::Aborted
    );
    let saved = f
        .operations
        .transfer
        .catalog
        .home_acceptance(f.identity, f.take.acceptance_id())?
        .ok_or("missing readback")?;
    assert_eq!(
        saved
            .work_reservation()
            .ok_or("work reservation missing")?
            .state(),
        HomeAcceptanceWorkReservationState::Aborted
    );
    let (settled_session, settled_profile) =
        owners::settle(f, &terminal, &resolved, Some((&session, &profile)))?;
    let replay_resolution = f.operations.transfer.catalog.resolve_home_acceptance(
        f.identity,
        &terminal,
        Some(&settled_session),
        Some(&settled_profile),
        || Err(OperationsError::Conflict("resolution replay sampled clock")),
    )?;
    assert!(replay_resolution.replayed());
    let late_profile_intent = HomeAcceptanceProfileIntent::new(
        f.take,
        f.identity.issuer(),
        Some((f.profile.id(), f.profile.revision())),
    )?;
    assert!(matches!(
        f.profiles.acquire_home_acceptance_profile(
            &late_profile_intent,
            f.identity.issuer(),
            session.held_reference().ok_or("original hold missing")?,
            || Err(heterarchy_orchestration_sessions::HomeAcceptanceProfileError::Conflict)
        )?,
        HomeAcceptanceProfileAcquireOutcome::Resolved(_)
    ));
    assert!(matches!(
        f.sessions.acquire_home_acceptance_session(
            &session.intent(),
            f.session.principal(),
            f.identity.issuer(),
            || Err(heterarchy_orchestration_sessions::HomeAcceptanceSessionError::Clock)
        )?,
        HomeAcceptanceSessionAcquireOutcome::Resolved(_)
    ));
    assert_eq!(
        f.source
            .store
            .get_agent_assignment(f.source.assignment.id())?,
        Some(f.source.assignment.clone())
    );
    assert!(
        f.source
            .store
            .get_agent_assignment(f.proposal.request().successor_id())?
            .is_none()
    );
    assert_eq!(
        f.operations
            .transfer
            .catalog
            .read_assignment_affinity(WORKSPACE, f.source.assignment.id())?,
        original_affinity
    );
    let _disabled = f.profiles.mutate_profile(&disable)?;
    let closed = f.sessions.close(
        f.session.id(),
        f.session.principal(),
        f.session.revision(),
        ts(760)?,
    )?;
    assert_eq!(closed.state(), forge_domain::AgentSessionState::Closed);
    Ok(())
}

#[test]
fn native_admission_accepts_current_recipient_revision_with_unchanged_proposal_identity()
-> SourceDispatchResult {
    let mut f = Fixture::new(false)?;
    let before = f.source.recipient;
    let focus = forge_domain::AgentFocus::new(
        Some(forge_domain::GoalAttribution::new(
            f.source.assignment.goal_id(),
            f.source.assignment.goal_revision(),
        )),
        Some(f.source.assignment.dag_revision()),
        Some(forge_domain::AgentFocusNode::new(
            f.source.assignment.node_id(),
            f.source.assignment.node_revision(),
        )),
    )?;
    let current = f.source.store.set_agent_focus(
        f.source.actor,
        &forge_domain::SetAgentFocus::new(
            before.agent_id(),
            f.source.assignment.activity_id(),
            before.agent_revision(),
            focus,
            ts(570)?,
        ),
        ts(570)?,
    )?;
    assert!(current.revision().get() > before.agent_revision().get());
    f.source.recipient = forge_domain::AgentAssignmentTarget::new(
        current.id(),
        current
            .session_id()
            .ok_or("current recipient Session missing")?,
        current.principal(),
        current.revision(),
    );
    f.take = f.request_for(f.take.acceptance_id())?;
    assert_eq!(f.proposal.request().recipient(), before);
    exercise_native_abort_recovery(&mut f, false)?;
    f.cleanup()
}

#[test]
fn native_source_abort_before_operations_stage_is_absorbing() -> SourceDispatchResult {
    let mut f = Fixture::new(false)?;
    let terminal = f.abort_source()?;
    let resolved = f.operations.transfer.catalog.resolve_home_acceptance(
        f.identity,
        &terminal,
        None,
        None,
        || timestamp(600),
    )?;
    assert!(resolved.admission().is_none());
    assert!(f.stage().is_err());
    let saved = f
        .operations
        .transfer
        .catalog
        .home_acceptance(f.identity, f.take.acceptance_id())?
        .ok_or("missing tombstone")?;
    assert!(saved.intent().proposal().is_none());
    assert!(saved.work_reservation().is_none());
    let (settled_session, _settled_profile) = owners::settle(&mut f, &terminal, &resolved, None)?;
    assert!(settled_session.held_reference().is_none());
    let session_intent =
        HomeAcceptanceSessionIntent::new(f.take, f.identity.issuer(), f.session.revision())?;
    assert!(matches!(
        f.sessions.acquire_home_acceptance_session(
            &session_intent,
            f.session.principal(),
            f.identity.issuer(),
            || Err(heterarchy_orchestration_sessions::HomeAcceptanceSessionError::Clock)
        )?,
        HomeAcceptanceSessionAcquireOutcome::Resolved(_)
    ));
    f.cleanup()
}

#[test]
fn durable_work_readback_rejects_source_digest_substitution_after_native_abort()
-> SourceDispatchResult {
    let mut f = Fixture::new(false)?;
    exercise_native_abort_recovery(&mut f, false)?;
    let saved = f
        .operations
        .transfer
        .catalog
        .home_acceptance(f.identity, f.take.acceptance_id())?
        .ok_or("settled acceptance missing")?;
    assert_eq!(
        saved
            .work_reservation()
            .ok_or("settled work missing")?
            .state(),
        HomeAcceptanceWorkReservationState::Aborted
    );
    // Inject corruption in this isolated fixture after confirming normal
    // historical readback. The production transition trigger forbids the edit.
    f.operations
        .transfer
        .catalog
        .connection
        .execute_batch("DROP TRIGGER ops_home_acceptance_work_reservation_transition;")?;
    let changed=f.operations.transfer.catalog.connection.execute(
        "UPDATE ops_home_acceptance_work_reservations SET terminal_source_digest=?1 WHERE acceptance_id=?2",
        params![[0xEF_u8;32].as_slice(),f.take.acceptance_id().as_bytes()],
    )?;
    assert_eq!(changed, 1);
    assert!(matches!(
        f.operations
            .transfer
            .catalog
            .home_acceptance(f.identity, f.take.acceptance_id()),
        Err(OperationsError::Corrupt(_))
    ));
    f.cleanup()
}

#[test]
fn abort_settles_only_its_attempt_while_another_attempt_still_holds_the_recipient()
-> SourceDispatchResult {
    let mut f = Fixture::new(false)?;
    let _staged = f.stage()?;
    let first = f.holds()?;
    let _admitted = f.operations.transfer.catalog.admit_home_acceptance(
        f.identity,
        f.take.acceptance_id(),
        f.proposal,
        None,
        &first.0,
        &first.1,
        || timestamp(640),
        |now, _| Ok((now, timestamp(900)?)),
    )?;
    let next = f.request_for(Uuid::from_u128(0xAC60))?;
    let _next_staged = f.operations.transfer.catalog.stage_home_acceptance(
        f.identity,
        next,
        f.proposal,
        None,
        || timestamp(650),
    )?;
    let second = f.holds_for(next)?;
    let window_called = Cell::new(false);
    let competing = f.operations.transfer.catalog.admit_home_acceptance(
        f.identity,
        next.acceptance_id(),
        f.proposal,
        None,
        &second.0,
        &second.1,
        || timestamp(660),
        |_, _| {
            window_called.set(true);
            Err(OperationsError::Conflict(
                "conflicting admission requested window",
            ))
        },
    );
    assert!(matches!(competing, Err(OperationsError::Conflict(_))));
    assert!(
        !window_called.get(),
        "competing reservation must fail before grant creation"
    );
    let first_terminal = f.abort_source()?;
    let first_resolution = f.operations.transfer.catalog.resolve_home_acceptance(
        f.identity,
        &first_terminal,
        Some(&first.0),
        Some(&first.1),
        || timestamp(700),
    )?;
    let _settled_first = owners::settle(
        &mut f,
        &first_terminal,
        &first_resolution,
        Some((&first.0, &first.1)),
    )?;
    assert!(matches!(
        f.sessions.close(
            f.session.id(),
            f.session.principal(),
            f.session.revision(),
            ts(750)?
        ),
        Err(heterarchy_orchestration_sessions::AgentSessionCatalogError::HomeAcceptancePending(_))
    ));
    let second_pending = f
        .sessions
        .home_acceptance_session(
            next.acceptance_id(),
            f.identity.issuer(),
            f.session.principal(),
        )?
        .ok_or("second Session hold lost")?;
    assert!(second_pending.held_reference().is_some());
    let second_terminal = f.abort_source_for(next)?;
    let second_resolution = f.operations.transfer.catalog.resolve_home_acceptance(
        f.identity,
        &second_terminal,
        Some(&second.0),
        Some(&second.1),
        || timestamp(710),
    )?;
    assert!(second_resolution.admission().is_none());
    let _settled_second = owners::settle(
        &mut f,
        &second_terminal,
        &second_resolution,
        Some((&second.0, &second.1)),
    )?;
    let closed = f.sessions.close(
        f.session.id(),
        f.session.principal(),
        f.session.revision(),
        ts(760)?,
    )?;
    assert_eq!(closed.state(), forge_domain::AgentSessionState::Closed);
    f.cleanup()
}
