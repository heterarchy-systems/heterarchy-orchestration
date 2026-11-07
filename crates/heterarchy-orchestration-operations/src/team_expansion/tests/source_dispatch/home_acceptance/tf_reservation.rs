//! A seat-only TF reservation is not an active work binding or a global work lock.
use super::*;
use crate::temporary_dispatch::{
    TemporaryDispatchAdmissionRequest, TemporaryDispatchAdmissionResult,
    TemporaryDispatchConsentCommand, TemporaryDispatchConsentKind, TemporaryDispatchRequestedScope,
    TemporaryDispatchReserveCommand,
    lifecycle::{TemporaryDispatchAdmissionBinding, TemporaryDispatchReservationState},
};

#[test]
fn reserved_tf_seat_preserves_independent_home_and_workspace_admission() -> SourceDispatchResult {
    for team_work in [false, true] {
        let mut f = Fixture::new(team_work)?;
        let connection = &f.operations.transfer.catalog.connection;
        ensure_schema_objects(connection, crate::temporary_dispatch::schema_objects())?;
        let (receiver, revision) = current_team_leader(connection, SECOND_TEAM_ID)?;
        let (home, _) = current_team_leader(connection, f.operations.preparation.source_team_id())?;
        let catalog = &mut f.operations.transfer.catalog;
        let request = TemporaryDispatchAdmissionRequest::new(
            Uuid::from_u128(0xAC70),
            WORKSPACE,
            SECOND_TEAM_ID,
            revision,
            receiver,
            MEMBER_FOUR_PARTICIPANT,
            TemporaryDispatchRequestedScope::new(
                "Future approved TF work; no Source activation".to_owned(),
            )?,
            ts(2_000)?,
        )?;
        let TemporaryDispatchAdmissionResult::Admitted(admitted) =
            catalog.admit_temporary_dispatch(&request, || timestamp(570))?
        else {
            return Err("expected independent TF request admission".into());
        };
        let binding = TemporaryDispatchAdmissionBinding::from_record(&admitted);
        for (id, kind, actor) in [
            (
                0xAC71,
                TemporaryDispatchConsentKind::TargetAcceptance,
                fourth_participant(),
            ),
            (
                0xAC72,
                TemporaryDispatchConsentKind::HomeLeaderConsent,
                home,
            ),
            (
                0xAC73,
                TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval,
                orchestrator(),
            ),
        ] {
            let _consent = catalog.record_temporary_dispatch_consent(
                &TemporaryDispatchConsentCommand::new(
                    Uuid::from_u128(id),
                    binding.clone(),
                    kind,
                    0,
                    actor,
                )?,
                || timestamp(580),
            )?;
        }
        let reserved = catalog.reserve_temporary_dispatch(
            &TemporaryDispatchReserveCommand::new(binding.clone(), 0, orchestrator())?,
            || timestamp(590),
        )?;
        assert_eq!(
            reserved.state(),
            TemporaryDispatchReservationState::Reserved
        );
        exercise_native_abort_recovery(&mut f, team_work)?;
        let unchanged = f
            .operations
            .transfer
            .catalog
            .read_temporary_dispatch_reservation(&binding, receiver)?
            .ok_or("Home attempt settlement removed the TF reservation")?;
        assert_eq!(
            unchanged.state(),
            TemporaryDispatchReservationState::Reserved
        );
        assert_eq!(unchanged.revision(), reserved.revision());
        f.cleanup()?;
    }
    Ok(())
}

fn current_team_leader(
    connection: &Connection,
    team: Uuid,
) -> SourceDispatchResult<(OperationalActor, u64)> {
    let (activity, agent, session, revision): (Vec<u8>, Vec<u8>, Vec<u8>, i64) = connection
        .query_row(
            "SELECT b.activity_id,b.agent_id,b.session_id,l.leadership_revision
         FROM ops_leadership l JOIN ops_current_effect_bindings b
         ON b.workspace_id=l.workspace_id AND b.participant_id=l.leader_participant_id
         WHERE l.workspace_id=?1 AND l.scope_kind=2 AND l.team_id=?2 AND l.state='ACTIVE'
         ORDER BY b.agent_id LIMIT 1",
            params![WORKSPACE.into_uuid().as_bytes(), team.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    Ok((
        actor(
            Uuid::from_slice(&activity)?,
            Uuid::from_slice(&agent)?,
            Uuid::from_slice(&session)?,
        ),
        u64::try_from(revision)?,
    ))
}
