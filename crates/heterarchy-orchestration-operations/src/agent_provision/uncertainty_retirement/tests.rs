use super::*;
use crate::{
    AgentProvisionDispatchAction, AgentRequestCommand, AgentRequestEffectScope,
    AgentRequestIndependence, AgentRequestIntent, AgentRequestRequester,
    AgentRequestResolutionSelection, ChatGptProjectId, CommitAgentRequestResolution,
    WorkspaceProjectConfigureCommand, WorkspaceProjectScope,
};
use forge_domain::{ActivityId, AgentInstanceId};
use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};
use std::{error::Error, fs, path::Path};

const WORKSPACE_UUID: u128 = 0xF001;
const REQUESTER_SESSION_UUID: u128 = 0xF004;

fn workspace_id() -> WorkspaceId {
    WorkspaceId::from_uuid(Uuid::from_u128(WORKSPACE_UUID))
}

fn requester_session_id() -> AgentSessionId {
    AgentSessionId::from_uuid(Uuid::from_u128(REQUESTER_SESSION_UUID))
}

fn prepare_unknown_provision(
    catalog: &mut OperationsCatalog,
    request_id: Uuid,
    base_time_ms: i64,
) -> Result<AgentProvisionOperation, Box<dyn Error>> {
    let bound = prepare_bound_provision(catalog, request_id, base_time_ms)?;
    let pending = catalog
        .next_agent_provision_dispatch(TimestampMillis::new(base_time_ms + 4)?)?
        .ok_or("pending dispatch missing")?;
    assert_eq!(pending.operation().operation_id(), bound.operation_id());
    assert_eq!(pending.operation().revision(), bound.revision() + 1);
    Ok(catalog.mark_agent_provision_effect_unknown(
        bound.operation_id(),
        pending.operation().revision(),
        TimestampMillis::new(base_time_ms + 5)?,
    )?)
}

fn prepare_bound_provision(
    catalog: &mut OperationsCatalog,
    request_id: Uuid,
    base_time_ms: i64,
) -> Result<AgentProvisionOperation, Box<dyn Error>> {
    if catalog
        .workspace_project_binding(workspace_id().into_uuid())?
        .is_none()
    {
        catalog.configure_workspace_project(
            WorkspaceProjectConfigureCommand::new(
                Uuid::now_v7(),
                WorkspaceProjectScope::new(
                    workspace_id().into_uuid(),
                    Uuid::from_u128(0xF005),
                    1,
                    1,
                    "uncertainty-retirement-test-workspace".to_owned(),
                )?,
                None,
                ChatGptProjectId::try_from("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
                TimestampMillis::new(base_time_ms)?,
            )?,
            &[],
        )?;
    }
    let request = AgentRequestCommand::new(
        request_id,
        AgentRequestRequester::new(
            workspace_id(),
            ActivityId::from_uuid(Uuid::from_u128(0xF002)),
            AgentInstanceId::from_uuid(Uuid::from_u128(0xF003)),
            requester_session_id(),
            1,
        )?,
        AgentRequestIntent::new(
            "retirement verifier".to_owned(),
            "verification".to_owned(),
            1,
            AgentRequestEffectScope::ReadOnly,
            AgentRequestIndependence::None,
            "unobservable provider recovery".to_owned(),
        )?,
        TimestampMillis::new(base_time_ms)?,
    )?;
    let _ = catalog.submit_agent_request(&request)?;
    let resolution = CommitAgentRequestResolution::new(
        request_id,
        1,
        1,
        vec![AgentRequestResolutionSelection::provision_new(0)?],
        TimestampMillis::new(base_time_ms + 1)?,
    )?;
    let _ = catalog.commit_agent_request_resolution(&resolution)?;
    let prepared = catalog.prepare_agent_provision_operation(
        request_id,
        0,
        TimestampMillis::new(base_time_ms + 2)?,
    )?;
    let operation_id = prepared.operation().operation_id();
    Ok(catalog.bind_agent_provision_target(
        operation_id,
        prepared.operation().revision(),
        AgentProfileId::from_uuid(Uuid::from_u128(request_id.as_u128() + 0x1000))?,
        AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(request_id.as_u128() + 0x2000))?,
        TimestampMillis::new(base_time_ms + 3)?,
    )?)
}

fn command(
    retirement_id: Uuid,
    operation: &AgentProvisionOperation,
    retired_at_ms: i64,
) -> Result<AgentProvisionUncertaintyRetirementCommand, OperationsError> {
    AgentProvisionUncertaintyRetirementCommand::new(
        retirement_id,
        AgentProvisionUncertaintyRetirementTarget::new(
            operation.operation_id(),
            operation.revision(),
            workspace_id(),
            requester_session_id(),
        )?,
        AuthenticatedPrincipal::TrustedStdio,
        ACTIVE_RECOVERY_VERSION,
        TimestampMillis::new(retired_at_ms)
            .map_err(|_| OperationsError::Conflict("test timestamp"))?,
    )
}

fn remove_fixture(root: &Path) -> Result<(), Box<dyn Error>> {
    fs::remove_dir_all(root)?;
    Ok(())
}

fn assert_late_writes_rejected(
    catalog: &mut OperationsCatalog,
    operation: &AgentProvisionOperation,
    retirement: &AgentProvisionUncertaintyRetirement,
) -> Result<(), Box<dyn Error>> {
    assert!(
        catalog
            .confirm_agent_provision_conversation(
                operation.operation_id(),
                retirement.retired_revision(),
                "late-conversation",
                TimestampMillis::new(100)?,
            )
            .is_err()
    );
    assert!(
        catalog
            .fail_agent_provision_effect(
                operation.operation_id(),
                retirement.retired_revision(),
                TimestampMillis::new(100)?,
                None,
            )
            .is_err()
    );
    assert!(
        catalog
            .mark_agent_provision_effect_unknown(
                operation.operation_id(),
                retirement.retired_revision(),
                TimestampMillis::new(100)?,
            )
            .is_err()
    );
    Ok(())
}

fn assert_changed_replays_rejected(
    catalog: &mut OperationsCatalog,
    retirement_id: Uuid,
    operation: &AgentProvisionOperation,
) -> Result<(), Box<dyn Error>> {
    let changed_actor = AgentProvisionUncertaintyRetirementCommand::new(
        retirement_id,
        AgentProvisionUncertaintyRetirementTarget::new(
            operation.operation_id(),
            operation.revision(),
            workspace_id(),
            requester_session_id(),
        )?,
        AuthenticatedPrincipal::DirectToken(forge_domain::DirectTokenId::from_uuid(
            Uuid::from_u128(0xF297),
        )),
        ACTIVE_RECOVERY_VERSION,
        TimestampMillis::new(201)?,
    )?;
    assert!(
        catalog
            .retire_agent_provision_uncertainty(changed_actor)
            .is_err()
    );
    let changed_id = AgentProvisionUncertaintyRetirementCommand::new(
        Uuid::from_u128(0xF203),
        AgentProvisionUncertaintyRetirementTarget::new(
            operation.operation_id(),
            operation.revision(),
            workspace_id(),
            requester_session_id(),
        )?,
        AuthenticatedPrincipal::TrustedStdio,
        ACTIVE_RECOVERY_VERSION,
        TimestampMillis::new(201)?,
    )?;
    assert!(
        catalog
            .retire_agent_provision_uncertainty(changed_id)
            .is_err()
    );
    Ok(())
}

#[test]
fn retirement_is_durable_idempotent_and_unblocks_next_dispatch() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!("agent-provision-retirement-{}", Uuid::now_v7()));
    fs::create_dir(&root)?;
    let path = root.join("operations.sqlite3");
    let destination = Uuid::from_u128(0xF100);
    let first;
    let retirement_id = Uuid::from_u128(0xF101);
    {
        let mut catalog = OperationsCatalog::open(&path, destination)?;
        first = prepare_unknown_provision(&mut catalog, Uuid::from_u128(0xF102), 10)?;
        let second = prepare_bound_provision(&mut catalog, Uuid::from_u128(0xF103), 30)?;
        let outcome =
            catalog.retire_agent_provision_uncertainty(command(retirement_id, &first, 40)?)?;
        assert!(outcome.created());
        assert_eq!(
            outcome.retirement().retired_revision(),
            first.revision() + 1
        );
        let retired = catalog
            .agent_provision_operation(first.operation_id())?
            .ok_or("retired operation missing")?;
        assert_eq!(retired.state(), AgentProvisionState::EffectUnknown);
        assert_eq!(retired.revision(), first.revision() + 1);
        assert_eq!(retired.uncertainty_retirement(), Some(outcome.retirement()));
        assert_eq!(catalog.agent_provision_in_flight_operation()?, None);
        let next = catalog
            .next_agent_provision_dispatch(TimestampMillis::new(41)?)?
            .ok_or("next prepared dispatch missing")?;
        assert_eq!(next.operation().operation_id(), second.operation_id());
        assert_eq!(
            next.dispatch()
                .map(super::super::AgentProvisionDispatch::action),
            Some(AgentProvisionDispatchAction::Execute)
        );
    }

    let mut reopened = OperationsCatalog::open(&path, destination)?;
    let stored = reopened
        .agent_provision_uncertainty_retirement(first.operation_id())?
        .ok_or("retirement missing after reopen")?;
    assert_eq!(stored.retirement_id(), retirement_id);
    let later_clock_replay = AgentProvisionUncertaintyRetirementCommand::new(
        retirement_id,
        AgentProvisionUncertaintyRetirementTarget::new(
            first.operation_id(),
            first.revision(),
            workspace_id(),
            requester_session_id(),
        )?,
        AuthenticatedPrincipal::TrustedStdio,
        ACTIVE_RECOVERY_VERSION,
        TimestampMillis::new(99)?,
    )?;
    let replay = reopened.retire_agent_provision_uncertainty(later_clock_replay)?;
    assert!(!replay.created());
    assert_eq!(replay.retirement(), &stored);
    assert_eq!(replay.retirement().retired_at(), TimestampMillis::new(40)?);
    assert_late_writes_rejected(&mut reopened, &first, &stored)?;
    drop(reopened);
    remove_fixture(&root)
}

#[test]
fn retirement_rejects_stale_foreign_regressing_and_changed_replays() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "agent-provision-retirement-fences-{}",
        Uuid::now_v7()
    ));
    fs::create_dir(&root)?;
    let path = root.join("operations.sqlite3");
    let mut catalog = OperationsCatalog::open(&path, Uuid::from_u128(0xF200))?;
    let operation = prepare_unknown_provision(&mut catalog, Uuid::from_u128(0xF201), 100)?;
    let retirement_id = Uuid::from_u128(0xF202);

    let stale = AgentProvisionUncertaintyRetirementCommand::new(
        retirement_id,
        AgentProvisionUncertaintyRetirementTarget::new(
            operation.operation_id(),
            operation.revision() - 1,
            workspace_id(),
            requester_session_id(),
        )?,
        AuthenticatedPrincipal::TrustedStdio,
        ACTIVE_RECOVERY_VERSION,
        TimestampMillis::new(200)?,
    )?;
    assert!(catalog.retire_agent_provision_uncertainty(stale).is_err());
    let foreign_workspace = AgentProvisionUncertaintyRetirementCommand::new(
        retirement_id,
        AgentProvisionUncertaintyRetirementTarget::new(
            operation.operation_id(),
            operation.revision(),
            WorkspaceId::from_uuid(Uuid::from_u128(0xF299)),
            requester_session_id(),
        )?,
        AuthenticatedPrincipal::TrustedStdio,
        ACTIVE_RECOVERY_VERSION,
        TimestampMillis::new(200)?,
    )?;
    assert!(
        catalog
            .retire_agent_provision_uncertainty(foreign_workspace)
            .is_err()
    );
    let foreign_session = AgentProvisionUncertaintyRetirementCommand::new(
        retirement_id,
        AgentProvisionUncertaintyRetirementTarget::new(
            operation.operation_id(),
            operation.revision(),
            workspace_id(),
            AgentSessionId::from_uuid(Uuid::from_u128(0xF298)),
        )?,
        AuthenticatedPrincipal::TrustedStdio,
        ACTIVE_RECOVERY_VERSION,
        TimestampMillis::new(200)?,
    )?;
    assert!(
        catalog
            .retire_agent_provision_uncertainty(foreign_session)
            .is_err()
    );
    let regressing = command(retirement_id, &operation, operation.updated_at().get() - 1)?;
    assert!(
        catalog
            .retire_agent_provision_uncertainty(regressing)
            .is_err()
    );

    let created =
        catalog.retire_agent_provision_uncertainty(command(retirement_id, &operation, 200)?)?;
    assert!(created.created());
    assert_changed_replays_rejected(&mut catalog, retirement_id, &operation)?;
    drop(catalog);
    remove_fixture(&root)
}

#[test]
fn v45_unknown_operation_migrates_with_active_recovery() -> Result<(), Box<dyn Error>> {
    let root =
        std::env::temp_dir().join(format!("agent-provision-retirement-v45-{}", Uuid::now_v7()));
    fs::create_dir(&root)?;
    let path = root.join("operations.sqlite3");
    let destination = Uuid::from_u128(0xF300);
    let operation_id;
    {
        let mut catalog = OperationsCatalog::open(&path, destination)?;
        let operation = prepare_unknown_provision(&mut catalog, Uuid::from_u128(0xF301), 10)?;
        operation_id = operation.operation_id();
    }
    let connection = Connection::open(&path)?;
    connection.execute_batch("DROP TABLE ops_agent_provision_uncertainty_retirements;")?;
    crate::catalog::restore_v46_message_schema_for_legacy_fixture(&connection)?;
    crate::catalog::restore_v47_assignment_schema_for_legacy_fixture(&connection)?;
    connection.pragma_update(None, "user_version", 45)?;
    drop(connection);

    let migrated = OperationsCatalog::open(&path, destination)?;
    let operation = migrated
        .agent_provision_operation(operation_id)?
        .ok_or("migrated operation missing")?;
    assert_eq!(operation.state(), AgentProvisionState::EffectUnknown);
    assert_eq!(operation.uncertainty_retirement(), None);
    assert_eq!(
        migrated
            .agent_provision_in_flight_operation()?
            .map(|value| value.operation_id()),
        Some(operation_id)
    );
    assert_eq!(
        migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
        crate::catalog::VERSION
    );
    drop(migrated);
    remove_fixture(&root)
}

#[test]
fn retirement_rejects_every_non_unknown_state_and_profile_backed_effect()
-> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "agent-provision-retirement-unsupported-{}",
        Uuid::now_v7()
    ));
    fs::create_dir(&root)?;
    let path = root.join("operations.sqlite3");
    let mut catalog = OperationsCatalog::open(&path, Uuid::from_u128(0xF400))?;
    let operation = prepare_unknown_provision(&mut catalog, Uuid::from_u128(0xF401), 10)?;

    for (state, conversation) in [
        (AgentProvisionState::Prepared, None),
        (AgentProvisionState::EffectPending, None),
        (AgentProvisionState::Observed, Some("observed")),
        (AgentProvisionState::Confirmed, Some("confirmed")),
        (AgentProvisionState::Failed, None),
        (AgentProvisionState::Cancelled, None),
    ] {
        catalog.connection.execute(
            "UPDATE ops_agent_provision_operations SET state=?1,provider_conversation_id=?2 WHERE operation_id=?3",
            params![
                super::super::encode_state(state),
                conversation,
                operation.operation_id().as_bytes(),
            ],
        )?;
        assert!(
            catalog
                .retire_agent_provision_uncertainty(command(Uuid::now_v7(), &operation, 100,)?)
                .is_err(),
            "state {state:?} unexpectedly retired"
        );
    }

    catalog.connection.execute(
        "UPDATE ops_agent_provision_operations SET state=3,provider_conversation_id=NULL,effect_kind=1 WHERE operation_id=?1",
        params![operation.operation_id().as_bytes()],
    )?;
    assert!(
        catalog
            .retire_agent_provision_uncertainty(command(Uuid::now_v7(), &operation, 100)?)
            .is_err()
    );
    assert_eq!(
        catalog.agent_provision_uncertainty_retirement(operation.operation_id())?,
        None
    );
    drop(catalog);
    remove_fixture(&root)
}
