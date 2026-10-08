use std::error::Error;
use std::path::PathBuf;

use forge_domain::{AuthenticatedPrincipal, DirectTokenId, TimestampMillis};
use heterarchy_orchestration_operations::{
    AgentProvisionDispatchAction, AgentProvisionEffectKind, AgentProvisionOperation,
    AgentProvisionState, AgentRequestEffectScope, AgentRequestIndependence,
};
use heterarchy_orchestration_sessions::{
    AgentProfileRuntimeSessionState, ProfileMutation, ProfileMutationCommand,
};
use uuid::Uuid;

use super::{OrchestrationRuntime, RuntimeError};
use crate::provisioning::AgentRequestSubmit;

const OPERATOR: AuthenticatedPrincipal = AuthenticatedPrincipal::TrustedStdio;

struct ProvisionFixture {
    root: PathBuf,
    runtime: OrchestrationRuntime,
    operation_id: Uuid,
}

impl ProvisionFixture {
    fn prepared() -> Result<Self, Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("provider-worker-flow-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let request_id = Uuid::from_u128(0xA105);
        runtime.submit_agent_request(AgentRequestSubmit {
            workspace_id: Uuid::from_u128(0xA101),
            activity_id: Uuid::from_u128(0xA102),
            requester_agent_id: Uuid::from_u128(0xA103),
            requester_session_id: Uuid::from_u128(0xA104),
            coordinator_revision: 1,
            request_id,
            objective: "prepare one provider target".to_owned(),
            capability: "chat".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::SourceWrite,
            independence: AgentRequestIndependence::None,
            reason: "provider worker lifecycle test".to_owned(),
            created_at_ms: 1,
        })?;
        runtime.reserve_agent_request_resolution(request_id, 2, 3)?;
        let prepared = runtime.prepare_agent_request_effect_targets(request_id, 6, 7)?;
        if prepared.len() != 1 {
            return Err("expected one prepared provider target".into());
        }
        let operation_id = AgentProvisionOperation::identity_for_slot(request_id, 0);
        let operation = runtime
            .provider_worker_inspect_provision(OPERATOR, operation_id)?
            .ok_or("prepared provision operation missing")?;
        if operation.state() != AgentProvisionState::Prepared
            || operation.effect_kind() != AgentProvisionEffectKind::ProvisionNew
            || operation.profile_id().is_none()
            || operation.runtime_session_id().is_none()
        {
            return Err("provider target was not fully prepared".into());
        }
        Ok(Self {
            root,
            runtime,
            operation_id,
        })
    }

    fn cleanup(self) -> Result<(), Box<dyn Error>> {
        let Self { root, runtime, .. } = self;
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}

fn runtime_session_snapshot(
    runtime: &OrchestrationRuntime,
    operation_id: Uuid,
) -> Result<(AgentProfileRuntimeSessionState, i64, Option<String>), Box<dyn Error>> {
    let operation = runtime
        .provider_worker_inspect_provision(OPERATOR, operation_id)?
        .ok_or("provision operation missing")?;
    let runtime_session_id = operation
        .runtime_session_id()
        .ok_or("runtime session identity missing")?;
    let profiles = runtime.profile_catalog();
    let guard = profiles
        .lock()
        .map_err(|_| RuntimeError::CatalogOwnerPoisoned)?;
    let session = guard
        .runtime_session(runtime_session_id)?
        .ok_or("runtime session missing")?;
    Ok((
        session.state(),
        session.revision().get(),
        session.provider_conversation_id().map(str::to_owned),
    ))
}

fn assert_execute_replays_as_inspect(
    fixture: &ProvisionFixture,
) -> Result<(u64, i64), Box<dyn Error>> {
    let execute = fixture
        .runtime
        .provider_worker_next_provision(OPERATOR, 10)?
        .ok_or("expected Execute dispatch")?;
    let execute_fence = execute.dispatch().ok_or("Execute fence missing")?;
    assert_eq!(
        execute_fence.action(),
        AgentProvisionDispatchAction::Execute
    );
    assert_eq!(
        execute.operation().state(),
        AgentProvisionState::EffectPending
    );
    let observation = fixture
        .runtime
        .provider_worker_in_flight_provision(OPERATOR)?
        .ok_or("in-flight readback missing")?;
    assert_eq!(&observation, execute.operation());
    assert_eq!(
        fixture
            .runtime
            .provider_worker_inspect_provision(OPERATOR, fixture.operation_id)?,
        Some(observation),
    );

    let inspect_pending = fixture
        .runtime
        .provider_worker_next_provision(OPERATOR, 11)?
        .ok_or("expected pending Inspect dispatch")?;
    let pending_fence = inspect_pending
        .dispatch()
        .ok_or("pending Inspect fence missing")?;
    assert_eq!(
        pending_fence.action(),
        AgentProvisionDispatchAction::Inspect
    );
    assert_eq!(pending_fence.operation_id(), execute_fence.operation_id());
    assert_eq!(
        pending_fence.operation_revision(),
        execute_fence.operation_revision()
    );

    let session_before = runtime_session_snapshot(&fixture.runtime, fixture.operation_id)?;
    assert_eq!(
        session_before.0,
        AgentProfileRuntimeSessionState::Provisioning
    );
    Ok((execute.operation().revision(), session_before.1))
}

fn assert_stale_confirmations_preserve_session(
    fixture: &ProvisionFixture,
    execute_revision: u64,
) -> Result<(AgentProfileRuntimeSessionState, i64, Option<String>), Box<dyn Error>> {
    let session_before = runtime_session_snapshot(&fixture.runtime, fixture.operation_id)?;
    let stale_revision = execute_revision
        .checked_sub(1)
        .ok_or("Execute revision underflow")?;
    assert!(matches!(
        fixture.runtime.provider_worker_confirm_provision(
            OPERATOR,
            fixture.operation_id,
            stale_revision,
            "conversation-exact",
            11,
        ),
        Err(RuntimeError::Conflict)
    ));
    assert_eq!(
        runtime_session_snapshot(&fixture.runtime, fixture.operation_id)?,
        session_before,
        "stale confirmation must leave the Session untouched"
    );

    // The observed time is later than the Session creation time but earlier
    // than the committed EffectPending timestamp. It must be rejected before
    // the Sessions catalog is changed.
    assert!(matches!(
        fixture.runtime.provider_worker_confirm_provision(
            OPERATOR,
            fixture.operation_id,
            execute_revision,
            "conversation-exact",
            9,
        ),
        Err(RuntimeError::Conflict)
    ));
    assert_eq!(
        runtime_session_snapshot(&fixture.runtime, fixture.operation_id)?,
        session_before,
        "stale time must not commit a Session confirmation"
    );
    Ok(session_before)
}

fn assert_unknown_inspect_and_confirmation_replay(
    fixture: &ProvisionFixture,
    execute_revision: u64,
    initial_session_revision: i64,
) -> Result<(), Box<dyn Error>> {
    let unknown = fixture.runtime.provider_worker_mark_provision_unknown(
        OPERATOR,
        fixture.operation_id,
        execute_revision,
        12,
    )?;
    assert_eq!(unknown.state(), AgentProvisionState::EffectUnknown);
    let inspect_unknown = fixture
        .runtime
        .provider_worker_next_provision(OPERATOR, 13)?
        .ok_or("expected Unknown Inspect dispatch")?;
    let unknown_fence = inspect_unknown
        .dispatch()
        .ok_or("Unknown Inspect fence missing")?;
    assert_eq!(
        unknown_fence.action(),
        AgentProvisionDispatchAction::Inspect
    );
    assert_eq!(unknown_fence.operation_id(), fixture.operation_id);
    assert_eq!(unknown_fence.operation_revision(), unknown.revision());

    let confirmed = fixture.runtime.provider_worker_confirm_provision(
        OPERATOR,
        fixture.operation_id,
        unknown_fence.operation_revision(),
        "conversation-exact",
        14,
    )?;
    assert_eq!(confirmed.state(), AgentProvisionState::Confirmed);
    assert_eq!(
        confirmed.provider_conversation_id(),
        Some("conversation-exact")
    );
    let ready = runtime_session_snapshot(&fixture.runtime, fixture.operation_id)?;
    assert_eq!(ready.0, AgentProfileRuntimeSessionState::Ready);
    assert_eq!(ready.1, initial_session_revision + 1);
    assert_eq!(ready.2.as_deref(), Some("conversation-exact"));

    assert!(matches!(
        fixture.runtime.provider_worker_confirm_provision(
            OPERATOR,
            fixture.operation_id,
            unknown_fence.operation_revision(),
            "conversation-changed",
            15,
        ),
        Err(RuntimeError::Conflict)
    ));
    let replay = fixture.runtime.provider_worker_confirm_provision(
        OPERATOR,
        fixture.operation_id,
        unknown_fence.operation_revision(),
        "conversation-exact",
        15,
    )?;
    assert_eq!(replay, confirmed);
    assert_eq!(
        runtime_session_snapshot(&fixture.runtime, fixture.operation_id)?,
        ready
    );
    Ok(())
}

#[test]
fn provision_execute_inspect_unknown_and_confirm_keep_one_exact_fence() -> Result<(), Box<dyn Error>>
{
    let fixture = ProvisionFixture::prepared()?;
    let (execute_revision, initial_session_revision) = assert_execute_replays_as_inspect(&fixture)?;
    let session_before = assert_stale_confirmations_preserve_session(&fixture, execute_revision)?;
    assert_eq!(
        session_before.0,
        AgentProfileRuntimeSessionState::Provisioning
    );
    assert_unknown_inspect_and_confirmation_replay(
        &fixture,
        execute_revision,
        initial_session_revision,
    )?;
    fixture.cleanup()?;
    Ok(())
}

#[test]
fn disabled_profile_does_not_release_a_fresh_execute() -> Result<(), Box<dyn Error>> {
    let fixture = ProvisionFixture::prepared()?;
    let operation = fixture
        .runtime
        .provider_worker_inspect_provision(OPERATOR, fixture.operation_id)?
        .ok_or("prepared provision operation missing")?;
    let profile_id = operation.profile_id().ok_or("profile identity missing")?;
    {
        let profiles = fixture.runtime.profile_catalog();
        let mut guard = profiles
            .lock()
            .map_err(|_| RuntimeError::CatalogOwnerPoisoned)?;
        let profile = guard.resolve(profile_id)?.ok_or("profile missing")?;
        guard.mutate_profile(&ProfileMutationCommand::new(
            Uuid::now_v7(),
            profile.id(),
            profile.revision(),
            ProfileMutation::Disable,
            TimestampMillis::new(8)?,
        )?)?;
    }

    let reconciled = fixture
        .runtime
        .provider_worker_next_provision(OPERATOR, 10)?
        .ok_or("expected disabled-profile reconciliation")?;
    assert!(reconciled.dispatch().is_none());
    assert_eq!(reconciled.operation().operation_id(), fixture.operation_id);
    assert_eq!(reconciled.operation().state(), AgentProvisionState::Failed);
    let failed = fixture
        .runtime
        .provider_worker_inspect_provision(OPERATOR, fixture.operation_id)?
        .ok_or("failed provision operation missing")?;
    assert_eq!(failed.state(), AgentProvisionState::Failed);
    assert_eq!(
        runtime_session_snapshot(&fixture.runtime, fixture.operation_id)?.0,
        AgentProfileRuntimeSessionState::Failed
    );

    fixture.cleanup()?;
    Ok(())
}

#[test]
fn failed_or_closed_new_runtime_target_does_not_release_execute() -> Result<(), Box<dyn Error>> {
    for terminal_state in [
        AgentProfileRuntimeSessionState::Failed,
        AgentProfileRuntimeSessionState::Closed,
    ] {
        let fixture = ProvisionFixture::prepared()?;
        let operation = fixture
            .runtime
            .provider_worker_inspect_provision(OPERATOR, fixture.operation_id)?
            .ok_or("prepared provision operation missing")?;
        let runtime_session_id = operation
            .runtime_session_id()
            .ok_or("runtime session identity missing")?;
        {
            let profiles = fixture.runtime.profile_catalog();
            let mut guard = profiles
                .lock()
                .map_err(|_| RuntimeError::CatalogOwnerPoisoned)?;
            let session = guard
                .runtime_session(runtime_session_id)?
                .ok_or("runtime session missing")?;
            guard.transition_runtime_session(
                runtime_session_id,
                session.revision(),
                terminal_state,
                TimestampMillis::new(8)?,
            )?;
        }

        let reconciled = fixture
            .runtime
            .provider_worker_next_provision(OPERATOR, 10)?
            .ok_or("expected terminal-target reconciliation")?;
        assert!(reconciled.dispatch().is_none());
        assert_eq!(reconciled.operation().operation_id(), fixture.operation_id);
        assert_eq!(reconciled.operation().state(), AgentProvisionState::Failed);
        let operation = fixture
            .runtime
            .provider_worker_inspect_provision(OPERATOR, fixture.operation_id)?
            .ok_or("terminal-target operation missing")?;
        assert_eq!(operation.state(), AgentProvisionState::Failed);
        assert_eq!(
            runtime_session_snapshot(&fixture.runtime, fixture.operation_id)?.0,
            terminal_state
        );

        fixture.cleanup()?;
    }
    Ok(())
}

#[test]
fn provision_worker_requires_an_operator_grant() -> Result<(), RuntimeError> {
    let root = std::env::temp_dir().join(format!("provider-worker-auth-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root)?;
    let runtime = OrchestrationRuntime::open(&root)?;
    let principal =
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(101)));

    assert!(matches!(
        runtime.provider_worker_next_provision(principal, 1),
        Err(RuntimeError::Forbidden)
    ));
    assert!(matches!(
        runtime.provider_worker_inspect_provision(principal, Uuid::from_u128(2)),
        Err(RuntimeError::Forbidden)
    ));

    drop(runtime);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
