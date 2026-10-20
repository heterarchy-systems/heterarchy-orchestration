//! Durable admission recovery across an adapter-clock failure.

use super::super::{AgentRequestAdmissionStage, AgentRequestSubmit, OrchestrationRuntime};
use forge_domain::{AuthenticatedPrincipal, DirectTokenId};
use heterarchy_orchestration_operations::{AgentRequestEffectScope, AgentRequestIndependence};
use std::error::Error;
use uuid::Uuid;

#[test]
fn exact_recovery_requires_owned_session_and_matches_full_intent() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!("request-admission-recovery-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let runtime = OrchestrationRuntime::open(&root)?;
    let principal =
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(10)));
    let issued = runtime.open_session(Uuid::from_u128(11), principal, 100)?;
    let intent = AgentRequestSubmit {
        workspace_id: Uuid::from_u128(1),
        activity_id: Uuid::from_u128(2),
        requester_agent_id: Uuid::from_u128(3),
        requester_session_id: issued.session.session_id,
        coordinator_revision: 1,
        request_id: Uuid::from_u128(5),
        objective: "Read exact partial admission without Source".to_owned(),
        capability: "admission-recovery-fixture".to_owned(),
        count: 1,
        effect_scope: AgentRequestEffectScope::ReadOnly,
        independence: AgentRequestIndependence::None,
        reason: "Compare every stored intent field".to_owned(),
        created_at_ms: 100,
    };
    crate::test_support::ensure_workspace_project_binding(&runtime, intent.workspace_id, 100)?;
    assert!(
        runtime
            .admit_agent_request(intent.clone(), Err::<i64, _>, |stage, _| stage)
            .is_err()
    );
    let snapshot = runtime
        .recover_agent_request_admission(intent.clone(), principal)?
        .ok_or("partial intent missing")?;
    assert_eq!(snapshot.receipt.state, "resolution_pending");
    assert!(snapshot.resolution.is_none());
    assert!(snapshot.operations.is_empty());
    let foreign =
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(12)));
    assert!(matches!(
        runtime.recover_agent_request_admission(intent.clone(), foreign),
        Err(crate::RuntimeError::Forbidden)
    ));
    let mut wrong_workspace = intent.clone();
    wrong_workspace.workspace_id = Uuid::from_u128(13);
    assert!(
        runtime
            .recover_agent_request_admission(wrong_workspace, principal)?
            .is_none()
    );
    let mut changed = intent.clone();
    changed.reason.push_str(" changed");
    assert!(matches!(
        runtime.recover_agent_request_admission(changed, principal),
        Err(crate::RuntimeError::Conflict)
    ));
    let mut changed = intent.clone();
    changed.effect_scope = AgentRequestEffectScope::SourceWrite;
    assert!(matches!(
        runtime.recover_agent_request_admission(changed, principal),
        Err(crate::RuntimeError::Conflict)
    ));
    let mut changed = intent.clone();
    changed.coordinator_revision += 1;
    assert!(matches!(
        runtime.recover_agent_request_admission(changed, principal),
        Err(crate::RuntimeError::Conflict)
    ));
    runtime.admit_agent_request(intent.clone(), |_| Ok(101), |_, error| error)?;
    let snapshot = runtime
        .recover_agent_request_admission(intent.clone(), principal)?
        .ok_or("prepared intent missing")?;
    assert_eq!(snapshot.receipt.state, "resolution_reserved");
    assert!(!snapshot.receipt.created);
    assert_eq!(snapshot.operations.len(), 1);
    assert_eq!(snapshot.operations[0].state, "prepared");
    assert!(snapshot.operations[0].profile_id.is_some());
    runtime.close_session(
        issued.session.session_id,
        principal,
        issued.session.revision,
        102,
    )?;
    assert!(matches!(
        runtime.recover_agent_request_admission(intent, principal),
        Err(crate::RuntimeError::Forbidden)
    ));
    drop(runtime);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn failed_stage_clock_retains_intent_and_exact_resume_prepares_once() -> Result<(), Box<dyn Error>>
{
    let root = std::env::temp_dir().join(format!("request-admission-clock-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let runtime = OrchestrationRuntime::open(&root)?;
    let command = AgentRequestSubmit {
        workspace_id: Uuid::from_u128(1),
        activity_id: Uuid::from_u128(2),
        requester_agent_id: Uuid::from_u128(3),
        requester_session_id: Uuid::from_u128(4),
        coordinator_revision: 1,
        request_id: Uuid::from_u128(5),
        objective: "Recover admission after a clock failure".to_owned(),
        capability: "admission-clock-fixture".to_owned(),
        count: 1,
        effect_scope: AgentRequestEffectScope::ReadOnly,
        independence: AgentRequestIndependence::None,
        reason: "Retain durable intent and avoid duplicate targets".to_owned(),
        created_at_ms: 100,
    };
    crate::test_support::ensure_workspace_project_binding(&runtime, command.workspace_id, 100)?;
    let failed = runtime.admit_agent_request(command.clone(), Err::<i64, _>, |stage, _| stage);
    assert_eq!(failed, Err(AgentRequestAdmissionStage::Reserve));
    let stored = runtime
        .agent_request(command.request_id)?
        .ok_or("durable intent missing")?;
    assert_eq!(stored.state, "resolution_pending");
    assert_eq!(stored.revision, 1);
    assert!(
        runtime
            .agent_request_provision_operations(command.request_id)?
            .is_empty()
    );

    let mut clock = 101;
    let resumed = runtime.admit_agent_request(
        command.clone(),
        |_| {
            clock += 1;
            Ok(clock)
        },
        |_, error| error,
    )?;
    assert!(!resumed.created);
    assert_eq!(resumed.state, "resolution_reserved");
    let prepared = runtime.agent_request_provision_operations(command.request_id)?;
    assert_eq!(prepared.len(), 1);
    assert_eq!(prepared[0].state, "prepared");
    let replayed = runtime.admit_agent_request(
        command.clone(),
        |_| {
            clock += 1;
            Ok(clock)
        },
        |_, error| error,
    )?;
    assert!(!replayed.created);
    assert_eq!(replayed, resumed);
    assert_eq!(
        runtime.agent_request_provision_operations(command.request_id)?,
        prepared
    );
    drop(runtime);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
