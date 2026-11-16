//! Exact `AgentRequest` history recovery without provider-side effects.

use super::super::{AgentRequestSubmit, OrchestrationRuntime};
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
        objective: "Read exact request intent without Source".to_owned(),
        capability: "admission-recovery-fixture".to_owned(),
        count: 1,
        effect_scope: AgentRequestEffectScope::ReadOnly,
        independence: AgentRequestIndependence::None,
        reason: "Compare every stored intent field".to_owned(),
        created_at_ms: 100,
    };
    runtime.submit_agent_request(intent.clone())?;
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
