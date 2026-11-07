use std::error::Error;
use std::fs;

use forge_domain::{
    AgentAssignmentTarget, AgentInstanceId, AgentInstanceRevision, AgentSessionId,
    AuthenticatedPrincipal, ForgeBackendId, HomeHandoffProposalRequestDigest,
    HomeHandoffTakeRequest, HomeTransferSourceScope, OrchestrationIssuerId, TimestampMillis,
    WorkspaceId, WorkspaceRevision,
};
use uuid::Uuid;

use super::*;
use crate::HomeAcceptanceSessionError;
use crate::catalog::AgentSessionCatalog;

fn temp_catalog() -> Result<(std::path::PathBuf, std::path::PathBuf), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!("home-session-recovery-{}", Uuid::now_v7()));
    fs::create_dir(&root)?;
    let path = root.join("agent-sessions.sqlite3");
    Ok((root, path))
}

fn intent(
    acceptance_id: Uuid,
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    issuer: OrchestrationIssuerId,
) -> Result<HomeAcceptanceSessionIntent, Box<dyn Error>> {
    let scope = HomeTransferSourceScope::new(
        WorkspaceId::from_uuid(Uuid::now_v7()),
        WorkspaceRevision::new(1)?,
        ForgeBackendId::from_uuid(Uuid::now_v7()),
        Uuid::now_v7(),
    )?;
    let recipient = AgentAssignmentTarget::new(
        AgentInstanceId::from_uuid(Uuid::now_v7()),
        session_id,
        principal,
        AgentInstanceRevision::new(1)?,
    );
    let request = HomeHandoffTakeRequest::new(
        acceptance_id,
        Uuid::now_v7(),
        Uuid::now_v7(),
        1,
        HomeHandoffProposalRequestDigest::from_bytes([0x61; 32])?,
        scope,
        recipient,
    )?;
    Ok(HomeAcceptanceSessionIntent::new(
        request,
        issuer,
        forge_domain::AgentSessionRevision::INITIAL,
    )?)
}

fn synthetic_aborted_record(
    intent: &HomeAcceptanceSessionIntent,
    resolved_at: TimestampMillis,
) -> Result<HomeAcceptanceSessionRecord, Box<dyn Error>> {
    let resolution = HomeAcceptanceSessionResolution::from_stored_parts(
        intent,
        &super::super::model::StoredSessionResolutionParts {
            outcome: HomeHandoffAcceptanceOutcomeKind::Aborted,
            source_digest: [0x71; DIGEST_BYTES],
            source_at: TimestampMillis::new(11)?,
            operations_digest: [0x72; DIGEST_BYTES],
            admission: None,
            held_at: None,
            resolved_at,
        },
    )?;
    Ok(HomeAcceptanceSessionRecord::resolved(
        intent,
        &resolution,
        None,
    ))
}

#[test]
fn persisted_absent_abort_absorbs_late_acquire_and_changed_request() -> Result<(), Box<dyn Error>> {
    // This fixture tests the local absorbing-row transition. Its terminal
    // digests are synthetic and do not prove another catalog wrote Source or
    // Operations receipts.
    let (root, path) = temp_catalog()?;
    let principal = AuthenticatedPrincipal::TrustedStdio;
    let issuer = OrchestrationIssuerId::from_uuid(Uuid::now_v7());
    let acceptance_id = Uuid::now_v7();
    let unborn_session = AgentSessionId::from_uuid(Uuid::now_v7());
    let request = intent(acceptance_id, unborn_session, principal, issuer)?;
    let record = synthetic_aborted_record(&request, TimestampMillis::new(12)?)?;
    let mut catalog = AgentSessionCatalog::open(&path)?;
    let transaction = catalog.connection.transaction()?;
    insert_resolved_absent(&transaction, &record)?;
    transaction.commit()?;

    let delayed = catalog.acquire_home_acceptance_session(&request, principal, issuer, || {
        Err(HomeAcceptanceSessionError::Clock)
    });
    assert!(matches!(
        delayed,
        Ok(HomeAcceptanceSessionAcquireOutcome::Resolved(existing)) if existing == record
    ));

    let changed = intent(acceptance_id, unborn_session, principal, issuer)?;
    assert!(matches!(
        catalog.acquire_home_acceptance_session(&changed, principal, issuer, || {
            Err(HomeAcceptanceSessionError::Clock)
        }),
        Err(AgentSessionCatalogError::HomeAcceptance(error))
            if matches!(error.as_ref(), HomeAcceptanceSessionError::Conflict)
    ));
    assert_eq!(
        catalog.home_acceptance_session(acceptance_id, issuer, principal)?,
        Some(record)
    );
    let held: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM home_acceptance_session_pins WHERE acceptance_id=?1 AND state=0",
        [acceptance_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(held, 0);

    drop(catalog);
    fs::remove_dir_all(root)?;
    Ok(())
}
