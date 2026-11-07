use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;

use forge_domain::{
    AgentAssignmentTarget, AgentInstanceId, AgentInstanceRevision, AgentSessionId,
    AgentSessionOpenRequestId, AgentSessionRevision, AgentSessionState, AuthenticatedPrincipal,
    ForgeBackendId, HomeHandoffProposalRequestDigest, HomeHandoffTakeRequest,
    HomeTransferSourceScope, OpenAgentSession, OrchestrationIssuerId, TimestampMillis, WorkspaceId,
    WorkspaceRevision,
};
use uuid::Uuid;

use super::*;
use crate::catalog::AGENT_SESSION_SCHEMA_VERSION;

struct TestFiles {
    root: PathBuf,
}

impl Drop for TestFiles {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.root);
    }
}

struct Fixture {
    files: TestFiles,
    path: PathBuf,
    catalog: AgentSessionCatalog,
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    issuer: OrchestrationIssuerId,
}

fn fixture(label: &str, principal: AuthenticatedPrincipal) -> Result<Fixture, Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!("home-session-{label}-{}", Uuid::now_v7()));
    fs::create_dir(&root)?;
    let files = TestFiles { root: root.clone() };
    let path = root.join("agent-sessions.sqlite3");
    let mut catalog = AgentSessionCatalog::open(&path)?;
    let session_id = AgentSessionId::from_uuid(Uuid::now_v7());
    let request_id = AgentSessionOpenRequestId::from_uuid(Uuid::now_v7());
    let session = catalog.open_session(&OpenAgentSession::new(
        session_id,
        request_id,
        principal,
        timestamp(10)?,
    ))?;
    if !session.created() {
        return Err("new fixture Session unexpectedly replayed".into());
    }
    Ok(Fixture {
        files,
        path,
        catalog,
        session_id,
        principal,
        issuer: OrchestrationIssuerId::from_uuid(Uuid::now_v7()),
    })
}

fn timestamp(value: i64) -> Result<TimestampMillis, Box<dyn Error>> {
    TimestampMillis::new(value).map_err(Into::into)
}

fn intent(
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    seed: u128,
    issuer: OrchestrationIssuerId,
) -> Result<HomeAcceptanceSessionIntent, Box<dyn Error>> {
    intent_with_session_revision(
        session_id,
        principal,
        seed,
        issuer,
        AgentSessionRevision::INITIAL,
    )
}

fn intent_with_session_revision(
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    seed: u128,
    issuer: OrchestrationIssuerId,
    session_revision: AgentSessionRevision,
) -> Result<HomeAcceptanceSessionIntent, Box<dyn Error>> {
    let scope = HomeTransferSourceScope::new(
        WorkspaceId::from_uuid(Uuid::from_u128(seed + 1)),
        WorkspaceRevision::new(1)?,
        ForgeBackendId::from_uuid(Uuid::from_u128(seed + 2)),
        Uuid::from_u128(seed + 3),
    )?;
    let target = AgentAssignmentTarget::new(
        AgentInstanceId::from_uuid(Uuid::from_u128(seed + 4)),
        session_id,
        principal,
        AgentInstanceRevision::new(1)?,
    );
    let request = HomeHandoffTakeRequest::new(
        Uuid::from_u128(seed + 10),
        Uuid::from_u128(seed + 11),
        Uuid::from_u128(seed + 12),
        1,
        HomeHandoffProposalRequestDigest::from_bytes([0x31; 32])?,
        scope,
        target,
    )?;
    Ok(HomeAcceptanceSessionIntent::new(
        request,
        issuer,
        session_revision,
    )?)
}

fn changed_intent(
    original: &HomeAcceptanceSessionIntent,
    seed: u128,
) -> Result<HomeAcceptanceSessionIntent, Box<dyn Error>> {
    let request = original.request();
    let changed = HomeHandoffTakeRequest::new(
        original.acceptance_id(),
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        request.expected_case_revision(),
        request.proposal_digest(),
        request.scope(),
        request.recipient(),
    )?;
    Ok(HomeAcceptanceSessionIntent::new(
        changed,
        original.issuer(),
        original.expected_session_revision(),
    )?)
}

fn acquire(
    catalog: &mut AgentSessionCatalog,
    value: &HomeAcceptanceSessionIntent,
    principal: AuthenticatedPrincipal,
    issuer: OrchestrationIssuerId,
    at: i64,
) -> Result<HomeAcceptanceSessionAcquireOutcome, AgentSessionCatalogError> {
    catalog.acquire_home_acceptance_session(value, principal, issuer, || {
        TimestampMillis::new(at).map_err(|_| HomeAcceptanceSessionError::Clock)
    })
}

fn assert_clock_failures_leave_no_hold(
    catalog: &mut AgentSessionCatalog,
    held: &HomeAcceptanceSessionIntent,
    principal: AuthenticatedPrincipal,
    issuer: OrchestrationIssuerId,
) -> Result<(), Box<dyn Error>> {
    let backwards_clock = acquire(catalog, held, principal, issuer, 9);
    assert!(matches!(
        backwards_clock,
        Err(AgentSessionCatalogError::HomeAcceptance(error))
            if matches!(error.as_ref(), HomeAcceptanceSessionError::Clock)
    ));
    assert_eq!(
        catalog.home_acceptance_session(held.acceptance_id(), issuer, principal)?,
        None,
        "a local clock before Session creation leaves no partial hold"
    );

    let failed_clock = catalog.acquire_home_acceptance_session(held, principal, issuer, || {
        Err(HomeAcceptanceSessionError::Clock)
    });
    assert!(matches!(
        failed_clock,
        Err(AgentSessionCatalogError::HomeAcceptance(error))
            if matches!(error.as_ref(), HomeAcceptanceSessionError::Clock)
    ));
    assert_eq!(
        catalog.home_acceptance_session(held.acceptance_id(), issuer, principal)?,
        None,
        "clock failure rolls back the complete Session hold intent"
    );
    Ok(())
}

#[test]
fn fresh_schema_replays_one_hold_and_conflicts_on_changed_intent() -> Result<(), Box<dyn Error>> {
    let Fixture {
        files: _files,
        path: _path,
        mut catalog,
        session_id,
        principal,
        issuer,
    } = fixture("replay", AuthenticatedPrincipal::TrustedStdio)?;
    let held = intent(session_id, principal, 0xA100, issuer)?;
    let schema_version: i64 =
        catalog
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(schema_version, AGENT_SESSION_SCHEMA_VERSION);

    assert_clock_failures_leave_no_hold(&mut catalog, &held, principal, issuer)?;

    let first = acquire(&mut catalog, &held, principal, issuer, 20)?;
    assert!(matches!(
        first,
        HomeAcceptanceSessionAcquireOutcome::Held { created: true, .. }
    ));
    let replay = acquire(&mut catalog, &held, principal, issuer, 21)?;
    assert!(matches!(
        replay,
        HomeAcceptanceSessionAcquireOutcome::Held { created: false, .. }
    ));
    assert_eq!(
        catalog
            .home_acceptance_session(held.acceptance_id(), issuer, principal)?
            .ok_or("held acceptance readback missing")?,
        match replay {
            HomeAcceptanceSessionAcquireOutcome::Held { record, .. } => record,
            HomeAcceptanceSessionAcquireOutcome::Resolved(_) => {
                return Err("hold unexpectedly resolved".into());
            }
        }
    );
    assert!(matches!(
        catalog.close(
            session_id,
            principal,
            AgentSessionRevision::INITIAL,
            timestamp(22)?,
        ),
        Err(AgentSessionCatalogError::HomeAcceptancePending(id)) if id == session_id
    ));
    assert_sql_guards_preserve_hold(&catalog, &held);
    assert_eq!(
        catalog
            .resolve(session_id, principal)?
            .ok_or("held Session missing")?
            .state(),
        AgentSessionState::Open
    );

    let changed = changed_intent(&held, 0xA200)?;
    assert!(matches!(
        acquire(&mut catalog, &changed, principal, issuer, 23),
        Err(AgentSessionCatalogError::HomeAcceptance(source))
            if matches!(source.as_ref(), HomeAcceptanceSessionError::Conflict)
    ));
    assert!(matches!(
        catalog.home_acceptance_session(
            held.acceptance_id(),
            issuer,
            AuthenticatedPrincipal::DirectToken(
                forge_domain::DirectTokenId::from_uuid(Uuid::now_v7())
            ),
        ),
        Err(AgentSessionCatalogError::HomeAcceptance(source))
            if matches!(
                source.as_ref(),
                HomeAcceptanceSessionError::Session(owner_error)
                    if matches!(
                        owner_error.as_ref(),
                        AgentSessionCatalogError::ForeignPrincipal(id) if *id == session_id
                    )
            )
    ));
    Ok(())
}

fn assert_sql_guards_preserve_hold(
    catalog: &AgentSessionCatalog,
    held: &HomeAcceptanceSessionIntent,
) {
    for sql in [
        "UPDATE home_acceptance_session_pins SET session_revision=2 WHERE acceptance_id=?1",
        "DELETE FROM home_acceptance_session_pins WHERE acceptance_id=?1",
    ] {
        assert!(
            catalog
                .connection
                .execute(sql, [held.acceptance_id().as_bytes()])
                .is_err()
        );
    }
    let direct_close = catalog.connection.execute(
        "UPDATE agent_sessions SET state=2,updated_at_ms=22,revision=2 WHERE session_id=?1",
        [held.session_id().into_uuid().as_bytes()],
    );
    assert!(
        direct_close.is_err(),
        "SQL owner guard must fence direct close writers"
    );
}

#[test]
fn separate_acceptances_hold_one_session_and_recovery_pages_are_bounded()
-> Result<(), Box<dyn Error>> {
    let Fixture {
        files: _files,
        path: _path,
        mut catalog,
        session_id,
        principal,
        issuer,
    } = fixture("multiple", AuthenticatedPrincipal::TrustedStdio)?;
    let first = intent(session_id, principal, 0xA300, issuer)?;
    let second = intent(session_id, principal, 0xA400, issuer)?;
    assert!(matches!(
        acquire(&mut catalog, &first, principal, issuer, 20)?,
        HomeAcceptanceSessionAcquireOutcome::Held { created: true, .. }
    ));
    assert!(matches!(
        acquire(&mut catalog, &second, principal, issuer, 21)?,
        HomeAcceptanceSessionAcquireOutcome::Held { created: true, .. }
    ));
    let page = catalog.pending_home_acceptance_sessions(issuer, None, 1)?;
    assert_eq!(page.records().len(), 1);
    let next = page.next_after().ok_or("second pending hold was omitted")?;
    let tail = catalog.pending_home_acceptance_sessions(issuer, Some(next), 1)?;
    assert_eq!(tail.records().len(), 1);
    assert_eq!(tail.next_after(), None);
    assert!(matches!(
        catalog.close(
            session_id,
            principal,
            AgentSessionRevision::INITIAL,
            timestamp(22)?,
        ),
        Err(AgentSessionCatalogError::HomeAcceptancePending(id)) if id == session_id
    ));
    assert!(matches!(
        catalog.pending_home_acceptance_sessions(issuer, None, 0),
        Err(AgentSessionCatalogError::HomeAcceptance(source))
            if matches!(source.as_ref(), HomeAcceptanceSessionError::PageSize)
    ));
    Ok(())
}

#[test]
fn acquisition_conflicts_when_session_is_closed_or_revision_is_stale() -> Result<(), Box<dyn Error>>
{
    let Fixture {
        files: _files,
        path: _path,
        mut catalog,
        session_id,
        principal,
        issuer,
    } = fixture("stale", AuthenticatedPrincipal::TrustedStdio)?;
    let value = intent(session_id, principal, 0xA500, issuer)?;
    let closed = catalog.close(
        session_id,
        principal,
        AgentSessionRevision::INITIAL,
        timestamp(20)?,
    )?;
    assert_eq!(closed.state(), AgentSessionState::Closed);
    assert!(matches!(
        acquire(&mut catalog, &value, principal, issuer, 21),
        Err(AgentSessionCatalogError::HomeAcceptance(error))
            if matches!(
                error.as_ref(),
                HomeAcceptanceSessionError::Session(source)
                    if matches!(
                        source.as_ref(),
                        AgentSessionCatalogError::Transition(
                            forge_domain::AgentSessionTransitionError::AlreadyClosed
                        )
                    )
            )
    ));
    assert_eq!(
        catalog.home_acceptance_session(value.acceptance_id(), issuer, principal)?,
        None
    );
    Ok(())
}

#[test]
fn stale_expected_revision_on_an_open_session_is_a_revision_conflict() -> Result<(), Box<dyn Error>>
{
    let Fixture {
        files: _files,
        path: _path,
        mut catalog,
        session_id,
        principal,
        issuer,
    } = fixture("stale-open-revision", AuthenticatedPrincipal::TrustedStdio)?;
    let stale = intent_with_session_revision(
        session_id,
        principal,
        0xA700,
        issuer,
        AgentSessionRevision::new(2)?,
    )?;
    let opened = catalog
        .resolve(session_id, principal)?
        .ok_or("Session disappeared")?;
    assert_eq!(opened.state(), AgentSessionState::Open);
    assert_eq!(opened.revision(), AgentSessionRevision::INITIAL);
    assert!(matches!(
        acquire(&mut catalog, &stale, principal, issuer, 21),
        Err(AgentSessionCatalogError::HomeAcceptance(error))
            if matches!(
                error.as_ref(),
                HomeAcceptanceSessionError::Session(source)
                    if matches!(source.as_ref(), AgentSessionCatalogError::RevisionConflict(id) if *id == session_id)
            )
    ));
    assert_eq!(
        catalog.home_acceptance_session(stale.acceptance_id(), issuer, principal)?,
        None
    );
    Ok(())
}

#[test]
fn two_catalog_connections_serialize_close_against_hold_acquisition() -> Result<(), Box<dyn Error>>
{
    let Fixture {
        files: _files,
        path,
        catalog: observer,
        session_id,
        principal,
        issuer,
    } = fixture("close-acquire-race", AuthenticatedPrincipal::TrustedStdio)?;
    let mut acquire_catalog = AgentSessionCatalog::open(&path)?;
    let mut close_catalog = AgentSessionCatalog::open(&path)?;
    let value = intent(session_id, principal, 0xA600, issuer)?;
    let barrier = Arc::new(Barrier::new(3));

    let acquire_barrier = Arc::clone(&barrier);
    let acquire_worker = thread::spawn(move || {
        acquire_barrier.wait();
        acquire(&mut acquire_catalog, &value, principal, issuer, 30)
    });
    let close_barrier = Arc::clone(&barrier);
    let close_worker = thread::spawn(move || {
        close_barrier.wait();
        close_catalog.close(
            session_id,
            principal,
            AgentSessionRevision::INITIAL,
            TimestampMillis::new(30).map_err(|_| AgentSessionCatalogError::Integrity)?,
        )
    });
    barrier.wait();
    let acquired = acquire_worker
        .join()
        .map_err(|_| "acquire worker panicked")?;
    let closed = close_worker.join().map_err(|_| "close worker panicked")?;

    match (acquired, closed) {
        (
            Ok(HomeAcceptanceSessionAcquireOutcome::Held { created: true, .. }),
            Err(AgentSessionCatalogError::HomeAcceptancePending(id)),
        ) if id == session_id => {
            assert!(matches!(
                observer
                    .home_acceptance_session(value.acceptance_id(), issuer, principal)?
                    .ok_or("serialized hold missing")?
                    .state(),
                HomeAcceptanceSessionState::Held { .. }
            ));
        }
        (Err(AgentSessionCatalogError::HomeAcceptance(error)), Ok(closed_session))
            if matches!(
                error.as_ref(),
                HomeAcceptanceSessionError::Session(source)
                    if matches!(
                        source.as_ref(),
                        AgentSessionCatalogError::Transition(
                            forge_domain::AgentSessionTransitionError::AlreadyClosed
                        )
                    )
            ) =>
        {
            assert_eq!(closed_session.state(), AgentSessionState::Closed);
            assert_eq!(
                observer.resolve(session_id, principal)?,
                Some(closed_session)
            );
            assert_eq!(
                observer.home_acceptance_session(value.acceptance_id(), issuer, principal)?,
                None
            );
        }
        _ => return Err("close and acquire did not serialize as one winner".into()),
    }
    Ok(())
}

#[test]
fn schema_one_readonly_resolution_stays_readonly_and_writable_open_migrates()
-> Result<(), Box<dyn Error>> {
    let Fixture {
        files: _files,
        path,
        mut catalog,
        session_id,
        principal,
        issuer,
    } = fixture("legacy-v1", AuthenticatedPrincipal::TrustedStdio)?;
    let open_session = catalog
        .resolve(session_id, principal)?
        .ok_or("fresh Session missing")?;
    let second = catalog.open_session(&OpenAgentSession::new(
        AgentSessionId::from_uuid(Uuid::now_v7()),
        AgentSessionOpenRequestId::from_uuid(Uuid::now_v7()),
        principal,
        timestamp(11)?,
    ))?;
    let closed = catalog.close(
        second.session().id(),
        principal,
        second.session().revision(),
        timestamp(12)?,
    )?;
    super::downgrade_empty_v2_for_fixture(&mut catalog.connection)?;
    drop(catalog);
    let before_readonly = fs::read(&path)?;

    let readonly = AgentSessionCatalog::open_existing_read_only(&path)?
        .ok_or("schema1 Session catalog disappeared")?;
    assert_eq!(
        readonly.resolve(session_id, principal)?.as_ref(),
        Some(&open_session)
    );
    assert_eq!(
        readonly.home_acceptance_session(Uuid::now_v7(), issuer, principal)?,
        None,
        "schema1 history is exact and has no Home acceptance rows"
    );
    assert!(
        readonly
            .pending_home_acceptance_sessions(issuer, None, 64)?
            .records()
            .is_empty()
    );
    drop(readonly);
    assert_eq!(fs::read(&path)?, before_readonly);

    let migrated = AgentSessionCatalog::open(&path)?;
    assert_eq!(
        migrated.resolve(session_id, principal)?.as_ref(),
        Some(&open_session)
    );
    assert_eq!(migrated.resolve(closed.id(), principal)?, Some(closed));
    let user_version: i64 =
        migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(user_version, AGENT_SESSION_SCHEMA_VERSION);
    let fabricated_holds: i64 = migrated.connection.query_row(
        "SELECT COUNT(*) FROM home_acceptance_session_pins",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(fabricated_holds, 0);
    drop(migrated);
    Ok(())
}

#[test]
fn damaged_current_schema_does_not_fall_back_to_legacy_resolution() -> Result<(), Box<dyn Error>> {
    let Fixture {
        files: _files,
        path,
        catalog,
        session_id: _,
        principal: _,
        issuer: _,
    } = fixture("damaged-current", AuthenticatedPrincipal::TrustedStdio)?;
    catalog
        .connection
        .execute_batch("CREATE TABLE rogue_home_schema(value INTEGER)")?;
    drop(catalog);
    let database_before = fs::read(&path)?;
    assert!(matches!(
        AgentSessionCatalog::open_existing_read_only(&path),
        Err(AgentSessionCatalogError::Schema(_))
    ));
    assert_eq!(fs::read(&path)?, database_before);
    Ok(())
}
