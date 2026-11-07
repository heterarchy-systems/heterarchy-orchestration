use super::super::{
    AgentProfileCreationOrigin, AgentProfileDeletionCommand, AgentProfileProviderFamily,
    CreateAgentProfile, ProfileMutation, ProfileMutationCommand,
};
use super::*;
use crate::catalog::{AgentSessionCatalog, HomeAcceptanceSessionAcquireOutcome};
use forge_domain::{
    AgentSessionId, AgentSessionOpenRequestId, AuthenticatedPrincipal, DirectTokenId,
    OpenAgentSession,
};
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

struct Root(PathBuf);

impl Drop for Root {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && self.0.exists()
        {
            eprintln!("failed to remove Profile Home test root: {error}");
        }
    }
}

struct ProfileFixture {
    root: Root,
    profiles: AgentProfileCatalog,
    sessions: AgentSessionCatalog,
    profile: AgentProfile,
    session: forge_domain::RegisteredAgentSession,
    issuer: OrchestrationIssuerId,
}

fn root(label: &str) -> Result<Root, Box<dyn Error>> {
    let path = std::env::temp_dir().join(format!("profile-home-{label}-{}", Uuid::now_v7()));
    fs::create_dir(&path)?;
    Ok(Root(path))
}

fn created_profile(
    catalog: &mut AgentProfileCatalog,
    stable_name: &str,
    seed: u128,
    at: i64,
) -> Result<AgentProfile, Box<dyn Error>> {
    Ok(catalog
        .create_profile(&CreateAgentProfile::new(
            AgentProfileId::from_uuid(Uuid::from_u128(seed + 1))?,
            Uuid::from_u128(seed + 2),
            stable_name.to_owned(),
            format!("{stable_name} display"),
            AgentProfileProviderFamily::ChatGpt,
            None,
            None,
            vec!["backend".to_owned()],
            AgentProfileCreationOrigin::UserManual,
            TimestampMillis::new(at)?,
        )?)?
        .profile()
        .clone())
}

fn create_linked_session(
    profiles: &mut AgentProfileCatalog,
    sessions: &mut AgentSessionCatalog,
    profile: &AgentProfile,
    seed: u128,
    at: i64,
) -> Result<forge_domain::RegisteredAgentSession, Box<dyn Error>> {
    let principal =
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(seed + 3)));
    let opened = sessions.open_session(&OpenAgentSession::new(
        AgentSessionId::from_uuid(Uuid::from_u128(seed + 4)),
        AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(seed + 5)),
        principal,
        TimestampMillis::new(at)?,
    ))?;
    profiles.link_resolved_logical_session(
        Uuid::from_u128(seed + 6),
        profile.id(),
        opened.session(),
        TimestampMillis::new(at + 1)?,
    )?;
    Ok(opened.session().clone())
}

fn profile_fixture(label: &str) -> Result<ProfileFixture, Box<dyn Error>> {
    let root = root(label)?;
    let mut profiles = AgentProfileCatalog::open(root.0.join("profiles.sqlite"))?;
    let mut sessions = AgentSessionCatalog::open(root.0.join("sessions.sqlite"))?;
    let profile = created_profile(&mut profiles, "home-recipient", 0xA100, 1)?;
    let session = create_linked_session(&mut profiles, &mut sessions, &profile, 0xA200, 2)?;
    Ok(ProfileFixture {
        root,
        profiles,
        sessions,
        profile,
        session,
        issuer: OrchestrationIssuerId::from_uuid(Uuid::from_u128(0xA300)),
    })
}

fn take_request(
    session: &forge_domain::RegisteredAgentSession,
    seed: u128,
) -> Result<forge_domain::HomeHandoffTakeRequest, Box<dyn Error>> {
    Ok(forge_domain::HomeHandoffTakeRequest::new(
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        Uuid::from_u128(seed + 3),
        1,
        forge_domain::HomeHandoffProposalRequestDigest::from_bytes([9; 32])?,
        forge_domain::HomeTransferSourceScope::new(
            forge_domain::WorkspaceId::from_uuid(Uuid::from_u128(seed + 4)),
            forge_domain::WorkspaceRevision::new(1)?,
            forge_domain::ForgeBackendId::from_uuid(Uuid::from_u128(seed + 5)),
            Uuid::from_u128(seed + 6),
        )?,
        forge_domain::AgentAssignmentTarget::new(
            forge_domain::AgentInstanceId::from_uuid(Uuid::from_u128(seed + 7)),
            session.id(),
            session.principal(),
            forge_domain::AgentInstanceRevision::new(1)?,
        ),
    )?)
}

fn acquire_profile_pin(
    profiles: &mut AgentProfileCatalog,
    sessions: &mut AgentSessionCatalog,
    profile: &AgentProfile,
    session: &forge_domain::RegisteredAgentSession,
    issuer: OrchestrationIssuerId,
    request: forge_domain::HomeHandoffTakeRequest,
    at: i64,
) -> Result<HomeAcceptanceProfilePinRecord, Box<dyn Error>> {
    let session_intent =
        crate::catalog::HomeAcceptanceSessionIntent::new(request, issuer, session.revision())?;
    let session_acquired_at = TimestampMillis::new(at)?;
    let held = sessions.acquire_home_acceptance_session(
        &session_intent,
        session.principal(),
        issuer,
        || Ok(session_acquired_at),
    )?;
    let HomeAcceptanceSessionAcquireOutcome::Held { record, .. } = held else {
        return Err("test acceptance unexpectedly resolved".into());
    };
    let session_reference = record
        .held_reference()
        .ok_or("Session catalog omitted its held reference")?;
    let intent = HomeAcceptanceProfileIntent::new(
        request,
        issuer,
        Some((profile.id(), profile.revision())),
    )?;
    let profile_acquired_at = TimestampMillis::new(at + 2)?;
    let outcome =
        profiles.acquire_home_acceptance_profile(&intent, issuer, session_reference, || {
            Ok(profile_acquired_at)
        })?;
    match outcome {
        HomeAcceptanceProfileAcquireOutcome::Held {
            record,
            created: true,
        } => Ok(record),
        _ => Err("test Profile acceptance was not newly held".into()),
    }
}

#[test]
fn multiple_profile_pins_block_mutations_without_changing_profile() -> Result<(), Box<dyn Error>> {
    let mut fixture = profile_fixture("multiple-pins")?;
    let session_b = create_linked_session(
        &mut fixture.profiles,
        &mut fixture.sessions,
        &fixture.profile,
        0xA400,
        10,
    )?;
    let request_a = take_request(&fixture.session, 0xA500)?;
    let request_b = take_request(&session_b, 0xA600)?;
    let pin_a = acquire_profile_pin(
        &mut fixture.profiles,
        &mut fixture.sessions,
        &fixture.profile,
        &fixture.session,
        fixture.issuer,
        request_a,
        20,
    )?;
    let pin_b = acquire_profile_pin(
        &mut fixture.profiles,
        &mut fixture.sessions,
        &fixture.profile,
        &session_b,
        fixture.issuer,
        request_b,
        30,
    )?;
    assert_eq!(pin_a.state(), HomeAcceptanceProfileState::Held);
    assert_eq!(pin_b.state(), HomeAcceptanceProfileState::Held);
    assert_eq!(
        pin_a.profile_pin().ok_or("missing first pin")?.profile_id(),
        pin_b
            .profile_pin()
            .ok_or("missing second pin")?
            .profile_id()
    );

    let original = fixture
        .profiles
        .list_profiles(4)?
        .into_iter()
        .find(|item| item.id() == fixture.profile.id())
        .ok_or("Pinned Profile disappeared")?;
    let command = ProfileMutationCommand::new(
        Uuid::from_u128(0xA700),
        fixture.profile.id(),
        fixture.profile.revision(),
        ProfileMutation::Disable,
        TimestampMillis::new(40)?,
    )?;
    assert!(matches!(
        fixture.profiles.mutate_profile(&command),
        Err(AgentProfileError::HomeAcceptanceProfilePinned)
    ));
    assert!(matches!(
        fixture.profiles.retire_profile(
            fixture.profile.id(),
            fixture.profile.revision(),
            TimestampMillis::new(41)?,
        ),
        Err(AgentProfileError::HomeAcceptanceProfilePinned)
    ));
    assert!(matches!(
        fixture
            .profiles
            .begin_profile_deletion(&AgentProfileDeletionCommand::new(
                Uuid::from_u128(0xA701),
                fixture.profile.id(),
                fixture.profile.revision(),
                TimestampMillis::new(42)?,
            )?),
        Err(AgentProfileError::HomeAcceptanceProfilePinned)
    ));
    assert!(fixture.profiles
        .connection
        .execute(
            "UPDATE agent_profile_capabilities SET capability='manual-override' WHERE profile_id=?1",
            [fixture.profile.id().into_uuid().as_bytes()],
        )
        .is_err());
    let actual = fixture
        .profiles
        .list_profiles(4)?
        .into_iter()
        .find(|item| item.id() == fixture.profile.id())
        .ok_or("Pinned Profile disappeared after failed mutations")?;
    assert_eq!(actual, original);
    assert_eq!(
        fixture.profiles.home_acceptance_profile(
            &HomeAcceptanceProfileIntent::new(
                request_a,
                fixture.issuer,
                Some((fixture.profile.id(), fixture.profile.revision())),
            )?,
            fixture.issuer,
        )?,
        Some(pin_a)
    );
    Ok(())
}

#[test]
fn current_profile_and_v6_to_v7_migration_preserve_logical_link_history()
-> Result<(), Box<dyn Error>> {
    let fixture = profile_fixture("v6-profile-migration")?;
    let history_before = fixture
        .profiles
        .logical_session_history(fixture.profile.id())?;
    assert_eq!(history_before.links().len(), 1);
    crate::profile_catalog::home_acceptance::schema::downgrade_empty_to_v6_for_test(
        &fixture.profiles.connection,
    )?;
    assert_eq!(
        fixture
            .profiles
            .connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
        super::super::AGENT_PROFILE_V6_SCHEMA_VERSION,
    );
    drop(fixture.profiles);
    let profile_path = fixture.root.0.join("profiles.sqlite");

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let mut migrations = Vec::new();
    for _ in 0..2 {
        let path = profile_path.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        migrations.push(std::thread::spawn(move || {
            let mut connection = match rusqlite::Connection::open(path) {
                Ok(connection) => connection,
                Err(error) => return Err(error.to_string()),
            };
            if let Err(error) = connection.busy_timeout(crate::profile_catalog::BUSY_TIMEOUT) {
                return Err(error.to_string());
            }
            barrier.wait();
            crate::profile_catalog::migrate_v6_to_v7(&mut connection)
                .map_err(|error| error.to_string())
        }));
    }
    barrier.wait();
    for migration in migrations {
        let result = migration
            .join()
            .map_err(|_| std::io::Error::other("Profile migration worker panicked"))?;
        assert!(
            result.is_ok(),
            "concurrent Profile migration failed: {result:?}"
        );
    }

    let migrated = AgentProfileCatalog::open(profile_path)?;
    assert_eq!(
        migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
        super::super::AGENT_PROFILE_SCHEMA_VERSION,
    );
    assert_eq!(
        migrated.resolve(fixture.profile.id())?.as_ref(),
        Some(&fixture.profile)
    );
    assert_eq!(
        migrated.logical_session_link(fixture.session.id())?,
        Some(history_before.links()[0])
    );
    assert_eq!(
        migrated.logical_session_history(fixture.profile.id())?,
        history_before
    );
    let pin_count: i64 = migrated.connection.query_row(
        "SELECT COUNT(*) FROM agent_profile_home_acceptance_pins",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(pin_count, 0, "v6 migration fabricates no Home pin");
    Ok(())
}
