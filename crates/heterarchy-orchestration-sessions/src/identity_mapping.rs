//! Verified bridge between the new global Agent Profile catalog and the
//! existing app-global logical Agent Session authority.
//!
//! This bridge only records identity correspondence. It does not create a
//! Workspace participant, restore a role, claim leadership, or grant Forge
//! effect authority.

use forge_domain::{AgentSessionId, AuthenticatedPrincipal, TimestampMillis};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AgentProfileCatalog, AgentProfileError, AgentProfileId, AgentProfileLogicalSessionLinkOutcome,
    AgentSessionCatalog, AgentSessionCatalogError,
};

/// Link an already-issued logical Session to one global Agent Profile after
/// resolving the Session from the authoritative app-global Session catalog.
///
/// Closed Sessions remain linkable as historical identity evidence. A missing
/// or foreign-principal Session is intentionally indistinguishable here and is
/// rejected before the Profile catalog mutates.
///
/// # Errors
/// Returns Session authority failures, missing/foreign Session, or Profile
/// mapping persistence/conflict failures.
pub fn link_existing_logical_session(
    profile_catalog: &mut AgentProfileCatalog,
    session_catalog: &AgentSessionCatalog,
    profile_id: AgentProfileId,
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    request_id: Uuid,
    linked_at: TimestampMillis,
) -> Result<AgentProfileLogicalSessionLinkOutcome, AgentIdentityMappingError> {
    let Some(session) = session_catalog.resolve(session_id, principal)? else {
        return Err(AgentIdentityMappingError::SessionNotFound);
    };
    profile_catalog
        .link_verified_logical_session(
            request_id,
            profile_id,
            session_id,
            session.revision(),
            linked_at,
        )
        .map_err(Into::into)
}

/// Bounded errors at the Profile-to-logical-Session mapping boundary.
#[derive(Debug, Error)]
pub enum AgentIdentityMappingError {
    #[error("logical Agent Session was not found for the authenticated principal")]
    SessionNotFound,
    #[error("logical Agent Session authority failed")]
    SessionCatalog(#[from] AgentSessionCatalogError),
    #[error("global Agent Profile mapping failed")]
    ProfileCatalog(#[from] AgentProfileError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentProfileCreationOrigin, AgentProfileProviderFamily, CreateAgentProfile};
    use forge_domain::{AgentSessionOpenRequestId, OpenAgentSession};
    use std::error::Error;
    use std::fs;

    struct Fixture {
        root: std::path::PathBuf,
        profiles: AgentProfileCatalog,
        sessions: AgentSessionCatalog,
        principal: AuthenticatedPrincipal,
        profile_id: AgentProfileId,
        session_id: AgentSessionId,
    }

    fn fixture(name: &str) -> Result<Fixture, Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-agent-identity-mapping-{name}-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let mut profiles = AgentProfileCatalog::open(root.join("agent-profiles.sqlite3"))?;
        let mut sessions = AgentSessionCatalog::open(root.join("agent-sessions.sqlite3"))?;
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xD101))?;
        let profile = CreateAgentProfile::new(
            profile_id,
            Uuid::from_u128(0xD102),
            "luna-rust-01".to_owned(),
            "Luna Rust 01".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            Some("gpt-5.6-luna".to_owned()),
            Some("max".to_owned()),
            vec!["rust".to_owned()],
            AgentProfileCreationOrigin::UserManual,
            TimestampMillis::new(10)?,
        )?;
        let _created = profiles.create_profile(&profile)?;
        let principal = AuthenticatedPrincipal::TrustedStdio;
        let session_id = AgentSessionId::from_uuid(Uuid::from_u128(0xD103));
        let _session = sessions.open_session(&OpenAgentSession::new(
            session_id,
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xD104)),
            principal,
            TimestampMillis::new(11)?,
        ))?;
        Ok(Fixture {
            root,
            profiles,
            sessions,
            principal,
            profile_id,
            session_id,
        })
    }

    #[test]
    fn verified_session_link_is_idempotent_and_preserves_session_authority()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture("verified")?;
        let before = fixture
            .sessions
            .resolve(fixture.session_id, fixture.principal)?
            .ok_or("session missing before link")?;
        let first = link_existing_logical_session(
            &mut fixture.profiles,
            &fixture.sessions,
            fixture.profile_id,
            fixture.session_id,
            fixture.principal,
            Uuid::from_u128(0xD105),
            TimestampMillis::new(12)?,
        )?;
        assert!(first.created());
        assert_eq!(first.link().profile_revision().get(), 1);
        assert_eq!(first.link().session_revision().get(), 1);
        let replay = link_existing_logical_session(
            &mut fixture.profiles,
            &fixture.sessions,
            fixture.profile_id,
            fixture.session_id,
            fixture.principal,
            Uuid::from_u128(0xD105),
            TimestampMillis::new(13)?,
        )?;
        assert!(!replay.created());
        assert_eq!(replay.link(), first.link());
        let after = fixture
            .sessions
            .resolve(fixture.session_id, fixture.principal)?
            .ok_or("session missing after link")?;
        assert_eq!(before, after);
        drop(fixture.profiles);
        drop(fixture.sessions);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn missing_session_is_rejected_before_profile_mapping_mutation() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture("missing")?;
        let missing = AgentSessionId::from_uuid(Uuid::from_u128(0xD106));
        let result = link_existing_logical_session(
            &mut fixture.profiles,
            &fixture.sessions,
            fixture.profile_id,
            missing,
            fixture.principal,
            Uuid::from_u128(0xD107),
            TimestampMillis::new(12)?,
        );
        assert!(matches!(
            result,
            Err(AgentIdentityMappingError::SessionNotFound)
        ));
        assert_eq!(fixture.profiles.logical_session_link(missing)?, None);
        drop(fixture.profiles);
        drop(fixture.sessions);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn one_logical_session_cannot_map_to_two_profiles() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture("profile-conflict")?;
        let _first = link_existing_logical_session(
            &mut fixture.profiles,
            &fixture.sessions,
            fixture.profile_id,
            fixture.session_id,
            fixture.principal,
            Uuid::from_u128(0xD108),
            TimestampMillis::new(12)?,
        )?;
        let other_profile = AgentProfileId::from_uuid(Uuid::from_u128(0xD109))?;
        let _created = fixture.profiles.create_profile(&CreateAgentProfile::new(
            other_profile,
            Uuid::from_u128(0xD10A),
            "luna-review-01".to_owned(),
            "Luna Review 01".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            None,
            None,
            vec!["verification".to_owned()],
            AgentProfileCreationOrigin::UserManual,
            TimestampMillis::new(13)?,
        )?)?;
        let conflict = link_existing_logical_session(
            &mut fixture.profiles,
            &fixture.sessions,
            other_profile,
            fixture.session_id,
            fixture.principal,
            Uuid::from_u128(0xD10B),
            TimestampMillis::new(14)?,
        );
        assert!(matches!(
            conflict,
            Err(AgentIdentityMappingError::ProfileCatalog(
                AgentProfileError::LogicalSessionProfileConflict { .. }
            ))
        ));
        drop(fixture.profiles);
        drop(fixture.sessions);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }
}
