//! Orchestration-owned app-global Agent identity catalogs and native Forge
//! logical-session authority binding.
//!
//! Logical Sessions remain the authority used by existing Workspace
//! participation. The Global Agent Profile catalog is intentionally introduced
//! as a separate non-authoritative reusable identity catalog until the planned
//! Profile-to-participation mapping cutover is proven. Forge retains its exact
//! Workspace effect checks and never treats a provider error as permission to
//! proceed.

use forge_control::{
    AgentSessionAuthority, AgentSessionAuthorityFailure, AgentSessionResolveRequest, ControlError,
    ForgeControl,
};
use forge_domain::{
    AgentSessionId, AgentSessionOpenOutcome, AgentSessionRevision, AuthenticatedPrincipal,
    OpenAgentSession, RegisteredAgentSession, TimestampMillis,
};
use forge_persistence::GlobalRegistry;
use forge_workspace::LocationService;
use std::path::PathBuf;

mod catalog;
mod identity_mapping;
mod profile_catalog;
pub use catalog::{
    AGENT_SESSION_APPLICATION_ID, AGENT_SESSION_SCHEMA_VERSION, AgentSessionCatalog,
    AgentSessionCatalogError,
};
pub use identity_mapping::{AgentIdentityMappingError, link_existing_logical_session};
pub use profile_catalog::MAX_AGENT_PROFILE_RUNTIME_SESSIONS;
pub use profile_catalog::{
    AGENT_PROFILE_APPLICATION_ID, AGENT_PROFILE_SCHEMA_VERSION, AgentProfile, AgentProfileCatalog,
    AgentProfileCreateOutcome, AgentProfileCreationOrigin, AgentProfileDeletionCommand,
    AgentProfileDeletionIntent, AgentProfileDeletionIntentPage, AgentProfileDeletionManifest,
    AgentProfileDeletionRuntimeEntry, AgentProfileDeletionRuntimeEntryPage,
    AgentProfileDeletionRuntimeTarget, AgentProfileError, AgentProfileId,
    AgentProfileLifecycleState, AgentProfileLogicalSessionLink,
    AgentProfileLogicalSessionLinkOutcome, AgentProfileOperationalState,
    AgentProfileProviderFamily, AgentProfileRevision, AgentProfileRuntimeSession,
    AgentProfileRuntimeSessionCreateOutcome, AgentProfileRuntimeSessionHistory,
    AgentProfileRuntimeSessionId, AgentProfileRuntimeSessionPage,
    AgentProfileRuntimeSessionRevision, AgentProfileRuntimeSessionState, CreateAgentProfile,
    CreateAgentProfileRuntimeSession, ProfileConfiguration, ProfileMutation,
    ProfileMutationCommand, ProfileMutationKind, ProfileMutationReceipt,
};
pub use profile_catalog::{
    AgentProfileLogicalSessionHistory, MAX_AGENT_PROFILE_LOGICAL_SESSION_LINKS,
};
pub use profile_catalog::{
    AgentProfileResolutionClaim, AgentProfileResolutionClaimCommand,
    AgentProfileResolutionClaimKind, AgentProfileResolutionClaimTarget,
    AgentProfileResolverAvailability, AgentProfileResolverCandidate,
    AgentProfileResolverCandidates, MAX_AGENT_PROFILE_RESOLVER_CANDIDATES,
};

/// An explicitly selected durable catalog; no cached session decisions.
#[derive(Debug)]
pub struct SessionCatalogAuthority {
    path: PathBuf,
}

impl SessionCatalogAuthority {
    /// Bind the path without opening or creating a database.
    #[must_use]
    pub const fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Close one issued session in this authority's existing catalog.
    ///
    /// # Errors
    /// Returns not-found for a missing session/catalog or foreign principal,
    /// conflict for a stale revision/terminal transition, and persistence failures separately.
    pub fn close(
        &self,
        id: AgentSessionId,
        principal: AuthenticatedPrincipal,
        expected_revision: AgentSessionRevision,
        observed_at: TimestampMillis,
    ) -> Result<RegisteredAgentSession, ControlError> {
        let mut catalog = AgentSessionCatalog::open_existing(&self.path)
            .map_err(|error| map_catalog_error(&error))?
            .ok_or_else(|| AgentSessionAuthorityFailure::NotFound.into_control_error())?;
        catalog
            .close(id, principal, expected_revision, observed_at)
            .map_err(|error| map_catalog_error(&error))
    }
}

impl AgentSessionAuthority for SessionCatalogAuthority {
    fn open(&self, request: &OpenAgentSession) -> Result<AgentSessionOpenOutcome, ControlError> {
        AgentSessionCatalog::open(&self.path)
            .map_err(|error| map_catalog_error(&error))?
            .open_session(request)
            .map_err(|error| map_catalog_error(&error))
    }

    fn resolve(
        &self,
        request: AgentSessionResolveRequest,
    ) -> Result<Option<RegisteredAgentSession>, ControlError> {
        let Some(catalog) = AgentSessionCatalog::open_existing_read_only(&self.path)
            .map_err(|error| map_catalog_error(&error))?
        else {
            return Ok(None);
        };
        catalog
            .resolve(request.session_id, request.principal)
            .map_err(|error| map_catalog_error(&error))
    }
}

/// Native host composition with the existing explicit registry-relative path.
#[must_use]
pub fn control_with_session_catalog(registry: GlobalRegistry) -> ForgeControl {
    let path = registry
        .database_path()
        .with_file_name("agent-sessions.sqlite3");
    control_with_location_service(LocationService::new(registry), path)
}

/// Resolve the app-global, non-authoritative Agent Profile catalog path beside
/// the selected registry. Path resolution is observational; the database is
/// not created until [`AgentProfileCatalog::open`] is explicitly called.
#[must_use]
pub fn agent_profile_catalog_path(registry: &GlobalRegistry) -> PathBuf {
    registry
        .database_path()
        .with_file_name("agent-profiles.sqlite3")
}

/// Native host composition with an already owned Location service and session path.
#[must_use]
pub fn control_with_location_service(
    locations: LocationService<GlobalRegistry>,
    path: PathBuf,
) -> ForgeControl {
    ForgeControl::with_session_authority(locations, Box::new(SessionCatalogAuthority::new(path)))
}

// The native authority boundary carries only stable redacted session failures.
// Detailed SQLite/path errors remain available from the owned catalog API.
fn map_catalog_error(error: &AgentSessionCatalogError) -> ControlError {
    match error {
        AgentSessionCatalogError::SessionMissing(_)
        | AgentSessionCatalogError::ForeignPrincipal(_) => AgentSessionAuthorityFailure::NotFound,
        AgentSessionCatalogError::SessionIdConflict(_)
        | AgentSessionCatalogError::RevisionConflict(_)
        | AgentSessionCatalogError::Transition(_) => AgentSessionAuthorityFailure::Conflict,
        _ => AgentSessionAuthorityFailure::Unavailable,
    }
    .into_control_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution_does_not_initialize_an_empty_existing_catalog()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("session-resolve-empty-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("agent-sessions.sqlite3");
        drop(rusqlite::Connection::open(&path)?);
        let before = std::fs::read(&path)?;
        let authority = SessionCatalogAuthority::new(path.clone());
        assert!(
            authority
                .resolve(forge_control::AgentSessionResolveRequest {
                    session_id: forge_domain::AgentSessionId::from_uuid(uuid::Uuid::now_v7()),
                    principal: forge_domain::AuthenticatedPrincipal::TrustedStdio,
                })
                .is_err()
        );
        assert_eq!(std::fs::read(&path)?, before);
        assert_eq!(std::fs::read_dir(&root)?.count(), 1);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
    use forge_control::{ControlErrorCode, ForgeControlWorker};
    use forge_domain::{AgentSessionOpenRequestId, AgentSessionState};
    use uuid::Uuid;

    #[test]
    fn owned_close_fences_an_existing_forge_worker_and_replay_stays_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("orchestration-session-binding-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let registry = GlobalRegistry::open(root.join("registry.sqlite3"))?;
        let path = root.join("selected-sessions.sqlite3");
        let authority = SessionCatalogAuthority::new(path.clone());
        let control = control_with_location_service(LocationService::new(registry), path);
        let (client, mut worker) = ForgeControlWorker::start(control)?;
        let principal = AuthenticatedPrincipal::TrustedStdio;
        let nonce = AgentSessionOpenRequestId::from_uuid(Uuid::now_v7());
        let first = client.agent_session_open(OpenAgentSession::new(
            AgentSessionId::from_uuid(Uuid::now_v7()),
            nonce,
            principal,
            TimestampMillis::new(10)?,
        ))?;
        let id = first.session().id();
        let resolved = client.agent_session_resolve(AgentSessionResolveRequest {
            session_id: id,
            principal,
        })?;
        assert_eq!(resolved, *first.session());
        let closed = authority.close(
            id,
            principal,
            first.session().revision(),
            TimestampMillis::new(11)?,
        )?;
        assert!(
            matches!(client.agent_session_resolve(AgentSessionResolveRequest {session_id:id, principal}),
            Err(error) if error.code() == ControlErrorCode::AgentSessionClosed)
        );
        let replay = client.agent_session_open(OpenAgentSession::new(
            AgentSessionId::from_uuid(Uuid::now_v7()),
            nonce,
            principal,
            TimestampMillis::new(12)?,
        ))?;
        assert!(!replay.created());
        assert_eq!(replay.session(), &closed);
        assert_eq!(closed.state(), AgentSessionState::Closed);
        let corrupt = b"not a SQLite session catalog";
        std::fs::write(root.join("selected-sessions.sqlite3"), corrupt)?;
        assert!(
            matches!(client.agent_session_resolve(AgentSessionResolveRequest {session_id:id, principal}),
            Err(error) if error.code() == ControlErrorCode::AgentSessionAuthorityUnavailable)
        );
        assert_eq!(
            std::fs::read(root.join("selected-sessions.sqlite3"))?,
            corrupt
        );
        assert!(
            !root.join("agent-sessions.sqlite3").exists(),
            "must not initialize a fallback catalog"
        );
        drop(client);
        worker.shutdown()?;
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
