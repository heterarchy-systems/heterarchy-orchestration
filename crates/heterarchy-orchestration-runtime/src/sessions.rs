//! Logical session application operations over the existing canonical catalog.

use forge_domain::{
    AgentSessionId, AgentSessionOpenRequestId, AgentSessionRevision, AgentSessionState,
    AuthenticatedPrincipal, OpenAgentSession, RegisteredAgentSession, TimestampMillis,
};
use heterarchy_orchestration_sessions::{AgentSessionCatalog, AgentSessionCatalogError};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

const SESSION_CATALOG_FILE_NAME: &str = "agent-sessions.sqlite3";

/// Principal-owned lifecycle projection; identifiers confer no permission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    pub session_id: Uuid,
    pub request_id: Uuid,
    pub state: &'static str,
    pub revision: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// An exact open replay returns the original server-issued identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOpenSummary {
    pub session: SessionSummary,
    pub created: bool,
}

impl OrchestrationRuntime {
    /// Issue or recover one session for the authenticated caller.
    ///
    /// # Errors
    /// Returns invalid identity/time, replay conflicts and catalog failures.
    pub fn open_session(
        &self,
        request_id: Uuid,
        principal: AuthenticatedPrincipal,
        now_ms: i64,
    ) -> Result<SessionOpenSummary, RuntimeError> {
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let observed_at = TimestampMillis::new(now_ms).map_err(|_| RuntimeError::InvalidInput)?;
        let mut catalog =
            AgentSessionCatalog::open(self.data_root().join(SESSION_CATALOG_FILE_NAME))
                .map_err(session_error)?;
        let outcome = catalog
            .open_session(&OpenAgentSession::new(
                AgentSessionId::from_uuid(Uuid::now_v7()),
                AgentSessionOpenRequestId::from_uuid(request_id),
                principal,
                observed_at,
            ))
            .map_err(session_error)?;
        Ok(SessionOpenSummary {
            session: session_summary(outcome.session()),
            created: outcome.created(),
        })
    }

    /// Read an issued session only for its exact principal, including terminal
    /// state for recovery/readback. This never creates or migrates the catalog.
    ///
    /// # Errors
    /// Returns forbidden for missing/foreign identities and catalog failures.
    pub fn session(
        &self,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<SessionSummary, RuntimeError> {
        self.resolve_owned_session(session_id, principal)
            .map(|session| session_summary(&session))
    }

    /// Read an exact caller-owned Session-open receipt, including terminal state.
    /// This never creates a Session or migrates a missing catalog.
    ///
    /// # Errors
    /// Rejects nil request identities and catalog failures.
    pub fn session_open_readback(
        &self,
        request_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<SessionSummary>, RuntimeError> {
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let Some(catalog) = AgentSessionCatalog::open_existing_read_only(
            self.data_root().join(SESSION_CATALOG_FILE_NAME),
        )
        .map_err(session_error)?
        else {
            return Ok(None);
        };
        catalog
            .resolve_open_request(AgentSessionOpenRequestId::from_uuid(request_id), principal)
            .map(|session| session.as_ref().map(session_summary))
            .map_err(session_error)
    }

    /// Revalidate a body's session selector immediately before participant work.
    ///
    /// # Errors
    /// Rejects foreign, missing and closed sessions; authority failure never
    /// substitutes another catalog or cached permission.
    pub fn require_open_session(
        &self,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<(), RuntimeError> {
        let session = self.resolve_owned_session(session_id, principal)?;
        if session.state() != AgentSessionState::Open {
            return Err(RuntimeError::Forbidden);
        }
        Ok(())
    }

    /// Close the caller's session under its exact current revision.
    ///
    /// # Errors
    /// Returns forbidden, stale/terminal conflicts and catalog failures.
    pub fn close_session(
        &self,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        expected_revision: i64,
        now_ms: i64,
    ) -> Result<SessionSummary, RuntimeError> {
        if session_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let mut catalog =
            AgentSessionCatalog::open_existing(self.data_root().join(SESSION_CATALOG_FILE_NAME))
                .map_err(session_error)?
                .ok_or(RuntimeError::Forbidden)?;
        catalog
            .close(
                AgentSessionId::from_uuid(session_id),
                principal,
                AgentSessionRevision::new(expected_revision)
                    .map_err(|_| RuntimeError::InvalidInput)?,
                TimestampMillis::new(now_ms).map_err(|_| RuntimeError::InvalidInput)?,
            )
            .map(|session| session_summary(&session))
            .map_err(session_error)
    }

    pub(super) fn resolve_owned_session(
        &self,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<RegisteredAgentSession, RuntimeError> {
        if session_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = AgentSessionCatalog::open_existing_read_only(
            self.data_root().join(SESSION_CATALOG_FILE_NAME),
        )
        .map_err(session_error)?
        .ok_or(RuntimeError::Forbidden)?;
        catalog
            .resolve(AgentSessionId::from_uuid(session_id), principal)
            .map_err(session_error)?
            .ok_or(RuntimeError::Forbidden)
    }
}

fn session_summary(session: &RegisteredAgentSession) -> SessionSummary {
    SessionSummary {
        session_id: session.id().into_uuid(),
        request_id: session.request_id().into_uuid(),
        state: match session.state() {
            AgentSessionState::Open => "open",
            AgentSessionState::Closed => "closed",
        },
        revision: session.revision().get(),
        created_at_ms: session.created_at().get(),
        updated_at_ms: session.updated_at().get(),
    }
}

fn session_error(error: AgentSessionCatalogError) -> RuntimeError {
    match error {
        AgentSessionCatalogError::SessionMissing(_)
        | AgentSessionCatalogError::ForeignPrincipal(_) => RuntimeError::Forbidden,
        AgentSessionCatalogError::SessionIdConflict(_)
        | AgentSessionCatalogError::RevisionConflict(_)
        | AgentSessionCatalogError::Transition(_) => RuntimeError::Conflict,
        AgentSessionCatalogError::Timestamp(_) | AgentSessionCatalogError::Revision(_) => {
            RuntimeError::InvalidInput
        }
        other => RuntimeError::SessionCatalog(other),
    }
}
