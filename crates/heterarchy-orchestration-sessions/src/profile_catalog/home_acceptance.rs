//! Profile-catalog owner for recipient Profile pins during Home acceptance.

mod model;
mod repository;
pub(super) mod schema;

#[cfg(test)]
mod tests;

use forge_domain::{
    AgentSessionRevision, HomeHandoffTakeRequest, OrchestrationIssuerId, TimestampMillis,
};
use forge_persistence::HomeHandoffAcceptanceOutcomeRecord;
use thiserror::Error;
use uuid::Uuid;

use super::{
    AgentProfile, AgentProfileCatalog, AgentProfileError, AgentProfileId,
    AgentProfileLifecycleState, AgentProfileLogicalSessionLink, AgentProfileRevision,
    logical_session_link_by_session,
};
use repository::{acquire, get, pending, resolve};

pub use model::{
    HomeAcceptanceProfileAcquireOutcome, HomeAcceptanceProfileIntent,
    HomeAcceptanceProfilePendingPage, HomeAcceptanceProfilePinRecord, HomeAcceptanceProfileState,
};
pub use model::{
    HomeAcceptanceProfileOperationsAdmission, HomeAcceptanceProfileOperationsResolution,
    HomeAcceptanceProfilePinReference,
};

pub(super) const MAX_PENDING_PAGE_SIZE: u16 = 64;

#[derive(Debug, Error)]
pub enum HomeAcceptanceProfileError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Domain(#[from] forge_domain::HomeHandoffTakeRequestSnapshotError),
    #[error(transparent)]
    Timestamp(#[from] forge_domain::TimestampMillisError),
    #[error("Profile owner lookup failed")]
    Profile(#[source] Box<AgentProfileError>),
    #[error("Home Profile pin intent or owner receipt is invalid: {0}")]
    Invalid(&'static str),
    #[error("Home Profile pin conflicts with current Profile or logical Session link")]
    Conflict,
    #[error("Home Profile pin is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("Home Profile acceptance page limit must be between 1 and {MAX_PENDING_PAGE_SIZE}")]
    PageLimit,
    #[error("Home Profile pin Source outcome is not confirmed; exact owner readback is required")]
    SourceOutcomeUnknown,
    #[error(
        "Profile Home-pin clock regressed: expected at least {expected_at_least:?}, observed {observed:?}"
    )]
    Clock {
        profile_id: AgentProfileId,
        expected_at_least: TimestampMillis,
        observed: TimestampMillis,
    },
    #[error("Profile Home-pin schema is missing or damaged at {0}")]
    Schema(&'static str),
    #[error("Home Profile pin transaction may have committed for acceptance {acceptance_id}")]
    CommitUnconfirmed {
        acceptance_id: Uuid,
        #[source]
        source: Box<rusqlite::Error>,
    },
    #[error(
        "Home Profile pin {acceptance_id} committed but exact catalog binding could not be confirmed"
    )]
    ReadbackRequired {
        acceptance_id: Uuid,
        #[source]
        source: Box<AgentProfileError>,
    },
}

impl From<HomeAcceptanceProfileError> for AgentProfileError {
    fn from(error: HomeAcceptanceProfileError) -> Self {
        match error {
            HomeAcceptanceProfileError::Sql(source) => Self::Sql(source),
            HomeAcceptanceProfileError::Domain(_) => Self::Corrupt("Home take request snapshot"),
            HomeAcceptanceProfileError::Timestamp(source) => Self::Timestamp(source),
            HomeAcceptanceProfileError::Profile(source) => *source,
            HomeAcceptanceProfileError::Clock {
                profile_id,
                expected_at_least,
                observed,
            } => Self::ProfileClockRegression {
                profile_id,
                expected_at_least: expected_at_least.get(),
                observed: observed.get(),
            },
            HomeAcceptanceProfileError::Schema(name) => Self::Schema(name),
            HomeAcceptanceProfileError::Invalid("Profile is pinned by a Home acceptance") => {
                Self::HomeAcceptanceProfilePinned
            }
            HomeAcceptanceProfileError::Conflict
            | HomeAcceptanceProfileError::SourceOutcomeUnknown => {
                Self::HomeAcceptanceProfileConflict
            }
            HomeAcceptanceProfileError::PageLimit => Self::HomeAcceptanceProfilePageLimit,
            HomeAcceptanceProfileError::Corrupt(message) => Self::Corrupt(message),
            HomeAcceptanceProfileError::Invalid(_) => {
                Self::Corrupt("Home Profile acceptance input")
            }
            HomeAcceptanceProfileError::CommitUnconfirmed {
                acceptance_id,
                source,
            } => Self::HomeAcceptanceProfileCommitUnconfirmed {
                acceptance_id,
                source: *source,
            },
            HomeAcceptanceProfileError::ReadbackRequired {
                acceptance_id,
                source,
            } => Self::HomeAcceptanceProfileReadbackRequired {
                acceptance_id,
                source,
            },
        }
    }
}

impl From<AgentProfileError> for HomeAcceptanceProfileError {
    fn from(error: AgentProfileError) -> Self {
        Self::Profile(Box::new(error))
    }
}

impl AgentProfileCatalog {
    /// Pin the exact Active recipient Profile for one Source Home request.
    /// The caller supplies an authenticated Session hold observation; the
    /// Profile owner then validates the immutable Profile/Session link and
    /// current Profile revision in this catalog's write transaction.
    ///
    /// The Session observation is not a live cross-catalog credential. The
    /// trusted Runtime authenticates it and Orders owner acquisitions; Source
    /// Apply remains impossible until the real Operations admission commits.
    ///
    /// # Errors
    /// Rejects changed intent, issuer, stale Profile/link or Session pins,
    /// invalid schema, and uncertain commit/path readback.
    pub fn acquire_home_acceptance_profile<C>(
        &mut self,
        intent: &HomeAcceptanceProfileIntent,
        configured_issuer: OrchestrationIssuerId,
        session_hold: crate::catalog::HomeAcceptanceSessionHoldReference,
        sample_clock: C,
    ) -> Result<HomeAcceptanceProfileAcquireOutcome, AgentProfileError>
    where
        C: FnOnce() -> Result<TimestampMillis, HomeAcceptanceProfileError>,
    {
        if configured_issuer != intent.issuer()
            || session_hold.acceptance_id() != intent.request().acceptance_id()
            || session_hold.request_digest() != *intent.request().request_digest().as_bytes()
            || session_hold.session_id() != intent.request().recipient().session_id()
            || session_hold.principal() != intent.request().recipient().principal()
        {
            return Err(HomeAcceptanceProfileError::Conflict.into());
        }
        self.ensure_live_binding()?;
        let outcome =
            acquire(self, intent, session_hold, sample_clock).map_err(AgentProfileError::from)?;
        finish_mutation(self, intent.request().acceptance_id())?;
        Ok(outcome)
    }

    /// Settle one Profile pin using actual Source and Operations owner reads.
    /// DTO fields alone are not authenticated; Runtime must source these exact
    /// records from the configured owners before calling this native API.
    ///
    /// # Errors
    /// Rejects a stale Source terminal, mismatched absorbing Operations
    /// resolution, changed intent, corruption, or uncertain commit/readback.
    pub fn resolve_home_acceptance_profile<C>(
        &mut self,
        intent: &HomeAcceptanceProfileIntent,
        configured_issuer: OrchestrationIssuerId,
        source_terminal: &HomeHandoffAcceptanceOutcomeRecord,
        operations: &HomeAcceptanceProfileOperationsResolution,
        sample_clock: C,
    ) -> Result<HomeAcceptanceProfilePinRecord, AgentProfileError>
    where
        C: FnOnce() -> Result<TimestampMillis, HomeAcceptanceProfileError>,
    {
        if configured_issuer != intent.issuer()
            || source_terminal.issuer() != intent.issuer()
            || source_terminal.request() != intent.request()
        {
            return Err(HomeAcceptanceProfileError::Conflict.into());
        }
        self.ensure_live_binding()?;
        let record = resolve(self, intent, source_terminal, operations, sample_clock)
            .map_err(AgentProfileError::from)?;
        finish_mutation(self, intent.request().acceptance_id())?;
        Ok(record)
    }

    /// Read one exact Profile pin. The caller passes its authenticated issuer;
    /// the lookup is never an unauthenticated Profile-existence oracle.
    ///
    /// # Errors
    /// Rejects foreign owner, changed intent, corrupt Profile/link pins or
    /// invalid catalog schema.
    pub fn home_acceptance_profile(
        &self,
        intent: &HomeAcceptanceProfileIntent,
        configured_issuer: OrchestrationIssuerId,
    ) -> Result<Option<HomeAcceptanceProfilePinRecord>, AgentProfileError> {
        if configured_issuer != intent.issuer() {
            return Err(HomeAcceptanceProfileError::Conflict.into());
        }
        self.ensure_live_binding()?;
        let record = get(&self.connection, intent).map_err(AgentProfileError::from)?;
        self.ensure_live_binding()?;
        Ok(record)
    }

    /// Return one bounded owner page of unresolved Profile pins.
    ///
    /// # Errors
    /// Rejects zero/oversized pages, foreign owner, corrupt row/link, or schema
    /// and SQLite errors.
    pub fn pending_home_acceptance_profiles(
        &self,
        configured_issuer: OrchestrationIssuerId,
        after_acceptance_id: Option<Uuid>,
        limit: u16,
    ) -> Result<HomeAcceptanceProfilePendingPage, AgentProfileError> {
        if configured_issuer.into_uuid().is_nil() {
            return Err(HomeAcceptanceProfileError::Conflict.into());
        }
        self.ensure_live_binding()?;
        let page = pending(
            &self.connection,
            configured_issuer,
            after_acceptance_id,
            limit,
        )
        .map_err(AgentProfileError::from)?;
        self.ensure_live_binding()?;
        Ok(page)
    }
}

fn finish_mutation(
    catalog: &mut AgentProfileCatalog,
    acceptance_id: Uuid,
) -> Result<(), AgentProfileError> {
    catalog
        .ensure_live_binding()
        .map_err(|source| HomeAcceptanceProfileError::ReadbackRequired {
            acceptance_id,
            source: Box::new(source),
        })
        .map_err(AgentProfileError::from)
}

pub(super) fn ensure_unpinned(
    connection: &rusqlite::Connection,
    profile_id: AgentProfileId,
) -> Result<(), AgentProfileError> {
    let held: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins WHERE profile_id=?1 AND state=1)",
        [profile_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if held {
        return Err(
            HomeAcceptanceProfileError::Invalid("Profile is pinned by a Home acceptance").into(),
        );
    }
    Ok(())
}

pub(super) fn validate_profile_link(
    connection: &rusqlite::Connection,
    request: &HomeHandoffTakeRequest,
    profile: &AgentProfile,
    held_session_revision: AgentSessionRevision,
) -> Result<AgentProfileLogicalSessionLink, HomeAcceptanceProfileError> {
    if profile.lifecycle_state() != AgentProfileLifecycleState::Active {
        return Err(HomeAcceptanceProfileError::Conflict);
    }
    let link = logical_session_link_by_session(connection, request.recipient().session_id())?
        .ok_or(HomeAcceptanceProfileError::Conflict)?;
    if link.profile_id() != profile.id()
        || link.session_id() != request.recipient().session_id()
        || link.session_revision() > held_session_revision
        || link.profile_revision() > profile.revision()
    {
        return Err(HomeAcceptanceProfileError::Conflict);
    }
    Ok(link)
}

pub(super) fn ensure_pinned_profile_revision(
    current: &AgentProfile,
    expected_revision: AgentProfileRevision,
) -> Result<(), HomeAcceptanceProfileError> {
    if current.lifecycle_state() != AgentProfileLifecycleState::Active
        || current.revision() != expected_revision
    {
        return Err(HomeAcceptanceProfileError::Conflict);
    }
    Ok(())
}
