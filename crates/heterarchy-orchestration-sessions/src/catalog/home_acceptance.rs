//! Durable logical Session holds for exact Home accept-and-take requests.

mod model;
mod repository;
pub(super) mod schema;

#[cfg(test)]
mod tests;

pub use model::{
    HomeAcceptanceOperationsAdmission, HomeAcceptanceOperationsResolution,
    HomeAcceptanceSessionHoldReference, HomeAcceptanceSessionIntent, HomeAcceptanceSessionRecord,
    HomeAcceptanceSessionResolution, HomeAcceptanceSessionState,
};

use forge_domain::{
    AgentSessionId, AuthenticatedPrincipal, OrchestrationIssuerId, TimestampMillis,
};
use forge_persistence::HomeHandoffAcceptanceOutcomeRecord;
use rusqlite::Transaction;
use thiserror::Error;
use uuid::Uuid;

use super::{AgentSessionCatalog, AgentSessionCatalogError};
use repository::{acquire, get, pending, resolve};

const MAX_PENDING_PAGE_SIZE: u16 = 64;
/// One native acquire result. Resolved means a prior exact Aborted outcome
/// fenced this request before its delayed acquisition arrived.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeAcceptanceSessionAcquireOutcome {
    Held {
        record: HomeAcceptanceSessionRecord,
        created: bool,
    },
    Resolved(HomeAcceptanceSessionRecord),
}

/// Bounded page of unresolved Session holds, scoped to the selected issuer.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeAcceptanceSessionPendingPage {
    records: Vec<HomeAcceptanceSessionRecord>,
    next_after: Option<Uuid>,
}

impl HomeAcceptanceSessionPendingPage {
    pub fn records(&self) -> &[HomeAcceptanceSessionRecord] {
        &self.records
    }

    #[must_use]
    pub const fn next_after(&self) -> Option<Uuid> {
        self.next_after
    }
}

/// Typed Session-owner persistence and recovery failures.
#[derive(Debug, Error)]
pub enum HomeAcceptanceSessionError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error("Agent Session catalog readback failed")]
    Catalog(#[source] Box<AgentSessionCatalogError>),
    #[error("logical Session does not match the requested Home hold")]
    Session(#[source] Box<AgentSessionCatalogError>),
    #[error(transparent)]
    Snapshot(#[from] forge_domain::HomeHandoffTakeRequestSnapshotError),
    #[error("invalid Home Session hold intent: {0}")]
    InvalidIntent(&'static str),
    #[error("invalid Home Session hold resolution: {0}")]
    InvalidResolution(&'static str),
    #[error("Home Session hold request conflicts with prior history")]
    Conflict,
    #[error("Home Session hold record is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("Home Session catalog schema is missing or damaged at {0}")]
    Schema(&'static str),
    #[error("Home Session hold page size must be between 1 and {MAX_PENDING_PAGE_SIZE}")]
    PageSize,
    #[error("Home Session hold clock could not establish a valid local transition")]
    Clock,
    #[error(
        "Home acceptance {acceptance_id} may have committed; exact Session hold readback is required"
    )]
    CommitUnconfirmed {
        acceptance_id: Uuid,
        #[source]
        source: Box<rusqlite::Error>,
    },
    #[error(
        "Home acceptance {acceptance_id} committed but Session catalog binding/readback failed"
    )]
    ReadbackRequired {
        acceptance_id: Uuid,
        #[source]
        source: Box<AgentSessionCatalogError>,
    },
}

impl From<AgentSessionCatalogError> for HomeAcceptanceSessionError {
    fn from(error: AgentSessionCatalogError) -> Self {
        Self::Catalog(Box::new(error))
    }
}

#[cfg(test)]
pub(super) fn downgrade_empty_v2_for_fixture(
    connection: &mut rusqlite::Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    if !connection.is_autocommit() {
        return Err("fixture migration requires autocommit".into());
    }
    schema::validate_schema(connection)?;
    let row_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM home_acceptance_session_pins",
        [],
        |row| row.get(0),
    )?;
    if row_count != 0 {
        return Err("refusing to downgrade a populated Home acceptance fixture".into());
    }
    let previous_foreign_keys: i64 =
        connection.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    connection.pragma_update(None, "foreign_keys", false)?;
    let transition = (|| {
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for (kind, name, _) in schema::OBJECT_SQL.iter().rev() {
            let exists: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
                [name],
                |row| row.get(0),
            )?;
            if exists {
                transaction.execute_batch(&format!("DROP {kind} {name}"))?;
            }
        }
        transaction.pragma_update(None, "user_version", super::AGENT_SESSION_SCHEMA_VERSION_V1)?;
        transaction.commit()?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })();
    let restore = connection
        .pragma_update(None, "foreign_keys", previous_foreign_keys)
        .map_err(Box::<dyn std::error::Error>::from);
    transition.and(restore)?;
    crate::catalog::validate_legacy_schema(connection)?;
    Ok(())
}

fn catalog_error(error: AgentSessionCatalogError) -> HomeAcceptanceSessionError {
    HomeAcceptanceSessionError::Catalog(Box::new(error))
}

/// Native catalog acquire/get/resolve API. Callers authenticate the configured
/// issuer and consume actual Source and Operations owner reads before settling.
impl AgentSessionCatalog {
    /// Commit one exact acceptance intent and pin its currently Open Session.
    ///
    /// A prior exact Held row replays without resampling time. A prior
    /// resolution replays as Resolved and cannot reopen the pin. The principal
    /// and issuer arguments come from the authenticated runtime/configuration,
    /// not from an untrusted request body.
    ///
    /// # Errors
    /// Rejects foreign issuer/principal, changed intent, closed/stale Session,
    /// schema damage, and uncertain commit or catalog binding.
    pub fn acquire_home_acceptance_session<C>(
        &mut self,
        intent: &HomeAcceptanceSessionIntent,
        authenticated_principal: AuthenticatedPrincipal,
        configured_issuer: OrchestrationIssuerId,
        sample_clock: C,
    ) -> Result<HomeAcceptanceSessionAcquireOutcome, AgentSessionCatalogError>
    where
        C: FnOnce() -> Result<TimestampMillis, HomeAcceptanceSessionError>,
    {
        if configured_issuer != intent.issuer() || authenticated_principal != intent.principal() {
            return Err(HomeAcceptanceSessionError::Conflict.into_catalog_error());
        }
        self.ensure_live_binding()?;
        let outcome = acquire(
            self,
            intent,
            authenticated_principal,
            configured_issuer,
            sample_clock,
        )
        .map_err(HomeAcceptanceSessionError::into_catalog_error)?;
        finish_home_mutation(self, intent.acceptance_id())?;
        Ok(outcome)
    }

    /// Resolve an exact Source terminal after Operations has durably settled
    /// admission/absence for this acceptance. This is a trusted native owner
    /// boundary; DTO fields do not authenticate the external catalog reads.
    /// The configured issuer, immutable intent, Source terminal and Operations
    /// resolution must agree. Recovery may settle after the original Session
    /// closes; it never releases by timeout, missing Source row or clock expiry.
    ///
    /// # Errors
    /// Rejects changed Source/Operations evidence, stale identity, incompatible
    /// local hold, schema damage and uncertain commit/readback outcomes.
    pub fn resolve_home_acceptance_session<C>(
        &mut self,
        intent: &HomeAcceptanceSessionIntent,
        configured_issuer: OrchestrationIssuerId,
        source_terminal: &HomeHandoffAcceptanceOutcomeRecord,
        operations: HomeAcceptanceOperationsResolution,
        sample_clock: C,
    ) -> Result<HomeAcceptanceSessionRecord, AgentSessionCatalogError>
    where
        C: FnOnce() -> Result<TimestampMillis, HomeAcceptanceSessionError>,
    {
        if configured_issuer != intent.issuer() {
            return Err(HomeAcceptanceSessionError::Conflict.into_catalog_error());
        }
        self.ensure_live_binding()?;
        let record = resolve(self, intent, source_terminal, operations, sample_clock)
            .map_err(HomeAcceptanceSessionError::into_catalog_error)?;
        finish_home_mutation(self, intent.acceptance_id())?;
        Ok(record)
    }

    /// Read exact acceptance history only for its original issuer and principal.
    /// Missing history remains None; callers cannot turn absence into release.
    ///
    /// # Errors
    /// Rejects foreign owner identities, corrupt rows and lost catalog binding.
    pub fn home_acceptance_session(
        &self,
        acceptance_id: Uuid,
        configured_issuer: OrchestrationIssuerId,
        authenticated_principal: AuthenticatedPrincipal,
    ) -> Result<Option<HomeAcceptanceSessionRecord>, AgentSessionCatalogError> {
        self.ensure_live_binding()?;
        let record = get(
            self,
            acceptance_id,
            configured_issuer,
            Some(authenticated_principal),
        )
        .map_err(HomeAcceptanceSessionError::into_catalog_error)?;
        self.ensure_live_binding()?;
        Ok(record)
    }

    /// Enumerate unresolved receipts for one configured issuer in a stable,
    /// bounded acceptance-ID order for restart recovery.
    ///
    /// # Errors
    /// Rejects invalid issuer/page cursor, damaged schema or lost catalog binding.
    pub fn pending_home_acceptance_sessions(
        &self,
        configured_issuer: OrchestrationIssuerId,
        after: Option<Uuid>,
        limit: u16,
    ) -> Result<HomeAcceptanceSessionPendingPage, AgentSessionCatalogError> {
        self.ensure_live_binding()?;
        let page = pending(self, configured_issuer, after, limit)
            .map_err(HomeAcceptanceSessionError::into_catalog_error)?;
        self.ensure_live_binding()?;
        Ok(page)
    }
}

fn finish_home_mutation(
    catalog: &mut AgentSessionCatalog,
    acceptance_id: Uuid,
) -> Result<(), AgentSessionCatalogError> {
    catalog
        .ensure_live_binding()
        .and_then(|()| catalog.capture_sidecar_identities())
        .and_then(|()| catalog.ensure_live_binding())
        .map_err(|source| {
            HomeAcceptanceSessionError::ReadbackRequired {
                acceptance_id,
                source: Box::new(source),
            }
            .into_catalog_error()
        })
}

pub(super) fn ensure_session_unpinned(
    transaction: &Transaction<'_>,
    session_id: AgentSessionId,
) -> Result<(), AgentSessionCatalogError> {
    repository::ensure_unpinned(transaction, session_id)
}

impl HomeAcceptanceSessionError {
    fn into_catalog_error(self) -> AgentSessionCatalogError {
        AgentSessionCatalogError::HomeAcceptance(Box::new(self))
    }
}
