//! Runtime-owned wrappers for the bounded Forge operations import.
//!
//! Source observations are fetched by the transport owner before these
//! methods enter the synchronous catalog. The runtime never performs source
//! I/O while holding its catalog lock.

use heterarchy_orchestration_operations::{
    AgentOperationsExportPage, AgentOperationsFreeze, ImportProgress, ImportVerificationTarget,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

impl OrchestrationRuntime {
    /// Prepare or replay one import from an exact frozen Forge observation.
    ///
    /// # Errors
    /// Returns source-identity conflicts, catalog ownership failures, or
    /// uncertain durable commits.
    pub fn import_prepare(
        &self,
        source: &AgentOperationsFreeze,
    ) -> Result<ImportProgress, RuntimeError> {
        let mut catalog = self.catalog()?;
        catalog.prepare(source).map_err(Into::into)
    }

    /// Apply one already observed bounded Forge export page.
    ///
    /// # Errors
    /// Returns page-order, replay, digest, catalog, or uncertain-commit
    /// failures.
    pub fn import_page(
        &self,
        page: &AgentOperationsExportPage,
    ) -> Result<ImportProgress, RuntimeError> {
        let mut catalog = self.catalog()?;
        catalog.import_page(page).map_err(Into::into)
    }

    /// Apply one page only when the caller's full progress snapshot is fresh.
    ///
    /// # Errors
    /// Returns stale-fence, page-order, digest, catalog, or uncertain-commit
    /// failures.
    pub fn import_page_expected(
        &self,
        page: &AgentOperationsExportPage,
        expected: &ImportProgress,
    ) -> Result<ImportProgress, RuntimeError> {
        let mut catalog = self.catalog()?;
        catalog
            .import_page_expected(page, expected)
            .map_err(Into::into)
    }

    /// Verify one already observed bounded Forge export page.
    ///
    /// # Errors
    /// Returns source/destination digest, cursor, count, catalog, or
    /// uncertain-commit failures.
    pub fn import_verify_page(
        &self,
        page: &AgentOperationsExportPage,
    ) -> Result<ImportProgress, RuntimeError> {
        let mut catalog = self.catalog()?;
        catalog.verify_page(page).map_err(Into::into)
    }

    /// Verify one page only when the caller's full progress snapshot is fresh.
    ///
    /// # Errors
    /// Returns stale-fence, source/destination digest, cursor, catalog, or
    /// uncertain-commit failures.
    pub fn import_verify_page_expected(
        &self,
        page: &AgentOperationsExportPage,
        expected: &ImportProgress,
    ) -> Result<ImportProgress, RuntimeError> {
        let mut catalog = self.catalog()?;
        catalog
            .verify_page_expected(page, expected)
            .map_err(Into::into)
    }

    /// Read durable progress for one import identity.
    ///
    /// # Errors
    /// Returns malformed progress, catalog ownership, or storage failures.
    pub fn import_progress(&self, migration: Uuid) -> Result<Option<ImportProgress>, RuntimeError> {
        let catalog = self.catalog()?;
        catalog.progress(migration).map_err(Into::into)
    }

    /// Read the exact persisted cursor needed for the next verification page.
    ///
    /// # Errors
    /// Returns incomplete/corrupt verification progress or catalog ownership
    /// failures.
    pub fn import_verification_target(
        &self,
        migration: Uuid,
    ) -> Result<ImportVerificationTarget, RuntimeError> {
        let catalog = self.catalog()?;
        catalog.verification_target(migration).map_err(Into::into)
    }

    /// Abort one pre-membership import and read back its terminal progress.
    ///
    /// # Errors
    /// Returns membership-handoff conflicts, catalog ownership, or uncertain
    /// durable commit failures.
    pub fn import_abort(&self, migration: Uuid) -> Result<ImportProgress, RuntimeError> {
        let mut catalog = self.catalog()?;
        catalog.abort_import(migration).map_err(Into::into)
    }

    /// Abort one import only when the caller's full progress snapshot is fresh.
    ///
    /// # Errors
    /// Returns stale-fence, membership-handoff, catalog, or uncertain-commit
    /// failures.
    pub fn import_abort_expected(
        &self,
        migration: Uuid,
        expected: &ImportProgress,
    ) -> Result<ImportProgress, RuntimeError> {
        let mut catalog = self.catalog()?;
        catalog
            .abort_import_expected(migration, expected)
            .map_err(Into::into)
    }
}
