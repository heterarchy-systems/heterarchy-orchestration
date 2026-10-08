//! Owned local token bootstrap for application consumers.
//!
//! The Backend and OS-owner credential CLI use the same pepper/catalog policy.
//! Keeping that policy in the
//! auth crate prevents any caller from accidentally creating a second token
//! authority or bypassing the recovery check for a missing Keychain pepper.

use crate::TokenAuthorityPaths;
use crate::pepper_store::{MacOsMcpTokenPepperStore, McpTokenPepper};
use forge_domain::TimestampMillis;
use thiserror::Error;

use crate::{
    MCP_TOKEN_DIGEST_BYTES, McpTokenCatalog, McpTokenCatalogError, McpTokenClient,
    McpTokenClientError, McpTokenId, McpTokenIssueRequest, McpTokenIssued, McpTokenMetadata,
    McpTokenRevision, McpTokenRotation, McpTokenService, McpTokenWorker, McpTokenWorkerError,
};

const TOKEN_CATALOG_FILE_NAME: &str = "mcp-tokens.sqlite3";

/// Persistence bootstrap policy for the app-global token authority.
///
/// Initializing an empty authority creates the catalog and Keychain pepper,
/// but it does not issue a bearer token or grant Workspace access.  With no
/// active token, the HTTP transport remains reachable only to return an
/// authentication failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpTokenBootstrapMode {
    /// Require an already initialized catalog and its existing pepper.
    ExistingAuthority,
    /// Permit explicit creation of an empty catalog and pepper.
    InitializeAuthority,
}

impl McpTokenBootstrapMode {
    pub(crate) const fn permits_initialization(self) -> bool {
        matches!(self, Self::InitializeAuthority)
    }
}

/// Public owner of the app-global token worker.
///
/// The worker owns the `SQLite` catalog and the control service owns the
/// in-memory pepper.  Callers retain only a bounded cloneable client and must
/// call [`Self::shutdown`] when their component lifecycle ends.  `Drop` on
/// the underlying worker is a synchronous safety net for error paths.
#[must_use]
#[derive(Debug)]
pub struct McpTokenRuntime {
    client: McpTokenClient,
    worker: Option<McpTokenWorker<McpTokenCatalog>>,
}

impl McpTokenRuntime {
    /// Start the app-global token worker using the canonical bootstrap policy.
    ///
    /// A missing catalog is allowed only for explicit authority
    /// initialization.  A missing pepper with existing records is always a
    /// recovery error; the runtime never fabricates a replacement authority
    /// in that case.
    ///
    /// # Errors
    ///
    /// Returns an error when catalog, Keychain pepper, randomness, or worker
    /// bootstrap fails.
    pub fn start(
        paths: &TokenAuthorityPaths,
        mode: McpTokenBootstrapMode,
    ) -> Result<Self, McpTokenRuntimeError> {
        let catalog_path = paths.application_support().join(TOKEN_CATALOG_FILE_NAME);
        let catalog = open_token_catalog(&catalog_path, mode)?;
        let has_records = !catalog
            .list(1)
            .map_err(McpTokenRuntimeError::Catalog)?
            .is_empty();
        let store = MacOsMcpTokenPepperStore;
        let pepper = match store.load().map_err(McpTokenRuntimeError::Pepper)? {
            Some(pepper) => pepper,
            None if mode.permits_initialization() && !has_records => {
                let mut bytes = [0_u8; MCP_TOKEN_DIGEST_BYTES];
                getrandom::fill(&mut bytes).map_err(McpTokenRuntimeError::PepperRandomness)?;
                let candidate = McpTokenPepper::new(bytes);
                store
                    .store_if_absent(&candidate)
                    .map_err(McpTokenRuntimeError::Pepper)?;
                store
                    .load()
                    .map_err(McpTokenRuntimeError::Pepper)?
                    .ok_or(McpTokenRuntimeError::PepperWriteUnconfirmed)?
            }
            None => return Err(McpTokenRuntimeError::PepperRecoveryRequired),
        };
        let (client, worker) =
            McpTokenWorker::start(McpTokenService::new(pepper.into_bytes(), catalog))
                .map_err(McpTokenRuntimeError::Worker)?;
        Ok(Self {
            client,
            worker: Some(worker),
        })
    }

    /// Assemble an owner from an already-started worker.
    ///
    /// For composition owners that already hold a started worker and client
    /// (test harnesses, desktop composition); production binaries should use
    /// [`Self::start`] so the Keychain/catalog bootstrap policy stays
    /// centralized.
    pub fn from_parts(client: McpTokenClient, worker: McpTokenWorker<McpTokenCatalog>) -> Self {
        Self {
            client,
            worker: Some(worker),
        }
    }

    /// Return a bounded cloneable client for token issue/authentication and
    /// lifecycle operations.
    #[must_use]
    pub fn client(&self) -> McpTokenClient {
        self.client.clone()
    }

    /// Issue a token through the owned worker.
    ///
    /// # Errors
    ///
    /// Returns the token worker error when the bounded request cannot be
    /// admitted or completed.
    pub fn issue(
        &self,
        request: McpTokenIssueRequest,
    ) -> Result<McpTokenIssued, McpTokenClientError> {
        self.client.issue(request)
    }

    /// Rotate a token through the owned worker.
    ///
    /// # Errors
    ///
    /// Returns the token worker error when the exact token revision or request
    /// cannot be completed.
    pub fn rotate(
        &self,
        id: McpTokenId,
        expected_revision: McpTokenRevision,
        rotated_at: TimestampMillis,
    ) -> Result<McpTokenRotation, McpTokenClientError> {
        self.client.rotate(id, expected_revision, rotated_at)
    }

    /// Revoke a token through the owned worker.
    ///
    /// # Errors
    ///
    /// Returns the token worker error when the exact token revision or request
    /// cannot be completed.
    pub fn revoke(
        &self,
        id: McpTokenId,
        expected_revision: McpTokenRevision,
        disabled_at: TimestampMillis,
    ) -> Result<McpTokenMetadata, McpTokenClientError> {
        self.client.revoke(id, expected_revision, disabled_at)
    }

    /// List non-secret token metadata through the owned worker.
    ///
    /// # Errors
    ///
    /// Returns the token worker error when the bounded list request cannot be
    /// completed.
    pub fn list(&self, limit: usize) -> Result<Vec<McpTokenMetadata>, McpTokenClientError> {
        self.client.list(limit)
    }

    /// Stop admission, drain the bounded worker, and join its owner thread.
    ///
    /// # Errors
    ///
    /// Returns an error when the owned token worker fails during shutdown.
    pub fn shutdown(&mut self) -> Result<(), McpTokenWorkerError> {
        self.worker
            .as_mut()
            .ok_or(McpTokenWorkerError::AlreadyShutdown)?
            .shutdown()?;
        self.worker = None;
        Ok(())
    }

    /// Whether the token worker still accepts requests.
    pub fn is_accepting(&self) -> bool {
        self.worker
            .as_ref()
            .is_some_and(McpTokenWorker::is_accepting)
    }
}

impl Drop for McpTokenRuntime {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.as_mut() {
            let _ignored = worker.shutdown();
        }
        self.worker = None;
    }
}

fn open_token_catalog(
    path: &std::path::Path,
    mode: McpTokenBootstrapMode,
) -> Result<McpTokenCatalog, McpTokenRuntimeError> {
    if mode.permits_initialization() {
        let parent = path.parent().ok_or(McpTokenRuntimeError::NotConfigured)?;
        std::fs::create_dir_all(parent).map_err(McpTokenRuntimeError::CatalogDirectory)?;
    } else if !path.is_file() {
        return Err(McpTokenRuntimeError::NotConfigured);
    }
    // The existing catalog owner rejects symlink parents/files and hardlinks.
    McpTokenCatalog::open(path).map_err(McpTokenRuntimeError::Catalog)
}

/// Failure while bootstrapping the shared token worker.
#[derive(Debug, Error)]
pub enum McpTokenRuntimeError {
    #[error("token catalog directory could not be initialized")]
    CatalogDirectory(#[source] std::io::Error),
    #[error("MCP token connection is not configured")]
    NotConfigured,
    #[error("MCP token catalog failed")]
    Catalog(#[source] McpTokenCatalogError),
    #[error("MCP token pepper Keychain operation failed")]
    Pepper(#[source] crate::pepper_store::McpTokenPepperError),
    #[error("secure randomness for the MCP token pepper is unavailable")]
    PepperRandomness(#[source] getrandom::Error),
    #[error("MCP token pepper is missing while durable token records exist")]
    PepperRecoveryRequired,
    #[error("MCP token pepper write could not be confirmed")]
    PepperWriteUnconfirmed,
    #[error("MCP token worker lifecycle failed")]
    Worker(#[source] McpTokenWorkerError),
}

// Keep the module's ownership contract visible to compile-time consumers.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<McpTokenRuntime>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_bootstrap_creates_missing_parent_but_existing_mode_does_not()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("token-first-install-{}", uuid::Uuid::now_v7()));
        let path = root
            .join("application-support")
            .join(TOKEN_CATALOG_FILE_NAME);
        assert!(matches!(
            open_token_catalog(&path, McpTokenBootstrapMode::ExistingAuthority),
            Err(McpTokenRuntimeError::NotConfigured)
        ));
        assert!(!root.exists());
        let catalog = open_token_catalog(&path, McpTokenBootstrapMode::InitializeAuthority)?;
        assert!(catalog.list(1)?.is_empty());
        drop(catalog);
        let existing = open_token_catalog(&path, McpTokenBootstrapMode::ExistingAuthority)?;
        assert!(existing.list(1)?.is_empty());
        drop(existing);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn runtime_error_does_not_expose_token_material() {
        let error = McpTokenRuntimeError::PepperRecoveryRequired;
        assert_eq!(
            error.to_string(),
            "MCP token pepper is missing while durable token records exist"
        );
    }

    #[test]
    fn worker_shutdown_error_remains_typed() {
        assert_eq!(
            McpTokenWorkerError::AlreadyShutdown.to_string(),
            "MCP token worker is already shut down"
        );
    }

    #[test]
    fn bootstrap_mode_names_empty_authority_initialization_explicitly() {
        assert!(!McpTokenBootstrapMode::ExistingAuthority.permits_initialization());
        assert!(McpTokenBootstrapMode::InitializeAuthority.permits_initialization());
    }
}
