use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Read;
use std::net::SocketAddr;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use heterarchy_orchestration_sessions::{
    AgentSessionCatalog, AgentSessionCatalogError, SessionCatalogAuthority,
};
use thiserror::Error;

use crate::internal_session::{
    InternalSessionServerConfig, InternalSessionServerHandle, InternalSessionServerJoinState,
};

const MAX_SECRET_BYTES: usize = 4 * 1024;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const SHUTDOWN_RETRIES: usize = 2;

/// Usage for the independently runnable, read-only Session Authority host.
pub const SESSION_AUTHORITY_USAGE: &str = "usage: heterarchy-orchestration-backend session-authority --port <port> --catalog <absolute-existing-catalog> --issuer-secret-file <absolute-private-file>";

/// Explicit host inputs; the issuer secret remains in a private file.
#[derive(Debug)]
pub struct SessionAuthorityOptions {
    port: u16,
    catalog_path: PathBuf,
    secret_path: PathBuf,
}

/// Parse only the Session Authority command; other commands retain their owner.
///
/// # Errors
/// Rejects missing/duplicate/unknown arguments, invalid ports and relative paths.
pub fn parse(
    arguments: &[OsString],
) -> Result<Option<SessionAuthorityOptions>, SessionAuthorityProcessError> {
    if arguments.first().and_then(|arg| arg.to_str()) != Some("session-authority") {
        return Ok(None);
    }
    let mut port = None;
    let mut catalog_path = None;
    let mut secret_path = None;
    let (fields, remainder) = arguments[1..].as_chunks::<2>();
    for pair in fields {
        match pair[0].to_str() {
            Some("--port") if port.is_none() => {
                let text = pair[1]
                    .to_str()
                    .ok_or(SessionAuthorityProcessError::InvalidArguments)?;
                let value: u16 = text
                    .parse()
                    .map_err(|_| SessionAuthorityProcessError::InvalidArguments)?;
                if value.to_string() != text {
                    return Err(SessionAuthorityProcessError::InvalidArguments);
                }
                port = Some(value);
            }
            Some("--catalog") if catalog_path.is_none() => {
                catalog_path = Some(absolute_path(&pair[1])?);
            }
            Some("--issuer-secret-file") if secret_path.is_none() => {
                secret_path = Some(absolute_path(&pair[1])?);
            }
            _ => return Err(SessionAuthorityProcessError::InvalidArguments),
        }
    }
    if !remainder.is_empty() {
        return Err(SessionAuthorityProcessError::InvalidArguments);
    }
    Ok(Some(SessionAuthorityOptions {
        port: port.ok_or(SessionAuthorityProcessError::InvalidArguments)?,
        catalog_path: catalog_path.ok_or(SessionAuthorityProcessError::InvalidArguments)?,
        secret_path: secret_path.ok_or(SessionAuthorityProcessError::InvalidArguments)?,
    }))
}

fn absolute_path(value: &OsString) -> Result<PathBuf, SessionAuthorityProcessError> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(SessionAuthorityProcessError::InvalidArguments);
    }
    Ok(path)
}

/// Validated configuration for an in-process loopback Session Authority listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedSessionAuthorityConfig {
    listen: SocketAddr,
    catalog_path: PathBuf,
    secret_path: PathBuf,
}

impl EmbeddedSessionAuthorityConfig {
    /// Validate loopback bind address and explicit absolute catalog and secret paths.
    ///
    /// # Errors
    /// Rejects non-loopback `listen` with [`SessionAuthorityProcessError::InvalidConfiguration`]
    /// and non-absolute paths with [`SessionAuthorityProcessError::InvalidArguments`].
    pub fn new(
        listen: SocketAddr,
        catalog_path: PathBuf,
        secret_path: PathBuf,
    ) -> Result<Self, SessionAuthorityProcessError> {
        if !listen.ip().is_loopback() {
            return Err(SessionAuthorityProcessError::InvalidConfiguration);
        }
        if !catalog_path.is_absolute() || !secret_path.is_absolute() {
            return Err(SessionAuthorityProcessError::InvalidArguments);
        }
        Ok(Self {
            listen,
            catalog_path,
            secret_path,
        })
    }

    #[must_use]
    pub const fn listen(&self) -> SocketAddr {
        self.listen
    }

    #[must_use]
    pub fn catalog_path(&self) -> &Path {
        &self.catalog_path
    }

    #[must_use]
    pub fn secret_path(&self) -> &Path {
        &self.secret_path
    }
}

/// Owned in-process Session Authority listener supervised by the Backend host.
#[derive(Debug)]
pub struct EmbeddedSessionAuthorityHandle {
    server: InternalSessionServerHandle,
}

impl EmbeddedSessionAuthorityHandle {
    /// Validate the canonical private secret file and existing read-only `AgentSessionCatalog`,
    /// then start the loopback Session Authority listener.
    ///
    /// # Errors
    /// Returns typed secret, catalog, configuration, join, or listener bind failures.
    pub async fn start(
        config: EmbeddedSessionAuthorityConfig,
    ) -> Result<Self, SessionAuthorityProcessError> {
        let (authority, server_config) =
            tokio::task::spawn_blocking(move || prepare_embedded(config))
                .await
                .map_err(SessionAuthorityProcessError::PreparationJoin)??;
        let server = InternalSessionServerHandle::start(authority, server_config)
            .await
            .map_err(|_| SessionAuthorityProcessError::ListenerStart)?;
        Ok(Self { server })
    }

    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.server.address()
    }

    /// Observe unexpected termination of the supervised loopback listener.
    pub async fn wait_for_unexpected_exit(&mut self) -> SessionAuthorityProcessError {
        let _state = self.server.wait_for_listener().await;
        SessionAuthorityProcessError::ListenerExited
    }

    /// Stop admission and drain in-flight session resolution queries within the bounded budget.
    ///
    /// # Errors
    /// Returns [`SessionAuthorityProcessError::ShutdownIncomplete`] if queries or the listener
    /// remain pending after retries, or [`SessionAuthorityProcessError::ListenerExited`] if the
    /// listener task failed.
    pub async fn shutdown(self) -> Result<(), SessionAuthorityProcessError> {
        shutdown_server(self.server).await
    }
}

/// Run the existing loopback Session Authority under the Backend's Tokio host.
/// This reads an existing logical-session catalog; it grants no work admission.
///
/// # Errors
/// Returns typed bootstrap, secret, listener, signal, and shutdown failures.
pub async fn run_session_authority_process(
    options: SessionAuthorityOptions,
) -> Result<(), SessionAuthorityProcessError> {
    let (authority, config) = tokio::task::spawn_blocking(move || prepare(options))
        .await
        .map_err(SessionAuthorityProcessError::PreparationJoin)??;
    run_server(authority, config).await
}

fn prepare(
    options: SessionAuthorityOptions,
) -> Result<(Arc<SessionCatalogAuthority>, InternalSessionServerConfig), SessionAuthorityProcessError>
{
    prepare_embedded(EmbeddedSessionAuthorityConfig {
        listen: SocketAddr::from(([127, 0, 0, 1], options.port)),
        catalog_path: options.catalog_path,
        secret_path: options.secret_path,
    })
}

fn prepare_embedded(
    config: EmbeddedSessionAuthorityConfig,
) -> Result<(Arc<SessionCatalogAuthority>, InternalSessionServerConfig), SessionAuthorityProcessError>
{
    let secret = read_secret(&config.secret_path)?;
    if AgentSessionCatalog::open_existing_read_only(&config.catalog_path)?.is_none() {
        return Err(SessionAuthorityProcessError::CatalogMissing);
    }
    let authority = Arc::new(SessionCatalogAuthority::new(config.catalog_path));
    let server_config = InternalSessionServerConfig::new(config.listen, secret)
        .map_err(|_| SessionAuthorityProcessError::InvalidConfiguration)?;
    Ok((authority, server_config))
}

fn read_secret(path: &Path) -> Result<String, SessionAuthorityProcessError> {
    if !path.is_absolute() {
        return Err(SessionAuthorityProcessError::InsecureSecret);
    }
    let path_meta =
        fs::symlink_metadata(path).map_err(|_| SessionAuthorityProcessError::InsecureSecret)?;
    if !path_meta.file_type().is_file()
        || path
            .canonicalize()
            .map_err(|_| SessionAuthorityProcessError::InsecureSecret)?
            != path
    {
        return Err(SessionAuthorityProcessError::InsecureSecret);
    }
    let file = File::open(path).map_err(|_| SessionAuthorityProcessError::InsecureSecret)?;
    let fd_meta = file
        .metadata()
        .map_err(|_| SessionAuthorityProcessError::InsecureSecret)?;
    if (fd_meta.dev(), fd_meta.ino()) != (path_meta.dev(), path_meta.ino())
        || !fd_meta.file_type().is_file()
        || fd_meta.nlink() != 1
        || (fd_meta.permissions().mode() & 0o7777) != 0o600
    {
        return Err(SessionAuthorityProcessError::InsecureSecret);
    }
    let mut bytes = Vec::with_capacity(MAX_SECRET_BYTES + 1);
    file.take((MAX_SECRET_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(SessionAuthorityProcessError::SecretIo)?;
    if bytes.len() > MAX_SECRET_BYTES {
        return Err(SessionAuthorityProcessError::InvalidSecret);
    }
    let raw = std::str::from_utf8(&bytes).map_err(SessionAuthorityProcessError::SecretUtf8)?;
    let trimmed = raw.trim_end_matches(['\r', '\n']);
    validate_secret(trimmed)
}

fn validate_secret(secret: &str) -> Result<String, SessionAuthorityProcessError> {
    if secret.is_empty() || secret.contains('\n') || secret.contains('\r') {
        return Err(SessionAuthorityProcessError::InvalidSecret);
    }
    Ok(secret.to_owned())
}

async fn run_server(
    authority: Arc<SessionCatalogAuthority>,
    config: InternalSessionServerConfig,
) -> Result<(), SessionAuthorityProcessError> {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(SessionAuthorityProcessError::Signal)?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(SessionAuthorityProcessError::Signal)?;
    let mut server = InternalSessionServerHandle::start(authority, config)
        .await
        .map_err(|_| SessionAuthorityProcessError::ListenerStart)?;
    println!("session_authority_endpoint=http://{}", server.address());

    let signal = tokio::select! {
        received = interrupt.recv() => received.ok_or_else(|| SessionAuthorityProcessError::Signal(std::io::Error::other("signal receiver closed"))),
        received = terminate.recv() => received.ok_or_else(|| SessionAuthorityProcessError::Signal(std::io::Error::other("signal receiver closed"))),
        _ = server.wait_for_listener() => Err(SessionAuthorityProcessError::ListenerExited),
    };
    shutdown_server(server).await?;
    signal
}

async fn shutdown_server(
    mut server: InternalSessionServerHandle,
) -> Result<(), SessionAuthorityProcessError> {
    let mut report = server.shutdown(SHUTDOWN_TIMEOUT).await;
    if !report.is_complete() {
        for _ in 0..SHUTDOWN_RETRIES {
            report = server.shutdown(RETRY_TIMEOUT).await;
            if report.is_complete() {
                break;
            }
        }
    }

    if !report.is_complete() {
        return Err(SessionAuthorityProcessError::ShutdownIncomplete {
            pending_queries: report.pending_queries(),
            server: report.server(),
            pending_handle: Box::new(server),
        });
    }
    if !report.is_successful() {
        return Err(SessionAuthorityProcessError::ListenerExited);
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum SessionAuthorityProcessError {
    #[error("session authority command arguments are invalid")]
    InvalidArguments,
    #[error("session authority catalog is missing")]
    CatalogMissing,
    #[error("session authority catalog failed")]
    Catalog(#[from] AgentSessionCatalogError),
    #[error("session authority secret file could not be read")]
    SecretIo(#[source] std::io::Error),
    #[error("session authority secret must be a canonical private regular file")]
    InsecureSecret,
    #[error("session authority secret file is not UTF-8")]
    SecretUtf8(#[source] std::str::Utf8Error),
    #[error("session authority secret file is empty, oversized, or contains an interior newline")]
    InvalidSecret,
    #[error("session authority server configuration is invalid")]
    InvalidConfiguration,
    #[error("session authority bootstrap task failed to join")]
    PreparationJoin(#[source] tokio::task::JoinError),
    #[error("session authority listener failed to start")]
    ListenerStart,
    #[error("session authority listener terminated unexpectedly")]
    ListenerExited,
    #[error("session authority shutdown signal failed")]
    Signal(#[source] std::io::Error),
    #[error(
        "session authority shutdown incomplete: pending_queries={pending_queries}, server={server:?}"
    )]
    ShutdownIncomplete {
        pending_queries: usize,
        server: InternalSessionServerJoinState,
        /// Retained listener/query ownership for an explicit shutdown retry.
        pending_handle: Box<InternalSessionServerHandle>,
    },
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod embedded_supervision_tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let root = std::env::temp_dir()
                .join(format!("embedded-session-host-{}", uuid::Uuid::now_v7()));
            fs::create_dir(&root)?;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
            Ok(Self(root.canonicalize()?))
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn embedded_config_rejects_non_loopback_addresses_and_relative_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let non_loopback = EmbeddedSessionAuthorityConfig::new(
            SocketAddr::from(([192, 0, 2, 10], 9000)),
            PathBuf::from("/catalog.sqlite3"),
            PathBuf::from("/issuer-secret"),
        );
        assert!(matches!(
            non_loopback,
            Err(SessionAuthorityProcessError::InvalidConfiguration)
        ));

        let unspecified = EmbeddedSessionAuthorityConfig::new(
            SocketAddr::from(([0, 0, 0, 0], 9000)),
            PathBuf::from("/catalog.sqlite3"),
            PathBuf::from("/issuer-secret"),
        );
        assert!(matches!(
            unspecified,
            Err(SessionAuthorityProcessError::InvalidConfiguration)
        ));

        let relative_catalog = EmbeddedSessionAuthorityConfig::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            PathBuf::from("relative-catalog.sqlite3"),
            PathBuf::from("/issuer-secret"),
        );
        assert!(matches!(
            relative_catalog,
            Err(SessionAuthorityProcessError::InvalidArguments)
        ));

        let relative_secret = EmbeddedSessionAuthorityConfig::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            PathBuf::from("/catalog.sqlite3"),
            PathBuf::from("relative-secret"),
        );
        assert!(matches!(
            relative_secret,
            Err(SessionAuthorityProcessError::InvalidArguments)
        ));

        let valid = EmbeddedSessionAuthorityConfig::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            PathBuf::from("/catalog.sqlite3"),
            PathBuf::from("/issuer-secret"),
        )?;
        assert_eq!(valid.listen(), SocketAddr::from(([127, 0, 0, 1], 0)));
        assert_eq!(valid.catalog_path(), Path::new("/catalog.sqlite3"));
        assert_eq!(valid.secret_path(), Path::new("/issuer-secret"));
        Ok(())
    }

    #[tokio::test]
    async fn embedded_handle_rejects_insecure_secret_and_missing_catalog_then_shuts_down_cleanly()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TempDir::new()?;
        let secret_path = fixture.0.join("issuer-secret");
        fs::write(&secret_path, "embedded-test-issuer\n")?;
        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o644))?;

        let catalog_path = fixture.0.join("agent-sessions.sqlite3");
        let config = EmbeddedSessionAuthorityConfig::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            catalog_path.clone(),
            secret_path.clone(),
        )?;

        let insecure = EmbeddedSessionAuthorityHandle::start(config.clone()).await;
        assert!(matches!(
            insecure,
            Err(SessionAuthorityProcessError::InsecureSecret)
        ));

        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o600))?;
        let missing_catalog = EmbeddedSessionAuthorityHandle::start(config.clone()).await;
        assert!(matches!(
            missing_catalog,
            Err(SessionAuthorityProcessError::CatalogMissing)
        ));
        assert!(!catalog_path.exists());

        let initialized = AgentSessionCatalog::open(&catalog_path)?;
        drop(initialized);
        assert!(AgentSessionCatalog::open_existing_read_only(&catalog_path)?.is_some());

        let handle = EmbeddedSessionAuthorityHandle::start(config).await?;
        assert!(handle.address().ip().is_loopback());
        assert_ne!(handle.address().port(), 0);
        handle.shutdown().await?;
        Ok(())
    }

    #[test]
    fn read_secret_rejects_executable_0700_mode_and_hardlinks()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TempDir::new()?;
        let secret_path = fixture.0.join("issuer-secret");
        fs::write(&secret_path, "embedded-test-issuer\n")?;
        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o700))?;

        let executable = read_secret(&secret_path);
        assert!(matches!(
            executable,
            Err(SessionAuthorityProcessError::InsecureSecret)
        ));

        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o600))?;
        assert_eq!(read_secret(&secret_path)?, "embedded-test-issuer");

        let hard_link = fixture.0.join("issuer-secret-link");
        fs::hard_link(&secret_path, &hard_link)?;
        let linked = read_secret(&secret_path);
        assert!(matches!(
            linked,
            Err(SessionAuthorityProcessError::InsecureSecret)
        ));
        Ok(())
    }
}
