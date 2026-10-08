use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Read;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
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
    let secret = read_secret(&options.secret_path)?;
    if AgentSessionCatalog::open_existing_read_only(&options.catalog_path)?.is_none() {
        return Err(SessionAuthorityProcessError::CatalogMissing);
    }
    let authority = Arc::new(SessionCatalogAuthority::new(options.catalog_path));
    let config =
        InternalSessionServerConfig::new(SocketAddr::from(([127, 0, 0, 1], options.port)), secret)
            .map_err(|_| SessionAuthorityProcessError::InvalidConfiguration)?;
    Ok((authority, config))
}

fn read_secret(path: &Path) -> Result<String, SessionAuthorityProcessError> {
    if !path.is_absolute()
        || !fs::symlink_metadata(path)
            .map_err(SessionAuthorityProcessError::SecretIo)?
            .is_file()
        || path
            .canonicalize()
            .map_err(SessionAuthorityProcessError::SecretIo)?
            != path
    {
        return Err(SessionAuthorityProcessError::InsecureSecret);
    }
    let file = File::open(path).map_err(SessionAuthorityProcessError::SecretIo)?;
    let metadata = file
        .metadata()
        .map_err(SessionAuthorityProcessError::SecretIo)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(SessionAuthorityProcessError::InsecureSecret);
    }
    let mut bytes = Vec::with_capacity(MAX_SECRET_BYTES + 1);
    file.take((MAX_SECRET_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(SessionAuthorityProcessError::SecretIo)?;
    if bytes.len() > MAX_SECRET_BYTES {
        return Err(SessionAuthorityProcessError::InvalidSecret);
    }
    let text = std::str::from_utf8(&bytes).map_err(SessionAuthorityProcessError::SecretUtf8)?;
    let text = match text.strip_suffix('\n') {
        Some(text) => text.strip_suffix('\r').unwrap_or(text),
        None => text,
    };
    if text.is_empty() || text.contains('\n') || text.contains('\r') {
        return Err(SessionAuthorityProcessError::InvalidSecret);
    }
    Ok(text.to_owned())
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
    signal
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
