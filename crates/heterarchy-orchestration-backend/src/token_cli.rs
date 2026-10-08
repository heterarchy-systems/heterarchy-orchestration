//! Explicit one-shot token bootstrap for the native backend operator.
//!
//! This command delegates to the existing app-global auth runtime. It reserves
//! a private output file before opening that authority and never prints a
//! bearer reveal. If the issue reply or secret write is uncertain, it reports
//! safe readback identity and never repeats the issue operation.

use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use forge_domain::TimestampMillis;
use heterarchy_orchestration_auth::{
    MAX_MCP_TOKEN_LIST_RESULTS, MCP_TOKEN_REVEAL_CHARS, McpTokenBootstrapMode, McpTokenClientError,
    McpTokenId, McpTokenIssueRequest, McpTokenIssued, McpTokenLabel, McpTokenMetadata,
    McpTokenRuntime, TokenAuthorityPaths,
};
use thiserror::Error;

/// Usage text for the explicit token bootstrap command.
pub const TOKEN_ISSUE_USAGE: &str = "usage: heterarchy-orchestration-backend token issue --initialize-authority --label <label> --output <new-private-file>";

/// Parsed backend command. Non-token arguments remain owned by the daemon CLI.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenCliCommand {
    /// The arguments belong to the ordinary backend daemon command.
    NotTokenCommand,
    /// The operator requested token-command help.
    Help,
    /// One explicit first-install token issue action.
    Issue(TokenIssueOptions),
}

/// Validated inputs for a one-shot token issue.
#[derive(Debug, PartialEq, Eq)]
pub struct TokenIssueOptions {
    label: McpTokenLabel,
    output_path: PathBuf,
}

impl TokenIssueOptions {
    /// The non-secret operator label attached to the token.
    #[must_use]
    pub fn label(&self) -> &str {
        self.label.as_str()
    }

    /// The new file that will receive the one-time bearer reveal.
    #[must_use]
    pub fn output_path(&self) -> &Path {
        &self.output_path
    }
}

/// Non-secret metadata returned after a successful token issue and file sync.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenIssueMetadata {
    token_id: McpTokenId,
    label: String,
}

impl TokenIssueMetadata {
    /// Exact token identity to use in operator-principal configuration.
    #[must_use]
    pub const fn token_id(&self) -> McpTokenId {
        self.token_id
    }

    /// The operator-provided non-secret token label.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }
}

/// Safe, redacted failures from parsing or running the bootstrap command.
#[derive(Debug, Error)]
pub enum TokenCliError {
    #[error("token command arguments are invalid")]
    InvalidArguments,
    #[error("token issue requires the explicit --initialize-authority flag")]
    InitializationNotExplicit,
    #[error("token issue requires a valid --label")]
    InvalidLabel,
    #[error("token issue requires a non-empty --output path")]
    MissingOutput,
    #[error("token output must be a new private file; an existing path is never overwritten")]
    OutputAlreadyExists,
    #[error("could not create the private token output file")]
    OutputCreate(#[source] io::Error),
    #[error("could not prepare and sync the private token output file")]
    OutputPrepare(#[source] io::Error),
    #[error("the app-global token authority could not be initialized")]
    AuthorityBootstrap,
    #[error("token metadata could not be read before issue; no token was issued")]
    ReadBeforeIssue,
    #[error(
        "token issue outcome is unknown; do not retry. Bounded metadata readback succeeded: {readback_succeeded}. Possible token IDs: {possible_token_ids:?}. An empty candidate list is not proof that no token was issued."
    )]
    IssueOutcomeUnknown {
        possible_token_ids: Vec<String>,
        readback_succeeded: bool,
    },
    #[error(
        "token {token_id} was issued, but secure output at {path:?} could not be confirmed; do not rerun token issue, read back this exact identity"
    )]
    SecretOutputUnconfirmed {
        token_id: McpTokenId,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("token {token_id} was issued and saved, but worker shutdown could not be confirmed")]
    WorkerShutdownAfterIssue { token_id: McpTokenId },
    #[error("system time is unavailable for token metadata")]
    ClockUnavailable,
}

/// Parse token subcommands while leaving ordinary backend flags to the daemon.
///
/// The issue action requires explicit acknowledgement that this command may
/// initialize the existing app-global authority. It accepts a path as an
/// `OsString` so valid non-UTF-8 filesystem paths remain usable on macOS.
///
/// # Errors
/// Returns a redacted argument error when the token subcommand is malformed or
/// omits the explicit authority-initialization acknowledgement.
pub fn parse(arguments: &[OsString]) -> Result<TokenCliCommand, TokenCliError> {
    let Some(command) = arguments.first() else {
        return Ok(TokenCliCommand::NotTokenCommand);
    };
    if command.to_str() != Some("token") {
        return Ok(TokenCliCommand::NotTokenCommand);
    }
    if arguments.len() == 2 && matches!(arguments[1].to_str(), Some("--help" | "-h" | "help")) {
        return Ok(TokenCliCommand::Help);
    }
    if arguments.get(1).and_then(|argument| argument.to_str()) != Some("issue") {
        return Err(TokenCliError::InvalidArguments);
    }
    if arguments.len() == 3 && matches!(arguments[2].to_str(), Some("--help" | "-h" | "help")) {
        return Ok(TokenCliCommand::Help);
    }

    let mut initialize_authority = false;
    let mut label = None;
    let mut output_path = None;
    let mut index = 2;
    while index < arguments.len() {
        match arguments[index].to_str() {
            Some("--initialize-authority") if !initialize_authority => {
                initialize_authority = true;
                index += 1;
            }
            Some("--label") if label.is_none() => {
                let value = arguments
                    .get(index + 1)
                    .and_then(|argument| argument.to_str())
                    .ok_or(TokenCliError::InvalidLabel)?;
                label = Some(
                    McpTokenLabel::new(value.to_owned())
                        .map_err(|_| TokenCliError::InvalidLabel)?,
                );
                index += 2;
            }
            Some("--output") if output_path.is_none() => {
                let value = arguments
                    .get(index + 1)
                    .ok_or(TokenCliError::MissingOutput)?;
                if value.is_empty() {
                    return Err(TokenCliError::MissingOutput);
                }
                output_path = Some(PathBuf::from(value));
                index += 2;
            }
            _ => return Err(TokenCliError::InvalidArguments),
        }
    }

    if !initialize_authority {
        return Err(TokenCliError::InitializationNotExplicit);
    }
    Ok(TokenCliCommand::Issue(TokenIssueOptions {
        label: label.ok_or(TokenCliError::InvalidLabel)?,
        output_path: output_path.ok_or(TokenCliError::MissingOutput)?,
    }))
}

/// Initialize the existing native token authority and issue exactly once.
///
/// The output path is created with mode `0600` and held open before app paths,
/// Keychain state, or the catalog are touched. The returned value contains
/// only non-secret metadata for the operator's principal policy.
///
/// # Errors
/// Returns a redacted bootstrap, durable readback, worker, or output-file error.
pub fn execute(options: &TokenIssueOptions) -> Result<TokenIssueMetadata, TokenCliError> {
    let mut output = PrivateTokenOutput::reserve(&options.output_path)?;
    let paths = TokenAuthorityPaths::discover().map_err(|_| TokenCliError::AuthorityBootstrap)?;
    let mut runtime = McpTokenRuntime::start(&paths, McpTokenBootstrapMode::InitializeAuthority)
        .map_err(|_| TokenCliError::AuthorityBootstrap)?;
    let result = issue_reserved(&mut runtime, &mut output, &options.label);
    match (result, runtime.shutdown()) {
        (Ok(metadata), Ok(())) => Ok(metadata),
        (Ok(metadata), Err(_)) => Err(TokenCliError::WorkerShutdownAfterIssue {
            token_id: metadata.token_id,
        }),
        (Err(error), _) => Err(error),
    }
}

trait TokenCliAuthority {
    fn list(&mut self, limit: usize) -> Result<Vec<McpTokenMetadata>, TokenAuthorityFailure>;
    fn issue(
        &mut self,
        request: McpTokenIssueRequest,
    ) -> Result<McpTokenIssued, TokenAuthorityFailure>;
}

#[derive(Debug)]
struct TokenAuthorityFailure;

impl TokenCliAuthority for McpTokenRuntime {
    fn list(&mut self, limit: usize) -> Result<Vec<McpTokenMetadata>, TokenAuthorityFailure> {
        McpTokenRuntime::list(self, limit).map_err(|_| TokenAuthorityFailure)
    }

    fn issue(
        &mut self,
        request: McpTokenIssueRequest,
    ) -> Result<McpTokenIssued, TokenAuthorityFailure> {
        McpTokenRuntime::issue(self, request).map_err(|error: McpTokenClientError| match error {
            McpTokenClientError::ResponseDropped
            | McpTokenClientError::Service(_)
            | McpTokenClientError::WorkerShuttingDown
            | McpTokenClientError::QueueFull
            | McpTokenClientError::WorkerClosed => TokenAuthorityFailure,
        })
    }
}

#[cfg(test)]
fn issue_with_authority<A: TokenCliAuthority>(
    options: &TokenIssueOptions,
    authority: &mut A,
) -> Result<TokenIssueMetadata, TokenCliError> {
    let mut output = PrivateTokenOutput::reserve(&options.output_path)?;
    issue_reserved(authority, &mut output, &options.label)
}

fn issue_reserved<A: TokenCliAuthority, O: SecretOutput>(
    authority: &mut A,
    output: &mut O,
    label: &McpTokenLabel,
) -> Result<TokenIssueMetadata, TokenCliError> {
    let before = authority
        .list(MAX_MCP_TOKEN_LIST_RESULTS)
        .map_err(|_| TokenCliError::ReadBeforeIssue)?;
    let existing_ids = before
        .into_iter()
        .map(|metadata| metadata.id())
        .collect::<HashSet<_>>();
    let created_at = now_millis()?;
    let request = McpTokenIssueRequest::new(label.clone(), created_at);
    let Ok(issued) = authority.issue(request) else {
        let (possible_token_ids, readback_succeeded) =
            match authority.list(MAX_MCP_TOKEN_LIST_RESULTS) {
                Ok(records) => (
                    records
                        .into_iter()
                        .filter(|metadata| {
                            !existing_ids.contains(&metadata.id())
                                && metadata.label() == label
                                && metadata.created_at() == created_at
                        })
                        .map(|metadata| metadata.id().to_string())
                        .collect(),
                    true,
                ),
                Err(TokenAuthorityFailure) => (Vec::new(), false),
            };
        return Err(TokenCliError::IssueOutcomeUnknown {
            possible_token_ids,
            readback_succeeded,
        });
    };

    let (metadata, reveal) = issued.into_parts();
    let token_id = metadata.id();
    output.write_reveal(reveal.as_str()).map_err(|source| {
        TokenCliError::SecretOutputUnconfirmed {
            token_id,
            path: output.path().to_path_buf(),
            source,
        }
    })?;
    Ok(TokenIssueMetadata {
        token_id,
        label: metadata.label().as_str().to_owned(),
    })
}

fn now_millis() -> Result<TimestampMillis, TokenCliError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TokenCliError::ClockUnavailable)?;
    let value = i64::try_from(elapsed.as_millis()).map_err(|_| TokenCliError::ClockUnavailable)?;
    TimestampMillis::new(value).map_err(|_| TokenCliError::ClockUnavailable)
}

struct PrivateTokenOutput {
    file: File,
    path: PathBuf,
}

trait SecretOutput {
    fn path(&self) -> &Path;
    fn write_reveal(&mut self, reveal: &str) -> io::Result<()>;
}

impl PrivateTokenOutput {
    #[cfg(unix)]
    fn reserve(path: &Path) -> Result<Self, TokenCliError> {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let mut options = OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        let mut file = options.open(path).map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                TokenCliError::OutputAlreadyExists
            } else {
                TokenCliError::OutputCreate(error)
            }
        })?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(TokenCliError::OutputPrepare)?;
        let permissions = file
            .metadata()
            .map_err(TokenCliError::OutputPrepare)?
            .permissions()
            .mode()
            & 0o777;
        if permissions != 0o600 {
            return Err(TokenCliError::OutputPrepare(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private file mode could not be confirmed",
            )));
        }

        // Reserve the complete bounded output before any token mutation. The
        // zero-filled placeholder contains no secret and is replaced in place.
        file.write_all(&[0_u8; MCP_TOKEN_REVEAL_CHARS + 1])
            .and_then(|()| file.sync_all())
            .and_then(|()| file.seek(SeekFrom::Start(0)).map(|_| ()))
            .map_err(TokenCliError::OutputPrepare)?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(TokenCliError::OutputPrepare)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    #[cfg(not(unix))]
    fn reserve(_path: &Path) -> Result<Self, TokenCliError> {
        Err(TokenCliError::OutputPrepare(io::Error::new(
            io::ErrorKind::Unsupported,
            "private token files require Unix mode support",
        )))
    }
}

impl SecretOutput for PrivateTokenOutput {
    fn path(&self) -> &Path {
        &self.path
    }

    fn write_reveal(&mut self, reveal: &str) -> io::Result<()> {
        let result = self
            .file
            .seek(SeekFrom::Start(0))
            .and_then(|_| self.file.write_all(reveal.as_bytes()))
            .and_then(|()| self.file.write_all(b"\n"))
            .and_then(|()| self.file.set_len((MCP_TOKEN_REVEAL_CHARS + 1) as u64))
            .and_then(|()| self.file.sync_all());
        if result.is_err() {
            // Best effort: keep a failed output path free of partial reveal
            // bytes. If storage itself is failing, its mode remains 0600.
            let _ = self.file.set_len(0);
            let _ = self.file.sync_all();
        }
        result
    }
}

#[cfg(test)]
#[path = "token_cli/tests.rs"]
mod tests;
