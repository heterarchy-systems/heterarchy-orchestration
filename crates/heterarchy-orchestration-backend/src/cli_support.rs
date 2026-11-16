//! Shared secret-safe argument parsing, bounded file I/O, and loopback client
//! bootstrap helpers for the `profile`, `swarm`, and `autonomy` CLI subcommands.
//!
//! Raw bearer tokens are never accepted on `argv`, never echoed in error
//! messages, and redacted from `Debug` representations.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File};
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use hyper::header::HeaderValue;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use uuid::Uuid;

use crate::client::{ClientError, OrchestrationHttpClient};
use crate::wire::autonomy_loop::{
    ExecutionAuthorWire, ExecutionPreferenceWire, PrincipalIdentityWire,
};

/// Environment variable supplying the path to a bearer token file when `--bearer-file` is omitted.
pub const BEARER_FILE_ENV: &str = "HETERARCHY_ORCHESTRATION_BEARER_FILE";
/// Environment variable supplying the loopback backend `ip:port` when `--addr` is omitted.
pub const BACKEND_ADDR_ENV: &str = "HETERARCHY_ORCHESTRATION_BACKEND_ADDR";
/// Environment variable supplying the active Workspace UUID for spawned workers when `--workspace-id` is omitted.
pub const WORKSPACE_ID_ENV: &str = "HETERARCHY_WORKSPACE_ID";
/// Environment variable supplying the active Swarm run UUID for spawned workers when `--swarm-run-id` is omitted.
pub const SWARM_RUN_ID_ENV: &str = "HETERARCHY_SWARM_RUN_ID";
/// Environment variable supplying the active Swarm member UUID for spawned workers when member flags are omitted.
pub const SWARM_MEMBER_ID_ENV: &str = "HETERARCHY_SWARM_MEMBER_ID";
/// Default loopback backend listener address.
pub const DEFAULT_BACKEND_ADDR: &str = "127.0.0.1:8798";
/// Hard upper bound (`4 KiB`) when reading a bearer token file.
pub const MAX_BEARER_FILE_BYTES: usize = 4 * 1024;
/// Bounded per-call HTTP exchange timeout for CLI commands (`5s`).
pub const CLI_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

/// Validated loopback endpoint and bearer-file location for one CLI invocation.
#[derive(Clone, PartialEq, Eq)]
pub struct CliConnectionOptions {
    addr: SocketAddr,
    bearer_file: PathBuf,
}

impl fmt::Debug for CliConnectionOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliConnectionOptions")
            .field("addr", &self.addr)
            .field("bearer_file", &"[REDACTED_PATH]")
            .finish()
    }
}

impl CliConnectionOptions {
    /// Construct validated loopback connection options.
    ///
    /// # Errors
    /// Rejects non-loopback or zero-port addresses and empty bearer-file paths.
    pub fn new(addr: SocketAddr, bearer_file: PathBuf) -> Result<Self, CliSupportError> {
        if !addr.ip().is_loopback() || addr.port() == 0 {
            return Err(CliSupportError::InvalidBackendAddress);
        }
        if bearer_file.as_os_str().is_empty() {
            return Err(CliSupportError::MissingBearerFile);
        }
        Ok(Self { addr, bearer_file })
    }

    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn bearer_file(&self) -> &Path {
        &self.bearer_file
    }

    pub(crate) fn open_client_and_bearer(
        &self,
    ) -> Result<(OrchestrationHttpClient, RedactedBearer), CliSupportError> {
        let bearer = read_bearer_file(&self.bearer_file)?;
        let client = OrchestrationHttpClient::new(self.addr, CLI_HTTP_TIMEOUT)
            .map_err(CliSupportError::Client)?;
        Ok((client, bearer))
    }
}

/// Payload source for one structured CLI subcommand (`--request <json-file>` vs inline flags).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliPayload<T> {
    /// Load strict JSON payload from a bounded regular non-symlink file.
    RequestFile(PathBuf),
    /// Construct payload directly from parsed CLI flags.
    Inline(T),
}

impl<T: DeserializeOwned + Clone> CliPayload<T> {
    pub(crate) fn resolve(&self, max_bytes: usize) -> Result<T, CliSupportError> {
        match self {
            Self::RequestFile(path) => read_bounded_json_file(path, max_bytes),
            Self::Inline(value) => Ok(value.clone()),
        }
    }
}

/// Sensitive `Authorization: Bearer ...` header value with redacted `Debug` output.
pub(crate) struct RedactedBearer(HeaderValue);

impl fmt::Debug for RedactedBearer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RedactedBearer([REDACTED])")
    }
}

impl RedactedBearer {
    pub(crate) const fn header_value(&self) -> &HeaderValue {
        &self.0
    }
}

/// Redacted CLI parsing, file hygiene, and transport failures.
#[derive(Debug, Error)]
pub enum CliSupportError {
    #[error("CLI arguments are invalid or incomplete")]
    InvalidArguments,
    #[error(
        "raw bearer tokens are forbidden on the command line; supply --bearer-file <path> or HETERARCHY_ORCHESTRATION_BEARER_FILE"
    )]
    RawBearerFlagForbidden,
    #[error("a non-empty --bearer-file <path> or HETERARCHY_ORCHESTRATION_BEARER_FILE is required")]
    MissingBearerFile,
    #[error("backend address must be a valid non-zero loopback ip:port")]
    InvalidBackendAddress,
    #[error("bearer file must be an existing regular non-symlink file")]
    InsecureBearerFile,
    #[error("could not read the bearer token file")]
    BearerFileIo,
    #[error("bearer token file exceeds the 4 KiB secret hygiene bound")]
    BearerFileOversized,
    #[error("bearer token file is empty, non-UTF-8, or contains invalid secret characters")]
    InvalidBearerSecret,
    #[error("request JSON file must be an existing regular non-symlink file within size bounds")]
    InvalidRequestFile,
    #[error("request JSON payload failed strict schema validation")]
    InvalidRequestJson,
    #[error("failed to serialize CLI JSON response")]
    ResponseSerialize,
    #[error("Orchestration HTTP request failed: {0}")]
    Client(#[from] ClientError),
}

#[derive(Default)]
pub(crate) struct RawConnectionFlags {
    pub(crate) addr: Option<String>,
    pub(crate) bearer_file: Option<OsString>,
}

impl RawConnectionFlags {
    pub(crate) fn try_consume(
        &mut self,
        arguments: &[OsString],
        index: &mut usize,
    ) -> Result<bool, CliSupportError> {
        let current = &arguments[*index];
        if is_forbidden_raw_secret_flag(current) {
            return Err(CliSupportError::RawBearerFlagForbidden);
        }
        match current.to_str() {
            Some("--addr") => {
                if self.addr.is_some() {
                    return Err(CliSupportError::InvalidArguments);
                }
                let val = arguments
                    .get(*index + 1)
                    .and_then(|arg| arg.to_str())
                    .ok_or(CliSupportError::InvalidBackendAddress)?;
                self.addr = Some(val.to_owned());
                *index += 2;
                Ok(true)
            }
            Some("--bearer-file") => {
                if self.bearer_file.is_some() {
                    return Err(CliSupportError::InvalidArguments);
                }
                let val = arguments
                    .get(*index + 1)
                    .ok_or(CliSupportError::MissingBearerFile)?;
                if val.is_empty() {
                    return Err(CliSupportError::MissingBearerFile);
                }
                self.bearer_file = Some(val.clone());
                *index += 2;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(crate) fn resolve(
        self,
        env_addr: Option<&OsStr>,
        env_bearer_file: Option<&OsStr>,
    ) -> Result<CliConnectionOptions, CliSupportError> {
        let addr_str = match self.addr {
            Some(explicit) => explicit,
            None => match env_addr {
                Some(raw) => raw
                    .to_str()
                    .ok_or(CliSupportError::InvalidBackendAddress)?
                    .to_owned(),
                None => DEFAULT_BACKEND_ADDR.to_owned(),
            },
        };
        let addr = addr_str
            .parse::<SocketAddr>()
            .map_err(|_| CliSupportError::InvalidBackendAddress)?;
        let bearer_os = self
            .bearer_file
            .or_else(|| env_bearer_file.map(OsStr::to_os_string))
            .ok_or(CliSupportError::MissingBearerFile)?;
        if bearer_os.is_empty() {
            return Err(CliSupportError::MissingBearerFile);
        }
        CliConnectionOptions::new(addr, PathBuf::from(bearer_os))
    }

    pub(crate) fn resolve_from_env<F>(
        self,
        env_lookup: &F,
    ) -> Result<CliConnectionOptions, CliSupportError>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let env_addr = env_lookup(BACKEND_ADDR_ENV);
        let env_bearer = env_lookup(BEARER_FILE_ENV);
        self.resolve(env_addr.as_deref(), env_bearer.as_deref())
    }
}

/// Shared envelope classification for `<family> <subcommand> [flags...]` CLI entrypoints.
pub(crate) enum ParsedSubcommandHead<'a> {
    NotThisCommand,
    Help,
    Subcommand {
        name: &'a str,
        next_index: usize,
        conn: RawConnectionFlags,
    },
}

pub(crate) fn parse_subcommand_head<'a>(
    expected_family: &str,
    arguments: &'a [OsString],
) -> Result<ParsedSubcommandHead<'a>, CliSupportError> {
    if arguments.first().and_then(|arg| arg.to_str()) != Some(expected_family) {
        return Ok(ParsedSubcommandHead::NotThisCommand);
    }
    if arguments.len() == 1 || (arguments.len() == 2 && is_help_flag(&arguments[1])) {
        return Ok(ParsedSubcommandHead::Help);
    }
    let mut conn = RawConnectionFlags::default();
    let mut index = 1;
    while index < arguments.len() && conn.try_consume(arguments, &mut index)? {}
    let Some(sub_os) = arguments.get(index) else {
        return Err(CliSupportError::InvalidArguments);
    };
    if is_help_flag(sub_os) && index + 1 == arguments.len() {
        return Ok(ParsedSubcommandHead::Help);
    }
    let name = sub_os.to_str().ok_or(CliSupportError::InvalidArguments)?;
    index += 1;
    if arguments.get(index).is_some_and(|arg| is_help_flag(arg)) && index + 1 == arguments.len() {
        return Ok(ParsedSubcommandHead::Help);
    }
    Ok(ParsedSubcommandHead::Subcommand {
        name,
        next_index: index,
        conn,
    })
}

pub(crate) fn env_canonical_uuid<F>(
    env_lookup: &F,
    key: &str,
) -> Result<Option<Uuid>, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let Some(raw_os) = env_lookup(key) else {
        return Ok(None);
    };
    if raw_os.is_empty() {
        return Ok(None);
    }
    let raw = raw_os.to_str().ok_or(CliSupportError::InvalidArguments)?;
    parse_canonical_uuid(raw).map(Some)
}

pub(crate) fn is_help_flag(arg: &OsStr) -> bool {
    matches!(arg.to_str(), Some("--help" | "-h" | "help"))
}

pub(crate) fn is_forbidden_raw_secret_flag(arg: &OsStr) -> bool {
    let Some(text) = arg.to_str() else {
        return false;
    };
    let flag_name = text.split_once('=').map_or(text, |(prefix, _)| prefix);
    matches!(
        flag_name,
        "--bearer" | "--bearer-token" | "--token" | "--auth-token" | "--authorization"
    )
}

pub(crate) fn read_bearer_file(path: &Path) -> Result<RedactedBearer, CliSupportError> {
    if path.as_os_str().is_empty() {
        return Err(CliSupportError::MissingBearerFile);
    }
    let link_meta = fs::symlink_metadata(path).map_err(|_| CliSupportError::InsecureBearerFile)?;
    if !link_meta.file_type().is_file() {
        return Err(CliSupportError::InsecureBearerFile);
    }
    let file = File::open(path).map_err(|_| CliSupportError::BearerFileIo)?;
    let fd_meta = file.metadata().map_err(|_| CliSupportError::BearerFileIo)?;
    if !fd_meta.file_type().is_file() || !same_file_identity(&link_meta, &fd_meta) {
        return Err(CliSupportError::InsecureBearerFile);
    }
    let mut bytes = Vec::with_capacity(256);
    file.take((MAX_BEARER_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| CliSupportError::BearerFileIo)?;
    if bytes.len() > MAX_BEARER_FILE_BYTES {
        bytes.fill(0);
        return Err(CliSupportError::BearerFileOversized);
    }
    let parsed = build_sensitive_bearer_header(&bytes);
    bytes.fill(0);
    parsed
}

fn build_sensitive_bearer_header(bytes: &[u8]) -> Result<RedactedBearer, CliSupportError> {
    let raw = std::str::from_utf8(bytes).map_err(|_| CliSupportError::InvalidBearerSecret)?;
    let trimmed = raw.trim_end_matches(['\r', '\n']);
    let token = trimmed.strip_prefix("Bearer ").unwrap_or(trimmed);
    if token.is_empty()
        || token
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(CliSupportError::InvalidBearerSecret);
    }
    let header_text = format!("Bearer {token}");
    if header_text.len() > heterarchy_orchestration_auth::MAX_MCP_TOKEN_BEARER_BYTES {
        return Err(CliSupportError::InvalidBearerSecret);
    }
    let mut header =
        HeaderValue::from_str(&header_text).map_err(|_| CliSupportError::InvalidBearerSecret)?;
    header.set_sensitive(true);
    Ok(RedactedBearer(header))
}

#[cfg(unix)]
fn same_file_identity(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    (a.dev(), a.ino()) == (b.dev(), b.ino())
}

#[cfg(not(unix))]
fn same_file_identity(_a: &fs::Metadata, _b: &fs::Metadata) -> bool {
    true
}

pub(crate) fn read_bounded_json_file<T: DeserializeOwned>(
    path: &Path,
    max_bytes: usize,
) -> Result<T, CliSupportError> {
    if path.as_os_str().is_empty() {
        return Err(CliSupportError::InvalidRequestFile);
    }
    let link_meta = fs::symlink_metadata(path).map_err(|_| CliSupportError::InvalidRequestFile)?;
    if !link_meta.file_type().is_file() {
        return Err(CliSupportError::InvalidRequestFile);
    }
    let file = File::open(path).map_err(|_| CliSupportError::InvalidRequestFile)?;
    let fd_meta = file
        .metadata()
        .map_err(|_| CliSupportError::InvalidRequestFile)?;
    if !fd_meta.file_type().is_file() || !same_file_identity(&link_meta, &fd_meta) {
        return Err(CliSupportError::InvalidRequestFile);
    }
    let mut bytes = Vec::new();
    file.take((max_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| CliSupportError::InvalidRequestFile)?;
    if bytes.is_empty() || bytes.len() > max_bytes {
        return Err(CliSupportError::InvalidRequestFile);
    }
    serde_json::from_slice(&bytes).map_err(|_| CliSupportError::InvalidRequestJson)
}

pub(crate) fn format_json_output<T: Serialize>(value: &T) -> Result<String, CliSupportError> {
    serde_json::to_string_pretty(value).map_err(|_| CliSupportError::ResponseSerialize)
}

pub(crate) fn next_utf8_value<'a>(
    arguments: &'a [OsString],
    index: &mut usize,
) -> Result<&'a str, CliSupportError> {
    let val = arguments
        .get(*index + 1)
        .and_then(|arg| arg.to_str())
        .filter(|text| !text.is_empty())
        .ok_or(CliSupportError::InvalidArguments)?;
    *index += 2;
    Ok(val)
}

pub(crate) fn next_path_value(
    arguments: &[OsString],
    index: &mut usize,
) -> Result<PathBuf, CliSupportError> {
    let val = arguments
        .get(*index + 1)
        .filter(|arg| !arg.is_empty())
        .ok_or(CliSupportError::InvalidArguments)?;
    *index += 2;
    Ok(PathBuf::from(val))
}

pub(crate) fn parse_canonical_uuid(raw: &str) -> Result<Uuid, CliSupportError> {
    let parsed = Uuid::parse_str(raw).map_err(|_| CliSupportError::InvalidArguments)?;
    if parsed.is_nil() || parsed.to_string() != raw {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(parsed)
}

pub(crate) fn set_canonical_uuid_string(
    slot: &mut Option<String>,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    let raw = next_utf8_value(arguments, index)?;
    let _ = parse_canonical_uuid(raw)?;
    set_once(slot, raw.to_owned())
}

pub(crate) fn parse_lowercase_hex_digest(
    raw: &str,
    reject_all_zero: bool,
) -> Result<String, CliSupportError> {
    if raw.len() != 64 || !raw.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(CliSupportError::InvalidArguments);
    }
    if reject_all_zero && raw.bytes().all(|b| b == b'0') {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(raw.to_owned())
}

pub(crate) fn parse_exact<T>(raw: &str) -> Result<T, CliSupportError>
where
    T: std::str::FromStr + fmt::Display,
{
    let parsed = raw
        .parse::<T>()
        .map_err(|_| CliSupportError::InvalidArguments)?;
    if parsed.to_string() != raw {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(parsed)
}

pub(crate) fn set_once<T>(slot: &mut Option<T>, value: T) -> Result<(), CliSupportError> {
    if slot.is_some() {
        return Err(CliSupportError::InvalidArguments);
    }
    *slot = Some(value);
    Ok(())
}

/// Shared flag accumulator for three-axis [`ExecutionAuthorWire`] (`profile_id`, `participant_id`, `principal`).
#[derive(Default)]
pub(crate) struct AuthorFlagBuilder {
    pub(crate) profile_id: Option<String>,
    pub(crate) participant_id: Option<String>,
    pub(crate) principal_kind: Option<String>,
    pub(crate) principal_id: Option<String>,
}

impl AuthorFlagBuilder {
    pub(crate) fn try_consume_prefixed(
        &mut self,
        prefix: &str,
        flag: &str,
        arguments: &[OsString],
        index: &mut usize,
    ) -> Result<bool, CliSupportError> {
        let Some(suffix) = flag.strip_prefix(prefix) else {
            return Ok(false);
        };
        match suffix {
            "-profile-id" => {
                set_canonical_uuid_string(&mut self.profile_id, arguments, index)?;
                Ok(true)
            }
            "-participant-id" => {
                set_canonical_uuid_string(&mut self.participant_id, arguments, index)?;
                Ok(true)
            }
            "-principal-kind" => {
                let kind = parse_principal_kind_flag(next_utf8_value(arguments, index)?)?;
                set_once(&mut self.principal_kind, kind.to_owned())?;
                Ok(true)
            }
            "-principal-id" => {
                set_canonical_uuid_string(&mut self.principal_id, arguments, index)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(crate) fn build(self) -> Result<ExecutionAuthorWire, CliSupportError> {
        let kind = self
            .principal_kind
            .ok_or(CliSupportError::InvalidArguments)?;
        let id = match kind.as_str() {
            "trusted_stdio" => {
                if self.principal_id.is_some() {
                    return Err(CliSupportError::InvalidArguments);
                }
                String::new()
            }
            _ => self.principal_id.ok_or(CliSupportError::InvalidArguments)?,
        };
        Ok(ExecutionAuthorWire {
            profile_id: self.profile_id.ok_or(CliSupportError::InvalidArguments)?,
            participant_id: self
                .participant_id
                .ok_or(CliSupportError::InvalidArguments)?,
            principal: PrincipalIdentityWire { kind, id },
        })
    }
}

pub(crate) fn parse_principal_kind_flag(raw: &str) -> Result<&'static str, CliSupportError> {
    match raw {
        "trusted_stdio" => Ok("trusted_stdio"),
        "direct_token" => Ok("direct_token"),
        "oauth_grant" => Ok("oauth_grant"),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

/// Shared flag accumulator for [`ExecutionPreferenceWire`] across `profile` and `swarm model-switch` CLIs.
#[derive(Default)]
pub(crate) struct ExecutionPreferenceFlagBuilder {
    pub(crate) cli_engine: Option<String>,
    pub(crate) preferred_model: Option<String>,
    pub(crate) preferred_reasoning: Option<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) role_prompt_digest_hex: Option<String>,
}

/// Resolved profile preference fields extracted from inline CLI flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedProfileFields {
    pub(crate) preferred_model: Option<String>,
    pub(crate) preferred_reasoning: Option<String>,
    pub(crate) base_capabilities: Vec<String>,
    pub(crate) execution_preference: Option<ExecutionPreferenceWire>,
}

impl ExecutionPreferenceFlagBuilder {
    pub(crate) fn try_consume(
        &mut self,
        flag: &str,
        arguments: &[OsString],
        index: &mut usize,
    ) -> Result<bool, CliSupportError> {
        match flag {
            "--cli-engine" => {
                let raw = next_utf8_value(arguments, index)?;
                if !matches!(raw, "codex" | "agy") {
                    return Err(CliSupportError::InvalidArguments);
                }
                set_once(&mut self.cli_engine, raw.to_owned())?;
                Ok(true)
            }
            "--preferred-model" => {
                set_once(
                    &mut self.preferred_model,
                    next_utf8_value(arguments, index)?.to_owned(),
                )?;
                Ok(true)
            }
            "--preferred-reasoning" => {
                set_once(
                    &mut self.preferred_reasoning,
                    next_utf8_value(arguments, index)?.to_owned(),
                )?;
                Ok(true)
            }
            "--capability" | "--base-capability" => {
                self.capabilities
                    .push(next_utf8_value(arguments, index)?.to_owned());
                Ok(true)
            }
            "--role-prompt-digest" => {
                let hex = parse_lowercase_hex_digest(next_utf8_value(arguments, index)?, true)?;
                set_once(&mut self.role_prompt_digest_hex, hex)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(crate) fn has_any(&self) -> bool {
        self.cli_engine.is_some()
            || self.preferred_model.is_some()
            || self.preferred_reasoning.is_some()
            || !self.capabilities.is_empty()
            || self.role_prompt_digest_hex.is_some()
    }

    pub(crate) fn into_profile_fields(self) -> Result<ResolvedProfileFields, CliSupportError> {
        if let Some(cli_engine) = self.cli_engine {
            let model = self
                .preferred_model
                .ok_or(CliSupportError::InvalidArguments)?;
            let pref = ExecutionPreferenceWire {
                cli_engine,
                preferred_model: model.clone(),
                preferred_reasoning: self.preferred_reasoning.clone(),
                capabilities: self.capabilities.clone(),
                role_prompt_digest_hex: self.role_prompt_digest_hex,
            };
            Ok(ResolvedProfileFields {
                preferred_model: Some(model),
                preferred_reasoning: self.preferred_reasoning,
                base_capabilities: self.capabilities,
                execution_preference: Some(pref),
            })
        } else {
            if self.role_prompt_digest_hex.is_some() {
                return Err(CliSupportError::InvalidArguments);
            }
            Ok(ResolvedProfileFields {
                preferred_model: self.preferred_model,
                preferred_reasoning: self.preferred_reasoning,
                base_capabilities: self.capabilities,
                execution_preference: None,
            })
        }
    }

    pub(crate) fn into_required_preference(
        self,
    ) -> Result<ExecutionPreferenceWire, CliSupportError> {
        let cli_engine = self.cli_engine.ok_or(CliSupportError::InvalidArguments)?;
        let preferred_model = self
            .preferred_model
            .ok_or(CliSupportError::InvalidArguments)?;
        Ok(ExecutionPreferenceWire {
            cli_engine,
            preferred_model,
            preferred_reasoning: self.preferred_reasoning,
            capabilities: self.capabilities,
            role_prompt_digest_hex: self.role_prompt_digest_hex,
        })
    }
}
