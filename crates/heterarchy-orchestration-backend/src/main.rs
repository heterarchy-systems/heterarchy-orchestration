use std::env;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use forge_domain::{AuthenticatedPrincipal, DirectTokenId, ForgeBackendId, OrchestrationIssuerId};
use heterarchy_orchestration_auth::McpTokenBootstrapMode;
use heterarchy_orchestration_backend::{
    AdmissionIssuerBootstrap, BackendConfig, BackendError, TokenAuthorityBootstrap, autonomy_cli,
    profile_cli, serve, session_process, swarm_cli, token_cli,
};
use heterarchy_orchestration_operations::BrokerClusterConfig;
use heterarchy_orchestration_runtime::access::OperatorPolicy;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DATA_DIR_ENV: &str = "HETERARCHY_ORCHESTRATION_DATA_DIR";
const LISTEN_ENV: &str = "HETERARCHY_ORCHESTRATION_BACKEND_ADDR";
const TOKEN_BOOTSTRAP_ENV: &str = "HETERARCHY_ORCHESTRATION_TOKEN_BOOTSTRAP";
const DEFAULT_LISTEN: &str = "127.0.0.1:8798";
const OPERATOR_PRINCIPALS_ENV: &str = "HETERARCHY_ORCHESTRATION_OPERATOR_PRINCIPALS";
const FORGE_SOURCE_CONFIG_ENV: &str = "HETERARCHY_ORCHESTRATION_FORGE_SOURCE_CONFIG";
const ADMISSION_ISSUER_ID_ENV: &str = "HETERARCHY_ORCHESTRATION_ADMISSION_ISSUER_ID";
const ADMISSION_BACKEND_ID_ENV: &str = "HETERARCHY_ORCHESTRATION_ADMISSION_BACKEND_ID";
const ADMISSION_TTL_MS_ENV: &str = "HETERARCHY_ORCHESTRATION_ADMISSION_TTL_MS";
const BINDING_REJECTION_TRUST_MANIFEST_ENV: &str =
    "HETERARCHY_ORCHESTRATION_BINDING_REJECTION_TRUST_MANIFEST";
const BROKER_ADDR_ENV: &str = "HETERARCHY_ORCHESTRATION_BROKER_ADDR";
const SESSION_AUTHORITY_ADDR_ENV: &str = "HETERARCHY_ORCHESTRATION_SESSION_AUTHORITY_ADDR";
const SESSION_AUTHORITY_CATALOG_ENV: &str = "HETERARCHY_ORCHESTRATION_SESSION_AUTHORITY_CATALOG";
const SESSION_AUTHORITY_SECRET_FILE_ENV: &str =
    "HETERARCHY_ORCHESTRATION_SESSION_AUTHORITY_SECRET_FILE";
const WORKER_BEARER_FILE_ENV: &str = "HETERARCHY_ORCHESTRATION_WORKER_BEARER_FILE";

#[derive(Debug)]
enum TopLevelCommand {
    ServeDaemon,
    Version,
    Help,
    SessionAuthorityHelp,
    SessionAuthority(session_process::SessionAuthorityOptions),
    TokenHelp,
    TokenIssue(token_cli::TokenIssueOptions),
    ProfileHelp,
    Profile(Box<profile_cli::ProfileCliOptions>),
    SwarmHelp,
    Swarm(Box<swarm_cli::SwarmCliOptions>),
    AutonomyHelp,
    Autonomy(Box<autonomy_cli::AutonomyCliOptions>),
}

fn classify_cli_command(raw_arguments: &[OsString]) -> Result<TopLevelCommand, MainError> {
    let Some(first) = raw_arguments.first() else {
        return Ok(TopLevelCommand::ServeDaemon);
    };
    match first.to_str() {
        Some("--version" | "version") if raw_arguments.len() == 1 => Ok(TopLevelCommand::Version),
        Some("--help" | "-h" | "help") if raw_arguments.len() == 1 => Ok(TopLevelCommand::Help),
        Some("session-authority") => {
            if raw_arguments.len() == 2
                && matches!(raw_arguments[1].to_str(), Some("--help" | "-h" | "help"))
            {
                return Ok(TopLevelCommand::SessionAuthorityHelp);
            }
            session_process::parse(raw_arguments)?
                .map(TopLevelCommand::SessionAuthority)
                .ok_or(MainError::UnexpectedArguments)
        }
        Some("token") => match token_cli::parse(raw_arguments)? {
            token_cli::TokenCliCommand::Help => Ok(TopLevelCommand::TokenHelp),
            token_cli::TokenCliCommand::Issue(options) => Ok(TopLevelCommand::TokenIssue(options)),
            token_cli::TokenCliCommand::NotTokenCommand => Err(MainError::UnexpectedArguments),
        },
        Some("profile") => match profile_cli::parse(raw_arguments)? {
            profile_cli::ProfileCliCommand::Help => Ok(TopLevelCommand::ProfileHelp),
            profile_cli::ProfileCliCommand::Run(options) => Ok(TopLevelCommand::Profile(options)),
            profile_cli::ProfileCliCommand::NotProfileCommand => {
                Err(MainError::UnexpectedArguments)
            }
        },
        Some("swarm") => match swarm_cli::parse(raw_arguments)? {
            swarm_cli::SwarmCliCommand::Help => Ok(TopLevelCommand::SwarmHelp),
            swarm_cli::SwarmCliCommand::Run(options) => Ok(TopLevelCommand::Swarm(options)),
            swarm_cli::SwarmCliCommand::NotSwarmCommand => Err(MainError::UnexpectedArguments),
        },
        Some("autonomy") => match autonomy_cli::parse(raw_arguments)? {
            autonomy_cli::AutonomyCliCommand::Help => Ok(TopLevelCommand::AutonomyHelp),
            autonomy_cli::AutonomyCliCommand::Run(options) => {
                Ok(TopLevelCommand::Autonomy(options))
            }
            autonomy_cli::AutonomyCliCommand::NotAutonomyCommand => {
                Err(MainError::UnexpectedArguments)
            }
        },
        _ => Err(MainError::UnexpectedArguments),
    }
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let raw_arguments = env::args_os().skip(1).collect::<Vec<_>>();
    match classify_cli_command(&raw_arguments)? {
        TopLevelCommand::ServeDaemon => run_daemon_from_env().await,
        TopLevelCommand::Version => {
            println!(
                "heterarchy-orchestration-backend {}",
                env!("CARGO_PKG_VERSION")
            );
            Ok(())
        }
        TopLevelCommand::Help => {
            print_usage();
            Ok(())
        }
        TopLevelCommand::SessionAuthorityHelp => {
            println!("{}", session_process::SESSION_AUTHORITY_USAGE);
            Ok(())
        }
        TopLevelCommand::SessionAuthority(options) => {
            session_process::run_session_authority_process(options).await?;
            Ok(())
        }
        TopLevelCommand::TokenHelp => {
            println!("{}", token_cli::TOKEN_ISSUE_USAGE);
            Ok(())
        }
        TopLevelCommand::TokenIssue(options) => {
            let metadata = token_cli::execute(&options)?;
            println!("token_id={}", metadata.token_id());
            println!("operator_principal=token:{}", metadata.token_id());
            Ok(())
        }
        TopLevelCommand::ProfileHelp => {
            println!("{}", profile_cli::PROFILE_CLI_USAGE);
            Ok(())
        }
        TopLevelCommand::Profile(options) => {
            let json = profile_cli::execute(&options).await?;
            println!("{json}");
            Ok(())
        }
        TopLevelCommand::SwarmHelp => {
            println!("{}", swarm_cli::SWARM_CLI_USAGE);
            Ok(())
        }
        TopLevelCommand::Swarm(options) => {
            let json = swarm_cli::execute(&options).await?;
            println!("{json}");
            Ok(())
        }
        TopLevelCommand::AutonomyHelp => {
            println!("{}", autonomy_cli::AUTONOMY_CLI_USAGE);
            Ok(())
        }
        TopLevelCommand::Autonomy(options) => {
            let json = autonomy_cli::execute(&options).await?;
            println!("{json}");
            Ok(())
        }
    }
}

async fn run_daemon_from_env() -> Result<(), MainError> {
    tracing_subscriber::fmt()
        .json()
        .with_target(false)
        .try_init()
        .map_err(|_| MainError::Tracing)?;

    let data_root = env::var_os(DATA_DIR_ENV)
        .map(PathBuf::from)
        .ok_or(MainError::MissingDataRoot)?;
    let listen = optional_utf8_env(LISTEN_ENV, || MainError::InvalidListen)?
        .unwrap_or_else(|| DEFAULT_LISTEN.to_owned())
        .parse::<SocketAddr>()
        .map_err(|_| MainError::InvalidListen)?;
    let mut config = BackendConfig::new(listen, data_root.clone())?;
    if let Some(broker_cluster) = parse_broker_cluster_env()? {
        config = config.with_broker_cluster(broker_cluster);
    }
    if let Some(admission_issuer) = parse_admission_issuer_config()? {
        config = config.with_admission_issuer(admission_issuer);
    }
    if let Some(raw) =
        optional_utf8_env(OPERATOR_PRINCIPALS_ENV, || MainError::InvalidOperatorPolicy)?
    {
        config = config.with_operator_policy(parse_operator_policy(&raw)?);
    }
    config = with_binding_rejection_trust(config, &data_root)?;
    // Default policy requires an already-initialized app-global token
    // authority; first-run bootstrapping is an explicit operator decision.
    if env::var(TOKEN_BOOTSTRAP_ENV).as_deref() == Ok("initialize") {
        config = config.with_token_authority(TokenAuthorityBootstrap::AppGlobal(
            McpTokenBootstrapMode::InitializeAuthority,
        ));
    }
    if let Some(path) = env::var_os(FORGE_SOURCE_CONFIG_ENV) {
        let authority =
            heterarchy_orchestration_backend::forge_source_config::load(&PathBuf::from(path))
                .map_err(|_| MainError::InvalidForgeSource)?;
        config = config.with_coordinator_authority(authority);
    }
    if let Some(embedded_session_authority) = parse_embedded_session_authority_config()? {
        config = config.with_embedded_session_authority(embedded_session_authority);
    }
    if let Some(worker_bearer) = env::var_os(WORKER_BEARER_FILE_ENV) {
        if worker_bearer.is_empty() {
            return Err(MainError::InvalidWorkerBearerFile);
        }
        config = config.with_worker_bearer_file(PathBuf::from(worker_bearer));
    }
    serve_until_shutdown(config).await
}

async fn serve_until_shutdown(config: BackendConfig) -> Result<(), MainError> {
    let cancellation = CancellationToken::new();
    let server = serve(config, cancellation.clone());
    tokio::pin!(server);
    // Service managers stop the daemon with SIGTERM; Ctrl+C stays SIGINT.
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(MainError::Signal)?;

    tokio::select! {
        result = &mut server => result.map_err(MainError::Backend),
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(MainError::Signal)?;
            cancellation.cancel();
            server.await.map_err(MainError::Backend)
        }
        _ = terminate.recv() => {
            cancellation.cancel();
            server.await.map_err(MainError::Backend)
        }
    }
}

fn with_binding_rejection_trust(
    config: BackendConfig,
    data_root: &Path,
) -> Result<BackendConfig, MainError> {
    let Some(path) = env::var_os(BINDING_REJECTION_TRUST_MANIFEST_ENV) else {
        return Ok(config);
    };
    let deployments = heterarchy_orchestration_backend::binding_rejection_trust::load(
        &PathBuf::from(path),
        data_root,
    )
    .map_err(|_| MainError::InvalidBindingRejectionTrustManifest)?;
    Ok(config.with_trusted_binding_rejection_legacy_deployments(deployments))
}

fn usage_text() -> String {
    let mut out = String::with_capacity(1280);
    let _ = writeln!(
        out,
        "usage: heterarchy-orchestration-backend [--version | --help]"
    );
    let _ = writeln!(out, "{}", token_cli::TOKEN_ISSUE_USAGE);
    let _ = writeln!(out, "{}", session_process::SESSION_AUTHORITY_USAGE);
    let _ = writeln!(out, "{}", profile_cli::PROFILE_CLI_USAGE);
    let _ = writeln!(out, "{}", swarm_cli::SWARM_CLI_USAGE);
    let _ = writeln!(out, "{}", autonomy_cli::AUTONOMY_CLI_USAGE);
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "The Orchestration headless backend daemon serves HTTP command/query + SSE."
    );
    let _ = writeln!(out, "Configuration is environmental:");
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_DATA_DIR (required): daemon-owned data root"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_BACKEND_ADDR: listen/client address, default 127.0.0.1:8798"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_BEARER_FILE: private bearer token file path for profile/swarm/autonomy CLI"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_WORKER_BEARER_FILE: optional private bearer token file forwarded to dispatched Swarm workers"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_TOKEN_BOOTSTRAP: \"initialize\" for first-run authority bootstrap"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_OPERATOR_PRINCIPALS: comma-separated token: operator UUIDs"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_FORGE_SOURCE_CONFIG: host-trusted Forge source config path"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_BINDING_REJECTION_TRUST_MANIFEST: sealed legacy deployment allowlist path"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_ADMISSION_ISSUER_ID: explicit Forge admission issuer UUID"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_ADMISSION_BACKEND_ID: explicit Forge backend UUID"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_ADMISSION_TTL_MS: admission envelope TTL in milliseconds"
    );
    let _ = writeln!(
        out,
        "  HETERARCHY_ORCHESTRATION_BROKER_ADDR: single-node Broker address (e.g. 127.0.0.1:8811 or 127.0.0.1:8811@127.0.0.1:9811)"
    );
    let _ = write!(
        out,
        "  HETERARCHY_ORCHESTRATION_SESSION_AUTHORITY_ADDR / _CATALOG / _SECRET_FILE: optional in-process loopback Session Authority supervision"
    );
    out
}

fn print_usage() {
    println!("{}", usage_text());
}

fn optional_utf8_env<F>(key: &str, on_invalid_utf8: F) -> Result<Option<String>, MainError>
where
    F: FnOnce() -> MainError,
{
    env::var_os(key)
        .map(|value| value.into_string().map_err(|_| on_invalid_utf8()))
        .transpose()
}

fn parse_canonical_non_nil_uuid(raw: &str) -> Option<Uuid> {
    let parsed = Uuid::parse_str(raw).ok()?;
    if parsed.is_nil() || parsed.to_string() != raw {
        return None;
    }
    Some(parsed)
}

fn parse_broker_cluster_env() -> Result<Option<BrokerClusterConfig>, MainError> {
    let Some(raw) = optional_utf8_env(BROKER_ADDR_ENV, || MainError::InvalidBrokerAddr)? else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(MainError::InvalidBrokerAddr);
    }
    if let Some((broker_part, ops_part)) = trimmed.split_once('@') {
        let broker_addr = broker_part
            .trim()
            .parse::<SocketAddr>()
            .map_err(|_| MainError::InvalidBrokerAddr)?;
        let ops_addr = ops_part
            .trim()
            .parse::<SocketAddr>()
            .map_err(|_| MainError::InvalidBrokerAddr)?;
        BrokerClusterConfig::from_single_node(broker_addr, ops_addr)
            .map(Some)
            .map_err(|_| MainError::InvalidBrokerAddr)
    } else {
        let broker_addr = trimmed
            .parse::<SocketAddr>()
            .map_err(|_| MainError::InvalidBrokerAddr)?;
        BrokerClusterConfig::from_broker_address(broker_addr)
            .map(Some)
            .map_err(|_| MainError::InvalidBrokerAddr)
    }
}

fn parse_operator_policy(raw: &str) -> Result<OperatorPolicy, MainError> {
    if raw.len() > 768 || raw.is_empty() {
        return Err(MainError::InvalidOperatorPolicy);
    }
    let principals = raw
        .split(',')
        .map(|entry| {
            let (kind, identity) = entry
                .split_once(':')
                .ok_or(MainError::InvalidOperatorPolicy)?;
            let id =
                parse_canonical_non_nil_uuid(identity).ok_or(MainError::InvalidOperatorPolicy)?;
            match kind {
                "token" => Ok(AuthenticatedPrincipal::DirectToken(
                    DirectTokenId::from_uuid(id),
                )),
                _ => Err(MainError::InvalidOperatorPolicy),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    OperatorPolicy::new(principals).map_err(|_| MainError::InvalidOperatorPolicy)
}

fn parse_admission_issuer_config() -> Result<Option<AdmissionIssuerBootstrap>, MainError> {
    let issuer = optional_utf8_env(ADMISSION_ISSUER_ID_ENV, || {
        MainError::InvalidAdmissionIssuer
    })?;
    let backend = optional_utf8_env(ADMISSION_BACKEND_ID_ENV, || {
        MainError::InvalidAdmissionIssuer
    })?;
    let ttl = optional_utf8_env(ADMISSION_TTL_MS_ENV, || MainError::InvalidAdmissionIssuer)?;
    parse_admission_issuer_values(issuer.as_deref(), backend.as_deref(), ttl.as_deref())
}

fn parse_admission_issuer_values(
    issuer: Option<&str>,
    backend: Option<&str>,
    ttl: Option<&str>,
) -> Result<Option<AdmissionIssuerBootstrap>, MainError> {
    let (Some(issuer), Some(backend), Some(ttl)) = (issuer, backend, ttl) else {
        return if issuer.is_none() && backend.is_none() && ttl.is_none() {
            Ok(None)
        } else {
            Err(MainError::InvalidAdmissionIssuer)
        };
    };
    let issuer_id =
        parse_canonical_non_nil_uuid(issuer).ok_or(MainError::InvalidAdmissionIssuer)?;
    let backend_id =
        parse_canonical_non_nil_uuid(backend).ok_or(MainError::InvalidAdmissionIssuer)?;
    let admission_ttl_ms = ttl
        .parse::<i64>()
        .map_err(|_| MainError::InvalidAdmissionIssuer)?;
    if admission_ttl_ms.to_string() != ttl {
        return Err(MainError::InvalidAdmissionIssuer);
    }
    AdmissionIssuerBootstrap::new(
        OrchestrationIssuerId::from_uuid(issuer_id),
        ForgeBackendId::from_uuid(backend_id),
        admission_ttl_ms,
    )
    .map(Some)
    .map_err(|_| MainError::InvalidAdmissionIssuer)
}

fn parse_embedded_session_authority_config() -> Result<
    Option<heterarchy_orchestration_backend::session_process::EmbeddedSessionAuthorityConfig>,
    MainError,
> {
    let addr = optional_utf8_env(SESSION_AUTHORITY_ADDR_ENV, || {
        MainError::InvalidEmbeddedSessionAuthority
    })?;
    let catalog = env::var_os(SESSION_AUTHORITY_CATALOG_ENV).map(PathBuf::from);
    let secret = env::var_os(SESSION_AUTHORITY_SECRET_FILE_ENV).map(PathBuf::from);
    parse_embedded_session_authority_values(addr.as_deref(), catalog, secret)
}

fn parse_embedded_session_authority_values(
    addr: Option<&str>,
    catalog: Option<PathBuf>,
    secret: Option<PathBuf>,
) -> Result<
    Option<heterarchy_orchestration_backend::session_process::EmbeddedSessionAuthorityConfig>,
    MainError,
> {
    let (addr, catalog, secret) = match (addr, catalog, secret) {
        (Some(addr), Some(catalog), Some(secret)) => (addr, catalog, secret),
        (None, None, None) => return Ok(None),
        _ => return Err(MainError::InvalidEmbeddedSessionAuthority),
    };
    let listen = addr
        .parse::<SocketAddr>()
        .map_err(|_| MainError::InvalidEmbeddedSessionAuthority)?;
    heterarchy_orchestration_backend::session_process::EmbeddedSessionAuthorityConfig::new(
        listen, catalog, secret,
    )
    .map(Some)
    .map_err(|_| MainError::InvalidEmbeddedSessionAuthority)
}

#[derive(Debug, Error)]
enum MainError {
    #[error("session authority host failed: {0}")]
    SessionAuthority(#[from] session_process::SessionAuthorityProcessError),
    #[error("token command failed: {0}")]
    Token(#[from] token_cli::TokenCliError),
    #[error("profile command failed: {0}")]
    Profile(#[from] profile_cli::ProfileCliError),
    #[error("swarm command failed: {0}")]
    Swarm(#[from] swarm_cli::SwarmCliError),
    #[error("autonomy command failed: {0}")]
    Autonomy(#[from] autonomy_cli::AutonomyCliError),
    #[error("unexpected CLI arguments; run with --help for usage")]
    UnexpectedArguments,
    #[error("HETERARCHY_ORCHESTRATION_DATA_DIR is required")]
    MissingDataRoot,
    #[error("HETERARCHY_ORCHESTRATION_BACKEND_ADDR is invalid")]
    InvalidListen,
    #[error("HETERARCHY_ORCHESTRATION_BROKER_ADDR is invalid")]
    InvalidBrokerAddr,
    #[error(
        "HETERARCHY_ORCHESTRATION_OPERATOR_PRINCIPALS must name bounded exact token: identities"
    )]
    InvalidOperatorPolicy,
    #[error(
        "HETERARCHY_ORCHESTRATION_ADMISSION_ISSUER_ID, HETERARCHY_ORCHESTRATION_ADMISSION_BACKEND_ID, and HETERARCHY_ORCHESTRATION_ADMISSION_TTL_MS must be supplied together with canonical values"
    )]
    InvalidAdmissionIssuer,
    #[error("HETERARCHY_ORCHESTRATION_FORGE_SOURCE_CONFIG is invalid or unavailable")]
    InvalidForgeSource,
    #[error("HETERARCHY_ORCHESTRATION_WORKER_BEARER_FILE is invalid")]
    InvalidWorkerBearerFile,
    #[error(
        "HETERARCHY_ORCHESTRATION_SESSION_AUTHORITY_ADDR, HETERARCHY_ORCHESTRATION_SESSION_AUTHORITY_CATALOG, and HETERARCHY_ORCHESTRATION_SESSION_AUTHORITY_SECRET_FILE must be supplied together with loopback and absolute paths"
    )]
    InvalidEmbeddedSessionAuthority,
    #[error("HETERARCHY_ORCHESTRATION_BINDING_REJECTION_TRUST_MANIFEST is invalid or unavailable")]
    InvalidBindingRejectionTrustManifest,
    #[error("structured tracing initialization failed")]
    Tracing,
    #[error("signal handling failed")]
    Signal(#[source] std::io::Error),
    #[error("backend failed")]
    Backend(#[from] BackendError),
}

#[cfg(test)]
mod tests {
    use super::*;

    const ISSUER: &str = "00000000-0000-0000-0000-0000000000ab";
    const BACKEND: &str = "00000000-0000-0000-0000-0000000000cd";
    const BACKEND_UPPER: &str = "00000000-0000-0000-0000-0000000000CD";

    #[test]
    fn admission_issuer_configuration_is_optional_when_all_values_are_absent() {
        assert!(matches!(
            parse_admission_issuer_values(None, None, None),
            Ok(None)
        ));
    }

    #[test]
    fn admission_issuer_configuration_rejects_partial_values() {
        for values in [
            (Some(ISSUER), None, None),
            (None, Some(BACKEND), None),
            (None, None, Some("300000")),
            (Some(ISSUER), Some(BACKEND), None),
            (Some(ISSUER), None, Some("300000")),
            (None, Some(BACKEND), Some("300000")),
        ] {
            assert!(
                parse_admission_issuer_values(values.0, values.1, values.2).is_err(),
                "unexpectedly accepted {values:?}"
            );
        }
    }

    #[test]
    fn admission_issuer_configuration_rejects_noncanonical_ids_and_ttl_bounds() {
        for values in [
            (Some("not-an-id"), Some(BACKEND), Some("300000")),
            (
                Some("00000000-0000-0000-0000-000000000000"),
                Some(BACKEND),
                Some("300000"),
            ),
            (Some(ISSUER), Some(BACKEND_UPPER), Some("300000")),
            (Some(ISSUER), Some(BACKEND), Some("999")),
            (Some(ISSUER), Some(BACKEND), Some("3600001")),
            (Some(ISSUER), Some(BACKEND), Some("not-a-number")),
            (Some(ISSUER), Some(BACKEND), Some("+1000")),
            (Some(ISSUER), Some(BACKEND), Some("01000")),
        ] {
            assert!(
                parse_admission_issuer_values(values.0, values.1, values.2).is_err(),
                "unexpectedly accepted {values:?}"
            );
        }
    }

    #[test]
    fn admission_issuer_configuration_accepts_exact_typed_values() -> Result<(), &'static str> {
        let config = parse_admission_issuer_values(Some(ISSUER), Some(BACKEND), Some("300000"))
            .map_err(|_| "valid configuration rejected")?
            .ok_or("configuration absent")?;
        assert_eq!(config.issuer_id().into_uuid(), Uuid::from_u128(0xab));
        assert_eq!(config.backend_id().into_uuid(), Uuid::from_u128(0xcd));
        assert_eq!(config.admission_ttl_ms(), 300_000);
        Ok(())
    }

    fn os_args(items: &[&str]) -> Vec<OsString> {
        items.iter().map(OsString::from).collect()
    }

    #[test]
    fn top_level_cli_dispatch_unifies_daemon_help_version_and_subcommands() {
        assert!(matches!(
            classify_cli_command(&[]),
            Ok(TopLevelCommand::ServeDaemon)
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&["--version"])),
            Ok(TopLevelCommand::Version)
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&["--help"])),
            Ok(TopLevelCommand::Help)
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&["session-authority", "--help"])),
            Ok(TopLevelCommand::SessionAuthorityHelp)
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&["token", "--help"])),
            Ok(TopLevelCommand::TokenHelp)
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&["profile", "--help"])),
            Ok(TopLevelCommand::ProfileHelp)
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&["profile", "list", "--bearer-file", "/tmp/tok"])),
            Ok(TopLevelCommand::Profile(_))
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&["swarm", "--help"])),
            Ok(TopLevelCommand::SwarmHelp)
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&["autonomy", "--help"])),
            Ok(TopLevelCommand::AutonomyHelp)
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&[
                "autonomy",
                "scope-get",
                "--bearer-file",
                "/tmp/tok"
            ])),
            Ok(TopLevelCommand::Autonomy(_))
        ));
        assert!(matches!(
            classify_cli_command(&os_args(&[
                "swarm",
                "list",
                "--workspace-id",
                "0196ed00-0000-7000-8000-000000000001",
                "--bearer-file",
                "/tmp/tok"
            ])),
            Ok(TopLevelCommand::Swarm(_))
        ));

        // Trailing or unknown arguments fail closed without echoing raw argv text.
        for bad in [
            &["--version", "extra"][..],
            &["--help", "extra"],
            &["unknown-subcommand", "sensitive-token-value"],
        ] {
            let err = classify_cli_command(&os_args(bad));
            assert!(matches!(err, Err(MainError::UnexpectedArguments)));
            if let Err(actual) = err {
                assert!(!actual.to_string().contains("sensitive-token-value"));
            }
        }
    }

    #[test]
    fn usage_text_documents_profile_swarm_and_autonomy_subcommands() {
        let rendered = usage_text();
        assert!(rendered.contains(token_cli::TOKEN_ISSUE_USAGE));
        assert!(rendered.contains(session_process::SESSION_AUTHORITY_USAGE));
        assert!(rendered.contains(profile_cli::PROFILE_CLI_USAGE));
        assert!(rendered.contains(swarm_cli::SWARM_CLI_USAGE));
        assert!(rendered.contains(autonomy_cli::AUTONOMY_CLI_USAGE));
        assert!(rendered.contains("HETERARCHY_ORCHESTRATION_BEARER_FILE"));
        assert!(rendered.contains("HETERARCHY_ORCHESTRATION_WORKER_BEARER_FILE"));
    }
}
