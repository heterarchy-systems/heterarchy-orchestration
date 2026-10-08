use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use forge_domain::{AuthenticatedPrincipal, DirectTokenId, ForgeBackendId, OrchestrationIssuerId};
use heterarchy_orchestration_auth::McpTokenBootstrapMode;
use heterarchy_orchestration_backend::{
    AdmissionIssuerBootstrap, BackendConfig, BackendError, TokenAuthorityBootstrap, serve,
};
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

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let raw_arguments = env::args_os().skip(1).collect::<Vec<_>>();
    if raw_arguments.len() == 2
        && raw_arguments[0] == "session-authority"
        && matches!(raw_arguments[1].to_str(), Some("--help" | "-h" | "help"))
    {
        println!(
            "{}",
            heterarchy_orchestration_backend::session_process::SESSION_AUTHORITY_USAGE
        );
        return Ok(());
    }
    if let Some(options) = heterarchy_orchestration_backend::session_process::parse(&raw_arguments)?
    {
        heterarchy_orchestration_backend::session_process::run_session_authority_process(options)
            .await?;
        return Ok(());
    }
    match heterarchy_orchestration_backend::token_cli::parse(&raw_arguments)? {
        heterarchy_orchestration_backend::token_cli::TokenCliCommand::NotTokenCommand => {}
        heterarchy_orchestration_backend::token_cli::TokenCliCommand::Help => {
            println!(
                "{}",
                heterarchy_orchestration_backend::token_cli::TOKEN_ISSUE_USAGE
            );
            return Ok(());
        }
        heterarchy_orchestration_backend::token_cli::TokenCliCommand::Issue(options) => {
            let metadata = heterarchy_orchestration_backend::token_cli::execute(&options)?;
            println!("token_id={}", metadata.token_id());
            println!("operator_principal=token:{}", metadata.token_id());
            return Ok(());
        }
    }
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        None => {}
        Some("--version" | "version") => {
            println!(
                "heterarchy-orchestration-backend {}",
                env!("CARGO_PKG_VERSION")
            );
            return Ok(());
        }
        Some("--help" | "-h" | "help") => {
            print_usage();
            return Ok(());
        }
        Some(other) => return Err(MainError::UnexpectedArgument(other.to_owned())),
    }

    tracing_subscriber::fmt()
        .json()
        .with_target(false)
        .try_init()
        .map_err(|_| MainError::Tracing)?;

    let data_root = env::var_os(DATA_DIR_ENV)
        .map(PathBuf::from)
        .ok_or(MainError::MissingDataRoot)?;
    let listen = env::var(LISTEN_ENV)
        .unwrap_or_else(|_| DEFAULT_LISTEN.to_owned())
        .parse::<SocketAddr>()
        .map_err(|_| MainError::InvalidListen)?;
    let mut config = BackendConfig::new(listen, data_root)?;
    if let Some(admission_issuer) = parse_admission_issuer_config()? {
        config = config.with_admission_issuer(admission_issuer);
    }
    if let Some(raw) = env::var_os(OPERATOR_PRINCIPALS_ENV) {
        let raw = raw.to_str().ok_or(MainError::InvalidOperatorPolicy)?;
        config = config.with_operator_policy(parse_operator_policy(raw)?);
    }
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

fn print_usage() {
    println!("usage: heterarchy-orchestration-backend [--version | --help]");
    println!(
        "{}",
        heterarchy_orchestration_backend::token_cli::TOKEN_ISSUE_USAGE
    );
    println!(
        "{}",
        heterarchy_orchestration_backend::session_process::SESSION_AUTHORITY_USAGE
    );
    println!();
    println!("The Orchestration headless backend daemon serves HTTP command/query + SSE.");
    println!("Configuration is environmental:");
    println!("  HETERARCHY_ORCHESTRATION_DATA_DIR (required): daemon-owned data root");
    println!("  HETERARCHY_ORCHESTRATION_BACKEND_ADDR: listen address, default 127.0.0.1:8798");
    println!(
        "  HETERARCHY_ORCHESTRATION_TOKEN_BOOTSTRAP: \"initialize\" for first-run authority bootstrap"
    );
    println!(
        "  HETERARCHY_ORCHESTRATION_OPERATOR_PRINCIPALS: comma-separated token: operator UUIDs"
    );
    println!(
        "  HETERARCHY_ORCHESTRATION_FORGE_SOURCE_CONFIG: host-trusted Forge source config path"
    );
    println!(
        "  HETERARCHY_ORCHESTRATION_ADMISSION_ISSUER_ID: explicit Forge admission issuer UUID"
    );
    println!("  HETERARCHY_ORCHESTRATION_ADMISSION_BACKEND_ID: explicit Forge backend UUID");
    println!("  HETERARCHY_ORCHESTRATION_ADMISSION_TTL_MS: admission envelope TTL in milliseconds");
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
            let id = Uuid::parse_str(identity).map_err(|_| MainError::InvalidOperatorPolicy)?;
            if id.is_nil() || id.to_string() != identity {
                return Err(MainError::InvalidOperatorPolicy);
            }
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
    let issuer = env::var_os(ADMISSION_ISSUER_ID_ENV)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| MainError::InvalidAdmissionIssuer)
        })
        .transpose()?;
    let backend = env::var_os(ADMISSION_BACKEND_ID_ENV)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| MainError::InvalidAdmissionIssuer)
        })
        .transpose()?;
    let ttl = env::var_os(ADMISSION_TTL_MS_ENV)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| MainError::InvalidAdmissionIssuer)
        })
        .transpose()?;
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
    let issuer_id = Uuid::parse_str(issuer).map_err(|_| MainError::InvalidAdmissionIssuer)?;
    let backend_id = Uuid::parse_str(backend).map_err(|_| MainError::InvalidAdmissionIssuer)?;
    if issuer_id.is_nil()
        || backend_id.is_nil()
        || issuer_id.to_string() != issuer
        || backend_id.to_string() != backend
    {
        return Err(MainError::InvalidAdmissionIssuer);
    }
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

#[derive(Debug, Error)]
enum MainError {
    #[error("session authority host failed: {0}")]
    SessionAuthority(
        #[from] heterarchy_orchestration_backend::session_process::SessionAuthorityProcessError,
    ),
    #[error("token command failed: {0}")]
    Token(#[from] heterarchy_orchestration_backend::token_cli::TokenCliError),
    #[error("unexpected argument {0:?}")]
    UnexpectedArgument(String),
    #[error("HETERARCHY_ORCHESTRATION_DATA_DIR is required")]
    MissingDataRoot,
    #[error("HETERARCHY_ORCHESTRATION_BACKEND_ADDR is invalid")]
    InvalidListen,
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
}
