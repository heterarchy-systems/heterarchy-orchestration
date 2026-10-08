//! Bounded, host-trusted configuration for the Forge HTTP source authority.
//!
//! This adapter retains no source catalog or permission cache. It reads one
//! explicit configuration file and bearer file, validates the configured
//! Workspace references, and builds the existing bounded HTTP authority.

use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use forge_backend::ops::ScopeDto;
use forge_control::{ResolvedWorkspaceScope, WorkspaceLocator};
use forge_domain::{LocationId, LocationRevision, WorkspaceId, WorkspaceRevision};
use serde::Deserialize;
use thiserror::Error;

const MAX_CONFIG_BYTES: usize = 16 * 1024;
const MAX_BEARER_BYTES: usize = 4 * 1024;
const MAX_SCOPES: usize = 64;
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

/// Redacted failures while loading the trusted Forge source configuration.
///
/// Variants intentionally contain no paths, parser messages, or credential
/// material so callers can safely report them through ordinary diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ForgeSourceConfigError {
    /// The explicit configuration path was not absolute.
    #[error("Forge source configuration path must be absolute")]
    ConfigPathNotAbsolute,
    /// The configuration file could not be read.
    #[error("Forge source configuration could not be read")]
    ConfigRead,
    /// The configuration file exceeded its byte limit.
    #[error("Forge source configuration exceeds its size limit")]
    ConfigTooLarge,
    /// The configuration file was not UTF-8.
    #[error("Forge source configuration encoding is invalid")]
    ConfigEncoding,
    /// The configuration document did not match its strict schema.
    #[error("Forge source configuration document is invalid")]
    ConfigDocument,
    /// A configured Workspace scope failed domain validation.
    #[error("Forge source Workspace scope is invalid")]
    Scope,
    /// The bearer-file path was not absolute.
    #[error("Forge source bearer-file path must be absolute")]
    BearerPathNotAbsolute,
    /// The bearer file could not be read.
    #[error("Forge source bearer file could not be read")]
    BearerRead,
    /// The bearer file exceeded its byte limit.
    #[error("Forge source bearer file exceeds its size limit")]
    BearerTooLarge,
    /// The bearer file was not UTF-8.
    #[error("Forge source bearer encoding is invalid")]
    BearerEncoding,
    /// The bearer file contained more than one line or invalid framing.
    #[error("Forge source bearer framing is invalid")]
    BearerFraming,
    /// The configured address or bearer could not construct the HTTP client.
    #[error("Forge source HTTP client configuration is invalid")]
    HttpClient,
    /// The validated inputs could not construct the coordinator authority.
    #[error("Forge source coordinator authority configuration is invalid")]
    CoordinatorAuthority,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceConfig {
    address: SocketAddr,
    bearer_file: PathBuf,
    scopes: Vec<ScopeDto>,
    #[serde(default)]
    effect_facts: Option<EffectFactConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectFactConfig {
    backend_id: uuid::Uuid,
    destination_id: uuid::Uuid,
}

#[derive(Clone, Copy)]
enum ReadFailure {
    Io,
    TooLarge,
}

/// Load the explicit host-trusted Forge source configuration.
///
/// The configuration and bearer files must be absolute paths. The bearer is
/// read only while constructing the existing HTTP client and is not retained
/// by this module or included in its error/debug surface.
///
/// # Errors
/// Returns a closed, redacted error for path, size, framing, parsing, scope,
/// client, or authority validation failures.
pub fn load(path: &Path) -> Result<crate::CoordinatorAuthority, ForgeSourceConfigError> {
    if !path.is_absolute() {
        return Err(ForgeSourceConfigError::ConfigPathNotAbsolute);
    }

    let config_bytes = read_bounded(path, MAX_CONFIG_BYTES).map_err(|failure| match failure {
        ReadFailure::Io => ForgeSourceConfigError::ConfigRead,
        ReadFailure::TooLarge => ForgeSourceConfigError::ConfigTooLarge,
    })?;
    let config_text =
        std::str::from_utf8(&config_bytes).map_err(|_| ForgeSourceConfigError::ConfigEncoding)?;
    let config: SourceConfig =
        serde_json::from_str(config_text).map_err(|_| ForgeSourceConfigError::ConfigDocument)?;

    if !config.bearer_file.is_absolute() {
        return Err(ForgeSourceConfigError::BearerPathNotAbsolute);
    }
    let scopes = validated_scopes(config.scopes)?;

    let bearer_bytes =
        read_bounded(&config.bearer_file, MAX_BEARER_BYTES).map_err(|failure| match failure {
            ReadFailure::Io => ForgeSourceConfigError::BearerRead,
            ReadFailure::TooLarge => ForgeSourceConfigError::BearerTooLarge,
        })?;
    let bearer = bearer_text(&bearer_bytes)?;

    let commands =
        crate::forge_commands::ForgeCommandHttpClient::new(config.address, bearer, HTTP_TIMEOUT)
            .map_err(|_| ForgeSourceConfigError::HttpClient)?;
    let source = crate::forge_source::ForgeSourceHttpClient::new(commands);
    let authority = crate::CoordinatorAuthority::from_http(source, scopes)
        .map_err(|_| ForgeSourceConfigError::CoordinatorAuthority)?;
    if let Some(identity) = config.effect_facts {
        let client = forge_backend::ForgeHttpClient::new(
            config.address,
            bearer,
            forge_domain::ForgeBackendId::from_uuid(identity.backend_id),
            identity.destination_id,
            HTTP_TIMEOUT,
        )
        .map_err(|_| ForgeSourceConfigError::HttpClient)?;
        Ok(authority.with_effect_facts(client))
    } else {
        Ok(authority)
    }
}

fn validated_scopes(
    source_scopes: Vec<ScopeDto>,
) -> Result<Vec<ResolvedWorkspaceScope>, ForgeSourceConfigError> {
    if source_scopes.len() > MAX_SCOPES {
        return Err(ForgeSourceConfigError::Scope);
    }

    let mut scopes = Vec::with_capacity(source_scopes.len());
    let mut workspace_ids = Vec::with_capacity(source_scopes.len());
    for scope in source_scopes {
        let workspace_id = scope.workspace_id;
        let location_id = scope.locator.location_id;
        if workspace_id.is_nil() || location_id.is_nil() || workspace_ids.contains(&workspace_id) {
            return Err(ForgeSourceConfigError::Scope);
        }

        let workspace_revision = WorkspaceRevision::new(scope.workspace_revision)
            .map_err(|_| ForgeSourceConfigError::Scope)?;
        let location_revision = LocationRevision::new(scope.locator.expected_revision)
            .map_err(|_| ForgeSourceConfigError::Scope)?;
        let locator = WorkspaceLocator::new(
            LocationId::from_uuid(location_id),
            location_revision,
            Path::new(&scope.locator.relative_root),
        )
        .map_err(|_| ForgeSourceConfigError::Scope)?;

        scopes.push(ResolvedWorkspaceScope::from_observation(
            WorkspaceId::from_uuid(workspace_id),
            workspace_revision,
            locator,
        ));
        workspace_ids.push(workspace_id);
    }

    Ok(scopes)
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, ReadFailure> {
    let read_limit = u64::try_from(maximum)
        .ok()
        .and_then(|maximum| maximum.checked_add(1))
        .ok_or(ReadFailure::TooLarge)?;
    let mut bytes = Vec::with_capacity(maximum.saturating_add(1));
    File::open(path)
        .map_err(|_| ReadFailure::Io)?
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|_| ReadFailure::Io)?;
    if bytes.len() > maximum {
        return Err(ReadFailure::TooLarge);
    }
    Ok(bytes)
}

fn bearer_text(bytes: &[u8]) -> Result<&str, ForgeSourceConfigError> {
    let framed = if let Some(content) = bytes.strip_suffix(b"\r\n") {
        content
    } else if let Some(content) = bytes.strip_suffix(b"\n") {
        content
    } else {
        bytes
    };
    if framed.contains(&b'\n') || framed.contains(&b'\r') {
        return Err(ForgeSourceConfigError::BearerFraming);
    }
    std::str::from_utf8(framed).map_err(|_| ForgeSourceConfigError::BearerEncoding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::error::Error as StdError;
    use uuid::Uuid;

    const SECRET: &str = "bearer-secret-must-not-appear-in-debug";
    type TestResult = Result<(), Box<dyn StdError>>;

    struct Fixture {
        directory: PathBuf,
        config_path: PathBuf,
        bearer_path: PathBuf,
    }

    impl Fixture {
        fn new(bearer: &[u8]) -> Result<Self, Box<dyn StdError>> {
            let directory = std::env::temp_dir().join(format!(
                "orchestration-forge-source-config-{}",
                Uuid::now_v7()
            ));
            std::fs::create_dir_all(&directory)?;
            let config_path = directory.join("source.json");
            let bearer_path = directory.join("bearer.txt");
            std::fs::write(&bearer_path, bearer)?;
            Ok(Self {
                directory,
                config_path,
                bearer_path,
            })
        }

        fn document(&self, scopes: &[Value]) -> Result<Value, Box<dyn StdError>> {
            let bearer_file = serde_json::to_value(&self.bearer_path)?;
            Ok(json!({
                "address": "127.0.0.1:1",
                "bearer_file": bearer_file,
                "scopes": scopes,
            }))
        }

        fn write_document(&self, document: &Value) -> Result<(), Box<dyn StdError>> {
            std::fs::write(&self.config_path, serde_json::to_vec(document)?)?;
            Ok(())
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    fn scope(workspace_id: Uuid, location_id: Uuid, relative_root: &str) -> Value {
        json!({
            "workspace_id": workspace_id,
            "workspace_revision": 1,
            "locator": {
                "location_id": location_id,
                "expected_revision": 1,
                "relative_root": relative_root,
            }
        })
    }

    fn valid_scopes() -> Vec<Value> {
        vec![scope(Uuid::from_u128(1), Uuid::from_u128(2), ".")]
    }

    fn assert_load_error(
        fixture: &Fixture,
        document: &Value,
        expected: ForgeSourceConfigError,
    ) -> TestResult {
        fixture.write_document(document)?;
        assert_eq!(load(&fixture.config_path).err(), Some(expected));
        Ok(())
    }

    #[test]
    fn loads_multiline_json_and_redacts_the_bearer_from_debug() -> TestResult {
        let bearer = format!("{SECRET}\r\n");
        let fixture = Fixture::new(bearer.as_bytes())?;
        let document = fixture.document(&valid_scopes())?;
        let pretty_json = serde_json::to_string_pretty(&document)?;
        std::fs::write(&fixture.config_path, format!("\n{pretty_json}\n \t"))?;

        let authority = load(&fixture.config_path)?;
        assert!(!format!("{authority:?}").contains(SECRET));
        Ok(())
    }

    #[test]
    fn effect_fact_client_requires_explicit_valid_pinned_identities() -> TestResult {
        let fixture = Fixture::new(SECRET.as_bytes())?;
        let mut document = fixture.document(&valid_scopes())?;
        document["effect_facts"] =
            json!({"backend_id": Uuid::from_u128(3), "destination_id": Uuid::from_u128(4)});
        fixture.write_document(&document)?;
        let authority = load(&fixture.config_path)?;
        assert!(!format!("{authority:?}").contains(SECRET));
        document["effect_facts"]["backend_id"] = json!(Uuid::nil());
        assert_load_error(&fixture, &document, ForgeSourceConfigError::HttpClient)?;
        document["effect_facts"]["backend_id"] = json!(Uuid::from_u128(3));
        document["effect_facts"]["destination_id"] = json!(Uuid::nil());
        assert_load_error(&fixture, &document, ForgeSourceConfigError::HttpClient)?;
        Ok(())
    }

    #[test]
    fn rejects_trailing_json_documents_and_relative_paths() -> TestResult {
        let fixture = Fixture::new(SECRET.as_bytes())?;
        let document = fixture.document(&valid_scopes())?;
        let compact_json = serde_json::to_string(&document)?;
        std::fs::write(
            &fixture.config_path,
            format!("{compact_json}\n{compact_json}"),
        )?;
        assert_eq!(
            load(&fixture.config_path).err(),
            Some(ForgeSourceConfigError::ConfigDocument)
        );

        assert_eq!(
            load(Path::new("relative-source.json")).err(),
            Some(ForgeSourceConfigError::ConfigPathNotAbsolute)
        );

        let mut document = fixture.document(&valid_scopes())?;
        document["bearer_file"] = json!("relative-bearer.txt");
        assert_load_error(
            &fixture,
            &document,
            ForgeSourceConfigError::BearerPathNotAbsolute,
        )?;
        Ok(())
    }

    #[test]
    fn rejects_unknown_fields_and_invalid_addresses() -> TestResult {
        let fixture = Fixture::new(SECRET.as_bytes())?;
        let mut document = fixture.document(&valid_scopes())?;
        document["backend_id"] = json!(Uuid::from_u128(3));
        assert_load_error(&fixture, &document, ForgeSourceConfigError::ConfigDocument)?;

        let mut document = fixture.document(&valid_scopes())?;
        document["scopes"][0]["unexpected"] = json!(true);
        assert_load_error(&fixture, &document, ForgeSourceConfigError::ConfigDocument)?;

        let mut document = fixture.document(&valid_scopes())?;
        document["address"] = json!("not-a-socket-address");
        assert_load_error(&fixture, &document, ForgeSourceConfigError::ConfigDocument)?;

        let mut document = fixture.document(&valid_scopes())?;
        document["address"] = json!("192.0.2.1:8080");
        assert_load_error(&fixture, &document, ForgeSourceConfigError::HttpClient)?;
        Ok(())
    }

    #[test]
    fn rejects_non_utf8_json_and_bearer_and_additional_bearer_lines() -> TestResult {
        let fixture = Fixture::new(SECRET.as_bytes())?;
        std::fs::write(&fixture.config_path, [0xff])?;
        assert_eq!(
            load(&fixture.config_path).err(),
            Some(ForgeSourceConfigError::ConfigEncoding)
        );

        let fixture = Fixture::new(&[0xff])?;
        fixture.write_document(&fixture.document(&valid_scopes())?)?;
        assert_eq!(
            load(&fixture.config_path).err(),
            Some(ForgeSourceConfigError::BearerEncoding)
        );

        let bearer = format!("{SECRET}\n\n");
        let fixture = Fixture::new(bearer.as_bytes())?;
        fixture.write_document(&fixture.document(&valid_scopes())?)?;
        assert_eq!(
            load(&fixture.config_path).err(),
            Some(ForgeSourceConfigError::BearerFraming)
        );
        Ok(())
    }

    #[test]
    fn enforces_file_size_limits() -> TestResult {
        let fixture = Fixture::new(SECRET.as_bytes())?;
        std::fs::write(&fixture.config_path, vec![b' '; MAX_CONFIG_BYTES + 1])?;
        assert_eq!(
            load(&fixture.config_path).err(),
            Some(ForgeSourceConfigError::ConfigTooLarge)
        );

        let oversized_bearer = [b'x'; MAX_BEARER_BYTES + 1];
        let fixture = Fixture::new(&oversized_bearer)?;
        fixture.write_document(&fixture.document(&valid_scopes())?)?;
        assert_eq!(
            load(&fixture.config_path).err(),
            Some(ForgeSourceConfigError::BearerTooLarge)
        );
        Ok(())
    }

    #[test]
    fn rejects_nil_and_duplicate_workspace_or_location_identities() -> TestResult {
        let fixture = Fixture::new(SECRET.as_bytes())?;
        let nil = fixture.document(&[scope(Uuid::nil(), Uuid::from_u128(2), ".")])?;
        assert_load_error(&fixture, &nil, ForgeSourceConfigError::Scope)?;

        let nil_location = fixture.document(&[scope(Uuid::from_u128(1), Uuid::nil(), ".")])?;
        assert_load_error(&fixture, &nil_location, ForgeSourceConfigError::Scope)?;

        let duplicate = fixture.document(&[
            scope(Uuid::from_u128(1), Uuid::from_u128(2), "."),
            scope(Uuid::from_u128(1), Uuid::from_u128(3), "workspace"),
        ])?;
        assert_load_error(&fixture, &duplicate, ForgeSourceConfigError::Scope)?;
        Ok(())
    }

    #[test]
    fn rejects_nonpositive_revisions_invalid_paths_and_oversized_scope_sets() -> TestResult {
        let fixture = Fixture::new(SECRET.as_bytes())?;
        let mut workspace_revision = valid_scopes();
        workspace_revision[0]["workspace_revision"] = json!(0);
        assert_load_error(
            &fixture,
            &fixture.document(&workspace_revision)?,
            ForgeSourceConfigError::Scope,
        )?;

        let mut location_revision = valid_scopes();
        location_revision[0]["locator"]["expected_revision"] = json!(0);
        assert_load_error(
            &fixture,
            &fixture.document(&location_revision)?,
            ForgeSourceConfigError::Scope,
        )?;

        let invalid_path =
            fixture.document(&[scope(Uuid::from_u128(1), Uuid::from_u128(2), "../outside")])?;
        assert_load_error(&fixture, &invalid_path, ForgeSourceConfigError::Scope)?;

        let upper = u64::try_from(MAX_SCOPES)?;
        let too_many: Vec<Value> = (0..=upper)
            .map(|index| {
                scope(
                    Uuid::from_u128(u128::from(index + 1)),
                    Uuid::from_u128(u128::from(index + 100)),
                    ".",
                )
            })
            .collect();
        assert_load_error(
            &fixture,
            &fixture.document(&too_many)?,
            ForgeSourceConfigError::Scope,
        )?;
        Ok(())
    }
}
