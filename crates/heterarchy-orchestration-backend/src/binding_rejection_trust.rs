//! Trusted local allowlist for the bounded binding-rejection repair.

use std::fs::{self, File, Metadata};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

use heterarchy_orchestration_operations::{
    BindingRejectionSourceRequestPolicy, LegacyBindingRejectionEnvelopePolicy,
    TrustedLegacyBindingRejectionDeployment,
};
use serde::Deserialize;
use thiserror::Error;

const MAX_MANIFEST_BYTES: usize = 8 * 1024;
const MAX_DEPLOYMENTS: usize = 8;

/// Redacted failures while loading the local deployment allowlist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BindingRejectionTrustError {
    #[error("binding rejection trust manifest path must be absolute")]
    PathNotAbsolute,
    #[error("binding rejection trust manifest is unavailable")]
    Unavailable,
    #[error("binding rejection trust manifest permissions are unsafe")]
    UnsafeFile,
    #[error("binding rejection trust manifest exceeds its size limit")]
    TooLarge,
    #[error("binding rejection trust manifest encoding is invalid")]
    Encoding,
    #[error("binding rejection trust manifest document is invalid")]
    Document,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustManifest {
    schema_version: u8,
    legacy_deployments: Vec<DeploymentEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentEntry {
    artifact_sha256: String,
    envelope_request_id_policy: EnvelopePolicy,
    durable_request_id_policy: DurableRequestPolicy,
    source_request_id_policy: SourceRequestPolicy,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum EnvelopePolicy {
    GeneratedUuidV7,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum DurableRequestPolicy {
    PersistedUuidV8,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SourceRequestPolicy {
    BodyEqualsEnvelope,
}

/// Load one strict, bounded, local deployment allowlist.
///
/// The file must be an absolute, regular, non-symlink path and must not be
/// writable by group or other users. The document contains policy evidence,
/// not a caller-controlled feature flag.
///
/// # Errors
/// Returns a redacted error for unsafe files, malformed documents, unknown
/// policy values, duplicate artifacts, or out-of-bounds input.
pub fn load(
    path: &Path,
    owner_root: &Path,
) -> Result<Vec<TrustedLegacyBindingRejectionDeployment>, BindingRejectionTrustError> {
    if !path.is_absolute() || !owner_root.is_absolute() {
        return Err(BindingRejectionTrustError::PathNotAbsolute);
    }
    let path_metadata =
        fs::symlink_metadata(path).map_err(|_| BindingRejectionTrustError::Unavailable)?;
    let owner_metadata =
        fs::symlink_metadata(owner_root).map_err(|_| BindingRejectionTrustError::Unavailable)?;
    if owner_metadata.file_type().is_symlink()
        || !owner_metadata.is_dir()
        || owner_metadata.permissions().mode() & 0o022 != 0
        || path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || path_metadata.permissions().mode() & 0o022 != 0
        || path_metadata.uid() != owner_metadata.uid()
    {
        return Err(BindingRejectionTrustError::UnsafeFile);
    }
    let file = File::open(path).map_err(|_| BindingRejectionTrustError::Unavailable)?;
    let opened_metadata = file
        .metadata()
        .map_err(|_| BindingRejectionTrustError::Unavailable)?;
    let current_path_metadata =
        fs::symlink_metadata(path).map_err(|_| BindingRejectionTrustError::Unavailable)?;
    if !same_file(&path_metadata, &opened_metadata)
        || !same_file(&opened_metadata, &current_path_metadata)
        || current_path_metadata.file_type().is_symlink()
        || !opened_metadata.is_file()
        || opened_metadata.permissions().mode() & 0o022 != 0
        || opened_metadata.uid() != owner_metadata.uid()
    {
        return Err(BindingRejectionTrustError::UnsafeFile);
    }
    if opened_metadata.len() > MAX_MANIFEST_BYTES as u64 {
        return Err(BindingRejectionTrustError::TooLarge);
    }
    let read_limit =
        u64::try_from(MAX_MANIFEST_BYTES + 1).map_err(|_| BindingRejectionTrustError::TooLarge)?;
    let mut bytes = Vec::with_capacity(MAX_MANIFEST_BYTES + 1);
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|_| BindingRejectionTrustError::Unavailable)?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(BindingRejectionTrustError::TooLarge);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| BindingRejectionTrustError::Encoding)?;
    let manifest: TrustManifest =
        serde_json::from_str(text).map_err(|_| BindingRejectionTrustError::Document)?;
    if manifest.schema_version != 1
        || manifest.legacy_deployments.is_empty()
        || manifest.legacy_deployments.len() > MAX_DEPLOYMENTS
    {
        return Err(BindingRejectionTrustError::Document);
    }

    let mut deployments = Vec::with_capacity(manifest.legacy_deployments.len());
    for entry in manifest.legacy_deployments {
        let digest =
            parse_sha256(&entry.artifact_sha256).ok_or(BindingRejectionTrustError::Document)?;
        if deployments
            .iter()
            .any(|deployment: &TrustedLegacyBindingRejectionDeployment| {
                deployment.artifact_digest() == digest
            })
        {
            return Err(BindingRejectionTrustError::Document);
        }
        let envelope_policy = match entry.envelope_request_id_policy {
            EnvelopePolicy::GeneratedUuidV7 => {
                LegacyBindingRejectionEnvelopePolicy::GeneratedUuidV7
            }
        };
        let durable_uuid_version = match entry.durable_request_id_policy {
            DurableRequestPolicy::PersistedUuidV8 => 8,
        };
        let source_request_policy = match entry.source_request_id_policy {
            SourceRequestPolicy::BodyEqualsEnvelope => {
                BindingRejectionSourceRequestPolicy::BodyEqualsEnvelope
            }
        };
        deployments.push(
            TrustedLegacyBindingRejectionDeployment::new(
                digest,
                envelope_policy,
                durable_uuid_version,
                source_request_policy,
            )
            .map_err(|_| BindingRejectionTrustError::Document)?,
        );
    }
    Ok(deployments)
}

fn same_file(left: &Metadata, right: &Metadata) -> bool {
    left.dev() == right.dev() && left.ino() == right.ino()
}

fn parse_sha256(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (index, slot) in digest.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    (digest != [0; 32]).then_some(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use uuid::Uuid;

    const ARTIFACT: &str = "9288f5cf024fa285c2a8e8ff6ab28f947fcd0a76ba7c819bdc298496f3327e4b";

    fn manifest_path() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-binding-rejection-trust-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        Ok(root.join("manifest.json"))
    }

    fn write_manifest(
        path: &Path,
        artifact: &str,
        source_policy: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        fs::write(
            path,
            format!(
                r#"{{"schema_version":1,"legacy_deployments":[{{"artifact_sha256":"{artifact}","envelope_request_id_policy":"generated_uuid_v7","durable_request_id_policy":"persisted_uuid_v8","source_request_id_policy":"{source_policy}"}}]}}"#,
            ),
        )?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(())
    }

    #[test]
    fn loads_exact_closed_legacy_deployment_policy() -> Result<(), Box<dyn std::error::Error>> {
        let path = manifest_path()?;
        write_manifest(&path, ARTIFACT, "body_equals_envelope")?;
        let deployments = load(&path, path.parent().ok_or("parent")?)?;
        assert_eq!(deployments.len(), 1);
        assert_eq!(
            deployments[0].artifact_digest(),
            parse_sha256(ARTIFACT).ok_or("digest")?
        );
        assert_eq!(deployments[0].durable_uuid_version(), 8);
        fs::remove_dir_all(path.parent().ok_or("parent")?)?;
        Ok(())
    }

    #[test]
    fn rejects_unknown_policy_duplicate_digest_and_writable_manifest()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = manifest_path()?;
        write_manifest(&path, ARTIFACT, "legacy_absence_is_retryable")?;
        assert_eq!(
            load(&path, path.parent().ok_or("parent")?),
            Err(BindingRejectionTrustError::Document)
        );

        let entry = format!(
            r#"{{"artifact_sha256":"{ARTIFACT}","envelope_request_id_policy":"generated_uuid_v7","durable_request_id_policy":"persisted_uuid_v8","source_request_id_policy":"body_equals_envelope"}}"#,
        );
        fs::write(
            &path,
            format!(r#"{{"schema_version":1,"legacy_deployments":[{entry},{entry}]}}"#),
        )?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        assert_eq!(
            load(&path, path.parent().ok_or("parent")?),
            Err(BindingRejectionTrustError::Document)
        );

        fs::set_permissions(&path, fs::Permissions::from_mode(0o622))?;
        assert_eq!(
            load(&path, path.parent().ok_or("parent")?),
            Err(BindingRejectionTrustError::UnsafeFile)
        );
        fs::remove_dir_all(path.parent().ok_or("parent")?)?;
        Ok(())
    }

    #[test]
    fn rejects_symlink_oversize_unknown_field_and_unsafe_owner_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = manifest_path()?;
        let root = path.parent().ok_or("parent")?;
        write_manifest(&path, ARTIFACT, "body_equals_envelope")?;

        let link = root.join("manifest-link.json");
        symlink(&path, &link)?;
        assert_eq!(
            load(&link, root),
            Err(BindingRejectionTrustError::UnsafeFile)
        );

        fs::write(&path, vec![b' '; MAX_MANIFEST_BYTES + 1])?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        assert_eq!(load(&path, root), Err(BindingRejectionTrustError::TooLarge));

        fs::write(
            &path,
            format!(
                r#"{{"schema_version":1,"unexpected":true,"legacy_deployments":[{{"artifact_sha256":"{ARTIFACT}","envelope_request_id_policy":"generated_uuid_v7","durable_request_id_policy":"persisted_uuid_v8","source_request_id_policy":"body_equals_envelope"}}]}}"#,
            ),
        )?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        assert_eq!(load(&path, root), Err(BindingRejectionTrustError::Document));

        fs::set_permissions(root, fs::Permissions::from_mode(0o777))?;
        assert_eq!(
            load(&path, root),
            Err(BindingRejectionTrustError::UnsafeFile)
        );
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
