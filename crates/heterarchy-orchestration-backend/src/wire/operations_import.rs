//! Operator-only source import contracts.

use heterarchy_orchestration_operations::{ImportHandoffSeal, ImportPhase, ImportProgress};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

pub use forge_backend::ops::agent_operations_migration::ExportCursorWire;

/// Prepare or exactly replay one destination import from a trusted Forge
/// freeze observation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportPrepareRequest {
    pub scope: WorkspaceQueryScope,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
}

/// Advance one bounded import/verification/abort operation under a full
/// progress-count fence. The migration identity is carried by the URL.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportStepRequest {
    pub scope: WorkspaceQueryScope,
    pub destination_id: Uuid,
    pub expected_pages: u64,
    pub expected_verified_pages: u64,
}

/// Closed durable import phase projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportPhaseWire {
    Importing,
    Imported,
    Verified,
    Aborted,
}

impl From<ImportPhase> for ImportPhaseWire {
    fn from(value: ImportPhase) -> Self {
        match value {
            ImportPhase::Importing => Self::Importing,
            ImportPhase::Imported => Self::Imported,
            ImportPhase::Verified => Self::Verified,
            ImportPhase::Aborted => Self::Aborted,
        }
    }
}

/// Exact durable progress returned by every import operation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportProgressWire {
    pub migration_id: Uuid,
    pub workspace_id: Uuid,
    pub destination_id: Uuid,
    pub source_revision: u64,
    pub phase: ImportPhaseWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_cursor: Option<ExportCursorWire>,
    pub page_count: u64,
    pub record_count: u64,
    pub import_digest: String,
    pub verified_pages: u64,
    pub verify_digest: String,
}

/// Progress readback preserves explicit absence for an unknown migration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportProgressResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub progress: Option<ImportProgressWire>,
}

/// Caller-saved import revision and digest are equality expectations only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportHandoffSealRequest {
    pub scope: WorkspaceQueryScope,
    pub destination_id: Uuid,
    pub expected_source_revision: u64,
    pub expected_verified_digest: String,
}

/// Exact Workspace/destination selector for pure handoff and current-import reads.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportHandoffQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub destination_id: Uuid,
}

/// Source-derived immutable proof for one verified import handoff.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportHandoffSealWire {
    pub migration_id: Uuid,
    pub workspace_id: Uuid,
    pub destination_id: Uuid,
    pub source_revision: u64,
    pub verified_digest: String,
    pub page_count: u64,
    pub record_count: u64,
    pub verified_pages: u64,
    pub issuer_id: Uuid,
    pub backend_id: Uuid,
    pub sealed_at_ms: i64,
}

impl From<&ImportHandoffSeal> for ImportHandoffSealWire {
    fn from(value: &ImportHandoffSeal) -> Self {
        Self {
            migration_id: value.migration_id(),
            workspace_id: value.workspace_id().into_uuid(),
            destination_id: value.destination_id(),
            source_revision: value.source_revision(),
            verified_digest: digest_hex(&value.verified_digest()),
            page_count: value.page_count(),
            record_count: value.record_count(),
            verified_pages: value.verified_pages(),
            issuer_id: value.issuer_id().into_uuid(),
            backend_id: value.backend_id().into_uuid(),
            sealed_at_ms: value.sealed_at_ms(),
        }
    }
}

/// Non-null seal result returned by successful or exact replay.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportHandoffSealResponse {
    pub handoff_seal: ImportHandoffSealWire,
}

/// Exact seal readback preserves explicit absence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportHandoffReadbackResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub handoff_seal: Option<ImportHandoffSealWire>,
}

/// Current Workspace import and optional seal, with absence explicit for both.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CurrentImportHandoffResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub progress: Option<ImportProgressWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub handoff_seal: Option<ImportHandoffSealWire>,
}

impl From<&ImportProgress> for ImportProgressWire {
    fn from(value: &ImportProgress) -> Self {
        Self {
            migration_id: value.migration_id(),
            workspace_id: value.workspace_id().into_uuid(),
            destination_id: value.destination_id(),
            source_revision: value.source_revision(),
            phase: value.phase().into(),
            next_cursor: value.next_cursor().map(ExportCursorWire::from),
            page_count: value.page_count(),
            record_count: value.record_count(),
            import_digest: digest_hex(&value.digest()),
            verified_pages: value.verified_pages(),
            verify_digest: digest_hex(&value.verify_digest()),
        }
    }
}

fn digest_hex(digest: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_response_requires_explicit_nullable_progress() {
        assert!(serde_json::from_str::<ImportProgressResponse>("{}").is_err());
        assert!(serde_json::from_str::<ImportProgressResponse>(r#"{"progress":null}"#).is_ok());
    }

    #[test]
    fn handoff_readbacks_require_each_nullable_field_and_use_camel_case()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(serde_json::from_str::<ImportHandoffReadbackResponse>("{}").is_err());
        let readback =
            serde_json::from_str::<ImportHandoffReadbackResponse>(r#"{"handoffSeal":null}"#)?;
        assert!(readback.handoff_seal.is_none());

        assert!(
            serde_json::from_str::<CurrentImportHandoffResponse>(r#"{"progress":null}"#).is_err()
        );
        assert!(
            serde_json::from_str::<CurrentImportHandoffResponse>(r#"{"handoffSeal":null}"#)
                .is_err()
        );
        let current = serde_json::from_str::<CurrentImportHandoffResponse>(
            r#"{"progress":null,"handoffSeal":null}"#,
        )?;
        assert!(current.progress.is_none());
        assert!(current.handoff_seal.is_none());

        let request = ImportHandoffSealRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            destination_id: Uuid::from_u128(5),
            expected_source_revision: 6,
            expected_verified_digest: "11".repeat(32),
        };
        let encoded = serde_json::to_value(request)?;
        assert_eq!(encoded["expectedSourceRevision"], 6);
        assert!(encoded.get("expectedVerifiedDigest").is_some());
        assert!(encoded.get("expected_source_revision").is_none());

        let seal = serde_json::to_value(ImportHandoffSealWire {
            migration_id: Uuid::from_u128(7),
            workspace_id: Uuid::from_u128(8),
            destination_id: Uuid::from_u128(9),
            source_revision: 10,
            verified_digest: "22".repeat(32),
            page_count: 11,
            record_count: 12,
            verified_pages: 11,
            issuer_id: Uuid::from_u128(13),
            backend_id: Uuid::from_u128(14),
            sealed_at_ms: 15,
        })?;
        assert_eq!(seal["verifiedPages"], 11);
        assert_eq!(seal["sourceRevision"], 10);
        assert!(seal.get("verified_pages").is_none());
        Ok(())
    }
}
