//! Operator-only source import contracts.

use heterarchy_orchestration_operations::{ImportPhase, ImportProgress};
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
}
