//! Wire contracts for the command/diff/writer-lease source reads:
//! `command.list`, `diff.read`, `writer_lease.get`, and `writer_lease.list`.
//!
//! `diff.list`, `diff_doctor`, and `diff_artifact_doctor` intentionally have
//! no contract on this surface: no configured Forge HTTP contract exists, so
//! they stay fail-closed instead of approximating one. Responses are bounded
//! source observations and never grant mutation or lease rights.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::super::leadership::WorkspaceQueryScope;
use super::workspace::SourceScopeWire;

/// `POST /internal/v1/workspaces/{workspace_id}/forge/command-list` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandListQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/command-list` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandListQueryResponse {
    pub activity_id: String,
    pub catalog_fingerprint: String,
    pub commands: Vec<ProjectCommandWire>,
}

/// One discovered project command.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectCommandWire {
    pub command_id: String,
    pub class: ProjectCommandClassWire,
}

/// The closed project-command class vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectCommandClassWire {
    Verify,
    GeneratedPackage,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/diff-read` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffReadQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub diff_id: Uuid,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/diff-read` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffReadQueryResponse {
    pub scope: SourceScopeWire,
    pub diff: DiffEvidenceWire,
    pub health: DiffArtifactHealthWire,
    pub content: Option<String>,
}

/// One diff evidence row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffEvidenceWire {
    pub diff_id: String,
    pub workspace_id: String,
    pub activity_id: String,
    pub change_set_id: String,
    pub origin: DiffOriginWire,
    pub mutation_id: Option<String>,
    pub entries: Vec<DiffEntryWire>,
    pub state: DiffEvidenceStateWire,
    pub render_kind: DiffRenderKindWire,
    pub compressed_hash: Option<String>,
    pub uncompressed_hash: Option<String>,
    pub compressed_size_bytes: Option<u64>,
    pub uncompressed_size_bytes: Option<u64>,
    pub renderer_version: u32,
    pub redaction_policy_version: u32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub revision: i64,
}

/// One diff entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffEntryWire {
    pub subject: DiffEntrySubjectWire,
    pub primary_path: String,
    pub secondary_path: Option<String>,
    pub change_kind: DiffChangeKindWire,
    pub additions: u32,
    pub deletions: u32,
}

/// The closed diff-origin vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffOriginWire {
    FileMutation,
    TreeChangeSet,
}

/// The closed diff entry subject vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffEntrySubjectWire {
    File,
    Directory,
}

/// The closed diff change-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffChangeKindWire {
    Created,
    Modified,
    Deleted,
    Moved,
}

/// The closed diff evidence state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffEvidenceStateWire {
    Intent,
    Ready,
    SummaryOnly,
    Abandoned,
    PurgePending,
    Tombstoned,
}

/// The closed diff render-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffRenderKindWire {
    Text,
    Binary,
    Oversized,
    Structured,
}

/// The closed diff artifact health vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffArtifactHealthWire {
    Available,
    NotApplicable,
    Archived,
    Missing,
    Corrupt,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/diff-list` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffListQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
    pub limit: u32,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/diff-list` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffListQueryResponse {
    pub scope: SourceScopeWire,
    pub activity_id: String,
    pub diffs: Vec<DiffEvidenceWire>,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/diff-doctor` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffDoctorQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub diff_id: Uuid,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/diff-doctor` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffDoctorQueryResponse {
    pub scope: SourceScopeWire,
    pub diff_id: String,
    pub health: DiffArtifactHealthWire,
    pub evidence: DiffEvidenceWire,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/writer-lease-get`
/// request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WriterLeaseGetQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub lease_id: Uuid,
    pub scope_offset: u64,
    pub expected_revision: Option<i64>,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/writer-lease-get`
/// response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WriterLeaseGetQueryResponse {
    pub lease: Option<WriterLeaseProjectionWire>,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/writer-lease-list`
/// request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WriterLeaseListQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub limit: u64,
    pub cursor: Option<Uuid>,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/writer-lease-list`
/// response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WriterLeaseListQueryResponse {
    pub leases: Vec<WriterLeaseProjectionWire>,
    pub limit: u64,
    pub truncated: bool,
    pub next_cursor: Option<String>,
}

/// One full Writer Lease projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WriterLeaseProjectionWire {
    pub lease_id: String,
    pub owner_activity_id: String,
    pub agent_id: String,
    pub owner_session_id: Option<String>,
    pub revision: i64,
    pub lease_until_ms: i64,
    pub observed_at_ms: i64,
    pub state: WriterLeaseStateWire,
    pub status: WriterLeaseStatusWire,
    pub blocking_reason: Option<String>,
    pub recovery_action: String,
    pub total_scope_count: u64,
    pub scope_offset: u64,
    pub next_scope_offset: Option<u64>,
    pub scope_page_limit: u64,
    pub scopes: Vec<WriterLeaseScopeWire>,
}

/// One Writer Lease scope entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WriterLeaseScopeWire {
    pub kind: WriterLeaseScopeKindWire,
    pub path_base64: String,
    pub display_path: String,
    pub display_path_truncated: bool,
}

/// The closed Writer Lease state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterLeaseStateWire {
    Active,
    Released,
}

/// The closed derived Writer Lease status vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterLeaseStatusWire {
    Active,
    ExpiredRecoveryRequired,
    Released,
}

/// The closed Writer Lease scope-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterLeaseScopeKindWire {
    ExactPath,
    DirectorySubtree,
}
