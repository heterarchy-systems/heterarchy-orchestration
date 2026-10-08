//! Wire contracts for the confined file source reads: `file.list`,
//! `text.search`, and `file.read_slice`.
//!
//! Path fields carry the canonical URL-safe unpadded base64 tokens the
//! established surface uses; plaintext paths are invalid here. Responses
//! are bounded source observations and never grant mutation rights.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::super::leadership::WorkspaceQueryScope;
use super::workspace::SourceScopeWire;

/// `POST /internal/v1/workspaces/{workspace_id}/forge/file-list` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileListQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
    pub directory: Option<String>,
    pub max_results: u32,
    pub max_output_bytes: u32,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/file-list` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileListQueryResponse {
    pub scope: SourceScopeWire,
    pub activity_id: String,
    pub directory: Option<String>,
    pub directory_display_path: Option<String>,
    pub directory_display_path_truncated: bool,
    pub entries: Vec<FileEntryWire>,
    pub scanned_entries: u64,
    pub truncated: bool,
}

/// One bounded directory-listing entry.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileEntryWire {
    pub path: String,
    pub display_path: String,
    pub display_path_truncated: bool,
    pub kind: FileEntryKindWire,
    pub size_bytes: Option<u64>,
}

/// The closed file entry-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FileEntryKindWire {
    Directory,
    RegularFile,
    SymbolicLink,
    Other,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/text-search` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TextSearchQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
    pub root: Option<String>,
    pub query: String,
    pub max_results: u32,
    pub max_output_bytes: u32,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/text-search` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TextSearchQueryResponse {
    pub scope: SourceScopeWire,
    pub activity_id: String,
    pub root: Option<String>,
    pub root_display_path: Option<String>,
    pub root_display_path_truncated: bool,
    pub matches: Vec<TextSearchMatchWire>,
    pub scanned_entries: u64,
    pub scanned_files: u64,
    pub truncated: bool,
}

/// One bounded literal-search match.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TextSearchMatchWire {
    pub path: String,
    pub display_path: String,
    pub display_path_truncated: bool,
    pub line: u64,
    pub preview: String,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/file-read-slice`
/// request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileReadSliceQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
    pub path: String,
    pub start_line: u64,
    pub end_line: u64,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/file-read-slice`
/// response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileReadSliceQueryResponse {
    pub scope: SourceScopeWire,
    pub activity_id: String,
    pub path: String,
    pub display_path: String,
    pub display_path_truncated: bool,
    pub content: String,
    pub whole_file_hash: String,
    pub range_hash: String,
    pub line_hashes: Vec<String>,
    pub start_line: u64,
    pub end_line: u64,
    pub total_lines: u64,
    pub eol: SourceEolWire,
    pub has_final_newline: bool,
}

/// The closed source end-of-line vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceEolWire {
    Lf,
    CrLf,
    Cr,
    Mixed,
    None,
}
