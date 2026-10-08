//! Source read observations for the confined file family: `file.list`,
//! `text.search`, and `file.read_slice`.
//!
//! The decoders replicate the exact acceptance class of the established
//! native/HTTP projections, including the canonical URL-safe unpadded
//! base64 relative-path tokens and the bounded lossy display paths. Paths
//! stay opaque bytes on this boundary; a decoded token never becomes a
//! permission, and read content carries no Writer Lease or mutation grant.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use forge_backend::ops::{CommandOp, LocatorDto, ScopeDto};
use forge_control::FileActivityScope;
use forge_domain::{
    FileListOptions, RuntimeInstanceId, SourceLineRange, TextSearchOptions, TextSearchQuery,
    TimestampMillis, WorkspaceRelativePath,
};
use serde_json::{Value, json};
use std::os::unix::ffi::OsStringExt;
use uuid::Uuid;

use super::super::{ForgeSourceError, ForgeSourceHttpClient, map_command_error};

/// The bounded lossy display path used by every established projection.
pub(super) const DISPLAY_PATH_MAX_CHARS: usize = 512;

/// One entry of a bounded directory listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeFileListEntry {
    path_token: String,
    display_path: String,
    display_path_truncated: bool,
    kind: ForgeFileEntryKind,
    size_bytes: Option<u64>,
}

impl ForgeFileListEntry {
    /// The canonical URL-safe unpadded base64 path token.
    #[must_use]
    pub fn path_token(&self) -> &str {
        &self.path_token
    }

    /// The bounded lossy display path.
    #[must_use]
    pub fn display_path(&self) -> &str {
        &self.display_path
    }

    /// Whether the display path was truncated.
    #[must_use]
    pub const fn display_path_truncated(&self) -> bool {
        self.display_path_truncated
    }

    /// The closed entry-kind vocabulary.
    #[must_use]
    pub const fn kind(&self) -> ForgeFileEntryKind {
        self.kind
    }

    /// The exact size in bytes, when the entry kind carries one.
    #[must_use]
    pub const fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }
}

/// The closed file entry-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeFileEntryKind {
    Directory,
    RegularFile,
    SymbolicLink,
    Other,
}

/// One bounded directory listing observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeFileList {
    scope: ForgeSourceScope,
    activity_id: String,
    directory_token: Option<String>,
    directory_display_path: Option<String>,
    directory_display_path_truncated: bool,
    entries: Vec<ForgeFileListEntry>,
    scanned_entries: u64,
    truncated: bool,
}

impl ForgeFileList {
    /// The exact source scope echoed by Forge.
    #[must_use]
    pub const fn scope(&self) -> &ForgeSourceScope {
        &self.scope
    }

    /// The exact Activity identity echoed by Forge.
    #[must_use]
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// The canonical directory token, when a subdirectory was listed.
    #[must_use]
    pub fn directory_token(&self) -> Option<&str> {
        self.directory_token.as_deref()
    }

    /// The bounded lossy display path of the listed directory.
    #[must_use]
    pub fn directory_display_path(&self) -> Option<&str> {
        self.directory_display_path.as_deref()
    }

    /// Whether the directory display path was truncated.
    #[must_use]
    pub const fn directory_display_path_truncated(&self) -> bool {
        self.directory_display_path_truncated
    }

    /// The bounded entry list.
    #[must_use]
    pub fn entries(&self) -> &[ForgeFileListEntry] {
        &self.entries
    }

    /// How many entries Forge scanned.
    #[must_use]
    pub const fn scanned_entries(&self) -> u64 {
        self.scanned_entries
    }

    /// Whether Forge truncated the listing.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

/// One bounded literal-search match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeTextSearchMatch {
    path_token: String,
    display_path: String,
    display_path_truncated: bool,
    line: u64,
    preview: String,
}

impl ForgeTextSearchMatch {
    /// The canonical URL-safe unpadded base64 path token.
    #[must_use]
    pub fn path_token(&self) -> &str {
        &self.path_token
    }

    /// The bounded lossy display path.
    #[must_use]
    pub fn display_path(&self) -> &str {
        &self.display_path
    }

    /// Whether the display path was truncated.
    #[must_use]
    pub const fn display_path_truncated(&self) -> bool {
        self.display_path_truncated
    }

    /// The one-based line number of the match.
    #[must_use]
    pub const fn line(&self) -> u64 {
        self.line
    }

    /// The bounded match preview.
    #[must_use]
    pub fn preview(&self) -> &str {
        &self.preview
    }
}

/// One bounded recursive literal-search observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeTextSearch {
    scope: ForgeSourceScope,
    activity_id: String,
    root_token: Option<String>,
    root_display_path: Option<String>,
    root_display_path_truncated: bool,
    matches: Vec<ForgeTextSearchMatch>,
    scanned_entries: u64,
    scanned_files: u64,
    truncated: bool,
}

impl ForgeTextSearch {
    /// The exact source scope echoed by Forge.
    #[must_use]
    pub const fn scope(&self) -> &ForgeSourceScope {
        &self.scope
    }

    /// The exact Activity identity echoed by Forge.
    #[must_use]
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// The canonical root token, when a subtree was searched.
    #[must_use]
    pub fn root_token(&self) -> Option<&str> {
        self.root_token.as_deref()
    }

    /// The bounded lossy display path of the search root.
    #[must_use]
    pub fn root_display_path(&self) -> Option<&str> {
        self.root_display_path.as_deref()
    }

    /// Whether the root display path was truncated.
    #[must_use]
    pub const fn root_display_path_truncated(&self) -> bool {
        self.root_display_path_truncated
    }

    /// The bounded match list.
    #[must_use]
    pub fn matches(&self) -> &[ForgeTextSearchMatch] {
        &self.matches
    }

    /// How many entries Forge scanned.
    #[must_use]
    pub const fn scanned_entries(&self) -> u64 {
        self.scanned_entries
    }

    /// How many files Forge scanned.
    #[must_use]
    pub const fn scanned_files(&self) -> u64 {
        self.scanned_files
    }

    /// Whether Forge truncated the search.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

/// One bounded source line-slice observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeFileReadSlice {
    scope: ForgeSourceScope,
    activity_id: String,
    path_token: String,
    display_path: String,
    display_path_truncated: bool,
    content: String,
    whole_file_hash: String,
    range_hash: String,
    line_hashes: Vec<String>,
    start_line: u64,
    end_line: u64,
    total_lines: u64,
    eol: ForgeSourceEol,
    has_final_newline: bool,
}

impl ForgeFileReadSlice {
    /// The exact source scope echoed by Forge.
    #[must_use]
    pub const fn scope(&self) -> &ForgeSourceScope {
        &self.scope
    }

    /// The exact Activity identity echoed by Forge.
    #[must_use]
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// The canonical URL-safe unpadded base64 path token.
    #[must_use]
    pub fn path_token(&self) -> &str {
        &self.path_token
    }

    /// The bounded lossy display path.
    #[must_use]
    pub fn display_path(&self) -> &str {
        &self.display_path
    }

    /// Whether the display path was truncated.
    #[must_use]
    pub const fn display_path_truncated(&self) -> bool {
        self.display_path_truncated
    }

    /// The slice content.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// The whole-file content hash echoed by Forge.
    #[must_use]
    pub fn whole_file_hash(&self) -> &str {
        &self.whole_file_hash
    }

    /// The requested-range content hash echoed by Forge.
    #[must_use]
    pub fn range_hash(&self) -> &str {
        &self.range_hash
    }

    /// The per-line hashes of the slice.
    #[must_use]
    pub fn line_hashes(&self) -> &[String] {
        &self.line_hashes
    }

    /// The one-based first line of the slice.
    #[must_use]
    pub const fn start_line(&self) -> u64 {
        self.start_line
    }

    /// The one-based last line of the slice.
    #[must_use]
    pub const fn end_line(&self) -> u64 {
        self.end_line
    }

    /// The total line count of the whole file.
    #[must_use]
    pub const fn total_lines(&self) -> u64 {
        self.total_lines
    }

    /// The closed end-of-line vocabulary.
    #[must_use]
    pub const fn eol(&self) -> ForgeSourceEol {
        self.eol
    }

    /// Whether the whole file ends with a final newline.
    #[must_use]
    pub const fn has_final_newline(&self) -> bool {
        self.has_final_newline
    }
}

/// The closed source end-of-line vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeSourceEol {
    Lf,
    CrLf,
    Cr,
    Mixed,
    None,
}

/// The exact source scope echoed by a file-family readback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeSourceScope {
    workspace_id: Uuid,
    workspace_revision: i64,
    locator_token: String,
}

impl ForgeSourceScope {
    /// The exact Workspace identity echoed by Forge.
    #[must_use]
    pub const fn workspace_id(&self) -> Uuid {
        self.workspace_id
    }

    /// The exact Workspace revision echoed by Forge.
    #[must_use]
    pub const fn workspace_revision(&self) -> i64 {
        self.workspace_revision
    }

    /// The canonical locator wire form echoed by Forge.
    #[must_use]
    pub fn locator_token(&self) -> &str {
        &self.locator_token
    }
}

impl ForgeSourceHttpClient {
    /// Observe one bounded directory listing (`file.list`).
    ///
    /// `directory_token` selects an optional Workspace-relative subdirectory
    /// by its canonical token. The activity attribution is the caller's own
    /// live-process identity; Forge revalidates it against durable state.
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn file_list(
        &self,
        scope: &ScopeDto,
        activity: &FileActivityScope,
        directory_token: Option<&str>,
        options: &FileListOptions,
    ) -> Result<ForgeFileList, ForgeSourceError> {
        let payload = file_payload(scope, activity, |object| {
            if let Some(token) = directory_token {
                object.insert(
                    "directory_base64".to_owned(),
                    Value::String(token.to_owned()),
                );
            }
            insert_bounds(
                object,
                max_results(options.max_results())?,
                options.max_output_bytes(),
            )?;
            Ok(())
        })?;
        let readback = self
            .commands()
            .command(CommandOp::FileList, payload)
            .await
            .map_err(map_command_error)?;
        file_list_from_wire(&readback)
    }

    /// Observe one bounded recursive literal search (`text.search`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn text_search(
        &self,
        scope: &ScopeDto,
        activity: &FileActivityScope,
        root_token: Option<&str>,
        query: &TextSearchQuery,
        options: &TextSearchOptions,
    ) -> Result<ForgeTextSearch, ForgeSourceError> {
        let payload = file_payload(scope, activity, |object| {
            if let Some(token) = root_token {
                object.insert("root_base64".to_owned(), Value::String(token.to_owned()));
            }
            object.insert("query".to_owned(), Value::String(query.as_str().to_owned()));
            insert_bounds(
                object,
                max_results(options.max_results())?,
                options.max_output_bytes(),
            )?;
            Ok(())
        })?;
        let readback = self
            .commands()
            .command(CommandOp::TextSearch, payload)
            .await
            .map_err(map_command_error)?;
        text_search_from_wire(&readback)
    }

    /// Observe one bounded source line slice (`file.read_slice`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn file_read_slice(
        &self,
        scope: &ScopeDto,
        activity: &FileActivityScope,
        path_token: &str,
        line_range: SourceLineRange,
    ) -> Result<ForgeFileReadSlice, ForgeSourceError> {
        let payload = file_payload(scope, activity, |object| {
            object.insert(
                "path_base64".to_owned(),
                Value::String(path_token.to_owned()),
            );
            object.insert("start_line".to_owned(), json!(line_range.start_line()));
            object.insert("end_line".to_owned(), json!(line_range.end_line()));
            Ok(())
        })?;
        let readback = self
            .commands()
            .command(CommandOp::FileReadSlice, payload)
            .await
            .map_err(map_command_error)?;
        file_read_slice_from_wire(&readback)
    }
}

/// Build the shared `{scope, activity, ...}` file-family payload.
fn file_payload(
    scope: &ScopeDto,
    activity: &FileActivityScope,
    fields: impl FnOnce(&mut serde_json::Map<String, Value>) -> Result<(), ForgeSourceError>,
) -> Result<Value, ForgeSourceError> {
    let mut object = serde_json::Map::new();
    object.insert("scope".to_owned(), scope_json(scope));
    object.insert(
        "activity".to_owned(),
        json!({
            "activity_id": activity.activity_id.into_uuid(),
            "runtime_instance_id": activity.runtime_instance_id.into_uuid(),
            "observed_at_ms": activity.observed_at.get(),
        }),
    );
    fields(&mut object)?;
    Ok(Value::Object(object))
}

fn scope_json(scope: &ScopeDto) -> Value {
    json!({
        "workspace_id": scope.workspace_id,
        "workspace_revision": scope.workspace_revision,
        "locator": {
            "location_id": scope.locator.location_id,
            "expected_revision": scope.locator.expected_revision,
            "relative_root": scope.locator.relative_root,
        },
    })
}

fn insert_bounds(
    object: &mut serde_json::Map<String, Value>,
    max_results: u32,
    max_output_bytes: usize,
) -> Result<(), ForgeSourceError> {
    let bounded = u32::try_from(max_output_bytes).map_err(|_| ForgeSourceError::InvalidScope)?;
    object.insert("max_results".to_owned(), json!(max_results));
    object.insert("max_output_bytes".to_owned(), json!(bounded));
    Ok(())
}

fn max_results(value: usize) -> Result<u32, ForgeSourceError> {
    u32::try_from(value).map_err(|_| ForgeSourceError::InvalidScope)
}

/// Build the exact Forge source scope DTO from canonical wire fields.
///
/// # Errors
/// Returns `InvalidScope` for a non-UTF-8 relative root or a non-positive
/// revision.
pub fn scope_dto(
    workspace_id: Uuid,
    workspace_revision: i64,
    location_id: Uuid,
    expected_revision: i64,
    relative_root: &str,
) -> Result<ScopeDto, ForgeSourceError> {
    if workspace_id.is_nil()
        || location_id.is_nil()
        || workspace_revision <= 0
        || expected_revision <= 0
    {
        return Err(ForgeSourceError::InvalidScope);
    }
    Ok(ScopeDto {
        workspace_id,
        workspace_revision,
        locator: LocatorDto {
            location_id,
            expected_revision,
            relative_root: relative_root.to_owned(),
        },
    })
}

/// The caller's live Activity attribution for one file-family read. This is
/// process identity metadata, never Workspace authority.
///
/// # Errors
/// Returns `InvalidScope` for a nil Activity identity or an invalid clock.
pub fn activity_scope(
    activity_id: Uuid,
    observed_at_ms: i64,
) -> Result<FileActivityScope, ForgeSourceError> {
    if activity_id.is_nil() {
        return Err(ForgeSourceError::InvalidScope);
    }
    let observed_at =
        TimestampMillis::new(observed_at_ms).map_err(|_| ForgeSourceError::InvalidScope)?;
    Ok(FileActivityScope {
        activity_id: forge_domain::ActivityId::from_uuid(activity_id),
        runtime_instance_id: RuntimeInstanceId::from_uuid(Uuid::now_v7()),
        observed_at,
    })
}

/// Decode one canonical URL-safe unpadded base64 relative-path token.
///
/// # Errors
/// Returns `Protocol` for malformed, non-canonical, or invalid path tokens.
pub fn decode_path_token(token: &str) -> Result<WorkspaceRelativePath, ForgeSourceError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| ForgeSourceError::Protocol)?;
    if URL_SAFE_NO_PAD.encode(&bytes) != token {
        return Err(ForgeSourceError::Protocol);
    }
    WorkspaceRelativePath::new(std::path::PathBuf::from(std::ffi::OsString::from_vec(
        bytes,
    )))
    .map_err(|_| ForgeSourceError::Protocol)
}

fn decode_optional_path_token(
    value: Option<&Value>,
) -> Result<Option<WorkspaceRelativePath>, ForgeSourceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(token)) => decode_path_token(token).map(Some),
        Some(_) => Err(ForgeSourceError::Protocol),
    }
}

/// The bounded lossy display path, matching the established projection.
#[must_use]
pub fn display_relative_path(path: &WorkspaceRelativePath) -> (String, bool) {
    let path = path.as_path().as_os_str().to_string_lossy();
    let mut display = String::with_capacity(path.len().min(DISPLAY_PATH_MAX_CHARS));
    let mut length = 0_usize;
    for character in path.chars() {
        if character.is_control() {
            for escaped in character.escape_default() {
                if !append_display_character(&mut display, &mut length, escaped) {
                    return (display, true);
                }
            }
        } else if !append_display_character(&mut display, &mut length, character) {
            return (display, true);
        }
    }
    (display, false)
}

fn append_display_character(display: &mut String, length: &mut usize, character: char) -> bool {
    if *length == DISPLAY_PATH_MAX_CHARS {
        // Only replace the last character after observing actual overflow.
        // The limit is in Unicode characters, independently of UTF-8 byte size.
        let _last = display.pop();
        display.push('…');
        return false;
    }
    display.push(character);
    *length += 1;
    true
}

/// Rebuild the exact source scope a file-family readback echoed.
///
/// # Errors
/// Returns `Protocol` for missing, malformed, or invalid scope fields.
pub fn source_scope_from_wire(scope: Option<&Value>) -> Result<ForgeSourceScope, ForgeSourceError> {
    use forge_control::WorkspaceLocator;
    let scope = scope.ok_or(ForgeSourceError::Protocol)?;
    let locator = scope.get("locator").ok_or(ForgeSourceError::Protocol)?;
    let location_id = locator
        .get("location_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ForgeSourceError::Protocol)?;
    let expected_revision = required_i64(locator.get("expected_revision"))?;
    let relative_root = required_string(locator.get("relative_root"))?;
    let workspace_id = scope
        .get("workspace_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ForgeSourceError::Protocol)?;
    let workspace_revision = required_i64(scope.get("workspace_revision"))?;
    let built = WorkspaceLocator::new(
        forge_domain::LocationId::from_uuid(location_id),
        forge_domain::LocationRevision::new(expected_revision)
            .map_err(|_| ForgeSourceError::Protocol)?,
        std::path::Path::new(&relative_root),
    )
    .map_err(|_| ForgeSourceError::Protocol)?;
    Ok(ForgeSourceScope {
        workspace_id,
        workspace_revision,
        locator_token: built.encode(),
    })
}

/// Decode the bounded `file.list` readback with the exact acceptance class
/// of the established projection.
///
/// # Errors
/// Returns `Protocol` for any missing, malformed, or unknown readback field.
pub fn file_list_from_wire(readback: &Value) -> Result<ForgeFileList, ForgeSourceError> {
    let directory = decode_optional_path_token(readback.get("directory_base64"))?;
    let (directory_display_path, directory_display_path_truncated) =
        directory.as_ref().map_or((None, false), |directory| {
            let (display, truncated) = display_relative_path(directory);
            (Some(display), truncated)
        });
    let entries = readback
        .get("entries")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?
        .iter()
        .map(|entry| {
            let path_token = required_string(entry.get("path_base64"))?;
            let path = decode_path_token(&path_token)?;
            let (display_path, display_path_truncated) = display_relative_path(&path);
            Ok(ForgeFileListEntry {
                path_token,
                display_path,
                display_path_truncated,
                kind: file_entry_kind(
                    entry
                        .get("kind")
                        .and_then(Value::as_str)
                        .ok_or(ForgeSourceError::Protocol)?,
                )?,
                size_bytes: optional_u64(entry.get("size_bytes"))?,
            })
        })
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;
    Ok(ForgeFileList {
        scope: source_scope_from_wire(readback.get("scope"))?,
        activity_id: required_string(readback.get("activity_id"))?,
        directory_token: readback
            .get("directory_base64")
            .and_then(Value::as_str)
            .map(str::to_owned),
        directory_display_path,
        directory_display_path_truncated,
        entries,
        scanned_entries: required_u64(readback.get("scanned_entries"))?,
        truncated: required_bool(readback.get("truncated"))?,
    })
}

/// Decode the bounded `text.search` readback.
///
/// # Errors
/// Returns `Protocol` for any missing, malformed, or unknown readback field.
pub fn text_search_from_wire(readback: &Value) -> Result<ForgeTextSearch, ForgeSourceError> {
    let root = decode_optional_path_token(readback.get("root_base64"))?;
    let (root_display_path, root_display_path_truncated) =
        root.as_ref().map_or((None, false), |root| {
            let (display, truncated) = display_relative_path(root);
            (Some(display), truncated)
        });
    let matches = readback
        .get("matches")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?
        .iter()
        .map(|entry| {
            let path_token = required_string(entry.get("path_base64"))?;
            let path = decode_path_token(&path_token)?;
            let (display_path, display_path_truncated) = display_relative_path(&path);
            Ok(ForgeTextSearchMatch {
                path_token,
                display_path,
                display_path_truncated,
                line: required_u64(entry.get("line"))?,
                preview: required_string(entry.get("preview"))?,
            })
        })
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;
    Ok(ForgeTextSearch {
        scope: source_scope_from_wire(readback.get("scope"))?,
        activity_id: required_string(readback.get("activity_id"))?,
        root_token: readback
            .get("root_base64")
            .and_then(Value::as_str)
            .map(str::to_owned),
        root_display_path,
        root_display_path_truncated,
        matches,
        scanned_entries: required_u64(readback.get("scanned_entries"))?,
        scanned_files: required_u64(readback.get("scanned_files"))?,
        truncated: required_bool(readback.get("truncated"))?,
    })
}

/// Decode the bounded `file.read_slice` readback.
///
/// # Errors
/// Returns `Protocol` for any missing, malformed, or unknown readback field.
pub fn file_read_slice_from_wire(readback: &Value) -> Result<ForgeFileReadSlice, ForgeSourceError> {
    let path_token = required_string(readback.get("path_base64"))?;
    let path = decode_path_token(&path_token)?;
    let (display_path, display_path_truncated) = display_relative_path(&path);
    Ok(ForgeFileReadSlice {
        scope: source_scope_from_wire(readback.get("scope"))?,
        activity_id: required_string(readback.get("activity_id"))?,
        path_token,
        display_path,
        display_path_truncated,
        content: required_string(readback.get("content"))?,
        whole_file_hash: required_string(readback.get("whole_file_hash"))?,
        range_hash: required_string(readback.get("range_hash"))?,
        line_hashes: string_list(readback.get("line_hashes"))?,
        start_line: required_u64(readback.get("start_line"))?,
        end_line: required_u64(readback.get("end_line"))?,
        total_lines: required_u64(readback.get("total_lines"))?,
        eol: source_eol(
            readback
                .get("eol")
                .and_then(Value::as_str)
                .ok_or(ForgeSourceError::Protocol)?,
        )?,
        has_final_newline: required_bool(readback.get("has_final_newline"))?,
    })
}

fn file_entry_kind(value: &str) -> Result<ForgeFileEntryKind, ForgeSourceError> {
    match value {
        "directory" => Ok(ForgeFileEntryKind::Directory),
        "regular_file" => Ok(ForgeFileEntryKind::RegularFile),
        "symbolic_link" => Ok(ForgeFileEntryKind::SymbolicLink),
        "other" => Ok(ForgeFileEntryKind::Other),
        _ => Err(ForgeSourceError::Protocol),
    }
}

fn source_eol(value: &str) -> Result<ForgeSourceEol, ForgeSourceError> {
    match value {
        "lf" => Ok(ForgeSourceEol::Lf),
        "crlf" => Ok(ForgeSourceEol::CrLf),
        "cr" => Ok(ForgeSourceEol::Cr),
        "mixed" => Ok(ForgeSourceEol::Mixed),
        "none" => Ok(ForgeSourceEol::None),
        _ => Err(ForgeSourceError::Protocol),
    }
}

fn string_list(value: Option<&Value>) -> Result<Vec<String>, ForgeSourceError> {
    let list = value
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?;
    list.iter()
        .map(|entry| required_string(Some(entry)))
        .collect()
}

fn required_string(value: Option<&Value>) -> Result<String, ForgeSourceError> {
    value
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(ForgeSourceError::Protocol)
}

fn required_i64(value: Option<&Value>) -> Result<i64, ForgeSourceError> {
    value
        .and_then(Value::as_i64)
        .ok_or(ForgeSourceError::Protocol)
}

fn required_u64(value: Option<&Value>) -> Result<u64, ForgeSourceError> {
    value
        .and_then(Value::as_u64)
        .ok_or(ForgeSourceError::Protocol)
}

fn optional_u64(value: Option<&Value>) -> Result<Option<u64>, ForgeSourceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_u64(value).map(Some),
    }
}

fn required_bool(value: Option<&Value>) -> Result<bool, ForgeSourceError> {
    value
        .and_then(Value::as_bool)
        .ok_or(ForgeSourceError::Protocol)
}
