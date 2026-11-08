//! Typed, bounded Source read observations for the Forge catalog families.
//!
//! Each function mirrors one covered Forge HTTP command operation and returns
//! a source observation only. Readbacks never become permissions: no result
//! grants mutation rights, lease ownership, scope membership, or actor
//! identity. Hosts without the configured HTTP source keep these reads
//! fail-closed instead of switching transports.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use forge_backend::ops::{LocatorDto, ScopeDto};
use forge_control::{FileActivityScope, WorkspaceLocator};
use forge_domain::{RuntimeInstanceId, TimestampMillis, WorkspaceRelativePath};
use serde_json::Value;
use std::os::unix::ffi::OsStringExt;
use uuid::Uuid;

use super::ForgeSourceError;

pub mod goal_dag;
pub mod leases;
pub mod workspace;

/// The bounded lossy display path used by every established projection.
const DISPLAY_PATH_MAX_CHARS: usize = 512;

/// The exact source scope echoed by a Workspace-scoped readback.
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

/// The caller's live Activity attribution for one Workspace-scoped read. This is
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
        let _last = display.pop();
        display.push('…');
        return false;
    }
    display.push(character);
    *length += 1;
    true
}

/// Rebuild the exact source scope a Workspace-scoped readback echoed.
///
/// # Errors
/// Returns `Protocol` for missing, malformed, or invalid scope fields.
pub fn source_scope_from_wire(scope: Option<&Value>) -> Result<ForgeSourceScope, ForgeSourceError> {
    let scope = scope.ok_or(ForgeSourceError::Protocol)?;
    let locator = scope.get("locator").ok_or(ForgeSourceError::Protocol)?;
    let location_id = locator
        .get("location_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ForgeSourceError::Protocol)?;
    let expected_revision = locator
        .get("expected_revision")
        .and_then(Value::as_i64)
        .ok_or(ForgeSourceError::Protocol)?;
    let relative_root = locator
        .get("relative_root")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(ForgeSourceError::Protocol)?;
    let workspace_id = scope
        .get("workspace_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ForgeSourceError::Protocol)?;
    let workspace_revision = scope
        .get("workspace_revision")
        .and_then(Value::as_i64)
        .ok_or(ForgeSourceError::Protocol)?;
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
