//! Source read observations for the Location/Workspace catalog family:
//! `location.list`, `workspace.list`, `workspace.discover`, and
//! `workspace.resolve`.
//!
//! The decoders preserve the exact acceptance class of the established
//! native/HTTP projections: entries stay bounded, identities stay canonical,
//! locators round-trip their exact wire form, and discovery vocabulary is
//! closed. A lossy or unknown readback is a protocol error, never a
//! best-effort projection.

use forge_backend::ops::CommandOp;
use forge_control::{MAX_WORKSPACE_CATALOG_ENTRIES, WorkspaceLocator};
use forge_domain::{
    LocationId, LocationListLimit, LocationName, LocationRevision, WorkspaceRevision,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::super::{
    ForgeSourceError, ForgeSourceHttpClient, canonical_uuid, decode_result, map_command_error,
};

/// One configured registry Location row observed from Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeLocationRow {
    location_id: Uuid,
    display_name: String,
    revision: i64,
    state: ForgeLocationState,
}

impl ForgeLocationRow {
    /// The exact Location identity returned by Forge.
    #[must_use]
    pub const fn location_id(&self) -> Uuid {
        self.location_id
    }

    /// The canonical display name returned by Forge.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// The exact Location revision returned by Forge.
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    /// The exact enabled/disabled state returned by Forge.
    #[must_use]
    pub const fn state(&self) -> ForgeLocationState {
        self.state
    }
}

/// The closed Location lifecycle state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeLocationState {
    /// The Location accepts Workspace registration and reads.
    Enabled,
    /// The Location is administratively disabled.
    Disabled,
}

impl ForgeLocationState {
    /// The exact wire vocabulary string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

/// One Workspace catalog entry under one exact Location root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeWorkspaceCatalogRow {
    locator: WorkspaceLocator,
    locator_wire: String,
    workspace_id: Uuid,
    workspace_revision: i64,
    display_name: String,
}

impl ForgeWorkspaceCatalogRow {
    /// The canonical locator wire form returned by Forge.
    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator_wire
    }

    /// The decoded locator identity.
    #[must_use]
    pub const fn locator_identity(&self) -> &WorkspaceLocator {
        &self.locator
    }

    /// The exact Workspace identity returned by Forge.
    #[must_use]
    pub const fn workspace_id(&self) -> Uuid {
        self.workspace_id
    }

    /// The exact Workspace revision returned by Forge.
    #[must_use]
    pub const fn workspace_revision(&self) -> i64 {
        self.workspace_revision
    }

    /// The canonical display name returned by Forge.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

/// One discovery candidate observed under one exact Location root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeWorkspaceDiscoveryRow {
    locator: WorkspaceLocator,
    locator_wire: String,
    kind: ForgeDiscoveryKind,
    display_name: String,
    present: Vec<ForgeDiscoverySignal>,
    disposition: ForgeDiscoveryDisposition,
    reason: ForgeDiscoveryReason,
}

impl ForgeWorkspaceDiscoveryRow {
    /// The canonical locator wire form returned by Forge.
    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator_wire
    }

    /// The decoded locator identity.
    #[must_use]
    pub const fn locator_identity(&self) -> &WorkspaceLocator {
        &self.locator
    }

    /// The candidate kind relative to the queried Location root.
    #[must_use]
    pub const fn kind(&self) -> ForgeDiscoveryKind {
        self.kind
    }

    /// The bounded display name returned by Forge.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// The bounded, rank-ordered present-signal list.
    #[must_use]
    pub fn present(&self) -> &[ForgeDiscoverySignal] {
        &self.present
    }

    /// The closed discovery disposition.
    #[must_use]
    pub const fn disposition(&self) -> ForgeDiscoveryDisposition {
        self.disposition
    }

    /// The closed discovery reason.
    #[must_use]
    pub const fn reason(&self) -> ForgeDiscoveryReason {
        self.reason
    }
}

/// The closed discovery candidate-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiscoveryKind {
    /// The queried Location root itself.
    LocationRoot,
    /// One direct child directory of the queried Location root.
    DirectChild,
}

/// The closed workspace-signal vocabulary in rank order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiscoverySignal {
    CargoWorkspace,
    GitRepository,
    AgentInstructions,
    ForgeState,
}

impl ForgeDiscoverySignal {
    /// The exact wire vocabulary string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CargoWorkspace => "cargo_workspace",
            Self::GitRepository => "git_repository",
            Self::AgentInstructions => "agent_instructions",
            Self::ForgeState => "forge_state",
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::CargoWorkspace => 0,
            Self::GitRepository => 1,
            Self::AgentInstructions => 2,
            Self::ForgeState => 3,
        }
    }

    fn from_wire(wire: &str) -> Option<Self> {
        match wire {
            "cargo_workspace" => Some(Self::CargoWorkspace),
            "git_repository" => Some(Self::GitRepository),
            "agent_instructions" => Some(Self::AgentInstructions),
            "forge_state" => Some(Self::ForgeState),
            _ => None,
        }
    }
}

/// The closed discovery disposition vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiscoveryDisposition {
    Candidate,
    Ignored,
    NotWorkspace,
}

impl ForgeDiscoveryDisposition {
    /// The exact wire vocabulary string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Ignored => "ignored",
            Self::NotWorkspace => "not_workspace",
        }
    }
}

/// The closed discovery reason vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiscoveryReason {
    RegisteredLocationRoot,
    ForgeState,
    CargoWorkspace,
    GitRepository,
    AgentInstructions,
    VcsMetadata,
    ForgeInternalState,
    BuildOutput,
    DependencyCache,
    FrameworkCache,
    CoverageOutput,
    Cache,
    Temporary,
    PythonCache,
    VirtualEnvironment,
    HiddenMetadata,
    NoWorkspaceSignals,
}

impl ForgeDiscoveryReason {
    /// The exact wire vocabulary string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RegisteredLocationRoot => "registered_location_root",
            Self::ForgeState => "forge_state",
            Self::CargoWorkspace => "cargo_workspace",
            Self::GitRepository => "git_repository",
            Self::AgentInstructions => "agent_instructions",
            Self::VcsMetadata => "vcs_metadata",
            Self::ForgeInternalState => "forge_internal_state",
            Self::BuildOutput => "build_output",
            Self::DependencyCache => "dependency_cache",
            Self::FrameworkCache => "framework_cache",
            Self::CoverageOutput => "coverage_output",
            Self::Cache => "cache",
            Self::Temporary => "temporary",
            Self::PythonCache => "python_cache",
            Self::VirtualEnvironment => "virtual_environment",
            Self::HiddenMetadata => "hidden_metadata",
            Self::NoWorkspaceSignals => "no_workspace_signals",
        }
    }
}

/// One fresh `workspace.resolve` observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForgeWorkspaceResolution {
    workspace_id: Uuid,
    workspace_revision: i64,
}

impl ForgeWorkspaceResolution {
    /// The exact resolved Workspace identity.
    #[must_use]
    pub const fn workspace_id(&self) -> Uuid {
        self.workspace_id
    }

    /// The exact resolved Workspace revision.
    #[must_use]
    pub const fn workspace_revision(&self) -> i64 {
        self.workspace_revision
    }
}

impl ForgeSourceHttpClient {
    /// Observe the configured registry Locations (`location.list`).
    ///
    /// This is a fresh source observation, not a permission grant.
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, or protocol error.
    pub async fn location_list(&self) -> Result<Vec<ForgeLocationRow>, ForgeSourceError> {
        let readback = self
            .commands()
            .command(CommandOp::LocationList, json!({}))
            .await
            .map_err(map_command_error)?;
        location_list_output_from_wire(&readback)
    }

    /// Observe the Workspace catalog under one exact Location root
    /// (`workspace.list`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn workspace_list(
        &self,
        location_id: LocationId,
        expected_revision: LocationRevision,
    ) -> Result<Vec<ForgeWorkspaceCatalogRow>, ForgeSourceError> {
        let readback = self
            .commands()
            .command(
                CommandOp::WorkspaceList,
                workspace_catalog_wire(location_id, expected_revision),
            )
            .await
            .map_err(map_command_error)?;
        workspace_list_output_from_wire(&readback, location_id, expected_revision)
    }

    /// Observe Workspace discovery candidates under one exact Location root
    /// (`workspace.discover`). This is a read-only observation.
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn workspace_discover(
        &self,
        location_id: LocationId,
        expected_revision: LocationRevision,
    ) -> Result<Vec<ForgeWorkspaceDiscoveryRow>, ForgeSourceError> {
        let readback = self
            .commands()
            .command(
                CommandOp::WorkspaceDiscover,
                workspace_catalog_wire(location_id, expected_revision),
            )
            .await
            .map_err(map_command_error)?;
        workspace_discover_output_from_wire(&readback, location_id, expected_revision)
    }

    /// Observe one fresh `workspace.resolve` resolution for the exact
    /// relative root.
    ///
    /// The result must be compared by the caller against the identity the
    /// original caller observed; this function never treats freshness as
    /// permission.
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error. A non-UTF-8 relative root is rejected without lossy conversion.
    pub async fn resolve_workspace_identity(
        &self,
        location_id: LocationId,
        expected_revision: LocationRevision,
        relative_root: &str,
    ) -> Result<ForgeWorkspaceResolution, ForgeSourceError> {
        let readback = self
            .commands()
            .command(
                CommandOp::WorkspaceResolve,
                json!({
                    "location_id": location_id.into_uuid(),
                    "expected_revision": expected_revision.get(),
                    "relative_root": relative_root,
                }),
            )
            .await
            .map_err(map_command_error)?;
        workspace_resolve_output_from_wire(&readback)
    }
}

fn workspace_catalog_wire(location_id: LocationId, expected_revision: LocationRevision) -> Value {
    json!({
        "location_id": location_id.into_uuid(),
        "expected_revision": expected_revision.get(),
    })
}

/// Decode the bounded `location.list` readback with the exact acceptance
/// class of the established projection.
///
/// # Errors
/// Returns `Protocol` for a missing entries array, an entry over the
/// configured list bound, a non-canonical identity, a non-canonical name, or
/// an unknown state.
pub fn location_list_output_from_wire(
    readback: &Value,
) -> Result<Vec<ForgeLocationRow>, ForgeSourceError> {
    let entries = readback
        .get("entries")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?;
    if u32::try_from(entries.len()).map_or(true, |len| len > LocationListLimit::DEFAULT.get()) {
        return Err(ForgeSourceError::Protocol);
    }
    entries
        .iter()
        .map(|entry| {
            let location_id = required_string(entry.get("location_id"))?;
            let parsed_id =
                Uuid::parse_str(&location_id).map_err(|_| ForgeSourceError::Protocol)?;
            if parsed_id.to_string() != location_id {
                return Err(ForgeSourceError::Protocol);
            }
            let revision = required_i64(entry.get("revision"))?;
            LocationRevision::new(revision).map_err(|_| ForgeSourceError::Protocol)?;
            let state = match entry
                .get("state")
                .and_then(Value::as_str)
                .ok_or(ForgeSourceError::Protocol)?
            {
                "enabled" => ForgeLocationState::Enabled,
                "disabled" => ForgeLocationState::Disabled,
                _ => return Err(ForgeSourceError::Protocol),
            };
            let display_name = required_string(entry.get("display_name"))?;
            let canonical_name =
                LocationName::new(display_name.as_str()).map_err(|_| ForgeSourceError::Protocol)?;
            if canonical_name.as_str() != display_name {
                return Err(ForgeSourceError::Protocol);
            }
            Ok(ForgeLocationRow {
                location_id: parsed_id,
                display_name,
                revision,
                state,
            })
        })
        .collect()
}

/// Decode the bounded `workspace.list` readback, requiring every entry to sit
/// under the exact queried Location root locator.
///
/// # Errors
/// Returns `Protocol` for a missing entries array, an entry over the catalog
/// bound, a non-canonical or foreign locator, a non-canonical identity, or a
/// non-canonical name.
pub fn workspace_list_output_from_wire(
    readback: &Value,
    expected_location_id: LocationId,
    expected_revision: LocationRevision,
) -> Result<Vec<ForgeWorkspaceCatalogRow>, ForgeSourceError> {
    let entries = readback
        .get("entries")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?;
    if entries.len() > MAX_WORKSPACE_CATALOG_ENTRIES {
        return Err(ForgeSourceError::Protocol);
    }
    let expected_root_locator = WorkspaceLocator::new(
        expected_location_id,
        expected_revision,
        std::path::Path::new("."),
    )
    .map_err(|_| ForgeSourceError::Protocol)?;
    entries
        .iter()
        .map(|entry| {
            let locator_wire = required_string(entry.get("locator"))?;
            let locator =
                WorkspaceLocator::decode(&locator_wire).map_err(|_| ForgeSourceError::Protocol)?;
            if locator.encode() != locator_wire || locator != expected_root_locator {
                return Err(ForgeSourceError::Protocol);
            }
            let display_name = required_string(entry.get("display_name"))?;
            let canonical_name =
                LocationName::new(display_name.as_str()).map_err(|_| ForgeSourceError::Protocol)?;
            if canonical_name.as_str() != display_name {
                return Err(ForgeSourceError::Protocol);
            }
            let workspace_id =
                canonical_uuid(required_string(entry.get("workspace_id"))?.as_str())?;
            let workspace_revision = required_i64(entry.get("workspace_revision"))?;
            WorkspaceRevision::new(workspace_revision).map_err(|_| ForgeSourceError::Protocol)?;
            Ok(ForgeWorkspaceCatalogRow {
                locator,
                locator_wire,
                workspace_id,
                workspace_revision,
                display_name,
            })
        })
        .collect()
}

/// Decode the bounded `workspace.discover` readback with the exact closed
/// vocabulary of the established projection.
///
/// # Errors
/// Returns `Protocol` for any unknown field set, non-canonical or foreign
/// locator, kind/root mismatch, oversized, unordered, or unknown signal, or
/// unknown disposition or reason.
pub fn workspace_discover_output_from_wire(
    readback: &Value,
    expected_location_id: LocationId,
    expected_revision: LocationRevision,
) -> Result<Vec<ForgeWorkspaceDiscoveryRow>, ForgeSourceError> {
    let object = readback.as_object().ok_or(ForgeSourceError::Protocol)?;
    if object.len() != 1 || !object.contains_key("entries") {
        return Err(ForgeSourceError::Protocol);
    }
    let entries = object
        .get("entries")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?;
    if entries.len() > MAX_WORKSPACE_CATALOG_ENTRIES {
        return Err(ForgeSourceError::Protocol);
    }
    entries
        .iter()
        .map(|entry| discovery_entry_from_wire(entry, expected_location_id, expected_revision))
        .collect()
}

/// Decode one bounded discovery entry with the exact closed vocabulary of
/// the established projection.
///
/// # Errors
/// Returns `Protocol` for any unknown field set, non-canonical or foreign
/// locator, kind/root mismatch, oversized, unordered, or unknown signal, or
/// unknown disposition or reason.
fn discovery_entry_from_wire(
    entry: &Value,
    expected_location_id: LocationId,
    expected_revision: LocationRevision,
) -> Result<ForgeWorkspaceDiscoveryRow, ForgeSourceError> {
    const FIELDS: [&str; 6] = [
        "locator",
        "kind",
        "display_name",
        "signals",
        "disposition",
        "reason",
    ];
    let object = entry.as_object().ok_or(ForgeSourceError::Protocol)?;
    if object.len() != FIELDS.len() || FIELDS.iter().any(|field| !object.contains_key(*field)) {
        return Err(ForgeSourceError::Protocol);
    }
    let locator_wire = required_string(object.get("locator"))?;
    let locator =
        WorkspaceLocator::decode(&locator_wire).map_err(|_| ForgeSourceError::Protocol)?;
    if locator.encode() != locator_wire
        || locator.location_id() != expected_location_id
        || locator.expected_revision() != expected_revision
    {
        return Err(ForgeSourceError::Protocol);
    }
    let kind = match required_string(object.get("kind"))?.as_str() {
        "location_root" => ForgeDiscoveryKind::LocationRoot,
        "direct_child" => ForgeDiscoveryKind::DirectChild,
        _ => return Err(ForgeSourceError::Protocol),
    };
    let is_location_root = locator.relative_root_bytes() == b".";
    if !matches!(
        (kind, is_location_root),
        (ForgeDiscoveryKind::LocationRoot, true) | (ForgeDiscoveryKind::DirectChild, false)
    ) {
        return Err(ForgeSourceError::Protocol);
    }
    let present = discovery_signals_from_wire(object.get("signals"))?;
    let display_name = required_string(object.get("display_name"))?;
    if bounded_display_name(&display_name) != display_name {
        return Err(ForgeSourceError::Protocol);
    }
    let disposition = match required_string(object.get("disposition"))?.as_str() {
        "candidate" => ForgeDiscoveryDisposition::Candidate,
        "ignored" => ForgeDiscoveryDisposition::Ignored,
        "not_workspace" => ForgeDiscoveryDisposition::NotWorkspace,
        _ => return Err(ForgeSourceError::Protocol),
    };
    let reason = discovery_reason_from_wire(object.get("reason"))?;
    Ok(ForgeWorkspaceDiscoveryRow {
        locator,
        locator_wire,
        kind,
        display_name,
        present,
        disposition,
        reason,
    })
}

/// Decode the bounded `{present: [...]}` signal set, enforcing the closed
/// vocabulary and its strict rank order.
///
/// # Errors
/// Returns `Protocol` for a missing signal object, an oversized list, or an
/// unknown, duplicated, or unordered signal.
fn discovery_signals_from_wire(
    signals: Option<&Value>,
) -> Result<Vec<ForgeDiscoverySignal>, ForgeSourceError> {
    let signals = signals
        .and_then(Value::as_object)
        .ok_or(ForgeSourceError::Protocol)?;
    if signals.len() != 1 || !signals.contains_key("present") {
        return Err(ForgeSourceError::Protocol);
    }
    let present = signals
        .get("present")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?;
    if present.len() > 4 {
        return Err(ForgeSourceError::Protocol);
    }
    let mut decoded_signals = Vec::with_capacity(present.len());
    let mut last_rank: Option<u8> = None;
    for signal in present {
        let signal = ForgeDiscoverySignal::from_wire(&required_string(Some(signal))?)
            .ok_or(ForgeSourceError::Protocol)?;
        if last_rank.is_some_and(|previous| signal.rank() <= previous) {
            return Err(ForgeSourceError::Protocol);
        }
        last_rank = Some(signal.rank());
        decoded_signals.push(signal);
    }
    Ok(decoded_signals)
}

/// Decode the closed discovery-reason vocabulary.
///
/// # Errors
/// Returns `Protocol` for any string outside the vocabulary.
fn discovery_reason_from_wire(
    reason: Option<&Value>,
) -> Result<ForgeDiscoveryReason, ForgeSourceError> {
    match required_string(reason)?.as_str() {
        "registered_location_root" => Ok(ForgeDiscoveryReason::RegisteredLocationRoot),
        "forge_state" => Ok(ForgeDiscoveryReason::ForgeState),
        "cargo_workspace" => Ok(ForgeDiscoveryReason::CargoWorkspace),
        "git_repository" => Ok(ForgeDiscoveryReason::GitRepository),
        "agent_instructions" => Ok(ForgeDiscoveryReason::AgentInstructions),
        "vcs_metadata" => Ok(ForgeDiscoveryReason::VcsMetadata),
        "forge_internal_state" => Ok(ForgeDiscoveryReason::ForgeInternalState),
        "build_output" => Ok(ForgeDiscoveryReason::BuildOutput),
        "dependency_cache" => Ok(ForgeDiscoveryReason::DependencyCache),
        "framework_cache" => Ok(ForgeDiscoveryReason::FrameworkCache),
        "coverage_output" => Ok(ForgeDiscoveryReason::CoverageOutput),
        "cache" => Ok(ForgeDiscoveryReason::Cache),
        "temporary" => Ok(ForgeDiscoveryReason::Temporary),
        "python_cache" => Ok(ForgeDiscoveryReason::PythonCache),
        "virtual_environment" => Ok(ForgeDiscoveryReason::VirtualEnvironment),
        "hidden_metadata" => Ok(ForgeDiscoveryReason::HiddenMetadata),
        "no_workspace_signals" => Ok(ForgeDiscoveryReason::NoWorkspaceSignals),
        _ => Err(ForgeSourceError::Protocol),
    }
}

/// Decode the exact `workspace.resolve` readback. Unlike the catalog lists,
/// the resolution readback is strictly two-field: this matches the backend's
/// existing production resolve readback contract.
///
/// # Errors
/// Returns `Protocol` for any extra field, a non-canonical Workspace
/// identity, or a non-positive revision.
pub fn workspace_resolve_output_from_wire(
    readback: &Value,
) -> Result<ForgeWorkspaceResolution, ForgeSourceError> {
    let decoded: WorkspaceResolveReadback = decode_result(readback.clone())?;
    let workspace_id = canonical_uuid(&decoded.workspace_id)?;
    if decoded.workspace_revision <= 0 {
        return Err(ForgeSourceError::Protocol);
    }
    WorkspaceRevision::new(decoded.workspace_revision).map_err(|_| ForgeSourceError::Protocol)?;
    Ok(ForgeWorkspaceResolution {
        workspace_id,
        workspace_revision: decoded.workspace_revision,
    })
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceResolveReadback {
    workspace_id: String,
    workspace_revision: i64,
}

/// The established display-name bound, mirroring the shared projection
/// limit of 512 characters.
const MAX_DISPLAY_NAME_CHARS: usize = 512;

fn bounded_display_name(value: &str) -> String {
    value.chars().take(MAX_DISPLAY_NAME_CHARS).collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::WorkspaceId;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn location_id() -> LocationId {
        // A letter-bearing identity so canonical-form mutations are visible.
        LocationId::from_uuid(Uuid::from_u128(0xA501))
    }

    fn revision() -> Result<LocationRevision, Box<dyn std::error::Error>> {
        Ok(LocationRevision::new(5)?)
    }

    fn root_locator() -> Result<WorkspaceLocator, Box<dyn std::error::Error>> {
        Ok(WorkspaceLocator::new(
            location_id(),
            revision()?,
            std::path::Path::new("."),
        )?)
    }

    fn child_locator() -> Result<WorkspaceLocator, Box<dyn std::error::Error>> {
        Ok(WorkspaceLocator::new(
            location_id(),
            revision()?,
            std::path::Path::new("alpha"),
        )?)
    }

    #[test]
    fn location_list_preserves_exact_rows_and_rejects_drift() -> TestResult {
        let readback = json!({
            "entries": [
                {
                    "location_id": location_id().into_uuid(),
                    "display_name": "Local Disk",
                    "revision": 5,
                    "state": "enabled",
                }
            ]
        });
        let rows = location_list_output_from_wire(&readback)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].location_id(), location_id().into_uuid());
        assert_eq!(rows[0].display_name(), "Local Disk");
        assert_eq!(rows[0].revision(), 5);
        assert_eq!(rows[0].state(), ForgeLocationState::Enabled);

        let mut noncanonical = readback.clone();
        noncanonical["entries"][0]["location_id"] =
            json!(location_id().into_uuid().to_string().to_ascii_uppercase());
        assert_eq!(
            location_list_output_from_wire(&noncanonical),
            Err(ForgeSourceError::Protocol)
        );

        let mut unknown_state = readback;
        unknown_state["entries"][0]["state"] = json!("paused");
        assert_eq!(
            location_list_output_from_wire(&unknown_state),
            Err(ForgeSourceError::Protocol)
        );
        assert_eq!(
            location_list_output_from_wire(&json!({})),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }

    #[test]
    fn workspace_list_requires_the_exact_root_locator() -> TestResult {
        let readback = json!({
            "entries": [
                {
                    "locator": root_locator()?.encode(),
                    "display_name": "Root Workspace",
                    "workspace_id": WorkspaceId::from_uuid(Uuid::from_u128(0x502)).into_uuid(),
                    "workspace_revision": 8,
                }
            ]
        });
        let rows = workspace_list_output_from_wire(&readback, location_id(), revision()?)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].locator(), root_locator()?.encode());
        assert_eq!(rows[0].workspace_id(), Uuid::from_u128(0x502));
        assert_eq!(rows[0].workspace_revision(), 8);

        let foreign = json!({
            "entries": [
                {
                    "locator": child_locator()?.encode(),
                    "display_name": "Child Workspace",
                    "workspace_id": WorkspaceId::from_uuid(Uuid::from_u128(0x503)).into_uuid(),
                    "workspace_revision": 8,
                }
            ]
        });
        assert_eq!(
            workspace_list_output_from_wire(&foreign, location_id(), revision()?),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }

    #[test]
    fn discover_preserves_the_closed_vocabulary_and_rejects_unknowns() -> TestResult {
        let readback = json!({
            "entries": [
                {
                    "locator": child_locator()?.encode(),
                    "kind": "direct_child",
                    "display_name": "alpha",
                    "signals": {"present": ["cargo_workspace", "git_repository"]},
                    "disposition": "candidate",
                    "reason": "cargo_workspace",
                }
            ]
        });
        let rows = workspace_discover_output_from_wire(&readback, location_id(), revision()?)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind(), ForgeDiscoveryKind::DirectChild);
        assert_eq!(
            rows[0].present(),
            [
                ForgeDiscoverySignal::CargoWorkspace,
                ForgeDiscoverySignal::GitRepository
            ]
        );
        assert_eq!(rows[0].disposition(), ForgeDiscoveryDisposition::Candidate);
        assert_eq!(rows[0].reason(), ForgeDiscoveryReason::CargoWorkspace);

        let kind_root_mismatch = json!({
            "entries": [
                {
                    "locator": child_locator()?.encode(),
                    "kind": "location_root",
                    "display_name": "alpha",
                    "signals": {"present": []},
                    "disposition": "candidate",
                    "reason": "cargo_workspace",
                }
            ]
        });
        assert_eq!(
            workspace_discover_output_from_wire(&kind_root_mismatch, location_id(), revision()?),
            Err(ForgeSourceError::Protocol)
        );

        let mut unordered = readback.clone();
        unordered["entries"][0]["signals"]["present"] =
            json!(["git_repository", "cargo_workspace"]);
        assert_eq!(
            workspace_discover_output_from_wire(&unordered, location_id(), revision()?),
            Err(ForgeSourceError::Protocol)
        );

        let mut unknown_reason = readback;
        unknown_reason["entries"][0]["reason"] = json!("vibes");
        assert_eq!(
            workspace_discover_output_from_wire(&unknown_reason, location_id(), revision()?),
            Err(ForgeSourceError::Protocol)
        );

        assert_eq!(
            workspace_discover_output_from_wire(
                &json!({"entries": [], "extra": 1}),
                location_id(),
                revision()?
            ),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }

    #[test]
    fn resolve_readback_is_strictly_two_canonical_fields() -> TestResult {
        let readback = json!({
            "workspace_id": WorkspaceId::from_uuid(Uuid::from_u128(0x504)).into_uuid(),
            "workspace_revision": 8,
        });
        let resolution = workspace_resolve_output_from_wire(&readback)?;
        assert_eq!(resolution.workspace_id(), Uuid::from_u128(0x504));
        assert_eq!(resolution.workspace_revision(), 8);

        let mut extra = readback.clone();
        extra["found"] = json!(true);
        assert_eq!(
            workspace_resolve_output_from_wire(&extra),
            Err(ForgeSourceError::Protocol)
        );

        let mut stale = readback;
        stale["workspace_revision"] = json!(0);
        assert_eq!(
            workspace_resolve_output_from_wire(&stale),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }
}
