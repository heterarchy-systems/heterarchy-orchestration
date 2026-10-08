//! Wire contracts for the Location/Workspace catalog source reads:
//! `location.list`, `workspace.list`, `workspace.discover`, and
//! `workspace.resolve`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::super::leadership::WorkspaceQueryScope;

/// `POST /internal/v1/forge/location-list` request: strictly empty.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocationListRequest {}

/// `POST /internal/v1/forge/location-list` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocationListResponse {
    pub entries: Vec<LocationRowWire>,
}

/// One configured registry Location row.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocationRowWire {
    pub location_id: Uuid,
    pub display_name: String,
    pub revision: i64,
    pub state: LocationStateWire,
}

/// The closed Location lifecycle state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LocationStateWire {
    Enabled,
    Disabled,
}

/// `POST /internal/v1/forge/workspace-list` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceListRequest {
    pub location_id: Uuid,
    pub expected_revision: i64,
}

/// `POST /internal/v1/forge/workspace-list` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceListResponse {
    pub entries: Vec<WorkspaceCatalogEntryWire>,
}

/// One Workspace catalog entry under one exact Location root.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceCatalogEntryWire {
    pub locator: String,
    pub display_name: String,
    pub workspace_id: Uuid,
    pub workspace_revision: i64,
}

/// `POST /internal/v1/forge/workspace-discover` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceDiscoverRequest {
    pub location_id: Uuid,
    pub expected_revision: i64,
}

/// `POST /internal/v1/forge/workspace-discover` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceDiscoverResponse {
    pub entries: Vec<WorkspaceDiscoveryEntryWire>,
}

/// One discovery candidate under one exact Location root.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceDiscoveryEntryWire {
    pub locator: String,
    pub kind: DiscoveryKindWire,
    pub display_name: String,
    pub signals: DiscoverySignalsWire,
    pub disposition: DiscoveryDispositionWire,
    pub reason: DiscoveryReasonWire,
}

/// The closed discovery candidate-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryKindWire {
    LocationRoot,
    DirectChild,
}

/// The bounded present-signal set.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscoverySignalsWire {
    pub present: Vec<DiscoverySignalWire>,
}

/// The closed workspace-signal vocabulary in rank order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoverySignalWire {
    CargoWorkspace,
    GitRepository,
    AgentInstructions,
    ForgeState,
}

/// The closed discovery disposition vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryDispositionWire {
    Candidate,
    Ignored,
    NotWorkspace,
}

/// The closed discovery reason vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryReasonWire {
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

/// `POST /internal/v1/workspaces/{workspace_id}/forge/workspace-resolve`
/// request: the exact caller-observed Workspace reference plus the optional
/// logical Session selector. Selectors never grant permission.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceResolveRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
}

/// The exact source scope echoed by a source read.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceScopeWire {
    pub workspace_id: Uuid,
    pub workspace_revision: i64,
    pub locator: String,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/workspace-resolve`
/// response: the fresh source resolution for the requested relative root.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceResolveResponse {
    pub workspace_id: Uuid,
    pub workspace_revision: i64,
    pub locator: String,
}
